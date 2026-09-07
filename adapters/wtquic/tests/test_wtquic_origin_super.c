/*
 * The parent/child report path, with real processes and a real pipe.
 *
 * A codec round trip proves nothing about this: the questions here are whether
 * the parent reads exact bytes across a real pipe, refuses a child that lies,
 * is cut short, writes twice or dies on a signal, and whether every child it
 * owns is reaped. Each case forks an actual child.
 *
 * No transport, socket, certificate or provider is involved: the child's work
 * is a callback that fills a report.
 */
#include "origin_super.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
static int checks;

#define CHECK(cond)                                                          \
    do {                                                                     \
        checks++;                                                            \
        if (!(cond)) {                                                       \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond);          \
            failures++;                                                      \
        }                                                                    \
    } while (0)

typedef enum {
    CHILD_GOOD_DENIAL = 0,
    CHILD_LIES_PASSED,      /* claims a pass its snapshot does not support */
    CHILD_WRONG_ROW,
    CHILD_SILENT,           /* exits writing nothing */
    CHILD_BLOCKS,           /* never exits on its own */
    CHILD_SIGNALS,          /* dies on a signal */
    CHILD_EXIT_DISAGREES,   /* a good report, but the wrong exit code */
    CHILD_NO_TRANSITION,    /* reports without announcing teardown */
    CHILD_DOUBLE_TRANSITION,/* announces teardown twice */
    CHILD_SLOW_TEARDOWN,    /* announces, then overruns the grace */
    CHILD_SLOW_WORK,        /* takes most of the work budget, then is normal */
    CHILD_BLOCKS_IN_WORK,   /* never announces, never exits */
    CHILD_PASSED_DOMAIN,    /* a checksummed report whose verdict field is 2 */
    CHILD_ROW_CORRECT,      /* reports whatever THIS row expects */
    CHILD_CROSS_PHASE,      /* substantial work AND substantial teardown */
    CHILD_FRAME_BAD_SUM,    /* a transition whose checksum does not hold */
    CHILD_FRAME_BAD_MAGIC,  /* a transition with the wrong magic */
    CHILD_FRAME_BAD_BYTES,  /* a transition declaring the wrong size */
    CHILD_FRAME_WRONG_KIND, /* a well-formed frame that is not the transition */
    CHILD_FRAME_FUTURE,     /* claims it transitioned after the parent read */
    CHILD_FRAME_PAST        /* claims it transitioned before the row began */
} child_mode_t;

typedef struct {
    child_mode_t mode;
    int          mangle_kind;   /* 0 none, 1 truncate, 2 corrupt, 3 duplicate */
    int          work_ms;       /* CHILD_CROSS_PHASE: time spent before the
                                 * transition */
    int          teardown_ms;   /* CHILD_CROSS_PHASE: time spent after it */
    int          before_ms;     /* parent-side setup delay, before launch */
    int          after_ms;      /* parent-side delay, after launch */
    int          wait_ms;       /* parent descheduled before its FIRST wait */
    int          waits;         /* parent-side: waits seen so far */
    int          score_ms;      /* parent descheduled before judging */
    int          eof_ms;        /* parent descheduled on the read that is EOF */
    size_t       read_cap;      /* cap on one read, so a boundary is real */
    int          eof_reads;     /* parent-side: EOF reads actually seen */
    int          data_reads;    /* parent-side: data reads actually seen */
} tctx_t;

/* The same clock every deadline in this fixture is expressed in. */
static long long
mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void
burn_ms(int ms)
{
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000 * 1000;
    nanosleep(&ts, NULL);
}

static void
hook_before(void *ctx) { burn_ms(((tctx_t *)ctx)->before_ms); }
static void
hook_after(void *ctx)  { burn_ms(((tctx_t *)ctx)->after_ms); }

/*
 * The parent loses the CPU before the FIRST wait -- counted, not inferred from
 * how much has arrived. Which read a byte count precedes is not something the
 * count can say: the pipe may split or join any write.
 */
static void
hook_wait(void *ctx, size_t got)
{
    tctx_t *t = ctx;

    (void)got;
    if (t->waits++ != 0) {
        return;
    }
    burn_ms(t->wait_ms);
}

static void
hook_score(void *ctx) { burn_ms(((tctx_t *)ctx)->score_ms); }

/*
 * The delay that has to land on the EOF read and no other. Only the read's own
 * result can say which one that is: how much arrived earlier is a fact about
 * previous reads, and the pipe is free to split any write, so a nonzero total
 * does not mean the next read returns EOF.
 */
static void
hook_after_read(void *ctx, long long k)
{
    tctx_t *t = ctx;

    if (k == 0) {
        t->eof_reads++;
        burn_ms(t->eof_ms);
    } else {
        t->data_reads++;
    }
}

/* A complete, correct ESTABLISHED observation. */
static void
fill_good_positive(origin_obs_t *o)
{
    origin_sealed_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.query_ok = true;
    rec.image_exact = true;
    rec.kind = ORIGIN_SEALED_KIND_NONE;

    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    origin_obs_close_attempt(o);
    (void)origin_obs_close_attempted(o);
    origin_obs_closed(o);
    origin_obs_sealed(o, &rec);
    origin_obs_close_result(o, true);
    origin_obs_barrier_done(o);
    origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o);
    origin_obs_teardown_ok(o);
}

/* A complete, correct 403 observation for the given row. */
static void
fill_good_denial(origin_obs_t *o)
{
    origin_sealed_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.query_ok = true;
    rec.image_exact = true;
    rec.kind = ORIGIN_SEALED_KIND_NONE;

    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_profile_unselected(o, true, true);
    origin_obs_sealed(o, &rec);
    origin_obs_barrier_done(o);
    origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o);
    origin_obs_teardown_ok(o);
}

static int
child_body(void *ctx, size_t row_index, origin_report_t *out)
{
    tctx_t *t = ctx;
    origin_obs_t *o = origin_obs_new();
    origin_snapshot_t s;
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);
    bool passed;

    if (t->mode == CHILD_CROSS_PHASE) {
        /* Each phase stays inside its OWN budget while their sum exceeds the
         * work budget: the only thing that can accept this row is a teardown
         * clock that really is separate. */
        struct timespec ts = { 0, 0 };
        ts.tv_nsec = (long)t->work_ms * 1000 * 1000;
        nanosleep(&ts, NULL);
        origin_super_child_enter_teardown();
        ts.tv_nsec = (long)t->teardown_ms * 1000 * 1000;
        nanosleep(&ts, NULL);
        fill_good_denial(o);
        origin_obs_snapshot(o, &s);
        passed = origin_row_passed(&rows[row_index], &s, NULL, 0);
        origin_report_init(out, (uint32_t)row_index, passed, &s);
        origin_obs_free(o);
        return passed ? 0 : 1;
    }
    if (t->mode >= CHILD_FRAME_BAD_SUM && t->mode <= CHILD_FRAME_PAST) {
        /* one malformed transition on the REAL wire, then a normal report */
        origin_phase_t ph;

        origin_phase_init(&ph, ORIGIN_PHASE_ENTER_TEARDOWN,
                          (uint64_t)mono_ms());
        switch (t->mode) {
        case CHILD_FRAME_BAD_MAGIC:  ph.magic = 0x4f504830u;     break;
        case CHILD_FRAME_BAD_BYTES:  ph.bytes = (uint32_t)sizeof(ph) - 4u;
                                                                 break;
        case CHILD_FRAME_WRONG_KIND: ph.kind = 7u;               break;
        case CHILD_FRAME_FUTURE:     ph.at_ms += 1000000u;       break;
        case CHILD_FRAME_PAST:       ph.at_ms = 0u;              break;
        default:                                                 break;
        }
        /*
         * Every case except the checksum one is RESEALED, so exactly one
         * field is wrong. Without that a broken magic would also break the
         * checksum, and the checksum rule would answer for both.
         */
        if (t->mode != CHILD_FRAME_BAD_SUM) {
            origin_phase_reseal(&ph);
        } else {
            ph.checksum ^= 0x5a5a5a5au;
        }
        origin_super_child_emit_phase(&ph);
        fill_good_denial(o);
        origin_obs_snapshot(o, &s);
        passed = origin_row_passed(&rows[row_index], &s, NULL, 0);
        origin_report_init(out, (uint32_t)row_index, passed, &s);
        origin_obs_free(o);
        return passed ? 0 : 1;
    }
    if (t->mode == CHILD_SLOW_WORK) {
        /* legal work that nearly fills the budget, then a legal teardown */
        struct timespec ts = { 0, 250 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    if (t->mode == CHILD_BLOCKS_IN_WORK) {
        for (;;) {
            struct timespec ts = { 1, 0 };
            nanosleep(&ts, NULL);       /* never announces teardown */
        }
    }
    if (t->mode == CHILD_BLOCKS) {
        for (;;) {
            struct timespec ts = { 1, 0 };
            nanosleep(&ts, NULL);   /* the parent must stop this from outside */
        }
    }
    if (t->mode == CHILD_SIGNALS) {
        raise(SIGKILL);
    }
    if (t->mode == CHILD_SILENT) {
        origin_obs_free(o);
        _exit(3);                   /* exits before the supervisor writes */
    }
    if (t->mode == CHILD_SLOW_TEARDOWN) {
        /* announce the transition, then take longer than the grace allows */
        origin_super_child_enter_teardown();
        for (;;) {
            struct timespec ts = { 1, 0 };
            nanosleep(&ts, NULL);
        }
    }
    if (t->mode == CHILD_ROW_CORRECT) {
        /* build whatever THIS row expects, so a whole table can pass */
        if (rows[row_index].expect == ROW_EXPECT_ESTABLISH) {
            fill_good_positive(o);
        } else {
            fill_good_denial(o);
        }
    } else {
        fill_good_denial(o);
    }
    if (t->mode != CHILD_NO_TRANSITION) {
        origin_super_child_enter_teardown();
        if (t->mode == CHILD_DOUBLE_TRANSITION) {
            origin_super_child_enter_teardown();
        }
    }
    origin_obs_snapshot(o, &s);
    passed = origin_row_passed(&rows[row_index], &s, NULL, 0);
    origin_report_init(out, (uint32_t)row_index, passed, &s);
    if (t->mode == CHILD_LIES_PASSED) {
        /* keep the (failing) snapshot but claim success, re-checksumming so
         * only the parent's recomputation can catch it */
        origin_snapshot_t bad = s;
        bad.status = 404;           /* not an Origin denial */
        origin_report_init(out, (uint32_t)row_index, true, &bad);
    }
    if (t->mode == CHILD_WRONG_ROW) {
        origin_report_init(out, (uint32_t)row_index + 1u, passed, &s);
    }
    if (t->mode == CHILD_PASSED_DOMAIN) {
        /* a properly checksummed record whose verdict field is neither 0 nor
         * 1: a silent truncation would read 2 as true */
        origin_report_init(out, (uint32_t)row_index, passed, &s);
        out->passed = 2u;
        origin_report_reseal(out);
    }
    origin_obs_free(o);
    if (t->mode == CHILD_EXIT_DISAGREES) {
        return 1;                   /* a passing report with a failing exit */
    }
    return passed ? 0 : 1;
}

static size_t
mangle(void *ctx, void *buf, size_t len, size_t cap)
{
    tctx_t *t = ctx;
    unsigned char *p = buf;
    switch (t->mangle_kind) {
    case 1:
        return len > 8 ? len - 8 : 0;          /* a short record */
    case 2:
        p[len / 2] ^= 0xffu;                   /* a corrupted record */
        return len;
    case 3:
        if (len * 2 <= cap) {                  /* a second record */
            memcpy(p + len, p, len);
            return len * 2;
        }
        return len;
    default:
        return len;
    }
}

/* One run with every timing knob spelled out. */
static void
run_timed(tctx_t *t, int budget_ms, int grace_ms, origin_child_result_t *r)
{
    origin_super_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.child = child_body;
    cfg.ctx = t;
    cfg.row_budget_ms = budget_ms;
    cfg.teardown_grace_ms = grace_ms;
    cfg.mangle = t->mangle_kind != 0 ? mangle : NULL;
    cfg.on_before_launch = t->before_ms > 0 ? hook_before : NULL;
    cfg.on_after_launch = t->after_ms > 0 ? hook_after : NULL;
    cfg.on_before_wait = t->wait_ms > 0 ? hook_wait : NULL;
    cfg.read_cap = t->read_cap;
    cfg.on_before_score = t->score_ms > 0 ? hook_score : NULL;
    /* always installed: it is also how a case proves which reads happened */
    cfg.on_after_read = hook_after_read;
    origin_super_run_row(&cfg, 2, r);   /* row 2 is a 403 denial */
}

static void
run_one_ex(child_mode_t mode, int mangle_kind, int budget_ms, int grace_ms,
           origin_child_result_t *r)
{
    tctx_t t;

    memset(&t, 0, sizeof(t));
    t.mode = mode;
    t.mangle_kind = mangle_kind;
    run_timed(&t, budget_ms, grace_ms, r);
}

static void
run_one(child_mode_t mode, int mangle_kind, int budget_ms,
        origin_child_result_t *r)
{
    run_one_ex(mode, mangle_kind, budget_ms, 2000, r);
}

int
main(void)
{
    origin_child_result_t r;

    /* -- the honest path -------------------------------------------------- */
    run_one(CHILD_GOOD_DENIAL, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_OK);
    CHECK(r.passed);
    CHECK(r.reaped && r.pid > 0);
    CHECK(r.exit_code == 0 && r.signal == 0);

    /* -- the parent decides, not the child -------------------------------- */
    run_one(CHILD_LIES_PASSED, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_CLAIM_MISMATCH);
    CHECK(!r.passed);
    CHECK(r.reaped);

    /* -- a report for another row is not this row's evidence --------------- */
    run_one(CHILD_WRONG_ROW, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_REPORT);
    CHECK(r.reaped);

    /* -- no report at all -------------------------------------------------- */
    run_one(CHILD_SILENT, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_NO_REPORT);
    CHECK(!r.passed);
    CHECK(r.reaped && r.exit_code == 3);

    /* -- short, corrupt and duplicated records ----------------------------- */
    run_one(CHILD_GOOD_DENIAL, 1, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_REPORT);
    CHECK(r.reaped);
    run_one(CHILD_GOOD_DENIAL, 2, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_REPORT);
    CHECK(r.reaped);
    run_one(CHILD_GOOD_DENIAL, 3, 5000, &r);
    checks++;
    if (r.status != ORIGIN_CHILD_EXTRA_DATA) {
        printf("FAIL: duplicate record gave status %d, detail: %s\n",
               (int)r.status, r.detail);
        failures++;
    }
    CHECK(r.reaped);

    /* -- a signal is never a row result ------------------------------------ */
    run_one(CHILD_SIGNALS, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_SIGNAL);
    CHECK(!r.passed && r.signal != 0);
    CHECK(r.reaped);

    /* -- exit status must agree with the report ---------------------------- */
    run_one(CHILD_EXIT_DISAGREES, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_EXIT_MISMATCH);
    CHECK(r.reaped);

    /* -- a blocked child is stopped from outside, and reaped --------------- */
    {
        pid_t blocked;
        run_one(CHILD_BLOCKS, 0, 300, &r);   /* short injected budget */
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(!r.passed);
        CHECK(r.reaped);
        blocked = r.pid;
        CHECK(blocked > 0);
        /* the owned child is gone: signal 0 to the exact pid must fail */
        checks++;
        if (blocked > 0 && kill(blocked, 0) == 0) {
            printf("FAIL: the timed-out child %ld is still alive\n",
                   (long)blocked);
            failures++;
        }
    }

    /* -- an unrelated sentinel process is untouched ------------------------ */
    {
        pid_t sentinel = fork();
        if (sentinel == 0) {
            struct timespec ts = { 2, 0 };
            nanosleep(&ts, NULL);
            _exit(0);
        }
        CHECK(sentinel > 0);
        run_one(CHILD_BLOCKS, 0, 200, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT && r.reaped);
        /* the supervisor stops its OWN child by pid; nothing else */
        checks++;
        if (kill(sentinel, 0) != 0) {
            printf("FAIL: an unrelated process was killed\n");
            failures++;
        }
        kill(sentinel, SIGTERM);
        {
            int st;
            while (waitpid(sentinel, &st, 0) < 0) { }
        }
    }

    /*
     * -- the three parent deadlines, all four directions --------------------
     *
     * The parent cannot tell work from teardown by watching a silent pipe, so
     * these prove it is using the child's own transition: near-full work plus a
     * legal teardown is accepted, and each phase is stopped on its OWN clock.
     */
    /* 1. near-full work plus a legal teardown is ACCEPTED */
    run_one_ex(CHILD_SLOW_WORK, 0, 400, 400, &r);
    CHECK(r.status == ORIGIN_CHILD_OK);
    CHECK(r.passed && r.reaped);

    /* 2. work that never ends is stopped on the WORK deadline */
    run_one_ex(CHILD_BLOCKS_IN_WORK, 0, 250, 4000, &r);
    CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
    CHECK(strstr(r.detail, "work deadline") != NULL);
    CHECK(r.reaped && !r.passed);

    /* 3. teardown that never ends is stopped on the TEARDOWN grace, and the
     *    generous work budget is not what caught it */
    run_one_ex(CHILD_SLOW_TEARDOWN, 0, 4000, 250, &r);
    CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
    CHECK(strstr(r.detail, "teardown grace") != NULL);
    CHECK(r.reaped && !r.passed);

    /*
     * 3b. A SUCCESSFUL row that spends substantial time in BOTH phases. Each
     * phase is inside its own budget; their sum is not. Only a genuinely
     * separate teardown clock can accept this, so deleting or aliasing that
     * reset turns this row into a timeout.
     */
    {
        tctx_t t;
        long long t0, elapsed;

        memset(&t, 0, sizeof(t));
        t.mode = CHILD_CROSS_PHASE;
        t.work_ms = 300;
        t.teardown_ms = 300;
        t0 = mono_ms();
        run_timed(&t, 500, 500, &r);
        elapsed = mono_ms() - t0;
        CHECK(r.status == ORIGIN_CHILD_OK);
        CHECK(r.passed && r.reaped);
        /* it really did cross the work budget: a run that finished inside it
         * would prove nothing about the split */
        CHECK(elapsed > 500);
    }

    /*
     * 3c. The WORK clock starts before the first setup operation. The parent
     * spends 250 ms of setup against a 120 ms budget, so the row must be
     * refused however quick the child is. A clock started after the fork
     * would donate that setup to the row.
     */
    {
        tctx_t t;

        memset(&t, 0, sizeof(t));
        t.mode = CHILD_GOOD_DENIAL;
        t.before_ms = 250;
        run_timed(&t, 120, 2000, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(strstr(r.detail, "work deadline") != NULL);
        CHECK(r.reaped && !r.passed);
    }

    /*
     * 3d. The TEARDOWN grace runs from the child's own transition instant,
     * not from whenever the parent got round to reading it. The child
     * announces, then overruns an 80 ms grace by tearing down for 200 ms,
     * while the parent is not reading for 400 ms. Measured from receipt the
     * child would look punctual; measured from its own transition it is not.
     */
    {
        tctx_t t;

        memset(&t, 0, sizeof(t));
        t.mode = CHILD_CROSS_PHASE;
        t.work_ms = 0;
        t.teardown_ms = 200;
        t.after_ms = 400;
        run_timed(&t, 4000, 80, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(strstr(r.detail, "teardown grace") != NULL);
        CHECK(r.reaped && !r.passed);
    }

    /*
     * 3e. The transition instant must lie inside the window the parent can
     * vouch for. A child cannot buy grace by claiming it transitioned later
     * than the parent read the frame, nor by back-dating it before the row
     * began.
     */
    {
        tctx_t t;
        child_mode_t bad[] = { CHILD_FRAME_FUTURE, CHILD_FRAME_PAST };
        size_t i;

        for (i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            memset(&t, 0, sizeof(t));
            t.mode = bad[i];
            run_timed(&t, 4000, 2000, &r);
            CHECK(r.status == ORIGIN_CHILD_BAD_PHASE);
            CHECK(r.reaped && !r.passed);
        }
    }

    /*
     * 3f. The frame's integrity, exercised on the real child-to-parent wire.
     * Each of the four accepted fields is broken in turn; every one is a
     * refused sequence, not a transition the parent acts on.
     */
    {
        tctx_t t;
        child_mode_t bad[] = { CHILD_FRAME_BAD_SUM, CHILD_FRAME_BAD_MAGIC,
                               CHILD_FRAME_BAD_BYTES, CHILD_FRAME_WRONG_KIND };
        size_t i;

        for (i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            memset(&t, 0, sizeof(t));
            t.mode = bad[i];
            run_timed(&t, 4000, 2000, &r);
            CHECK(r.status == ORIGIN_CHILD_BAD_PHASE);
            CHECK(r.reaped && !r.passed);
        }
    }

    /*
     * 3g. A READY DESCRIPTOR AT THE EDGE. Three schedules, one rule each: the
     * deadline that authorized a read stays authoritative until that read has
     * been judged against it, EOF included, and leaving the loop is not
     * permission to publish. Each of these is refused by exactly one of those
     * rules, and none of them is a hostile child -- every one is the ordinary
     * case of a parent that lost the CPU at the wrong moment.
     */
    {
        tctx_t t;

        /*
         * (i) The transition itself arrives late. It was emitted in time, so
         * every rule about the frame's own contents accepts it; what must
         * refuse it is that the READ happened after the work deadline. If the
         * transition is allowed to install its grace first, the check that
         * follows inspects the deadline the late bytes just bought.
         */
        memset(&t, 0, sizeof(t));
        t.mode = CHILD_CROSS_PHASE;
        t.work_ms = 40;             /* transitions inside a 120 ms budget */
        t.teardown_ms = 0;
        t.wait_ms = 220;            /* but the parent reads it long after */
        run_timed(&t, 120, 4000, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        /* attribution: this is a WORK overrun, not a teardown grace it never
         * legitimately entered */
        CHECK(strstr(r.detail, "work deadline") != NULL);
        CHECK(r.reaped && !r.passed);

        /*
         * (ii) EOF arrives after the grace. EOF is the one read result that
         * ends the loop, so it is the one that can leave without being judged.
         *
         * The delay is selected by the READ'S OWN RESULT, never by how much
         * arrived before it: the pipe may split any write, so a nonzero total
         * says nothing about what the next read returns, and a delay chosen
         * that way can land on an ordinary data read whose surviving check
         * then answers on EOF's behalf.
         *
         * The multiple reads are made real by CAPPING the read, not by giving
         * the child a sleep to write around. A timed gap is not a boundary:
         * the parent need not be scheduled during it, and one read may then
         * take both frames and the report together. With a capped capacity the
         * boundary exists no matter when either process runs.
         *
         * The child announces teardown TWICE, so a run that skipped the check
         * reaches the structural refusal instead of a timeout -- which is what
         * makes the expected status decisive on its own terms.
         */
        memset(&t, 0, sizeof(t));
        t.mode = CHILD_DOUBLE_TRANSITION;
        t.read_cap = sizeof(origin_phase_t);   /* one frame per read at most */
        t.eof_ms = 800;             /* only on the read that RETURNS zero */
        run_timed(&t, 4000, 400, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(strstr(r.detail, "teardown grace") != NULL);
        CHECK(r.reaped && !r.passed);
        /*
         * Not vacuous: the run really did reach EOF, and really did take more
         * than one data read to get there, so the refusal came from the EOF
         * read and not from an earlier one that happened to be late.
         */
        CHECK(t.eof_reads == 1);
        CHECK(t.data_reads >= 2);

        /*
         * The same case with the parent descheduled for longer than any gap
         * the child could have left. Under a timing-based fragmentation
         * assumption this is the schedule that collapses every write into one
         * read; under a capped read it changes nothing.
         */
        memset(&t, 0, sizeof(t));
        t.mode = CHILD_DOUBLE_TRANSITION;
        t.read_cap = sizeof(origin_phase_t);
        t.after_ms = 200;           /* the parent is simply not there yet */
        t.eof_ms = 800;
        run_timed(&t, 4000, 400, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(strstr(r.detail, "teardown grace") != NULL);
        CHECK(t.eof_reads == 1);
        CHECK(t.data_reads >= 2);

        /*
         * (iii) Everything was read in time, and the grace then expires while
         * the parent is reaping and judging. Nothing in the loop can see this;
         * only the gate that stands immediately before the verdict can.
         */
        memset(&t, 0, sizeof(t));
        t.mode = CHILD_GOOD_DENIAL;
        t.score_ms = 300;
        run_timed(&t, 4000, 100, &r);
        CHECK(r.status == ORIGIN_CHILD_TIMEOUT);
        CHECK(strstr(r.detail, "teardown grace") != NULL);
        CHECK(!r.passed);
    }

    /* 4. the table ceiling preempts an ACTIVE row rather than waiting for it */
    {
        tctx_t t;
        origin_super_cfg_t cfg;
        origin_child_result_t last;
        size_t executed = 0, failed = 0;
        struct timespec t0, t1;
        long long elapsed;
        bool ok;

        memset(&t, 0, sizeof(t));
        t.mode = CHILD_BLOCKS_IN_WORK;
        memset(&cfg, 0, sizeof(cfg));
        cfg.child = child_body;
        cfg.ctx = &t;
        cfg.row_budget_ms = 30000;      /* far beyond the ceiling */
        cfg.teardown_grace_ms = 30000;
        cfg.table_ceiling_ms = 300;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        ok = origin_super_run_table(&cfg, &executed, &failed, &last);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        elapsed = (t1.tv_sec - t0.tv_sec) * 1000 +
                  (t1.tv_nsec - t0.tv_nsec) / 1000000;
        CHECK(!ok);
        CHECK(last.status == ORIGIN_CHILD_TABLE_CEILING);
        CHECK(last.reaped);
        /* bounded by the ceiling, not by the row's own 30s budget */
        checks++;
        if (elapsed > 5000) {
            printf("FAIL: the table ran %lldms past a 300ms ceiling\n",
                   elapsed);
            failures++;
        }
    }

    /* -- the phase sequence itself ----------------------------------------- */
    run_one(CHILD_NO_TRANSITION, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_PHASE);
    CHECK(r.reaped && !r.passed);

    run_one(CHILD_DOUBLE_TRANSITION, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_PHASE);
    CHECK(r.reaped && !r.passed);

    /* -- a verdict field outside {0,1} is not a boolean --------------------- */
    run_one(CHILD_PASSED_DOMAIN, 0, 5000, &r);
    CHECK(r.status == ORIGIN_CHILD_BAD_REPORT);
    CHECK(!r.passed && r.reaped);

    /* -- a complete all-pass ten-row table --------------------------------- */
    {
        tctx_t t;
        origin_super_cfg_t cfg;
        origin_child_result_t last;
        size_t executed = 0, failed = 0, n = 0;
        bool ok;

        (void)origin_rows(&n);
        memset(&t, 0, sizeof(t));
        t.mode = CHILD_ROW_CORRECT;     /* each row reports what it expects */
        memset(&cfg, 0, sizeof(cfg));
        cfg.child = child_body;
        cfg.ctx = &t;
        cfg.row_budget_ms = 5000;
        cfg.teardown_grace_ms = 2000;
        ok = origin_super_run_table(&cfg, &executed, &failed, &last);
        CHECK(ok);
        CHECK(executed == n && failed == n);
    }

    /* -- the table stops at the first failing row -------------------------- */
    {
        tctx_t t;
        origin_super_cfg_t cfg;
        size_t executed = 0, failed = 0;
        origin_child_result_t last;
        bool ok;

        memset(&t, 0, sizeof(t));
        t.mode = CHILD_GOOD_DENIAL;    /* every row reports a 403 denial */
        memset(&cfg, 0, sizeof(cfg));
        cfg.child = child_body;
        cfg.ctx = &t;
        cfg.row_budget_ms = 5000;
        ok = origin_super_run_table(&cfg, &executed, &failed, &last);
        /* row 0 expects an establishment, so a 403 report fails it first */
        CHECK(!ok);
        CHECK(failed == 0 && executed == 1);
    }

    if (failures != 0) {
        printf("FAILED: %d of %d supervisor checks\n", failures, checks);
        return 1;
    }
    printf("PASS: %d supervisor checks\n", checks);
    return 0;
}
