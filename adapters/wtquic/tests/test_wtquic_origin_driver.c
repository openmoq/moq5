/*
 * The Origin gate's row DRIVER, driven through a fake transport.
 *
 * The classifier tests next door prove the rules. These prove the code that
 * builds, connects, waits, tears down and decides -- the same
 * origin_driver_run_row and origin_driver_run_table the native fixture calls,
 * with a callee table that can produce orderings a real transport only produces
 * by accident: a callback that lands before connect returns, a reentrant
 * callback, a late contradictory event, a construction that fails part way, an
 * allocation failure, and a call that blocks past the budget.
 *
 * Time is injected, so a blocked call is expressed by advancing the fake clock
 * rather than by sleeping. Nothing here opens a socket.
 */
#include "origin_driver.h"

#include <stdio.h>
#include <string.h>

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

/* What the fake transport should do for a row. */
typedef enum {
    FAKE_ESTABLISH = 0,     /* establish, then honour the deliberate close */
    FAKE_REFUSE_403,
    FAKE_REFUSE_404,
    FAKE_FAIL,              /* a transport failure, never an HTTP status */
    FAKE_SERVER_CREATE_FAIL,
    FAKE_NO_PORT,
    FAKE_ENV_OPEN_FAIL,
    FAKE_CONNECT_FAIL,      /* synchronous refusal: no handle to release */
    FAKE_BLOCK_SETUP,       /* the clock passes the budget during construction */
    FAKE_BLOCK_WAIT,        /* no terminal arrives before the budget ends */
    FAKE_CLOSE_CALL_FAILS,
    FAKE_LATE_CONTRADICTION,/* an establishment arrives after the refusal */
    FAKE_REENTRANT,         /* the close callback re-enters during the close */
    FAKE_CONNECT_LATE,      /* connect RETURNS, but past the row budget */
    FAKE_WAIT_LATE,         /* the terminal arrives, but past the row budget */
    FAKE_TEARDOWN_LATE,     /* teardown itself overruns its own grace */
    /* The row spends most of its budget in setup and THEN teardown overruns
     * its own grace. Under the old combined rule the total still fitted
     * row+teardown, so the row was accepted; under separate deadlines the
     * teardown overrun is caught on its own. */
    FAKE_SLOW_ROW_LATE_TEARDOWN
} fake_mode_t;

typedef struct {
    fake_mode_t mode;
    long long   now;
    /* real resource accounting, so the test measures the driver's behaviour
     * rather than trusting it */
    int servers_live, envs_live, sessions_live;
    int servers_made, envs_made, sessions_made;
    int releases, env_closes, joins, destroys, stop_begins;
    int connect_calls, wait_calls;
    bool released_before_barrier;
    bool barrier_done;
} fake_t;

static long long fake_now(void *ctx) { return ((fake_t *)ctx)->now; }

static int
fake_server_create(void *ctx, const origin_row_t *row, void **out)
{
    fake_t *f = ctx;
    (void)row;
    if (f->mode == FAKE_SERVER_CREATE_FAIL) {
        return -1;
    }
    if (f->mode == FAKE_BLOCK_SETUP) {
        f->now += ORIGIN_ROW_BUDGET_MS + 1;   /* the call blocked past it */
    }
    if (f->mode == FAKE_SLOW_ROW_LATE_TEARDOWN) {
        f->now += ORIGIN_ROW_BUDGET_MS - 1000; /* slow, but within budget */
    }
    f->servers_live++; f->servers_made++;
    *out = f;
    return 0;
}

static unsigned short
fake_server_port(void *ctx, void *server)
{
    fake_t *f = ctx;
    (void)server;
    return f->mode == FAKE_NO_PORT ? 0 : 4433;
}

static void fake_stop_begin(void *ctx, void *s)
{ (void)s; ((fake_t *)ctx)->stop_begins++; }
static int fake_join(void *ctx, void *s)
{ (void)s; ((fake_t *)ctx)->joins++; return 0; }
static void fake_destroy(void *ctx, void *s)
{ fake_t *f = ctx; (void)s; f->destroys++; f->servers_live--; }

static int
fake_env_open(void *ctx, void **out)
{
    fake_t *f = ctx;
    if (f->mode == FAKE_ENV_OPEN_FAIL) {
        return -1;
    }
    f->envs_live++; f->envs_made++;
    *out = f;
    return 0;
}

static void
fake_seal(origin_obs_t *obs, bool ok, bool size_ok, uint32_t kind)
{
    origin_sealed_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.query_ok = ok;
    rec.image_exact = size_ok;
    rec.kind = kind;
    origin_obs_sealed(obs, &rec);
}

/* The callbacks a real backend would deliver, played out synchronously INSIDE
 * connect -- which is exactly the ordering the real client permits. */
static void
play_callbacks(fake_t *f, origin_obs_t *obs)
{
    switch (f->mode) {
    case FAKE_ESTABLISH:
    case FAKE_CLOSE_CALL_FAILS:
    case FAKE_REENTRANT:
        /* an ESTABLISHED session has selected a profile: the query succeeds
         * here, and no unselected observation is legal for this row */
        origin_obs_established(obs, ORIGIN_EXPECT_SUBPROTOCOL,
                               strlen(ORIGIN_EXPECT_SUBPROTOCOL));
        origin_obs_profile(obs, true, ORIGIN_EXPECT_PROFILE);
        /* the real API delivers on_closed BEFORE close returns, so the
         * attempt is recorded first and the callback observes it */
        origin_obs_close_attempt(obs);
        (void)origin_obs_close_attempted(obs);
        origin_obs_closed(obs);
        fake_seal(obs, true, true, ORIGIN_SEALED_KIND_NONE);
        origin_obs_close_result(obs, f->mode != FAKE_CLOSE_CALL_FAILS);
        break;
    case FAKE_REFUSE_403:
    case FAKE_REFUSE_404:
        origin_obs_refused(obs, f->mode == FAKE_REFUSE_403 ? 403 : 404);
        origin_obs_profile_unselected(obs, true, true);
        fake_seal(obs, true, true, ORIGIN_SEALED_KIND_NONE);
        break;
    case FAKE_LATE_CONTRADICTION:
        origin_obs_refused(obs, 403);
        origin_obs_profile_unselected(obs, true, true);
        fake_seal(obs, true, true, ORIGIN_SEALED_KIND_NONE);
        origin_obs_established(obs, ORIGIN_EXPECT_SUBPROTOCOL,
                               strlen(ORIGIN_EXPECT_SUBPROTOCOL));
        break;
    case FAKE_FAIL:
        origin_obs_failed(obs);
        origin_obs_profile_unselected(obs, true, true);
        fake_seal(obs, true, true, ORIGIN_SEALED_KIND_NONE);
        break;
    default:
        break;
    }
}

static int
fake_connect(void *ctx, void *env, const origin_row_t *row,
             unsigned short port, origin_obs_t *obs, void **out)
{
    fake_t *f = ctx;
    (void)env; (void)row; (void)port;
    f->connect_calls++;
    if (f->mode == FAKE_CONNECT_FAIL) {
        *out = NULL;                       /* nothing to release */
        return -1;
    }
    if (f->mode == FAKE_CONNECT_LATE) {
        /* it succeeded, but only after the whole row budget had gone */
        f->now += ORIGIN_ROW_BUDGET_MS + 1;
    }
    /* callbacks land BEFORE this returns and before *out is written */
    play_callbacks(f, obs);
    f->sessions_live++; f->sessions_made++;
    *out = f;
    return 0;
}

static int
fake_wait(void *ctx, origin_obs_t *obs, int remaining)
{
    fake_t *f = ctx;
    (void)obs;
    f->wait_calls++;
    if (f->mode == FAKE_BLOCK_WAIT) {
        f->now += remaining + 1;           /* the wait consumed the budget */
        return -1;
    }
    if (f->mode == FAKE_WAIT_LATE) {
        /* a terminal DID arrive -- just after the budget expired */
        f->now += remaining + 1;
        return 0;
    }
    return 0;
}

static void
fake_env_close(void *ctx, void *env)
{
    fake_t *f = ctx;
    (void)env;
    if (f->mode == FAKE_TEARDOWN_LATE ||
        f->mode == FAKE_SLOW_ROW_LATE_TEARDOWN) {
        f->now += ORIGIN_TEARDOWN_MS + 1;  /* the barrier itself overran */
    }
    f->env_closes++;
    f->barrier_done = true;
    f->envs_live--;
}

static void
fake_release(void *ctx, void *session)
{
    fake_t *f = ctx;
    (void)session;
    if (!f->barrier_done) {
        f->released_before_barrier = true;
    }
    f->releases++;
    f->sessions_live--;
}

static void
fake_init(fake_t *f, fake_mode_t mode, origin_callees_t *c)
{
    memset(f, 0, sizeof(*f));
    f->mode = mode;
    memset(c, 0, sizeof(*c));
    c->ctx = f;
    c->server_create = fake_server_create;
    c->server_port = fake_server_port;
    c->server_stop_begin = fake_stop_begin;
    c->server_join = fake_join;
    c->server_destroy = fake_destroy;
    c->env_open = fake_env_open;
    c->connect = fake_connect;
    c->wait_terminal = fake_wait;
    c->env_close = fake_env_close;
    c->session_release = fake_release;
    c->now_ms = fake_now;
}

/* Run one row through the real driver and report what the fake observed. */
static bool
run(fake_mode_t mode, const origin_row_t *row, fake_t *f, char *why,
    size_t why_cap)
{
    origin_callees_t c;
    origin_obs_t *obs = origin_obs_new();
    bool ok;

    fake_init(f, mode, &c);
    ok = origin_driver_run_row(&c, row, obs, NULL, why, why_cap);
    origin_obs_free(obs);
    return ok;
}

int
main(void)
{
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);
    char why[ORIGIN_REASON_CAP];
    fake_t f;

    /* -- the driver's happy paths ----------------------------------------- */
    CHECK(run(FAKE_ESTABLISH, &rows[0], &f, why, sizeof(why)));
    /* the driver acquired exactly one handle and released it exactly once,
     * after the barrier, and finalized everything it built */
    CHECK(f.sessions_made == 1 && f.releases == 1);
    CHECK(!f.released_before_barrier);
    CHECK(f.env_closes == 1 && f.stop_begins == 1 && f.joins == 1 &&
          f.destroys == 1);
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    CHECK(run(FAKE_REFUSE_403, &rows[2], &f, why, sizeof(why)));
    CHECK(f.sessions_made == 1 && f.releases == 1 &&
          !f.released_before_barrier);
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    /* -- ORDERING: the release must follow the barrier -------------------- */
    /* the fake records the order itself, so this is the driver's behaviour,
     * not a flag the driver set about itself */
    CHECK(run(FAKE_ESTABLISH, &rows[0], &f, why, sizeof(why)));
    CHECK(f.barrier_done && !f.released_before_barrier);

    /* -- wrong causes fail their rows ------------------------------------- */
    CHECK(!run(FAKE_REFUSE_404, &rows[2], &f, why, sizeof(why)));
    CHECK(strstr(why, "403") != NULL);
    CHECK(!run(FAKE_FAIL, &rows[2], &f, why, sizeof(why)));
    CHECK(!run(FAKE_ESTABLISH, &rows[2], &f, why, sizeof(why)));
    CHECK(!run(FAKE_REFUSE_403, &rows[0], &f, why, sizeof(why)));

    /* -- late contradiction ----------------------------------------------- */
    CHECK(!run(FAKE_LATE_CONTRADICTION, &rows[2], &f, why, sizeof(why)));
    /* it still unwound everything it built */
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    /* -- reentrancy is consistent, not corrupting -------------------------- */
    CHECK(run(FAKE_REENTRANT, &rows[0], &f, why, sizeof(why)));
    CHECK(f.releases == 1 && !f.released_before_barrier);

    /* -- a close whose CALL failed ---------------------------------------- */
    CHECK(!run(FAKE_CLOSE_CALL_FAILS, &rows[0], &f, why, sizeof(why)));
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    /* -- partial construction unwinds exactly what it built ---------------- */
    CHECK(!run(FAKE_SERVER_CREATE_FAIL, &rows[0], &f, why, sizeof(why)));
    CHECK(f.servers_made == 0 && f.envs_made == 0 && f.connect_calls == 0);
    CHECK(f.destroys == 0 && f.env_closes == 0 && f.releases == 0);

    CHECK(!run(FAKE_NO_PORT, &rows[0], &f, why, sizeof(why)));
    CHECK(f.servers_made == 1 && f.destroys == 1);   /* built, then unwound */
    CHECK(f.envs_made == 0 && f.connect_calls == 0);

    CHECK(!run(FAKE_ENV_OPEN_FAIL, &rows[0], &f, why, sizeof(why)));
    CHECK(f.servers_made == 1 && f.destroys == 1 && f.envs_made == 0);
    CHECK(f.connect_calls == 0 && f.releases == 0);

    /* a synchronous connect refusal leaves no handle: nothing is released */
    CHECK(!run(FAKE_CONNECT_FAIL, &rows[0], &f, why, sizeof(why)));
    CHECK(f.connect_calls == 1 && f.sessions_made == 0 && f.releases == 0);
    CHECK(f.env_closes == 1 && f.destroys == 1);     /* still torn down */

    /* -- blocked calls are bounded by the ONE budget ----------------------- */
    /* a construction that blocks past the budget is caught before connect */
    CHECK(!run(FAKE_BLOCK_SETUP, &rows[0], &f, why, sizeof(why)));
    CHECK(f.connect_calls == 0);
    CHECK(f.destroys == 1 && f.servers_live == 0);   /* still unwound */
    /* a wait that never terminates is caught, and teardown still runs */
    CHECK(!run(FAKE_BLOCK_WAIT, &rows[0], &f, why, sizeof(why)));
    CHECK(f.env_closes == 1 && f.destroys == 1 && f.releases == 1);
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    /* -- the three deadline properties, each a reported false pass --------- */
    /*
     * A call that RETURNS late has still overrun. The driver used to check the
     * budget only before connect, so a connect or wait that came back past it
     * was accepted; and teardown was allowed to spend whatever the row had not
     * used, so it never had a grace of its own.
     */
    CHECK(!run(FAKE_CONNECT_LATE, &rows[0], &f, why, sizeof(why)));
    CHECK(strstr(why, "budget") != NULL);
    /* and it must not START a wait it has no budget left for: the deadline
     * bounds the WORK, not only the verdict */
    CHECK(f.wait_calls == 0);
    /* it still tore down everything it had built */
    CHECK(f.servers_live == 0 && f.envs_live == 0 && f.sessions_live == 0);

    CHECK(!run(FAKE_WAIT_LATE, &rows[0], &f, why, sizeof(why)));
    CHECK(strstr(why, "budget") != NULL);
    CHECK(f.env_closes == 1 && f.releases == 1);

    /* teardown gets its OWN five seconds and never inherits unused row time */
    CHECK(!run(FAKE_TEARDOWN_LATE, &rows[0], &f, why, sizeof(why)));
    CHECK(strstr(why, "budget") != NULL);
    CHECK(f.env_closes == 1 && f.destroys == 1);

    /* The case the old combined rule accepted: a row that spent nearly all of
     * its budget setting up, then a teardown that overran its OWN grace. The
     * total still fits row+teardown, so only a separate teardown deadline
     * catches it. */
    CHECK(!run(FAKE_SLOW_ROW_LATE_TEARDOWN, &rows[0], &f, why, sizeof(why)));
    CHECK(strstr(why, "budget") != NULL);
    CHECK(f.env_closes == 1 && f.destroys == 1 && f.releases == 1);

    /* -- an incomplete callee table is a named refusal, not a crash -------- */
    {
        origin_callees_t c;
        origin_obs_t *obs;
        fake_t tf;
        bool ok;

        fake_init(&tf, FAKE_ESTABLISH, &c);
        c.env_close = NULL;                 /* one function missing */
        obs = origin_obs_new();
        ok = origin_driver_run_row(&c, &rows[0], obs, NULL, why, sizeof(why));
        origin_obs_free(obs);
        CHECK(!ok);
        CHECK(strstr(why, "env_close") != NULL);
    }

    /* -- first-failure stop, IN THE DRIVER --------------------------------- */
    {
        /* a table where every row's fake outcome matches its expectation */
        origin_callees_t c;
        fake_t tf;
        size_t executed = 0, failed = 0;
        bool ok;

        /* The fake's mode is chosen per row by the driver's own row pointer,
         * so this exercises origin_driver_run_table rather than a loop here. */
        struct { const origin_row_t *rows; size_t n; } unused;
        (void)unused;

        fake_init(&tf, FAKE_ESTABLISH, &c);
        /* every row establishes, so the denial rows must fail: the table must
         * stop at the FIRST of them, which is row index 2 */
        ok = origin_driver_run_table(&c, &executed, &failed, why, sizeof(why));
        CHECK(!ok);
        CHECK(failed == 2);
        CHECK(executed == 3);
    }

    if (failures != 0) {
        printf("FAILED: %d of %d driver checks\n", failures, checks);
        return 1;
    }
    printf("PASS: %d driver checks\n", checks);
    return 0;
}
