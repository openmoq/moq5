#include "origin_driver.h"

#include <stdio.h>
#include <string.h>

static bool
say(char *out, size_t cap, const char *why)
{
    if (out != NULL && cap > 0) {
        snprintf(out, cap, "%s", why);
    }
    return false;
}

/* Every callee this driver will call. A missing one is a named fixture
 * refusal, not a crash on a null pointer part way through a row. */
static const char *
missing_callee(const origin_callees_t *c)
{
    if (c->server_create == NULL)     return "server_create";
    if (c->server_port == NULL)       return "server_port";
    if (c->server_stop_begin == NULL) return "server_stop_begin";
    if (c->server_join == NULL)       return "server_join";
    if (c->server_destroy == NULL)    return "server_destroy";
    if (c->env_open == NULL)          return "env_open";
    if (c->connect == NULL)           return "connect";
    if (c->wait_terminal == NULL)     return "wait_terminal";
    if (c->env_close == NULL)         return "env_close";
    if (c->session_release == NULL)   return "session_release";
    if (c->now_ms == NULL)            return "now_ms";
    return NULL;
}

bool
origin_driver_run_row(const origin_callees_t *c, const origin_row_t *row,
                      origin_obs_t *obs, bool *unsafe_out,
                      char *reason, size_t reason_cap)
{
    void *server = NULL;
    void *env = NULL;
    void *session = NULL;
    unsigned short port = 0;
    long long row_started, teardown_started;
    bool overran = false;
    origin_snapshot_t snap;
    const char *missing;

    if (unsafe_out != NULL) {
        *unsafe_out = false;
    }
    if (c == NULL || row == NULL || obs == NULL) {
        return say(reason, reason_cap, "no callees, row or observation record");
    }
    if (reason != NULL && reason_cap > 0) {
        reason[0] = '\0';
    }
    missing = missing_callee(c);
    if (missing != NULL) {
        char buf[96];
        snprintf(buf, sizeof(buf), "the callee table has no %s", missing);
        origin_obs_setup_failed(obs, buf);
        origin_obs_snapshot(obs, &snap);
        return origin_row_passed(row, &snap, reason, reason_cap);
    }

    /*
     * ONE monotonic row deadline, started before any construction. It is
     * checked after EVERY synchronous return -- not only before connect -- and
     * once more immediately before a verdict is taken, because a call that
     * returns late has still overrun.
     */
    row_started = c->now_ms(c->ctx);
#define ROW_OVERRAN() \
    (c->now_ms(c->ctx) - row_started >= (long long)ORIGIN_ROW_BUDGET_MS)

    if (c->server_create(c->ctx, row, &server) != 0 || server == NULL) {
        origin_obs_setup_failed(obs, "the server could not be created");
        goto unwind;
    }
    origin_obs_constructed(obs);
    if (ROW_OVERRAN()) { overran = true; goto unwind; }

    port = c->server_port(c->ctx, server);
    if (port == 0) {
        origin_obs_setup_failed(obs, "the server reported no bound port");
        goto unwind;
    }
    if (ROW_OVERRAN()) { overran = true; goto unwind; }

    if (c->env_open(c->ctx, &env) != 0 || env == NULL) {
        origin_obs_setup_failed(obs, "the client environment could not open");
        goto unwind;
    }
    origin_obs_constructed(obs);
    if (ROW_OVERRAN()) { overran = true; goto unwind; }

    /*
     * Callbacks for this session may already have run by the time connect
     * returns; they record through `obs` and never through `session`, which is
     * why the handle is captured only here and only once.
     */
    if (c->connect(c->ctx, env, row, port, obs, &session) == 0) {
        if (session != NULL) {
            origin_obs_ref_acquired(obs);
        }
        /* a connect that RETURNED late has still overrun the row */
        if (ROW_OVERRAN()) { overran = true; goto unwind; }
        {
            long long spent = c->now_ms(c->ctx) - row_started;
            int left = (int)((long long)ORIGIN_ROW_BUDGET_MS - spent);
            if (left < 0) { left = 0; }
            if (c->wait_terminal(c->ctx, obs, left) != 0) {
                overran = true;
                goto unwind;
            }
        }
        /* and a wait that RETURNED late has too */
        if (ROW_OVERRAN()) { overran = true; goto unwind; }
    } else {
        origin_obs_setup_failed(obs, "the connect call failed synchronously");
    }

unwind:
    /* The phase transition: work has ended and teardown is beginning. The
     * parent's teardown clock starts on this, not on a guess. */
    if (c->on_enter_teardown != NULL) {
        c->on_enter_teardown(c->ctx);
    }
    /*
     * Teardown gets its OWN monotonic grace, started here. It never inherits
     * unused row budget: a row that set up quickly does not thereby buy extra
     * time to shut down.
     */
    teardown_started = c->now_ms(c->ctx);
#define TEARDOWN_OVERRAN() \
    (c->now_ms(c->ctx) - teardown_started >= (long long)ORIGIN_TEARDOWN_MS)
    {
        bool late = false;

        if (server != NULL) {
            c->server_stop_begin(c->ctx, server);
            if (TEARDOWN_OVERRAN()) { late = true; }
            if (c->server_join(c->ctx, server) != 0) {
                /*
                 * The server's completion barrier did not hold. Destroying it
                 * is not permitted before join succeeds, and a live pump may
                 * still borrow the observation record -- so nothing more is
                 * torn down and nothing may be snapshotted, freed or reported.
                 * The row is isolated in its own child, so process teardown
                 * reclaims what is left.
                 */
                origin_obs_setup_failed(obs,
                                        "the server completion barrier failed; "
                                        "teardown stopped without destroy");
                if (unsafe_out != NULL) {
                    *unsafe_out = true;
                }
                if (reason != NULL && reason_cap > 0) {
                    snprintf(reason, reason_cap,
                             "the server completion barrier failed; teardown "
                             "stopped without destroy");
                }
                return false;
            }
            if (TEARDOWN_OVERRAN()) { late = true; }
            c->server_destroy(c->ctx, server);
            if (TEARDOWN_OVERRAN()) { late = true; }
            origin_obs_finalized(obs);
        }
        if (env != NULL) {
            /* the completion barrier: after this the backend will never touch
             * a session again, which is what makes the release below legal */
            c->env_close(c->ctx, env);
            if (TEARDOWN_OVERRAN()) { late = true; }
            origin_obs_barrier_done(obs);
            origin_obs_finalized(obs);
        }
        if (session != NULL) {
            c->session_release(c->ctx, session);
            if (TEARDOWN_OVERRAN()) { late = true; }
            origin_obs_ref_released(obs);
        }
        if (overran || late) {
            origin_obs_timed_out(obs);
        } else {
            origin_obs_teardown_ok(obs);
        }
    }
    /* one last check before any verdict is taken */
    if (!overran && ROW_OVERRAN()) {
        origin_obs_timed_out(obs);
    }
#undef ROW_OVERRAN
#undef TEARDOWN_OVERRAN

    origin_obs_snapshot(obs, &snap);
    return origin_row_passed(row, &snap, reason, reason_cap);
}

bool
origin_driver_run_table(const origin_callees_t *c, size_t *executed,
                        size_t *failed_index, char *reason, size_t reason_cap)
{
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);
    size_t i;

    if (executed != NULL) {
        *executed = 0;
    }
    if (failed_index != NULL) {
        *failed_index = n;
    }
    for (i = 0; i < n; i++) {
        origin_obs_t *obs = origin_obs_new();
        bool ok;

        if (obs == NULL) {
            if (failed_index != NULL) {
                *failed_index = i;
            }
            return say(reason, reason_cap,
                       "the observation record could not be allocated");
        }
        if (executed != NULL) {
            (*executed)++;
        }
        {
            bool unsafe = false;
            ok = origin_driver_run_row(c, &rows[i], obs, &unsafe, reason,
                                       reason_cap);
            if (unsafe) {
                /* callback-owned state may still be live: do not free it */
                if (failed_index != NULL) {
                    *failed_index = i;
                }
                return false;
            }
        }
        origin_obs_free(obs);
        if (!ok) {
            /* First failure stops the table: a later row cannot explain an
             * earlier one, and continuing would report a count that hides
             * which row actually decided the result. */
            if (failed_index != NULL) {
                *failed_index = i;
            }
            return false;
        }
    }
    return true;
}
