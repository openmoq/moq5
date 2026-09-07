/*
 * The ACTUAL native callback adapters and the ACTUAL owning pump, exercised
 * with only the transport operations substituted.
 *
 * The production translation unit is included with its entry point renamed
 * away, so `ev_established`, `ev_refused`, `ev_closed`, `server_pump` and the
 * row bind/unbind protocol under test are the exact objects the native run
 * would use. Nothing here opens a socket, loads a provider, reads a
 * certificate or talks to a network: every WT and LibMoQ operation the fixture
 * calls is defined below.
 *
 * This exists because an offline fake of the CALLBACKS can agree with itself
 * while disagreeing with the API -- which is exactly how a positive row that
 * could never pass natively went unnoticed.
 */
#define main moqr_origin_gate_disabled_main
#include "test_wtquic_origin_gate.c"
#undef main

#include <stdio.h>

/* the user pointer the substituted close hands back to on_closed */
static void *k_row_user;

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

/* -- substituted transport operations -------------------------------------- */

static bool     g_established;      /* the session reached establishment */
static bool     g_close_ok = true;  /* what wtq_session_close returns */
static int      g_close_calls;
static bool     g_seal_ok = true;
static uint32_t g_seal_kind;
static uint64_t g_seal_quic_code;
static uint32_t g_seal_domain;
static uint32_t g_seal_reserved1;
static uint32_t g_seal_reserved0;
static long long g_seal_native_code;
/* how many trailing groups a producer writes; 4 = the whole object */
static int      g_seal_written = 4;
/* whether the producer restores the caller's struct_size, as the API does */
static bool     g_seal_restore_size = true;
static int      g_release_calls;

wtq_result_t
wtq_session_webtransport_profile(const wtq_session_t *session,
                                 wtq_webtransport_profile_t *profile_out)
{
    (void)session;
    if (!g_established) {
        /* the documented unselected behaviour: report STATE and leave the
         * caller's output word exactly as it was */
        return WTQ_ERR_STATE;
    }
    *profile_out = (wtq_webtransport_profile_t)ORIGIN_EXPECT_PROFILE;
    return WTQ_OK;
}

wtq_result_t
wtq_session_transport_error(const wtq_session_t *session,
                            wtq_transport_error_t *out)
{
    (void)session;
    uint32_t caller_size = out->struct_size;

    if (!g_seal_ok) {
        return WTQ_ERR_STATE;
    }
    /*
     * Model the real contract: copy up to the CALLER's size and restore the
     * caller's struct_size. `g_seal_written` lets a case behave like an older
     * producer that stops before the trailing fields, so the caller's poison
     * survives there and the fixture must notice.
     */
    if (g_seal_written >= 1) { out->kind = (uint16_t)g_seal_kind;
                               out->reserved0 = g_seal_reserved0; }
    if (g_seal_written >= 2) { out->quic_code = g_seal_quic_code; }
    if (g_seal_written >= 3) { out->native_domain = g_seal_domain;
                               out->reserved1 = g_seal_reserved1; }
    if (g_seal_written >= 4) { out->native_code = g_seal_native_code; }
    if (g_seal_restore_size) {
        out->struct_size = caller_size;
    } else {
        /* a producer that reports a FILLED size instead of restoring the
         * caller's -- not what the API promises */
        out->struct_size = (uint32_t)(sizeof(*out) - 8u);
    }
    return WTQ_OK;
}

wtq_result_t
wtq_session_close(wtq_session_t *session, uint32_t code, const uint8_t *reason,
                  size_t reason_len)
{
    (void)code; (void)reason; (void)reason_len;
    g_close_calls++;
    /*
     * The real API delivers on_closed BEFORE this returns. Modelling that is
     * the point: a fixture that records the close attempt only afterwards
     * leaves the reentrant callback unable to see it.
     *
     * The user pointer is whichever row context this session belongs to --
     * `k_row_user` for the direct callback cases, and the session handle
     * itself in the composed run, where connect hands back the row context.
     */
    ev_closed(session, 0, NULL, 0, true,
              k_row_user != NULL ? k_row_user : (void *)session);
    return g_close_ok ? WTQ_OK : WTQ_ERR_CLOSED;
}

/*
 * After a failed completion barrier these three calls are ILLEGAL: the server
 * was never proven quiescent, so destroying it, closing the environment or
 * releasing the session could each race a live pump that still borrows the
 * observation record. The child under test cannot reach its own ledger on
 * that path -- it exits from inside the driver -- so the prohibition is
 * enforced where it would be violated.
 */
static int g_forbid_teardown;

static void
forbidden_after_failed_barrier(const char *what)
{
    if (g_forbid_teardown) {
        fprintf(stderr, "%s was called after the completion barrier failed\n",
                what);
        _exit(4);
    }
}void wtq_session_release(wtq_session_t *s)
{ (void)s; forbidden_after_failed_barrier("session_release"); g_release_calls++; }
const char *wtq_version(void) { return "0.0.0-substituted"; }

/*
 * The transport operations the COMPOSITION test needs. They succeed, so the
 * real chain -- supervisor, child, driver, callees, callbacks, teardown,
 * report -- actually runs. What they do NOT do is decide policy: the outcome
 * for a row is taken from the frozen row table, because this test is about the
 * composition, not about the server's Origin decision (which only native
 * traffic can prove).
 */
static int g_env_opens, g_env_closes, g_server_creates, g_server_destroys;
static int g_joins, g_stop_begins, g_connects;
static bool g_join_fails;
static bool g_env_open_fails;
static const origin_row_t *g_active_row;
static const char *g_expected_bind_address;
static bool g_bind_address_matches;



wtq_result_t wtq_msquic_env_open(const wtq_msquic_env_cfg_t *c,
                                 wtq_msquic_env_t **o)
{
    (void)c;
    /* the ATTEMPT is what the ledger counts; a substitute that counted only
     * successes would let a skipped environment look like a refused one */
    g_env_opens++;
    if (g_env_open_fails) {
        return WTQ_ERR_STATE;
    }
    *o = (wtq_msquic_env_t *)&g_env_opens;
    return WTQ_OK;
}
void wtq_msquic_env_close(wtq_msquic_env_t *e)
{ (void)e; forbidden_after_failed_barrier("env_close"); g_env_closes++; }
void wtq_msquic_env_cfg_init(wtq_msquic_env_cfg_t *c)
{ if (c) { memset(c, 0, sizeof(*c)); c->struct_size = (uint32_t)sizeof(*c); } }
void wtq_msquic_client_cfg_init_ex(wtq_msquic_client_cfg_t *c, size_t n)
{ if (c) { memset(c, 0, n); c->struct_size = (uint32_t)n; } }
void wtq_connect_config_init_ex(wtq_connect_config_t *c, size_t n)
{ if (c) { memset(c, 0, n); c->struct_size = (uint32_t)n; } }
wtq_result_t wtq_msquic_client_connect_with_trust(
    wtq_msquic_env_t *e, const wtq_msquic_client_cfg_t *c,
    const wtq_msquic_client_trust_cfg_t *t, wtq_session_t **o)
{
    row_ctx_t *rc = c != NULL ? c->user : NULL;
    (void)e; (void)t;

    g_connects++;

    /*
     * Callbacks land BEFORE this returns and before *o is written -- the
     * ordering the real client permits and the reason the fixture must never
     * record through an output handle.
     */
    if (rc != NULL && g_active_row != NULL) {
        wtq_str_t sub;
        if (g_active_row->expect == ROW_EXPECT_ESTABLISH) {
            g_established = true;
            sub.data = ORIGIN_EXPECT_SUBPROTOCOL;
            sub.len = strlen(ORIGIN_EXPECT_SUBPROTOCOL);
            ev_established((wtq_session_t *)rc, sub, rc);
        } else {
            g_established = false;
            ev_refused((wtq_session_t *)rc, ORIGIN_REFUSAL_STATUS, rc);
        }
    }
    *o = (wtq_session_t *)rc;
    return WTQ_OK;
}

/* -- substituted LibMoQ facade + session operations ------------------------ */

static moq_wtquic_msquic_managed_conn_t *g_conns[2];
static size_t   g_conn_count;
static moq_session_t *g_conn_session[2];
static int      g_events_left;          /* events the fake session will yield */
static bool     g_emit_terminal;
static bool     g_poll_fails;
static int      g_cleanups;
static int      g_acks;
static moq_result_t g_ack_result = MOQ_OK;

moq_wtquic_msquic_managed_conn_t *
moq_wtquic_msquic_lane_next_conn(moq_wtquic_msquic_managed_lane_t *lane,
                                 moq_wtquic_msquic_managed_conn_t *prev)
{
    size_t i;
    (void)lane;
    if (prev == NULL) {
        return g_conn_count > 0 ? g_conns[0] : NULL;
    }
    for (i = 0; i + 1 < g_conn_count; i++) {
        if (g_conns[i] == prev) {
            return g_conns[i + 1];
        }
    }
    return NULL;
}

moq_session_t *
moq_wtquic_msquic_managed_conn_session(moq_wtquic_msquic_managed_conn_t *c)
{
    size_t i;
    for (i = 0; i < g_conn_count; i++) {
        if (g_conns[i] == c) {
            return g_conn_session[i];
        }
    }
    return NULL;
}

moq_result_t
moq_wtquic_msquic_managed_conn_ack_terminal(
    moq_wtquic_msquic_managed_conn_t *c)
{
    (void)c;
    g_acks++;
    return g_ack_result;
}

moq_result_t
moq_session_poll_events_ex(moq_session_t *s, void *out, size_t cap,
                           size_t element_size, size_t *out_count)
{
    moq_event_t *ev = out;
    (void)s; (void)cap; (void)element_size;
    if (g_poll_fails) {
        return MOQ_ERR_WRONG_STATE;
    }
    if (g_events_left <= 0) {
        *out_count = 0;
        return MOQ_OK;
    }
    memset(ev, 0, sizeof(*ev));
    g_events_left--;
    ev->kind = (g_events_left == 0 && g_emit_terminal)
                   ? MOQ_EVENT_SESSION_CLOSED : MOQ_EVENT_SUBSCRIBE_REQUEST;
    *out_count = 1;
    return MOQ_OK;
}

void moq_event_cleanup(moq_event_t *event) { (void)event; g_cleanups++; }

/* The remaining facade entry points the TU references but these cases never
 * reach. */
moq_result_t moq_wtquic_msquic_managed_create(
    const moq_wtquic_msquic_managed_cfg_t *c, moq_wtquic_msquic_managed_t **o)
{
    g_server_creates++;
    g_bind_address_matches =
        c != NULL && c->host != NULL && g_expected_bind_address != NULL &&
        strcmp(c->host, g_expected_bind_address) == 0;
    *o = (moq_wtquic_msquic_managed_t *)&g_server_creates;
    return MOQ_OK;
}
uint16_t moq_wtquic_msquic_managed_port(const moq_wtquic_msquic_managed_t *m)
{ (void)m; return 4433; }
bool moq_wtquic_msquic_managed_stop_begin(moq_wtquic_msquic_managed_t *m)
{ (void)m; g_stop_begins++; return true; }
moq_result_t moq_wtquic_msquic_managed_join(moq_wtquic_msquic_managed_t *m)
{ (void)m; g_joins++; return g_join_fails ? MOQ_ERR_WRONG_STATE : MOQ_OK; }
void moq_wtquic_msquic_managed_destroy(moq_wtquic_msquic_managed_t *m)
{ (void)m; forbidden_after_failed_barrier("server_destroy"); g_server_destroys++; }
void moq_wtquic_msquic_managed_cfg_init_sized(
    moq_wtquic_msquic_managed_cfg_t *c, size_t n)
{ if (c) { memset(c, 0, n); c->struct_size = (uint32_t)n; } }
const moq_alloc_t *moq_alloc_default(void)
{ static const moq_alloc_t a; return &a; }

/* -- the row context the callbacks are given ------------------------------- */

static row_ctx_t g_row;

/*
 * End a direct case the way the protocol requires: UNBIND first, then free.
 * Freeing while the row context still points at the record leaves a dangling
 * borrow -- harmless here only because nothing dereferences it, which is
 * exactly the kind of "works by luck" this fixture exists to rule out.
 */
static void
end_row(origin_obs_t *o)
{
    row_unbind(&g_row);
    origin_obs_free(o);
}

static origin_obs_t *
begin_row(void)
{
    origin_obs_t *o = origin_obs_new();
    row_bind(&g_row, o);
    g_established = false;
    g_close_ok = true;
    g_close_calls = 0;
    g_seal_ok = true;
    g_seal_kind = ORIGIN_SEALED_KIND_NONE;
    g_seal_quic_code = 0;
    g_seal_domain = 0;
    g_seal_reserved0 = 0;
    g_seal_reserved1 = 0;
    g_seal_native_code = 0;
    g_seal_written = 4;
    g_seal_restore_size = true;
    g_release_calls = 0;
    return o;
}

/*
 * What the child's lifecycle ledger must read once the chain has run. The
 * counters live in the CHILD -- the parent cannot see them -- so the child
 * checks its own and fails the row by exit status. Without this the chain
 * could skip the environment, the server, the barrier or the release and the
 * parent would still be handed a well-formed passing report.
 */
enum { LIFE_FULL = 0, LIFE_JOIN_FAILS = 1, LIFE_ENV_FAILS = 2 };
static int g_lifecycle_mode = LIFE_FULL;

static int
lifecycle_ok(void)
{
    switch (g_lifecycle_mode) {
    case LIFE_FULL:
        /* constructed, connected, waited, stopped, joined, destroyed, the
         * environment barrier taken, and the session released exactly once */
        return g_server_creates == 1 && g_stop_begins == 1 && g_joins == 1 &&
               g_server_destroys == 1 && g_env_opens == 1 &&
               g_env_closes == 1 && g_connects == 1 && g_release_calls == 1 &&
               g_bind_address_matches;
    case LIFE_JOIN_FAILS:
        /* Unreachable by construction: that child exits from inside the
         * driver. What must hold there is enforced at the point of violation
         * by forbidden_after_failed_barrier, and by the parent requiring the
         * exact named child-level exit status. */
        return 1;
    case LIFE_ENV_FAILS:
        /* the environment never opened, so there is nothing to close, connect
         * or release -- but the server it did build is still torn down */
        return g_env_opens == 1 && g_env_closes == 0 && g_connects == 0 &&
               g_release_calls == 0 && g_server_creates == 1 &&
               g_server_destroys == 1 && g_bind_address_matches;
    default:
        return 0;
    }
}

static int
composed_child(void *ctx, size_t row_index, origin_report_t *out)
{
    int rc;

    rc = native_row_child(ctx, row_index, out);
    if (!lifecycle_ok()) {
        fprintf(stderr,
                "composed child lifecycle ledger wrong (mode %d): "
                "env %d/%d server %d/%d stop %d join %d connect %d rel %d\n",
                g_lifecycle_mode, g_env_opens, g_env_closes, g_server_creates,
                g_server_destroys, g_stop_begins, g_joins, g_connects,
                g_release_calls);
        _exit(4);
    }
    return rc;
}

/* signals a row's terminal a short time from now, from another thread */
static void *
late_terminal(void *arg)
{
    row_ctx_t *c = arg;
    struct timespec rq = { 0, 60 * 1000 * 1000 };

    nanosleep(&rq, NULL);
    pthread_mutex_lock(&c->mu);
    c->terminal = true;
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->mu);
    return NULL;
}

int
main(void)
{
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);
    origin_snapshot_t s;
    char why[ORIGIN_REASON_CAP];
    origin_obs_t *o;

    CHECK(pthread_mutex_init(&g_row.mu, NULL) == 0);
    CHECK(pthread_cond_init(&g_row.cv, NULL) == 0);
    k_row_user = &g_row;

    /* -- a POSITIVE row, through the real callbacks ------------------------ */
    /*
     * This is the case the offline fake could not see: ev_established queries
     * the SELECTED profile, and the verdict must accept that rather than
     * demanding an unselected observation no established session can make.
     */
    {
        wtq_str_t sub;
        o = begin_row();
        g_established = true;
        sub.data = ORIGIN_EXPECT_SUBPROTOCOL;
        sub.len = strlen(ORIGIN_EXPECT_SUBPROTOCOL);
        ev_established((wtq_session_t *)&g_row, sub, &g_row);
        /* the driver's post-barrier steps */
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o);
        origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        checks++;
        if (!origin_row_passed(&rows[0], &s, why, sizeof(why))) {
            printf("FAIL: a native positive row cannot pass: %s\n", why);
            failures++;
        }
        /* on_closed really did fire before close returned, and saw the attempt */
        CHECK(g_close_calls == 1);
        CHECK(s.close_attempted && s.close_seen_attempt_in_callback);
        CHECK(s.n_closed == 1 && s.close_ok);
        CHECK(!s.profile_before_seen);   /* an established row must not claim it */
        end_row(o);
    }

    /* -- a 403 row, through the real callbacks ----------------------------- */
    {
        o = begin_row();
        g_established = false;
        ev_refused((wtq_session_t *)&g_row, ORIGIN_REFUSAL_STATUS, &g_row);
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o);
        origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        checks++;
        if (!origin_row_passed(&rows[2], &s, why, sizeof(why))) {
            printf("FAIL: a native 403 row cannot pass: %s\n", why);
            failures++;
        }
        /* the unselected query really was made, and preserved its output */
        CHECK(s.profile_before_seen && s.profile_before_state &&
              s.profile_before_untouched);
        end_row(o);
    }

    /* -- callbacks that arrive with no row bound do nothing ---------------- */
    {
        row_unbind(&g_row);
        ev_refused((wtq_session_t *)&g_row, 403, &g_row);   /* must not crash */
        CHECK(row_obs(&g_row) == NULL);
    }

    /* -- a failed close fails the row, through the real adapter ------------ */
    {
        wtq_str_t sub;
        o = begin_row();
        g_established = true;
        g_close_ok = false;
        sub.data = ORIGIN_EXPECT_SUBPROTOCOL;
        sub.len = strlen(ORIGIN_EXPECT_SUBPROTOCOL);
        ev_established((wtq_session_t *)&g_row, sub, &g_row);
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o); origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        CHECK(!origin_row_passed(&rows[0], &s, why, sizeof(why)));
        CHECK(strstr(why, "close") != NULL);
        end_row(o);
    }

    /*
     * -- every part of the sealed shape, each with its OWN reason -----------
     *
     * A record rejected by a neighbouring predicate proves nothing about the
     * target rule, so each case perturbs exactly one field and asserts the
     * reason that field's predicate gives.
     */
    {
        struct seal_case {
            const char *what;
            bool     ok;
            uint32_t kind;
            uint64_t quic;
            uint32_t domain;
            uint32_t reserved0;
            uint32_t reserved1;
            long long native;
            int      written;
            bool     restore_size;
            const char *reason;
        };
        static const struct seal_case kBad[] = {
            { "a failed read",        false, 0, 0, 0, 0, 0, 0, 4, true,  "could not be read" },
            { "a transport kind",     true,  1, 0, 0, 0, 0, 0, 4, true,  "transport error kind" },
            { "a local kind",         true,  3, 0, 0, 0, 0, 0, 4, true,  "transport error kind" },
            { "a nonzero QUIC code",  true,  0, 7, 0, 0, 0, 0, 4, true,  "QUIC code" },
            { "a native domain",      true,  0, 0, 1, 0, 0, 0, 4, true,  "native error domain" },
            { "a nonzero native code",true,  0, 0, 0, 0, 0, 9, 4, true,  "native code" },
            { "a dirty first reserved word",
                                      true,  0, 0, 0, 6, 0, 0, 4, true,  "first reserved" },
            { "a dirty second reserved word",
                                      true,  0, 0, 0, 0, 5, 0, 4, true,  "second reserved" },
            /* An older producer that stops before the trailing fields leaves
             * the caller's poison there; the first predicate that looks at a
             * poisoned field is the one that refuses. */
            { "a short producer",     true,  0, 0, 0, 0, 0, 0, 2, true,
              "native error domain" },
            /* Every VALUE is right, but the caller's struct_size was not
             * restored. No field predicate looks at that word, so only the
             * whole-object comparison can see it. */
            { "a producer that clobbers the caller size",
                                      true,  0, 0, 0, 0, 0, 0, 4, false,
              "written whole" },
        };
        size_t i;
        for (i = 0; i < sizeof(kBad) / sizeof(kBad[0]); i++) {
            o = begin_row();
            g_seal_ok = kBad[i].ok;
            g_seal_kind = kBad[i].kind;
            g_seal_quic_code = kBad[i].quic;
            g_seal_domain = kBad[i].domain;
            g_seal_reserved0 = kBad[i].reserved0;
            g_seal_reserved1 = kBad[i].reserved1;
            g_seal_native_code = kBad[i].native;
            g_seal_written = kBad[i].written;
            g_seal_restore_size = kBad[i].restore_size;
            ev_refused((wtq_session_t *)&g_row, ORIGIN_REFUSAL_STATUS, &g_row);
            origin_obs_constructed(o); origin_obs_constructed(o);
            origin_obs_ref_acquired(o); origin_obs_barrier_done(o);
            row_unbind(&g_row);
            origin_obs_ref_released(o);
            origin_obs_finalized(o); origin_obs_finalized(o);
            origin_obs_teardown_ok(o);
            origin_obs_snapshot(o, &s);
            checks++;
            if (origin_row_passed(&rows[2], &s, why, sizeof(why))) {
                printf("FAIL: sealed shape (%s) was accepted\n", kBad[i].what);
                failures++;
            } else if (strstr(why, kBad[i].reason) == NULL) {
                /* the RIGHT predicate must be the one that refused */
                printf("FAIL: %s was refused for the wrong reason: %s\n",
                       kBad[i].what, why);
                failures++;
            }
            end_row(o);
        }
        /* and the exact full write still passes */
        o = begin_row();
        ev_refused((wtq_session_t *)&g_row, ORIGIN_REFUSAL_STATUS, &g_row);
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o); origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        CHECK(s.sealed.image_exact);
        CHECK(origin_row_passed(&rows[2], &s, why, sizeof(why)));
        end_row(o);
    }

    /*
     * -- the REAL composition, end to end ----------------------------------
     *
     * This is the chain the previous fixture never entered: an abort() placed
     * in native_row_child left the old test green because it manufactured the
     * lifecycle by hand instead of calling it. Here the supervisor forks a
     * child, the child calls native_row_child, which builds native_callees and
     * runs origin_driver_run_row, whose native_connect delivers callbacks
     * before it returns, whose native_wait observes the terminal, whose
     * teardown runs stop/join/destroy and the env barrier and unbind and
     * release -- and the parent judges the retained report.
     */
    {
        native_ctx_t nctx;
        row_ctx_t rowc;
        origin_super_cfg_t scfg;
        origin_child_result_t res;
        size_t n_rows = 0;
        const origin_row_t *all = origin_rows(&n_rows);

        memset(&rowc, 0, sizeof(rowc));
        CHECK(pthread_mutex_init(&rowc.mu, NULL) == 0);
        CHECK(row_cond_init(&rowc.cv) == 0);
        memset(&nctx, 0, sizeof(nctx));
        nctx.cert = "unused"; nctx.key = "unused";
        nctx.ca_file = "unused"; nctx.server_name = "127.0.0.1";
        nctx.bind_address = "::1";
        nctx.row = &rowc;
        g_expected_bind_address = nctx.bind_address;
        g_native = &nctx;

        k_row_user = NULL;      /* the composed run routes by session handle */
        memset(&scfg, 0, sizeof(scfg));
        scfg.child = composed_child;
        /*
         * The ledger lives in the child, so the PARENT's only guarantee that
         * it ran is that the wrapper is the configured entry point. A check
         * of a child-local flag could never see the supervisor being wired
         * straight to native_row_child, because the code holding that check
         * would not run at all.
         */
        checks++;
        if (scfg.child != composed_child) {
            printf("FAIL: the supervisor was not wired to the ledger "
                   "wrapper, so no lifecycle assertion runs at all\n");
            failures++;
        }
        scfg.ctx = &nctx;
        scfg.row_budget_ms = 5000;
        scfg.teardown_grace_ms = 2000;

        /* an ADMITTED row, all the way through */
        g_lifecycle_mode = LIFE_FULL;
        g_active_row = &all[0];
        origin_super_run_row(&scfg, 0, &res);
        checks++;
        if (res.status != ORIGIN_CHILD_OK || !res.passed) {
            printf("FAIL: the composed admitted row did not pass: "
                   "status=%d %s\n", (int)res.status, res.detail);
            failures++;
        }
        CHECK(res.reaped);

        /* a 403 row, all the way through */
        g_lifecycle_mode = LIFE_FULL;
        g_active_row = &all[2];
        origin_super_run_row(&scfg, 2, &res);
        checks++;
        if (res.status != ORIGIN_CHILD_OK || !res.passed) {
            printf("FAIL: the composed 403 row did not pass: status=%d %s\n",
                   (int)res.status, res.detail);
            failures++;
        }
        CHECK(res.reaped);

        /*
         * A row whose server completion barrier fails must NOT destroy the
         * server, must not free callback-owned state, and must publish no
         * completed row -- while the parent still reaps the exact child.
         */
        g_lifecycle_mode = LIFE_JOIN_FAILS;
        g_forbid_teardown = 1;
        g_active_row = &all[0];
        g_join_fails = true;
        origin_super_run_row(&scfg, 0, &res);
        g_join_fails = false;
        g_forbid_teardown = 0;
        checks++;
        if (res.passed || res.status == ORIGIN_CHILD_OK) {
            printf("FAIL: a failed barrier published a completed row\n");
            failures++;
        }
        CHECK(res.reaped);
        /*
         * The exact named disposition, not merely "some failure". Accepting
         * BAD_PHASE here as well would let a missing or corrupt transition
         * masquerade as the barrier refusal this case is about; exit 4 would
         * mean an illegal teardown call was made anyway.
         */
        CHECK(res.exit_code == 3);
        CHECK(res.status == ORIGIN_CHILD_NO_REPORT);

        /* a client environment that cannot open fails the row through the
         * same chain, and nothing is released */
        g_lifecycle_mode = LIFE_ENV_FAILS;
        g_active_row = &all[0];
        g_env_open_fails = true;
        origin_super_run_row(&scfg, 0, &res);
        g_env_open_fails = false;
        checks++;
        if (res.passed) {
            printf("FAIL: a row whose env never opened passed\n");
            failures++;
        }
        CHECK(res.reaped);
        /*
         * Every one of these four runs also asserted its own lifecycle ledger
         * INSIDE the child, where the counters live: a child whose ledger did
         * not match exits 4, and one that never entered the wrapper at all
         * exits 5. No legitimate outcome uses either code.
         */
        CHECK(res.exit_code != 4);
        g_lifecycle_mode = LIFE_FULL;

        pthread_cond_destroy(&rowc.cv);
        pthread_mutex_destroy(&rowc.mu);
        g_native = NULL;
        g_active_row = NULL;
        g_expected_bind_address = NULL;
        k_row_user = &g_row;    /* restore the direct-case routing */
    }

    /*
     * -- the terminal wait's CLOCK DOMAIN ---------------------------------
     *
     * The budget is a CLOCK_MONOTONIC duration. A wait that converted it
     * through the realtime clock would still look correct here on a quiet
     * machine and be wrong the moment the wall clock stepped, so both
     * directions are measured against CLOCK_MONOTONIC itself: an early wake
     * must return well inside the budget, and an expiry must not return
     * before it.
     */
    {
        row_ctx_t w;
        pthread_t th;
        long long t0, spent;

        memset(&w, 0, sizeof(w));
        CHECK(pthread_mutex_init(&w.mu, NULL) == 0);
        CHECK(row_cond_init(&w.cv) == 0);

        /* a terminal that arrives long before the deadline */
        t0 = mono_ms();
        CHECK(pthread_create(&th, NULL, late_terminal, &w) == 0);
        checks++;
        if (!wait_terminal(&w, 4000)) {
            printf("FAIL: the wait missed a terminal that arrived in time\n");
            failures++;
        }
        spent = mono_ms() - t0;
        pthread_join(th, NULL);
        checks++;
        if (spent >= 2000) {
            printf("FAIL: the wait did not wake on the terminal: %lldms\n",
                   spent);
            failures++;
        }

        /* and a deadline that expires with no terminal at all */
        w.terminal = false;
        t0 = mono_ms();
        checks++;
        if (wait_terminal(&w, 120)) {
            printf("FAIL: the wait claimed a terminal that never arrived\n");
            failures++;
        }
        spent = mono_ms() - t0;
        checks++;
        if (spent < 120) {
            printf("FAIL: the wait gave up %lldms before its deadline\n",
                   120 - spent);
            failures++;
        }
        pthread_cond_destroy(&w.cv);
        pthread_mutex_destroy(&w.mu);
    }

    /* -- the OWNING PUMP, through the real function ------------------------ */
    {
        moq_wtquic_msquic_managed_conn_t *c0 = (void *)0x1;
        moq_session_t *sess = (void *)0x2;
        native_ctx_t nctx;

        memset(&nctx, 0, sizeof(nctx));
        nctx.row = &g_row;

        /* every polled event is cleaned exactly once, terminal included */
        o = begin_row();
        g_conns[0] = c0; g_conn_session[0] = sess; g_conn_count = 1;
        g_events_left = 3; g_emit_terminal = true; g_poll_fails = false;
        g_cleanups = 0; g_acks = 0; g_ack_result = MOQ_OK;
        (void)server_pump(NULL, NULL, 0, &nctx);
        CHECK(g_cleanups == 3);
        CHECK(g_acks == 1);          /* one ack, after the terminal was polled */
        origin_obs_snapshot(o, &s);
        CHECK(!s.pump_failed);
        end_row(o);

        /* no terminal polled: nothing is acknowledged */
        o = begin_row();
        g_events_left = 2; g_emit_terminal = false;
        g_cleanups = 0; g_acks = 0;
        (void)server_pump(NULL, NULL, 0, &nctx);
        CHECK(g_cleanups == 2);
        CHECK(g_acks == 0);
        end_row(o);

        /* a pre-session child is never acknowledged */
        o = begin_row();
        g_conn_session[0] = NULL;
        g_events_left = 0; g_cleanups = 0; g_acks = 0;
        (void)server_pump(NULL, NULL, 0, &nctx);
        CHECK(g_acks == 0 && g_cleanups == 0);
        origin_obs_snapshot(o, &s);
        CHECK(!s.pump_failed);
        end_row(o);
        g_conn_session[0] = sess;

        /* a poll failure fails the row by name rather than disappearing */
        o = begin_row();
        g_events_left = 2; g_emit_terminal = true; g_poll_fails = true;
        g_cleanups = 0; g_acks = 0;
        (void)server_pump(NULL, NULL, 0, &nctx);
        /* an otherwise complete 403 row, so the PUMP failure is what decides */
        ev_refused((wtq_session_t *)&g_row, ORIGIN_REFUSAL_STATUS, &g_row);
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o); origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        CHECK(s.pump_failed);
        CHECK(!origin_row_passed(&rows[2], &s, why, sizeof(why)));
        CHECK(strstr(why, "poll") != NULL);
        end_row(o);
        g_poll_fails = false;

        /* a refused acknowledgment likewise */
        o = begin_row();
        g_events_left = 1; g_emit_terminal = true;
        g_cleanups = 0; g_acks = 0; g_ack_result = MOQ_ERR_WRONG_STATE;
        (void)server_pump(NULL, NULL, 0, &nctx);
        ev_refused((wtq_session_t *)&g_row, ORIGIN_REFUSAL_STATUS, &g_row);
        origin_obs_constructed(o); origin_obs_constructed(o);
        origin_obs_ref_acquired(o); origin_obs_barrier_done(o);
        origin_obs_ref_released(o);
        origin_obs_finalized(o); origin_obs_finalized(o);
        origin_obs_teardown_ok(o);
        origin_obs_snapshot(o, &s);
        CHECK(g_acks == 1 && s.pump_failed);
        CHECK(!origin_row_passed(&rows[2], &s, why, sizeof(why)));
        CHECK(strstr(why, "acknowledge") != NULL);
        end_row(o);
        g_ack_result = MOQ_OK;
    }

    pthread_cond_destroy(&g_row.cv);
    pthread_mutex_destroy(&g_row.mu);
    if (failures != 0) {
        printf("FAILED: %d of %d native checks\n", failures, checks);
        return 1;
    }
    printf("PASS: %d native checks\n", checks);
    return 0;
}
