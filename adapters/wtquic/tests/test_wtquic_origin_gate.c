/*
 * Origin authorization at the real boundary: the LibMoQ managed WebTransport
 * SERVER, and a directly driven WebTransport client that verifies the server's
 * certificate against a private CA.
 *
 * What this fixture is for: proving that the server's Origin policy decides
 * admission, and that a denial is the server's own 403 rather than anything
 * else that also fails to connect. The row table, the observation recorders and
 * the verdict all live in origin_rows.[ch] and are exercised without a provider
 * by test_wtquic_origin_rows; this file is the transport wiring around them.
 *
 * LIFETIME, which is the part that is easy to get wrong here:
 *
 *   The direct client is NOT the managed facade. wtq_msquic_client_connect*
 *   starts the connection while holding no caller guard, and the backend may
 *   dispatch events before the call assigns *session_out. So a callback can run
 *   before this code has any handle at all. Every callback therefore uses the
 *   session argument it was given and never a handle slot the driver may not
 *   have filled yet.
 *
 *   Establishment is not terminal. Exactly one terminal fires per session --
 *   on_closed for an established session, on_refused/on_failed when
 *   establishment never completed -- so row state stays alive until the
 *   terminal is observed and the environment's completion barrier has run.
 *
 *   The reference returned by a successful connect is app-owned, captured once
 *   after the call returns, and released only after wtq_msquic_env_close has
 *   returned. Callbacks never release it. A refused synchronous call leaves the
 *   output NULL, which is not a handle to release.
 *
 * Modes: --identity reports the linked dependency identities and creates
 * nothing at all; --rows runs the table only when explicitly invoked (the
 * registered CTest entry is identity-only).
 */
#include "origin_driver.h"
#include "origin_super.h"
#include "origin_rows.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "moq/wtquic_msquic_managed.h"
#include "wtquic/session.h"
#include "wtquic/wtquic_msquic.h"


/* -- per-row client state -------------------------------------------------- */

/*
 * The callbacks' view of the current row.
 *
 * `obs` is BORROWED. The driver owns each observation record and frees it after
 * the verdict, so this context binds the record before a connect that may
 * dispatch callbacks and unbinds it after the completion barrier -- never
 * leaving a freed pointer here between rows.
 */
typedef struct {
    origin_obs_t   *obs;        /* borrowed, under mu */
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    bool            terminal;   /* a terminal event has been observed */
} row_ctx_t;

/*
 * Create the row's condition on the monotonic clock where the platform offers
 * it. Darwin has no such attribute and uses the relative wait instead, so this
 * is a plain init there -- the clock domain is still monotonic because the
 * wait never converts through wall time.
 */
static int
row_cond_init(pthread_cond_t *cv)
{
#if defined(CLOCK_MONOTONIC) && !defined(__APPLE__)
    pthread_condattr_t attr;
    int rc;

    if (pthread_condattr_init(&attr) != 0) {
        return -1;
    }
    if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
        pthread_condattr_destroy(&attr);
        return -1;
    }
    rc = pthread_cond_init(cv, &attr);
    pthread_condattr_destroy(&attr);
    return rc;
#else
    return pthread_cond_init(cv, NULL);
#endif
}

/* Bind the borrowed record and reset the row's latch, before any callback. */
static void
row_bind(row_ctx_t *c, origin_obs_t *obs)
{
    pthread_mutex_lock(&c->mu);
    c->obs = obs;
    c->terminal = false;
    pthread_mutex_unlock(&c->mu);
}

/* Unbind after the barrier, so no callback path can reach a record the driver
 * is about to free. */
static void
row_unbind(row_ctx_t *c)
{
    pthread_mutex_lock(&c->mu);
    c->obs = NULL;
    pthread_mutex_unlock(&c->mu);
}

/* The bound record, or NULL. Callbacks that arrive outside a row do nothing. */
static origin_obs_t *
row_obs(row_ctx_t *c)
{
    origin_obs_t *o;
    pthread_mutex_lock(&c->mu);
    o = c->obs;
    pthread_mutex_unlock(&c->mu);
    return o;
}

static void
row_signal_terminal(row_ctx_t *c)
{
    pthread_mutex_lock(&c->mu);
    c->terminal = true;
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->mu);
}

/* Read the sealed transport-error record at a documented point: it is final
 * from immediately before the terminal callback onwards. */
static void
record_sealed(origin_obs_t *o, wtq_session_t *s)
{
    wtq_transport_error_t err, want;
    origin_sealed_t rec;
    wtq_result_t r;

    /*
     * POISON the whole object first, then set only the caller size. The API
     * copies what fits and restores struct_size, so a size comparison could
     * only rediscover the input; and every value this matrix expects is zero,
     * so a zeroed buffer would look correct even if a shorter producer never
     * wrote the trailing fields. Poison makes "was it written" measurable.
     */
    memset(&err, 0xa5, sizeof(err));
    err.struct_size = (uint32_t)sizeof(err);
    r = wtq_session_transport_error(s, &err);

    /* the exact object a complete, no-transport-cause seal must produce */
    memset(&want, 0, sizeof(want));
    want.struct_size = (uint32_t)sizeof(want);
    /*
     * The whole sized record is captured, not just its kind. The READ's own
     * result is separate evidence from what the record says: a failed read is
     * not "nothing happened".
     */
    memset(&rec, 0, sizeof(rec));
    rec.query_ok = (r == WTQ_OK);
    /* the caller's size survived AND every other byte was overwritten */
    rec.image_exact = (r == WTQ_OK &&
                       memcmp(&err, &want, sizeof(err)) == 0);
    if (r == WTQ_OK) {
        rec.kind = err.kind;
        rec.reserved0 = err.reserved0;
        rec.quic_code = err.quic_code;
        rec.native_domain = err.native_domain;
        rec.reserved1 = err.reserved1;
        rec.native_code = (long long)err.native_code;
    }
    origin_obs_sealed(o, &rec);
}

/*
 * Query the profile on a session that never selected one, at a legal point.
 *
 * The output word is poisoned first with a value no profile uses. The query
 * must report the unselected state AND leave that poison intact -- otherwise a
 * zero written into the output could not be told apart from CURRENT.
 */
static void
record_unselected_profile(origin_obs_t *o, const wtq_session_t *s)
{
    const uint32_t kPoison = 0xa5a5a5a5u;
    wtq_webtransport_profile_t p;
    wtq_result_t r;

    memcpy(&p, &kPoison, sizeof(p) < sizeof(kPoison) ? sizeof(p)
                                                     : sizeof(kPoison));
    r = wtq_session_webtransport_profile(s, &p);
    origin_obs_profile_unselected(o, r == WTQ_ERR_STATE,
                                  memcmp(&p, &kPoison,
                                         sizeof(p) < sizeof(kPoison)
                                             ? sizeof(p)
                                             : sizeof(kPoison)) == 0);
}

static void
ev_established(wtq_session_t *s, wtq_str_t subprotocol, void *user)
{
    row_ctx_t *c = user;
    origin_obs_t *o = row_obs(c);
    wtq_webtransport_profile_t profile = (wtq_webtransport_profile_t)0;
    wtq_result_t pr;

    if (o == NULL) {
        return;             /* no row is bound: nothing to record */
    }
    /* the argument is borrowed for this callback only */
    origin_obs_established(o, subprotocol.data, subprotocol.len);

    /* an established session HAS selected a profile: the query succeeds here */
    pr = wtq_session_webtransport_profile(s, &profile);
    origin_obs_profile(o, pr == WTQ_OK, (uint32_t)profile);

    /*
     * Establishment is provisional. The library delivers on_closed BEFORE
     * wtq_session_close returns, so the ATTEMPT is recorded first -- otherwise
     * the reentrant on_closed cannot tell this from a peer-initiated close --
     * and the call's own RESULT is recorded after it returns.
     */
    origin_obs_close_attempt(o);
    origin_obs_close_result(o, wtq_session_close(s, 0, NULL, 0) == WTQ_OK);
}

static void
ev_refused(wtq_session_t *s, uint16_t http_status, void *user)
{
    row_ctx_t *c = user;
    origin_obs_t *o = row_obs(c);

    if (o == NULL) {
        return;
    }
    origin_obs_refused(o, http_status);
    /* this session never selected a profile: the legal point to prove that */
    record_unselected_profile(o, s);
    record_sealed(o, s);
    row_signal_terminal(c);
}

static void
ev_failed(wtq_session_t *s, wtq_connect_failure_t why, void *user)
{
    row_ctx_t *c = user;
    origin_obs_t *o = row_obs(c);
    (void)why;

    if (o == NULL) {
        return;
    }
    origin_obs_failed(o);
    record_unselected_profile(o, s);
    record_sealed(o, s);
    row_signal_terminal(c);
}

static void
ev_closed(wtq_session_t *s, uint32_t code, const uint8_t *reason,
          size_t reason_len, bool clean, void *user)
{
    row_ctx_t *c = user;
    origin_obs_t *o = row_obs(c);
    (void)code; (void)reason; (void)reason_len; (void)clean;

    if (o == NULL) {
        return;
    }
    origin_obs_closed(o);
    /* this reentrant callback observes the local attempt, which is what
     * separates our deliberate close from a peer-initiated one */
    (void)origin_obs_close_attempted(o);
    record_sealed(o, s);
    row_signal_terminal(c);
}

static const wtq_session_events_t k_client_events = {
    .struct_size = (uint32_t)sizeof(wtq_session_events_t),
    .on_established = ev_established,
    .on_refused = ev_refused,
    .on_failed = ev_failed,
    .on_closed = ev_closed,
};

/*
 * Wait for the row's terminal within a duration measured on the SAME clock the
 * driver's deadline uses.
 *
 * The driver's budget is monotonic. Converting it through a wall clock -- as a
 * default condition variable's absolute timeout does -- lets a clock step turn
 * a valid row into an early refusal, or leave this wait running past the
 * deadline it claims to honour. Darwin has no monotonic condition attribute,
 * so the relative wait is used there and the remaining time is recomputed from
 * CLOCK_MONOTONIC after every wakeup; elsewhere the condition itself is
 * monotonic. No polling sleep is involved either way.
 */
static long long
mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static bool
wait_terminal(row_ctx_t *c, int budget_ms)
{
    long long deadline = mono_ms() + budget_ms;
    bool got;

    pthread_mutex_lock(&c->mu);
    while (!c->terminal) {
        long long left = deadline - mono_ms();
        struct timespec rel;

        if (left <= 0) {
            break;
        }
        rel.tv_sec = (time_t)(left / 1000);
        rel.tv_nsec = (long)(left % 1000) * 1000000L;
#if defined(__APPLE__)
        if (pthread_cond_timedwait_relative_np(&c->cv, &c->mu, &rel) != 0 &&
            !c->terminal) {
            /* a timeout or a spurious wake: the loop re-measures the deadline */
            if (deadline - mono_ms() <= 0) {
                break;
            }
        }
#else
        {
            struct timespec until;
            clock_gettime(CLOCK_MONOTONIC, &until);
            until.tv_sec += rel.tv_sec;
            until.tv_nsec += rel.tv_nsec;
            if (until.tv_nsec >= 1000000000L) {
                until.tv_sec++;
                until.tv_nsec -= 1000000000L;
            }
            if (pthread_cond_timedwait(&c->cv, &c->mu, &until) != 0 &&
                !c->terminal && deadline - mono_ms() <= 0) {
                break;
            }
        }
#endif
    }
    got = c->terminal;
    pthread_mutex_unlock(&c->mu);
    return got;
}

/* -- the pump the managed server needs ------------------------------------- */

/*
 * The owning lane pump.
 *
 * An established WebTransport connection owns a MoQ session even when no MoQ
 * payload ever flows, so this pump is not a no-op. It polls each presented
 * child's session, and once it has observed MOQ_EVENT_SESSION_CLOSED it
 * acknowledges that child from this same callback -- which is the only place
 * acknowledgment is legal. A pre-session child has no terminal event and
 * nothing to acknowledge; it is reclaimed on transport terminal and quiescence
 * alone, and is deliberately not acknowledged here.
 *
 * No handle is retained: conn, session and adapter pointers are valid only for
 * this callback and may be reused as soon as it returns.
 */
/*
 * The pump runs on the server's lane thread while a row is in flight, so it
 * reports failures into the SAME row record the client callbacks use, through
 * the same borrow protocol. It never retains a conn or session pointer.
 */
typedef struct native_ctx native_ctx_t_fwd;
static void pump_report_failure(native_ctx_t_fwd *n, const char *why);

static int
server_pump(moq_wtquic_msquic_managed_t *m,
            moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
            void *user)
{
    native_ctx_t_fwd *nf = user;
    moq_wtquic_msquic_managed_conn_t *conn;
    (void)m; (void)now_us;

    for (conn = moq_wtquic_msquic_lane_next_conn(lane, NULL); conn != NULL;
         conn = moq_wtquic_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *sess = moq_wtquic_msquic_managed_conn_session(conn);
        bool closed = false;

        if (sess == NULL) {
            /* a pre-session child has no terminal to observe and nothing to
             * acknowledge; it is reclaimed on transport terminal alone */
            continue;
        }
        for (;;) {
            moq_event_t ev;
            size_t got = 0;
            moq_result_t pr;

            /* the sized poll distinguishes an ERROR from an empty queue; the
             * convenience wrapper reports both as zero */
            pr = moq_session_poll_events_ex(sess, &ev, 1, sizeof(ev), &got);
            if (pr != MOQ_OK) {
                pump_report_failure(nf,
                                    "the owning pump could not poll a session");
                break;
            }
            if (got == 0) {
                break;
            }
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                closed = true;
            }
            /* every polled event is cleaned, whether it was processed or
             * dropped -- including the terminal, and BEFORE any acknowledgment,
             * because a successful ack lets the connection be reclaimed as
             * soon as this callback returns */
            moq_event_cleanup(&ev);
        }
        if (closed) {
            /* legal only from this owning callback, and only once the terminal
             * has actually been polled */
            moq_result_t ar = moq_wtquic_msquic_managed_conn_ack_terminal(conn);
            if (ar != MOQ_OK) {
                /* a refused acknowledgment leaves the child linked forever;
                 * it must fail the row rather than disappear */
                pump_report_failure(nf,
                                    "the owning pump could not acknowledge a "
                                    "terminal child");
            }
        }
    }
    return 0;
}

/* -- the real callee table ------------------------------------------------- */

/*
 * What a native row needs from its operator. These are inputs, not defaults:
 * the fixture never invents a credential, a CA or an address.
 */
struct native_ctx {
    const char *cert;        /* server certificate, private-CA issued */
    const char *key;         /* its private key */
    const char *ca_file;     /* the CA the CLIENT trusts, in PEM */
    const char *server_name; /* the identity checked in the certificate */
    const char *bind_address;/* exact loopback literal the server binds */
    row_ctx_t  *row;         /* the current row's callback state */
};
typedef struct native_ctx native_ctx_t;

static void
pump_report_failure(native_ctx_t_fwd *n, const char *why)
{
    origin_obs_t *o;

    if (n == NULL || n->row == NULL) {
        return;
    }
    o = row_obs(n->row);
    if (o != NULL) {
        origin_obs_pump_failed(o, why);
    }
}

static long long
native_now_ms(void *ctx)
{
    struct timespec ts;
    (void)ctx;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int
native_server_create(void *ctx, const origin_row_t *row, void **out)
{
    native_ctx_t *n = ctx;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_t *m = NULL;
    static const char *const kProtos[] = { ORIGIN_EXPECT_SUBPROTOCOL };

    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = n->bind_address;
    cfg.port = 0;                       /* ephemeral; _port() reports it */
    cfg.cert_path = n->cert;
    cfg.key_path = n->key;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = kProtos;
    cfg.wt_protocol_count = 1;
    cfg.webtransport_profile = ORIGIN_EXPECT_PROFILE;
    cfg.origin_policy = (uint32_t)row->policy;
    if (row->policy == ORIGIN_POLICY_ALLOWLIST) {
        cfg.allowed_origins = row->allow;
        cfg.allowed_origin_count = row->allow_count;
    }
    cfg.lane_count = 1;
    cfg.max_connections = 4;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_user = n;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_OK) {
        return -1;
    }
    *out = m;
    return 0;
}

static unsigned short
native_server_port(void *ctx, void *server)
{
    (void)ctx;
    return moq_wtquic_msquic_managed_port(server);
}

static void
native_stop_begin(void *ctx, void *server)
{
    (void)ctx;
    /* idempotent and cannot fail; false only means another caller began
     * teardown first, which is not an error */
    (void)moq_wtquic_msquic_managed_stop_begin(server);
}

static int
native_join(void *ctx, void *server)
{
    (void)ctx;
    return moq_wtquic_msquic_managed_join(server) == MOQ_OK ? 0 : -1;
}

static void
native_destroy(void *ctx, void *server)
{
    (void)ctx;
    moq_wtquic_msquic_managed_destroy(server);
}

static int
native_env_open(void *ctx, void **out)
{
    wtq_msquic_env_cfg_t ecfg;
    wtq_msquic_env_t *env = NULL;
    (void)ctx;

    wtq_msquic_env_cfg_init(&ecfg);
    if (wtq_msquic_env_open(&ecfg, &env) != WTQ_OK) {
        return -1;
    }
    *out = env;
    return 0;
}

static int
native_connect(void *ctx, void *env, const origin_row_t *row,
               unsigned short port, origin_obs_t *obs, void **out)
{
    native_ctx_t *n = ctx;
    wtq_msquic_client_cfg_t ccfg;
    wtq_msquic_client_trust_cfg_t trust = WTQ_MSQUIC_CLIENT_TRUST_CFG_INIT;
    wtq_connect_config_t conn;
    wtq_session_t *session = NULL;
    static const char *const kProtos[] = { ORIGIN_EXPECT_SUBPROTOCOL };

    /* Bind the row's record to the callbacks BEFORE a connect that may
     * dispatch them. The record is the driver's; this only borrows it. */
    row_bind(n->row, obs);

    wtq_msquic_client_cfg_init(&ccfg);
    ccfg.server_name = n->server_name;
    ccfg.port = port;
    /* explicit trust and insecure_skip_verify conflict; the trusted entry
     * refuses the combination, and this fixture never sets it */
    ccfg.user = n->row;
    ccfg.events = &k_client_events;

    wtq_connect_config_init(&conn);
    conn.authority = n->server_name;
    conn.path = "/moq";
    conn.origin = row->client_origin;      /* NULL means none is sent */
    conn.subprotocols = kProtos;
    conn.subprotocol_count = 1;
    conn.webtransport_profile = ORIGIN_EXPECT_PROFILE;
    ccfg.connect = &conn;

    trust.additional_ca_file = n->ca_file;

    if (wtq_msquic_client_connect_with_trust(env, &ccfg, &trust, &session)
        != WTQ_OK) {
        *out = NULL;    /* a refused call leaves nothing to release */
        return -1;
    }
    *out = session;
    return 0;
}

static int
native_wait(void *ctx, origin_obs_t *obs, int remaining)
{
    native_ctx_t *n = ctx;
    /* already bound at connect; rebinding here would be an unsynchronized
     * write of a pointer the callbacks are using */
    (void)obs;
    return wait_terminal(n->row, remaining) ? 0 : -1;
}

static void
native_env_close(void *ctx, void *env)
{
    native_ctx_t *n = ctx;

    /* THE completion barrier: after this returns the backend will never touch
     * a session again. Only then is it safe to unbind the borrowed record, and
     * only after unbinding may the driver free it. */
    wtq_msquic_env_close(env);
    row_unbind(n->row);
}

static void
native_session_release(void *ctx, void *session)
{
    (void)ctx;
    wtq_session_release(session);
}

/* The child's phase transition, emitted immediately before unwind. */
static void
native_enter_teardown(void *ctx)
{
    (void)ctx;
    origin_super_child_enter_teardown();
}

static void
native_callees(origin_callees_t *c, native_ctx_t *n)
{
    memset(c, 0, sizeof(*c));
    c->on_enter_teardown = native_enter_teardown;
    c->ctx = n;
    c->server_create = native_server_create;
    c->server_port = native_server_port;
    c->server_stop_begin = native_stop_begin;
    c->server_join = native_join;
    c->server_destroy = native_destroy;
    c->env_open = native_env_open;
    c->connect = native_connect;
    c->wait_terminal = native_wait;
    c->env_close = native_env_close;
    c->session_release = native_session_release;
    c->now_ms = native_now_ms;
}

/*
 * One row, inside its own child process.
 *
 * The child builds the observation record the driver owns, runs the real row,
 * and hands back a report. It never decides the outcome on its own account:
 * the parent recomputes the verdict from this snapshot.
 */
static native_ctx_t *g_native;

static int
native_row_child(void *ctx, size_t row_index, origin_report_t *out)
{
    native_ctx_t *n = ctx;
    origin_callees_t callees;
    origin_obs_t *obs = origin_obs_new();
    origin_snapshot_t snap;
    size_t count = 0;
    const origin_row_t *rows = origin_rows(&count);
    bool ok;

    if (obs == NULL || row_index >= count) {
        return 1;
    }
    native_callees(&callees, n);
    {
        bool unsafe = false;
        ok = origin_driver_run_row(&callees, &rows[row_index], obs, &unsafe,
                                   NULL, 0);
        if (unsafe) {
            /*
             * The server's completion barrier failed, so a live pump may still
             * borrow this record. Nothing is snapshotted, freed or reported:
             * the child exits and process teardown reclaims the rest, and the
             * parent sees a row with no final report.
             */
            fprintf(stderr,
                    "origin-gate: row %zu teardown barrier failed; exiting "
                    "without a report\n", row_index);
            _exit(3);
        }
    }
    origin_obs_snapshot(obs, &snap);
    origin_report_init(out, (uint32_t)row_index, ok, &snap);
    origin_obs_free(obs);
    return ok ? 0 : 1;
}

/* -- identity mode --------------------------------------------------------- */

/*
 * Genuine typed references to the two entry points this fixture depends on.
 *
 * `volatile` so the compiler cannot fold an address comparison to a constant
 * and drop the reference: an earlier version printed "linked" from a build with
 * no WT, LibMoQ or provider library at all, because `&fn != NULL` folds and the
 * unused function then disappears. These force real relocations, so the symbol
 * must actually resolve at link and load time, and `nm`/`otool` on this
 * executable can confirm it independently.
 */
typedef wtq_result_t (*trusted_connect_fn)(
    wtq_msquic_env_t *, const wtq_msquic_client_cfg_t *,
    const wtq_msquic_client_trust_cfg_t *, wtq_session_t **);
typedef void (*facade_cfg_init_fn)(moq_wtquic_msquic_managed_cfg_t *, size_t);

static trusted_connect_fn volatile g_trusted_connect =
    wtq_msquic_client_connect_with_trust;
static facade_cfg_init_fn volatile g_facade_cfg_init =
    moq_wtquic_msquic_managed_cfg_init_sized;

/*
 * Report what this binary is actually linked against, creating nothing: no
 * environment, no registration, no listener, no client, no certificate and no
 * socket. This runs before any of those exist so the identities can be bound to
 * the exact binary a later native run would use.
 *
 * Every claim below is an OBSERVATION, not a label. The version comes from a
 * real call into the WT library; the facade is proved by actually calling its
 * pure struct initializer and reading back what it wrote; the trusted entry is
 * proved by a volatile typed pointer the linker had to resolve.
 */
static int
mode_identity(void)
{
    size_t n = 0;
    moq_wtquic_msquic_managed_cfg_t cfg;
    trusted_connect_fn tc = g_trusted_connect;
    facade_cfg_init_fn fi = g_facade_cfg_init;

    (void)origin_rows(&n);

    /* a real call into the facade: it initializes a struct and creates
     * nothing -- no environment, listener, client, socket or credential */
    memset(&cfg, 0, sizeof(cfg));
    fi(&cfg, sizeof(cfg));

    printf("origin-gate identity\n");
    printf("  wtquic runtime version:  %s\n", wtq_version());
    printf("  wtquic header version:   %s\n", WTQ_VERSION_STRING);
    printf("  trusted client entry:    %s\n",
           tc != NULL ? "resolved" : "UNRESOLVED");
    printf("  facade cfg_init:         %s\n",
           fi != NULL ? "resolved" : "UNRESOLVED");
    printf("  facade struct_size:      %u (observed from a real call)\n",
           (unsigned)cfg.struct_size);
    printf("  row table:               %zu rows\n", n);
    printf("  created:                 nothing\n");

    /* The observation must actually hold: an unresolved pointer or a facade
     * that wrote nothing means this binary cannot be the one a native run
     * uses, and saying so is the point of this mode. */
    if (tc == NULL || fi == NULL ||
        cfg.struct_size != (uint32_t)sizeof(cfg)) {
        fprintf(stderr,
                "origin-gate: identity could not be established from this "
                "binary\n");
        return 1;
    }
    return 0;
}

int
main(int argc, char **argv)
{
    const char *mode = argc > 1 ? argv[1] : "--identity";

    if (strcmp(mode, "--identity") == 0) {
        return mode_identity();
    }
    if (strcmp(mode, "--rows") == 0) {
        native_ctx_t n;
        row_ctx_t row;
        origin_super_cfg_t cfg;
        origin_child_result_t last;
        size_t executed = 0, failed = 0;
        bool ok;

        /* Required inputs; the fixture invents no credential or address. */
        if (argc != 7) {
            fprintf(stderr,
                    "usage: %s --rows <cert> <key> <ca-pem> <server-name> "
                    "<bind-address>\n",
                    argv[0]);
            return 2;
        }
        memset(&n, 0, sizeof(n));
        memset(&row, 0, sizeof(row));
        n.cert = argv[2];
        n.key = argv[3];
        n.ca_file = argv[4];
        n.server_name = argv[5];
        n.bind_address = argv[6];
        n.row = &row;
        if (pthread_mutex_init(&row.mu, NULL) != 0) {
            fprintf(stderr, "origin-gate: the row mutex could not be created\n");
            return 2;
        }
        if (row_cond_init(&row.cv) != 0) {
            pthread_mutex_destroy(&row.mu);
            fprintf(stderr,
                    "origin-gate: the row condition could not be created\n");
            return 2;
        }
        g_native = &n;

        /*
         * Each row runs in its own OWNED child. The synchronous transport
         * calls cannot be bounded from inside the process that makes them, and
         * unwinding a blocked one here would tear down callback state the
         * backend may still be using -- so the parent stops a stuck row from
         * outside and judges its report rather than trusting it.
         */
        memset(&cfg, 0, sizeof(cfg));
        cfg.child = native_row_child;
        cfg.ctx = &n;
        memset(&last, 0, sizeof(last));
        ok = origin_super_run_table(&cfg, &executed, &failed, &last);

        printf("origin-gate rows: executed %zu\n", executed);
        if (!ok) {
            printf("failed at row %zu: status=%d %s\n", failed,
                   (int)last.status, last.detail);
        } else {
            printf("all rows passed\n");
        }
        pthread_cond_destroy(&row.cv);
        pthread_mutex_destroy(&row.mu);
        return ok ? 0 : 1;
    }
    fprintf(stderr,
            "usage: %s [--identity|--rows <cert> <key> <ca-pem> <name> "
            "<bind-address>]\n",
            argv[0]);
    return 2;
}
