/*
 * The admin listener boundary: real loopback sockets, one real owner thread,
 * and the real frozen state machine behind it.
 *
 * This is the ONLY test in this tier that opens a socket. Everything about
 * parsing, negotiation, deadlines, banks and the release protocol is proven
 * sans-I/O elsewhere; what can only be proven here is that the wrapper binds,
 * owns, serves and unwinds correctly -- including the parts a sans-I/O test
 * cannot see, such as whether a failed start leaks and whether two threads
 * touch the same state.
 *
 * Every wait in this file is bounded and every bound fails the run. A test that
 * hangs reports nothing.
 */
#include "../cli/admin_listen.h"
#include "../cli/broker.h"
#include "../obs/moqr_obs.h"

/* The listener's deterministic failure seams, compiled only into this
 * target's copy of the listener. */
void moqr_admin_listen_test_fail_clock(int on);
void moqr_admin_listen_test_fail_poll(int on);
void moqr_admin_listen_test_park_owner(int on);
void moqr_admin_listen_test_fail_view_cv(int on);
int  moqr_admin_listen_test_view_mu_balance(void);
int  moqr_admin_listen_test_turns(void);
/* Declared in admin_listen.h: listener-bound, event-driven observation. */
/* The frozen client states, as the snapshot reports them. */
#define CS_FREE 0
#define CS_READING 1
#define CS_PARSED 2
#define CS_WAITING 3
#define CS_WRITING 4
#define CS_DONE 5
void moqr_admin_listen_test_script_write(const int *results, int n);
void moqr_admin_listen_test_fail_shutdown(int on);
void moqr_admin_listen_test_client_write_budget(int bytes);
void moqr_admin_listen_test_freeze_client(int slot);
int  moqr_admin_listen_test_read_calls(void);
void moqr_admin_listen_test_reset_read_calls(void);
void moqr_admin_listen_test_set_poll_ms(int ms);
void moqr_admin_listen_test_fail_transition(int which);
int  moqr_admin_listen_test_transition_hit(void);
void moqr_admin_listen_test_on_bytes_failure_hook(void (*fn)(void *),
                                                   void *ctx);

#include "../../../tests/unit/test_support.h"

#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>


/* -- a coordinator stand-in ------------------------------------------------
 *
 * It answers the three seams from its own memory. It holds no relay, bind or
 * shard state -- which is the point: if the listener ever needed live state,
 * this harness could not satisfy it. */
/*
 * The coordinator stand-in.
 *
 * The wake callback does what a real one does and NOTHING more: it records
 * that the lanes were asked to publish. It does not notify -- a real lane
 * publishes later, on its own thread, and notifies at THAT point. A rig whose
 * wake notifies synchronously would make the production notification look
 * load-bearing when it never runs.
 */
/* The immutable document the rig hands the endpoint, and the one switch that
 * withholds it (set only by the case that proves construction refuses). */
#define RIG_INFO "{\"api\":\"v1\",\"rig\":\"listen\"}"
static int g_rig_omit_info;
static size_t g_rig_info_len;   /* 0: the document's real length */

typedef struct rig {
    moqr_broker_t     broker;
    atomic_int        wakes;         /* wake_all invocations                 */
    atomic_int        wake_fails;    /* make wake_all report failure         */
    atomic_int        collects;
    atomic_int        renders;
    atomic_int        emits;
    atomic_int        suppressions;
    atomic_ullong     suppressed_serial;
    atomic_int        poison;        /* answer INVAL instead of OK           */
    atomic_int        poison_once;   /* poison exactly one generation        */
    atomic_int        render_fails;  /* fail the RENDER, not the collect      */
    atomic_int        big_body;      /* render a body far larger than one
                                      * write chunk                          */
    atomic_int        latched;       /* a pending signal demand, as a handler
                                      * would leave it                       */
    atomic_ullong     last_emit_len;
    atomic_ullong     collected_serial;

    /* the deferred-publication model */
    atomic_int        asked;         /* lanes were asked to publish          */
    atomic_ullong     published_serial;   /* the generation a lane published */
    atomic_int        publish_delay_ms;
    /* An EXPLICIT hold. Publication does not happen until the test releases
     * it; a long delay that the test usually lowers later is still a
     * wall-clock hold, and it makes the state depend on the scheduler. */
    /* The publisher's barrier: it blocks here while held, reports that it is
     * blocked, and is woken directly on release or stop. */
    pthread_mutex_t   pub_mu;
    pthread_cond_t    pub_cv;
    /* What was actually acquired, so a refusal unwinds exactly that much and
     * teardown destroys each object exactly once. */
    int               pub_mu_live;
    int               pub_cv_live;
    int               broker_live;
    int               refuse_listen;   /* take the post-init refusal path */

    int               fail_mu_init;    /* refuse the mutex acquisition       */
    int               fail_cv_init;    /* refuse the condition acquisition   */
    int               fail_mu_destroy; /* the mutex refuses to be destroyed  */
    int               fail_cv_destroy; /* the condition refuses to be freed  */
    int               pub_held;
    int               pub_blocked;
    int               pub_stop;
    atomic_int        notify_enabled;
    atomic_int        notifies;
    pthread_t         publisher;
    atomic_int        publisher_live;

    moqr_admin_listen_t *listen;
} rig_t;


/* Being asked is the publisher's barrier condition, so every path that asks
 * must also release it -- the lane wake, and any test standing in for one. */
static void
rig_ask(rig_t *r)
{
    atomic_store(&r->asked, 1);
    (void)pthread_mutex_lock(&r->pub_mu);
    (void)pthread_cond_broadcast(&r->pub_cv);
    (void)pthread_mutex_unlock(&r->pub_mu);
}
static moqr_result_t
rig_wake_all(void *ctx)
{
    rig_t *r = (rig_t *)ctx;
    atomic_fetch_add(&r->wakes, 1);
    if (atomic_load(&r->wake_fails)) {
        return MOQR_ERR_INTERNAL;   /* a lane could not be woken */
    }
    /* A real wake ASKS the lanes to publish. It does not publish, and it does
     * not notify -- but it does release the publisher's barrier, because being
     * asked is exactly the condition the publisher is waiting on. */
    rig_ask(r);
    return MOQR_OK;
}

/* A lane: it publishes some time after being asked, then notifies AT THE
 * PUBLISH POINT -- the production sequence. */
static void *
rig_publisher(void *arg)
{
    rig_t *r = (rig_t *)arg;
    uint64_t last = 0;

    for (;;) {
        uint64_t serial = 0;
        uint32_t demand = 0;
        int delay_ms;

        /*
         * A REAL BARRIER, not a poll.
         *
         * The publisher blocks on a condition until there is something to
         * publish and the test has released the hold, and it says so while it
         * is blocked. Looping on a short sleep is an atomic polling gate: it
         * cannot report that it reached the held state, it wakes late, and it
         * makes the fixture's behaviour a function of the tick.
         */
        (void)pthread_mutex_lock(&r->pub_mu);
        for (;;) {
            bool have_work = false;
            if (r->pub_stop) {
                break;
            }
            if (!r->pub_held && atomic_load(&r->asked) &&
                moqr_broker_current(&r->broker, &serial, &demand) &&
                serial != last) {
                have_work = true;
            }
            if (have_work) {
                break;
            }
            r->pub_blocked = 1;
            (void)pthread_cond_broadcast(&r->pub_cv);
            (void)pthread_cond_wait(&r->pub_cv, &r->pub_mu);
        }
        r->pub_blocked = 0;
        if (r->pub_stop) {
            (void)pthread_cond_broadcast(&r->pub_cv);
            (void)pthread_mutex_unlock(&r->pub_mu);
            break;
        }
        delay_ms = atomic_load(&r->publish_delay_ms);
        (void)pthread_mutex_unlock(&r->pub_mu);

        /* The one case that deliberately exercises a physical timing property
         * -- a row that lands after the owner is already waiting -- keeps its
         * short delay, bounded and interruptible by the stop signal. */
        for (int i = 0; i < delay_ms; i++) {
            struct timespec d = { 0, 1000 * 1000 };
            (void)pthread_mutex_lock(&r->pub_mu);
            if (r->pub_stop) {
                (void)pthread_mutex_unlock(&r->pub_mu);
                return NULL;
            }
            (void)pthread_mutex_unlock(&r->pub_mu);
            (void)nanosleep(&d, NULL);
        }

        last = serial;
        atomic_store(&r->published_serial, serial);
        /* THE PRODUCTION SEQUENCE: notify at the publish point, not from the
         * wake callback. */
        if (atomic_load(&r->notify_enabled)) {
            atomic_fetch_add(&r->notifies, 1);
            moqr_admin_listen_notify(r->listen);
        }
    }
    return NULL;
}

/* Hold publication. The publisher blocks at its barrier; nothing is published
 * until the hold is released. */
static void
rig_hold_publication(rig_t *r)
{
    (void)pthread_mutex_lock(&r->pub_mu);
    r->pub_held = 1;
    (void)pthread_cond_broadcast(&r->pub_cv);
    (void)pthread_mutex_unlock(&r->pub_mu);
}

/* Release it, and wake the publisher directly. */
static void
rig_release_publication(rig_t *r)
{
    (void)pthread_mutex_lock(&r->pub_mu);
    r->pub_held = 0;
    (void)pthread_cond_broadcast(&r->pub_cv);
    (void)pthread_mutex_unlock(&r->pub_mu);
}

/* The publisher has REACHED the barrier. A case that needs the held state to
 * exist waits for this rather than assuming it. */
static bool
rig_wait_publisher_blocked(rig_t *r, int budget_ms)
{
    struct timespec deadline;
    bool blocked = false;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += budget_ms / 1000;
    deadline.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    (void)pthread_mutex_lock(&r->pub_mu);
    while (!r->pub_blocked) {
        if (pthread_cond_timedwait(&r->pub_cv, &r->pub_mu, &deadline) != 0) {
            break;
        }
    }
    blocked = r->pub_blocked != 0;
    (void)pthread_mutex_unlock(&r->pub_mu);
    return blocked;
}

/* Stop it, from any early-return path, and bound the join. */
static void
rig_stop_publisher(rig_t *r)
{
    if (!atomic_load(&r->publisher_live)) {
        return;
    }
    (void)pthread_mutex_lock(&r->pub_mu);
    r->pub_stop = 1;
    (void)pthread_cond_broadcast(&r->pub_cv);
    (void)pthread_mutex_unlock(&r->pub_mu);
    (void)pthread_join(r->publisher, NULL);
    atomic_store(&r->publisher_live, 0);
}

/* What a signal handler leaves behind: a lock-free flag, consumed exactly once
 * by whoever owns the broker. */
static bool
rig_signal_pending(void *ctx)
{
    rig_t *r = (rig_t *)ctx;
    return atomic_exchange(&r->latched, 0) != 0;
}

static moqr_result_t
rig_collect(void *ctx, uint64_t serial)
{
    rig_t *r = (rig_t *)ctx;
    atomic_fetch_add(&r->collects, 1);
    if (atomic_load(&r->poison)) {
        if (atomic_load(&r->poison_once)) {
            atomic_store(&r->poison, 0);
        }
        return MOQR_ERR_INVAL;
    }
    if (atomic_load(&r->published_serial) != serial) {
        return MOQR_ERR_WOULD_BLOCK;   /* this epoch is not complete yet */
    }
    atomic_store(&r->collected_serial, serial);
    return MOQR_OK;
}

static moqr_result_t
rig_render(void *ctx, uint64_t serial, char *const bodies[MOQR_ADMIN_BODY__COUNT],
           const size_t caps[MOQR_ADMIN_BODY__COUNT],
           size_t out_len[MOQR_ADMIN_BODY__COUNT])
{
    rig_t *r = (rig_t *)ctx;
    static const char *tag[MOQR_ADMIN_BODY__COUNT] = {
        [MOQR_OBS_FMT_PROMETHEUS_004] = "PROM",
        [MOQR_OBS_FMT_OPENMETRICS_100] = "OPENMETRICS",
        [MOQR_ADMIN_BODY_SHARDS] = "SHARDS",
    };
    atomic_fetch_add(&r->renders, 1);
    if (atomic_load(&r->render_fails)) {
        return MOQR_ERR_INVAL;
    }
    /* Render only into the storage handed in, and only within its cap: a
     * renderer that writes past the bank is exactly what the cap exists to
     * catch under ASan. */
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        int n = snprintf(bodies[k], caps[k], "%s serial=%llu\n", tag[k],
                         (unsigned long long)serial);
        if (n < 0 || (size_t)n >= caps[k]) {
            return MOQR_ERR_CAPACITY;
        }
        out_len[k] = (size_t)n;
        if (atomic_load(&r->big_body)) {
            /* Many write chunks, so a response cannot complete in one write
             * and a peer can vanish part way through one. */
            size_t want = caps[k] > 262144u ? 262144u : caps[k] - 1u;
            memset(bodies[k] + (size_t)n, 'x', want - (size_t)n);
            out_len[k] = want;
        }
    }
    return MOQR_OK;
}

static void
rig_emit(void *ctx, uint64_t serial, const char *body, size_t len)
{
    rig_t *r = (rig_t *)ctx;
    (void)serial;
    (void)body;
    atomic_fetch_add(&r->emits, 1);
    atomic_store(&r->last_emit_len, (unsigned long long)len);
}

static void
rig_emit_suppressed(void *ctx, uint64_t serial)
{
    rig_t *r = (rig_t *)ctx;
    atomic_fetch_add(&r->suppressions, 1);
    atomic_store(&r->suppressed_serial, (unsigned long long)serial);
}

/* The publisher is serial-driven, so nothing needs re-arming between
 * generations. Kept as the place a test says "a new generation starts here". */
static void
rig_arm_next_generation(rig_t *r)
{
    (void)r;
}

static void
rig_latch_signal(void *ctx)
{
    rig_t *r = (rig_t *)ctx;
    atomic_store(&r->latched, 1);
}

/*
 * THE RIG'S OWN LIFETIME LEDGER.
 *
 * The fixture owns three resources with destructors -- a broker, a mutex and a
 * condition -- and it acquires them one at a time, so a refusal partway
 * through must release exactly what it took. Counting each acquisition and
 * release makes that an assertion rather than an assumption: a skipped or
 * repeated destruction is a number, not a leak that only a sanitizer on some
 * other lane might notice.
 */
static int g_rig_mu_init;
static int g_rig_mu_destroy;
static int g_rig_cv_init;
static int g_rig_cv_destroy;
static int g_rig_broker_init;
static int g_rig_broker_destroy;

/* Destroys that were ASKED FOR but did not succeed. A refused destroy is not a
 * release, and it must not be able to leave the run looking balanced. */
static int g_rig_release_failures;
/* Refusals a case deliberately arranged, so the run gate can tell a proven
 * refusal from one nobody asked for. */
static int g_rig_expected_release_failures;

/*
 * Releases exactly what was acquired, innermost first.
 *
 * A destroy is counted -- and ownership given up -- only when the call
 * returns success. Counting the attempt would certify a refused destroy as a
 * release, which is the same attempt-versus-effect confusion the listener
 * itself was corrected for. Ownership is kept on failure so nothing later can
 * touch an object whose state is unknown.
 */
static void
rig_release_resources(rig_t *r)
{
    if (r->pub_cv_live) {
        if (!r->fail_cv_destroy && pthread_cond_destroy(&r->pub_cv) == 0) {
            r->pub_cv_live = 0;
            g_rig_cv_destroy++;
        } else {
            g_rig_release_failures++;
            printf("  riglife: the condition refused to be destroyed\n");
        }
    }
    if (r->pub_mu_live) {
        if (!r->fail_mu_destroy && pthread_mutex_destroy(&r->pub_mu) == 0) {
            r->pub_mu_live = 0;
            g_rig_mu_destroy++;
        } else {
            g_rig_release_failures++;
            printf("  riglife: the mutex refused to be destroyed\n");
        }
    }
    if (r->broker_live) {
        r->broker_live = 0;
        g_rig_broker_destroy++;
        moqr_broker_destroy(&r->broker);
    }
}

static moqr_result_t
rig_start_ex(rig_t *r, moqr_admin_listen_t **out, int refuse_listen,
             int fail_mu_init, int fail_cv_init)
{
    moqr_admin_listen_cfg_t cfg;
    moqr_cli_admin_t a;
    moqr_result_t rc;

    memset(r, 0, sizeof(*r));
    r->refuse_listen = refuse_listen;
    r->fail_mu_init = fail_mu_init;
    r->fail_cv_init = fail_cv_init;
    if (moqr_broker_init(&r->broker, MOQR_BROKER_BANKS) != MOQR_OK) {
        return MOQR_ERR_INTERNAL;
    }
    r->broker_live = 1;
    g_rig_broker_init++;
    atomic_store(&r->notify_enabled, 1);
    /* Acquired separately: a combined test cannot say which one succeeded, and
     * would strand the mutex when the condition refuses. */
    if (r->fail_mu_init || pthread_mutex_init(&r->pub_mu, NULL) != 0) {
        rig_release_resources(r);
        return MOQR_ERR_INTERNAL;
    }
    r->pub_mu_live = 1;
    g_rig_mu_init++;
    if (r->fail_cv_init || pthread_cond_init(&r->pub_cv, NULL) != 0) {
        rig_release_resources(r);
        return MOQR_ERR_INTERNAL;
    }
    r->pub_cv_live = 1;
    g_rig_cv_init++;
    memset(&a, 0, sizeof(a));
    a.enabled = true;
    a.mode = MOQR_CLI_ADMIN_TCP;
    (void)snprintf(a.host, sizeof(a.host), "127.0.0.1");
    a.port = 0;

    memset(&cfg, 0, sizeof(cfg));
    cfg.admin = &a;
    cfg.lanes = 1;
    cfg.broker = &r->broker;
    cfg.wake_all = rig_wake_all;
    cfg.collect = rig_collect;
    cfg.render = rig_render;
    cfg.signal_pending = rig_signal_pending;
    cfg.emit_signal = rig_emit;
    cfg.emit_suppressed = rig_emit_suppressed;
    cfg.ctx = r;
    /* The rig is the owner here: it freezes the document the endpoint
     * serves, exactly as the coordinator does in production. */
    cfg.info = g_rig_omit_info ? NULL : RIG_INFO;
    cfg.info_len = g_rig_omit_info ? 0u
                 : g_rig_info_len != 0u ? g_rig_info_len : strlen(RIG_INFO);
    /* An ephemeral port is only offered when the case wants the endpoint. A
     * case exercising the refusal path leaves port 0 unallowed, which is the
     * ordinary production refusal -- no special failure seam. */
    cfg.allow_ephemeral_port = r->refuse_listen ? false : true;

    rc = moqr_admin_listen_start(&cfg, out);
    if (rc != MOQR_OK) {
        rig_release_resources(r);
        return rc;
    }
    r->listen = *out;
    /* Phase two, in the production order: the pointer the callbacks reach is
     * installed (r->listen, above) BEFORE the endpoint is allowed to serve. */
    {
        moqr_result_t arc = moqr_admin_listen_activate(*out);
        if (arc == MOQR_ERR_WOULD_BLOCK) {
            /* Mirrors production: an unprovable owner releases NOTHING. The
             * caller is handed the endpoint and the verdict. */
            return MOQR_ERR_WOULD_BLOCK;
        }
        if (arc != MOQR_OK) {
            moqr_admin_listen_destroy(*out);
            rig_release_resources(r);
            r->listen = NULL;
            *out = NULL;
            return MOQR_ERR_INTERNAL;
        }
    }
    if (pthread_create(&r->publisher, NULL, rig_publisher, r) != 0) {
        (void)moqr_admin_listen_stop(*out);
        moqr_admin_listen_destroy(*out);
        rig_release_resources(r);
        return MOQR_ERR_INTERNAL;
    }
    atomic_store(&r->publisher_live, 1);
    return MOQR_OK;
}

static moqr_result_t
rig_start(rig_t *r, moqr_admin_listen_t **out)
{
    return rig_start_ex(r, out, 0, 0, 0);
}

static void
rig_stop(rig_t *r, moqr_admin_listen_t *l)
{
    /* The publisher blocks on the condition, so it is joined before anything
     * it can be blocked on is destroyed. */
    rig_stop_publisher(r);
    if (l != NULL) {
        (void)moqr_admin_listen_stop(l);
        moqr_admin_listen_destroy(l);
    }
    r->listen = NULL;
    rig_release_resources(r);
}

/* -- a bounded loopback client -------------------------------------------- */

static int
dial(int port)
{
    struct sockaddr_in sa;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        (void)close(fd);
        return -1;
    }
    return fd;
}

/* Read to EOF, bounded in both bytes and wall time. Returns the length, or -1
 * if the bound was reached -- never a partial success reported as success. */
static ssize_t
slurp(int fd, char *buf, size_t cap, int budget_ms)
{
    size_t used = 0;
    int spent = 0;

    while (spent <= budget_ms) {
        struct pollfd p;
        ssize_t n;
        p.fd = fd;
        p.events = POLLIN;
        p.revents = 0;
        if (poll(&p, 1, 25) < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        spent += 25;
        if ((p.revents & POLLIN) == 0) {
            if ((p.revents & (POLLERR | POLLNVAL)) != 0) {
                return -1;
            }
            continue;
        }
        n = read(fd, buf + used, cap - used - 1u);
        if (n == 0) {
            buf[used] = '\0';
            return (ssize_t)used;   /* the server closed: one request, one
                                     * response, then close */
        }
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            return -1;
        }
        used += (size_t)n;
        if (used + 1u >= cap) {
            return -1;
        }
    }
    return -1;
}

static bool
write_all(int fd, const char *s)
{
    size_t len = strlen(s);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, s + sent, len - sent);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        }
        return false;
    }
    return true;
}

/* One whole exchange, bounded. The budget is generous on purpose: none of
 * these cases is a latency test, and a bound tight enough to fail on a loaded
 * machine measures the machine rather than the endpoint. */
#define MOQR_TEST_EXCHANGE_BUDGET_MS 20000

static ssize_t
exchange(int port, const char *req, char *buf, size_t cap)
{
    ssize_t n;
    int fd = dial(port);
    if (fd < 0) {
        return -1;
    }
    /* A write failure is not a test failure on its own: the refusal slot
     * answers and closes without ever reading, so a request can legitimately
     * meet a closed peer. What matters is the response, so the read is always
     * attempted. */
    (void)write_all(fd, req);
    n = slurp(fd, buf, cap, MOQR_TEST_EXCHANGE_BUDGET_MS);
    (void)close(fd);
    return n;
}

/* -- the cases ------------------------------------------------------------ */

static int
test_start_refusals(void)
{
    int failures = 0;
    moqr_admin_listen_t *l = (moqr_admin_listen_t *)(void *)0x1;
    moqr_broker_t broker;
    moqr_cli_admin_t a;
    moqr_admin_listen_cfg_t base;

    MOQ_TEST_CHECK(moqr_broker_init(&broker, MOQR_BROKER_BANKS) == MOQR_OK);
    memset(&a, 0, sizeof(a));
    a.enabled = true;
    a.mode = MOQR_CLI_ADMIN_TCP;
    (void)snprintf(a.host, sizeof(a.host), "127.0.0.1");
    a.port = 0;
    memset(&base, 0, sizeof(base));
    base.admin = &a;
    base.lanes = 1;
    base.broker = &broker;
    base.wake_all = rig_wake_all;
    base.collect = rig_collect;
    base.render = rig_render;
    base.allow_ephemeral_port = true;

    /* A refused start must not leave a stale handle behind: `*out` is cleared
     * before any acquisition, so a caller that ignores the status still cannot
     * publish a readiness line over a dangling pointer. */
    MOQ_TEST_CHECK(moqr_admin_listen_start(NULL, &l) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(l == NULL);
    MOQ_TEST_CHECK(moqr_admin_listen_start(&base, NULL) == MOQR_ERR_INVAL);

    {
        moqr_admin_listen_cfg_t c = base;
        c.broker = NULL;
        l = (moqr_admin_listen_t *)(void *)0x1;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(l == NULL);
    }
    {
        moqr_admin_listen_cfg_t c = base;
        c.collect = NULL;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
    }
    {
        moqr_admin_listen_cfg_t c = base;
        c.render = NULL;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
    }
    {
        moqr_admin_listen_cfg_t c = base;
        c.wake_all = NULL;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
    }
    /* The signal surface is atomic: no partial wiring may consume demand
     * without either its document or its suppression diagnostic. */
    for (unsigned mask = 1u; mask < 7u; mask++) {
        moqr_admin_listen_cfg_t c = base;
        c.signal_pending = (mask & 1u) != 0u ? rig_signal_pending : NULL;
        c.emit_signal = (mask & 2u) != 0u ? rig_emit : NULL;
        c.emit_suppressed = (mask & 4u) != 0u ? rig_emit_suppressed : NULL;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(l == NULL);
    }
    /* Port 0 is an ephemeral port. It is reachable only through the test seam;
     * a configuration file cannot express it, and without the seam the
     * constructor refuses it rather than binding somewhere unpredictable. */
    {
        moqr_admin_listen_cfg_t c = base;
        c.allow_ephemeral_port = false;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(l == NULL);
    }
    /* A disabled or non-TCP section is not a listener this module can open. */
    {
        moqr_cli_admin_t off = a;
        moqr_admin_listen_cfg_t c = base;
        off.enabled = false;
        c.admin = &off;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
    }
    {
        moqr_cli_admin_t bad = a;
        moqr_admin_listen_cfg_t c = base;
        bad.mode = MOQR_CLI_ADMIN_OFF;
        c.admin = &bad;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
    }
    /* A host that is not a literal address never reaches a resolver here. */
    {
        moqr_cli_admin_t bad = a;
        moqr_admin_listen_cfg_t c = base;
        (void)snprintf(bad.host, sizeof(bad.host), "localhost");
        c.admin = &bad;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) != MOQR_OK);
        MOQ_TEST_CHECK(l == NULL);
    }
    /* Direct callers are not entitled to the parser's termination guarantee.
     * The socket boundary must reject this without reading beyond host[]. */
    {
        moqr_cli_admin_t bad = a;
        moqr_admin_listen_cfg_t c = base;
        memset(bad.host, '1', sizeof(bad.host));
        c.admin = &bad;
        l = NULL;
        MOQ_TEST_CHECK(moqr_admin_listen_start(&c, &l) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(l == NULL);
    }
    moqr_broker_destroy(&broker);
    return failures;
}

/* /api/v1/shards over the real listener: a generation request like a
 * scrape -- it collects and is served the bank's shards slot, labelled JSON
 * with an exact Content-Length -- and a metrics scrape and a shards request
 * interleave through the same machinery. */
static int
test_serves_shards_over_a_generation(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[8192];
    ssize_t n;
    int port;
    int collects;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  shards: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    collects = atomic_load(&rig.collects);
    n = exchange(port, "GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "HTTP/1.1 200 OK\r\n") == NULL ||
        strstr(buf, "Content-Type: " MOQR_ADMIN_CT_JSON "\r\n") == NULL ||
        strstr(buf, "\r\n\r\nSHARDS serial=") == NULL ||
        strstr(buf, "PROM ") != NULL || strstr(buf, "OPENMETRICS") != NULL) {
        printf("  shards: not served the shards slot: %.120s\n", buf);
        failures++;
    } else {
        const char *body = strstr(buf, "\r\n\r\n") + 4;
        char clen[64];
        (void)snprintf(clen, sizeof(clen), "Content-Length: %zu\r\n",
                       strlen(body));
        if (strstr(buf, clen) == NULL) {
            printf("  shards: Content-Length does not match the body\n");
            failures++;
        }
    }
    if (atomic_load(&rig.collects) <= collects) {
        printf("  shards: no generation was collected for the request\n");
        failures++;
    }
    n = exchange(port, "GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n"
                       "Accept: text/plain\r\n\r\n", buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "HTTP/1.1 406 ") == NULL) {
        printf("  shards: a metrics-only Accept was not refused 406\n");
        failures++;
    }
    /* Interleaved: metrics, shards, metrics -- each its own slot. */
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "OPENMETRICS serial=") == NULL) {
        printf("  shards: a metrics scrape after shards failed\n");
        failures++;
    }
    n = exchange(port, "GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "SHARDS serial=") == NULL) {
        printf("  shards: a shards request after metrics failed\n");
        failures++;
    }
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/plain\r\n\r\n",
                 buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "PROM serial=") == NULL) {
        printf("  shards: a Prometheus scrape after shards failed\n");
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/* An endpoint that advertises /api/v1/info is not built without the bytes
 * to serve it: construction refuses, and nothing is left behind. */
static int
test_info_document_is_required(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;

    g_rig_omit_info = 1;
    if (rig_start(&rig, &l) == MOQR_OK || l != NULL) {
        printf("  info-required: the endpoint started without a document\n");
        failures++;
        rig_stop(&rig, l);
    }
    g_rig_omit_info = 0;
    /* An alleged length above the finite limit is refused at this boundary
     * too, without the endpoint reading a byte of it. */
    g_rig_info_len = SIZE_MAX;
    if (rig_start(&rig, &l) == MOQR_OK || l != NULL) {
        printf("  info-required: the endpoint started with an impossible "
               "document length\n");
        failures++;
        rig_stop(&rig, l);
    }
    g_rig_info_len = (size_t)MOQR_ADMIN_MAX_STATIC_DOC + 1u;
    if (rig_start(&rig, &l) == MOQR_OK || l != NULL) {
        printf("  info-required: the endpoint started with a document above "
               "the limit\n");
        failures++;
        rig_stop(&rig, l);
    }
    g_rig_info_len = 0;
    /* And with the document, the same rig starts. */
    if (rig_start(&rig, &l) != MOQR_OK || l == NULL) {
        printf("  info-required: the rig did not start once the document was "
               "supplied\n");
        failures++;
    } else {
        rig_stop(&rig, l);
    }
    return failures;
}

/* /api/v1/info over the real listener: the exact bytes the owner froze,
 * labelled JSON with an exact Content-Length, served without a single
 * collect or lane wake -- the generation machinery is not touched -- and the
 * shards document is not served. A metrics scrape after it still works. */
static int
test_serves_info_statically(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[8192];
    char clen[64];
    ssize_t n;
    int port;
    int collects, wakes;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  info: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    collects = atomic_load(&rig.collects);
    wakes = atomic_load(&rig.wakes);
    n = exchange(port, "GET /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    (void)snprintf(clen, sizeof(clen), "Content-Length: %zu\r\n",
                   strlen(RIG_INFO));
    if (n <= 0) {
        printf("  info: no response\n");
        failures++;
    } else {
        const char *body = strstr(buf, "\r\n\r\n");
        if (strstr(buf, "HTTP/1.1 200 OK\r\n") == NULL ||
            strstr(buf, "Content-Type: " MOQR_ADMIN_CT_JSON "\r\n") == NULL ||
            strstr(buf, clen) == NULL) {
            printf("  info: head is wrong: %.120s\n", buf);
            failures++;
        }
        if (body == NULL || strcmp(body + 4, RIG_INFO) != 0) {
            printf("  info: the body is not the owner's exact document\n");
            failures++;
        }
    }
    if (atomic_load(&rig.collects) != collects ||
        atomic_load(&rig.wakes) != wakes) {
        printf("  info: /info collected or woke lanes (collects %d->%d, wakes "
               "%d->%d)\n", collects, atomic_load(&rig.collects), wakes,
               atomic_load(&rig.wakes));
        failures++;
    }
    n = exchange(port, "GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                       "Accept: text/plain\r\n\r\n", buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "HTTP/1.1 406 ") == NULL) {
        printf("  info: a metrics-only Accept was not refused 406: %.64s\n", buf);
        failures++;
    }
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL ||
        strstr(buf, "OPENMETRICS serial=") == NULL) {
        printf("  info: a metrics scrape after /info failed: %.64s\n", buf);
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_serves_a_scrape(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[8192];
    ssize_t n;
    int port;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  serve: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    if (port <= 0) {
        printf("  serve: no bound port was reported\n");
        rig_stop(&rig, l);
        return 1;
    }

    /* Default negotiation: no Accept header selects OpenMetrics. */
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0) {
        printf("  serve: no response to a well-formed GET\n");
        failures++;
    } else {
        if (strstr(buf, "200 OK") == NULL) {
            printf("  serve: not a 200: %.64s\n", buf);
            failures++;
        }
        if (strstr(buf, MOQR_ADMIN_CT_OPENMETRICS) == NULL) {
            printf("  serve: the OpenMetrics content type is absent\n");
            failures++;
        }
        if (strstr(buf, "OPENMETRICS serial=") == NULL) {
            printf("  serve: the rendered body is absent\n");
            failures++;
        }
        /* The body came from a bank, so the exact generation the coordinator
         * collected is the one that was served. */
        if (strstr(buf, "PROM ") != NULL) {
            printf("  serve: the wrong format's body was delivered\n");
            failures++;
        }
        /* The WHOLE body, not a prefix of it. A close that races the last
         * write truncates silently, and only counting the bytes finds it. */
        {
            const char *cl = strstr(buf, "Content-Length: ");
            const char *body = strstr(buf, "\r\n\r\n");
            if (cl == NULL || body == NULL) {
                printf("  serve: no Content-Length or no body separator\n");
                failures++;
            } else {
                unsigned long want = strtoul(cl + 16, NULL, 10);
                size_t got = (size_t)n - (size_t)((body + 4) - buf);
                if (got != (size_t)want) {
                    printf("  serve: body is %zu bytes, Content-Length says "
                           "%lu\n", got, want);
                    failures++;
                }
            }
        }
    }
    if (atomic_load(&rig.renders) < 1) {
        printf("  serve: nothing was rendered\n");
        failures++;
    }
    /* A new generation's ONLY notice to the lanes is its wake. Dropping it
     * leaves publication to whatever the poll timeout happens to do, which is
     * a timing accident rather than a contract. */
    if (atomic_load(&rig.wakes) != 1) {
        printf("  serve: wake_all ran %d times for one generation, "
               "expected exactly 1\n", atomic_load(&rig.wakes));
        failures++;
    }
    if (moqr_admin_listen_wakes(l) != 1u) {
        printf("  serve: the wake counter reads %llu, expected 1\n",
               (unsigned long long)moqr_admin_listen_wakes(l));
        failures++;
    }
    /* HTTP-only demand emits no signal projection: a scrape must never write
     * an unsolicited dump. */
    if (atomic_load(&rig.emits) != 0) {
        printf("  serve: an HTTP scrape emitted a signal projection\n");
        failures++;
    }
    if (moqr_admin_listen_accepts(l) < 1u) {
        printf("  serve: the accept counter did not move\n");
        failures++;
    }

    /* Explicit Prometheus negotiation reaches the other body in the SAME bank. */
    n = exchange(port,
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n"
                 "Accept: text/plain; version=0.0.4\r\n\r\n", buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL ||
        strstr(buf, "PROM serial=") == NULL) {
        printf("  serve: Prometheus negotiation did not deliver its body\n");
        failures++;
    }

    /* The finite response table, through the real socket. */
    n = exchange(port, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "404") == NULL) {
        printf("  serve: an unknown target was not 404\n");
        failures++;
    }
    n = exchange(port, "POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "405") == NULL ||
        strstr(buf, "Allow: GET") == NULL) {
        printf("  serve: a non-GET was not 405 with Allow: GET\n");
        failures++;
    }
    if (n > 0 && strstr(buf, "OPENMETRICS serial=") != NULL) {
        printf("  serve: an error response carried a metrics body\n");
        failures++;
    }
    n = exchange(port,
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n"
                 "Accept: application/json\r\n\r\n", buf, sizeof(buf));
    if (n <= 0 || strstr(buf, "406") == NULL) {
        printf("  serve: an unacceptable Accept was not 406\n");
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

static int
test_signal_demand_emits_once(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    uint64_t serial = 0;
    bool wake = false;
    int spins;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  signal: start refused\n");
        return 1;
    }
    /* SIGNAL demand raised through the SAME broker the listener owns: one
     * identity space, so the owner thread renders and emits it. */
    MOQ_TEST_CHECK(moqr_broker_request(&rig.broker, MOQR_BROKER_DEMAND_SIGNAL,
                                       &serial, &wake) == MOQR_OK);
    MOQ_TEST_CHECK(serial != 0u);
    /* The request was made here, so the wake came back here: stand in for it,
     * then let the lane publish and notify as it would in production. */
    rig_ask(&rig);
    moqr_admin_listen_notify(l);

    for (spins = 0; spins < 200 && atomic_load(&rig.emits) == 0; spins++) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (spins >= 200) {
        printf("  signal: the projection was never emitted (bounded wait)\n");
        failures++;
    } else {
        if (atomic_load(&rig.emits) != 1) {
            printf("  signal: emitted %d times, expected exactly 1\n",
                   atomic_load(&rig.emits));
            failures++;
        }
        if (atomic_load(&rig.last_emit_len) == 0u) {
            printf("  signal: an empty projection was emitted\n");
            failures++;
        }
    }
    /* Signal-only demand must free its bank again rather than stranding it. */
    for (spins = 0; spins < 200 && moqr_broker_busy(&rig.broker); spins++) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (spins >= 200) {
        printf("  signal: the generation was never released (bounded wait)\n");
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/* The latch is the only thing a signal handler can leave behind, and turning it
 * into a generation is a broker transaction -- so it belongs to the same owner
 * as every other one. Nothing here touches the broker directly. */
static int
test_latched_signal_opens_a_generation(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int spins;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  latch: start refused\n");
        return 1;
    }
    atomic_store(&rig.latched, 1);
    moqr_admin_listen_notify(l);

    for (spins = 0; spins < 200 && atomic_load(&rig.emits) == 0; spins++) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (spins >= 200) {
        printf("  latch: a latched signal never opened a generation "
               "(bounded wait)\n");
        failures++;
    } else if (atomic_load(&rig.emits) != 1) {
        printf("  latch: emitted %d times for one latched signal, expected "
               "exactly 1\n", atomic_load(&rig.emits));
        failures++;
    }
    /* Consumed exactly once: the latch is clear and no second generation
     * follows it. */
    if (atomic_load(&rig.latched) != 0) {
        printf("  latch: the latch was not consumed\n");
        failures++;
    }
    for (spins = 0; spins < 40; spins++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (atomic_load(&rig.emits) != 1) {
        printf("  latch: one latched signal produced %d emissions\n",
               atomic_load(&rig.emits));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_terminality_fails_closed(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[4096];
    ssize_t n;
    int port;
    int wakes_before;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  terminal: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL) {
        printf("  terminal: the pre-terminal scrape did not succeed\n");
        failures++;
    }
    /* An in-flight generation, so terminality has real settlement work to do:
     * a release can open a deferred generation, and that is exactly the
     * transition that must NOT reach the lanes once they are terminal. */
    {
        uint64_t serial = 0;
        bool wake = false;
        MOQ_TEST_CHECK(moqr_broker_request(&rig.broker,
                                           MOQR_BROKER_DEMAND_SIGNAL, &serial,
                                           &wake) == MOQR_OK);
    }
    wakes_before = atomic_load(&rig.wakes);

    moqr_admin_listen_note_terminal(l);

    /*
     * Wait until the owner has actually PERFORMED the cancellation before
     * asserting anything about it. Terminality is a request; a connection made
     * in the window before the owner acts on it is admitted normally and is a
     * different case from the one under test here (it is covered by the
     * every-state cancellation case). Once cancelled the state machine admits
     * no further client, so an overflow refusal is the observable proof that
     * the transition has happened -- no sleep, and no guess about timing.
     */
    {
        char probe[1024];
        uint64_t before = moqr_admin_listen_refusals(l);
        for (int i = 0; i < 2000; i++) {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                           probe, sizeof(probe));
            if (moqr_admin_listen_refusals(l) > before) {
                break;
            }
            (void)nanosleep(&ts, NULL);
        }
        if (moqr_admin_listen_refusals(l) == before) {
            printf("  terminal: the owner never performed the "
                   "cancellation\n");
            failures++;
        }
    }

    /* After terminality a scrape must fail closed rather than wait on lanes
     * that will never pump again. */
    n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0) {
        printf("  terminal: no response after terminality (health %u, "
               "exited %d, accepts %llu, refusals %llu)\n",
               (unsigned)moqr_admin_listen_health(l),
               (int)moqr_admin_listen_owner_exited(l),
               (unsigned long long)moqr_admin_listen_accepts(l),
               (unsigned long long)moqr_admin_listen_refusals(l));
        failures++;
    } else if (strstr(buf, "200 OK") != NULL) {
        printf("  terminal: a scrape succeeded after terminality\n");
        failures++;
    } else if (strstr(buf, "OPENMETRICS serial=") != NULL) {
        printf("  terminal: a failed scrape carried a metrics body\n");
        failures++;
    }
    if (atomic_load(&rig.wakes) != wakes_before) {
        printf("  terminal: lanes were woken after terminality (%d -> %d)\n",
               wakes_before, atomic_load(&rig.wakes));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_overflow_uses_one_refusal_slot(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int fd[MOQR_ADMIN_MAX_CLIENTS + 4];
    uint32_t i;
    int port;
    int held = 0;
    char buf[2048];

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  overflow: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    /* Occupy every client slot with a connection that never completes a
     * request head, so nothing is served and nothing is released early. */
    for (i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        fd[held] = dial(port);
        if (fd[held] >= 0) {
            (void)write_all(fd[held], "GET /metrics HTTP/1.1\r\n");
            held++;
        }
    }
    if (held < (int)MOQR_ADMIN_MAX_CLIENTS) {
        printf("  overflow: only %d of %u slots could be occupied\n", held,
               MOQR_ADMIN_MAX_CLIENTS);
        failures++;
    }
    /* Give the owner a bounded moment to accept them all. */
    for (i = 0; i < 200u && moqr_admin_listen_accepts(l) < (uint64_t)held;
         i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }

    /* The next connection gets the ONE bounded refusal slot: a fixed 503 with
     * no body, no generation and no bank. */
    {
        ssize_t n = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                             buf, sizeof(buf));
        if (n <= 0) {
            printf("  overflow: the refusal slot produced no response\n");
            failures++;
        } else {
            if (strstr(buf, "503") == NULL) {
                printf("  overflow: the overflow response was not 503\n");
                failures++;
            }
            if (strstr(buf, "Retry-After") == NULL) {
                printf("  overflow: the 503 carried no Retry-After\n");
                failures++;
            }
            if (strstr(buf, "OPENMETRICS serial=") != NULL ||
                strstr(buf, "PROM serial=") != NULL) {
                printf("  overflow: the 503 carried a metrics body\n");
                failures++;
            }
        }
        if (moqr_admin_listen_refusals(l) < 1u) {
            printf("  overflow: the refusal counter did not move\n");
            failures++;
        }
    }
    /*
     * The slot is ONE slot, and a turn that fills it must stop accepting.
     *
     * Several peers are queued back to back, so one accept turn sees more than
     * one waiting. The turn may take at most ONE of them -- into the refusal
     * slot -- and must leave the rest in the kernel backlog. A loop that does
     * not re-check ownership capacity after each accept takes them all and
     * closes the surplus silently: those peers never get an answer, in this
     * slot lifetime or any later one.
     *
     * So every queued peer must end up answered, one per slot lifetime.
     */
    {
        enum { QUEUED = 4 };
        int q[QUEUED];
        char qbuf[QUEUED][1024];
        ssize_t qn[QUEUED];
        int opened = 0;
        int blocker;

        /*
         * A real barrier. While BOTH ownership classes are full the listening
         * descriptor is not polled at all, so connections made now are
         * guaranteed to still be queued when the next accept turn runs -- and
         * that turn therefore sees several at once, which is the condition
         * under test. An idle peer holds the refusal slot until its deadline.
         */
        blocker = dial(port);
        for (int k = 0; k < 400 && moqr_admin_listen_refusals(l) < 2u; k++) {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
        if (blocker < 0 || moqr_admin_listen_refusals(l) < 2u) {
            printf("  overflow: the barrier connection never took the "
                   "refusal slot\n");
            failures++;
        }
        for (int k = 0; k < QUEUED; k++) {
            q[k] = dial(port);
            if (q[k] >= 0) {
                (void)write_all(q[k],
                                "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
                opened++;
            }
        }
        if (opened != QUEUED) {
            printf("  overflow: only %d of %d overflow peers connected\n",
                   opened, QUEUED);
            failures++;
        }
        /* Release the barrier: the slot frees on its own deadline, and the
         * next accept turn finds every queued peer waiting. */
        if (blocker >= 0) {
            (void)close(blocker);
        }
        for (int k = 0; k < QUEUED; k++) {
            qn[k] = q[k] >= 0 ? slurp(q[k], qbuf[k], sizeof(qbuf[k]), 12000)
                              : -1;
            if (q[k] >= 0) {
                (void)close(q[k]);
            }
            if (qn[k] <= 0 || strstr(qbuf[k], "503") == NULL) {
                printf("  overflow: queued peer %d was never answered — the "
                       "accept loop took ownership it could not serve\n", k);
                failures++;
            }
            if (qn[k] > 0 && (strstr(qbuf[k], "OPENMETRICS serial=") != NULL ||
                              strstr(qbuf[k], "PROM serial=") != NULL)) {
                printf("  overflow: a 503 carried a metrics body\n");
                failures++;
            }
        }
    }
    for (i = 0; i < (uint32_t)held; i++) {
        (void)close(fd[i]);
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * A poisoned generation, in each of the three demand shapes.
 *
 * The failure that matters here is not the status code -- it is what the
 * poison does to the BROKER. A poisoned generation whose token is consumed
 * without a durable carrier strands the broker slot forever, and every later
 * scrape and signal waits on an identity nothing will ever retire. So each
 * case asserts, after the poison: the broker is idle again, the admin carriers
 * are settled, and a following generation completes normally.
 */
static bool
wait_broker_idle(rig_t *r, int budget_ms)
{
    for (int i = 0; i < budget_ms; i++) {
        struct timespec ts = { 0, 1000 * 1000 };
        if (!moqr_broker_busy(&r->broker)) {
            return true;
        }
        (void)nanosleep(&ts, NULL);
    }
    return !moqr_broker_busy(&r->broker);
}

static int
poison_case(const char *what, bool http, bool sig)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[4096];
    ssize_t n = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  poison[%s]: start refused\n", what);
        return 1;
    }
    if (http && sig) {
        /*
         * A JOINED generation: both demands must reach the SAME collecting
         * serial before it is poisoned. Publication is withheld so the
         * generation stays COLLECTING while they join; only then is the
         * poison introduced. Setting it up front would poison the signal's
         * own generation before the scrape could join it, and the joined case
         * -- the one where a single retirement owes two sinks -- would never
         * be exercised.
         */
        /* Hold publication off long enough for both demands to join: a row
         * landing first would complete the generation before it could be
         * poisoned, and the joined case would silently become a success. */
        rig_hold_publication(&rig);
        atomic_store(&rig.latched, 1);
        moqr_admin_listen_notify(l);
        for (int i = 0; i < 400 && atomic_load(&rig.collects) < 1; i++) {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
    } else {
        atomic_store(&rig.poison, 1);
        if (sig) {
            atomic_store(&rig.latched, 1);
            moqr_admin_listen_notify(l);
        }
    }
    if (http) {
        if (sig) {
            /* Join the collecting generation, then poison it. */
            int fd = dial(moqr_admin_listen_port(l));
            if (fd < 0) {
                printf("  poison[%s]: could not connect\n", what);
                rig_stop(&rig, l);
                return failures + 1;
            }
            (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
            for (int i = 0; i < 200; i++) {
                struct timespec ts = { 0, 5 * 1000 * 1000 };
                (void)nanosleep(&ts, NULL);
                if (atomic_load(&rig.collects) >= 2) {
                    break;
                }
            }
            atomic_store(&rig.poison, 1);
            moqr_admin_listen_notify(l);
            n = slurp(fd, buf, sizeof(buf), 4000);
            (void)close(fd);
        } else {
            n = exchange(moqr_admin_listen_port(l),
                         "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                         sizeof(buf));
        }
        if (n <= 0) {
            printf("  poison[%s]: no response\n", what);
            failures++;
        } else {
            /* 500, not 503: the generation is broken, not late, and "try
             * again shortly" would be a false statement about it. */
            if (strstr(buf, "500") == NULL) {
                printf("  poison[%s]: not 500: %.40s\n", what, buf);
                failures++;
            }
            if (strstr(buf, "OPENMETRICS serial=") != NULL ||
                strstr(buf, "PROM serial=") != NULL) {
                printf("  poison[%s]: a 500 carried a metrics body\n", what);
                failures++;
            }
        }
    }
    if (atomic_load(&rig.renders) != 0) {
        printf("  poison[%s]: a poisoned generation was rendered\n", what);
        failures++;
    }
    /* THE defect this case exists for. */
    if (!wait_broker_idle(&rig, 3000)) {
        printf("  poison[%s]: the broker never became idle again — the "
               "poisoned generation stranded its slot\n", what);
        failures++;
    }
    /* A poisoned SIGNAL generation owes its sink an explicit diagnostic;
     * silence is indistinguishable from a generation that is merely late. */
    if (sig) {
        for (int i = 0; i < 300 && atomic_load(&rig.suppressions) == 0; i++) {
            struct timespec ts = { 0, 10 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
        if (atomic_load(&rig.suppressions) != 1) {
            printf("  poison[%s]: %d suppression diagnostics, expected 1\n",
                   what, atomic_load(&rig.suppressions));
            failures++;
        }
    } else if (atomic_load(&rig.suppressions) != 0) {
        printf("  poison[%s]: a diagnostic went to a sink that never asked\n",
               what);
        failures++;
    }
    if (atomic_load(&rig.emits) != 0) {
        printf("  poison[%s]: a poisoned generation emitted a document\n",
               what);
        failures++;
    }
    /* And the endpoint still works afterwards. */
    atomic_store(&rig.poison, 0);
    rig_release_publication(&rig);
    rig_arm_next_generation(&rig);
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL ||
        strstr(buf, "OPENMETRICS serial=") == NULL) {
        printf("  poison[%s]: the endpoint did not recover — a later scrape "
               "was not served\n", what);
        failures++;
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  poison[%s]: health %u after a recoverable poison\n", what,
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_poisoned_generations(void)
{
    int failures = 0;
    failures += poison_case("http-only", true, false);
    failures += poison_case("signal-only", false, true);
    failures += poison_case("signal+http", true, true);
    return failures;
}

/*
 * The refusal slot's deadline is a DEADLINE.
 *
 * A peer that takes the slot and then does nothing at all -- never reads,
 * never writes, never closes -- must not hold it against the next overflow
 * connection. Nothing here depends on the peer producing an event, which is
 * exactly the property under test.
 */
static int
test_refusal_deadline_is_a_deadline(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int held[MOQR_ADMIN_MAX_CLIENTS];
    int n_held = 0;
    int idle = -1;
    int port;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  deadline: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        held[n_held] = dial(port);
        if (held[n_held] >= 0) {
            (void)write_all(held[n_held], "GET /metrics HTTP/1.1\r\n");
            n_held++;
        }
    }
    for (int i = 0; i < 400 &&
                    moqr_admin_listen_accepts(l) < (uint64_t)n_held; i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    /* An overflow peer that says nothing and never reads. */
    idle = dial(port);
    if (idle < 0) {
        printf("  deadline: could not open the idle overflow connection\n");
        failures++;
    }
    for (int i = 0; i < 400 && moqr_admin_listen_refusals(l) < 1u; i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (moqr_admin_listen_refusals(l) < 1u) {
        printf("  deadline: the idle peer never took the refusal slot\n");
        failures++;
    }
    /* The slot must free itself on its own deadline, with no event from the
     * peer. The deadline is 2s; allow it, then require the NEXT overflow
     * connection to be served. */
    {
        char buf[1024];
        ssize_t got;
        struct timespec ts = { 2, 300 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
        got = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                       buf, sizeof(buf));
        if (got <= 0 || strstr(buf, "503") == NULL) {
            printf("  deadline: the slot was still held by a silent peer — "
                   "the next overflow connection got no answer\n");
            failures++;
        }
        if (moqr_admin_listen_refusals(l) < 2u) {
            printf("  deadline: the refusal slot was never reused\n");
            failures++;
        }
    }
    if (idle >= 0) {
        (void)close(idle);
    }
    for (int i = 0; i < n_held; i++) {
        (void)close(held[i]);
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * The listener is the final socket boundary and decides for itself.
 *
 * Every one of these reaches the constructor DIRECTLY, past the configuration
 * parser, which is the whole point: a parser is not a boundary.
 */
static int
test_address_and_port_boundary(void)
{
    int failures = 0;
    moqr_broker_t broker;
    rig_t rig;
    static const char *refused_hosts[] = {
        "0.0.0.0",                 /* IPv4 wildcard                       */
        "::",                      /* IPv6 wildcard                       */
        "192.0.2.10",              /* a public IPv4 address               */
        "2001:db8::1",             /* a public IPv6 address               */
        "::ffff:127.0.0.1",        /* a v4-mapped loopback address        */
        "::ffff:0.0.0.0",          /* a v4-mapped wildcard                */
        "127.0.0.1 ",              /* not a literal                       */
        "localhost",               /* a name, never resolved here         */
        "",                        /* nothing at all                      */
    };
    static const int refused_ports[] = { -1, -65535, 65536, 100000 };

    memset(&rig, 0, sizeof(rig));
    MOQ_TEST_CHECK(moqr_broker_init(&broker, MOQR_BROKER_BANKS) == MOQR_OK);

    for (size_t i = 0; i < sizeof(refused_hosts) / sizeof(refused_hosts[0]);
         i++) {
        moqr_cli_admin_t a;
        moqr_admin_listen_cfg_t c;
        moqr_admin_listen_t *l = NULL;
        memset(&a, 0, sizeof(a));
        a.enabled = true;
        a.mode = MOQR_CLI_ADMIN_TCP;
        (void)snprintf(a.host, sizeof(a.host), "%s", refused_hosts[i]);
        a.port = 0;
        memset(&c, 0, sizeof(c));
        c.admin = &a;
        c.lanes = 1;
        c.broker = &broker;
        c.wake_all = rig_wake_all;
        c.collect = rig_collect;
        c.render = rig_render;
        c.ctx = &rig;
        c.info = RIG_INFO;
        c.info_len = strlen(RIG_INFO);
        c.allow_ephemeral_port = true;
        if (moqr_admin_listen_start(&c, &l) == MOQR_OK) {
            printf("  boundary: the listener bound '%s'\n", refused_hosts[i]);
            (void)moqr_admin_listen_stop(l);
            moqr_admin_listen_destroy(l);
            failures++;
        }
        if (l != NULL) {
            printf("  boundary: '%s' left a handle behind\n",
                   refused_hosts[i]);
            failures++;
        }
    }
    for (size_t i = 0; i < sizeof(refused_ports) / sizeof(refused_ports[0]);
         i++) {
        moqr_cli_admin_t a;
        moqr_admin_listen_cfg_t c;
        moqr_admin_listen_t *l = NULL;
        memset(&a, 0, sizeof(a));
        a.enabled = true;
        a.mode = MOQR_CLI_ADMIN_TCP;
        (void)snprintf(a.host, sizeof(a.host), "127.0.0.1");
        a.port = refused_ports[i];
        memset(&c, 0, sizeof(c));
        c.admin = &a;
        c.lanes = 1;
        c.broker = &broker;
        c.wake_all = rig_wake_all;
        c.collect = rig_collect;
        c.render = rig_render;
        c.ctx = &rig;
        c.info = RIG_INFO;
        c.info_len = strlen(RIG_INFO);
        c.allow_ephemeral_port = true;
        if (moqr_admin_listen_start(&c, &l) == MOQR_OK) {
            printf("  boundary: the listener accepted port %d\n",
                   refused_ports[i]);
            (void)moqr_admin_listen_stop(l);
            moqr_admin_listen_destroy(l);
            failures++;
        }
    }
    /* And the one address family that IS allowed, so the rule is not simply
     * "refuse everything". */
    {
        moqr_cli_admin_t a;
        moqr_admin_listen_cfg_t c;
        moqr_admin_listen_t *l = NULL;
        memset(&a, 0, sizeof(a));
        a.enabled = true;
        a.mode = MOQR_CLI_ADMIN_TCP;
        (void)snprintf(a.host, sizeof(a.host), "127.0.0.1");
        a.port = 0;
        memset(&c, 0, sizeof(c));
        c.admin = &a;
        c.lanes = 1;
        c.broker = &broker;
        c.wake_all = rig_wake_all;
        c.collect = rig_collect;
        c.render = rig_render;
        c.ctx = &rig;
        c.info = RIG_INFO;
        c.info_len = strlen(RIG_INFO);
        c.allow_ephemeral_port = true;
        if (moqr_admin_listen_start(&c, &l) != MOQR_OK) {
            printf("  boundary: a loopback address in 127/8 was refused\n");
            failures++;
        } else {
            (void)moqr_admin_listen_stop(l);
            moqr_admin_listen_destroy(l);
        }
    }
    moqr_broker_destroy(&broker);
    return failures;
}

/*
 * The production publication sequence, end to end.
 *
 * The owner asks the lanes to publish, a lane publishes LATER on another
 * thread and notifies at that point, and the owner is woken by that
 * notification. The poll timeout is a safety bound, not the mechanism: the
 * delay here is well inside one poll period so a run that only works because
 * of the timeout is not mistaken for one that works because of the
 * notification.
 */
static int
test_publication_notification(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[4096];
    ssize_t n;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  publish: start refused\n");
        return 1;
    }
    /* PUBLISH AFTER WAIT: the owner is already waiting when the row lands. */
    atomic_store(&rig.publish_delay_ms, 8);
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL) {
        printf("  publish: a deferred publication never completed\n");
        failures++;
    }
    if (atomic_load(&rig.notifies) < 1) {
        printf("  publish: the lane never notified at its publish point\n");
        failures++;
    }
    if (atomic_load(&rig.renders) != 1) {
        printf("  publish: rendered %d times, expected exactly 1\n",
               atomic_load(&rig.renders));
        failures++;
    }

    /* PUBLISH BEFORE WAIT: the row and its notification land before the owner
     * looks. A level-triggered notification is still pending; an edge that was
     * consumed would strand this generation until the timeout. */
    rig_arm_next_generation(&rig);
    rig_release_publication(&rig);
    rig_ask(&rig);                    /* as if the wake had already run */
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL) {
        printf("  publish: a publication landing before the wait was lost\n");
        failures++;
    }

    /* COALESCED: many notifications for one generation are not an error and
     * do not produce more than one document. */
    rig_arm_next_generation(&rig);
    rig_release_publication(&rig);
    for (int i = 0; i < 16; i++) {
        moqr_admin_listen_notify(l);
    }
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL) {
        printf("  publish: coalesced notifications lost a generation\n");
        failures++;
    }
    if (atomic_load(&rig.renders) != 3) {
        printf("  publish: %d renders for three generations\n",
               atomic_load(&rig.renders));
        failures++;
    }


    /*
     * THE NOTIFICATION IS THE MECHANISM, not the poll timeout.
     *
     * The safety bound is removed, so nothing but a notification can advance
     * the owner. With the lane's notify in place the generation completes; the
     * decisive mutant -- dropping it from the publish point -- leaves the
     * scrape unanswered, and no verdict here depends on how long anything
     * takes.
     */
    moqr_admin_listen_test_set_poll_ms(-1);
    rig_arm_next_generation(&rig);
    atomic_store(&rig.publish_delay_ms, 4);
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strstr(buf, "200 OK") == NULL) {
        printf("  publish: with no poll timeout, the publication "
               "notification did not advance the owner\n");
        failures++;
    }
    moqr_admin_listen_test_set_poll_ms(MOQR_ADMIN_POLL_MS);

    /* AFTER SHUTDOWN: a lane may still publish while the facades are being
     * joined. Notifying a stopped endpoint is defined, not a crash. */
    rig_stop_publisher(&rig);
    if (moqr_admin_listen_stop(l) != MOQR_OK) {
        printf("  publish: the owner did not stop cleanly\n");
        failures++;
    }
    moqr_admin_listen_notify(l);   /* must be safe after the join */
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/* -- concurrent scrapes, for the sanitizers ------------------------------- */

#define SCRAPERS 4
#define SCRAPES  6

typedef struct scraper {
    pthread_t t;
    int       port;
    int       ok;
} scraper_t;

static void *
scraper_main(void *arg)
{
    scraper_t *s = (scraper_t *)arg;
    char buf[8192];
    for (int i = 0; i < SCRAPES; i++) {
        ssize_t n = exchange(s->port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                             buf, sizeof(buf));
        /* Any response from the finite table is acceptable under contention;
         * what must never happen is silence, a truncated head, or a body on an
         * error. */
        if (n <= 0 || strncmp(buf, "HTTP/1.1 ", 9) != 0) {
            return NULL;
        }
        if (strstr(buf, "200 OK") == NULL &&
            (strstr(buf, "OPENMETRICS serial=") != NULL ||
             strstr(buf, "PROM serial=") != NULL)) {
            return NULL;
        }
        s->ok++;
    }
    return NULL;
}

/*
 * The owner's named terminals.
 *
 * A clock that stops and a poll that fails are both reachable in production and
 * neither can be provoked from outside, so they are injected. What is under
 * test is not that the owner dies -- it is that it dies NAMED, releases its
 * listening socket, and can be observed as dead by the serve loop instead of
 * quietly continuing to advertise an endpoint nobody serves.
 */
static int
owner_terminal_case(const char *what, void (*inject)(int),
                    moqr_admin_listen_health_t want)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int port;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  terminal[%s]: start refused\n", what);
        inject(0);
        return 1;
    }
    port = moqr_admin_listen_port(l);
    inject(1);
    moqr_admin_listen_notify(l);   /* make it take one more turn */
    for (i = 0; i < 400 && !moqr_admin_listen_owner_exited(l); i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    inject(0);
    if (i >= 400) {
        printf("  terminal[%s]: the owner never exited\n", what);
        failures++;
    }
    if (moqr_admin_listen_health(l) != want) {
        printf("  terminal[%s]: health %u, expected %u\n", what,
               (unsigned)moqr_admin_listen_health(l), (unsigned)want);
        failures++;
    }
    /*
     * A dead owner must not leave a listening socket behind. Accepting
     * connections nobody will ever answer is worse than refusing them: a
     * scraper sees a healthy TCP handshake and a hang, instead of an
     * immediate, diagnosable refusal.
     */
    {
        int fd = dial(port);
        if (fd >= 0) {
            char buf[512];
            (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
            (void)slurp(fd, buf, sizeof(buf), 500);
            (void)close(fd);
            printf("  terminal[%s]: the listening socket outlived the owner — "
                   "a dead endpoint still accepts connections\n", what);
            failures++;
        }
    }
    /* And the caller is told it did not stop because it was asked to. */
    rig_stop_publisher(&rig);
    if (moqr_admin_listen_stop(l) == MOQR_OK) {
        printf("  terminal[%s]: stop reported a clean shutdown\n", what);
        failures++;
    }
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

static int
test_owner_terminals(void)
{
    int failures = 0;
    failures += owner_terminal_case("clock", moqr_admin_listen_test_fail_clock,
                                    MOQR_ADMIN_HEALTH_NO_CLOCK);
    failures += owner_terminal_case("poll", moqr_admin_listen_test_fail_poll,
                                    MOQR_ADMIN_HEALTH_POLL_FAILED);
    return failures;
}

/*
 * A lane wake that cannot reach every lane is a FAILED wake: the generation's
 * one notification did not arrive, so the epoch can never complete. Reporting
 * it as successful would turn that into an indefinite silent stall.
 */
static int
test_failed_wake_is_terminal(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  wakefail: start refused\n");
        return 1;
    }
    atomic_store(&rig.wake_fails, 1);
    atomic_store(&rig.latched, 1);
    moqr_admin_listen_notify(l);
    for (i = 0; i < 400 && !moqr_admin_listen_owner_exited(l); i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (i >= 400) {
        printf("  wakefail: a failed lane wake did not stop the owner\n");
        failures++;
    } else if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_WAKE_FAILED) {
        printf("  wakefail: health %u, expected WAKE_FAILED\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/* Broker CAPACITY is serial-space exhaustion, not ordinary backpressure. Once
 * it is returned, no later generation can ever open; an endpoint that remains
 * healthy would answer every future scrape 503 forever. */
static int
test_http_serial_exhaustion_is_terminal(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    uint64_t serial = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    bool wake = false;
    int fd;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  serial-capacity: start refused\n");
        return 1;
    }
    /* Spend UINT64_MAX and retire it directly, leaving an idle but permanently
     * exhausted broker for the owner's next HTTP request. */
    moqr_broker_test_seed_serial(&rig.broker, UINT64_MAX);
    if (moqr_broker_request(&rig.broker, MOQR_BROKER_DEMAND_HTTP, &serial,
                            &wake) != MOQR_OK || serial != UINT64_MAX) {
        printf("  serial-capacity: could not spend the final serial\n");
        rig_stop(&rig, l);
        return 1;
    }
    moqr_broker_on_complete(&rig.broker, serial);
    if (!moqr_broker_take_serial(&rig.broker, serial, &demand, &bank) ||
        moqr_broker_release(&rig.broker, serial, bank, &wake) != MOQR_OK) {
        printf("  serial-capacity: could not retire the final serial\n");
        rig_stop(&rig, l);
        return 1;
    }

    fd = dial(moqr_admin_listen_port(l));
    if (fd < 0) {
        printf("  serial-capacity: could not connect\n");
        rig_stop(&rig, l);
        return 1;
    }
    (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    for (i = 0; i < 800 && !moqr_admin_listen_owner_exited(l); i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (i >= 800) {
        printf("  serial-capacity: HTTP exhaustion left the endpoint serving\n");
        failures++;
    } else if (moqr_admin_listen_health(l) !=
               MOQR_ADMIN_HEALTH_SERIAL_EXHAUSTED) {
        printf("  serial-capacity: health %u, expected SERIAL_EXHAUSTED\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    if (atomic_load(&rig.wakes) != 0) {
        printf("  serial-capacity: exhausted demand woke a lane\n");
        failures++;
    }
    (void)close(fd);
    rig_stop(&rig, l);
    return failures;
}

/*
 * A peer that half-closes its write side after sending a complete request is
 * reported as POLLIN|POLLHUP. Treating the hangup as a drop would throw away a
 * request that arrived in full.
 */
static int
test_half_close_still_answered(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int fd;
    char buf[4096];

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  halfclose: start refused\n");
        return 1;
    }
    fd = dial(moqr_admin_listen_port(l));
    if (fd < 0) {
        printf("  halfclose: could not connect\n");
        rig_stop(&rig, l);
        return 1;
    }
    (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    /* The request is complete, and the peer has nothing more to say. */
    (void)shutdown(fd, SHUT_WR);
    if (slurp(fd, buf, sizeof(buf), 4000) <= 0 ||
        strstr(buf, "200 OK") == NULL ||
        strstr(buf, "OPENMETRICS serial=") == NULL) {
        printf("  halfclose: a complete request from a half-closed peer was "
               "not answered\n");
        failures++;
    }
    (void)close(fd);
    rig_stop(&rig, l);
    return failures;
}

/*
 * A scraper must not be able to terminate the relay.
 *
 * This runs in a CHILD PROCESS with the DEFAULT SIGPIPE disposition, because
 * the production relay has the default disposition: a test that ignores
 * SIGPIPE process-wide masks exactly the failure it claims to check. The peer
 * connects, sends a request, and closes before the response can be written.
 * The owner must survive that and retire the request.
 */
static int
test_sigpipe_cannot_kill_the_relay(void)
{
    int failures = 0;
    int pipefd[2];
    pid_t pid;

    if (pipe(pipefd) != 0) {
        printf("  sigpipe: no pipe\n");
        return 1;
    }
    pid = fork();
    if (pid < 0) {
        printf("  sigpipe: no fork\n");
        (void)close(pipefd[0]);
        (void)close(pipefd[1]);
        return 1;
    }
    if (pid == 0) {
        /* The child. DEFAULT SIGPIPE disposition, deliberately. */
        rig_t rig;
        moqr_admin_listen_t *l = NULL;
        char verdict = 'F';
        int port;

        (void)close(pipefd[0]);
        (void)signal(SIGPIPE, SIG_DFL);
        if (rig_start(&rig, &l) != MOQR_OK) {
            (void)write(pipefd[1], &verdict, 1);
            _exit(2);
        }
        port = moqr_admin_listen_port(l);
        /*
         * Peers that vanish part way through their own response.
         *
         * The body is far larger than one write chunk, so the owner needs many
         * writes to deliver it. Each peer reads a little, then closes in an
         * ORDERLY way -- which is what eventually turns a later write into
         * EPIPE, and EPIPE is what raises SIGPIPE. A peer that merely resets
         * produces ECONNRESET, which does not.
         */
        atomic_store(&rig.big_body, 1);
        for (int i = 0; i < 6; i++) {
            int fd = dial(port);
            struct timespec ts = { 0, 150 * 1000 * 1000 };
            char sip[256];
            if (fd < 0) {
                continue;
            }
            (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
            /* Take a little of the response, then go away mid-stream. */
            {
                struct pollfd pf;
                pf.fd = fd;
                pf.events = POLLIN;
                pf.revents = 0;
                if (poll(&pf, 1, 2000) > 0 && (pf.revents & POLLIN) != 0) {
                    (void)read(fd, sip, sizeof(sip));
                }
            }
            if ((i & 1) == 0) {
                /* Half of them are RESET rather than closed politely. A reset
                 * peer is reported as a hangup, not as writable, so a loop
                 * that only writes on POLLOUT never finishes with it -- and
                 * its bank, and the broker slot behind it, are held for the
                 * life of the process. */
                struct linger lg;
                lg.l_onoff = 1;
                lg.l_linger = 0;
                (void)setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
            }
            (void)close(fd);
            (void)nanosleep(&ts, NULL);
        }
        atomic_store(&rig.big_body, 0);
        /*
         * And every one of those banks must have come back. A peer that
         * vanishes mid-response is reported as a hangup rather than as
         * writable, so a loop that only writes on POLLOUT would never finish
         * with it -- holding its bank, and with it a broker slot, for the life
         * of the process. Two such peers are a permanent /metrics outage.
         */
        if (!wait_broker_idle(&rig, 4000)) {
            (void)write(pipefd[1], &verdict, 1);
            _exit(4);
        }
        /* Still alive, and still serving. */
        {
            char buf[4096];
            ssize_t got;
            rig_arm_next_generation(&rig);
            got = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                           buf, sizeof(buf));
            if (got > 0 && strstr(buf, "200 OK") != NULL) {
                verdict = 'P';
            }
        }
        rig_stop(&rig, l);
        (void)write(pipefd[1], &verdict, 1);
        (void)close(pipefd[1]);
        _exit(verdict == 'P' ? 0 : 3);
    }
    /* The parent: the child must exit normally, not on a signal. */
    (void)close(pipefd[1]);
    {
        char verdict = 0;
        int status = 0;
        ssize_t got = read(pipefd[0], &verdict, 1);
        (void)close(pipefd[0]);
        if (waitpid(pid, &status, 0) != pid) {
            printf("  sigpipe: could not reap the child\n");
            failures++;
        } else if (WIFSIGNALED(status)) {
            printf("  sigpipe: the relay process was killed by signal %d — a "
                   "scraper terminated it\n", WTERMSIG(status));
            failures++;
        } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            printf("  sigpipe: the child exited %d\n",
                   WIFEXITED(status) ? WEXITSTATUS(status) : -1);
            failures++;
        }
        if (got != 1 || verdict != 'P') {
            printf("  sigpipe: the endpoint did not survive and keep "
                   "serving\n");
            failures++;
        }
    }
    return failures;
}

/*
 * A refused state-machine transition is an INVARIANT FAILURE, not a retry.
 *
 * Each of these is unreachable while the broker and the state machine agree
 * about who owns what, which is precisely why they are injected. What must not
 * happen is the endpoint quietly continuing to serve while the two sides
 * disagree about a bank or a token.
 */
static int
transition_failure_case(const char *what, int which)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[4096];
    int held_fd = -1;
    uint64_t held_serial[MOQR_BROKER_BANKS] = { 0 };
    uint32_t held_bank[MOQR_BROKER_BANKS] = { 0 };
    int held_tokens = 0;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  transition[%s]: start refused\n", what);
        return 1;
    }
    moqr_admin_listen_test_fail_transition(which);
    if (which == MOQR_ADMIN_TEST_FAIL_SETTLE ||
        which == MOQR_ADMIN_TEST_FAIL_SETTLE_SILENT ||
        which == MOQR_ADMIN_TEST_FAIL_SETTLE_RELEASE ||
        which == MOQR_ADMIN_TEST_FAIL_RECORD_TAKE) {
        /* Settlement belongs to a generation ABANDONED before anything was
         * rendered, so the client has to leave while its epoch is still
         * collecting. Publication is held off to make that window certain. */
        int fd;
        rig_hold_publication(&rig);
        fd = dial(moqr_admin_listen_port(l));
        if (fd < 0) {
            printf("  transition[%s]: could not connect\n", what);
            rig_stop(&rig, l);
            return 1;
        }
        (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        for (i = 0; i < 200 && atomic_load(&rig.collects) < 1; i++) {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
        /* The only waiter leaves, and publication stays held for the rest of
         * the case: a row landing now would render the generation into a bank,
         * and it would retire through the release path instead of the
         * abandonment path this case exists to reach. */
        (void)close(fd);
    } else if (which == MOQR_ADMIN_TEST_FAIL_REFUSE) {
        bool wake = false;
        /* Hold both broker banks in SENDING, so the client's HTTP request gets
         * the documented WOULD_BLOCK result and must traverse admin_refuse. */
        for (uint32_t j = 0; j < MOQR_BROKER_BANKS; j++) {
            uint32_t demand = 0;
            if (moqr_broker_request(&rig.broker, MOQR_BROKER_DEMAND_HTTP,
                                    &held_serial[j], &wake) != MOQR_OK) {
                break;
            }
            moqr_broker_on_complete(&rig.broker, held_serial[j]);
            if (!moqr_broker_take_serial(&rig.broker, held_serial[j],
                                         &demand, &held_bank[j])) {
                break;
            }
            held_tokens++;
        }
        if (held_tokens != (int)MOQR_BROKER_BANKS) {
            printf("  transition[%s]: could not occupy both broker banks\n",
                   what);
            moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_NONE);
            rig_stop(&rig, l);
            return 1;
        }
        (void)exchange(moqr_admin_listen_port(l),
                       "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                       sizeof(buf));
    } else if (which == MOQR_ADMIN_TEST_FAIL_ABORT) {
        /* A signal-only generation has no HTTP reader and closes through
         * admin_abort after rendering. */
        atomic_store(&rig.latched, 1);
        moqr_admin_listen_notify(l);
    } else if (which == MOQR_ADMIN_TEST_FAIL_TICK) {
        /* No protocol state is required: every owner turn ends in tick. */
        moqr_admin_listen_notify(l);
    } else if (which == MOQR_ADMIN_TEST_FAIL_ON_BYTES) {
        /* First establish a READING client while there is no signal demand.
         * Only then stage the latch and bytes together. Their readiness wakes
         * the next owner turn, so a failed read transition and the generation
         * handshake are ordered within that one turn. */
        held_fd = dial(moqr_admin_listen_port(l));
        if (held_fd < 0) {
            printf("  transition[%s]: could not connect\n", what);
            rig_stop(&rig, l);
            return 1;
        }
        for (i = 0; i < 400 && moqr_admin_listen_accepts(l) < 1u; i++) {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
        if (i >= 400) {
            printf("  transition[%s]: client was not accepted\n", what);
            (void)close(held_fd);
            rig_stop(&rig, l);
            return 1;
        }
        moqr_admin_listen_test_on_bytes_failure_hook(rig_latch_signal, &rig);
        (void)write_all(held_fd,
                        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    } else {
        /* One whole generation, which is what reaches the other two. */
        (void)exchange(moqr_admin_listen_port(l),
                       "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                       sizeof(buf));
    }
    /*
     * A waiter that disconnects is not noticed until its epoch deadline: a
     * WAITING client has nothing to read and nothing to write, so it is not in
     * the poll set at all. The bound here therefore has to outlast that
     * deadline, not just a few turns.
     */
    for (i = 0; i < 1600 && !moqr_admin_listen_owner_exited(l); i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_NONE);
    moqr_admin_listen_test_on_bytes_failure_hook(NULL, NULL);
    if (moqr_admin_listen_test_transition_hit() != which) {
        printf("  transition[%s]: exact seam was not reached (hit=%d)\n",
               what, moqr_admin_listen_test_transition_hit());
        failures++;
    }
    if (i >= 1600) {
        printf("  transition[%s]: a refused transition did not stop the "
               "endpoint — it kept serving with the two sides disagreeing\n",
               what);
        failures++;
    } else if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_INVARIANT) {
        printf("  transition[%s]: health %u, expected INVARIANT\n", what,
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    if (which == MOQR_ADMIN_TEST_FAIL_ON_BYTES) {
        if (atomic_load(&rig.wakes) != 0 ||
            atomic_load(&rig.collects) != 0) {
            printf("  transition[%s]: owner failure opened or woke a "
                   "generation later in the same turn (wakes=%d collects=%d "
                   "renders=%d)\n", what, atomic_load(&rig.wakes),
                   atomic_load(&rig.collects), atomic_load(&rig.renders));
            failures++;
        }
        if (moqr_broker_busy(&rig.broker)) {
            printf("  transition[%s]: owner failure left a broker "
                   "generation outstanding\n", what);
            failures++;
        }
    }
    if (held_fd >= 0) {
        (void)close(held_fd);
    }
    for (int j = 0; j < held_tokens; j++) {
        bool wake = false;
        if (moqr_broker_release(&rig.broker, held_serial[j], held_bank[j],
                                &wake) != MOQR_OK) {
            printf("  transition[%s]: could not release held broker token\n",
                   what);
            failures++;
        }
    }
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * A join that cannot prove the owner dead is not a clean shutdown, and the
 * owner's reach is much larger than this object.
 *
 * The owner is PARKED -- genuinely still running its loop, still reading the
 * broker and the callback context -- and the stop then has to say so rather
 * than pretend. What must not happen is any part of the dependency closure
 * being released while that thread can still touch it: not the listener, not
 * the broker, not the rig the callbacks reach. Under ASan, releasing any of
 * them here would be a use-after-free rather than a quiet corruption.
 */
static int
test_unprovable_join_refuses_to_release_anything(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int collects_before;
    int collects_after;
    moqr_result_t rc;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  join: start refused\n");
        return 1;
    }
    /* The owner will not honour the stop request. */
    moqr_admin_listen_test_park_owner(1);

    rc = moqr_admin_listen_stop(l);
    if (rc != MOQR_ERR_WOULD_BLOCK) {
        printf("  join: a live owner's stop returned %d, expected "
               "WOULD_BLOCK\n", (int)rc);
        failures++;
    }
    if (!moqr_admin_listen_owner_unjoinable(l)) {
        printf("  join: the endpoint does not report an unprovable owner\n");
        failures++;
    }
    if (moqr_admin_listen_owner_exited(l)) {
        printf("  join: the owner is reported dead while it is still "
               "running\n");
        failures++;
    }
    /*
     * Destroy must release NOTHING. The proof is that the still-running owner
     * keeps reaching its context afterwards: if the object or anything it
     * points at had been freed, this is where ASan sees it.
     */
    collects_before = moqr_admin_listen_test_turns();
    moqr_admin_listen_destroy(l);
    /* Liveness is observed as loop iterations, not as served work: a parked
     * owner has stopped SERVING, which is exactly the state a caller must
     * reason about, but it is still executing and still reaching this object. */
    for (int i = 0; i < 800 &&
                    moqr_admin_listen_test_turns() == collects_before; i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    collects_after = moqr_admin_listen_test_turns();
    if (collects_after == collects_before) {
        printf("  join: the parked owner stopped running, so this case "
               "proves nothing about a live one\n");
        failures++;
    }
    if (moqr_admin_listen_port(l) <= 0) {
        printf("  join: the endpoint was released while its owner may run\n");
        failures++;
    }

    /* Release the park solely to reap the fixture, and only then tear down. */
    moqr_admin_listen_test_park_owner(0);
    if (moqr_admin_listen_stop(l) == MOQR_ERR_WOULD_BLOCK) {
        printf("  join: the owner never became joinable\n");
        failures++;
    }
    rig_stop_publisher(&rig);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * The same unprovable-owner rule, reached through ACTIVATION rather than
 * through an ordinary stop.
 *
 * A failed activation has still started a thread. Collapsing that into a plain
 * "activation failed" would send the caller into its ordinary cleanup and free
 * the snapshot, broker and callback context a live owner still holds -- the
 * very use-after-free the stop path exists to prevent, reached through the
 * startup path instead.
 */
static int
test_activation_failure_reports_an_unprovable_owner(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_result_t rc;
    int collects_before;

    /* The owner fails its first turn AND does not leave. */
    moqr_admin_listen_test_park_owner(1);
    moqr_admin_listen_test_fail_clock(1);
    rc = rig_start(&rig, &l);
    moqr_admin_listen_test_fail_clock(0);

    if (rc != MOQR_ERR_WOULD_BLOCK) {
        printf("  activate: a failed activation over a live owner returned "
               "%d, expected WOULD_BLOCK\n", (int)rc);
        failures++;
        moqr_admin_listen_test_park_owner(0);
        if (rc == MOQR_OK) {
            rig_stop(&rig, l);
        }
        return failures;
    }
    if (l == NULL) {
        printf("  activate: the endpoint was discarded although its owner "
               "may still run\n");
        failures++;
        moqr_admin_listen_test_park_owner(0);
        return failures;
    }
    if (!moqr_admin_listen_owner_unjoinable(l)) {
        printf("  activate: the endpoint does not report an unprovable "
               "owner\n");
        failures++;
    }
    /* Nothing may have been released: the still-running owner keeps reaching
     * its own storage, which is where ASan would see it if anything had. */
    collects_before = atomic_load(&rig.collects);
    (void)collects_before;
    if (moqr_admin_listen_port(l) <= 0) {
        printf("  activate: the endpoint was released while its owner may "
               "run\n");
        failures++;
    }

    /* Release the park solely to reap the fixture. */
    moqr_admin_listen_test_park_owner(0);
    if (moqr_admin_listen_stop(l) == MOQR_ERR_WOULD_BLOCK) {
        printf("  activate: the owner never became joinable\n");
        failures++;
    }
    moqr_admin_listen_destroy(l);
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * A refused generation-wide failure while an HTTP waiter exists.
 *
 * The collect-failure path has exactly ONE expected refusal: a generation with
 * no HTTP demand has no client and no record, so the state machine does not
 * know the identity. A refusal with a waiter present means that waiter is owed
 * a response, and retiring the broker token as though it had been answered
 * would settle a generation behind its back.
 *
 * The owner is PARKED so its exit sequence cannot run and settle things after
 * the fact: what is observed is the token the poison path itself did or did
 * not consume. No verdict here depends on elapsed time.
 */
static int
test_fail_serial_refusal_with_a_waiter_is_invariant(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int fd;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  failserial: start refused\n");
        return 1;
    }
    atomic_store(&rig.poison_once, 1);
    atomic_store(&rig.poison, 1);
    moqr_admin_listen_test_park_owner(1);
    moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_FAIL_SERIAL);

    fd = dial(moqr_admin_listen_port(l));
    if (fd < 0) {
        printf("  failserial: could not connect\n");
        moqr_admin_listen_test_park_owner(0);
        moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_NONE);
        rig_stop(&rig, l);
        return 1;
    }
    (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    for (i = 0; i < 800 &&
                moqr_admin_listen_health(l) == MOQR_ADMIN_HEALTH_OK; i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_NONE);
    if (moqr_admin_listen_test_transition_hit() !=
        MOQR_ADMIN_TEST_FAIL_FAIL_SERIAL) {
        printf("  failserial: exact seam was not reached (hit=%d)\n",
               moqr_admin_listen_test_transition_hit());
        failures++;
    }
    if (i >= 800) {
        printf("  failserial: a refused generation failure with an HTTP "
               "waiter did not stop the endpoint\n");
        failures++;
    } else if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_INVARIANT) {
        printf("  failserial: health %u, expected INVARIANT\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    /*
     * THE property: the generation is still the broker's. A path that treated
     * the refusal as "no carrier" would have taken and released this token,
     * retiring a generation whose waiter was never answered.
     */
    if (!moqr_broker_busy(&rig.broker)) {
        printf("  failserial: the generation was retired although its HTTP "
               "waiter was never settled\n");
        failures++;
    }
    (void)close(fd);
    moqr_admin_listen_test_park_owner(0);
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

static int
test_refused_transitions_are_invariant_failures(void)
{
    int failures = 0;
    failures += transition_failure_case("ack_release",
                                        MOQR_ADMIN_TEST_FAIL_ACK_RELEASE);
    failures += transition_failure_case("reserve",
                                        MOQR_ADMIN_TEST_FAIL_RESERVE);
    failures += transition_failure_case("settle",
                                        MOQR_ADMIN_TEST_FAIL_SETTLE);
    failures += transition_failure_case("bank_release",
                                        MOQR_ADMIN_TEST_FAIL_BANK_RELEASE);
    /* The other two destructive releases, each at its own exact call. */
    failures += transition_failure_case("settle_release",
                                        MOQR_ADMIN_TEST_FAIL_SETTLE_RELEASE);
    failures += transition_failure_case("settle-silent",
                                        MOQR_ADMIN_TEST_FAIL_SETTLE_SILENT);
    failures += transition_failure_case("close",
                                        MOQR_ADMIN_TEST_FAIL_CLOSE);
    failures += transition_failure_case("on_bytes",
                                        MOQR_ADMIN_TEST_FAIL_ON_BYTES);
    failures += transition_failure_case("take_ready",
                                        MOQR_ADMIN_TEST_FAIL_TAKE_READY);
    failures += transition_failure_case("accept",
                                        MOQR_ADMIN_TEST_FAIL_ACCEPT);
    failures += transition_failure_case("bind_serial",
                                        MOQR_ADMIN_TEST_FAIL_BIND_SERIAL);
    failures += transition_failure_case("refuse",
                                        MOQR_ADMIN_TEST_FAIL_REFUSE);
    failures += transition_failure_case("on_written",
                                        MOQR_ADMIN_TEST_FAIL_ON_WRITTEN);
    failures += transition_failure_case("tick", MOQR_ADMIN_TEST_FAIL_TICK);
    failures += transition_failure_case("abort", MOQR_ADMIN_TEST_FAIL_ABORT);
    failures += transition_failure_case("commit",
                                        MOQR_ADMIN_TEST_FAIL_COMMIT);
    failures += transition_failure_case("record_take",
                                        MOQR_ADMIN_TEST_FAIL_RECORD_TAKE);
    failures += transition_failure_case("broker_request",
                                        MOQR_ADMIN_TEST_FAIL_BROKER_REQUEST);
    return failures;
}

/* -- the refusal slot's I/O results ---------------------------------------
 *
 * A fixed 503 on a loopback socket fits in one write, so the kernel here will
 * never produce a short write, an EAGAIN, an EINTR or a failed shutdown. Those
 * results are the property under test, not the kernel's behaviour, so they are
 * supplied directly and consumed one call at a time. Nothing below depends on
 * timing.
 */
static int
refusal_io_case(const char *what, const int *script, int n, int fail_shutdown,
                bool expect_answer)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int held[MOQR_ADMIN_MAX_CLIENTS];
    int n_held = 0;
    int port;
    char buf[2048];
    ssize_t got;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  refusal[%s]: start refused\n", what);
        return 1;
    }
    port = moqr_admin_listen_port(l);
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        held[n_held] = dial(port);
        if (held[n_held] >= 0) {
            (void)write_all(held[n_held], "GET /metrics HTTP/1.1\r\n");
            n_held++;
        }
    }
    for (int i = 0; i < 400 &&
                    moqr_admin_listen_accepts(l) < (uint64_t)n_held; i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    moqr_admin_listen_test_reset_read_calls();
    moqr_admin_listen_test_script_write(script, n);
    moqr_admin_listen_test_fail_shutdown(fail_shutdown);

    got = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                   sizeof(buf));
    moqr_admin_listen_test_script_write(NULL, 0);
    moqr_admin_listen_test_fail_shutdown(0);

    if (expect_answer) {
        if (got <= 0 || strstr(buf, "503") == NULL) {
            printf("  refusal[%s]: the 503 was not delivered\n", what);
            failures++;
        } else if (strstr(buf, "Retry-After") == NULL) {
            printf("  refusal[%s]: the 503 was truncated — the write was not "
                   "resumed from its exact offset\n", what);
            failures++;
        }
        /* One bounded read per draining turn, never a loop that empties the
         * socket in one go. */
        if (moqr_admin_listen_test_read_calls() > 1) {
            printf("  refusal[%s]: %d drain reads in one slot lifetime, "
                   "expected at most one per turn\n", what,
                   moqr_admin_listen_test_read_calls());
            failures++;
        }
    } else {
        /*
         * A failed half-close is about the SLOT, not about the peer: the bytes
         * already written are already on the wire, so whether this client sees
         * them is not the property. What must hold is that the slot is
         * released at once instead of draining a conversation that cannot be
         * ended cleanly -- which the reuse check below is what proves.
         */
        (void)got;
    }
    /* The slot must be reusable afterwards, whatever happened to the first
     * peer: one bad refusal cannot cost every later one. */
    got = exchange(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                   sizeof(buf));
    if (got <= 0 || strstr(buf, "503") == NULL) {
        printf("  refusal[%s]: the slot was not reusable afterwards\n", what);
        failures++;
    }
    for (int i = 0; i < n_held; i++) {
        (void)close(held[i]);
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_refusal_io_results(void)
{
    int failures = 0;
    /* A positive short write, resumed from the exact offset. */
    static const int short_write[] = { 40, 40, 40, 4096 };
    /* No progress at all, then completion: the offset must not move. */
    static const int eagain_then[] = { 0, 0, 4096 };
    static const int eintr_then[]  = { -1, -1, 4096 };
    /* A hard error: the peer is gone and the slot is released. */
    static const int hard_error[]  = { -2 };

    failures += refusal_io_case("short-write", short_write, 4, 0, true);
    failures += refusal_io_case("eagain", eagain_then, 3, 0, true);
    failures += refusal_io_case("eintr", eintr_then, 3, 0, true);
    failures += refusal_io_case("hard-error", hard_error, 1, 0, false);
    failures += refusal_io_case("shutdown-fails", NULL, 0, 1, false);
    return failures;
}

/* -- terminality with every in-flight state ------------------------------
 *
 * Cancellation obligations are PER STATE, so this case is only worth anything
 * if those states demonstrably exist at the moment terminality arrives. Each
 * one is built through the real listener and then OBSERVED through the owner's
 * own structure -- not inferred from an accept count, and never from a sleep.
 */

/* The six named states, in one observation. */
static bool
term_states_present(const moqr_admin_listen_view_t *v, const char **missing)
{
    int reading = 0, waiting = 0, writer_start = 0, writer_part = 0;
    int shared_bank = 0;

    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        switch (v->state[i]) {
        case CS_READING: reading++; break;
        case CS_PARSED:
        case CS_WAITING: waiting++; break;
        case CS_WRITING:
            if (v->bytes[i] == 0u) {
                writer_start++;
            } else {
                writer_part++;
            }
            break;
        default: break;
        }
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (v->pins[b] > 1) {
            shared_bank = 1;
        }
    }
    *missing = reading == 0        ? "a partial-request reader"
             : waiting == 0        ? "a parsed/waiting scrape"
             : writer_start == 0   ? "a writer before its first byte"
             : writer_part == 0    ? "a writer after at least one byte"
             : shared_bank == 0    ? "a bank pinned by more than one client"
                                   : NULL;
    return *missing == NULL;
}

/* Wait, bounded, until the owner's own view satisfies a predicate. Every step
 * below is driven by what the owner is actually holding, so nothing races the
 * deadlines that would otherwise retire these states while the test set them
 * up. */
/*
 * THE ARM BARRIER.
 *
 * A producer that starts before the wait is listening can spend publications
 * the wait never saw, which would make any count of them a claim about the
 * scheduler rather than about the wait. So the wait publishes the generation
 * it armed on, once, through an object the CALLER hands it -- not a global
 * selector, which would let a wait on one listener release a barrier belonging
 * to another, and not a flag a producer has to keep looking at.
 *
 * It is a distinct owner with a distinct lifetime, so it carries its own
 * acquisition state and its own entries in the run's balance statement.
 */
typedef struct term_arm {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             mu_live;
    int             cv_live;
    int             fail_cv_destroy;   /* the condition refuses to be freed */
    int             armed;
    int             waiters;   /* someone is blocked on THIS barrier */
    /* Threads that can still reach this barrier. Releasing it while any
     * remain is destroying an object somebody may still touch, so the release
     * refuses and says so rather than relying on a sanitizer to notice. */
    int             reachers;
    uint64_t        gen;
} term_arm_t;

static int g_arm_mu_init;
static int g_arm_mu_destroy;
static int g_arm_cv_init;
static int g_arm_cv_destroy;
static int g_arm_release_failures;
/* Releases attempted while a thread could still reach the barrier. */
static int g_arm_unsafe_release_attempts;
/* Refusals a case deliberately arranged. */
static int g_arm_expected_release_failures;

/* Releases exactly what was acquired, and only on a successful destroy. */
static void
term_arm_release(term_arm_t *a)
{
    if (a->reachers != 0) {
        g_arm_unsafe_release_attempts++;
        printf("  arm: release attempted while %d thread(s) can still reach "
               "this barrier\n", a->reachers);
        return;
    }
    if (a->cv_live) {
        if (!a->fail_cv_destroy && pthread_cond_destroy(&a->cv) == 0) {
            a->cv_live = 0;
            g_arm_cv_destroy++;
        } else {
            g_arm_release_failures++;
            printf("  arm: the condition refused to be destroyed\n");
        }
    }
    if (a->mu_live) {
        if (pthread_mutex_destroy(&a->mu) == 0) {
            a->mu_live = 0;
            g_arm_mu_destroy++;
        } else {
            g_arm_release_failures++;
            printf("  arm: the mutex refused to be destroyed\n");
        }
    }
}

/* One acquisition path. Each object is taken separately and recorded before
 * the next is attempted, so a refusal unwinds precisely what it took. */
static bool
term_arm_init(term_arm_t *a, int fail_mu, int fail_cv)
{
    memset(a, 0, sizeof(*a));
    if (fail_mu || pthread_mutex_init(&a->mu, NULL) != 0) {
        return false;
    }
    a->mu_live = 1;
    g_arm_mu_init++;
    if (fail_cv || pthread_cond_init(&a->cv, NULL) != 0) {
        term_arm_release(a);
        return false;
    }
    a->cv_live = 1;
    g_arm_cv_init++;
    return true;
}

static void
term_arm_publish(term_arm_t *a, uint64_t gen)
{
    if (a == NULL) {
        return;
    }
    (void)pthread_mutex_lock(&a->mu);
    a->gen = gen;
    a->armed = 1;
    (void)pthread_cond_broadcast(&a->cv);
    (void)pthread_mutex_unlock(&a->mu);
}

/* Blocks on the barrier until the wait has armed, or the deadline passes. */
static bool
term_arm_wait(term_arm_t *a, int budget_ms, uint64_t *gen)
{
    struct timespec deadline;
    bool armed;

    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return false;
    }
    deadline.tv_sec += budget_ms / 1000;
    deadline.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    (void)pthread_mutex_lock(&a->mu);
    /* Announced on the barrier itself, so a test can order against the fact
     * that someone is waiting on THIS one rather than against a delay. */
    a->waiters++;
    (void)pthread_cond_broadcast(&a->cv);
    while (!a->armed) {
        if (pthread_cond_timedwait(&a->cv, &a->mu, &deadline) != 0) {
            break;
        }
    }
    a->waiters--;
    armed = a->armed != 0;
    *gen = a->gen;
    (void)pthread_mutex_unlock(&a->mu);
    return armed;
}

/* Blocks until someone is waiting on this barrier, or the guard expires. */
static bool
term_arm_wait_for_waiter(term_arm_t *a, int budget_ms)
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
    (void)pthread_mutex_lock(&a->mu);
    while (a->waiters == 0) {
        if (pthread_cond_timedwait(&a->cv, &a->mu, &deadline) != 0) {
            break;
        }
    }
    seen = a->waiters > 0;
    (void)pthread_mutex_unlock(&a->mu);
    return seen;
}

/* Whether this barrier has been armed, read under its own lock. */
static bool
term_arm_is_armed(term_arm_t *a)
{
    bool armed;

    (void)pthread_mutex_lock(&a->mu);
    armed = a->armed != 0;
    (void)pthread_mutex_unlock(&a->mu);
    return armed;
}

static bool
term_wait_armed(moqr_admin_listen_t *l, moqr_admin_listen_view_t *v,
                bool (*pred)(const moqr_admin_listen_view_t *), int budget_ms,
                term_arm_t *arm)
{
    /*
     * Waits on PUBLICATION, bounded by ONE absolute deadline.
     *
     * The budget is wall time, and it has to be measured as wall time. Charging
     * a fixed slice per iteration charges the full slice for a wait that
     * returned instantly, so a burst of publications that do not satisfy the
     * predicate spends the whole budget in microseconds -- the caller is then
     * told the predicate never held when in truth it was never given the time
     * it asked for. The deadline below is computed once and never refreshed;
     * each wait gets only what is actually left.
     */
    struct timespec start;
    uint64_t gen = 0;

    if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
        printf("  term_wait: no monotonic clock — cannot bound this wait\n");
        return false;
    }
    if (moqr_admin_listen_test_view(l, v) != 0u && pred(v)) {
        return true;
    }
    gen = v->gen;
    term_arm_publish(arm, gen);
    for (;;) {
        struct timespec now;
        long elapsed_ms;
        int left;
        uint64_t got;

        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
            printf("  term_wait: the monotonic clock stopped answering\n");
            return false;
        }
        elapsed_ms = (long)(now.tv_sec - start.tv_sec) * 1000L +
                     (now.tv_nsec - start.tv_nsec) / 1000000L;
        left = budget_ms - (int)elapsed_ms;
        if (left <= 0) {
            return false;   /* the predicate never held within the budget */
        }
        got = moqr_admin_listen_test_wait_view(l, gen,
                                               left > 250 ? 250 : left, v);
        if (got == 0u) {
            continue;   /* no publication yet; the deadline above still runs */
        }
        gen = got;
        if (pred(v)) {
            return true;
        }
    }
}

/* The ordinary wait: no barrier, because nothing is ordered against it. */
static bool
term_wait(moqr_admin_listen_t *l, moqr_admin_listen_view_t *v,
          bool (*pred)(const moqr_admin_listen_view_t *), int budget_ms)
{
    return term_wait_armed(l, v, pred, budget_ms, NULL);
}

static int
term_count(const moqr_admin_listen_view_t *v, int st)
{
    int n = 0;
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v->state[i] == st) {
            n++;
        }
    }
    return n;
}

static bool
term_one_waiting(const moqr_admin_listen_view_t *v)
{
    return term_count(v, CS_WAITING) >= 1;
}

static bool
term_two_waiting(const moqr_admin_listen_view_t *v)
{
    /* WAITING, not PARSED: both must be bound to the SAME generation before
     * publication is released, or they will not share a bank. */
    return term_count(v, CS_WAITING) >= 2;
}

static bool
term_two_writing(const moqr_admin_listen_view_t *v)
{
    int at_start = 0;

    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v->state[i] == CS_WRITING && v->bytes[i] == 0u) {
            at_start++;
        }
    }
    return at_start >= 2;
}

static bool
term_split_writers(const moqr_admin_listen_view_t *v)
{
    int at_start = 0;
    int partial = 0;

    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v->state[i] != CS_WRITING) {
            continue;
        }
        if (v->bytes[i] == 0u) {
            at_start++;
        } else {
            partial++;
        }
    }
    return at_start >= 1 && partial >= 1;
}

static bool
term_one_more_waiting(const moqr_admin_listen_view_t *v)
{
    return term_count(v, CS_WAITING) + term_count(v, CS_PARSED) >= 1;
}

static bool
term_paused_at_cancel(const moqr_admin_listen_view_t *v)
{
    return v->paused_at_cancel != 0;
}

static bool
term_carrier_live(const moqr_admin_listen_view_t *v)
{
    /* The owner is HELD at the boundary with the carrier published, which is
     * the only moment the retained token is observable. */
    return v->retiring != 0 && v->paused != 0;
}

static bool
term_all_states(const moqr_admin_listen_view_t *v)
{
    const char *missing = NULL;
    return term_states_present(v, &missing);
}

static int
test_terminality_settles_every_state(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int port;
    int fds[8];
    int nfds = 0;
    moqr_admin_listen_view_t view;
    const char *missing = NULL;
    int waiter_fd = -1;
    unsigned settle_before = 0;
    unsigned cancel_before = 0;
    unsigned drop_before = 0;
    int live_at_cancel[MOQR_ADMIN_MAX_CLIENTS] = { 0 };
    unsigned drop_at_cancel[MOQR_ADMIN_MAX_CLIENTS] = { 0 };
    unsigned attempt_at_cancel[MOQR_ADMIN_MAX_CLIENTS] = { 0 };
    unsigned expect_site[MOQR_ADMIN_MAX_CLIENTS] = { 0 };
    int frozen_slot = -1;
    int n_expect_turn = 0;
    int n_expect_exit = 0;
    int n_live_at_cancel = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  termstates: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    memset(fds, -1, sizeof(fds));
    /* From here the owner reads a clock this case owns, so every interior
     * deadline below is a step rather than an interval to wait out. */
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "clock",
                                            &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }

    /* Two clients that will SHARE one bank: both join the same generation
     * while publication is held, then it completes for both at once. The hold
     * is ACKNOWLEDGED before anything depends on it -- a hold nobody has
     * reached is not a hold. */
    rig_hold_publication(&rig);
    if (!rig_wait_publisher_blocked(&rig, 4000)) {
        printf("  termstates: the publisher never reached its barrier\n");
        rig_stop(&rig, l);
        return 1;
    }
    moqr_admin_listen_test_client_write_budget(0);
    /* One at a time, each ACKNOWLEDGED as waiting before the next is dialled.
     * Two connections offered together are two connections the owner may take
     * in either order and in either turn; waiting for the first to be seen is
     * what makes the pair deterministic. The second joins the generation the
     * first opened -- demand arriving while a serial is collecting joins it. */
    for (int k = 0; k < 2; k++) {
        fds[nfds] = dial(port);
        if (fds[nfds] < 0) {
            continue;
        }
        (void)write_all(fds[nfds],
                        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        nfds++;
        if (!term_wait(l, &view, k == 0 ? term_one_waiting : term_two_waiting,
                       8000)) {
            printf("  termstates: client %d never reached the shared "
                   "generation\n", k + 1);
            moqr_admin_listen_test_client_write_budget(-1);
            rig_release_publication(&rig);
            for (int q = 0; q < nfds; q++) {
                if (fds[q] >= 0) {
                    (void)close(fds[q]);
                }
            }
            rig_stop(&rig, l);
            return 1;
        }
    }
    if (!term_wait(l, &view, term_two_waiting, 8000)) {
        printf("  termstates: the two shared-bank clients never joined one "
               "generation\n");
        moqr_admin_listen_test_client_write_budget(-1);
        rig_stop(&rig, l);
        return 1;
    }
    /* Release publication so their shared generation commits. */
    rig_release_publication(&rig);
    if (!term_wait(l, &view, term_two_writing, 8000)) {
        printf("  termstates: the shared generation never reached both "
               "writers\n");
        moqr_admin_listen_test_client_write_budget(-1);
        rig_stop(&rig, l);
        return 1;
    }
    /* Exactly one owner-side write advances exactly one client. The peer does
     * not decide which cancellation state exists, and no scheduling window is
     * used as evidence. */
    moqr_admin_listen_test_client_write_budget(1);
    if (!term_wait(l, &view, term_split_writers, 8000)) {
        printf("  termstates: the write budget did not split the two writers\n");
        moqr_admin_listen_test_client_write_budget(-1);
        rig_stop(&rig, l);
        return 1;
    }

    /* A partial-request reader, and a waiter with nothing published. */
    rig_hold_publication(&rig);
    fds[nfds] = dial(port);
    if (fds[nfds] >= 0) {
        (void)write_all(fds[nfds], "GET /metrics HTTP/1.1\r\n");
        nfds++;
    }
    fds[nfds] = dial(port);
    if (fds[nfds] >= 0) {
        (void)write_all(fds[nfds],
                        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        /* Remembered: this is the sole waiter of a generation that can never
         * complete, and closing it in phase two is what abandons it. */
        waiter_fd = fds[nfds];
        nfds++;
    }
    if (!term_wait(l, &view, term_one_more_waiting, 8000)) {
        printf("  termstates: the waiting scrape never bound a generation\n");
        moqr_admin_listen_test_client_write_budget(-1);
        rig_stop(&rig, l);
        return 1;
    }

    /* The slot that is mid-response is the one to keep frozen. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (view.state[i] == CS_WRITING && view.bytes[i] > 0u) {
            frozen_slot = (int)i;
            break;
        }
    }

    /* Every named state, observed AT ONCE in the owner's own view. */
    if (!term_wait(l, &view, term_all_states, 8000)) {
        (void)term_states_present(&view, &missing);
        printf("  termstates: the states did not coexist before terminality "
               "(missing %s) — this case would have proved nothing\n",
               missing != NULL ? missing : "?");
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            printf("    client %u state=%d bytes=%llu fd=%d\n", i,
                   view.state[i], view.bytes[i], view.fds[i]);
        }
        for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
            printf("    bank %u pins=%d\n", b, view.pins[b]);
        }
        failures++;
    }

    /*
     * PHASE TWO: the abandoned generation and its retained broker token,
     * observed WHILE IT EXISTS.
     *
     * The waiting scrape is the sole waiter of a generation publication will
     * never complete. Closing it makes that generation abandoned; the owner
     * claims the retirement, records the token, publishes the carrier and
     * stops at the pause -- so what follows is an observation, not an
     * inference from the emptiness afterwards.
     */
    /*
     * The global budget has served its purpose. Writes are released for every
     * client EXCEPT the one that is meant to still be mid-response when
     * cancellation runs: that slot stays frozen, so cancellation finds a
     * response with bytes already on the wire and drops it -- which is what
     * leaves a slot for the DONE sweep. Without it the fixture would exercise
     * only one of the two cleanup owners.
     */
    moqr_admin_listen_test_client_write_budget(-1);
    moqr_admin_listen_test_freeze_client(frozen_slot);
    moqr_admin_listen_test_pause_at_retire(l, 1);
    if (waiter_fd >= 0) {
        (void)close(waiter_fd);
        waiter_fd = -1;
    }
    /*
     * STEP the epoch deadline rather than wait for it. The waiting scrape is
     * not pollable, so nothing notices it left until its epoch expires; that
     * expiry is the state transition under test, and advancing the owner's
     * clock past it is what makes the abandonment deterministic instead of
     * scheduler-dependent. The margin clears the epoch budget only -- the
     * reader and writers have longer budgets and must survive.
     */
    {
        int before = failures;
        moqr_admin_listen_test_clock_advance_expect(
            l, MOQR_ADMIN_EPOCH_DEADLINE_US + 100000u, true, "termstates",
            &failures);
        if (failures != before) {
            goto termstates_done;
        }
    }
    if (!term_wait(l, &view, term_carrier_live, 12000)) {
        printf("  termstates: no abandoned generation with a retained broker "
               "token was ever observed\n");
        failures++;
    } else {
        if (view.retire_serial == 0u) {
            printf("  termstates: the retained carrier names serial 0\n");
            failures++;
        }
        if (view.retire_taken == 0) {
            printf("  termstates: the retirement was published without its "
                   "broker token recorded\n");
            failures++;
        }
        if (view.retire_bank >= MOQR_ADMIN_BANKS) {
            printf("  termstates: the retained carrier names bank %u\n",
                   view.retire_bank);
            failures++;
        }
        if ((view.retire_demand & MOQR_BROKER_DEMAND__ALL) == 0u) {
            printf("  termstates: the retained carrier carries no demand\n");
            failures++;
        }
        if (view.n_settle != 0u) {
            printf("  termstates: %u settlements before the carrier was even "
                   "released\n", view.n_settle);
            failures++;
        }
    }
    settle_before = view.n_settle;
    cancel_before = view.n_cancel;
    drop_before = view.n_drop;

    /*
     * PHASE THREE: terminal cleanup ownership, per slot.
     *
     * The owner is stopped immediately after cancellation and before EITHER
     * cleanup sweep, so the live descriptor set can be recorded before
     * anything touches it. Aggregate counts cannot tell the two sweeps apart;
     * per-slot site and count can.
     */
    moqr_admin_listen_test_pause_at_cancel(l, 1);
    rig_release_publication(&rig);
    moqr_admin_listen_note_terminal(l);
    moqr_admin_listen_test_pause_at_retire(l, 0);
    moqr_admin_listen_test_client_write_budget(-1);
    rig_stop_publisher(&rig);

    /* Record the live slot set while the owner is held at cancellation. */
    if (!term_wait(l, &view, term_paused_at_cancel, 8000)) {
        printf("  termstates: the owner never paused at cancellation\n");
        failures++;
    } else {
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            /* Baselines: the counters are cumulative over the whole run, so
             * what matters is what THIS cleanup adds. */
            /*
             * The expected owner is DETERMINED by the state cancellation left
             * behind, so "some named site" is not the claim.
             *
             * Cancellation turns a response that had already put a byte on the
             * wire into DONE -- the current turn's DONE sweep drops that one.
             * Everything else it can still answer 503, which stays WRITING
             * until the stop-driven exit sweep. So DONE here means TURN, and
             * anything still live and not DONE means EXIT.
             */
            live_at_cancel[i] = view.fds[i] >= 0 ? 1 : 0;
            drop_at_cancel[i] = view.drop_count[i];
            attempt_at_cancel[i] = view.drop_attempt[i];
            expect_site[i] = view.state[i] == CS_DONE ? MOQR_ADMIN_DROP_TURN
                                                      : MOQR_ADMIN_DROP_EXIT;
            if (live_at_cancel[i]) {
                if (expect_site[i] == MOQR_ADMIN_DROP_TURN) {
                    n_expect_turn++;
                } else {
                    n_expect_exit++;
                }
            }
            if (live_at_cancel[i]) {
                n_live_at_cancel++;
            }
        }
        if (n_live_at_cancel == 0) {
            printf("  termstates: no descriptor was live at cancellation — "
                   "this phase would prove nothing\n");
            failures++;
        }
        /* Every site this case claims to cover must have a slot. An oracle
         * that names a site the fixture never reaches is vacuous for it. */
        if (n_expect_turn == 0) {
            printf("  termstates: no slot was left for the DONE sweep — that "
                   "site's ownership is not exercised\n");
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                printf("    slot %u state=%d bytes=%llu fd=%d\n", i,
                       view.state[i], view.bytes[i], view.fds[i]);
            }
            failures++;
        }
        if (n_expect_exit == 0) {
            printf("  termstates: no slot was left for the exit sweep — that "
                   "site's ownership is not exercised\n");
            failures++;
        }
        drop_before = view.n_drop;
    }
    moqr_admin_listen_test_pause_at_cancel(l, 0);

    {
        int wakes_before = atomic_load(&rig.wakes);
        moqr_result_t rc = moqr_admin_listen_stop(l);
        if (rc != MOQR_OK) {
            printf("  termstates: the owner did not stop cleanly (%d, health "
                   "%u)\n", (int)rc, (unsigned)moqr_admin_listen_health(l));
            failures++;
        }
        if (atomic_load(&rig.wakes) != wakes_before) {
            printf("  termstates: a lane was woken after terminality\n");
            failures++;
        }
    }
    /*
     * EXACTLY ONCE, counted -- not inferred from a final empty state, which is
     * equally compatible with zero, one, or repeated attempts.
     */
    (void)moqr_admin_listen_test_view(l, &view);
    if (view.n_cancel != cancel_before + 1u) {
        printf("  termstates: cancellation ran %u times (was %u), expected "
               "exactly once [gen=%llu drop=%u settle=%u]\n", view.n_cancel,
               cancel_before, (unsigned long long)view.gen, view.n_drop,
               view.n_settle);
        failures++;
    }
    if (view.n_settle != settle_before + 1u) {
        printf("  termstates: %u abandoned generations settled (was %u), "
               "expected exactly the one that was held\n", view.n_settle,
               settle_before);
        failures++;
    }
    /* PER SLOT: each descriptor that was live at cancellation is dropped
     * exactly once, and by a cleanup site that names itself. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (!live_at_cancel[i]) {
            if (view.drop_count[i] != drop_at_cancel[i]) {
                printf("  termstates: slot %u was dropped again although it "
                       "held no descriptor at cancellation (%u -> %u)\n", i,
                       drop_at_cancel[i], view.drop_count[i]);
                failures++;
            }
            continue;
        }
        if (view.drop_count[i] != drop_at_cancel[i] + 1u) {
            printf("  termstates: slot %u dropped %u times during cleanup "
                   "(%u -> %u), expected exactly once\n", i,
                   view.drop_count[i] - drop_at_cancel[i], drop_at_cancel[i],
                   view.drop_count[i]);
            failures++;
        }
        if (view.drop_site[i] != expect_site[i]) {
            printf("  termstates: slot %u was dropped by site %u, expected "
                   "%u for the state cancellation left it in\n", i,
                   view.drop_site[i], expect_site[i]);
            failures++;
        }
        /* ASKED exactly once. The drop is idempotent, so a repeated call is
         * invisible in the effect and only visible here. */
        if (view.drop_attempt[i] != attempt_at_cancel[i] + 1u) {
            printf("  termstates: slot %u was asked to drop %u times during "
                   "cleanup, expected exactly once\n", i,
                   view.drop_attempt[i] - attempt_at_cancel[i]);
            failures++;
        }
    }
    if (view.n_drop != drop_before + (unsigned)n_live_at_cancel) {
        printf("  termstates: %u client drops (was %u), expected exactly %d "
               "-- one per descriptor live at cancellation\n", view.n_drop,
               drop_before, n_live_at_cancel);
        failures++;
    }
    /*
     * PER SITE, and non-vacuous. Equality alone accepts 0 == 0, which is what
     * "no release happened at all" also looks like.
     */
    if (view.n_bank_release == 0u) {
        printf("  termstates: no ordinary bank release happened at all\n");
        failures++;
    }
    if (view.n_bank_release != view.n_ack_release) {
        printf("  termstates: %u bank releases but %u acknowledgements — a "
               "release was performed without being settled\n",
               view.n_bank_release, view.n_ack_release);
        failures++;
    }
    /* EXACT, and tied together: every attempt succeeded, every success was a
     * broker release, and every broker release was acknowledged. `> 0` would
     * accept a duplicated success. */
    if (view.rel_attempt[MOQR_ADMIN_REL_BANK] !=
        view.rel_ok[MOQR_ADMIN_REL_BANK]) {
        printf("  termstates: %u ordinary release attempts but %u successes\n",
               view.rel_attempt[MOQR_ADMIN_REL_BANK],
               view.rel_ok[MOQR_ADMIN_REL_BANK]);
        failures++;
    }
    if (view.rel_ok[MOQR_ADMIN_REL_BANK] != view.n_bank_release ||
        view.n_bank_release != view.n_ack_release) {
        printf("  termstates: ordinary releases do not line up (site %u, "
               "counted %u, acknowledged %u)\n",
               view.rel_ok[MOQR_ADMIN_REL_BANK], view.n_bank_release,
               view.n_ack_release);
        failures++;
    }
    if (view.rel_serial[MOQR_ADMIN_REL_BANK] == 0u) {
        printf("  termstates: the ordinary release names serial 0\n");
        failures++;
    }
    if (view.rel_bank[MOQR_ADMIN_REL_BANK] >= MOQR_ADMIN_BANKS) {
        printf("  termstates: the ordinary release names bank %u\n",
               view.rel_bank[MOQR_ADMIN_REL_BANK]);
        failures++;
    }
    if (view.rel_attempt[MOQR_ADMIN_REL_SETTLE] != 1u) {
        printf("  termstates: the settlement callback ASKED to release %u "
               "times, expected exactly once\n",
               view.rel_attempt[MOQR_ADMIN_REL_SETTLE]);
        failures++;
    }
    if (view.rel_ok[MOQR_ADMIN_REL_SETTLE] != 1u) {
        printf("  termstates: the settlement callback released %u times, "
               "expected exactly the one held generation\n",
               view.rel_ok[MOQR_ADMIN_REL_SETTLE]);
        failures++;
    } else if (view.rel_serial[MOQR_ADMIN_REL_SETTLE] == 0u) {
        printf("  termstates: the settlement release names serial 0\n");
        failures++;
    }
    if (view.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] != 0u) {
        printf("  termstates: %u releases through the no-carrier site, which "
               "this scenario never reaches\n",
               view.rel_attempt[MOQR_ADMIN_REL_NOCARRIER]);
        failures++;
    }
    if (view.retiring != 0) {
        printf("  termstates: a retirement carrier outlived the shutdown\n");
        failures++;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (view.fds[i] >= 0) {
            printf("  termstates: client %u kept its descriptor\n", i);
            failures++;
        }
        if (view.state[i] != CS_FREE) {
            printf("  termstates: client %u is in state %d, not FREE\n", i,
                   view.state[i]);
            failures++;
        }
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (view.pins[b] != 0) {
            printf("  termstates: bank %u kept %d pin(s)\n", b, view.pins[b]);
            failures++;
        }
    }
    if (moqr_broker_busy(&rig.broker)) {
        printf("  termstates: the broker still holds a generation after "
               "terminality\n");
        failures++;
    }
termstates_done:
    for (int i = 0; i < nfds; i++) {
        if (fds[i] >= 0) {
            (void)close(fds[i]);
        }
    }
    /* Seams are global to the translation unit; leaving one armed would make
     * the next case fail for this case's reason. */
    moqr_admin_listen_test_freeze_client(-1);
    moqr_admin_listen_test_client_write_budget(-1);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

static int
test_render_failures_do_not_exhaust_the_ledger(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    char buf[4096];
    const int rounds = (int)MOQR_ADMIN_MAX_CLIENTS + 2;

    /*
     * A renderer failure must not consume a bounded resource. That path
     * already holds the authoritative demand from the token it took, so there
     * is nothing to record and rediscover later -- and a note left behind
     * because the demand lacked SIGNAL would occupy a poison-ledger slot for
     * good. The ledger is bounded, so enough of them turn the next ordinary
     * failure into a terminal. One repetition proves nothing; this runs more
     * of them than the ledger has room for.
     */
    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  ledger: start refused\n");
        return 1;
    }
    atomic_store(&rig.render_fails, 1);
    for (int i = 0; i < rounds; i++) {
        ssize_t n = exchange(moqr_admin_listen_port(l),
                             "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                             buf, sizeof(buf));
        if (n <= 0 || strstr(buf, "500") == NULL) {
            printf("  ledger: round %d was not answered 500\n", i);
            failures++;
            break;
        }
        if (strstr(buf, "OPENMETRICS serial=") != NULL ||
            strstr(buf, "PROM serial=") != NULL) {
            printf("  ledger: a 500 carried a metrics body\n");
            failures++;
            break;
        }
        if (!wait_broker_idle(&rig, 3000)) {
            printf("  ledger: round %d stranded its generation\n", i);
            failures++;
            break;
        }
        if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
            printf("  ledger: the endpoint failed after %d recoverable "
                   "render failures - a bounded resource was consumed\n", i);
            failures++;
            break;
        }
    }
    atomic_store(&rig.render_fails, 0);
    {
        ssize_t n = exchange(moqr_admin_listen_port(l),
                             "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                             buf, sizeof(buf));
        if (n <= 0 || strstr(buf, "200 OK") == NULL) {
            printf("  ledger: the endpoint did not recover\n");
            failures++;
        }
    }
    if (atomic_load(&rig.suppressions) != 0) {
        printf("  ledger: HTTP-only render failures emitted %d suppression "
               "diagnostics to a sink that never asked\n",
               atomic_load(&rig.suppressions));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/* The same failure with SIGNAL demand owes exactly one diagnostic and no
 * document. */
static int
test_render_failure_signal_demand(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int spins;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  ledgersig: start refused\n");
        return 1;
    }
    atomic_store(&rig.render_fails, 1);
    atomic_store(&rig.latched, 1);
    moqr_admin_listen_notify(l);
    for (spins = 0; spins < 800 && atomic_load(&rig.suppressions) == 0;
         spins++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    if (spins >= 800) {
        printf("  ledgersig: a poisoned render never told the signal sink\n");
        failures++;
    } else if (atomic_load(&rig.suppressions) != 1) {
        printf("  ledgersig: %d diagnostics, expected exactly 1\n",
               atomic_load(&rig.suppressions));
        failures++;
    }
    if (atomic_load(&rig.emits) != 0) {
        printf("  ledgersig: a failed render emitted a document\n");
        failures++;
    }
    if (!wait_broker_idle(&rig, 3000)) {
        printf("  ledgersig: the generation was never retired\n");
        failures++;
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  ledgersig: health %u after a recoverable render failure\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * Starting a thread is not a readiness acknowledgement: the owner can fail its
 * very first clock read or poll. Activation is what reports it reached the
 * serving loop, so with either failure injected it must refuse.
 */
static int
startup_failure_case(const char *what, void (*inject)(int))
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_result_t rc;

    inject(1);
    rc = rig_start(&rig, &l);
    inject(0);
    if (rc == MOQR_OK) {
        printf("  startup[%s]: the endpoint reported itself serving although "
               "its owner failed its first turn\n", what);
        failures++;
        rig_stop(&rig, l);
        return failures;
    }
    if (l != NULL) {
        printf("  startup[%s]: a refused start left a handle behind\n", what);
        failures++;
    }
    return failures;
}

static int
test_startup_refuses_a_failed_owner(void)
{
    int failures = 0;
    failures += startup_failure_case("clock", moqr_admin_listen_test_fail_clock);
    failures += startup_failure_case("poll", moqr_admin_listen_test_fail_poll);
    return failures;
}

/*
 * THE VIEW BELONGS TO ONE LISTENER.
 *
 * Two endpoints run at once and only one of them is driven. A publication from
 * B must not satisfy a waiter watching A, and A's evidence must not be
 * overwritten by B -- which is precisely what a process-global sample cannot
 * guarantee. The waiter here is event-driven: it blocks on A's own
 * publication, and the budget is a fail-closed bound rather than the
 * mechanism.
 */
static bool
view_has_a_client(const moqr_admin_listen_view_t *v)
{
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v->fds[i] >= 0) {
            return true;
        }
    }
    return false;
}

static int
test_view_is_listener_bound(void)
{
    int failures = 0;
    rig_t rig_a, rig_b;
    moqr_admin_listen_t *a = NULL, *b = NULL;
    moqr_admin_listen_view_t va, vb;
    uint64_t gen_a0, gen_b0;
    int fd = -1;

    if (rig_start(&rig_a, &a) != MOQR_OK) {
        printf("  twolisten: A did not start\n");
        return 1;
    }
    if (rig_start(&rig_b, &b) != MOQR_OK) {
        printf("  twolisten: B did not start\n");
        rig_stop(&rig_a, a);
        return 1;
    }
    if (moqr_admin_listen_port(a) == moqr_admin_listen_port(b)) {
        printf("  twolisten: both endpoints took the same port\n");
        failures++;
    }
    (void)moqr_admin_listen_test_view(a, &va);
    (void)moqr_admin_listen_test_view(b, &vb);
    gen_a0 = va.gen;
    gen_b0 = vb.gen;

    /* Drive B only. */
    fd = dial(moqr_admin_listen_port(b));
    if (fd < 0) {
        printf("  twolisten: could not reach B\n");
        failures++;
    } else {
        (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    }

    /* B publishes; A must not, and B's publication must not satisfy a waiter
     * watching A. A publication is one owner turn, not necessarily the accept
     * turn: ignore an empty turn that raced the dial and wait for B's accepted
     * client to appear in B's own published view. `term_wait` is itself proved
     * against a deterministic burst of non-matching publications below. */
    if (!term_wait(b, &vb, view_has_a_client, 8000)) {
        printf("  twolisten: B's view does not show the client it accepted\n");
        failures++;
    }
    /*
     * Generation counters are per listener, so comparing them across endpoints
     * proves nothing. What does: A's published CONTENT stays its own. A saw no
     * connection, so its client table and its accept counter must both still
     * be empty while B's view shows the client B accepted.
     */
    (void)moqr_admin_listen_test_view(a, &va);
    if (moqr_admin_listen_accepts(a) != 0u) {
        printf("  twolisten: A accepted %llu connections although only B was "
               "driven\n", (unsigned long long)moqr_admin_listen_accepts(a));
        failures++;
    }
    if (view_has_a_client(&va)) {
        printf("  twolisten: A's view shows a client only B ever had — the "
               "evidence is shared between endpoints\n");
        failures++;
    }
    /* And both did publish on their own -- an endpoint that never published
     * would make the isolation above vacuous. Waited for, not sampled. */
    if (moqr_admin_listen_test_wait_view(a, gen_a0, 4000, &va) == 0u) {
        printf("  twolisten: A never published its own view\n");
        failures++;
    }
    if (vb.gen <= gen_b0) {
        printf("  twolisten: B never published its own view\n");
        failures++;
    }
    if (view_has_a_client(&va)) {
        printf("  twolisten: A's later view shows a client only B ever had\n");
        failures++;
    }
    if (fd >= 0) {
        (void)close(fd);
    }
    rig_stop(&rig_b, b);
    rig_stop(&rig_a, a);
    return failures;
}

/*
 * A waiter must be told when a publication never comes, rather than being
 * handed a stale sample that happens to satisfy it.
 */
static int
test_view_wait_is_fail_closed(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t v;
    uint64_t gen;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  viewwait: start refused\n");
        return 1;
    }
    /* Park the owner: it stops publishing, so nothing newer can arrive. */
    moqr_admin_listen_test_park_owner(1);
    (void)moqr_admin_listen_stop(l);   /* asks it to stop; parked, it will not */
    (void)moqr_admin_listen_test_view(l, &v);
    gen = v.gen;
    {
        uint64_t got = moqr_admin_listen_test_wait_view(l, gen, 300, &v);
        if (got != 0u) {
            printf("  viewwait: a wait for a publication that never came "
                   "returned generation %llu\n", (unsigned long long)got);
            failures++;
        }
    }
    moqr_admin_listen_test_park_owner(0);
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * The no-admin-carrier retirement's own release.
 *
 * A signal-only generation poisoned before any client or record existed is
 * retired directly against the broker: there is no admin token to lose, which
 * is exactly why that path is allowed to release without a carrier. Its
 * release is a distinct call from the ordinary bank release and from the
 * settlement callback, so it needs a seam of its own -- otherwise "every
 * destructive release is exercised" is a claim about one of three.
 */
static int
test_nocarrier_release_is_its_own_call(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  nocarrier: start refused\n");
        return 1;
    }
    /* Signal-only demand, poisoned: no client, no generation record. */
    atomic_store(&rig.poison_once, 1);
    atomic_store(&rig.poison, 1);
    moqr_admin_listen_test_fail_transition(
        MOQR_ADMIN_TEST_FAIL_NOCARRIER_RELEASE);
    atomic_store(&rig.latched, 1);
    moqr_admin_listen_notify(l);

    for (i = 0; i < 800 && !moqr_admin_listen_owner_exited(l); i++) {
        struct timespec ts = { 0, 5 * 1000 * 1000 };
        (void)nanosleep(&ts, NULL);
    }
    moqr_admin_listen_test_fail_transition(MOQR_ADMIN_TEST_FAIL_NONE);
    if (i >= 800) {
        printf("  nocarrier: a refused no-carrier release did not stop the "
               "endpoint\n");
        failures++;
    } else if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_INVARIANT) {
        printf("  nocarrier: health %u, expected INVARIANT\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    if (moqr_admin_listen_test_transition_hit() !=
        MOQR_ADMIN_TEST_FAIL_NOCARRIER_RELEASE) {
        printf("  nocarrier: the exact seam was not reached (hit=%s)\n",
               moqr_admin_listen_test_site_name(
                   moqr_admin_listen_test_transition_hit()));
        failures++;
    }
    /* And the ledger says the same thing: exactly one attempt through THIS
     * site and none through either other. */
    {
        moqr_admin_listen_view_t v;
        (void)moqr_admin_listen_test_view(l, &v);
        if (v.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] != 1u) {
            printf("  nocarrier: %u attempts through the no-carrier site, "
                   "expected exactly one\n",
                   v.rel_attempt[MOQR_ADMIN_REL_NOCARRIER]);
            failures++;
        }
        if (v.rel_attempt[MOQR_ADMIN_REL_SETTLE] != 0u ||
            v.rel_attempt[MOQR_ADMIN_REL_BANK] != 0u) {
            printf("  nocarrier: the retirement also went through another "
                   "release site (bank=%u settle=%u)\n",
                   v.rel_attempt[MOQR_ADMIN_REL_BANK],
                   v.rel_attempt[MOQR_ADMIN_REL_SETTLE]);
            failures++;
        }
    }
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * THE INJECTED-SITE VOCABULARY IS UNIQUE, and it is one vocabulary.
 *
 * Two sites sharing an id is not a cosmetic problem: arming one silently
 * selects whichever wrapper the owner reaches first, and an oracle that
 * compares only the integer cannot tell that it never reached the site it
 * named. That is a false green, and it is exactly what happened.
 *
 * The values come from an enumeration in one header, so a collision cannot be
 * written; this proves the property that construction is supposed to give, and
 * fails if anyone ever replaces it with hand-numbered defines again.
 */
static int
test_transition_sites_are_unique(void)
{
    int failures = 0;
    static const int sites[] = {
#define MOQR_SITE_VALUE(name) MOQR_ADMIN_TEST_FAIL_##name,
        MOQR_ADMIN_TEST_SITES(MOQR_SITE_VALUE)
#undef MOQR_SITE_VALUE
    };
    const size_t n = sizeof(sites) / sizeof(sites[0]);

    if ((int)n != (int)MOQR_ADMIN_TEST_FAIL__COUNT) {
        printf("  sites: %zu names but %d values — the vocabulary is not one "
               "list\n", n, (int)MOQR_ADMIN_TEST_FAIL__COUNT);
        failures++;
    }
    for (size_t i = 0; i < n; i++) {
        for (size_t j = i + 1; j < n; j++) {
            if (sites[i] == sites[j]) {
                printf("  sites: %s and %s share id %d — arming one selects "
                       "the other\n",
                       moqr_admin_listen_test_site_name(sites[i]),
                       moqr_admin_listen_test_site_name(sites[j]), sites[i]);
                failures++;
            }
        }
    }
    /* And the names round-trip, so a diagnostic naming a site is naming the
     * site that was actually reached. */
    for (size_t i = 0; i < n; i++) {
        if (moqr_admin_listen_test_site_name(sites[i])[0] == '?') {
            printf("  sites: id %d has no name\n", sites[i]);
            failures++;
        }
    }
    return failures;
}

/*
 * A FAILED WAIT HANDS BACK NOTHING.
 *
 * Returning the current view alongside a 0 result makes the contract depend on
 * every caller remembering to check the result, and the current view is by
 * definition the stale one the caller already had. The output is written with
 * a sentinel first, so "nothing newer arrived" is visible in the payload as
 * well as in the return.
 */
static int
test_failed_wait_returns_no_payload(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t v;
    uint64_t gen;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  waitpayload: start refused\n");
        return 1;
    }
    /* A real, populated view first: the stale payload that must NOT come back. */
    {
        char buf[4096];
        (void)exchange(moqr_admin_listen_port(l),
                       "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                       sizeof(buf));
    }
    moqr_admin_listen_test_park_owner(1);
    (void)moqr_admin_listen_stop(l);   /* asked to stop; parked, it will not */
    if (moqr_admin_listen_test_view(l, &v) == 0u) {
        printf("  waitpayload: no view was ever published\n");
        failures++;
    }
    gen = v.gen;
    if (v.gen == 0u) {
        printf("  waitpayload: the populated view has generation 0\n");
        failures++;
    }

    /* Poison the caller's buffer, then wait for a publication that cannot
     * come. Both the result and the payload must say so. */
    memset(&v, 0xAB, sizeof(v));
    if (moqr_admin_listen_test_wait_view(l, gen, 200, &v) != 0u) {
        printf("  waitpayload: a wait with no publication reported "
               "success\n");
        failures++;
    }
    if (v.gen != 0u) {
        printf("  waitpayload: a failed wait returned generation %llu — a "
               "stale payload usable by a caller that did not check\n",
               (unsigned long long)v.gen);
        failures++;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v.fds[i] != 0 || v.state[i] != 0) {
            printf("  waitpayload: a failed wait left client %u populated\n",
                   i);
            failures++;
            break;
        }
    }
    moqr_admin_listen_test_park_owner(0);
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_stop_publisher(&rig);
    rig_release_resources(&rig);
    return failures;
}

/*
 * A view initialization that fails half way must release the half it took.
 *
 * Mutex and condition are two acquisitions; joining them with || left the mutex
 * initialized and unrecorded, so unwind could not release it. The arm is
 * injected because nothing else reaches it.
 */
static int
test_partial_view_init_unwinds(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_result_t rc;
    int balance_before;

    balance_before = moqr_admin_listen_test_view_mu_balance();
    moqr_admin_listen_test_fail_view_cv(1);
    rc = rig_start(&rig, &l);
    moqr_admin_listen_test_fail_view_cv(0);
    /* The half that WAS acquired must have been released. Nothing else on this
     * platform makes an undestroyed mutex visible, so the acquisition is
     * counted and the balance is the evidence. */
    if (moqr_admin_listen_test_view_mu_balance() != balance_before) {
        printf("  partialinit: the view lock was initialized and never "
               "destroyed (balance %d -> %d)\n", balance_before,
               moqr_admin_listen_test_view_mu_balance());
        failures++;
    }
    if (rc == MOQR_OK) {
        printf("  partialinit: a failed view initialization still started\n");
        failures++;
        rig_stop(&rig, l);
        return failures;
    }
    if (l != NULL) {
        printf("  partialinit: a refused start left a handle behind\n");
        failures++;
    }
    /* And the endpoint starts normally afterwards -- the failed attempt did
     * not leak a descriptor, a port or a lock that would prevent it. */
    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  partialinit: the endpoint could not start after a partial "
               "initialization -- something was not released\n");
        failures++;
    } else {
        rig_stop(&rig, l);
    }
    return failures;
}

/*
 * A SUCCESSFUL no-carrier release, with its identity.
 *
 * The refusal case proves the seam is exact; it cannot prove the operation,
 * because an injected failure returns before the broker is ever asked. This
 * runs the same path with nothing injected and requires exactly one successful
 * release through that site, naming a real serial and a real bank, and zero
 * traffic through either other site.
 */
static int
test_nocarrier_release_succeeds_once(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t before, after;
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  nocarrier-ok: start refused\n");
        return 1;
    }
    (void)moqr_admin_listen_test_view(l, &before);

    /* Signal-only demand, poisoned: no client and no generation record, so the
     * retirement goes straight to the broker. Nothing is injected. */
    atomic_store(&rig.poison_once, 1);
    atomic_store(&rig.poison, 1);
    atomic_store(&rig.latched, 1);
    moqr_admin_listen_notify(l);

    for (i = 0; i < 400; i++) {
        (void)moqr_admin_listen_test_view(l, &after);
        if (after.rel_ok[MOQR_ADMIN_REL_NOCARRIER] >
            before.rel_ok[MOQR_ADMIN_REL_NOCARRIER]) {
            break;
        }
        {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
    }
    if (i >= 400) {
        printf("  nocarrier-ok: the no-carrier site never released\n");
        failures++;
    }
    if (after.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] !=
        before.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] + 1u) {
        printf("  nocarrier-ok: %u attempts through the no-carrier site, "
               "expected exactly one\n",
               after.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] -
                   before.rel_attempt[MOQR_ADMIN_REL_NOCARRIER]);
        failures++;
    }
    if (after.rel_ok[MOQR_ADMIN_REL_NOCARRIER] !=
        before.rel_ok[MOQR_ADMIN_REL_NOCARRIER] + 1u) {
        printf("  nocarrier-ok: %u successful no-carrier releases, expected "
               "exactly one\n",
               after.rel_ok[MOQR_ADMIN_REL_NOCARRIER] -
                   before.rel_ok[MOQR_ADMIN_REL_NOCARRIER]);
        failures++;
    }
    if (after.rel_serial[MOQR_ADMIN_REL_NOCARRIER] == 0u) {
        printf("  nocarrier-ok: the release names serial 0\n");
        failures++;
    }
    if (after.rel_bank[MOQR_ADMIN_REL_NOCARRIER] >= MOQR_ADMIN_BANKS) {
        printf("  nocarrier-ok: the release names bank %u\n",
               after.rel_bank[MOQR_ADMIN_REL_NOCARRIER]);
        failures++;
    }
    if (after.rel_attempt[MOQR_ADMIN_REL_BANK] !=
            before.rel_attempt[MOQR_ADMIN_REL_BANK] ||
        after.rel_attempt[MOQR_ADMIN_REL_SETTLE] !=
            before.rel_attempt[MOQR_ADMIN_REL_SETTLE]) {
        printf("  nocarrier-ok: the retirement also went through another "
               "release site (bank +%u, settle +%u)\n",
               after.rel_attempt[MOQR_ADMIN_REL_BANK] -
                   before.rel_attempt[MOQR_ADMIN_REL_BANK],
               after.rel_attempt[MOQR_ADMIN_REL_SETTLE] -
                   before.rel_attempt[MOQR_ADMIN_REL_SETTLE]);
        failures++;
    }
    /* A no-carrier retirement has no admin record, so it owes no
     * acknowledgement and no settlement. */
    if (after.n_ack_release != before.n_ack_release ||
        after.n_settle != before.n_settle) {
        printf("  nocarrier-ok: the retirement acknowledged or settled an "
               "admin record it never had\n");
        failures++;
    }
    if (!wait_broker_idle(&rig, 3000)) {
        printf("  nocarrier-ok: the generation was never retired\n");
        failures++;
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  nocarrier-ok: health %u after a successful retirement\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * ONE ordinary generation, one ordinary release, counted exactly.
 *
 * The terminal fixture proves ownership across several clients; this proves
 * the ordinary release's cardinality on its own, so a duplicated success is
 * caught by a release oracle rather than by some later part of a large fixture
 * failing to assemble.
 */
static int
test_ordinary_release_happens_once(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t before, after;
    char buf[8192];
    int i;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  ordinary-once: start refused\n");
        return 1;
    }
    (void)moqr_admin_listen_test_view(l, &before);
    if (exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf)) <= 0 ||
        strstr(buf, "200 OK") == NULL) {
        printf("  ordinary-once: the scrape was not served\n");
        failures++;
    }
    for (i = 0; i < 400; i++) {
        (void)moqr_admin_listen_test_view(l, &after);
        if (after.n_ack_release > before.n_ack_release) {
            break;
        }
        {
            struct timespec ts = { 0, 5 * 1000 * 1000 };
            (void)nanosleep(&ts, NULL);
        }
    }
    if (i >= 400) {
        printf("  ordinary-once: the bank was never released\n");
        failures++;
    }
    if (after.rel_attempt[MOQR_ADMIN_REL_BANK] !=
        before.rel_attempt[MOQR_ADMIN_REL_BANK] + 1u) {
        printf("  ordinary-once: %u ordinary release attempts for one "
               "generation, expected exactly one\n",
               after.rel_attempt[MOQR_ADMIN_REL_BANK] -
                   before.rel_attempt[MOQR_ADMIN_REL_BANK]);
        failures++;
    }
    if (after.rel_ok[MOQR_ADMIN_REL_BANK] !=
        before.rel_ok[MOQR_ADMIN_REL_BANK] + 1u) {
        printf("  ordinary-once: %u successful ordinary releases, expected "
               "exactly one\n", after.rel_ok[MOQR_ADMIN_REL_BANK] -
                   before.rel_ok[MOQR_ADMIN_REL_BANK]);
        failures++;
    }
    if (after.n_ack_release != before.n_ack_release + 1u) {
        printf("  ordinary-once: %u acknowledgements, expected exactly one\n",
               after.n_ack_release - before.n_ack_release);
        failures++;
    }
    if (after.rel_serial[MOQR_ADMIN_REL_BANK] == 0u ||
        after.rel_bank[MOQR_ADMIN_REL_BANK] >= MOQR_ADMIN_BANKS) {
        printf("  ordinary-once: the release names serial %llu bank %u\n",
               after.rel_serial[MOQR_ADMIN_REL_BANK],
               after.rel_bank[MOQR_ADMIN_REL_BANK]);
        failures++;
    }
    if (after.rel_attempt[MOQR_ADMIN_REL_SETTLE] !=
            before.rel_attempt[MOQR_ADMIN_REL_SETTLE] ||
        after.rel_attempt[MOQR_ADMIN_REL_NOCARRIER] !=
            before.rel_attempt[MOQR_ADMIN_REL_NOCARRIER]) {
        printf("  ordinary-once: an ordinary generation also went through "
               "another release site\n");
        failures++;
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  ordinary-once: health %u after one ordinary generation\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/* -- the wait helper's own budget arithmetic -----------------------------
 *
 * A burst of publications that do not satisfy the predicate must not consume
 * the caller's budget. Charging a fixed slice per iteration does exactly that,
 * and the failure it produces is indistinguishable from "the fixture never
 * assembled" -- which is what it was mistaken for.
 *
 * This is not a stress loop. A counted burst is delivered while the wait is
 * running, larger than the old iteration count, and well inside the real
 * deadline; then the satisfying view is published.
 */
typedef struct burst_ctx {
    moqr_admin_listen_t *l;
    term_arm_t          *arm;      /* the barrier this burst is bound to     */
    int                  port;
    atomic_int           bursts;   /* ACKNOWLEDGED non-matching publications */
    atomic_int           armed;    /* the waiter was listening before we ran */
    atomic_int           done;
    atomic_ullong        last_gen;  /* the final acknowledged publication    */
    int                  fd;       /* written before done; read after join   */
} burst_ctx_t;

#define TERM_WAIT_BURST 64
/* Hang guards. One absolute bounded wait per event, never a refreshed loop. */
#define BURST_ARM_BUDGET_MS 4000
#define BURST_ACK_BUDGET_MS 2000   /* > the old 8000/250 = 32 iteration count */

static void *
burst_main(void *arg)
{
    burst_ctx_t *b = (burst_ctx_t *)arg;
    moqr_admin_listen_view_t v;
    uint64_t gen = 0;

    /*
     * ONE REQUEST, ONE ACKNOWLEDGED PUBLICATION.
     *
     * Counting calls to notify would count wake requests, and wake-pipe
     * traffic coalesces: several requests can produce one owner turn. What
     * this case needs to know is how many distinct views the wait was actually
     * offered, so each request waits for a new owner view generation before
     * the next one is issued. Every wait below is an event with a deadline
     * behind it; the deadline is a hang guard and never advances the count.
     */
    if (!term_arm_wait(b->arm, BURST_ARM_BUDGET_MS, &gen)) {
        atomic_store(&b->done, 1);
        return NULL;   /* the wait never armed; the case reports it */
    }
    atomic_store(&b->armed, 1);

    for (int i = 0; i < TERM_WAIT_BURST; i++) {
        uint64_t got;
        moqr_admin_listen_notify(b->l);
        got = moqr_admin_listen_test_wait_view(b->l, gen,
                                               BURST_ACK_BUDGET_MS, &v);
        if (got == 0u) {
            break;   /* no acknowledgement; the shortfall is the diagnostic */
        }
        gen = got;
        /* None of these satisfy the predicate: no client exists yet. */
        atomic_fetch_add(&b->bursts, 1);
        atomic_store(&b->last_gen, gen);
    }

    /* Now make the predicate true. */
    b->fd = dial(b->port);
    if (b->fd >= 0) {
        (void)write_all(b->fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    }
    atomic_store(&b->done, 1);
    return NULL;
}

/*
 * ONE ACQUISITION PATH for the barrier and the thread that uses it.
 *
 * The barrier and the producer are acquired in order, and a refusal at either
 * point releases exactly what was already taken. Folding this into the case
 * bodies is how the barrier came to be outside the lifetime proof in the first
 * place.
 */
static bool
burst_launch(burst_ctx_t *b, term_arm_t *arm, pthread_t *th,
             int fail_mu, int fail_cv, int fail_thread)
{
    if (!term_arm_init(arm, fail_mu, fail_cv)) {
        return false;
    }
    b->arm = arm;
    if (fail_thread || pthread_create(th, NULL, burst_main, b) != 0) {
        term_arm_release(arm);
        b->arm = NULL;
        return false;
    }
    return true;
}

static bool
view_any_client(const moqr_admin_listen_view_t *v)
{
    return view_has_a_client(v);
}

/*
 * THE FIXTURE'S OWN RESOURCES ARE BALANCED.
 *
 * Both the ordinary path and a refusal taken after the resources were already
 * acquired must leave the ledger level. A refusal that returns early without
 * unwinding, or a teardown that forgets one destructor, shows up here as a
 * count rather than as a leak nobody looks for.
 */
/*
 * A PARTIAL ACQUISITION UNWINDS EXACTLY WHAT IT TOOK.
 *
 * The rig takes three resources one at a time, so a refusal in the middle has
 * to release the ones already held and must not touch the ones it never
 * initialized. Destroying an object that was never initialized is outside the
 * POSIX contract, so the proof is the ledger: the class that was refused shows
 * no acquisition and no release at all.
 */
static bool
term_two_reading(const moqr_admin_listen_view_t *v)
{
    return term_count(v, CS_READING) >= 2;
}

/* Everything the boundary can say about a slot, in one line, so a decisive
 * turn can be described by what changed rather than by what was inferred. */
static void
peer_report(const char *when, const moqr_admin_listen_t *l,
            const moqr_admin_listen_view_t *v)
{
    printf("    %s: health=%u gen=%llu retiring=%d serial=%llu demand=%u "
           "pins=%d/%d\n",
           when, (unsigned)moqr_admin_listen_health(l),
           (unsigned long long)v->gen, v->retiring,
           (unsigned long long)v->retire_serial, v->retire_demand,
           v->pins[0], v->pins[1]);
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (v->state[i] == CS_FREE && v->drop_count[i] == 0 &&
            v->drop_attempt[i] == 0) {
            continue;
        }
        printf("      slot %u: state=%d fd=%d serial=%llu bytes=%llu "
               "drop=%u/%u attempt=%u\n", i, v->state[i], v->fds[i],
               v->serial[i], v->bytes[i], (unsigned)v->drop_site[i],
               (unsigned)v->drop_count[i], (unsigned)v->drop_attempt[i]);
    }
}

/*
 * TWO PEERS, ONE COLLECTING GENERATION -- ORDERED BY EVENTS.
 *
 * The question this answers is narrow: when two live clients are accepted and
 * then both present a complete request while publication is held, does either
 * of them get dropped? The two steps are separated so the answer cannot be an
 * artefact of the owner taking an accept and a request in the same turn: both
 * descriptors are established in READING slots first, acknowledged through the
 * published view, and only then are the requests written.
 *
 * The safety poll is removed for the case, so every turn the owner takes is
 * one an event asked for.
 */
static int
test_two_peers_share_one_generation(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t before;
    moqr_admin_listen_view_t after;
    int fds[2] = { -1, -1 };
    int port;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  twopeers: start refused\n");
        return 1;
    }
    port = moqr_admin_listen_port(l);
    moqr_admin_listen_test_set_poll_ms(-1);
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "clock",
                                            &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }

    /* Publication is held for the whole case: a generation that completes
     * would answer a waiter and end the scenario under test. */
    rig_hold_publication(&rig);
    if (!rig_wait_publisher_blocked(&rig, 4000)) {
        printf("  twopeers: the publisher never reached its barrier\n");
        failures++;
        goto done;
    }

    /* STEP ONE: two accepted descriptors, no requests yet. */
    for (int k = 0; k < 2; k++) {
        fds[k] = dial(port);
        if (fds[k] < 0) {
            printf("  twopeers: could not connect peer %d\n", k + 1);
            failures++;
            goto done;
        }
    }
    if (!term_wait(l, &before, term_two_reading, 8000)) {
        (void)moqr_admin_listen_test_view(l, &before);
        printf("  twopeers: the two peers were never both accepted into "
               "reading slots\n");
        peer_report("at the failure", l, &before);
        failures++;
        goto done;
    }
    printf("  twopeers: before the decisive turn\n");
    peer_report("accepted", l, &before);

    /* STEP TWO: both complete requests, then one settled observation. */
    for (int k = 0; k < 2; k++) {
        (void)write_all(fds[k], "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    }
    if (!term_wait(l, &after, term_two_waiting, 8000)) {
        (void)moqr_admin_listen_test_view(l, &after);
        printf("  twopeers: the two peers did not both join the collecting "
               "generation\n");
        peer_report("after", l, &after);
        failures++;
        goto done;
    }
    printf("  twopeers: after the decisive turn\n");
    peer_report("waiting", l, &after);

    /*
     * THE SHARED GENERATION, stated as the identity it is: both waiting peers
     * carry the SAME nonzero serial, and that serial is the one the broker is
     * currently collecting for HTTP demand. Publication is still held, so the
     * generation cannot have completed and been replaced underneath them.
     */
    {
        unsigned long long shared = 0;
        int waiting = 0;
        uint64_t cur = 0;
        uint32_t demand = 0;

        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            if (after.state[i] != CS_WAITING) {
                continue;
            }
            waiting++;
            if (after.serial[i] == 0u) {
                printf("  twopeers: slot %u is waiting on no generation\n", i);
                failures++;
            } else if (shared == 0u) {
                shared = after.serial[i];
            } else if (after.serial[i] != shared) {
                printf("  twopeers: the peers are on different generations "
                       "(%llu and %llu)\n", shared, after.serial[i]);
                failures++;
            }
        }
        if (waiting != 2) {
            printf("  twopeers: %d peers were waiting, expected 2\n", waiting);
            failures++;
        }
        if (!moqr_broker_current(&rig.broker, &cur, &demand)) {
            printf("  twopeers: the broker is collecting nothing while two "
                   "peers wait on it\n");
            failures++;
        } else {
            if ((unsigned long long)cur != shared) {
                printf("  twopeers: the peers wait on %llu but the broker is "
                       "collecting %llu\n", shared,
                       (unsigned long long)cur);
                failures++;
            }
            if ((demand & MOQR_BROKER_DEMAND_HTTP) == 0u) {
                printf("  twopeers: the shared generation carries no HTTP "
                       "demand (0x%x)\n", demand);
                failures++;
            }
        }
    }

    /* No live peer may be dropped on the way there. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (after.drop_count[i] != 0 || after.drop_attempt[i] != 0) {
            printf("  twopeers: slot %u was dropped (site %u, count %u, "
                   "attempt %u) while both peers were live\n", i,
                   (unsigned)after.drop_site[i],
                   (unsigned)after.drop_count[i],
                   (unsigned)after.drop_attempt[i]);
            failures++;
        }
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  twopeers: health %u after the decisive turn\n",
               (unsigned)moqr_admin_listen_health(l));
        failures++;
    }

done:
    moqr_admin_listen_test_set_poll_ms(MOQR_ADMIN_POLL_MS);
    rig_release_publication(&rig);
    for (int k = 0; k < 2; k++) {
        if (fds[k] >= 0) {
            (void)close(fds[k]);
        }
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * A REFUSED DESTROY IS NOT A RELEASE.
 *
 * The distinction between asking and succeeding is the same one the listener
 * itself was corrected for. A destroy that returns an error leaves an object
 * in a state nobody may rely on, so the fixture keeps ownership, does not
 * count it, and says so loudly enough that the run cannot end looking level.
 */
static int
test_rig_refused_destroy_is_not_a_release(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int cvd0, cv_fail0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  rigrefuse: start refused\n");
        return 1;
    }
    cvd0 = g_rig_cv_destroy;
    cv_fail0 = g_rig_release_failures;
    rig.fail_cv_destroy = 1;
    g_rig_expected_release_failures++;
    printf("    (one refused destroy is expected below)\n");
    rig_stop(&rig, l);

    if (g_rig_cv_destroy - cvd0 != 0) {
        printf("  rigrefuse: a refused destroy was counted as a release\n");
        failures++;
    }
    if (g_rig_release_failures - cv_fail0 != 1) {
        printf("  rigrefuse: the refusal was not recorded (%d)\n",
               g_rig_release_failures - cv_fail0);
        failures++;
    }
    if (!rig.pub_cv_live) {
        printf("  rigrefuse: ownership was given up despite the refusal\n");
        failures++;
    }
    /* Now let it go, so this case does not poison the run balance it is
     * testing. Both the count and the failure tally are corrected. */
    rig.fail_cv_destroy = 0;
    rig_release_resources(&rig);
    if (g_rig_cv_destroy - cvd0 != 1 || rig.pub_cv_live) {
        printf("  rigrefuse: the retry did not release the condition\n");
        failures++;
    }
    return failures;
}

/*
 * NO FLOOR, NO ARMING.
 *
 * The floor is the whole guarantee: without a real reading to compare
 * against, arming would install exactly the stale origin this refuses to
 * install. A clock that will not answer therefore refuses the arm rather than
 * taking the caller's word for the origin.
 */
/*
 * THE ARM BARRIER'S OWN LIFETIME.
 *
 * Four outcomes, each proved by the ledger rather than by inspection: a mutex
 * refusal takes nothing, a condition refusal takes and returns the mutex only,
 * a producer refusal returns the whole barrier, and the ordinary path is
 * balanced. Nothing here destroys an object it never initialized, and nothing
 * destroys one twice -- the ownership flags are the observation.
 */
/*
 * A THREAD IS A RESOURCE TOO.
 *
 * Creation and joining go through one narrow owner so a refusal traverses the
 * same acquisition path as a success, ownership is surrendered only when the
 * join actually succeeds, and joining something never created is caught here
 * rather than handed to pthreads. `owned` is the only authority on that: a
 * caller may not keep a second flag and clear it after a refused join.
 *
 * Whether a barrier may be released is decided by the barrier's own reacher
 * count, not by this owner -- see `term_arm_release`.
 */
static int g_watch_create_ok;
static int g_watch_create_refused;
static int g_watch_join_ok;
static int g_watch_join_failed;
static int g_watch_unowned_joins;
static int g_watch_detach_ok;
static int g_watch_detach_refused;
/* Refusals a case deliberately arranged, so the run gate can tell a proven
 * refusal from one nobody asked for. */
static int g_watch_expected_unowned;
static int g_watch_expected_join_failures;
static int g_watch_expected_create_refused;
/* Expectations deliberately stated with no ledger, so the run gate can tell a
 * proven refusal from one nobody asked for. */
static int g_expected_misdirected_expectations;
/* Mismatches a case deliberately arranged, so the run gate can tell a proven
 * mismatch from one that reached no verdict. */
static int g_expected_expectation_mismatches;
/* Probe facts a case deliberately arranged. */
static int g_expected_probe_errors;
static int g_expected_unprotected_samples;
static int g_expected_accessor_lock_failures;

typedef struct owned_thread {
    pthread_t   th;
    int         owned;
    int         fail_join;   /* fixture-only join refusal, lifted on retry */
    int         persistent;  /* a refusal that never lifts: a real failure */
    term_arm_t *reaches;     /* the barrier this thread can touch, if any */
} owned_thread_t;

/*
 * REACHABILITY IS THE OWNER'S BUSINESS.
 *
 * The barrier's reacher count is raised when the thread is acquired and
 * lowered when it is proven joined, both here. A caller cannot register or
 * cancel reachability on its own, so `owned` and the count cannot disagree.
 */
/* Every thread this owner currently holds, so a fail-stop can say plainly
 * which threads it is abandoning instead of leaving them unaccounted. */
#define OWNED_THREAD_SLOTS 8
static owned_thread_t *g_owned_threads[OWNED_THREAD_SLOTS];

static void
owned_thread_track(owned_thread_t *t)
{
    for (int i = 0; i < OWNED_THREAD_SLOTS; i++) {
        if (g_owned_threads[i] == NULL) {
            g_owned_threads[i] = t;
            return;
        }
    }
}

static void
owned_thread_untrack(owned_thread_t *t)
{
    for (int i = 0; i < OWNED_THREAD_SLOTS; i++) {
        if (g_owned_threads[i] == t) {
            g_owned_threads[i] = NULL;
        }
    }
}

static bool
owned_thread_create(owned_thread_t *t, void *(*fn)(void *), void *arg,
                    int fail_create, term_arm_t *reaches)
{
    t->owned = 0;
    t->fail_join = 0;
    t->persistent = 0;
    t->reaches = NULL;
    if (fail_create || pthread_create(&t->th, NULL, fn, arg) != 0) {
        g_watch_create_refused++;
        return false;
    }
    t->owned = 1;
    owned_thread_track(t);
    t->reaches = reaches;
    if (reaches != NULL) {
        reaches->reachers++;
    }
    g_watch_create_ok++;
    return true;
}

/* Ownership -- and reachability with it -- is surrendered only on a join that
 * actually succeeded. */
static bool
owned_thread_join(owned_thread_t *t)
{
    int rc;

    if (!t->owned) {
        g_watch_unowned_joins++;
        printf("  thread: a join was attempted on a thread nobody owns\n");
        return false;
    }
    rc = (t->fail_join || t->persistent) ? EBUSY : pthread_join(t->th, NULL);
    if (rc != 0) {
        g_watch_join_failed++;
        printf("  thread: the join refused (%d)\n", rc);
        return false;
    }
    t->owned = 0;
    owned_thread_untrack(t);
    if (t->reaches != NULL) {
        t->reaches->reachers--;
        t->reaches = NULL;
    }
    g_watch_join_ok++;
    return true;
}

/*
 * REPORTING ON THE WAY TO A FAIL-STOP IS BEST EFFORT AND BOUNDED.
 *
 * A fail-stop exists because some thread can still reach this frame. That
 * same thread may be blocked inside stdio holding a stream's lock, and the
 * stream may be a pipe nobody is draining -- so a plain fprintf/fflush before
 * _exit could wait forever on exactly the condition the stop is about. The
 * report therefore goes through a detached reporter thread that writes to the
 * descriptor itself, attempts the ordinary stdout flush, and then writes one
 * completion byte to a pipe. The stopping thread shares no lock with it: it
 * polls that pipe under ONE monotonic deadline of STOP_REPORT_BUDGET_MS and
 * stops regardless, so a reporter stalled anywhere -- including inside its
 * completion notification -- delays nothing beyond the deadline. No flag on
 * the standard streams is changed. If the pipe, the clock or the thread
 * cannot be had, the stop proceeds without a report.
 */
#define STOP_REPORT_BUDGET_MS 250

typedef struct stop_report {
    char    msg[512];
    size_t  len;
    int     done_fd;
} stop_report_t;

static void *
stop_report_main(void *arg)
{
    stop_report_t *r = (stop_report_t *)arg;
    const char *p = r->msg;
    size_t left = r->len;

    while (left > 0) {
        ssize_t w = write(STDERR_FILENO, p, left);
        if (w < 0 && errno == EINTR) {
            continue;
        }
        if (w <= 0) {
            break;
        }
        p += w;
        left -= (size_t)w;
    }
    (void)fflush(stdout);
    (void)write(r->done_fd, "D", 1);
    return NULL;
}

static int
stop_report_ms_until(const struct timespec *deadline)
{
    struct timespec now;
    long long ms;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    ms = (long long)(deadline->tv_sec - now.tv_sec) * 1000LL +
         (deadline->tv_nsec - now.tv_nsec) / 1000000L;
    if (ms <= 0) {
        return 0;
    }
    return ms > STOP_REPORT_BUDGET_MS ? STOP_REPORT_BUDGET_MS : (int)ms;
}

static void
stop_report_bounded(const char *fmt, ...)
{
    static stop_report_t r;   /* the process is about to stop; one report */
    pthread_t th;
    int done[2];
    struct timespec deadline;
    va_list ap;
    int n;

    va_start(ap, fmt);
    n = vsnprintf(r.msg, sizeof(r.msg), fmt, ap);
    va_end(ap);
    r.len = (n > 0 && (size_t)n < sizeof(r.msg)) ? (size_t)n
                                                 : sizeof(r.msg) - 1u;
    if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0 || pipe(done) != 0) {
        return;
    }
    deadline.tv_nsec += (long)STOP_REPORT_BUDGET_MS * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    r.done_fd = done[1];
    if (pthread_create(&th, NULL, stop_report_main, &r) != 0) {
        return;
    }
    /* Never joined: the process stops whether or not it finished. */
    (void)pthread_detach(th);
    for (;;) {
        struct pollfd pf = { .fd = done[0], .events = POLLIN, .revents = 0 };
        int left = stop_report_ms_until(&deadline);
        if (left <= 0) {
            break;
        }
        if (poll(&pf, 1, left) < 0 && errno == EINTR) {
            continue;   /* the same deadline, re-measured */
        }
        break;
    }
}

/*
 * RELINQUISH A THREAD THAT HAS PROVED IT REACHES NOTHING.
 *
 * A join is the wrong instrument once the thread has published that it will
 * never touch caller-owned storage again: there is nothing left to wait for,
 * and waiting is unbounded. Detach relinquishes the joinable resource instead,
 * and the ledger treats it as settling the acquisition exactly as a successful
 * join does. It is refused unless the release fact is proved, and a refused
 * detach is a fail-stop rather than a leak.
 */
static void
owned_thread_release_detach(owned_thread_t *t, const char *who)
{
    if (!t->owned) {
        g_watch_unowned_joins++;
        printf("  thread: a detach was attempted on a thread nobody owns\n");
        return;
    }
    if (pthread_detach(t->th) != 0) {
        g_watch_detach_refused++;
        stop_report_bounded("  %s: the detach refused; stopping before "
                            "teardown\n", who);
        _exit(90);
    }
    t->owned = 0;
    owned_thread_untrack(t);
    if (t->reaches != NULL) {
        t->reaches->reachers--;
        t->reaches = NULL;
    }
    g_watch_detach_ok++;
}

/*
 * A JOIN THAT WILL NOT SUCCEED IS NOT SOMETHING TO RETURN FROM.
 *
 * One arranged refusal is a fixture state and is lifted on the retry. A join
 * that still refuses is a real pthread failure, and the thread it names can
 * still reach this frame's storage -- so there is nothing safe to unwind to.
 * The process stops here, loudly, rather than tearing down stack the thread
 * may touch. A child case proves this path without ending the suite.
 */
static void
owned_thread_join_or_stop(owned_thread_t *t, const char *who)
{
    if (owned_thread_join(t)) {
        return;
    }
    t->fail_join = 0;
    if (owned_thread_join(t)) {
        return;
    }
    /*
     * The process is about to die, so no frame is unwound and no thread can
     * reach anything. Detaching every thread this owner still holds says the
     * abandonment is deliberate rather than forgotten, and leaves no
     * thread-leak report to be mistaken for one.
     */
    for (int i = 0; i < OWNED_THREAD_SLOTS; i++) {
        owned_thread_t *live = g_owned_threads[i];
        if (live != NULL && live->owned) {
            (void)pthread_detach(live->th);
            live->owned = 0;
            g_owned_threads[i] = NULL;
        }
    }
    stop_report_bounded("  %s: a thread could not be joined and can still "
                        "reach this frame; stopping before teardown\n", who);
    _exit(90);
}

static void *
arm_noop_main(void *arg)
{
    (void)arg;
    return NULL;
}

typedef struct arm_watch {
    term_arm_t *arm;
    int         budget_ms;
    bool        armed;
    uint64_t    gen;
} arm_watch_t;

static void *
arm_watch_main(void *arg)
{
    arm_watch_t *w = (arm_watch_t *)arg;

    w->armed = term_arm_wait(w->arm, w->budget_ms, &w->gen);
    return NULL;
}

#define ARM_B_SENTINEL 0xB0B0B0B0ull

typedef struct arm_a_ctx {
    moqr_admin_listen_t *l;
    term_arm_t          *arm;
    int                  budget_ms;
    bool                 satisfied;
} arm_a_ctx_t;

static void *
arm_a_main(void *arg)
{
    arm_a_ctx_t *c = (arm_a_ctx_t *)arg;
    moqr_admin_listen_view_t v;

    /* A generous guard: this wait is meant to end because its predicate came
     * true, and an expiry is a failure, not the mechanism. */
    c->satisfied = term_wait_armed(c->l, &v, view_any_client, c->budget_ms,
                                   c->arm);
    return NULL;
}

/*
 * A BARRIER BELONGS TO ITS OWN WAIT.
 *
 * When the wait found its barrier through a file-global selector, a wait on
 * one listener could publish into another case's barrier, releasing a producer
 * ordered against a different listener entirely. The association is an
 * argument now, so this is checkable: two listeners, two barriers, and a wait
 * armed on A must leave B's watcher exactly where it was.
 *
 * Every step here is an event. B's watcher announces that it is blocked on B.
 * A's wait announces the generation it armed on. B is then inspected under its
 * own lock, released with a value only this case writes -- so what the watcher
 * returns says who released it -- and A is finished by making its predicate
 * true, never by letting a budget expire.
 *
 * `stage` selects which acquisition is refused, so each unwind is proved on
 * its own: 1 refuses A, 2 refuses B, 3 refuses the watcher thread.
 */
static int
arm_association(int stage, int fail_join_b, int fail_join_a, int persistent_b)
{
    int failures = 0;
    rig_t rig_a;
    rig_t rig_b;
    moqr_admin_listen_t *la = NULL;
    moqr_admin_listen_t *lb = NULL;
    term_arm_t arm_a;
    term_arm_t arm_b;
    arm_watch_t watch_b;
    arm_a_ctx_t ctx_a;
    owned_thread_t th_b;
    owned_thread_t th_a;
    bool have_a = false;
    bool have_b = false;
    bool ran_a = false;      /* ctx_a describes a wait that actually ran */
    bool ran_b = false;      /* watch_b describes a watcher that actually ran */
    int fd = -1;
    uint64_t armed_gen = 0;
    int mu0 = g_arm_mu_init, mud0 = g_arm_mu_destroy;
    int cv0 = g_arm_cv_init, cvd0 = g_arm_cv_destroy;
    int cr0 = g_watch_create_ok, jn0 = g_watch_join_ok;
    int crf0 = g_watch_create_refused;
    int unsafe0 = g_arm_unsafe_release_attempts;

    /* EVERY context is complete before anything can jump to the tail, and the
     * tail reads none of them unless the acquisition that makes them
     * authoritative actually happened. */
    memset(&th_a, 0, sizeof(th_a));
    memset(&th_b, 0, sizeof(th_b));
    memset(&watch_b, 0, sizeof(watch_b));
    memset(&ctx_a, 0, sizeof(ctx_a));

    if (rig_start(&rig_a, &la) != MOQR_OK) {
        printf("  armassoc: listener A refused to start\n");
        return 1;
    }
    if (rig_start(&rig_b, &lb) != MOQR_OK) {
        printf("  armassoc: listener B refused to start\n");
        rig_stop(&rig_a, la);
        return 1;
    }

    /* STAGED. Each resource is taken and recorded before the next is
     * attempted, so a refusal releases exactly what it took. */
    have_a = term_arm_init(&arm_a, stage == 1, 0);
    if (have_a) {
        have_b = term_arm_init(&arm_b, 0, stage == 2);
    }
    if (have_a && have_b) {
        watch_b.arm = &arm_b;
        watch_b.budget_ms = 8000;
        watch_b.armed = true;
        watch_b.gen = 0;
        if (stage == 3) {
            g_watch_expected_create_refused++;
        }
        if (owned_thread_create(&th_b, arm_watch_main, &watch_b, stage == 3,
                                &arm_b)) {
            ran_b = true;
            th_b.fail_join = fail_join_b;
            th_b.persistent = persistent_b;
            /* The owner registered reachability, positively. Without it the
             * release guard has nothing to guard. */
            if (arm_b.reachers != 1) {
                printf("  armassoc: B's barrier records %d reacher(s) after "
                       "its thread was acquired, expected 1\n",
                       arm_b.reachers);
                failures++;
            }
        }
    }

    if (stage == 0 || stage == 4 || stage == 5) {
        if (!have_a || !have_b || !th_b.owned) {
            printf("  armassoc: the ordinary setup refused\n");
            failures++;
            goto done;
        }
        /* Ordered against the fact that B has a waiter, not against time. */
        if (!term_arm_wait_for_waiter(&arm_b, 8000)) {
            printf("  armassoc: nobody ever waited on B\n");
            failures++;
            goto done;
        }

        /* A's wait runs under its own ownership and announces when it has
         * armed. Stage 5 refuses that acquisition instead. */
        ctx_a.l = la;
        ctx_a.arm = &arm_a;
        ctx_a.budget_ms = 8000;
        ctx_a.satisfied = false;
        if (stage == 5) {
            g_watch_expected_create_refused++;
        }
        if (!owned_thread_create(&th_a, arm_a_main, &ctx_a, stage == 5,
                                 &arm_a)) {
            if (stage != 5) {
                printf("  armassoc: A's wait could not be started\n");
                failures++;
            }
            goto done;
        }
        ran_a = true;
        th_a.fail_join = fail_join_a;
        if (arm_a.reachers != 1) {
            printf("  armassoc: A's barrier records %d reacher(s) after its "
                   "thread was acquired, expected 1\n", arm_a.reachers);
            failures++;
        }
        if (!term_arm_wait(&arm_a, 8000, &armed_gen)) {
            printf("  armassoc: the wait never armed its own barrier\n");
            failures++;
            goto done;
        }

        /* Immediately, under B's own lock. */
        if (term_arm_is_armed(&arm_b)) {
            printf("  armassoc: B's barrier was armed by a wait on A\n");
            failures++;
        }

        /* Release B with a value only this case writes. */
        term_arm_publish(&arm_b, ARM_B_SENTINEL);
        /* Finish A by making its predicate TRUE. */
        fd = dial(moqr_admin_listen_port(la));
        if (fd < 0) {
            printf("  armassoc: A's predicate could not be satisfied\n");
            failures++;
        }
    } else {
        /* A refusal stage: prove exactly what was acquired came back. */
        if (stage == 1 && (have_a || have_b || th_b.owned)) {
            printf("  armassoc: a refused first barrier still produced "
                   "state\n");
            failures++;
        }
        if (stage == 2 && (!have_a || have_b || th_b.owned)) {
            printf("  armassoc: a refused second barrier left the wrong "
                   "state\n");
            failures++;
        }
        if (stage == 3) {
            if (!have_a || !have_b || th_b.owned) {
                printf("  armassoc: a refused watcher left the wrong "
                       "state\n");
                failures++;
            }
            if (g_watch_create_refused - crf0 != 1) {
                printf("  armassoc: the injected create refusal was recorded "
                       "%d time(s), expected once\n",
                       g_watch_create_refused - crf0);
                failures++;
            }
            if (arm_b.reachers != 0) {
                printf("  armassoc: a refused create still registered "
                       "reachability\n");
                failures++;
            }
        }
    }

done:
    /*
     * OWNERSHIP DECIDES, AND ONLY A REAL JOIN GIVES IT UP.
     *
     * The completion events are re-published so a thread still blocked can
     * finish, the arranged refusal is lifted on the retry, and a join that
     * still refuses stops the process rather than unwinding a frame the thread
     * can reach. Reachability is not touched here at all -- the thread owner
     * maintains it.
     */
    if (th_b.owned) {
        term_arm_publish(&arm_b, ARM_B_SENTINEL);
        owned_thread_join_or_stop(&th_b, "armassoc B");
    }
    if (th_a.owned) {
        if (fd < 0) {
            fd = dial(moqr_admin_listen_port(la));
        }
        owned_thread_join_or_stop(&th_a, "armassoc A");
    }
    if (stage == 5 && g_watch_create_refused - crf0 != 1) {
        printf("  armassoc: A's injected create refusal was recorded %d "
               "time(s), expected once\n", g_watch_create_refused - crf0);
        failures++;
    }
    if (stage == 5 && arm_a.reachers != 0) {
        printf("  armassoc: A's refused create still registered "
               "reachability\n");
        failures++;
    }
    if (ran_a && !ctx_a.satisfied) {
        printf("  armassoc: A's wait ended without its predicate — the "
               "budget expired instead of the event arriving\n");
        failures++;
    }
    if (ran_b && !watch_b.armed) {
        printf("  armassoc: B's watcher was never released\n");
        failures++;
    }
    if (ran_b && watch_b.armed && watch_b.gen != ARM_B_SENTINEL) {
        printf("  armassoc: B's watcher returned %llu, not this case's "
               "sentinel — something else released it\n",
               (unsigned long long)watch_b.gen);
        failures++;
    }
    if (have_b) {
        term_arm_release(&arm_b);
    }
    if (have_a) {
        term_arm_release(&arm_a);
    }
    if (fd >= 0) {
        (void)close(fd);
    }
    /* Whatever this stage took, it gave back -- barriers and threads alike --
     * and nothing was released while a thread could still reach it. */
    if (g_arm_mu_init - mu0 != g_arm_mu_destroy - mud0 ||
        g_arm_cv_init - cv0 != g_arm_cv_destroy - cvd0) {
        printf("  armassoc: stage %d left barriers behind — mutex %d/%d, "
               "condition %d/%d\n", stage,
               g_arm_mu_destroy - mud0, g_arm_mu_init - mu0,
               g_arm_cv_destroy - cvd0, g_arm_cv_init - cv0);
        failures++;
    }
    if (g_watch_create_ok - cr0 != g_watch_join_ok - jn0) {
        printf("  armassoc: stage %d created %d thread(s) and joined %d\n",
               stage, g_watch_create_ok - cr0, g_watch_join_ok - jn0);
        failures++;
    }
    if (g_arm_unsafe_release_attempts - unsafe0 != 0) {
        printf("  armassoc: stage %d released a barrier a thread could still "
               "reach (%d attempt(s))\n", stage,
               g_arm_unsafe_release_attempts - unsafe0);
        failures++;
    }
    rig_stop(&rig_b, lb);
    rig_stop(&rig_a, la);
    return failures;
}

static int
test_arm_belongs_to_its_own_wait(void)
{
    int failures = 0;

    failures += arm_association(0, 0, 0, 0);
    failures += arm_association(1, 0, 0, 0);
    failures += arm_association(2, 0, 0, 0);
    failures += arm_association(3, 0, 0, 0);
    failures += arm_association(5, 0, 0, 0);   /* A's create refuses */
    return failures;
}

/*
 * A REFUSED JOIN HOLDS THE BARRIER.
 *
 * Each side is refused once, on its own. Ownership survives the refusal, so
 * the barrier it reaches is not released; the completion event is still
 * there; the retry goes through the same owner and joins the real thread; and
 * only then does the release happen. The order is a ledger, not a hope: a
 * release attempted while a thread can still reach the barrier is counted and
 * named at the stage boundary and again for the whole run.
 */
static int
test_join_refusal_holds_the_barrier(void)
{
    int failures = 0;
    int unsafe0 = g_arm_unsafe_release_attempts;
    int failed0 = g_watch_join_failed;

    g_watch_expected_join_failures += 2;
    printf("    (two refused joins are expected below)\n");
    failures += arm_association(0, 1, 0, 0);   /* B's watcher refuses once */
    failures += arm_association(0, 0, 1, 0);   /* A's waiter refuses once */

    if (g_watch_join_failed - failed0 != 2) {
        printf("  joinhold: %d refused join(s), expected 2\n",
               g_watch_join_failed - failed0);
        failures++;
    }
    if (g_arm_unsafe_release_attempts - unsafe0 != 0) {
        printf("  joinhold: a barrier was released while its thread was "
               "still owned (%d attempt(s))\n",
               g_arm_unsafe_release_attempts - unsafe0);
        failures++;
    }
    return failures;
}

/*
 * THE THREAD OWNER REFUSES WHAT IT DOES NOT OWN.
 *
 * Joining a thread nobody created is caught here, before pthreads is asked to
 * do something undefined with an indeterminate handle.
 */
/*
 * A JOIN THAT NEVER SUCCEEDS STOPS THE PROCESS.
 *
 * One arranged refusal is a fixture state that the retry lifts. A refusal that
 * persists is a real pthread failure, and the thread it names can still reach
 * the frame the caller is about to unwind -- so returning into ordinary
 * teardown would be releasing storage somebody may still touch. This runs the
 * path in a child so the suite can observe the stop instead of suffering it.
 */
static int
test_persistent_join_refusal_stops(void)
{
    int failures = 0;
    pid_t pid;
    int status = 0;

    printf("    (a child is expected to stop with 90 below)\n");
    fflush(stdout);
    pid = fork();
    if (pid < 0) {
        printf("  joinstop: could not fork\n");
        return 1;
    }
    if (pid == 0) {
        /* A refusal that never lifts. Reaching the end of this call at all
         * would mean the fixture returned into teardown. */
        (void)arm_association(0, 1, 0, 1);
        stop_report_bounded("  joinstop: the child returned into teardown "
                            "with a thread it could not join\n");
        _exit(11);
    }
    if (waitpid(pid, &status, 0) != pid) {
        printf("  joinstop: could not wait for the child\n");
        return 1;
    }
    if (!WIFEXITED(status)) {
        printf("  joinstop: the child did not exit normally (status %d)\n",
               status);
        failures++;
    } else if (WEXITSTATUS(status) != 90) {
        printf("  joinstop: the child exited %d, expected the fail-stop 90\n",
               WEXITSTATUS(status));
        failures++;
    }
    return failures;
}

static int
test_thread_owner_refuses_an_unowned_join(void)
{
    int failures = 0;
    owned_thread_t t;
    int unowned0 = g_watch_unowned_joins;
    int failed0 = g_watch_join_failed;
    int cr0 = g_watch_create_ok, jn0 = g_watch_join_ok;

    memset(&t, 0, sizeof(t));
    g_watch_expected_unowned++;
    printf("    (one unowned join is expected below)\n");
    if (owned_thread_join(&t)) {
        printf("  threadown: joining an unowned thread reported success\n");
        failures++;
    }
    if (g_watch_unowned_joins - unowned0 != 1) {
        printf("  threadown: the unowned join was not recorded\n");
        failures++;
    }

    /* A refused join keeps ownership, so the thread is still there to join. */
    if (!owned_thread_create(&t, arm_noop_main, NULL, 0, NULL)) {
        printf("  threadown: the ordinary create refused\n");
        return failures + 1;
    }
    t.fail_join = 1;
    g_watch_expected_join_failures++;
    printf("    (one refused join is expected below)\n");
    if (owned_thread_join(&t)) {
        printf("  threadown: a refused join reported success\n");
        failures++;
    }
    if (g_watch_join_failed - failed0 != 1) {
        printf("  threadown: the refused join was not recorded\n");
        failures++;
    }
    if (!t.owned) {
        printf("  threadown: a refused join surrendered ownership\n");
        failures++;
    }
    t.fail_join = 0;
    if (!owned_thread_join(&t)) {
        printf("  threadown: the retry did not join\n");
        failures++;
    }
    if (t.owned) {
        printf("  threadown: a successful join kept ownership\n");
        failures++;
    }
    if (g_watch_create_ok - cr0 != g_watch_join_ok - jn0) {
        printf("  threadown: %d created, %d joined\n",
               g_watch_create_ok - cr0, g_watch_join_ok - jn0);
        failures++;
    }
    return failures;
}

static int
test_arm_lifetime_unwinds(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    burst_ctx_t b;
    term_arm_t arm;
    pthread_t th;
    int mu0, mud0, cv0, cvd0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  armlife: start refused\n");
        return 1;
    }
    memset(&b, 0, sizeof(b));
    b.l = l;
    b.port = moqr_admin_listen_port(l);
    b.fd = -1;

    /* The mutex refuses: nothing was taken. */
    mu0 = g_arm_mu_init; mud0 = g_arm_mu_destroy;
    cv0 = g_arm_cv_init; cvd0 = g_arm_cv_destroy;
    if (burst_launch(&b, &arm, &th, 1, 0, 0)) {
        printf("  armlife: a refused mutex still produced a barrier\n");
        failures++;
    }
    if (g_arm_mu_init - mu0 != 0 || g_arm_mu_destroy - mud0 != 0 ||
        g_arm_cv_init - cv0 != 0 || g_arm_cv_destroy - cvd0 != 0) {
        printf("  armlife: a refused mutex still moved the ledger — "
               "mutex %d/%d, condition %d/%d\n",
               g_arm_mu_destroy - mud0, g_arm_mu_init - mu0,
               g_arm_cv_destroy - cvd0, g_arm_cv_init - cv0);
        failures++;
    }

    /* The condition refuses: the mutex alone was taken, and returned. */
    mu0 = g_arm_mu_init; mud0 = g_arm_mu_destroy;
    cv0 = g_arm_cv_init; cvd0 = g_arm_cv_destroy;
    if (burst_launch(&b, &arm, &th, 0, 1, 0)) {
        printf("  armlife: a refused condition still produced a barrier\n");
        failures++;
    }
    if (g_arm_mu_init - mu0 != 1 || g_arm_mu_destroy - mud0 != 1) {
        printf("  armlife: the mutex was not taken and returned exactly once "
               "(%d/%d)\n", g_arm_mu_destroy - mud0, g_arm_mu_init - mu0);
        failures++;
    }
    if (g_arm_cv_init - cv0 != 0 || g_arm_cv_destroy - cvd0 != 0) {
        printf("  armlife: a condition that was never acquired was released "
               "%d time(s)\n", g_arm_cv_destroy - cvd0);
        failures++;
    }

    /* The producer refuses: the whole barrier comes back. */
    mu0 = g_arm_mu_init; mud0 = g_arm_mu_destroy;
    cv0 = g_arm_cv_init; cvd0 = g_arm_cv_destroy;
    if (burst_launch(&b, &arm, &th, 0, 0, 1)) {
        printf("  armlife: a refused producer still reported success\n");
        failures++;
    }
    if (g_arm_mu_init - mu0 != 1 || g_arm_mu_destroy - mud0 != 1 ||
        g_arm_cv_init - cv0 != 1 || g_arm_cv_destroy - cvd0 != 1) {
        printf("  armlife: a refused producer left the barrier behind — "
               "mutex %d/%d, condition %d/%d\n",
               g_arm_mu_destroy - mud0, g_arm_mu_init - mu0,
               g_arm_cv_destroy - cvd0, g_arm_cv_init - cv0);
        failures++;
    }
    if (b.arm != NULL) {
        printf("  armlife: a refused producer left a barrier pointer "
               "behind\n");
        failures++;
    }

    /* And the ordinary path. */
    mu0 = g_arm_mu_init; mud0 = g_arm_mu_destroy;
    cv0 = g_arm_cv_init; cvd0 = g_arm_cv_destroy;
    if (!burst_launch(&b, &arm, &th, 0, 0, 0)) {
        printf("  armlife: the ordinary launch refused\n");
        rig_stop(&rig, l);
        return failures + 1;
    }
    term_arm_publish(&arm, 1u);   /* release the producer's barrier wait */
    (void)pthread_join(th, NULL);
    if (b.fd >= 0) {
        (void)close(b.fd);
        b.fd = -1;
    }
    term_arm_release(&arm);
    if (g_arm_mu_init - mu0 != 1 || g_arm_mu_destroy - mud0 != 1 ||
        g_arm_cv_init - cv0 != 1 || g_arm_cv_destroy - cvd0 != 1) {
        printf("  armlife: the ordinary path was not balanced — "
               "mutex %d/%d, condition %d/%d\n",
               g_arm_mu_destroy - mud0, g_arm_mu_init - mu0,
               g_arm_cv_destroy - cvd0, g_arm_cv_init - cv0);
        failures++;
    }
    if (arm.mu_live || arm.cv_live) {
        printf("  armlife: a released barrier is still owned "
               "(mutex %d, condition %d)\n", arm.mu_live, arm.cv_live);
        failures++;
    }

    /* And a destroy that refuses is not a release. */
    cv0 = g_arm_cv_init; cvd0 = g_arm_cv_destroy;
    if (!term_arm_init(&arm, 0, 0)) {
        printf("  armlife: the refusal phase could not build a barrier\n");
        rig_stop(&rig, l);
        return failures + 1;
    }
    arm.fail_cv_destroy = 1;
    g_arm_expected_release_failures++;
    printf("    (one refused arm destroy is expected below)\n");
    term_arm_release(&arm);
    if (g_arm_cv_destroy - cvd0 != 0) {
        printf("  armlife: a refused destroy was counted as a release\n");
        failures++;
    }
    if (!arm.cv_live) {
        printf("  armlife: ownership was given up despite the refusal\n");
        failures++;
    }
    arm.fail_cv_destroy = 0;
    term_arm_release(&arm);
    if (g_arm_cv_destroy - cvd0 != 1 || arm.cv_live) {
        printf("  armlife: the retry did not release the condition\n");
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

static int
test_arming_without_a_clock_refuses(void)
{
    int failures = 0;
    int armed = 1;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t v;
    uint64_t logical_now = 1;
    uint64_t last_real = 1;
    uint64_t gen;
    char buf[4096];
    ssize_t n;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  armnoclock: start refused\n");
        return 1;
    }
    /*
     * The seam refuses only the sample ARMING takes. Reusing the owner's own
     * clock-failure seam would let this case terminate the endpoint for
     * NO_CLOCK on the next turn and still report a pass, because the endpoint
     * dying is not something the arming assertions look at.
     */
    moqr_admin_listen_test_fail_arm_clock(1);
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, false, "armnoclock",
                                            &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &logical_now, &last_real);
    /* The SELECTOR is the thing that decides which clock the owner reads, so
     * it is the thing a refusal must not have moved. A zero value proves
     * nothing on its own -- a clock can be selected with the value still
     * zero, and the owner would then read that zero. */
    if (armed) {
        printf("  armnoclock: a refused arm still selected the logical "
               "clock\n");
        failures++;
    }
    if (logical_now != 0u) {
        printf("  armnoclock: a refused arm still installed an origin "
               "(%llu)\n", (unsigned long long)logical_now);
        failures++;
    }

    /* The owner took a turn -- acknowledged, not assumed -- and is still
     * healthy while the seam is set. */
    gen = moqr_admin_listen_test_view(l, &v);
    moqr_admin_listen_notify(l);
    if (moqr_admin_listen_test_wait_view(l, gen, 2000, &v) == 0u) {
        printf("  armnoclock: the owner took no turn after the refusal\n");
        failures++;
    }
    if (moqr_admin_listen_health(l) != MOQR_ADMIN_HEALTH_OK) {
        printf("  armnoclock: the refusal terminated the endpoint (health "
               "%u)\n", (unsigned)moqr_admin_listen_health(l));
        failures++;
    }

    /* And it is still serving. */
    n = exchange(moqr_admin_listen_port(l),
                 "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", buf,
                 sizeof(buf));
    if (n <= 0 || strncmp(buf, "HTTP/1.1 200", 12) != 0) {
        printf("  armnoclock: the endpoint stopped serving after a refused "
               "arm (%zd bytes)\n", n);
        failures++;
    }

    moqr_admin_listen_test_fail_arm_clock(0);
    rig_stop(&rig, l);
    return failures;
}

/*
 * CHOOSING A CLOCK AND READING IT ARE ONE STEP.
 *
 * If the owner released the view lock between deciding to use real time and
 * sampling it, arming could land in the gap: the owner would go on to use a
 * later reading than the origin arming installed, and its next turn would read
 * a time before the one it had just used -- expiring live deadlines and
 * dropping live clients.
 *
 * The owner answers this itself, at the sample point, from a marker the same
 * checked lock boundary maintains: while the step is indivisible this thread
 * holds the lock, and the probe says so. Neither wrong state -- nobody holding
 * it, or somebody else holding it -- can be mistaken for the right one. That
 * is a state, not a race: no second thread, no scheduling and no elapsed time
 * takes part in the verdict.
 *
 * The shape itself -- one checked take, the sample inside it, and the marker
 * written only by that boundary -- is pinned in `check_listener_contracts`,
 * because an edit that unlocks early while leaving the marker set is not a
 * state any runtime oracle can see.
 */
#define POLL_ACK_BUDGET_MS 4000
/* Bounds a mutant, not an ordinary run. */
#define COMPLETION_SEAM_BUDGET_MS 1500

/*
 * ONE DEADLINE, WITH AN IDENTITY.
 *
 * Every phase of the B transaction -- contention, acknowledgement, completion
 * and cleanup -- spends the SAME monotonic budget. A helper that mints its own
 * would let the transaction run for as many budgets as it has phases, and the
 * only symptom would be a longer run, which is not a verdict. So the deadline
 * is an object with a generation, its issuance is counted, and a case can
 * require that exactly one was issued and that every phase carried that one.
 */
typedef struct txn_deadline {
    struct timespec at;
    unsigned        id;
} txn_deadline_t;

static unsigned g_txn_deadlines_issued;

static txn_deadline_t
txn_deadline_new(int budget_ms)
{
    txn_deadline_t d;

    memset(&d, 0, sizeof(d));
    if (clock_gettime(CLOCK_MONOTONIC, &d.at) != 0) {
        d.id = 0u;
        return d;
    }
    d.at.tv_sec += budget_ms / 1000;
    d.at.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (d.at.tv_nsec >= 1000000000L) {
        d.at.tv_sec++;
        d.at.tv_nsec -= 1000000000L;
    }
    g_txn_deadlines_issued++;
    d.id = g_txn_deadlines_issued;
    return d;
}

/* What is left of THIS deadline, in milliseconds; <= 0 once it has passed. */
static int
txn_remaining_ms(const txn_deadline_t *d)
{
    struct timespec now;
    long ms;

    if (d->id == 0u || clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    ms = (long)(d->at.tv_sec - now.tv_sec) * 1000L +
         (d->at.tv_nsec - now.tv_nsec) / 1000000L;
    return ms > 0 ? (int)ms : 0;
}

/* A platform condition wait needs a realtime instant; it is DERIVED from the
 * remaining monotonic budget and is never itself the authority. */
static void
txn_realtime_from(const txn_deadline_t *d, struct timespec *out)
{
    int left = txn_remaining_ms(d);

    if (clock_gettime(CLOCK_REALTIME, out) != 0) {
        memset(out, 0, sizeof(*out));
        return;
    }
    out->tv_sec += left / 1000;
    out->tv_nsec += (long)(left % 1000) * 1000000L;
    if (out->tv_nsec >= 1000000000L) {
        out->tv_sec++;
        out->tv_nsec -= 1000000000L;
    }
}


/*
 * Wait for THIS request to be acknowledged with THIS value, under ONE absolute
 * deadline for the whole operation.
 *
 * A per-wait budget renews itself: an owner that keeps publishing views while
 * never acknowledging would be waited on forever. The deadline here can only
 * reject a missing acknowledgement; nothing about it can create one.
 */
static bool
await_poll_ack_within(moqr_admin_listen_t *l, unsigned want_gen, int want_ms,
                      const txn_deadline_t *deadline, const char *who)
{
    moqr_admin_listen_view_t v;
    uint64_t gen = moqr_admin_listen_test_view(l, &v);

    if (deadline->id == 0u) {
        printf("  %s: no transaction deadline to spend\n", who);
        return false;
    }
    for (;;) {
        int left;
        int ack_ms = 0;
        int adopted_ms = 0;
        uint64_t got;

        int drain_held = 0;
        int answered = moqr_admin_listen_test_poll_ack_for(l, want_gen,
                                                           &ack_ms,
                                                           &adopted_ms,
                                                           &drain_held);
        if (answered < 0) {
            /* A later generation was answered and this one never was:
             * skipped, not slow. Spending the rest of the deadline on it
             * would report a timeout for something that is not coming. */
            printf("  %s: request %u was skipped — a later generation was "
                   "acknowledged without it\n", who, want_gen);
            return false;
        }
        if (answered > 0) {
            if (drain_held != 1) {
                printf("  %s: request %u was drained without the view lock "
                       "held by the owner\n", who, want_gen);
                return false;
            }
            if (ack_ms != want_ms || adopted_ms != want_ms) {
                printf("  %s: request %u was acknowledged with %d and adopted "
                       "%d, expected %d\n", who, want_gen, ack_ms, adopted_ms,
                       want_ms);
                return false;
            }
            return true;
        }
        left = txn_remaining_ms(deadline);
        if (left <= 0) {
            printf("  %s: request %u was never acknowledged\n", who,
                   want_gen);
            return false;
        }
        got = moqr_admin_listen_test_wait_view(l, gen,
                                               left > 200 ? 200 : left, &v);
        if (got != 0u) {
            gen = got;
        }
    }
}

/* A single-phase caller still spends one identified deadline. */
static bool
await_poll_ack(moqr_admin_listen_t *l, unsigned want_gen, int want_ms,
               const char *who)
{
    txn_deadline_t d = txn_deadline_new(POLL_ACK_BUDGET_MS);

    return await_poll_ack_within(l, want_gen, want_ms, &d, who);
}

#define CLOCK_STEP_TURNS 8
#define CLOCK_STEP_HITS_PER_TURN 1u

static int
test_clock_choice_and_sample_are_one_step(void)
{
    int failures = 0;
    unsigned req_gen = 0;
    unsigned ack_gen = 0;
    int ack_ms = 0;
    int adopted_ms = 0;
    moqr_admin_listen_probe_t p0;
    moqr_admin_listen_probe_t p1;
    int armed = 1;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t v;
    uint64_t gen;
    uint64_t logical_now = 0;
    uint64_t last_real = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  clockstep: start refused\n");
        return 1;
    }

    /*
     * THE TICK IS DISARMED AFTER ACTIVATION, AND THE OWNER SAYS SO ITSELF.
     *
     * A publication is not an acknowledgement that a setting was adopted: the
     * view the test sees after its own notify may belong to a turn that was
     * already in flight. So the request is listener-bound and generational,
     * and the owner publishes the answer only after adopting the timeout for
     * the poll it is about to enter, naming the request it answers and the
     * value it took. Activation itself runs under the ordinary tick; the mode
     * changes only afterwards, by request. The single deadline inside the wait
     * can only fail a missing acknowledgement; it can never stand in for one.
     */
    /*
     * EACH REQUEST IS OBSERVED IN ITS OWN RIGHT.
     *
     * Submitting two requests and looking only at the second proves nothing
     * about generations: an acknowledgement hard-wired to that number would
     * satisfy it. The first is awaited and checked before the second is even
     * issued, so a constant answer is wrong for one of them whichever
     * constant it is.
     */
    req_gen = moqr_admin_listen_test_request_poll_ms(l, MOQR_ADMIN_POLL_MS);
    if (!await_poll_ack(l, req_gen, MOQR_ADMIN_POLL_MS, "clockstep")) {
        rig_stop(&rig, l);
        return 1;
    }
    req_gen = moqr_admin_listen_test_request_poll_ms(l, -1);
    if (!await_poll_ack(l, req_gen, -1, "clockstep")) {
        rig_stop(&rig, l);
        return 1;
    }
    gen = moqr_admin_listen_test_view(l, &v);
    (void)ack_gen; (void)ack_ms; (void)adopted_ms;

    /*
     * An EXACT number of owner turns, each requested and each acknowledged
     * through the published view. Every one of them samples the real clock,
     * so the probe must have run exactly once per turn -- the positive half
     * of the proof. A counter that only ever reads zero would say the same
     * thing whether the step were sound or the probe had been deleted.
     */
    moqr_admin_listen_test_clock_probe(l, &p0);
    /*
     * The counts are read FROM THE PUBLISHED VIEWS, not from a separate
     * accessor: the owner samples its clock at the top of a turn and publishes
     * at the end of it, so an accessor read after a publication races the next
     * sample. A snapshot taken inside the same critical section that publishes
     * cannot.
     */
    gen = moqr_admin_listen_test_view(l, &v);
    p0 = v.probe;
    for (int i = 0; i < CLOCK_STEP_TURNS; i++) {
        uint64_t got;
        unsigned before = v.probe.hits;

        moqr_admin_listen_notify(l);
        got = moqr_admin_listen_test_wait_view(l, gen, 2000, &v);
        if (got == 0u) {
            printf("  clockstep: the owner did not acknowledge turn %d\n",
                   i + 1);
            failures++;
            break;
        }
        gen = got;
        /* EXACTLY ONE. The owner reads its clock once per turn, so a turn
         * that reached the probe twice, or not at all, is not the step this
         * case is about. Learning the number from the implementation under
         * test would let that implementation define its own contract. */
        /* The mode as EXECUTED, for the turn this view describes. */
        if (v.probe.poll_used_ms != -1) {
            printf("  clockstep: turn %d polled with %d, not the acknowledged "
                   "-1\n", i + 1, v.probe.poll_used_ms);
            failures++;
            break;
        }
        if (v.probe.hits - before != CLOCK_STEP_HITS_PER_TURN) {
            printf("  clockstep: turn %d reached the probe %u time(s), "
                   "expected %u\n", i + 1, v.probe.hits - before,
                   (unsigned)CLOCK_STEP_HITS_PER_TURN);
            failures++;
            break;
        }
    }
    p1 = v.probe;
    if (p1.hits - p0.hits != CLOCK_STEP_HITS_PER_TURN *
                             (unsigned)CLOCK_STEP_TURNS) {
        printf("  clockstep: %u probe hits over %d requested turns, expected "
               "%u\n", p1.hits - p0.hits, CLOCK_STEP_TURNS,
               CLOCK_STEP_HITS_PER_TURN * (unsigned)CLOCK_STEP_TURNS);
        failures++;
    }
    /* The real mutex's own answers. Nobody holding it means the sample was
     * unprotected; somebody else holding it means this is not the owner's
     * critical section; a marker that disagrees with the mutex means one of
     * them is not telling the truth. */
    if (p1.unlocked != 0u) {
        printf("  clockstep: the real clock was sampled %u time(s) while "
               "view_mu was held by nobody\n", p1.unlocked);
        failures++;
    }
    if (p1.foreign != 0u) {
        printf("  clockstep: the real clock was sampled %u time(s) while "
               "another thread held view_mu\n", p1.foreign);
        failures++;
    }
    if (p1.errors != 0u) {
        printf("  clockstep: the mutex answered %u probe(s) with neither "
               "held-by-me, held-by-another nor free\n", p1.errors);
        failures++;
    }
    if (p1.disagreements != 0u) {
        printf("  clockstep: the ownership marker and the real mutex "
               "disagreed %u time(s)\n", p1.disagreements);
        failures++;
    }
    /* And the reading the owner used is never ahead of the clock it is
     * handed. */
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "clockstep",
                                            &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &logical_now, &last_real);
    if (last_real == 0u) {
        printf("  clockstep: the owner never recorded a real reading\n");
        failures++;
    } else if (logical_now < last_real) {
        printf("  clockstep: the clock went backwards — logical %llu is "
               "before the real time %llu the owner had already used\n",
               (unsigned long long)logical_now,
               (unsigned long long)last_real);
        failures++;
    }

    /*
     * THE GLOBAL DEFAULT IS NOT THIS LISTENER'S MODE.
     *
     * Calling the legacy global setter restores nothing here: this listener
     * adopted its timeout through a request it acknowledged, and only another
     * such request can change it. Pinned rather than assumed, because an
     * earlier version of this case used that call as a restore claim.
     */
    moqr_admin_listen_test_set_poll_ms(MOQR_ADMIN_POLL_MS);
    {
        unsigned g = 0;
        int m = 0;
        int adopted = 0;

        moqr_admin_listen_test_poll_ack(l, &g, &m, &adopted);
        if (adopted != -1) {
            printf("  clockstep: the global default changed this listener's "
                   "adopted mode to %d\n", adopted);
            failures++;
        }
    }
    /* Restored the only way it can be, then stopped. */
    req_gen = moqr_admin_listen_test_request_poll_ms(l, MOQR_ADMIN_POLL_MS);
    if (!await_poll_ack(l, req_gen, MOQR_ADMIN_POLL_MS, "clockstep")) {
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

/*
 * A STALE RE-ARM CANNOT REWIND AN ADVANCED CLOCK.
 *
 * Arming a second time is still arming, so everything the clock has already
 * reached -- including every step advanced onto it -- is part of the floor.
 */
/*
 * ADVANCING A CLOCK NOBODY IS READING IS NOT A STEP.
 *
 * Until the logical clock is selected the owner is on real time, so an advance
 * would move a value nothing reads and wake the owner for a transition that
 * did not happen. It is refused, and it changes nothing.
 */
static int
test_advance_without_an_armed_clock_refuses(void)
{
    int failures = 0;
    int armed = 1;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    uint64_t before = 0;
    uint64_t after = 0;
    uint64_t last_real = 0;
    unsigned posts_before;
    unsigned posts_after;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  advunarmed: start refused\n");
        return 1;
    }
    /*
     * NOTHING HERE WAITS FOR QUIET.
     *
     * The verdict below is a synchronous count read either side of a
     * synchronous operation, so whatever else the owner is doing is irrelevant
     * -- and a wait that timed out would only have proved that nothing had
     * arrived yet.
     */
    moqr_admin_listen_test_clock_state(l, &armed, &before, &last_real);
    if (armed) {
        printf("  advunarmed: the clock was already selected\n");
        failures++;
    }

    /*
     * THE WAKE IS A COUNT, NOT AN ABSENCE.
     *
     * Read at the boundary that posts wakes, immediately either side of the
     * refused operation: whatever that call sent is already in the second
     * reading. Waiting to see whether a view turns up would only prove that
     * none turned up yet.
     */
    posts_before = moqr_admin_listen_test_wake_posts(l);
    moqr_admin_listen_test_clock_advance_expect(l, 1000u, false, "advunarmed",
                                                &failures);
    posts_after = moqr_admin_listen_test_wake_posts(l);
    if (posts_after != posts_before) {
        printf("  advunarmed: a refused advance posted %u wake(s)\n",
               posts_after - posts_before);
        failures++;
    }

    moqr_admin_listen_test_clock_state(l, &armed, &after, &last_real);
    if (armed) {
        printf("  advunarmed: a refused advance selected the clock\n");
        failures++;
    }
    if (after != before) {
        printf("  advunarmed: a refused advance still moved the value, %llu "
               "to %llu\n", (unsigned long long)before,
               (unsigned long long)after);
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

/*
 * A STATED EXPECTATION OWNS THE VERDICT.
 *
 * Returning a result the caller must act on is not enough: `if (op(...)) { }`
 * consumes it and handles nothing, and the case stays green. So the comparison
 * itself increments the ledger the case is judged by -- once, whether or not
 * the caller writes any control flow around the call. An expectation stated
 * with nowhere to record a mismatch is counted separately, so a misdirected
 * call cannot pass by having nobody to tell.
 */
/*
 * WHAT IS ACKNOWLEDGED IS WHAT WAS TAKEN.
 *
 * The owner captures a request's generation and timeout together, releases the
 * view lock to drain the wake pipe, then acknowledges. If it reread the
 * generation afterwards, a request that arrived in that window would be
 * acknowledged with the PREVIOUS request's timeout -- and since the
 * generations would then compare equal, its own timeout would never be
 * adopted at all.
 *
 * The seam is a barrier, not a race: the owner is held exactly there while the
 * second request is issued, so the interleaving is arranged rather than hoped
 * for.
 */
typedef struct poll_req_ctx {
    moqr_admin_listen_t *l;
    int                  ms;
    unsigned             gen;
    int                  busy;      /* what the REAL view lock reported */
    /*
     * TWO DIFFERENT FACTS.
     *
     * `done` says the work finished and every result the caller reads is
     * final. `released` says this thread will never touch the context, the
     * listener, or any other caller-owned storage again -- it is published as
     * B's last access, and observing it requires acquiring the mutex B
     * released, which is what makes it a lifetime proof rather than a
     * promise.
     */
    /* One condition per fact: sharing one would let the release signal wake a
     * caller still waiting for completion, and a missing completion signal
     * would then be invisible. */
    pthread_mutex_t      mu;
    pthread_cond_t       cv_done;
    pthread_cond_t       cv_released;
    int                  done;
    int                  released;
    /* A waiter-first seam: B waits here until the caller is provably inside
     * the completion wait, so a missing signal is a proof rather than a race. */
    int                  hold_done;
    int                  waiter_in_wait;
} poll_req_ctx_t;

/*
 * THE ONLY WAY A PUBLICATION FAILURE ENDS.
 *
 * A primitive that refuses inside a publication leaves the fact unpublished
 * and possibly the mutex held; there is no state to continue from. This is
 * the single terminal action every failure arm takes, and it does not return
 * -- so an arm that "handles" a refusal by returning or falling through is
 * not a spelling the authority has to recognise, it is one the compiler and
 * the structural check both reject.
 */
_Noreturn static void
poll_req_fail_stop(const char *what)
{
    stop_report_bounded("  request: %s; stopping before teardown\n", what);
    _exit(90);
}

/*
 * Publish completion: results are final, and the caller may read them.
 *
 * One checked interval -- acquire, hold for a waiter-first seam if armed,
 * set, checked broadcast, checked unlock -- and every refusal, including the
 * seam's own condition wait, is the fail-stop above. Nothing here may publish
 * after a refused wait or return while holding the mutex.
 */
static void
poll_req_publish_completion(poll_req_ctx_t *c)
{
    if (pthread_mutex_lock(&c->mu) != 0) {
        poll_req_fail_stop("could not take the mutex to publish completion");
    }
    while (c->hold_done && !c->waiter_in_wait) {
        if (pthread_cond_wait(&c->cv_done, &c->mu) != 0) {
            poll_req_fail_stop("the completion seam's wait refused");
        }
    }
    c->done = 1;
    if (pthread_cond_broadcast(&c->cv_done) != 0) {
        poll_req_fail_stop("could not signal completion");
    }
    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex completion was "
                           "published under");
    }
}

/*
 * Publish the release: this thread will never touch caller-owned storage
 * again. It is the LAST access to the context; the caller of this operation
 * may do nothing afterwards but return.
 */
static void
poll_req_publish_release(poll_req_ctx_t *c)
{
    if (pthread_mutex_lock(&c->mu) != 0) {
        poll_req_fail_stop("could not take the mutex to publish the release");
    }
    c->released = 1;
    if (pthread_cond_broadcast(&c->cv_released) != 0) {
        poll_req_fail_stop("could not signal the release");
    }
    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex the release was "
                           "published under");
    }
}

static void *
poll_req_main(void *arg)
{
    poll_req_ctx_t *c = (poll_req_ctx_t *)arg;

    /*
     * Touch the real lock first and say what it reported. Only a thread that
     * has actually found `view_mu` busy can publish this, so it is evidence of
     * contention rather than of having been created.
     */
    c->busy = moqr_admin_listen_test_view_mu_is_busy(c->l);
    moqr_admin_listen_test_poll_seam_contended(c->l);
    /* Now the ordinary blocking path. */
    c->gen = moqr_admin_listen_test_request_poll_ms(c->l, c->ms);
    poll_req_publish_completion(c);
    /* Penultimate: after this, only the return. */
    poll_req_publish_release(c);
    return NULL;
}

/*
 * ONE ABSOLUTE DEADLINE FOR THE WHOLE TRANSACTION.
 *
 * Computed once and passed to every wait, including cleanup: a per-wait budget
 * renews itself, so a thread that never returns could consume one budget in
 * the main path and another in the tail and then be joined unboundedly. This
 * waits on the request's own completion condition -- no polling of an
 * unrelated event -- and says plainly whether the completion arrived.
 */
static bool
await_request_done(poll_req_ctx_t *c, const txn_deadline_t *deadline,
                   const char *who)
{
    struct timespec at;
    bool done;

    txn_realtime_from(deadline, &at);
    if (pthread_mutex_lock(&c->mu) != 0) {
        printf("  %s: could not take the mutex to await completion\n", who);
        return false;
    }
    /* Announce that this caller is about to wait, so a seam can hold the
     * worker until the wait is genuinely entered. */
    c->waiter_in_wait = 1;
    (void)pthread_cond_broadcast(&c->cv_done);
    /* Once a wait is entered, a timeout is a non-event; rereading the flag
     * afterwards would let an expired deadline stand in for being told. */
    while (!c->done) {
        if (pthread_cond_timedwait(&c->cv_done, &c->mu, &at) != 0) {
            (void)pthread_mutex_unlock(&c->mu);
            printf("  %s: the second request never returned\n", who);
            return false;
        }
    }
    done = true;
    (void)pthread_mutex_unlock(&c->mu);
    return done;
}

/*
 * Wait for B to say it will never touch anything of ours again.
 *
 * On B's OWN condition: an unrelated publication cannot stand in for this,
 * because observing it requires taking the mutex B released as its final act.
 * Spends the one transaction deadline; expiry may only reject.
 */
static bool
await_request_released(poll_req_ctx_t *c, const txn_deadline_t *deadline)
{
    struct timespec at;
    bool released = false;

    txn_realtime_from(deadline, &at);
    if (pthread_mutex_lock(&c->mu) != 0) {
        return false;
    }
    /*
     * THE WAIT'S OUTCOME IS THE ANSWER, NOT A LATER FLAG READ.
     *
     * A flag already set when this arrives is an event that happened; that
     * needs no wait. But once a wait has been entered, a timeout -- or any
     * other refusal -- is a non-event, and rereading the flag afterwards would
     * let an expired deadline authorise teardown. That is exactly the timeout
     * standing in for a positive result this series forbids.
     */
    while (!c->released) {
        if (pthread_cond_timedwait(&c->cv_released, &c->mu, &at) != 0) {
            (void)pthread_mutex_unlock(&c->mu);
            return false;
        }
    }
    released = true;
    (void)pthread_mutex_unlock(&c->mu);
    return released;
}

/*
 * ONE OPERATION OWNS THE WHOLE TRANSITION.
 *
 * Awaiting the release and relinquishing the thread are a single decision: a
 * caller that could pass its own proof to the detach could ignore or forge the
 * event and tear down storage the thread still reaches. There is no argument
 * here to get wrong -- the event branch is the only path to the detach, and
 * every other outcome stops the process before anything is destroyed.
 */
static void
poll_req_await_release_and_detach_or_stop(poll_req_ctx_t *c,
                                          owned_thread_t *t,
                                          const txn_deadline_t *deadline,
                                          const char *who)
{
    if (!await_request_released(c, deadline)) {
        stop_report_bounded("  %s: completed but never released what it can "
                            "reach; stopping before teardown\n", who);
        _exit(90);
    }
    owned_thread_release_detach(t, who);
}

/*
 * BEING TOLD IS THE ONLY WAY TO LEARN.
 *
 * The caller is held inside the completion wait before the worker publishes,
 * so the signal is the only thing that can end that wait: a deadline expiring
 * is a non-event, not a slower success. The seam makes the waiter-first order
 * a fact rather than a race -- the worker does not publish until the caller
 * has announced, under the same mutex, that it is about to wait.
 *
 * The budget here is deliberately short: it bounds a mutant, and no ordinary
 * run spends it.
 */
static int
test_completion_signal_is_required(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    poll_req_ctx_t ctx;
    owned_thread_t th;
    txn_deadline_t txn;

    memset(&ctx, 0, sizeof(ctx));
    memset(&th, 0, sizeof(th));
    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  completion: start refused\n");
        return 1;
    }
    if (pthread_mutex_init(&ctx.mu, NULL) != 0 ||
        pthread_cond_init(&ctx.cv_done, NULL) != 0 ||
        pthread_cond_init(&ctx.cv_released, NULL) != 0) {
        printf("  completion: could not build the completion event\n");
        rig_stop(&rig, l);
        return 1;
    }
    ctx.l = l;
    ctx.ms = MOQR_ADMIN_POLL_MS;
    ctx.hold_done = 1;
    txn = txn_deadline_new(COMPLETION_SEAM_BUDGET_MS);
    if (!owned_thread_create(&th, poll_req_main, &ctx, 0, NULL)) {
        printf("  completion: the worker could not be started\n");
        (void)pthread_cond_destroy(&ctx.cv_released);
        (void)pthread_cond_destroy(&ctx.cv_done);
        (void)pthread_mutex_destroy(&ctx.mu);
        rig_stop(&rig, l);
        return 1;
    }
    if (!await_request_done(&ctx, &txn, "completion")) {
        printf("  completion: the worker never told the waiting caller it had "
               "finished\n");
        failures++;
    }
    poll_req_await_release_and_detach_or_stop(&ctx, &th, &txn, "completion");
    (void)pthread_cond_destroy(&ctx.cv_released);
    (void)pthread_cond_destroy(&ctx.cv_done);
    (void)pthread_mutex_destroy(&ctx.mu);
    rig_stop(&rig, l);
    return failures;
}

/*
 * A NEWER REQUEST IS NOT STRANDED BY THE OLDER ONE'S DRAIN.
 *
 * The owner captures the request, drains the wake pipe and acknowledges under
 * ONE hold of the view lock. If it released that lock across the drain, a
 * request arriving in the window would have its wake drained while the request
 * itself stayed pending -- and with A adopted as -1 there is no periodic tick
 * to rescue it, so nothing would ever wake the owner again.
 *
 * The interleaving is established, not hoped for. The owner is held at the
 * capture on a separate lock while still owning the view lock; B then asks the
 * REAL view lock whether it is busy and publishes what it found; only once
 * that contention is observed is the owner released. A thread-started flag
 * would prove nothing -- it can be published and the thread descheduled before
 * it touches anything.
 */
static int
test_poll_ack_names_what_it_took(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    poll_req_ctx_t ctx_b;
    owned_thread_t th_b;
    unsigned gen_a = 0;
    bool started = false;
    txn_deadline_t txn;
    unsigned issued0 = 0;

    memset(&th_b, 0, sizeof(th_b));
    memset(&ctx_b, 0, sizeof(ctx_b));
    txn = txn_deadline_new(POLL_ACK_BUDGET_MS);
    issued0 = g_txn_deadlines_issued;
    if (txn.id == 0u) {
        printf("  pollseam: no clock to bound this transaction\n");
        return 1;
    }
    if (pthread_mutex_init(&ctx_b.mu, NULL) != 0 ||
        pthread_cond_init(&ctx_b.cv_done, NULL) != 0 ||
        pthread_cond_init(&ctx_b.cv_released, NULL) != 0) {
        printf("  pollseam: could not build B's completion event\n");
        return 1;
    }
    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  pollseam: start refused\n");
        return 1;
    }

    moqr_admin_listen_test_poll_seam(l, 1);
    gen_a = moqr_admin_listen_test_request_poll_ms(l, -1);
    if (!moqr_admin_listen_test_poll_seam_await(l, txn_remaining_ms(&txn))) {
        printf("  pollseam: the owner never reached the capture\n");
        failures++;
        goto done;
    }

    ctx_b.l = l;
    ctx_b.ms = 250;
    ctx_b.busy = -2;
    if (!owned_thread_create(&th_b, poll_req_main, &ctx_b, 0, NULL)) {
        printf("  pollseam: B's requester could not be started\n");
        failures++;
        goto done;
    }
    started = true;
    if (!moqr_admin_listen_test_poll_seam_await_contended(
            l, txn_remaining_ms(&txn))) {
        printf("  pollseam: B never reported touching the view lock\n");
        failures++;
        goto done;
    }
    if (ctx_b.busy != 1) {
        printf("  pollseam: B found the view lock %s, expected it busy\n",
               ctx_b.busy == 0 ? "free" : "unreadable");
        failures++;
        goto done;
    }

    /* Only now. */
    moqr_admin_listen_test_poll_seam(l, 0);

    if (!await_poll_ack_within(l, gen_a, -1, &txn, "pollseam")) {
        failures++;
        goto done;
    }
    if (!await_request_done(&ctx_b, &txn, "pollseam")) {
        failures++;
        goto done;
    }
    if (ctx_b.gen == gen_a) {
        printf("  pollseam: the two requests share a generation\n");
        failures++;
        goto done;
    }
    if (!await_poll_ack_within(l, ctx_b.gen, 250, &txn, "pollseam")) {
        failures++;
    }

done:
    /* Every arm releases the seam and settles ownership before teardown. */
    moqr_admin_listen_test_poll_seam(l, 0);
    if (started) {
        /* The SAME deadline, never a fresh one. A thread that has not
         * completed still owns reachability, so there is nothing safe to
         * unwind to and nothing to be gained by blocking on a join. */
        if (!await_request_done(&ctx_b, &txn, "pollseam")) {
            stop_report_bounded("  pollseam: B never completed and can still "
                                "reach this frame; stopping before "
                                "teardown\n");
            _exit(90);
        }
        /*
         * WORK DONE IS NOT LIFETIME DONE.
         *
         * The results are readable once `done` is published, but this frame
         * and this context may only be torn down once B has published that it
         * will never touch them again -- observed through B's own condition,
         * on the same transaction deadline, and required before anything is
         * destroyed.
         */
        poll_req_await_release_and_detach_or_stop(&ctx_b, &th_b, &txn,
                                                  "pollseam B");
    }
    if (g_txn_deadlines_issued != issued0) {
        printf("  pollseam: the transaction spent %u deadline(s), expected "
               "the one it was issued\n",
               g_txn_deadlines_issued - issued0 + 1u);
        failures++;
    }
    (void)pthread_cond_destroy(&ctx_b.cv_released);
    (void)pthread_cond_destroy(&ctx_b.cv_done);
    (void)pthread_mutex_destroy(&ctx_b.mu);
    rig_stop(&rig, l);
    return failures;
}

typedef struct mu_observer {
    moqr_admin_listen_t *l;
    int                  busy;      /* the mutex's own answer, once */
    pthread_mutex_t      mu;
    pthread_cond_t       cv;
    int                  done;
} mu_observer_t;

static void *
mu_observer_main(void *arg)
{
    mu_observer_t *o = (mu_observer_t *)arg;

    /* Exactly one real attempt, from a thread that owns nothing. Whatever it
     * acquires it releases; whatever it finds it publishes. */
    o->busy = moqr_admin_listen_test_view_mu_is_busy(o->l);
    (void)pthread_mutex_lock(&o->mu);
    o->done = 1;
    (void)pthread_cond_broadcast(&o->cv);
    (void)pthread_mutex_unlock(&o->mu);
    return NULL;
}

/*
 * Is the view lock free, asked by somebody who does not hold it?
 *
 * The owning thread cannot answer this about itself: trylock reports EBUSY
 * whether the caller owns the lock or another party does. A separate thread
 * that owns nothing gets an unambiguous answer, and it is a mutex result
 * rather than a scheduling observation. Returns -1 if the observer could not
 * be run or did not report.
 */
static int
view_mu_free_per_observer(moqr_admin_listen_t *l, const char *who)
{
    mu_observer_t o;
    owned_thread_t th;
    struct timespec deadline;
    int busy = -1;

    memset(&o, 0, sizeof(o));
    memset(&th, 0, sizeof(th));
    o.l = l;
    o.busy = -2;
    if (pthread_mutex_init(&o.mu, NULL) != 0 ||
        pthread_cond_init(&o.cv, NULL) != 0) {
        printf("  %s: could not build the observer's event\n", who);
        return -1;
    }
    if (!owned_thread_create(&th, mu_observer_main, &o, 0, NULL)) {
        printf("  %s: the observer could not be started\n", who);
        (void)pthread_cond_destroy(&o.cv);
        (void)pthread_mutex_destroy(&o.mu);
        return -1;
    }
    if (clock_gettime(CLOCK_REALTIME, &deadline) == 0) {
        deadline.tv_sec += 4;
        (void)pthread_mutex_lock(&o.mu);
        while (!o.done) {
            if (pthread_cond_timedwait(&o.cv, &o.mu, &deadline) != 0) {
                break;
            }
        }
        if (o.done) {
            busy = o.busy;
        }
        (void)pthread_mutex_unlock(&o.mu);
    }
    if (busy < 0) {
        printf("  %s: the observer never reported\n", who);
        owned_thread_join_or_stop(&th, who);
        (void)pthread_cond_destroy(&o.cv);
        (void)pthread_mutex_destroy(&o.mu);
        return -1;
    }
    owned_thread_join_or_stop(&th, who);
    (void)pthread_cond_destroy(&o.cv);
    (void)pthread_mutex_destroy(&o.mu);
    return busy == 0 ? 1 : 0;
}

/*
 * A REFUSED RELEASE IS RECORDED, AND THE ARM REALLY RUNS.
 *
 * The probe's release-refusal arm cannot be reached by an owner that is doing
 * its job, and a source check can only say where an increment sits -- never
 * that control flow reaches it. So the arm is exercised for real: the probe is
 * run from a caller that does not hold the view lock, which is exactly the
 * unprotected sample it exists to detect, and a one-shot failpoint returns a
 * refusal AFTER the genuine release. The mutex is free afterwards, the real
 * counter moves, and the injection is proved consumed rather than merely
 * armed.
 */
static int
test_probe_records_a_refused_release(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_probe_t before;
    moqr_admin_listen_probe_t after;
    int consumed0;
    int free_after_probe;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  probefail: start refused\n");
        return 1;
    }
    /* The owner is joined first, so nothing else can be holding the view lock
     * while the probe runs: the "nobody held it" arm is reached because nobody
     * does, not because a scheduler happened to look away. */
    rig_stop_publisher(&rig);
    (void)moqr_admin_listen_stop(l);
    moqr_admin_listen_test_clock_probe(l, &before);
    consumed0 = moqr_admin_listen_test_probe_unlock_consumed();

    moqr_admin_listen_test_fail_probe_unlock(1);
    moqr_admin_listen_test_probe_once(l);

    /*
     * THE MUTEX IS ASKED FIRST, BY SOMEBODY WHO OWNS NOTHING.
     *
     * Before any accessor runs: a reader that takes and releases the view lock
     * would hand back a hold this probe may have retained, healing the very
     * state being measured. Nothing touches the lock between the probe and
     * this question.
     */
    free_after_probe = view_mu_free_per_observer(l, "probefail");

    moqr_admin_listen_test_clock_probe(l, &after);
    if (moqr_admin_listen_test_probe_unlock_consumed() - consumed0 != 1) {
        printf("  probefail: the injected refusal was consumed %d time(s), "
               "expected once\n",
               moqr_admin_listen_test_probe_unlock_consumed() - consumed0);
        failures++;
    }
    if (after.errors - before.errors != 1u) {
        printf("  probefail: the refused release moved the error count by %u, "
               "expected 1\n", after.errors - before.errors);
        failures++;
    }
    if (after.unlocked - before.unlocked != 1u) {
        printf("  probefail: the unprotected sample was not recorded\n");
        failures++;
    }
    if (free_after_probe != 1) {
        printf("  probefail: the view lock was left held by the refused "
               "release\n");
        failures++;
    }
    /* The injection is one-shot: a second probe records no further error. */
    moqr_admin_listen_test_probe_once(l);
    moqr_admin_listen_test_clock_probe(l, &after);
    if (after.errors - before.errors != 1u) {
        printf("  probefail: the injection was not one-shot (%u errors)\n",
               after.errors - before.errors);
        failures++;
    }

    g_expected_probe_errors += 1;
    g_expected_unprotected_samples += 2;
    moqr_admin_listen_destroy(l);
    rig.listen = NULL;
    rig_release_resources(&rig);
    return failures;
}

static int
test_clock_expectation_owns_the_verdict(void)
{
    int failures = 0;
    int ledger = 0;
    int misdirected0 = moqr_admin_listen_test_clock_expect_misdirected();
    rig_t rig;
    moqr_admin_listen_t *l = NULL;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  expectverdict: start refused\n");
        return 1;
    }
    g_expected_misdirected_expectations++;
    g_expected_expectation_mismatches += 3;
    printf("    (three deliberate expectation mismatches follow)\n");

    /* A mismatch, with no branching at the call site at all. */
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, false,
                                            "expectverdict", &ledger);
    if (ledger != 1) {
        printf("  expectverdict: an arm mismatch moved the ledger %d time(s), "
               "expected once\n", ledger);
        failures++;
    }
    /* The clock is armed now, so an advance succeeds; expect a refusal. */
    moqr_admin_listen_test_clock_advance_expect(l, 1000u, false,
                                                "expectverdict", &ledger);
    if (ledger != 2) {
        printf("  expectverdict: an advance mismatch moved the ledger to %d, "
               "expected 2\n", ledger);
        failures++;
    }
    /* And a match moves nothing. */
    moqr_admin_listen_test_clock_advance_expect(l, 1000u, true,
                                                "expectverdict", &ledger);
    if (ledger != 2) {
        printf("  expectverdict: a satisfied expectation moved the ledger to "
               "%d\n", ledger);
        failures++;
    }

    /* Stated with nowhere to record it: counted, never ignored. */
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, false,
                                            "expectverdict", NULL);
    if (moqr_admin_listen_test_clock_expect_misdirected() - misdirected0 != 1) {
        printf("  expectverdict: a mismatch with no ledger was not "
               "recorded\n");
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

static int
test_rearming_cannot_rewind_the_clock(void)
{
    int failures = 0;
    int armed = 1;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    uint64_t before = 0;
    uint64_t after = 0;
    uint64_t last_real = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  rearm: start refused\n");
        return 1;
    }
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "rearm",
                                            &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }
    moqr_admin_listen_test_clock_advance_expect(l, 60ull * 1000000ull, true,
                                                "rearm", &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &before, &last_real);

    /* The same origin as the first arming, long after the clock has moved. */
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "rearm",
                                            &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &after, &last_real);
    if (after < before) {
        printf("  rearm: a stale origin rewound the clock from %llu to %llu\n",
               (unsigned long long)before, (unsigned long long)after);
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

/*
 * AN ADVANCE THAT WOULD WRAP IS REFUSED.
 *
 * Wrapping is the same backwards jump arriving through the other door, and a
 * step that cannot be taken is refused rather than approximated.
 */
static int
test_overflowing_advance_is_refused(void)
{
    int failures = 0;
    int armed = 1;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    uint64_t before = 0;
    uint64_t after = 0;
    uint64_t last_real = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  advovf: start refused\n");
        return 1;
    }
    moqr_admin_listen_test_clock_arm_expect(l, UINT64_MAX - 100ull, true,
                                            "advovf", &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }
    moqr_admin_listen_test_clock_state(l, &armed, &before, &last_real);
    if (before != UINT64_MAX - 100ull) {
        printf("  advovf: the origin was not installed (%llu)\n",
               (unsigned long long)before);
        failures++;
    }
    moqr_admin_listen_test_clock_advance_expect(l, 1000ull, false, "advovf",
                                                &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &after, &last_real);
    if (after != before) {
        printf("  advovf: a refused advance still moved the clock, %llu to "
               "%llu\n", (unsigned long long)before,
               (unsigned long long)after);
        failures++;
    }
    /* And an advance that fits is still taken. */
    moqr_admin_listen_test_clock_advance_expect(l, 50ull, true, "advovf",
                                                &failures);
    moqr_admin_listen_test_clock_state(l, &armed, &after, &last_real);
    if (after != before + 50ull) {
        printf("  advovf: the fitting advance did not land (%llu)\n",
               (unsigned long long)after);
        failures++;
    }

    rig_stop(&rig, l);
    return failures;
}

static int
test_rig_partial_acquisition_unwinds(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    int mu0, mud0, cv0, cvd0, bk0, bkd0;

    /* Refused at the mutex: the broker alone was taken. */
    mu0 = g_rig_mu_init; mud0 = g_rig_mu_destroy;
    cv0 = g_rig_cv_init; cvd0 = g_rig_cv_destroy;
    bk0 = g_rig_broker_init; bkd0 = g_rig_broker_destroy;
    if (rig_start_ex(&rig, &l, 0, 1, 0) == MOQR_OK) {
        printf("  rigpartial: the refused mutex still produced a rig\n");
        rig_stop(&rig, l);
        return 1;
    }
    if (g_rig_broker_init - bk0 != 1 || g_rig_broker_destroy - bkd0 != 1) {
        printf("  rigpartial: the broker was not taken and released exactly "
               "once (%d/%d)\n", g_rig_broker_destroy - bkd0,
               g_rig_broker_init - bk0);
        failures++;
    }
    if (g_rig_mu_init - mu0 != 0 || g_rig_mu_destroy - mud0 != 0 ||
        g_rig_cv_init - cv0 != 0 || g_rig_cv_destroy - cvd0 != 0) {
        printf("  rigpartial: a refused acquisition was still released — "
               "mutex %d/%d, condition %d/%d\n",
               g_rig_mu_destroy - mud0, g_rig_mu_init - mu0,
               g_rig_cv_destroy - cvd0, g_rig_cv_init - cv0);
        failures++;
    }

    /* Refused at the condition: the broker and the mutex were taken. */
    mu0 = g_rig_mu_init; mud0 = g_rig_mu_destroy;
    cv0 = g_rig_cv_init; cvd0 = g_rig_cv_destroy;
    bk0 = g_rig_broker_init; bkd0 = g_rig_broker_destroy;
    if (rig_start_ex(&rig, &l, 0, 0, 1) == MOQR_OK) {
        printf("  rigpartial: the refused condition still produced a rig\n");
        rig_stop(&rig, l);
        return failures + 1;
    }
    if (g_rig_broker_init - bk0 != 1 || g_rig_broker_destroy - bkd0 != 1 ||
        g_rig_mu_init - mu0 != 1 || g_rig_mu_destroy - mud0 != 1) {
        printf("  rigpartial: the broker and mutex were not released exactly "
               "once — broker %d/%d, mutex %d/%d\n",
               g_rig_broker_destroy - bkd0, g_rig_broker_init - bk0,
               g_rig_mu_destroy - mud0, g_rig_mu_init - mu0);
        failures++;
    }
    if (g_rig_cv_init - cv0 != 0 || g_rig_cv_destroy - cvd0 != 0) {
        printf("  rigpartial: the condition was never acquired but was "
               "released %d time(s)\n", g_rig_cv_destroy - cvd0);
        failures++;
    }
    return failures;
}

static int
test_rig_lifetime_is_balanced(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    const int mu0 = g_rig_mu_init, mud0 = g_rig_mu_destroy;
    const int cv0 = g_rig_cv_init, cvd0 = g_rig_cv_destroy;
    const int bk0 = g_rig_broker_init, bkd0 = g_rig_broker_destroy;
    moqr_result_t rc;

    /* The ordinary path. */
    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  riglife: start refused\n");
        return 1;
    }
    if (g_rig_mu_init - mu0 != 1 || g_rig_cv_init - cv0 != 1 ||
        g_rig_broker_init - bk0 != 1) {
        printf("  riglife: a running rig did not acquire all three "
               "resources\n");
        failures++;
    }
    rig_stop(&rig, l);
    l = NULL;
    if (g_rig_mu_destroy - mud0 != 1) {
        printf("  riglife: the mutex was released %d times, expected once\n",
               g_rig_mu_destroy - mud0);
        failures++;
    }
    if (g_rig_cv_destroy - cvd0 != 1) {
        printf("  riglife: the condition was released %d times, expected "
               "once\n", g_rig_cv_destroy - cvd0);
        failures++;
    }
    if (g_rig_broker_destroy - bkd0 != 1) {
        printf("  riglife: the broker was released %d times, expected once\n",
               g_rig_broker_destroy - bkd0);
        failures++;
    }
    /*
     * Ownership was given up, so nothing can reach these objects again. This
     * is checked here and returns immediately if it does not hold: proving
     * idempotence by actually destroying a destroyed object would be outside
     * the POSIX contract, so the guard itself is the thing observed.
     */
    if (rig.pub_mu_live || rig.pub_cv_live || rig.broker_live) {
        printf("  riglife: a released resource is still owned "
               "(mutex %d, condition %d, broker %d)\n",
               rig.pub_mu_live, rig.pub_cv_live, rig.broker_live);
        return failures + 1;
    }

    /* A refusal AFTER every resource was acquired: the ordinary production
     * refusal for a port that was never allowed to be chosen. */
    rc = rig_start_ex(&rig, &l, 1, 0, 0);
    if (rc == MOQR_OK) {
        printf("  riglife: the refusal path was not taken\n");
        rig_stop(&rig, l);
        return failures + 1;
    }
    if (l != NULL) {
        printf("  riglife: a refused start still handed back an endpoint\n");
        failures++;
    }
    if (g_rig_mu_init - mu0 != 2 || g_rig_cv_init - cv0 != 2 ||
        g_rig_broker_init - bk0 != 2) {
        printf("  riglife: the refusal did not reach the acquisitions it is "
               "supposed to unwind\n");
        failures++;
    }
    /* The refusal itself must have released everything it took, before any
     * later teardown has a chance to cover for it. */
    if (g_rig_mu_destroy - mud0 != 2 || g_rig_cv_destroy - cvd0 != 2 ||
        g_rig_broker_destroy - bkd0 != 2) {
        printf("  riglife: the refusal left resources behind — mu %d/%d, "
               "cv %d/%d, broker %d/%d\n",
               g_rig_mu_destroy - mud0, g_rig_mu_init - mu0,
               g_rig_cv_destroy - cvd0, g_rig_cv_init - cv0,
               g_rig_broker_destroy - bkd0, g_rig_broker_init - bk0);
        failures++;
    }
    /* A caller that tears down after a refusal must not release anything a
     * second time: the refusal already did it. */
    rig_stop(&rig, NULL);
    if (g_rig_mu_destroy - mud0 != 2 || g_rig_cv_destroy - cvd0 != 2 ||
        g_rig_broker_destroy - bkd0 != 2) {
        printf("  riglife: teardown after a refusal released something "
               "twice — mu %d, cv %d, broker %d\n",
               g_rig_mu_destroy - mud0, g_rig_cv_destroy - cvd0,
               g_rig_broker_destroy - bkd0);
        failures++;
    }

    /* Nothing this case acquired is outstanding, on either path. The totals
     * are deliberately not asserted: other cases own rigs of their own, and an
     * oracle that depends on all of them is an oracle about none of them. */
    if (g_rig_mu_init - mu0 != g_rig_mu_destroy - mud0 ||
        g_rig_cv_init - cv0 != g_rig_cv_destroy - cvd0 ||
        g_rig_broker_init - bk0 != g_rig_broker_destroy - bkd0) {
        printf("  riglife: this case left resources outstanding\n");
        failures++;
    }
    return failures;
}

static int
test_term_wait_budget_is_real_time(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t v;
    burst_ctx_t b;
    term_arm_t arm;
    pthread_t th;
    struct timespec t0, t1;
    long elapsed_ms;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  waitbudget: start refused\n");
        return 1;
    }
    memset(&b, 0, sizeof(b));
    b.l = l;
    b.port = moqr_admin_listen_port(l);
    b.fd = -1;
    if (!burst_launch(&b, &arm, &th, 0, 0, 0)) {
        printf("  waitbudget: could not build the arm barrier\n");
        rig_stop(&rig, l);
        return 1;
    }
    /*
     * No safety poll for the length of this case. Every owner turn during the
     * burst must be one this producer asked for, or the generation delta below
     * would include turns nobody requested and the count would be audited
     * against the wrong number.
     */
    moqr_admin_listen_test_set_poll_ms(-1);
    (void)clock_gettime(CLOCK_MONOTONIC, &t0);
    if (!term_wait_armed(l, &v, view_any_client, 8000, &arm)) {
        /* Safe to name mid-flight: the counter is atomic and counts only
         * publications the owner acknowledged. */
        printf("  waitbudget: the wait gave up before its budget — %d "
               "acknowledged non-matching publications consumed it\n",
               atomic_load(&b.bursts));
        failures++;
    }
    (void)clock_gettime(CLOCK_MONOTONIC, &t1);
    elapsed_ms = (long)(t1.tv_sec - t0.tv_sec) * 1000L +
                 (t1.tv_nsec - t0.tv_nsec) / 1000000L;
    (void)pthread_join(th, NULL);
    moqr_admin_listen_test_set_poll_ms(MOQR_ADMIN_POLL_MS);
    if (b.fd >= 0) {
        (void)close(b.fd);
    }
    if (!atomic_load(&b.armed)) {
        printf("  waitbudget: the producer ran before the wait armed\n");
        failures++;
    }
    if (atomic_load(&b.bursts) != TERM_WAIT_BURST) {
        printf("  waitbudget: %d of %d publications were acknowledged\n",
               atomic_load(&b.bursts), TERM_WAIT_BURST);
        failures++;
    }
    /*
     * EXACTLY, not at least.
     *
     * The contract the producer runs on is one wake request, one observed
     * publication, so the owner's generation must have advanced by exactly the
     * number acknowledged -- measured from the generation the wait armed on to
     * the last acknowledged publication, with the satisfying client's own
     * turns deliberately outside the window. A lower bound would let unrelated
     * turns stand in for publications a request-counting producer never
     * received.
     */
    {
        uint64_t last = atomic_load(&b.last_gen);
        uint64_t published = last >= arm.gen ? last - arm.gen : 0u;
        if (published != (uint64_t)atomic_load(&b.bursts)) {
            printf("  waitbudget: %d publications were acknowledged but the "
                   "owner advanced by %llu\n", atomic_load(&b.bursts),
                   (unsigned long long)published);
            failures++;
        }
    }
    term_arm_release(&arm);
    /* The burst is deliberately well inside the budget, so a wait that ends
     * before the predicate holds cannot be blamed on real time. */
    if (elapsed_ms >= 8000) {
        printf("  waitbudget: the wait actually consumed its whole budget "
               "(%ld ms) — this case would prove nothing\n", elapsed_ms);
        failures++;
    }
    rig_stop(&rig, l);
    return failures;
}

#define LOGICAL_STEP_BOUND_MS 4000

/*
 * THE LOGICAL ADVANCE REACHES THE OWNER.
 *
 * The owner reads its clock at the top of every turn, so moving the logical
 * clock past an interior deadline is what makes that deadline expire. This
 * case pins the advance and nothing else: the owner's turns arrive by the
 * ordinary production means, and the oracle is the expiry itself, observed as
 * the retirement carrier held at the boundary where the retained token exists.
 */
/*
 * THE ADVANCE SEAM DELIVERS ITS OWN WAKE.
 *
 * This is a promise the deterministic seam makes to the tests that use it: the
 * logical step applies NOW, not on the owner's next turn from some other
 * cause. It is not a claim about production correctness -- the owner re-reads
 * its clock every turn, and the 50 ms safety poll makes a missed wake a
 * bounded-latency matter, which is why the epoch case above does not depend on
 * it.
 *
 * To pin the seam's promise the safety poll is removed for the length of this
 * case, so the owner is parked in an indefinite poll with nothing else that
 * can move it. A setup wake, acknowledged by a view generation, establishes
 * that it is parked; then the advance must produce the next generation on its
 * own.
 */
static int
test_logical_advance_delivers_its_wake(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t view;
    uint64_t parked_gen = 0;
    uint64_t woke_gen = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  advancewake: start refused\n");
        return 1;
    }
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "clock",
                                            &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }
    /* Nothing but a wake can move the owner from here. */
    moqr_admin_listen_test_set_poll_ms(-1);

    /* Park it: one wake, one acknowledged turn, and it is back in poll. */
    parked_gen = moqr_admin_listen_test_view(l, &view);
    moqr_admin_listen_notify(l);
    for (int guard = 0; guard < 40 && woke_gen == 0u; guard++) {
        woke_gen = moqr_admin_listen_test_wait_view(l, parked_gen, 50, &view);
    }
    if (woke_gen == 0u) {
        printf("  advancewake: the owner never acknowledged the setup wake\n");
        failures++;
        goto done;
    }
    parked_gen = woke_gen;

    /* THE SEAM. With no safety tick behind it, only the advance's own wake can
     * produce another owner turn. */
    woke_gen = 0;
    {
        int before = failures;
        moqr_admin_listen_test_clock_advance_expect(l, 1000u, true,
                                                    "advancewake", &failures);
        if (failures != before) {
            goto done;
        }
    }
    for (int guard = 0; guard < 40 && woke_gen == 0u; guard++) {
        woke_gen = moqr_admin_listen_test_wait_view(l, parked_gen, 50, &view);
    }
    if (woke_gen == 0u) {
        printf("  advancewake: the logical advance did not wake the owner — "
               "no turn followed it\n");
        failures++;
    } else if (woke_gen <= parked_gen) {
        printf("  advancewake: the owner did not take a new turn "
               "(%llu after %llu)\n",
               (unsigned long long)woke_gen, (unsigned long long)parked_gen);
        failures++;
    }

done:
    moqr_admin_listen_test_set_poll_ms(MOQR_ADMIN_POLL_MS);
    rig_stop(&rig, l);
    return failures;
}

static int
test_logical_advance_expires_the_epoch(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    moqr_admin_listen_view_t view;
    int fd = -1;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  logicaladvance: start refused\n");
        return 1;
    }
    moqr_admin_listen_test_clock_arm_expect(l, 1000000u, true, "clock",
                                            &failures);
    if (failures != 0) {
        rig_stop(&rig, l);
        return failures;
    }

    /* One scrape, sole waiter of a generation publication will never
     * complete. */
    rig_hold_publication(&rig);
    if (!rig_wait_publisher_blocked(&rig, 4000)) {
        printf("  logicaladvance: the publisher never reached its barrier\n");
        failures++;
        goto done;
    }
    fd = dial(moqr_admin_listen_port(l));
    if (fd < 0) {
        printf("  logicaladvance: could not connect\n");
        failures++;
        goto done;
    }
    (void)write_all(fd, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    if (!term_wait(l, &view, term_one_more_waiting, 8000)) {
        printf("  logicaladvance: the scrape never bound a generation\n");
        failures++;
        goto done;
    }
    /* Held where the retained token is observable. Arming this before the
     * waiter binds would park the owner on an unrelated retirement. */
    moqr_admin_listen_test_pause_at_retire(l, 1);
    (void)close(fd);
    fd = -1;

    /* Nothing has expired yet: the clock has not moved. */
    (void)moqr_admin_listen_test_view(l, &view);
    if (view.retiring != 0) {
        printf("  logicaladvance: a generation retired before the clock "
               "moved\n");
        failures++;
    }

    /* THE ADVANCE. Moving the clock past the epoch budget is what makes the
     * deadline expire. */
    {
        int before = failures;
        moqr_admin_listen_test_clock_advance_expect(
            l, MOQR_ADMIN_EPOCH_DEADLINE_US + 100000u, true, "logicaladvance",
            &failures);
        if (failures != before) {
            goto done;
        }
    }
    if (!term_wait(l, &view, term_carrier_live, LOGICAL_STEP_BOUND_MS)) {
        printf("  logicaladvance: the epoch never expired — the logical "
               "advance did not reach the owner's clock\n");
        failures++;
    }

done:
    moqr_admin_listen_test_pause_at_retire(l, 0);
    rig_release_publication(&rig);
    if (fd >= 0) {
        (void)close(fd);
    }
    rig_stop(&rig, l);
    return failures;
}

static int
test_concurrent_scrapes(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;
    scraper_t s[SCRAPERS];
    int spawned = 0;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  concurrent: start refused\n");
        return 1;
    }
    for (int i = 0; i < SCRAPERS; i++) {
        s[i].port = moqr_admin_listen_port(l);
        s[i].ok = 0;
        if (pthread_create(&s[i].t, NULL, scraper_main, &s[i]) != 0) {
            break;
        }
        spawned++;
    }
    if (spawned != SCRAPERS) {
        printf("  concurrent: only %d of %d scrapers started\n", spawned,
               SCRAPERS);
        failures++;
    }
    for (int i = 0; i < spawned; i++) {
        if (pthread_join(s[i].t, NULL) != 0) {
            printf("  concurrent: a scraper could not be joined\n");
            failures++;
        }
    }
    for (int i = 0; i < spawned; i++) {
        if (s[i].ok != SCRAPES) {
            printf("  concurrent: scraper %d completed %d of %d exchanges\n", i,
                   s[i].ok, SCRAPES);
            failures++;
        }
    }
    /* Stopping under load must still join cleanly and settle every bank. */
    rig_stop(&rig, l);
    return failures;
}

static int
test_stop_is_idempotent_from_the_owner(void)
{
    int failures = 0;
    rig_t rig;
    moqr_admin_listen_t *l = NULL;

    if (rig_start(&rig, &l) != MOQR_OK) {
        printf("  stop: start refused\n");
        return 1;
    }
    /* An in-flight generation at stop time must be cancelled by the owner and
     * settled before it exits -- not left owing a release. */
    {
        uint64_t serial = 0;
        bool wake = false;
        MOQ_TEST_CHECK(moqr_broker_request(&rig.broker,
                                           MOQR_BROKER_DEMAND_HTTP, &serial,
                                           &wake) == MOQR_OK);
    }
    rig_stop(&rig, l);
    /* A NULL stop is a refusal, not a crash. */
    MOQ_TEST_CHECK(moqr_admin_listen_stop(NULL) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_admin_listen_port(NULL) == 0);
    MOQ_TEST_CHECK(moqr_admin_listen_accepts(NULL) == 0u);
    moqr_admin_listen_note_terminal(NULL);
    moqr_admin_listen_notify(NULL);
    return failures;
}

int
main(int argc, char **argv)
{
    /*
     * A CASE SELECTOR.
     *
     * The whole target covers a physical socket boundary and costs about
     * twenty-five seconds. Multiplying that by every mutant is how a slow
     * gate turns into a widened timeout, so a named subset can be run
     * directly instead. With no argument every case runs, as before.
     */
    static const struct { const char *name; int (*fn)(void); } cases[] = {
        { "transition_sites_are_unique", test_transition_sites_are_unique },
        { "start_refusals", test_start_refusals },
        { "serves_a_scrape", test_serves_a_scrape },
        { "serves_info_statically", test_serves_info_statically },
        { "info_document_is_required", test_info_document_is_required },
        { "serves_shards_over_a_generation", test_serves_shards_over_a_generation },
        { "signal_demand_emits_once", test_signal_demand_emits_once },
        { "latched_signal_opens_a_generation", test_latched_signal_opens_a_generation },
        { "terminality_fails_closed", test_terminality_fails_closed },
        { "terminality_settles_every_state", test_terminality_settles_every_state },
        { "view_is_listener_bound", test_view_is_listener_bound },
        { "view_wait_is_fail_closed", test_view_wait_is_fail_closed },
        { "rig_lifetime_is_balanced", test_rig_lifetime_is_balanced },
        { "clock_choice_and_sample_are_one_step", test_clock_choice_and_sample_are_one_step },
        { "advance_without_an_armed_clock_refuses", test_advance_without_an_armed_clock_refuses },
        { "poll_ack_names_what_it_took", test_poll_ack_names_what_it_took },
        { "completion_signal_is_required", test_completion_signal_is_required },
        { "probe_records_a_refused_release", test_probe_records_a_refused_release },
        { "clock_expectation_owns_the_verdict", test_clock_expectation_owns_the_verdict },
        { "rearming_cannot_rewind_the_clock", test_rearming_cannot_rewind_the_clock },
        { "overflowing_advance_is_refused", test_overflowing_advance_is_refused },
        { "arming_without_a_clock_refuses", test_arming_without_a_clock_refuses },
        { "arm_lifetime_unwinds", test_arm_lifetime_unwinds },
        { "arm_belongs_to_its_own_wait", test_arm_belongs_to_its_own_wait },
        { "thread_owner_refuses_an_unowned_join", test_thread_owner_refuses_an_unowned_join },
        { "join_refusal_holds_the_barrier", test_join_refusal_holds_the_barrier },
        { "persistent_join_refusal_stops", test_persistent_join_refusal_stops },
        { "rig_partial_acquisition_unwinds", test_rig_partial_acquisition_unwinds },
        { "rig_refused_destroy_is_not_a_release", test_rig_refused_destroy_is_not_a_release },
        { "two_peers_share_one_generation", test_two_peers_share_one_generation },
        { "term_wait_budget_is_real_time", test_term_wait_budget_is_real_time },
        { "logical_advance_expires_the_epoch", test_logical_advance_expires_the_epoch },
        { "logical_advance_delivers_its_wake", test_logical_advance_delivers_its_wake },
        { "failed_wait_returns_no_payload", test_failed_wait_returns_no_payload },
        { "partial_view_init_unwinds", test_partial_view_init_unwinds },
        { "overflow_uses_one_refusal_slot", test_overflow_uses_one_refusal_slot },
        { "poisoned_generations", test_poisoned_generations },
        { "render_failures_do_not_exhaust_the_ledger", test_render_failures_do_not_exhaust_the_ledger },
        { "render_failure_signal_demand", test_render_failure_signal_demand },
        { "refusal_deadline_is_a_deadline", test_refusal_deadline_is_a_deadline },
        { "refusal_io_results", test_refusal_io_results },
        { "address_and_port_boundary", test_address_and_port_boundary },
        { "publication_notification", test_publication_notification },
        { "owner_terminals", test_owner_terminals },
        { "startup_refuses_a_failed_owner", test_startup_refuses_a_failed_owner },
        { "failed_wake_is_terminal", test_failed_wake_is_terminal },
        { "http_serial_exhaustion_is_terminal", test_http_serial_exhaustion_is_terminal },
        { "refused_transitions_are_invariant_failures", test_refused_transitions_are_invariant_failures },
        { "nocarrier_release_is_its_own_call", test_nocarrier_release_is_its_own_call },
        { "nocarrier_release_succeeds_once", test_nocarrier_release_succeeds_once },
        { "ordinary_release_happens_once", test_ordinary_release_happens_once },
        { "fail_serial_refusal_with_a_waiter_is_invariant", test_fail_serial_refusal_with_a_waiter_is_invariant },
        { "unprovable_join_refuses_to_release_anything", test_unprovable_join_refuses_to_release_anything },
        { "activation_failure_reports_an_unprovable_owner", test_activation_failure_reports_an_unprovable_owner },
        { "half_close_still_answered", test_half_close_still_answered },
        { "sigpipe_cannot_kill_the_relay", test_sigpipe_cannot_kill_the_relay },
        { "concurrent_scrapes", test_concurrent_scrapes },
        { "stop_is_idempotent_from_the_owner", test_stop_is_idempotent_from_the_owner },
    };
    const size_t n_cases = sizeof(cases) / sizeof(cases[0]);
    int failures = 0;
    int ran = 0;
    int requested = argc - 1;

    /*
     * FAIL CLOSED ON THE SELECTION ITSELF, before anything runs.
     *
     * A selector that quietly ignores a name it does not know turns a
     * misspelled mutant target into a green run of some other case. Every
     * supplied name must resolve to exactly one case, no name may be given
     * twice, and at the end the number of cases run must equal the number
     * requested -- otherwise this is not authority, it is a coincidence.
     */
    for (int a = 1; a < argc; a++) {
        int matches = 0;
        for (size_t i = 0; i < n_cases; i++) {
            if (strcmp(argv[a], cases[i].name) == 0) {
                matches++;
            }
        }
        if (matches == 0) {
            printf("FAIL: relay_admin_listen: unknown case '%s'\n", argv[a]);
            return 2;
        }
        if (matches > 1) {
            printf("FAIL: relay_admin_listen: case '%s' is defined %d times\n",
                   argv[a], matches);
            return 2;
        }
        for (int b = 1; b < a; b++) {
            if (strcmp(argv[a], argv[b]) == 0) {
                printf("FAIL: relay_admin_listen: case '%s' requested "
                       "twice\n", argv[a]);
                return 2;
            }
        }
    }

    for (size_t i = 0; i < n_cases; i++) {
        bool selected = argc < 2;
        for (int a = 1; !selected && a < argc; a++) {
            if (strcmp(argv[a], cases[i].name) == 0) {
                selected = true;
            }
        }
        if (!selected) {
            continue;
        }
        failures += cases[i].fn();
        ran++;
    }
    /*
     * THE RUN'S OWN BALANCE.
     *
     * A case-local check can only speak for its own rigs; a resource another
     * selected case abandoned is invisible to it. This gate is the whole
     * selection's statement: everything the fixture acquired was successfully
     * released by the time the run ends.
     */
    if (g_rig_mu_init != g_rig_mu_destroy ||
        g_rig_cv_init != g_rig_cv_destroy ||
        g_rig_broker_init != g_rig_broker_destroy) {
        printf("  runbalance: the selection ended holding fixture resources — "
               "mutex %d/%d, condition %d/%d, broker %d/%d\n",
               g_rig_mu_destroy, g_rig_mu_init,
               g_rig_cv_destroy, g_rig_cv_init,
               g_rig_broker_destroy, g_rig_broker_init);
        failures++;
    }
    /* The arm barrier is a distinct owner with a distinct lifetime, so it is
     * counted distinctly. */
    if (g_arm_mu_init != g_arm_mu_destroy ||
        g_arm_cv_init != g_arm_cv_destroy) {
        printf("  runbalance: the selection ended holding arm resources — "
               "mutex %d/%d, condition %d/%d\n",
               g_arm_mu_destroy, g_arm_mu_init,
               g_arm_cv_destroy, g_arm_cv_init);
        failures++;
    }
    if (g_arm_release_failures != g_arm_expected_release_failures) {
        printf("  runbalance: %d arm destroy call(s) were refused, %d were "
               "arranged\n", g_arm_release_failures,
               g_arm_expected_release_failures);
        failures++;
    }
    /* Threads the fixture created are threads the fixture joined. */
    /* A create is settled by exactly one successful join or one
     * release-qualified detach. */
    if (g_watch_create_ok != g_watch_join_ok + g_watch_detach_ok) {
        printf("  runbalance: the selection created %d fixture thread(s), "
               "joined %d and detached %d\n", g_watch_create_ok,
               g_watch_join_ok, g_watch_detach_ok);
        failures++;
    }
    if (g_watch_detach_refused != 0) {
        printf("  runbalance: %d detach(es) were refused\n",
               g_watch_detach_refused);
        failures++;
    }
    if (g_watch_unowned_joins != g_watch_expected_unowned ||
        g_watch_join_failed != g_watch_expected_join_failures) {
        printf("  runbalance: %d unowned join(s) and %d refused join(s), %d "
               "and %d were arranged\n", g_watch_unowned_joins,
               g_watch_join_failed, g_watch_expected_unowned,
               g_watch_expected_join_failures);
        failures++;
    }
    /* A create refusal that nobody arranged is a real refusal, and a case that
     * arranged one and did not get it proved nothing. */
    if (g_watch_create_refused != g_watch_expected_create_refused) {
        printf("  runbalance: %d create refusal(s), %d were arranged\n",
               g_watch_create_refused, g_watch_expected_create_refused);
        failures++;
    }
    if (moqr_admin_listen_test_clock_expect_misdirected() !=
        g_expected_misdirected_expectations) {
        printf("  runbalance: %d clock expectation(s) had no failure ledger, "
               "%d were arranged\n",
               moqr_admin_listen_test_clock_expect_misdirected(),
               g_expected_misdirected_expectations);
        failures++;
    }
    /* The comparison's own count, not any caller's. A mismatch routed to a
     * ledger nobody reads still lands here. */
    if (moqr_admin_listen_test_clock_expect_mismatches() !=
        g_expected_expectation_mismatches) {
        printf("  runbalance: the clock boundary saw %d expectation "
               "mismatch(es), %d were arranged\n",
               moqr_admin_listen_test_clock_expect_mismatches(),
               g_expected_expectation_mismatches);
        failures++;
    }
    if (moqr_admin_listen_test_accessor_lock_failures() !=
        g_expected_accessor_lock_failures) {
        printf("  runbalance: %d accessor call(s) could not take the view "
               "lock, %d were arranged\n",
               moqr_admin_listen_test_accessor_lock_failures(),
               g_expected_accessor_lock_failures);
        failures++;
    }
    if (g_arm_unsafe_release_attempts != 0) {
        printf("  runbalance: %d barrier release(s) were attempted while a "
               "thread could still reach them\n",
               g_arm_unsafe_release_attempts);
        failures++;
    }
    if (g_rig_release_failures != g_rig_expected_release_failures) {
        printf("  runbalance: %d fixture destroy call(s) were refused, %d "
               "were arranged\n", g_rig_release_failures,
               g_rig_expected_release_failures);
        failures++;
    }
    if (ran == 0) {
        printf("FAIL: relay_admin_listen selected no case\n");
        return 1;
    }
    if (requested > 0 && ran != requested) {
        printf("FAIL: relay_admin_listen: %d case(s) requested but %d ran\n",
               requested, ran);
        return 2;
    }
    printf("relay_admin_listen: %d case(s) run\n", ran);
    if (failures != 0) {
        printf("FAIL: relay_admin_listen (%d)\n", failures);
        return 1;
    }
    printf("PASS: relay_admin_listen\n");
    return 0;
}
