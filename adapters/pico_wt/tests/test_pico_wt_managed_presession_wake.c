/*
 * Managed pico WT: an explicit wake handled BEFORE the WT session exists
 * must be acknowledged to a waiter.
 *
 * The facade's wait()/wake() pair is the cross-thread signal an owner uses
 * to hand work to the network thread and learn that the thread has taken a
 * cycle. While the client is still connecting there is no session and no
 * pump, but the owner may already have queued work (a snapshot, a close)
 * and woken the loop for it. The contract this test pins:
 *
 *   - an accepted explicit wake() is acknowledged: a bounded wait() returns
 *     MOQ_OK even though no pump ran;
 *   - the acknowledgement is level-retained and consumed exactly once: an
 *     immediate nonblocking second wait() returns MOQ_DONE;
 *   - a second wake() produces exactly one second acknowledgement;
 *   - ordinary pre-session loop traffic (the client's own Initial sends and
 *     time checks) does NOT fabricate an acknowledgement: a bounded wait()
 *     BEFORE any wake returns MOQ_DONE;
 *   - on_pump and on_activity are never called before the session exists.
 *
 * The peer is a locally bound UDP socket that never answers. Binding it is
 * what keeps the client pre-session for the test's lifetime: an unbound port
 * would answer with ICMP unreachable and the client would go terminal. An
 * unavailable sink is a NAMED environment failure (exit 3), never a skip.
 *
 * Allocations routed through the facade's CONFIGURED allocator go through a
 * counting allocator, so stop/destroy must leave that count balanced. That
 * is what the counter proves and no more: picoquic and h3zero allocate on
 * their own paths, which the counter does not see. The h3 teardown is
 * judged elsewhere -- by the sanitizer lane and the deletion source path.
 *
 * Provenance of the wake: this facade's packet loop receives its wake-up
 * callback for the explicit picoquic_wake_up_network_thread() call that
 * moq_pico_wt_managed_wake() makes on the facade's private thread context,
 * the one such call site in the adapter. Nothing else in this process wakes
 * that loop, which is what lets the pre-wake control attribute a later
 * MOQ_OK to the wake and to nothing else.
 */

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)
#define CHECK_RESULT(label, got, want) do { \
    moq_result_t g_ = (got); \
    if (g_ != (want)) { \
        fprintf(stderr, "FAIL: %s:%d: %s: expected %d, got %d\n", \
                __FILE__, __LINE__, (label), (int)(want), (int)g_); \
        failures++; \
    } else { \
        printf("ok: %s: %d\n", (label), (int)g_); \
    } \
} while (0)

/* -- counting allocator ---------------------------------------------- */

typedef struct { atomic_long live; atomic_long allocs; } counter_t;

static void *c_alloc(size_t size, void *ctx)
{
    counter_t *c = (counter_t *)ctx;
    void *p = malloc(size);
    if (p) { atomic_fetch_add(&c->live, 1); atomic_fetch_add(&c->allocs, 1); }
    return p;
}
static void *c_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    counter_t *c = (counter_t *)ctx;
    (void)old_size;
    void *p = realloc(ptr, new_size);
    if (p && !ptr) { atomic_fetch_add(&c->live, 1); atomic_fetch_add(&c->allocs, 1); }
    return p;
}
static void c_free(void *ptr, size_t size, void *ctx)
{
    counter_t *c = (counter_t *)ctx;
    (void)size;
    if (ptr) { free(ptr); atomic_fetch_sub(&c->live, 1); }
}

/* -- callback inventory --------------------------------------------- */

static atomic_int pumps;
static atomic_int activities;

static int count_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    atomic_fetch_add(&pumps, 1);
    return 0;
}
static void count_activity(moq_pico_wt_managed_t *m, void *ctx)
{
    (void)m; (void)ctx;
    atomic_fetch_add(&activities, 1);
}

/* -- the silent peer -------------------------------------------------- */

/* Bind a UDP socket on 127.0.0.1 with a kernel-chosen port and never read
 * from it. Returns the fd, or -1 with the reason printed. */
static int open_udp_sink(int *port_out)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("ENV: socket(AF_INET, SOCK_DGRAM)"); return -1; }
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("ENV: bind(127.0.0.1:0)");
        close(fd);
        return -1;
    }
    socklen_t len = sizeof(sa);
    if (getsockname(fd, (struct sockaddr *)&sa, &len) < 0) {
        perror("ENV: getsockname");
        close(fd);
        return -1;
    }
    *port_out = (int)ntohs(sa.sin_port);
    return fd;
}

#define BOUNDED_WAIT_US   1000000ULL   /* 1 s: far below the handshake bound */
#define PRE_WAKE_WAIT_US   300000ULL   /* long enough to span several loop cycles */
/* An acknowledgement is PROMPT: it must arrive well inside the bound, not at
 * it. A wait that returns MOQ_OK only because its timeout elapsed and then
 * found the bit set (a bit published without its condition signal) is not
 * an acknowledgement a waiter can use. */
#define PROMPT_LIMIT_US    500000ULL

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* A bounded wait that must return MOQ_OK promptly. */
static void expect_prompt_ack(const char *label, moq_pico_wt_managed_t *cli)
{
    uint64_t t0 = mono_us();
    moq_result_t r = moq_pico_wt_managed_wait(cli, BOUNDED_WAIT_US);
    uint64_t took = mono_us() - t0;
    if (r != MOQ_OK) {
        fprintf(stderr, "FAIL: %s: expected MOQ_OK, got %d after %llu us\n",
                label, (int)r, (unsigned long long)took);
        failures++;
    } else if (took > PROMPT_LIMIT_US) {
        fprintf(stderr, "FAIL: %s: MOQ_OK arrived after %llu us, over the %llu us prompt limit "
                        "(a retained bit without its signal)\n",
                label, (unsigned long long)took, (unsigned long long)PROMPT_LIMIT_US);
        failures++;
    } else {
        printf("ok: %s: 0 after %llu us\n", label, (unsigned long long)took);
    }
}

int main(void)
{
    int port = 0;
    int sink = open_udp_sink(&port);
    if (sink < 0) {
        fprintf(stderr, "test_pico_wt_managed_presession_wake: ENVIRONMENT: "
                        "no local UDP sink could be bound\n");
        return 3;
    }
    printf("udp sink bound at 127.0.0.1:%d (never answers)\n", port);

    counter_t counter = { 0, 0 };
    moq_alloc_t alloc = { &counter, c_alloc, c_realloc, c_free };

    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_pump = count_pump;
    cfg.on_activity = count_activity;

    moq_pico_wt_managed_t *cli = NULL;
    CHECK_RESULT("create", moq_pico_wt_managed_create(&cfg, &cli), MOQ_OK);
    if (!cli) { close(sink); return 1; }

    /* Pre-session, nonterminal, loop up: a wake is only accepted once the
     * network thread exists, and no terminal has latched. */
    CHECK(moq_pico_wt_managed_session(cli) == NULL);
    CHECK(!moq_pico_wt_managed_is_fatal(cli));
    CHECK(!moq_pico_wt_managed_is_closed(cli));

    /* CONTROL: without any wake, ordinary pre-session loop activity (the
     * client's Initial sends, time checks) must not read as activity. */
    CHECK_RESULT("pre-wake wait (control)",
                 moq_pico_wt_managed_wait(cli, PRE_WAKE_WAIT_US), MOQ_DONE);
    CHECK(moq_pico_wt_managed_session(cli) == NULL);
    CHECK(!moq_pico_wt_managed_is_fatal(cli));

    /* THE ROW: an accepted explicit wake, handled before the session exists,
     * must be acknowledged to a bounded waiter. */
    CHECK_RESULT("wake 1", moq_pico_wt_managed_wake(cli), MOQ_OK);
    expect_prompt_ack("wait after wake 1", cli);
    /* Consumed exactly once: the level is cleared by the wait that saw it. */
    CHECK_RESULT("nonblocking wait after consuming wake 1",
                 moq_pico_wt_managed_wait(cli, 0), MOQ_DONE);

    /* A second wake is a second, single acknowledgement. */
    CHECK_RESULT("wake 2", moq_pico_wt_managed_wake(cli), MOQ_OK);
    expect_prompt_ack("wait after wake 2", cli);
    CHECK_RESULT("nonblocking wait after consuming wake 2",
                 moq_pico_wt_managed_wait(cli, 0), MOQ_DONE);

    /* Still pre-session and nonterminal; no pump, no post-pump signal. */
    CHECK(moq_pico_wt_managed_session(cli) == NULL);
    CHECK(!moq_pico_wt_managed_is_fatal(cli));
    CHECK(!moq_pico_wt_managed_is_closed(cli));
    printf("callbacks: on_pump=%d on_activity=%d\n",
           atomic_load(&pumps), atomic_load(&activities));
    CHECK(atomic_load(&pumps) == 0);
    CHECK(atomic_load(&activities) == 0);

    /* Bounded teardown: stop joins the network thread; destroy frees. The
     * ledger so far is flushed first so a teardown fault cannot erase it. */
    fflush(stdout);
    CHECK_RESULT("stop", moq_pico_wt_managed_stop(cli), MOQ_OK);
    CHECK_RESULT("wait after stop", moq_pico_wt_managed_wait(cli, 0), MOQ_ERR_CLOSED);
    fflush(stdout);
    moq_pico_wt_managed_destroy(cli);
    close(sink);

    printf("configured allocator: %ld allocations, %ld live after destroy\n",
           atomic_load(&counter.allocs), atomic_load(&counter.live));
    CHECK(atomic_load(&counter.allocs) > 0);
    CHECK(atomic_load(&counter.live) == 0);
    CHECK(atomic_load(&pumps) == 0);
    CHECK(atomic_load(&activities) == 0);

    if (failures == 0)
        printf("test_pico_wt_managed_presession_wake: PASS\n");
    else
        fprintf(stderr, "test_pico_wt_managed_presession_wake: %d failure(s)\n",
                failures);
    return failures ? 1 : 0;
}
