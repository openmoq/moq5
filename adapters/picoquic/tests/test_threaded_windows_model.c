/*
 * Deterministic host coverage of the real Windows shim and managed callers.
 * This is NOT native Windows runtime coverage. The WT target selects the
 * actual WT facade; the default target selects the raw facade.
 *
 * Load provider/libc declarations in host mode, then select only the facade's
 * Windows branches. Typed Win32 fakes model the native wait and Winsock calls;
 * the provider remains real except for starting/waking/joining its thread.
 * No socket or thread is started, and no deadline algorithm is copied here.
 */
#ifdef _WIN32
#error "Run this model on a POSIX host; it does not replace Windows tests"
#endif
#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>
#include <picoquic_packet_loop.h>
#include "../../common/moq_alpn.h"
#include "../../common/moq_setup_tokens.h"
#include "../../common/moq_adapter_binding.h"
#include "../../common/moq_pq_stream_backlog.h"
#include "../picoquic_endpoint.h"
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef MOQ_TEST_WT_MANAGED_MODEL
#include <moq/pico_wt_managed.h>
#include "../../pico_wt/pico_wt_adapter.h"
#include <picoquic_utils.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif

static int model_timespec_get(struct timespec *out, int base);
static picoquic_network_thread_ctx_t *model_start_thread(picoquic_quic_t *quic,
    picoquic_packet_loop_param_t *param, picoquic_packet_loop_cb_fn callback,
    void *ctx, int *ret);
static int model_wake_thread(picoquic_network_thread_ctx_t *ctx);
static void model_delete_thread(picoquic_network_thread_ctx_t *ctx);

/* Do not collide with host pthread declarations. These names are private to
 * this translation unit; the shipped header/source gain no test seams. */
#define pthread_mutex_t model_mutex_t
#define pthread_cond_t model_cond_t
#define pthread_t model_thread_t
#define pthread_mutex_init model_mutex_init
#define pthread_mutex_destroy model_mutex_destroy
#define pthread_mutex_lock model_mutex_lock
#define pthread_mutex_unlock model_mutex_unlock
#define pthread_mutex_trylock model_mutex_trylock
#define pthread_cond_init model_cond_init
#define pthread_cond_destroy model_cond_destroy
#define pthread_cond_broadcast model_cond_broadcast
#define pthread_cond_wait model_cond_wait
#define pthread_cond_timedwait model_cond_timedwait
#define pthread_self model_thread_self
#define pthread_equal model_thread_equal
#define clock_gettime model_clock_gettime
#define timespec_get model_timespec_get
#define picoquic_start_network_thread model_start_thread
#define picoquic_wake_up_network_thread model_wake_thread
#define picoquic_delete_network_thread model_delete_thread
/* Exercise the guarded unsupported-D22 failure even with a newer provider.
 * All provider headers were already loaded using their genuine host ABI. */
#ifndef MOQ_TEST_WT_MANAGED_MODEL
#undef PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION
#endif
#define _WIN32 1
#include "../../common/moq_posix_compat.h"
/* Count caller retries above the real shim too: a shim validation/clock
 * failure need not reach SleepConditionVariableSRW. Neither wrapper changes
 * the result or implements any deadline arithmetic. */
static int model_checked_timedwait(pthread_cond_t *cond, pthread_mutex_t *lock,
                                   const struct timespec *deadline);
static int model_checked_wait(pthread_cond_t *cond, pthread_mutex_t *lock);
#undef pthread_cond_timedwait
#undef pthread_cond_wait
#define pthread_cond_timedwait model_checked_timedwait
#define pthread_cond_wait model_checked_wait
#ifdef MOQ_TEST_WT_MANAGED_MODEL
#include "../../pico_wt/pico_wt_managed.c"
#else
#include "../moq_picoquic_threaded.c"
#include "../pq_lane_group_unsupported.c"
#endif
#undef _WIN32

#ifdef MOQ_TEST_WT_MANAGED_MODEL
typedef moq_pico_wt_managed_t model_facade_t;
#define model_public_wait moq_pico_wt_managed_wait
#define model_public_stop moq_pico_wt_managed_stop
#else
typedef moq_pq_threaded_t model_facade_t;
#define model_public_wait moq_pq_threaded_wait
#define model_public_stop moq_pq_threaded_stop
#endif

static unsigned model_checks, model_failures;
static const char *model_row;
#define CHECK(expr) do { \
    model_checks++; \
    if (!(expr)) { \
        fprintf(stderr, "FAIL [%s] %s:%d: %s\n", model_row, __FILE__, \
                __LINE__, #expr); \
        model_failures++; \
    } \
} while (0)
#define REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "SETUP/MODEL [%s] %s:%d: %s\n", model_row, __FILE__, \
                __LINE__, #expr); \
        exit(2); \
    } \
} while (0)

typedef struct wait_step {
    DWORD milliseconds;
    DWORD error;                 /* zero means a signalled/spurious wake */
    bool set_time;               /* explicit wall-clock change during wait */
    struct timespec time;
    void (*while_unlocked)(void *ctx);
    void *ctx;
} wait_step_t;

static struct {
    struct timespec now;
    const wait_step_t *steps;
    size_t step_count, calls;
    unsigned clock_calls, fail_clock_at, releases, reacquires;
    unsigned cond_calls, cond_limit;
    bool persistent_clock_failure, leave_failed_clock_untouched;
    DWORD last_error;
} wait_model;

static void clock_reset(struct timespec now, const wait_step_t *steps, size_t n)
{
    memset(&wait_model, 0, sizeof(wait_model));
    wait_model.now = now;
    wait_model.steps = steps;
    wait_model.step_count = n;
    wait_model.cond_limit = 1;
}

static int model_timespec_get(struct timespec *out, int base)
{
    REQUIRE(out && base == TIME_UTC);
    wait_model.clock_calls++;
    bool failed = wait_model.fail_clock_at &&
        (wait_model.clock_calls == wait_model.fail_clock_at ||
         (wait_model.persistent_clock_failure &&
          wait_model.clock_calls > wait_model.fail_clock_at));
    /* Original shim rows retain their deterministic failure output. New
     * caller rows may leave it untouched: failure promises no timestamp. */
    if (!failed || !wait_model.leave_failed_clock_untouched)
        *out = wait_model.now;
    return failed ? 0 : TIME_UTC;
}

static int model_checked_timedwait(pthread_cond_t *cond, pthread_mutex_t *lock,
                                   const struct timespec *deadline)
{
    REQUIRE(wait_model.cond_calls < wait_model.cond_limit);
    wait_model.cond_calls++;
    return model_cond_timedwait(cond, lock, deadline);
}
static int model_checked_wait(pthread_cond_t *cond, pthread_mutex_t *lock)
{
    REQUIRE(wait_model.cond_calls < wait_model.cond_limit);
    wait_model.cond_calls++;
    return model_cond_wait(cond, lock);
}

void InitializeSRWLock(SRWLOCK *lock)
{
    REQUIRE(lock);
    lock->initialized = 1;
    lock->held = 0;
}
void AcquireSRWLockExclusive(SRWLOCK *lock)
{
    REQUIRE(lock && lock->initialized && !lock->held);
    lock->held = 1;
}
void ReleaseSRWLockExclusive(SRWLOCK *lock)
{
    REQUIRE(lock && lock->initialized && lock->held);
    lock->held = 0;
}
BOOL TryAcquireSRWLockExclusive(SRWLOCK *lock)
{
    REQUIRE(lock && lock->initialized);
    if (lock->held) return 0;
    lock->held = 1;
    return 1;
}
void InitializeConditionVariable(CONDITION_VARIABLE *cond)
{
    REQUIRE(cond);
    cond->initialized = 1;
}
void WakeAllConditionVariable(CONDITION_VARIABLE *cond)
{
    REQUIRE(cond && cond->initialized);
}
BOOL SleepConditionVariableSRW(CONDITION_VARIABLE *cond, SRWLOCK *lock,
                               DWORD milliseconds, ULONG flags)
{
    REQUIRE(cond && cond->initialized && lock && lock->initialized && lock->held);
    /* An unexpected retry must fail boundedly, never hang a causal run. */
    REQUIRE(wait_model.calls < wait_model.step_count);
    const wait_step_t *step = &wait_model.steps[wait_model.calls++];
    CHECK(flags == 0);
    CHECK(milliseconds == step->milliseconds);
    lock->held = 0;
    wait_model.releases++;
    if (step->while_unlocked) step->while_unlocked(step->ctx);
    if (step->set_time) {
        wait_model.now = step->time;
    } else if (step->error == ERROR_TIMEOUT) {
        REQUIRE(milliseconds != INFINITE);
        /* Advance by the ACTUAL native request, not the expected request.
         * Thus an old zero-ms wait really does return before the deadline. */
        wait_model.now.tv_sec += (time_t)(milliseconds / 1000);
        wait_model.now.tv_nsec += (long)(milliseconds % 1000) * 1000000L;
        if (wait_model.now.tv_nsec >= 1000000000L) {
            wait_model.now.tv_sec++;
            wait_model.now.tv_nsec -= 1000000000L;
        }
    }
    lock->held = 1;
    wait_model.reacquires++;
    wait_model.last_error = step->error;
    return step->error == 0;
}
DWORD GetLastError(void) { return wait_model.last_error; }
DWORD GetCurrentThreadId(void) { return 1; }
void Sleep(DWORD milliseconds)
{
    (void)milliseconds;
    /* WT startup's separate polling/race is outside this correction. */
    REQUIRE(false);
}
BOOL QueryPerformanceFrequency(LARGE_INTEGER *frequency)
{
    REQUIRE(frequency);
    frequency->QuadPart = 1000000;
    return 1;
}
BOOL QueryPerformanceCounter(LARGE_INTEGER *counter)
{
    REQUIRE(counter);
    counter->QuadPart = 0;
    return 1;
}

static bool at_or_after(struct timespec time, struct timespec deadline)
{
    return time.tv_sec > deadline.tv_sec ||
        (time.tv_sec == deadline.tv_sec && time.tv_nsec >= deadline.tv_nsec);
}

static void wait_row(const char *name, struct timespec now, struct timespec until,
    const wait_step_t *steps, size_t n, size_t expected_calls,
    unsigned fail_clock_at, int expected_result)
{
    model_row = name;
    clock_reset(now, steps, n);
    wait_model.fail_clock_at = fail_clock_at;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    REQUIRE(pthread_mutex_init(&lock, NULL) == 0);
    REQUIRE(pthread_cond_init(&cond, NULL) == 0);
    REQUIRE(pthread_mutex_lock(&lock) == 0);
    int result = pthread_cond_timedwait(&cond, &lock, &until);
    CHECK(result == expected_result);
    CHECK(wait_model.calls == expected_calls);
    CHECK(wait_model.releases == expected_calls);
    CHECK(wait_model.reacquires == expected_calls);
    CHECK(lock.held == 1);
    if (result == ETIMEDOUT)
        CHECK(at_or_after(wait_model.now, until));
    if (fail_clock_at) CHECK(wait_model.clock_calls == fail_clock_at);
    CHECK(pthread_mutex_unlock(&lock) == 0);
    CHECK(pthread_cond_destroy(&cond) == 0);
    CHECK(pthread_mutex_destroy(&lock) == 0);
}

static void test_waits(void)
{
    const wait_step_t one[] = {{ .milliseconds = 1, .error = ERROR_TIMEOUT }};
    const wait_step_t two[] = {{ .milliseconds = 2, .error = ERROR_TIMEOUT }};
    const wait_step_t zero[] = {{ .milliseconds = 0, .error = ERROR_TIMEOUT }};
    const struct timespec now = { 100, 0 };
    wait_row("500us", now, (struct timespec){100, 500000}, one, 1, 1, 0, ETIMEDOUT);
    wait_row("1ns", now, (struct timespec){100, 1}, one, 1, 1, 0, ETIMEDOUT);
    wait_row("exact-ms", now, (struct timespec){100, 1000000}, one, 1, 1, 0, ETIMEDOUT);
    wait_row("ms-plus-ns", now, (struct timespec){100, 1000001}, two, 1, 1, 0, ETIMEDOUT);
    wait_row("second-borrow", (struct timespec){100, 999999999},
        (struct timespec){101, 0}, one, 1, 1, 0, ETIMEDOUT);
    wait_row("negative-second-borrow", (struct timespec){-1, 999999999},
        (struct timespec){0, 0}, one, 1, 1, 0, ETIMEDOUT);
    wait_row("already-equal", now, now, zero, 1, 1, 0, ETIMEDOUT);
    wait_row("already-past", now, (struct timespec){99, 0}, zero, 1, 1, 0, ETIMEDOUT);

    const wait_step_t capped[] = {
        { .milliseconds = INFINITE - 1, .error = ERROR_TIMEOUT },
        { .milliseconds = 1, .error = ERROR_TIMEOUT }
    };
    wait_row("finite-cap-recheck", (struct timespec){0, 0},
        (struct timespec){4294967, 295000000}, capped, 2, 2, 0, ETIMEDOUT);
    const wait_step_t rollback[] = {
        { .milliseconds = 2, .error = ERROR_TIMEOUT, .set_time = true,
          .time = {99, 998000000} },
        { .milliseconds = 4, .error = ERROR_TIMEOUT }
    };
    wait_row("rollback-recheck", now, (struct timespec){100, 2000000},
        rollback, 2, 2, 0, ETIMEDOUT);

    const wait_step_t wake[] = {{ .milliseconds = 1 }};
    const wait_step_t expired_wake[] = {{ .milliseconds = 0 }};
    wait_row("early-signal-or-spurious", now, (struct timespec){100, 1000000},
        wake, 1, 1, 0, 0);
    wait_row("expired-but-signalled", now, now, expired_wake, 1, 1, 0, 0);
    const wait_step_t error[] = {
        { .milliseconds = 1, .error = ERROR_INVALID_PARAMETER }
    };
    wait_row("native-error", now, (struct timespec){100, 1000000}, error, 1, 1, 0, EINVAL);
    /* A spare wake keeps an incorrect old clock-failure path bounded; the
     * corrected code must not consume it. */
    wait_row("clock-fails-before-wait", now, (struct timespec){100, 1000000},
        wake, 1, 0, 1, EINVAL);
    wait_row("clock-fails-after-timeout", now, (struct timespec){100, 1000000},
        one, 1, 1, 2, EINVAL);
    wait_row("invalid-negative-nsec", now, (struct timespec){100, -1},
        expired_wake, 1, 0, 0, EINVAL);
    const wait_step_t invalid_wake[] = {{ .milliseconds = 1000 }};
    wait_row("invalid-billion-nsec", now, (struct timespec){100, 1000000000},
        invalid_wake, 1, 0, 0, EINVAL);

    /* Actual public caller: an idle pre-handshake facade has no activity or
     * terminal latch. Only the private lock/condition need initialization. */
    model_row = "public-wait-500us";
    model_facade_t t;
    memset(&t, 0, sizeof(t));
    REQUIRE(pthread_mutex_init(&t.mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&t.cond, NULL) == 0);
    clock_reset(now, one, 1);
    CHECK(model_public_wait(&t, 500) == MOQ_DONE);
    CHECK(at_or_after(wait_model.now, (struct timespec){100, 500000}));
    CHECK(wait_model.calls == 1 && wait_model.releases == 1 &&
          wait_model.reacquires == 1 && t.mutex.held == 0);
    model_row = "public-wait-controls";
    clock_reset(now, NULL, 0);
    t.activity_pending = true;
    CHECK(model_public_wait(&t, 500) == MOQ_OK);
    CHECK(!t.activity_pending && !t.mutex.held);
    CHECK(model_public_wait(&t, 0) == MOQ_DONE);
    t.stopped = true;
    CHECK(model_public_wait(&t, 500) == MOQ_ERR_CLOSED);
    CHECK(wait_model.calls == 0 && wait_model.clock_calls == 0 && !t.mutex.held);
    CHECK(pthread_cond_destroy(&t.cond) == 0);
    CHECK(pthread_mutex_destroy(&t.mutex) == 0);
}

#ifndef MOQ_TEST_WT_MANAGED_MODEL
typedef struct allocation {
    void *ptr;
    size_t size;
} allocation_t;
typedef struct alloc_model {
    allocation_t live[32];
    unsigned calls, fail_at, blocks;
    size_t bytes;
} alloc_model_t;

static size_t allocation_index(alloc_model_t *a, void *ptr)
{
    for (size_t i = 0; i < sizeof(a->live) / sizeof(a->live[0]); i++)
        if (a->live[i].ptr == ptr) return i;
    REQUIRE(false);
    return 0;
}
static void *count_alloc(size_t size, void *ctx)
{
    alloc_model_t *a = ctx;
    if (++a->calls == a->fail_at) return NULL;
    size_t i = allocation_index(a, NULL);
    void *ptr = malloc(size);
    REQUIRE(ptr);
    a->live[i] = (allocation_t){ .ptr = ptr, .size = size };
    a->blocks++;
    a->bytes += size;
    return ptr;
}
static void count_free(void *ptr, size_t size, void *ctx)
{
    if (!ptr) return;
    alloc_model_t *a = ctx;
    size_t i = allocation_index(a, ptr);
    CHECK(a->live[i].size == size);
    a->bytes -= a->live[i].size;
    a->blocks--;
    a->live[i] = (allocation_t){0};
    free(ptr);
}
static void *count_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    if (!ptr) return count_alloc(new_size, ctx);
    if (!new_size) { count_free(ptr, old_size, ctx); return NULL; }
    alloc_model_t *a = ctx;
    size_t i = allocation_index(a, ptr);
    CHECK(a->live[i].size == old_size);
    if (++a->calls == a->fail_at) return NULL;
    void *next = realloc(ptr, new_size);
    REQUIRE(next);
    a->bytes = a->bytes - a->live[i].size + new_size;
    a->live[i] = (allocation_t){ .ptr = next, .size = new_size };
    return next;
}
#endif

static struct {
    int startup_error;
    unsigned startups, successes, cleanups, references;
    unsigned starts, wakes, deletes, configured;
    bool fail_start, unready, thread_live, fail_startup_shim_clock;
    model_facade_t *owner;
    picoquic_network_thread_ctx_t thread;
} net_model;

int WSAStartup(WORD version, WSADATA *data)
{
    CHECK(version == MAKEWORD(2, 2));
    REQUIRE(data);
    net_model.startups++;
    if (net_model.startup_error) return net_model.startup_error;
    data->wVersion = version;
    net_model.successes++;
    net_model.references++;
    return 0;
}
int WSACleanup(void)
{
    net_model.cleanups++;
    CHECK(net_model.references > 0);
    if (net_model.references) net_model.references--;
    return 0;
}
static picoquic_network_thread_ctx_t *model_start_thread(picoquic_quic_t *quic,
    picoquic_packet_loop_param_t *param, picoquic_packet_loop_cb_fn callback,
    void *ctx, int *ret)
{
    REQUIRE(quic && param && callback && ctx && ret);
    net_model.starts++;
    *ret = net_model.fail_start ? -1 : 0;
    if (net_model.fail_start) return NULL;
    REQUIRE(!net_model.thread_live);
    net_model.thread_live = true;
    if (net_model.fail_startup_shim_clock)
        wait_model.fail_clock_at = wait_model.clock_calls + 2;
    /* Publish the creator's ready latch synchronously. No worker exists:
     * stop can call the fake wake/join without a real wait. This control
     * exercises facade ownership, not provider thread synchronization. */
    model_facade_t *t = ctx;
    net_model.owner = t;
    REQUIRE(pthread_mutex_lock(&t->mutex) == 0);
    t->loop_came_up = !net_model.unready;
    t->loop_running = false;
    REQUIRE(pthread_mutex_unlock(&t->mutex) == 0);
    return &net_model.thread;
}
static int model_wake_thread(picoquic_network_thread_ctx_t *ctx)
{
    CHECK(ctx == &net_model.thread);
    CHECK(net_model.thread_live);
    if (net_model.owner && net_model.owner->stopped) {
        CHECK(!net_model.owner->mutex.held);
        CHECK(net_model.owner->wake_in_flight == 0);
    }
    net_model.wakes++;
    return 0;
}
static void model_delete_thread(picoquic_network_thread_ctx_t *ctx)
{
    CHECK(ctx == &net_model.thread);
    CHECK(net_model.thread_live);
    if (net_model.owner) {
        CHECK(!net_model.owner->mutex.held);
        CHECK(net_model.owner->wake_in_flight == 0);
    }
    net_model.thread_live = false;
    net_model.deletes++;
}

static void facade_init(model_facade_t *t)
{
    memset(t, 0, sizeof(*t));
    REQUIRE(pthread_mutex_init(&t->mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&t->cond, NULL) == 0);
}
static void facade_finish(model_facade_t *t)
{
    CHECK(!t->mutex.held);
    int rc = pthread_mutex_trylock(&t->mutex);
    CHECK(rc == 0);
    if (rc == 0) CHECK(pthread_mutex_unlock(&t->mutex) == 0);
    CHECK(pthread_cond_destroy(&t->cond) == 0);
    CHECK(pthread_mutex_destroy(&t->mutex) == 0);
}
static void publish_activity(void *ctx)
{
    model_facade_t *t = ctx;
    REQUIRE(!t->mutex.held);
    t->activity_pending = true;
}
static void publish_terminal_activity(void *ctx)
{
    model_facade_t *t = ctx;
    publish_activity(ctx);
#ifdef MOQ_TEST_WT_MANAGED_MODEL
    t->closed = true;
#else
    t->fatal = true;
#endif
}
static void finish_loop(void *ctx)
{
    model_facade_t *t = ctx;
    REQUIRE(!t->mutex.held);
    t->loop_running = false;
}
static void finish_fatal_loop(void *ctx)
{
    model_facade_t *t = ctx;
    finish_loop(ctx);
    t->fatal = true;
}
static void release_wake_borrow(void *ctx)
{
    model_facade_t *t = ctx;
    REQUIRE(!t->mutex.held && t->wake_in_flight == 1);
    CHECK(net_model.wakes == 0 && net_model.deletes == 0);
    t->wake_in_flight = 0;
}

enum caller_failure { INITIAL_CLOCK, SHIM_CLOCK, NATIVE_WAIT, INFINITE_WAIT };

static void test_public_failure(enum caller_failure which)
{
    model_facade_t t;
    facade_init(&t);
    const wait_step_t error[] = {{
        .milliseconds = which == INFINITE_WAIT ? INFINITE : 1,
        .error = ERROR_INVALID_PARAMETER
    }};
    bool native = which == NATIVE_WAIT || which == INFINITE_WAIT;
    clock_reset((struct timespec){100, 0}, native ? error : NULL, native ? 1 : 0);
    wait_model.cond_limit = which == INITIAL_CLOCK ? 0 : 1;
    wait_model.fail_clock_at = which == INITIAL_CLOCK ? 1 : which == SHIM_CLOCK ? 2 : 0;
    wait_model.persistent_clock_failure = true;
    wait_model.leave_failed_clock_untouched = true;
    CHECK(model_public_wait(&t, which == INFINITE_WAIT ? UINT64_MAX : 500) ==
          MOQ_ERR_INTERNAL);
    CHECK(wait_model.calls == (native ? 1u : 0u));
    CHECK(wait_model.cond_calls == wait_model.cond_limit);
    CHECK(wait_model.clock_calls == (which == INFINITE_WAIT ? 0u :
          which == INITIAL_CLOCK ? 1u : 2u));
    CHECK(!t.stopped && !t.fatal && !t.activity_pending);
    facade_finish(&t);
}

static void public_control(const char *name, uint64_t timeout, DWORD error,
    void (*publish)(void *), bool spurious_first, moq_result_t expected)
{
    model_row = name;
    model_facade_t t;
    facade_init(&t);
    wait_step_t steps[2] = {{0}, {0}};
    size_t n = spurious_first ? 2 : 1;
    steps[0].milliseconds = timeout == UINT64_MAX ? INFINITE : 1;
    steps[n - 1] = (wait_step_t){ .milliseconds = steps[0].milliseconds,
        .error = error, .while_unlocked = publish, .ctx = &t };
    clock_reset((struct timespec){100, 0}, steps, n);
    wait_model.cond_limit = (unsigned)n;
    CHECK(model_public_wait(&t, timeout) == expected);
    CHECK(wait_model.calls == n && wait_model.cond_calls == n);
    CHECK(t.activity_pending == (expected == MOQ_ERR_CLOSED));
    facade_finish(&t);
}

static void test_public_controls(void)
{
    public_control("public-signal", 500, 0, publish_activity, false, MOQ_OK);
    public_control("public-spurious-then-signal", 500, 0, publish_activity, true, MOQ_OK);
    public_control("public-timeout", 500, ERROR_TIMEOUT, NULL, false, MOQ_DONE);
    public_control("public-timeout-activity", 500, ERROR_TIMEOUT, publish_activity, false, MOQ_OK);
    public_control("public-error-activity", 500, ERROR_INVALID_PARAMETER, publish_activity, false, MOQ_OK);
    public_control("public-error-terminal-activity", 500, ERROR_INVALID_PARAMETER,
                   publish_terminal_activity, false, MOQ_ERR_CLOSED);
    public_control("public-infinite-signal", UINT64_MAX, 0, publish_activity, false, MOQ_OK);
    public_control("public-infinite-error-terminal", UINT64_MAX, ERROR_INVALID_PARAMETER,
                   publish_terminal_activity, false, MOQ_ERR_CLOSED);

    model_row = "public-entry-precedence-before-clock";
    model_facade_t t;
    facade_init(&t);
    clock_reset((struct timespec){100, 0}, NULL, 0);
    wait_model.cond_limit = 0;
    wait_model.fail_clock_at = 1;
    wait_model.leave_failed_clock_untouched = true;
    t.activity_pending = true;
    CHECK(model_public_wait(&t, 500) == MOQ_OK && !t.activity_pending);
    t.activity_pending = true;
    t.stopped = true;
    CHECK(model_public_wait(&t, 500) == MOQ_ERR_CLOSED && t.activity_pending);
    CHECK(wait_model.clock_calls == 0 && wait_model.cond_calls == 0);
    facade_finish(&t);
}

static void stop_init(model_facade_t *t)
{
    facade_init(t);
    memset(&net_model, 0, sizeof(net_model));
    net_model.owner = t;
    net_model.thread_live = true;
    t->thread_ctx = &net_model.thread;
    t->loop_running = true;
}
static void stop_finished(model_facade_t *t)
{
    CHECK(net_model.wakes == 1 && net_model.deletes == 1 && !net_model.thread_live);
    CHECK(t->stopped && t->activity_pending && !t->thread_ctx && !t->wake_in_flight);
    facade_finish(t);
}
static void test_stop_failure(enum caller_failure which)
{
    REQUIRE(which != INFINITE_WAIT);
    model_facade_t t;
    stop_init(&t);
    wait_step_t step = { .milliseconds = 3000, .error = ERROR_INVALID_PARAMETER };
    if (which == INITIAL_CLOCK) {
        /* The initial-clock error is reached only AFTER the ownership fence
         * drains a real modeled borrower. No error may bypass that fence. */
        t.wake_in_flight = 1;
        step = (wait_step_t){ .milliseconds = INFINITE,
            .while_unlocked = release_wake_borrow, .ctx = &t };
    }
    clock_reset((struct timespec){100, 0}, which == SHIM_CLOCK ? NULL : &step,
                which == SHIM_CLOCK ? 0 : 1);
    wait_model.fail_clock_at = which == INITIAL_CLOCK ? 1 : which == SHIM_CLOCK ? 2 : 0;
    wait_model.persistent_clock_failure = true;
    wait_model.leave_failed_clock_untouched = true;
    CHECK(model_public_stop(&t) == MOQ_OK);
    CHECK(wait_model.cond_calls == 1);
    CHECK(wait_model.calls == (which == SHIM_CLOCK ? 0u : 1u));
    CHECK(wait_model.clock_calls == (which == INITIAL_CLOCK ? 1u : 2u));
    stop_finished(&t);
}

static void stop_control(const char *name, DWORD error,
                         void (*publish)(void *), moq_result_t expected)
{
    model_row = name;
    model_facade_t t;
    stop_init(&t);
    wait_step_t step = { .milliseconds = 3000, .error = error,
        .while_unlocked = publish, .ctx = &t };
    clock_reset((struct timespec){100, 0}, &step, 1);
    CHECK(model_public_stop(&t) == expected);
    CHECK(wait_model.cond_calls == 1 && wait_model.calls == 1);
    stop_finished(&t);
}
static void test_stop_controls(void)
{
    stop_control("stop-loop-exits", 0, finish_loop, MOQ_OK);
    stop_control("stop-timeout-still-joins", ERROR_TIMEOUT, NULL, MOQ_OK);
    stop_control("stop-fatal-precedence", 0, finish_fatal_loop, MOQ_ERR_CLOSED);

    model_row = "stop-untimed-fence-survives-error";
    model_facade_t t;
    stop_init(&t);
    t.wake_in_flight = 1;
    wait_step_t steps[] = {
        { .milliseconds = INFINITE, .error = ERROR_INVALID_PARAMETER },
        { .milliseconds = INFINITE, .while_unlocked = release_wake_borrow, .ctx = &t },
        { .milliseconds = 3000, .while_unlocked = finish_loop, .ctx = &t }
    };
    clock_reset((struct timespec){100, 0}, steps, 3);
    wait_model.cond_limit = 3;
    CHECK(model_public_stop(&t) == MOQ_OK);
    CHECK(wait_model.calls == 3 && wait_model.cond_calls == 3);
    stop_finished(&t);
}

#ifndef MOQ_TEST_WT_MANAGED_MODEL
static int unused_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                        uint64_t now, void *ctx)
{
    (void)t; (void)lane; (void)now; (void)ctx;
    REQUIRE(false);              /* no worker is running in this fixture */
    return 0;
}
static int reject_configuration(picoquic_quic_t *quic, void *ctx)
{
    REQUIRE(quic && ctx == &net_model);
    net_model.configured++;
    return -1;
}

enum create_case {
    INVALID_CONFIG, WRAPPER_OOM, STARTUP_FAIL, SHORT_TARGET, BAD_TARGET,
    TARGET_OOM, NULL_TOKENS, BAD_TOKEN, OVERSIZE_TOKEN, TOKEN_OOM,
    UNSUPPORTED_D22, BAD_ALPN, ALPN_OOM, CONFIGURE_FAIL, THREAD_FAIL, SUCCESS,
    STARTUP_CLOCK, STARTUP_SHIM, STARTUP_NATIVE, READY_CLOCK
};

static void create_row(enum create_case which, const char *name,
                       moq_result_t expected, unsigned startups, unsigned successes)
{
    model_row = name;
    memset(&net_model, 0, sizeof(net_model));
    clock_reset((struct timespec){100, 0}, NULL, 0);
    alloc_model_t memory = {0};
    moq_alloc_t alloc = { .ctx = &memory, .alloc = count_alloc,
        .realloc = count_realloc, .free = count_free };
    moq_connection_target_t target = { .struct_size = sizeof(target),
        .target_present = MOQ_BINDING_TARGET_COMPLETE,
        .authority = { .data = (const uint8_t *)"127.0.0.1:443", .len = 13 },
        .path_query = { .data = (const uint8_t *)"/", .len = 1 } };
    const uint8_t token_bytes[] = { 'a', 'b', 'c' };
    moq_auth_token_t token = { .token_type = 1,
        .token_value = { .data = token_bytes, .len = sizeof(token_bytes) } };
    const char *alpn18[] = { "moqt-18" };
    const char *alpn22[] = { "moqt-22" };
    const char *bad_alpn[] = { "" };
    const wait_step_t startup_errors[] = {
        { .milliseconds = 2000, .error = ERROR_INVALID_PARAMETER },
        { .milliseconds = 2000, .error = ERROR_INVALID_PARAMETER },
        { .milliseconds = 2000, .error = ERROR_INVALID_PARAMETER }
    };
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = unused_pump;
    cfg.target = &target;
    switch (which) {
    case INVALID_CONFIG: cfg.port = 0; break;
    case WRAPPER_OOM: memory.fail_at = 1; break;
    case STARTUP_FAIL: net_model.startup_error = 10091; break; /* WSASYSNOTREADY */
    case SHORT_TARGET: target.struct_size = MOQ_CONNECTION_TARGET_V0_SIZE - 1; break;
    case BAD_TARGET: target.authority.data = NULL; break;
    case TARGET_OOM: memory.fail_at = 2; break;  /* wrapper, target */
    case NULL_TOKENS: cfg.setup_auth_token_count = 1; break;
    case BAD_TOKEN:
    case OVERSIZE_TOKEN:
    case TOKEN_OOM:
        cfg.setup_auth_tokens = &token;
        cfg.setup_auth_token_count = 1;
        if (which == BAD_TOKEN) token.token_value.data = NULL;
        if (which == OVERSIZE_TOKEN) token.token_value.len = (size_t)UINT16_MAX + 1;
        if (which == TOKEN_OOM) memory.fail_at = 3; /* wrapper, target, tokens */
        break;
    case UNSUPPORTED_D22: cfg.alpn_list = alpn22; cfg.alpn_count = 1; break;
    case BAD_ALPN: cfg.alpn_list = bad_alpn; cfg.alpn_count = 1; break;
    case ALPN_OOM:
        cfg.alpn_list = alpn18;
        cfg.alpn_count = 1;
        memory.fail_at = 2;
        break;
    case CONFIGURE_FAIL:
        cfg.configure_quic = reject_configuration;
        cfg.configure_quic_ctx = &net_model;
        break;
    case THREAD_FAIL:
    case SUCCESS:
    case STARTUP_CLOCK:
    case STARTUP_SHIM:
    case STARTUP_NATIVE:
    case READY_CLOCK:
        /* Defer session creation to the handshake, which this model never
         * delivers. Only the actual provider context/connection are created. */
        cfg.alpn_list = alpn18;
        cfg.alpn_count = 1;
        cfg.setup_auth_tokens = &token;
        cfg.setup_auth_token_count = 1;
        net_model.fail_start = which == THREAD_FAIL;
        net_model.unready = which == STARTUP_CLOCK || which == STARTUP_SHIM ||
                            which == STARTUP_NATIVE;
        net_model.fail_startup_shim_clock = which == STARTUP_SHIM;
        if (which == STARTUP_CLOCK || which == READY_CLOCK) {
            wait_model.fail_clock_at = 1;
            wait_model.persistent_clock_failure = true;
            /* The ready-latch control writes a deterministic failed-clock
             * value so it isolates readiness from an unwritten clock output. */
            wait_model.leave_failed_clock_untouched = which == STARTUP_CLOCK;
            wait_model.cond_limit = 0;
        } else if (which == STARTUP_SHIM) {
            wait_model.leave_failed_clock_untouched = true;
            wait_model.cond_limit = 3;
        } else if (which == STARTUP_NATIVE) {
            wait_model.steps = startup_errors;
            wait_model.step_count = 3;
            wait_model.cond_limit = 3;
        }
        break;
    }
    moq_pq_threaded_t *t = NULL;
    moq_result_t result = moq_pq_threaded_create(&cfg, &t);
    CHECK(result == expected);
    CHECK((t != NULL) == (expected == MOQ_OK));
    CHECK(net_model.startups == startups);
    CHECK(net_model.successes == successes);
    if (which == READY_CLOCK) {
        CHECK(wait_model.clock_calls == 1 && wait_model.cond_calls == 0);
        /* This row isolates startup's already-ready precedence. */
        wait_model.fail_clock_at = 0;
        wait_model.persistent_clock_failure = false;
    }
    if (t) {
        CHECK(net_model.references == 1 && net_model.cleanups == 0);
        CHECK(net_model.starts == 1 && t->thread_ctx == &net_model.thread);
        CHECK(t->quic && t->active_cnx && t->client_deferred);
        CHECK(t->target_bytes && t->target_bytes_len ==
              target.authority.len + target.path_query.len);
        CHECK(t->setup_tokens.count == 1 && t->setup_tokens.tokens != &token);
        if (t->setup_tokens.count == 1 && t->setup_tokens.tokens)
            CHECK(memcmp(t->setup_tokens.tokens[0].token_value.data,
                         token_bytes, sizeof(token_bytes)) == 0);
        CHECK(moq_pq_threaded_stop(t) == MOQ_OK);
        CHECK(net_model.wakes == 1 && net_model.deletes == 1);
        CHECK(net_model.references == 1 && net_model.cleanups == 0);
        moq_pq_threaded_destroy(t);
    }
    CHECK(net_model.cleanups == successes && net_model.references == 0);
    CHECK(memory.blocks == 0 && memory.bytes == 0);
    if (memory.fail_at) CHECK(memory.calls == memory.fail_at);
    CHECK(net_model.configured == (unsigned)(which == CONFIGURE_FAIL));
    bool startup_wait_error = which == STARTUP_CLOCK || which == STARTUP_SHIM ||
                              which == STARTUP_NATIVE;
    bool created = which == SUCCESS || which == READY_CLOCK;
    CHECK(net_model.starts == (which == THREAD_FAIL || startup_wait_error ? 3u :
                              created ? 1u : 0u));
    CHECK(net_model.deletes == (startup_wait_error ? 3u : created ? 1u : 0u));
    CHECK(!net_model.thread_live);
    CHECK(wait_model.calls == (which == STARTUP_NATIVE ? 3u : 0u));
    CHECK(wait_model.cond_calls ==
          (which == STARTUP_SHIM || which == STARTUP_NATIVE ? 3u : 0u));
    if (startup_wait_error)
        CHECK(wait_model.clock_calls == (which == STARTUP_CLOCK ? 3u : 6u));
}

static void test_startup(void)
{
    create_row(INVALID_CONFIG, "pre-start-invalid", MOQ_ERR_INVAL, 0, 0);
    create_row(WRAPPER_OOM, "pre-start-oom", MOQ_ERR_NOMEM, 0, 0);
    create_row(STARTUP_FAIL, "startup-itself-fails", MOQ_ERR_INTERNAL, 1, 0);
    create_row(SHORT_TARGET, "short-target", MOQ_ERR_INVAL, 1, 1);
    create_row(BAD_TARGET, "bad-target", MOQ_ERR_INVAL, 1, 1);
    create_row(TARGET_OOM, "target-copy-oom", MOQ_ERR_NOMEM, 1, 1);
    create_row(NULL_TOKENS, "missing-tokens", MOQ_ERR_INVAL, 1, 1);
    create_row(BAD_TOKEN, "bad-token", MOQ_ERR_INVAL, 1, 1);
    create_row(OVERSIZE_TOKEN, "oversize-token", MOQ_ERR_INVAL, 1, 1);
    create_row(TOKEN_OOM, "token-copy-oom", MOQ_ERR_NOMEM, 1, 1);
    create_row(UNSUPPORTED_D22, "unsupported-d22", MOQ_ERR_UNSUPPORTED, 1, 1);
    create_row(BAD_ALPN, "common-label-invalid", MOQ_ERR_INTERNAL, 1, 1);
    create_row(ALPN_OOM, "common-label-oom", MOQ_ERR_INTERNAL, 1, 1);
    create_row(CONFIGURE_FAIL, "provider-configure-fails", MOQ_ERR_INTERNAL, 1, 1);
    create_row(THREAD_FAIL, "thread-start-fails", MOQ_ERR_INTERNAL, 1, 1);
    create_row(SUCCESS, "create-stop-destroy", MOQ_OK, 1, 1);
}

static void single_lane_control(moq_perspective_t perspective, uint32_t lanes,
                                 bool legacy)
{
    model_row = perspective == MOQ_PERSPECTIVE_SERVER ? "windows-server" : "windows-client";
    memset(&net_model, 0, sizeof(net_model));
    clock_reset((struct timespec){100, 0}, NULL, 0);
    alloc_model_t memory = {0};
    moq_alloc_t alloc = { .ctx = &memory, .alloc = count_alloc,
        .realloc = count_realloc, .free = count_free };
    const char *alpns[] = { "moqt-18" };
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = perspective;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.on_lane_pump = unused_pump;
    cfg.insecure_skip_verify = true;
    cfg.lane_count = lanes;
    cfg.alpn_list = alpns;
    cfg.alpn_count = 1;
#if defined(MOQ_TEST_CERT_PATH) && defined(MOQ_TEST_KEY_PATH)
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
#else
    REQUIRE(perspective != MOQ_PERSPECTIVE_SERVER);
#endif
    if (legacy) {
        cfg.struct_size = (uint32_t)offsetof(moq_pq_threaded_cfg_t, lane_count);
        cfg.lane_count = MOQ_PQ_THREADED_MAX_LANES + 1;
    }
    moq_pq_threaded_t *t = NULL;
    CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_OK);
    CHECK(t != NULL);
    if (t) {
        CHECK(!t->group && !t->group_ctx && !t->own_loop);
        CHECK(moq_pq_threaded_lane_count(t) == 1);
        moq_pq_threaded_lane_t *lane = moq_pq_threaded_lane(t, 0);
        CHECK(lane != NULL && moq_pq_threaded_lane(t, 1) == NULL);
        if (lane) {
            moq_picoquic_threaded_lane_stats_t stats, before;
            memset(&stats, 0xa5, sizeof(stats));
            memcpy(&before, &stats, sizeof(stats));
            CHECK(moq_picoquic_threaded_lane_get_stats(NULL, &stats, sizeof(stats)) == MOQ_ERR_INVAL);
            CHECK(moq_picoquic_threaded_lane_get_stats(lane, NULL, sizeof(stats)) == MOQ_ERR_INVAL);
            CHECK(moq_picoquic_threaded_lane_get_stats(lane, &stats,
                  MOQ_PICOQUIC_THREADED_LANE_STATS_V0_SIZE - 1) == MOQ_ERR_INVAL);
            CHECK(moq_picoquic_threaded_lane_get_stats(lane, &stats, sizeof(stats)) == MOQ_ERR_UNSUPPORTED);
            CHECK(memcmp(&stats, &before, sizeof(stats)) == 0);
        }
        /* Model the worker's real scheduling transition before select. A
         * second public wake must coalesce, preserving main's wake policy. */
        int64_t delta = 1000;
        REQUIRE(pthread_mutex_lock(&t->mutex) == 0);
        pq_wake_sched_on_time_check(&t->sched, &delta);
        REQUIRE(pthread_mutex_unlock(&t->mutex) == 0);
        CHECK(delta == 1000 && t->sched.in_select);
        CHECK(moq_pq_threaded_wake(t) == MOQ_OK);
        CHECK(moq_pq_threaded_wake(t) == MOQ_OK);
        CHECK(net_model.wakes == 1 && t->wake_in_flight == 0);
        CHECK(moq_pq_threaded_stop(t) == MOQ_OK);
        CHECK(net_model.wakes == 2 && net_model.deletes == 1);
        CHECK(moq_pq_threaded_wake(t) == MOQ_ERR_CLOSED);
        moq_pq_threaded_destroy(t);
    }
    CHECK(net_model.startups == 1 && net_model.cleanups == 1 && !net_model.references);
    CHECK(!net_model.thread_live && memory.blocks == 0 && memory.bytes == 0);
}

static void test_windows_groups(void)
{
    for (unsigned i = 0; i < 15; i++) {
        model_row = "windows-group-refusal";
        memset(&net_model, 0, sizeof(net_model));
        clock_reset((struct timespec){100, 0}, NULL, 0);
        alloc_model_t memory = {0};
        moq_alloc_t alloc = { .ctx = &memory, .alloc = count_alloc,
            .realloc = count_realloc, .free = count_free };
        struct sockaddr bad_address;
        memset(&bad_address, 0, sizeof(bad_address));
        bad_address.sa_family = AF_UNSPEC;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.lane_count = 2;
        cfg.port = 443;
        cfg.cert_path = "unused-certificate";
        cfg.key_path = "unused-key";
        cfg.on_lane_pump = unused_pump;
        moq_result_t expected = MOQ_ERR_INVAL;
        switch (i) {
        case 0: expected = MOQ_ERR_UNSUPPORTED; break;
        case 1: cfg.lane_count = MOQ_PQ_THREADED_MAX_LANES; expected = MOQ_ERR_UNSUPPORTED; break;
        case 2: cfg.lane_count = MOQ_PQ_THREADED_MAX_LANES + 1; break;
        case 3: cfg.port = 0; break;
        case 4: cfg.alloc = NULL; break;
        case 5: alloc.realloc = NULL; break;
        case 6: cfg.on_lane_pump = NULL; break;
        case 7: cfg.cert_path = NULL; break;
        case 8: cfg.key_path = ""; break;
        case 9: cfg.socket_buffer_bytes = UINT32_MAX; break;
        case 10: cfg.bind_address = &bad_address; break;
        case 11: cfg.perspective = MOQ_PERSPECTIVE_CLIENT; expected = MOQ_ERR_UNSUPPORTED; break;
        case 12: cfg.perspective = 0; break;
        case 13: cfg.struct_size = sizeof(cfg.struct_size); break;
        case 14: break;          /* NULL cfg */
        }
        moq_pq_threaded_t sentinel;
        moq_pq_threaded_t *out = &sentinel;
        CHECK(moq_pq_threaded_create(i == 14 ? NULL : &cfg, &out) == expected);
        CHECK(out == NULL);
        CHECK(moq_pq_threaded_create(&cfg, NULL) == MOQ_ERR_INVAL);
        CHECK(memory.calls == 0 && memory.blocks == 0 && memory.bytes == 0);
        CHECK(net_model.startups == 0 && net_model.cleanups == 0 &&
              net_model.starts == 0 && net_model.wakes == 0 && net_model.deletes == 0);
        CHECK(wait_model.calls == 0 && wait_model.clock_calls == 0);
    }
    CHECK(moq_pq_threaded_wake(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_pq_threaded_stop(NULL) == MOQ_ERR_INVAL);
    for (unsigned server = 0; server < 2; server++) {
        moq_perspective_t perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
        single_lane_control(perspective, 0, false);
        single_lane_control(perspective, 1, false);
        single_lane_control(perspective, 0, true);
    }
}
#endif

int main(int argc, char **argv)
{
    model_row = "arguments";
    if (argc != 2) {
        fprintf(stderr, "usage: %s <model-row>\n", argv[0]);
        return 2;
    }
    model_row = argv[1];
    if (!strcmp(argv[1], "wait")) test_waits();
    else if (!strcmp(argv[1], "public-clock")) test_public_failure(INITIAL_CLOCK);
    else if (!strcmp(argv[1], "public-shim")) test_public_failure(SHIM_CLOCK);
    else if (!strcmp(argv[1], "public-native")) test_public_failure(NATIVE_WAIT);
    else if (!strcmp(argv[1], "public-infinite")) test_public_failure(INFINITE_WAIT);
    else if (!strcmp(argv[1], "public-controls")) test_public_controls();
    else if (!strcmp(argv[1], "stop-clock")) test_stop_failure(INITIAL_CLOCK);
    else if (!strcmp(argv[1], "stop-shim")) test_stop_failure(SHIM_CLOCK);
    else if (!strcmp(argv[1], "stop-native")) test_stop_failure(NATIVE_WAIT);
    else if (!strcmp(argv[1], "stop-controls")) test_stop_controls();
#ifndef MOQ_TEST_WT_MANAGED_MODEL
    else if (!strcmp(argv[1], "windows-groups")) test_windows_groups();
    else if (!strcmp(argv[1], "startup")) test_startup();
    else if (!strcmp(argv[1], "startup-clock"))
        create_row(STARTUP_CLOCK, argv[1], MOQ_ERR_INTERNAL, 1, 1);
    else if (!strcmp(argv[1], "startup-shim"))
        create_row(STARTUP_SHIM, argv[1], MOQ_ERR_INTERNAL, 1, 1);
    else if (!strcmp(argv[1], "startup-native"))
        create_row(STARTUP_NATIVE, argv[1], MOQ_ERR_INTERNAL, 1, 1);
    else if (!strcmp(argv[1], "startup-ready-clock"))
        create_row(READY_CLOCK, argv[1], MOQ_OK, 1, 1);
#endif
    else {
        fprintf(stderr, "unknown model row: %s\n", argv[1]);
        return 2;
    }
    printf("windows model %s: %u checks, %u failures (host fakes, not Windows runtime)\n",
           argv[1], model_checks, model_failures);
    return model_failures ? 1 : 0;
}
