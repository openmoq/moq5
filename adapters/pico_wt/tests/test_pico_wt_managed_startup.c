/*
 * Managed startup through the actual create/stop/destroy implementation.
 * The provider owns a real pthread here, but opens no socket. Its typed
 * start/wake/delete replacements deliver the real packet-loop callbacks.
 * Condition waits and the clock are scripted; elapsed time is not an oracle.
 *
 * The race row deliberately leaves the provider's plain return_code write
 * unpublished to the creator: its scheduling flag uses relaxed atomics.
 * A thread-sanitized build can therefore check the production access. Other
 * rows publish their scheduling gates, separating lifecycle assertions from
 * that race check. This fixture does not qualify native Windows execution.
 */
#include <moq/pico_wt_managed.h>
#include <picoquic.h>
#include <picoquic_packet_loop.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "../../common/moq_posix_compat.h"

static picoquic_network_thread_ctx_t *model_start(picoquic_quic_t *,
    picoquic_packet_loop_param_t *, picoquic_packet_loop_cb_fn, void *, int *);
static int model_wake(picoquic_network_thread_ctx_t *);
static void model_delete(picoquic_network_thread_ctx_t *);
static void model_quic_free(picoquic_quic_t *);
static int model_unlock(pthread_mutex_t *);
static int model_wait(pthread_cond_t *, pthread_mutex_t *, const struct timespec *);
static int model_clock(clockid_t, struct timespec *);
static int model_usleep(useconds_t);

#define picoquic_start_network_thread model_start
#define picoquic_wake_up_network_thread model_wake
#define picoquic_delete_network_thread model_delete
#define picoquic_free model_quic_free
#define pthread_mutex_unlock model_unlock
#define pthread_cond_timedwait model_wait
#define clock_gettime model_clock
#define usleep model_usleep
#include "../pico_wt_managed.c"
#undef picoquic_start_network_thread
#undef picoquic_wake_up_network_thread
#undef picoquic_delete_network_thread
#undef picoquic_free
#undef pthread_mutex_unlock
#undef pthread_cond_timedwait
#undef clock_gettime
#undef usleep

static unsigned checks, failures;
static const char *row;
#define CHECK(e) do { checks++; if (!(e)) { \
    fprintf(stderr, "FAIL [%s] %s:%d: %s\n", row, __FILE__, __LINE__, #e); \
    failures++; } } while (0)
#define REQUIRE(e) do { if (!(e)) { \
    fprintf(stderr, "SETUP/MODEL [%s] %s:%d: %s\n", row, __FILE__, __LINE__, #e); \
    exit(2); } } while (0)

typedef enum {
    READY, READY_EXIT, DURING_WAIT, FAILED, LATE_READY, RACE
} startup_mode_t;
typedef struct candidate {
    picoquic_network_thread_ctx_t ctx;
    pthread_t thread;
    startup_mode_t mode;
    unsigned id;
    atomic_int proceed, finished, stop;
    int ready_rc, port_rc;
} candidate_t;
typedef struct allocation { void *ptr; size_t size; } allocation_t;
static struct {
    pthread_t creator;
    candidate_t *active;
    moq_pico_wt_managed_t *facade;
    startup_mode_t modes[3];
    allocation_t allocations[16];
    unsigned starts, joins, wakes, quic_frees, facade_frees;
    unsigned waits, sleeps, clock_calls, tick;
    unsigned started[3], joined[3], quic_freed, facade_freed;
    size_t blocks, bytes;
    bool clock_failure, wait_error;
} model;

/* The process/CTest deadline also bounds a failed scheduling prerequisite.
 * No delay or elapsed-time threshold determines a product result. */
static void await_value(atomic_int *flag, int value, memory_order order)
{
    for (unsigned i = 0; i < 1000000; i++) {
        if (atomic_load_explicit(flag, order) >= value) return;
        sched_yield();
    }
    REQUIRE(false);
}

static void *provider_thread(void *arg)
{
    candidate_t *c = arg;
    moq_pico_wt_managed_t *m = c->ctx.loop_callback_ctx;
    if (c->mode == DURING_WAIT || c->mode == LATE_READY)
        await_value(&c->proceed, 1, memory_order_acquire);
    if (c->mode != FAILED && c->mode != RACE) {
        picoquic_packet_loop_options_t options;
        memset(&options, 0, sizeof(options));
        c->ready_rc = c->ctx.loop_callback(c->ctx.quic,
            picoquic_packet_loop_ready, m, &options);
        /* sockloop calls port_update even when READY returned an error. */
        struct sockaddr_storage addr = {0};
        c->port_rc = c->ctx.loop_callback(c->ctx.quic,
            picoquic_packet_loop_port_update, m, &addr);
        if (c->port_rc == 0) c->ctx.thread_is_ready = 1;
        if (c->mode == READY) {
            atomic_store_explicit(&c->finished, 1, memory_order_release);
            await_value(&c->stop, 1, memory_order_acquire);
        }
        if (c->ready_rc == 0)
            REQUIRE(loop_terminate(m) == PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP);
    }
    c->ctx.thread_is_ready = 0;
    c->ctx.return_code = c->mode == READY ? 0 : EIO;
    atomic_store_explicit(&c->finished, 2,
        c->mode == RACE ? memory_order_relaxed : memory_order_release);
    return NULL;
}

static void deliver_ready(candidate_t *c)
{
    atomic_store_explicit(&c->proceed, 1, memory_order_release);
    await_value(&c->finished, 2, memory_order_acquire);
}

static picoquic_network_thread_ctx_t *model_start(picoquic_quic_t *quic,
    picoquic_packet_loop_param_t *param, picoquic_packet_loop_cb_fn callback,
    void *ctx, int *ret)
{
    REQUIRE(model.active == NULL && model.starts < 3);
    REQUIRE(quic && param && callback && ctx && ret);
    REQUIRE(model.joins == model.starts && model.quic_frees == 0);
    model.facade = ctx;
    REQUIRE(model.facade->quic == quic && !model.facade->loop_came_up);
    candidate_t *c = calloc(1, sizeof(*c));
    REQUIRE(c);
    c->id = model.starts++;
    c->mode = model.modes[c->id];
    c->ctx.quic = quic;
    c->ctx.param = param;
    c->ctx.loop_callback = callback;
    c->ctx.loop_callback_ctx = ctx;
    c->ctx.is_threaded = 1;
    atomic_init(&c->proceed, 0);
    atomic_init(&c->finished, 0);
    atomic_init(&c->stop, 0);
    model.active = c;
    model.started[c->id] = ++model.tick;
    *ret = 0;
    REQUIRE(pthread_create(&c->thread, NULL, provider_thread, c) == 0);
    if (c->mode != DURING_WAIT && c->mode != LATE_READY)
        await_value(&c->finished, c->mode == READY ? 1 : 2,
            c->mode == RACE ? memory_order_relaxed : memory_order_acquire);
    return &c->ctx;
}

static int model_unlock(pthread_mutex_t *mutex)
{
    int rc = pthread_mutex_unlock(mutex);
    /* Schedule READY + exit after the creator releases its first observation
     * lock. The callback uses the same real mutex as the production code. */
    if (rc == 0 && pthread_equal(pthread_self(), model.creator) &&
        model.active && model.active->mode == DURING_WAIT &&
        !atomic_load_explicit(&model.active->finished, memory_order_acquire))
        deliver_ready(model.active);
    return rc;
}

static int model_clock(clockid_t clock, struct timespec *out)
{
    REQUIRE(clock == CLOCK_REALTIME && out);
    model.clock_calls++;
    if (model.clock_failure) return -1; /* failure leaves output untouched */
    *out = (struct timespec){ .tv_sec = 100, .tv_nsec = 123456789 };
    return 0;
}

static int model_wait(pthread_cond_t *cond, pthread_mutex_t *mutex,
                      const struct timespec *deadline)
{
    REQUIRE(model.active && cond == &model.facade->cond);
    REQUIRE(mutex == &model.facade->mutex && deadline);
    REQUIRE(++model.waits <= 3); /* unexpected retries fail boundedly */
    CHECK(deadline->tv_sec == 102 && deadline->tv_nsec == 123456789);
    REQUIRE(pthread_mutex_unlock(mutex) == 0);
    /* The first wake has no readiness publication. The same absolute
     * deadline must cover the next wait as well. */
    if (model.active->mode == DURING_WAIT && model.waits > 1)
        deliver_ready(model.active);
    REQUIRE(pthread_mutex_lock(mutex) == 0);
    return model.active->mode == DURING_WAIT ? 0 :
        model.wait_error ? EINVAL : ETIMEDOUT;
}

static int model_usleep(useconds_t us)
{
    REQUIRE(us == 1000 && ++model.sleeps <= 6000);
    return 0;
}

static int model_wake(picoquic_network_thread_ctx_t *ctx)
{
    REQUIRE(model.active && ctx == &model.active->ctx);
    model.wakes++;
    atomic_store_explicit(&model.active->stop, 1, memory_order_release);
    await_value(&model.active->finished, 2, memory_order_acquire);
    return 0;
}

static void model_delete(picoquic_network_thread_ctx_t *ctx)
{
    candidate_t *c = model.active;
    REQUIRE(c && ctx == &c->ctx && model.facade && !model.quic_frees);
    REQUIRE(pthread_mutex_trylock(&model.facade->mutex) == 0);
    REQUIRE(pthread_mutex_unlock(&model.facade->mutex) == 0);
    if (c->mode == LATE_READY) deliver_ready(c);
    atomic_store_explicit(&c->stop, 1, memory_order_release);
    REQUIRE(pthread_join(c->thread, NULL) == 0);
    model.joined[c->id] = ++model.tick;
    model.joins++;
    if (c->mode == LATE_READY) {
        CHECK(c->ready_rc == PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP);
        CHECK(c->port_rc == PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP);
        CHECK(!model.facade->loop_came_up && !model.facade->loop_running);
        CHECK(model.facade->session == NULL && model.facade->conn == NULL);
    }
    model.active = NULL;
    free(c);
}

static void model_quic_free(picoquic_quic_t *quic)
{
    REQUIRE(!model.active && model.joins == model.starts);
    model.quic_frees++;
    model.quic_freed = ++model.tick;
    picoquic_free(quic);
}

static size_t allocation_slot(void *ptr)
{
    for (size_t i = 0; i < 16; i++)
        if (model.allocations[i].ptr == ptr) return i;
    REQUIRE(false);
    return 0;
}
static void *count_alloc(size_t size, void *ctx)
{
    (void)ctx;
    size_t i = allocation_slot(NULL);
    void *ptr = malloc(size);
    REQUIRE(ptr);
    model.allocations[i] = (allocation_t){ .ptr = ptr, .size = size };
    model.blocks++;
    model.bytes += size;
    return ptr;
}
static void count_free(void *ptr, size_t size, void *ctx)
{
    (void)ctx;
    if (!ptr) return;
    size_t i = allocation_slot(ptr);
    REQUIRE(model.allocations[i].size == size);
    if (ptr == model.facade) {
        REQUIRE(!model.active && model.quic_frees == 1);
        model.facade_frees++;
        model.facade_freed = ++model.tick;
        model.facade = NULL;
    }
    model.allocations[i] = (allocation_t){0};
    model.blocks--;
    model.bytes -= size;
    free(ptr);
}
static void *count_realloc(void *ptr, size_t old_size, size_t size, void *ctx)
{
    void *next = count_alloc(size, ctx);
    if (ptr) memcpy(next, ptr, old_size < size ? old_size : size);
    count_free(ptr, old_size, ctx);
    return next;
}
static int on_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    REQUIRE(false); /* no session or network input exists in this fixture */
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 4) return 2;
    row = argv[1];
    model.creator = pthread_self();
    unsigned attempts = 1;
    bool success = true;
    startup_mode_t mode;
    if (strcmp(row, "ready") == 0) mode = READY;
    else if (strcmp(row, "ready-exit") == 0) mode = READY_EXIT;
    else if (strcmp(row, "ready-clock-failure") == 0) {
        mode = READY_EXIT;
        model.clock_failure = true;
    }
    else if (strcmp(row, "ready-during-wait") == 0) mode = DURING_WAIT;
    else if (strcmp(row, "retry") == 0) { mode = LATE_READY; attempts = 2; }
    else if (strcmp(row, "failure") == 0 || strcmp(row, "race") == 0 ||
             strcmp(row, "late-ready") == 0 || strcmp(row, "clock-failure") == 0 ||
             strcmp(row, "wait-error") == 0) {
        mode = strcmp(row, "late-ready") == 0 ? LATE_READY :
               strcmp(row, "race") == 0 ? RACE : FAILED;
        attempts = 3;
        success = false;
        model.clock_failure = strcmp(row, "clock-failure") == 0;
        model.wait_error = strcmp(row, "wait-error") == 0;
    } else return 2;
    for (unsigned i = 0; i < 3; i++) model.modes[i] = mode;
    if (strcmp(row, "retry") == 0) model.modes[1] = READY_EXIT;
    (void)model_usleep; /* poll-delay seam: bounded calls, no elapsed sleep */

    moq_alloc_t alloc = { .alloc = count_alloc, .realloc = count_realloc,
                         .free = count_free, .ctx = NULL };
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 4433; /* supplied to the fake provider; never bound */
    cfg.cert_path = argv[2];
    cfg.key_path = argv[3];
    cfg.on_pump = on_pump;
    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    REQUIRE(model.starts > 0); /* cert/QUIC/setup errors are not startup evidence */
    CHECK(rc == (success ? MOQ_OK : MOQ_ERR_INTERNAL));
    CHECK((m != NULL) == success);
    CHECK(model.starts == attempts);
    if (m) {
        CHECK(model.joins + 1 == model.starts && model.active != NULL);
        CHECK(m->loop_came_up && m->network_thread_id_set);
        CHECK(m->loop_running == (mode == READY));
        CHECK(m->session == NULL && m->conn == NULL);
        REQUIRE(moq_pico_wt_managed_stop(m) == MOQ_OK);
        CHECK(model.joins == model.starts && model.wakes == 1);
        CHECK(moq_pico_wt_managed_stop(m) == MOQ_OK);
        CHECK(model.joins == model.starts && model.wakes == 1);
        moq_pico_wt_managed_destroy(m);
    }
    CHECK(!model.active && model.joins == model.starts);
    for (unsigned i = 0; i < model.starts; i++) {
        CHECK(model.started[i] < model.joined[i]);
        if (i + 1 < model.starts) CHECK(model.joined[i] < model.started[i + 1]);
    }
    CHECK(model.quic_frees == 1 && model.facade_frees == 1);
    CHECK(model.joined[model.starts - 1] < model.quic_freed);
    CHECK(model.quic_freed < model.facade_freed);
    CHECK(model.blocks == 0 && model.bytes == 0);
    fprintf(stderr, "%s: %u checks, %u failures; starts=%u joins=%u\n",
            row, checks, failures, model.starts, model.joins);
    return failures ? 1 : 0;
}
