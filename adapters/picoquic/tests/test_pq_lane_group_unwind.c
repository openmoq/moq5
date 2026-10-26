/*
 * The PicoQUIC lane group unwinds construction cleanly when a lane worker
 * cannot start after an earlier lane's worker is already running. The group
 * builds every lane's resources first and starts the threads second, so an
 * allocation failure always unwinds before any thread exists; this exercises
 * the other path -- a spawn that fails once a worker is already running -- and
 * confirms that worker is stopped and joined (no lane thread survives), every
 * socket and wake pipe is closed (no descriptor leak), and every block is
 * freed. White-box: links the threaded adapter's test-internals archive, which
 * exposes a seam to fail a chosen lane spawn and to count the lane worker
 * bodies in flight.
 */
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* The test-internals seam: fail the Nth lane spawn (1-based; 0 disables), and
 * the running count of lane worker bodies. */
extern _Atomic int moq_pq_threaded_test_fail_spawn_at;
extern _Atomic int moq_pq_threaded_test_live_threads;
extern _Atomic int moq_pq_threaded_test_spawn_fail_reached;
extern _Atomic int moq_pq_threaded_test_live_at_spawn_fail;

static int failures;
#define CHECK(c) do { if (!(c)) { \
    printf("CHECK failed: %s (%s:%d)\n", #c, __FILE__, __LINE__); failures++; } \
} while (0)

/* A counting allocator over the default: it never fails (the spawn seam is the
 * injected failure), but `live` must return to zero once the unwind has freed
 * everything it took. */
typedef struct {
    const moq_alloc_t *base;
    _Atomic long       live;
} count_alloc_t;

static void *ca_alloc(size_t size, void *ctx)
{
    count_alloc_t *c = ctx;
    void *p = c->base->alloc(size, c->base->ctx);
    if (p) atomic_fetch_add(&c->live, 1);
    return p;
}

static void *ca_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    count_alloc_t *c = ctx;
    void *p = c->base->realloc(ptr, old_size, new_size, c->base->ctx);
    if (p && !ptr) atomic_fetch_add(&c->live, 1);
    else if (!p && ptr && new_size == 0) atomic_fetch_sub(&c->live, 1);
    return p;
}

static void ca_free(void *ptr, size_t size, void *ctx)
{
    count_alloc_t *c = ctx;
    if (ptr) atomic_fetch_sub(&c->live, 1);
    c->base->free(ptr, size, c->base->ctx);
}

/* Open descriptors below a small ceiling; the group opens a handful (two
 * sockets and a wake pipe per lane) and a clean unwind returns to the start. */
static int open_fds(void)
{
    int n = 0;
    for (int fd = 0; fd < 256; fd++)
        if (fcntl(fd, F_GETFD) != -1) n++;
    return n;
}

static int pick_free_udp_port(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(a);
    int port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0)
        port = ntohs(a.sin_port);
    close(fd);
    return port;
}

/* The group does not call the pump until an inbound connection arrives, and
 * this construction fails before any client, so the body is never reached. */
static int noop_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now, void *vctx)
{
    (void)t; (void)lane; (void)now; (void)vctx;
    return 0;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    count_alloc_t ca = { .base = moq_alloc_default(), .live = 0 };
    moq_alloc_t alloc = { .ctx = &ca, .alloc = ca_alloc,
                          .realloc = ca_realloc, .free = ca_free };

    int port = pick_free_udp_port();
    CHECK(port > 0);

    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
    cfg.port = port;
    cfg.lane_count = 2;
    cfg.max_connections = 8;
    cfg.on_lane_pump = noop_pump;

    int fd_before = open_fds();

    /* Fail the second lane's spawn. The seam takes that failure only after the
     * first lane's worker has entered its body, so the unwind must stop and
     * join a running worker rather than leak it. */
    atomic_store(&moq_pq_threaded_test_live_threads, 0);
    atomic_store(&moq_pq_threaded_test_spawn_fail_reached, 0);
    atomic_store(&moq_pq_threaded_test_live_at_spawn_fail, 0);
    atomic_store(&moq_pq_threaded_test_fail_spawn_at, 2);
    moq_pq_threaded_t *srv = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &srv);
    atomic_store(&moq_pq_threaded_test_fail_spawn_at, 0);

    int fd_after = open_fds();

    CHECK(rc != MOQ_OK);
    CHECK(srv == NULL);
    /* The failure was the injected one, taken with the first worker running. */
    CHECK(atomic_load(&moq_pq_threaded_test_spawn_fail_reached) == 1);
    CHECK(atomic_load(&moq_pq_threaded_test_live_at_spawn_fail) == 1);
    CHECK(atomic_load(&moq_pq_threaded_test_live_threads) == 0);  /* joined */
    CHECK(atomic_load(&ca.live) == 0);                            /* freed */
    CHECK(fd_after == fd_before);                                 /* closed */
    printf("unwind: injected=%d live_at_fail=%d live_threads=%d "
           "live_blocks=%ld fds %d->%d\n",
           atomic_load(&moq_pq_threaded_test_spawn_fail_reached),
           atomic_load(&moq_pq_threaded_test_live_at_spawn_fail),
           atomic_load(&moq_pq_threaded_test_live_threads),
           atomic_load(&ca.live), fd_before, fd_after);

    if (failures) {
        printf("FAILED: %d check(s) [pq_lane_group_unwind]\n", failures);
        return 1;
    }
    printf("PASS: pq_lane_group_unwind\n");
    return 0;
}
