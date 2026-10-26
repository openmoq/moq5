/*
 * The public lane-stats getter folds a lane's detect-ring full and closed drops
 * into detection_drops: it sums the two, and at the counters' limit it
 * saturates rather than wrapping. A real two-lane group server is created and
 * one lane's counters are preset through the test-internals seam; the record
 * is then read through the public getter, so the projection itself -- not only
 * the saturating helper it uses -- is what is checked.
 */
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

extern int moq_pq_threaded_test_set_detect_drops(moq_pq_threaded_lane_t *lane,
                                                 uint64_t full, uint64_t closed);

static int failures;
#define CHECK(c) do { if (!(c)) { \
    printf("CHECK failed: %s (%s:%d)\n", #c, __FILE__, __LINE__); failures++; } \
} while (0)

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

static int noop_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now, void *vctx)
{
    (void)t; (void)lane; (void)now; (void)vctx;
    return 0;
}

static uint64_t detection_drops(moq_pq_threaded_lane_t *lane)
{
    moq_picoquic_threaded_lane_stats_t st;
    memset(&st, 0, sizeof(st));
    CHECK(moq_picoquic_threaded_lane_get_stats(lane, &st, sizeof(st)) == MOQ_OK);
    return st.detection_drops;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
    cfg.port = pick_free_udp_port();
    cfg.lane_count = 2;
    cfg.max_connections = 8;
    cfg.on_lane_pump = noop_pump;
    moq_pq_threaded_t *srv = NULL;
    CHECK(cfg.port > 0);
    CHECK(moq_pq_threaded_create(&cfg, &srv) == MOQ_OK);
    if (!srv) { printf("FAILED: no server [pq_lane_stats]\n"); return 1; }

    moq_pq_threaded_lane_t *lane = moq_pq_threaded_lane(srv, 1);
    CHECK(lane != NULL);

    /* An ordinary pair is the sum of both counters. */
    CHECK(moq_pq_threaded_test_set_detect_drops(lane, 10, 20) == 0);
    uint64_t sum = detection_drops(lane);
    /* At the limit the fold saturates instead of wrapping to a small value. */
    CHECK(moq_pq_threaded_test_set_detect_drops(lane, UINT64_MAX - 1, 5) == 0);
    uint64_t sat = detection_drops(lane);
    CHECK(moq_pq_threaded_test_set_detect_drops(lane, UINT64_MAX, UINT64_MAX) == 0);
    uint64_t both = detection_drops(lane);
    printf("lane_stats: sum=%llu near_limit=%llu both_max=%llu\n",
           (unsigned long long)sum, (unsigned long long)sat,
           (unsigned long long)both);
    CHECK(sum == 30);
    CHECK(sat == UINT64_MAX);
    CHECK(both == UINT64_MAX);

    (void)moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);

    if (failures) {
        printf("FAILED: %d check(s) [pq_lane_stats]\n", failures);
        return 1;
    }
    printf("PASS: pq_lane_stats\n");
    return 0;
}
