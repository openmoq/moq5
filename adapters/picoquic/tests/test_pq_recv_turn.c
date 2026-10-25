/*
 * The lane's per-turn receive budget is shared fairly across its ready
 * sockets, so one busy socket cannot starve another. One real receive turn of
 * the test-internals build (lane_recv_turn over lane_receive), in two rows:
 *
 *   fair    over two scripted sockets, each holding 20 datagrams, more than
 *           its fair share of 16: the turn reads exactly 16 from each and
 *           leaves 4 in each. The script is the input witness. A UDP socket
 *           cannot be asked how many datagrams it holds without reading
 *           them, a successful send does not mean its datagram has arrived,
 *           and readiness means one datagram is queued, not twenty, so the
 *           exact share is asserted here and not over real sockets.
 *   native  over two real loopback UDP sockets, once poll has reported both
 *           readable after 20 datagrams were sent to each: the turn reads at
 *           least one and at most its share from each, and its reads plus a
 *           drain afterwards never exceed what was sent. Every setup call is
 *           checked and the row stops at the first that fails. The drain
 *           tells an empty socket (EAGAIN or EWOULDBLOCK) from an interrupted
 *           read (EINTR, retried) and from any other error (a failure).
 *
 * The datagrams are oversized, so each is read and counted toward the budget
 * but discarded before routing, which is why no connection context is built.
 * The share arithmetic itself is covered in test_pq_recv_budget.c.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* MOQ_PQ_THREADED_TESTING seams (deliberately not in the public header). */
extern int moq_pq_threaded_test_recv_turn(const int *fds, int nsock, int *reads);
extern int moq_pq_threaded_test_recv_turn_scripted(int *queued, int nsock,
                                                   int *reads);

static int failures;
#define CHECK(c) do { if (!(c)) { \
    printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #c); failures++; } \
} while (0)

#define OVERSIZE 1600   /* > PICOQUIC_MAX_PACKET_SIZE (1536): the oversize path */
#define BUDGET   32     /* PQ_GROUP_RECV_BUDGET */
#define SHARE    16     /* pq_recv_share(2, 32): the fair per-socket share */
#define QUEUED   20     /* > SHARE, so the budget, not the input, is the limit */
#define READY_MS 5000   /* bound on waiting for sent datagrams to be readable */
#define RCVBUF   (2 * QUEUED * OVERSIZE)   /* requested: twice one socket's payload */

/* -- fair: scripted sockets ------------------------------------------------- */

static void row_fair(void)
{
    int queued[2] = { QUEUED, QUEUED };
    int reads[2] = { -1, -1 };
    int received = moq_pq_threaded_test_recv_turn_scripted(queued, 2, reads);
    printf("fair: received=%d reads=%d/%d left=%d/%d (share=%d budget=%d)\n",
           received, reads[0], reads[1], queued[0], queued[1], SHARE, BUDGET);
    CHECK(received == BUDGET);                       /* the whole budget spent */
    CHECK(reads[0] == SHARE && reads[1] == SHARE);   /* exactly its share each */
    CHECK(queued[0] == QUEUED - SHARE &&             /* with datagrams left in */
          queued[1] == QUEUED - SHARE);              /* each: the share stopped it */
}

/* -- native: loopback UDP sockets ------------------------------------------- */

static void setup_failed(const char *step, int err)
{
    printf("FAIL: native setup: %s: %s\n", step, err ? strerror(err) : "");
}

/* A nonblocking loopback UDP socket with a modest receive buffer, or -1 after
 * the failed step is reported. RCVBUF leaves headroom for the payload sent to
 * the socket, and the size in effect is read back and reported. No buffer
 * size shows that the kernel will keep every datagram, its own accounting
 * included, and the row does not need that: readiness shows a datagram is
 * queued, and the exact witness is the fair row's. */
static int open_receiver(struct sockaddr_in *addr)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        setup_failed("socket", errno);
        return -1;
    }
    int want = RCVBUF, have = 0;
    socklen_t have_len = sizeof(have);
    socklen_t addr_len = sizeof(*addr);
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const int flags = fcntl(fd, F_GETFL, 0);
    const char *step = NULL;
    int err = 0;
    if (flags < 0)
        step = "fcntl(F_GETFL)";
    else if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
        step = "fcntl(F_SETFL, O_NONBLOCK)";
    else if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &want, sizeof(want)) != 0)
        step = "setsockopt(SO_RCVBUF)";
    else if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &have, &have_len) != 0)
        step = "getsockopt(SO_RCVBUF)";
    else if (bind(fd, (struct sockaddr *)addr, sizeof(*addr)) != 0)
        step = "bind";
    else if (getsockname(fd, (struct sockaddr *)addr, &addr_len) != 0)
        step = "getsockname";
    if (step) {
        err = errno;
        setup_failed(step, err);
        close(fd);
        return -1;
    }
    printf("native setup: SO_RCVBUF %d bytes requested, %d in effect\n", want,
           have);
    return fd;
}

/* Send `n` oversized datagrams to `to`, retrying an interrupted send. */
static bool send_all(int snd, const struct sockaddr_in *to, int n)
{
    static uint8_t big[OVERSIZE];
    memset(big, 0x5A, sizeof(big));
    for (int k = 0; k < n; k++) {
        ssize_t r;
        do {
            r = sendto(snd, big, sizeof(big), 0, (const struct sockaddr *)to,
                       sizeof(*to));
        } while (r < 0 && errno == EINTR);
        if (r != (ssize_t)sizeof(big)) {
            setup_failed("sendto", r < 0 ? errno : 0);
            return false;
        }
    }
    return true;
}

static long long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Wait, at most READY_MS, until poll has reported each socket readable. */
static bool wait_readable(const int *fds, int n)
{
    bool seen[2] = { false, false };
    const long long deadline = now_ms() + READY_MS;
    for (;;) {
        struct pollfd pfd[2];
        int idx[2];
        int ask = 0;
        for (int i = 0; i < n; i++) {
            if (seen[i]) continue;
            pfd[ask].fd = fds[i];
            pfd[ask].events = POLLIN;
            pfd[ask].revents = 0;
            idx[ask++] = i;
        }
        if (ask == 0) return true;
        const long long left = deadline - now_ms();
        if (left <= 0) {
            printf("FAIL: native setup: not every socket readable within %d ms\n",
                   READY_MS);
            return false;
        }
        int r = poll(pfd, (nfds_t)ask, (int)left);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) {
            setup_failed("poll", errno);
            return false;
        }
        for (int j = 0; j < ask; j++) {
            if (pfd[j].revents & (POLLERR | POLLNVAL)) {
                printf("FAIL: native setup: poll reported an error on socket %d\n",
                       idx[j]);
                return false;
            }
            if (pfd[j].revents & POLLIN) seen[idx[j]] = true;
        }
    }
}

/* Read the socket until it is empty. Returns the datagrams read, or -1 after
 * reporting an error that is neither an empty socket nor an interrupted
 * read. */
static int drain(int fd)
{
    uint8_t buf[OVERSIZE];
    int n = 0;
    for (;;) {
        ssize_t r = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (r >= 0) {
            n++;
            continue;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return n;
        printf("FAIL: native drain: recv: %s\n", strerror(errno));
        return -1;
    }
}

static void row_native(void)
{
    struct sockaddr_in addr[2];
    int fds[2] = { -1, -1 };
    int snd = -1;
    fds[0] = open_receiver(&addr[0]);
    if (fds[0] >= 0) fds[1] = open_receiver(&addr[1]);
    if (fds[1] >= 0) {
        snd = socket(AF_INET, SOCK_DGRAM, 0);
        if (snd < 0) setup_failed("socket (sender)", errno);
    }
    const bool ready = snd >= 0 && send_all(snd, &addr[0], QUEUED) &&
                       send_all(snd, &addr[1], QUEUED) && wait_readable(fds, 2);
    CHECK(ready);
    if (ready) {
        int reads[2] = { -1, -1 };
        int received = moq_pq_threaded_test_recv_turn(fds, 2, reads);
        int drained[2] = { drain(fds[0]), drain(fds[1]) };
        printf("native: sent=%d/%d received=%d reads=%d/%d drained=%d/%d "
               "(share=%d budget=%d)\n", QUEUED, QUEUED, received, reads[0],
               reads[1], drained[0], drained[1], SHARE, BUDGET);
        CHECK(received == reads[0] + reads[1]);
        CHECK(received <= BUDGET);
        for (int i = 0; i < 2; i++) {
            CHECK(reads[i] >= 1);       /* readable: one at least was queued */
            CHECK(reads[i] <= SHARE);   /* never more than its share */
            CHECK(drained[i] >= 0);     /* the drain met no error */
            CHECK(drained[i] < 0 ||     /* nothing read twice or invented */
                  reads[i] + drained[i] <= QUEUED);
        }
    }
    for (int i = 0; i < 2; i++)
        if (fds[i] >= 0) close(fds[i]);
    if (snd >= 0) close(snd);
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IONBF, 0);
    const char *row = argc > 1 ? argv[1] : "all";
    const bool all = strcmp(row, "all") == 0;
    const bool fair = all || strcmp(row, "fair") == 0;
    const bool native = all || strcmp(row, "native") == 0;
    if (!fair && !native) {
        printf("FAIL: no row %s (fair, native)\n", row);
        return 2;
    }
    if (fair) row_fair();
    if (native) row_native();
    if (failures) {
        printf("FAILED: %d check(s) [pq_recv_turn %s]\n", failures, row);
        return 1;
    }
    printf("PASS: pq_recv_turn %s\n", row);
    return 0;
}
