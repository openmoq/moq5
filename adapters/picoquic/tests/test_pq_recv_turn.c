/*
 * The lane's per-turn receive budget is shared fairly across its ready sockets,
 * so one busy socket cannot starve another. This drives a single real receive
 * turn -- through the test-internals seam, over the actual lane_receive path --
 * across two loopback sockets each queued with more datagrams than its fair
 * share, then confirms each was read down by exactly that share rather than one
 * being drained while the other waited. The datagrams are oversized, so each is
 * read and counted toward the budget but discarded before routing; only the
 * read distribution is under test, which is why no connection context is built.
 * The share arithmetic itself is covered separately in test_pq_recv_budget.c.
 */
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

extern int moq_pq_threaded_test_recv_turn(const int *fds, int nsock);

static int failures;
#define CHECK(c) do { if (!(c)) { \
    printf("CHECK failed: %s (%s:%d)\n", #c, __FILE__, __LINE__); failures++; } \
} while (0)

#define OVERSIZE 1600   /* > PICOQUIC_MAX_PACKET_SIZE (1536): the oversize path */
#define BUDGET   32     /* PQ_GROUP_RECV_BUDGET */
#define SHARE    16     /* pq_recv_share(2, 32): the fair per-socket share */
#define QUEUED   20     /* > SHARE, so the budget, not the queue, is the limit */

static int bound_loopback_udp(struct sockaddr_in *addr)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    /* Non-blocking, as the lane's own sockets are: a drained socket returns
     * EAGAIN and the receive turn moves on, rather than blocking. */
    (void)fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    int rcvbuf = 4 * 1024 * 1024;   /* hold every queued datagram, undropped */
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(*addr);
    if (bind(fd, (struct sockaddr *)addr, sizeof(*addr)) != 0 ||
        getsockname(fd, (struct sockaddr *)addr, &len) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int drain_count(int fd)
{
    uint8_t buf[OVERSIZE];
    int n = 0;
    while (recv(fd, buf, sizeof(buf), MSG_DONTWAIT) >= 0)
        n++;
    return n;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    int fds[2];
    struct sockaddr_in addr[2];
    fds[0] = bound_loopback_udp(&addr[0]);
    fds[1] = bound_loopback_udp(&addr[1]);
    CHECK(fds[0] >= 0 && fds[1] >= 0);

    int snd = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(snd >= 0);
    static uint8_t big[OVERSIZE];
    memset(big, 0x5A, sizeof(big));
    /* Loopback delivery is synchronous: every datagram is queued on its socket
     * by the time its sendto returns, so the receive turn below sees them all. */
    for (int s = 0; s < 2; s++)
        for (int k = 0; k < QUEUED; k++)
            CHECK(sendto(snd, big, sizeof(big), 0, (struct sockaddr *)&addr[s],
                         sizeof(addr[s])) == (ssize_t)sizeof(big));

    int received = moq_pq_threaded_test_recv_turn(fds, 2);

    int read0 = QUEUED - drain_count(fds[0]);
    int read1 = QUEUED - drain_count(fds[1]);
    printf("recv_turn: received=%d read0=%d read1=%d (share=%d budget=%d)\n",
           received, read0, read1, SHARE, BUDGET);

    CHECK(received == BUDGET);   /* the whole budget was spent, */
    CHECK(read0 == SHARE);       /* each socket read exactly its fair share, */
    CHECK(read1 == SHARE);       /* so neither starved the other */

    close(fds[0]);
    close(fds[1]);
    close(snd);

    if (failures) {
        printf("FAILED: %d check(s) [pq_recv_turn]\n", failures);
        return 1;
    }
    printf("PASS: pq_recv_turn\n");
    return 0;
}
