/*
 * The lane group: N internal single-lane facades on one listener port, each
 * driven by the group's loop on its own thread. See pq_lane_group.h.
 *
 * One turn of a lane's loop, in the order picoquic's packet loop calls back:
 * the time check (the facade folds its deadlines), a bounded wait on the
 * lane's sockets and wake pipe, the wake-up callback when the application
 * woke the lane, then up to PQ_GROUP_RECV_BUDGET datagrams read and routed,
 * up to PQ_GROUP_INJECT_BUDGET forwarded datagrams injected, the
 * stateless-reset candidates other lanes offered looked at with reset
 * sending disabled, the after-receive callback once for all of them, then up
 * to PQ_GROUP_SEND_BUDGET packets sent and the after-send callback, which
 * pumps. A turn that stops at a budget with work left makes the next wait
 * zero, so a refilling socket or inbox cannot starve the pump, the deadlines
 * or a stop.
 */

#include "pq_lane_group.h"

#include "pq_cid_route.h"
#include "pq_group_crypto.h"
#include "pq_inbox.h"
#include "pq_recv_budget.h"

#include <picosocks.h>

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define PQ_GROUP_RECV_BUDGET    32
#define PQ_GROUP_INJECT_BUDGET  64
#define PQ_GROUP_DETECT_BUDGET  32
#define PQ_GROUP_SEND_BUDGET    16
#define PQ_GROUP_FANOUT_BUDGET  8
#define PQ_GROUP_INBOX_SLOTS    128u
#define PQ_GROUP_DETECT_SLOTS   32u
#define PQ_GROUP_MAX_WAIT_US    10000000ll
#define PQ_GROUP_READY_WAIT_S   2
#define PQ_GROUP_EXIT_WAIT_S    3

/* The routing module's restated constants must be picoquic's. */
_Static_assert(PQ_ROUTE_RESET_MIN_LEN == PICOQUIC_RESET_PACKET_MIN_SIZE,
               "reset minimum");
_Static_assert(PQ_INBOX_PAYLOAD == PICOQUIC_MAX_PACKET_SIZE, "slot payload");

enum {
    ST_RECEIVED, ST_FWD_OUT, ST_FWD_IN, ST_OVERSIZE, ST_FANOUT_OUT,
    ST_FANOUT_IN, ST_FANOUT_SKIPPED, ST_REARMS, ST_COUNT
};

struct pq_lane_socket {
    int fd;
    int af;
    uint16_t n_port;                 /* network order */
    bool specific;                   /* bound to one address, not a wildcard */
    struct sockaddr_storage bound;
};

struct pq_lane {
    struct pq_lane_group *g;
    uint32_t index;
    moq_pq_threaded_t *facade;       /* the lane's internal facade */
    pthread_t thread;
    bool thread_started;
    int wake_r, wake_w;              /* the loop's wake pipe */
    _Atomic int signaled;            /* a producer's pipe byte is pending */
    struct pq_lane_socket sock[2];
    int nsock;
    pq_inbox_t inbox;                /* datagrams routed here by other lanes */
    pq_inbox_t detect;               /* reset candidates offered to this lane */
    pq_group_crypto_lane_t crypto;
    picoquic_packet_loop_param_t send_param;
    uint64_t deadline_served;        /* the lane deadline instant last pumped for */
    _Atomic uint64_t st[ST_COUNT];
};

struct pq_lane_group {
    moq_pq_threaded_t *facade;       /* the group facade */
    moq_alloc_t alloc;
    uint32_t count;
    struct pq_lane *lanes;
    pq_group_keys_t keys;
    size_t max_connections;
    _Atomic size_t conn_total;
    _Atomic int closing;
    uint64_t (*app_lane_deadline_us)(void *, uint32_t);
    void *app_lane_deadline_ctx;
};

/* ------------------------------------------------------------------ */
/* counters                                                            */
/* ------------------------------------------------------------------ */

static void st_add(struct pq_lane *ln, int which, uint64_t n)
{
    uint64_t cur = atomic_load_explicit(&ln->st[which], memory_order_relaxed);
    uint64_t next = cur + n < cur ? UINT64_MAX : cur + n;
    atomic_store_explicit(&ln->st[which], next, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* wake pipe                                                           */
/* ------------------------------------------------------------------ */

static int open_wake_pipe(struct pq_lane *ln)
{
    int fds[2];
    if (pipe(fds) != 0) return -1;
    for (int i = 0; i < 2; i++) {
        int fl = fcntl(fds[i], F_GETFL, 0);
        if (fl < 0 || fcntl(fds[i], F_SETFL, fl | O_NONBLOCK) < 0 ||
            fcntl(fds[i], F_SETFD, FD_CLOEXEC) < 0) {
            close(fds[0]);
            close(fds[1]);
            return -1;
        }
    }
    ln->wake_r = fds[0];
    ln->wake_w = fds[1];
    return 0;
}

/* A producer's signal: one pipe byte until the owner has drained it. */
static void lane_signal(struct pq_lane *ln)
{
    if (atomic_exchange_explicit(&ln->signaled, 1, memory_order_acq_rel) == 0) {
        uint8_t one = 1;
        (void)!write(ln->wake_w, &one, 1);   /* a full pipe holds a wake */
    }
}

static void drain_pipe(int fd)
{
    uint8_t buf[64];
    while (read(fd, buf, sizeof(buf)) > 0)
        ;
}

/* ------------------------------------------------------------------ */
/* sockets                                                             */
/* ------------------------------------------------------------------ */

static int lane_open_socket(struct pq_lane *ln, int af, int port,
                            const struct sockaddr_storage *bind_addr,
                            uint32_t buffer_bytes)
{
    struct pq_lane_socket *s = &ln->sock[ln->nsock];
    int one = 1;
    int recv_set = 0, send_set = 0;
    int fd = socket(af, SOCK_DGRAM, IPPROTO_UDP);
    if (fd < 0) return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0)
        goto fail;
#if defined(SO_REUSEPORT)
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0)
        goto fail;
#else
    goto fail;   /* the group needs a shared port */
#endif
    if (af == AF_INET6 &&
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one)) != 0)
        goto fail;
    if (picoquic_socket_set_ecn_options_ex(fd, af, &recv_set, &send_set, 0) != 0 ||
        picoquic_socket_set_pkt_info(fd, af) != 0)
        goto fail;
    if (buffer_bytes != 0) {
        int b = (int)buffer_bytes;   /* the OS may grant less */
        (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &b, sizeof(b));
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &b, sizeof(b));
    }
    memset(&s->bound, 0, sizeof(s->bound));
    socklen_t salen;
    if (bind_addr->ss_family == af) {
        s->specific = true;
        if (af == AF_INET) {
            memcpy(&s->bound, bind_addr, sizeof(struct sockaddr_in));
            ((struct sockaddr_in *)&s->bound)->sin_port = htons((uint16_t)port);
            salen = sizeof(struct sockaddr_in);
        } else {
            memcpy(&s->bound, bind_addr, sizeof(struct sockaddr_in6));
            ((struct sockaddr_in6 *)&s->bound)->sin6_port = htons((uint16_t)port);
            salen = sizeof(struct sockaddr_in6);
        }
    } else {
        s->specific = false;
        if (af == AF_INET) {
            struct sockaddr_in *a = (struct sockaddr_in *)&s->bound;
            a->sin_family = AF_INET;
            a->sin_addr.s_addr = htonl(INADDR_ANY);
            a->sin_port = htons((uint16_t)port);
            salen = sizeof(*a);
        } else {
            struct sockaddr_in6 *a = (struct sockaddr_in6 *)&s->bound;
            a->sin6_family = AF_INET6;
            a->sin6_addr = in6addr_any;
            a->sin6_port = htons((uint16_t)port);
            salen = sizeof(*a);
        }
    }
    if (bind(fd, (struct sockaddr *)&s->bound, salen) != 0)
        goto fail;
    {
        int fl = fcntl(fd, F_GETFL, 0);
        if (fl < 0 || fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0 ||
            fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
            goto fail;
    }
    s->fd = fd;
    s->af = af;
    s->n_port = htons((uint16_t)port);
    ln->nsock++;
    return 0;
fail:
    close(fd);
    return -1;
}

static void lane_close_sockets(struct pq_lane *ln)
{
    for (int i = 0; i < ln->nsock; i++) {
        close(ln->sock[i].fd);
        ln->sock[i].fd = -1;
    }
    ln->nsock = 0;
}

/* ------------------------------------------------------------------ */
/* connection IDs                                                      */
/* ------------------------------------------------------------------ */

static void group_cnx_id_cb(picoquic_quic_t *quic,
                            picoquic_connection_id_t cnx_id_local,
                            picoquic_connection_id_t cnx_id_remote,
                            void *cb_data,
                            picoquic_connection_id_t *cnx_id_returned)
{
    struct pq_lane *ln = cb_data;
    (void)quic;
    (void)cnx_id_remote;
    /* cnx_id_local holds PQ_GROUP_CID_LEN random bytes: the first is the
     * random byte, the next the nonce. */
    pq_group_cid_make(&ln->crypto, ln->index, cnx_id_local.id,
                      cnx_id_returned->id);
    cnx_id_returned->id_len = PQ_GROUP_CID_LEN;
}

#ifdef MOQ_PQ_THREADED_TESTING
/* A test can place client-chosen connection IDs on lanes itself instead of by
 * the keyed hash, so a fixture that needs work on every lane gets it
 * deterministically. It must map one ID to one lane for every caller. Defined
 * only in the test-internals archive, never the shipped library. */
uint32_t (*_Atomic moq_pq_threaded_test_owner_override)(const uint8_t *dcid,
                                                       size_t len,
                                                       uint32_t lanes);
#endif

static uint32_t route_owner(void *ctx, const uint8_t *dcid, size_t len,
                            uint32_t lanes)
{
#ifdef MOQ_PQ_THREADED_TESTING
    uint32_t (*fn)(const uint8_t *, size_t, uint32_t) =
        atomic_load(&moq_pq_threaded_test_owner_override);
    if (fn) return fn(dcid, len, lanes);
#endif
    return pq_group_owner_of(ctx, dcid, len, lanes);
}

static const pq_route_ops_t k_route_ops = {
    .verify = pq_group_cid_verify,
    .owner = route_owner,
};

/* ------------------------------------------------------------------ */
/* the loop                                                            */
/* ------------------------------------------------------------------ */

static void lane_loop_exited(struct pq_lane *ln)
{
    moq_pq_threaded_t *t = ln->facade;
    pthread_mutex_lock(&t->mutex);
    t->loop_running = false;
    t->loop_exited = true;
    pthread_cond_broadcast(&t->cond);
    pthread_mutex_unlock(&t->mutex);
}

/* A lane's exit that the group did not ask for (a pump returned nonzero, or
 * a loop failure) ends the group: every other lane is told to close. */
static void group_request_stop(struct pq_lane_group *g, bool fatal)
{
    if (atomic_exchange(&g->closing, 1) != 0)
        return;
    moq_pq_threaded_t *gf = g->facade;
    pthread_mutex_lock(&gf->mutex);
    if (fatal) {
        gf->fatal = true;
    } else {
        gf->pump_exit = true;
    }
    gf->activity_pending = true;
    pthread_cond_broadcast(&gf->cond);
    pthread_mutex_unlock(&gf->mutex);
    for (uint32_t i = 0; i < g->count; i++) {
        moq_pq_threaded_t *t = g->lanes[i].facade;
        if (!t) continue;
        pthread_mutex_lock(&t->mutex);
        t->pump_exit = true;
        pthread_mutex_unlock(&t->mutex);
        lane_signal(&g->lanes[i]);
    }
}

/* Inject one datagram into the lane's context; `detect_only` looks for a
 * stateless reset without ever answering with one. */
/* Hand a datagram to this lane's context. Returns the connection it was
 * associated with, or NULL when the context tied it to none -- the signal
 * that a verifiable-CID datagram the router sent here may in fact be a
 * stateless reset for a connection another lane owns. */
static picoquic_cnx_t *lane_inject(struct pq_lane *ln, uint8_t *bytes,
                                   size_t len, struct sockaddr_storage *from,
                                   struct sockaddr_storage *to, int if_index,
                                   unsigned char ecn, uint64_t now,
                                   bool detect_only)
{
    picoquic_quic_t *quic = ln->facade->quic;
    picoquic_cnx_t *first = NULL;
    /* A detection copy must not draw a reset of its own, so reset sending is
     * off across the inject, then restored to the group's policy, which the
     * group enforces on at context create -- the lane answers unknown
     * connection IDs it owns with resets. */
    if (detect_only) picoquic_set_stateless_reset_sending(quic, 0);
    (void)picoquic_incoming_packet_ex(quic, bytes, len, (struct sockaddr *)from,
                                      (struct sockaddr *)to, if_index, ecn,
                                      &first, now);
    if (detect_only) picoquic_set_stateless_reset_sending(quic, 1);
    return first;
}

/* Offer a reset candidate to every other lane, within this turn's budget. */
static void lane_fanout(struct pq_lane *ln, const uint8_t *bytes, size_t len,
                        const struct sockaddr_storage *from,
                        const struct sockaddr_storage *to, int if_index,
                        unsigned char ecn, int *fanout)
{
    struct pq_lane_group *g = ln->g;
    if (*fanout >= PQ_GROUP_FANOUT_BUDGET) {
        st_add(ln, ST_FANOUT_SKIPPED, 1);
        return;
    }
    (*fanout)++;
    st_add(ln, ST_FANOUT_OUT, 1);
    for (uint32_t i = 0; i < g->count; i++) {
        struct pq_lane *other = &g->lanes[i];
        if (other == ln) continue;
        if (pq_inbox_push(&other->detect, bytes, len, from, to, if_index, ecn))
            lane_signal(other);
    }
}

/* Read this turn's datagrams from one socket. Returns the datagrams read. */
static int lane_receive(struct pq_lane *ln, struct pq_lane_socket *s,
                        int budget, uint64_t now, size_t *bytes_total,
                        int *fanout)
{
    struct pq_lane_group *g = ln->g;
    uint8_t buf[PICOQUIC_MAX_PACKET_SIZE + 1];
    int n = 0;
    while (n < budget) {
        struct sockaddr_storage from, to;
        int if_index = 0;
        unsigned char ecn = 0;
        memset(&from, 0, sizeof(from));
        memset(&to, 0, sizeof(to));
        int got = picoquic_recvmsg(s->fd, &from, &to, &if_index, &ecn, buf,
                                   (int)sizeof(buf));
        if (got <= 0) break;
        n++;
        st_add(ln, ST_RECEIVED, 1);
        if ((size_t)got > PICOQUIC_MAX_PACKET_SIZE) {
            st_add(ln, ST_OVERSIZE, 1);   /* never processed truncated */
            continue;
        }
        /* The socket's port: the destination from PKTINFO carries none. */
        if (to.ss_family == AF_INET) {
            ((struct sockaddr_in *)&to)->sin_port = s->n_port;
        } else if (to.ss_family == AF_INET6) {
            ((struct sockaddr_in6 *)&to)->sin6_port = s->n_port;
        }
        pq_route_t r = pq_cid_route(buf, (size_t)got, ln->index, g->count,
                                    &k_route_ops, &ln->crypto);
        switch (r.kind) {
        case PQ_ROUTE_LOCAL: {
            picoquic_cnx_t *first = lane_inject(ln, buf, (size_t)got, &from,
                                                &to, if_index, ecn, now, false);
            *bytes_total += (size_t)got;
            /* Routed here by its CID but tied to no connection: it may be a
             * reset for a connection another lane owns (its token, over a
             * coincidental or retired CID). Offer it to the others for
             * detection only -- no lane answers a copy with a reset. */
            if (!first && (size_t)got >= PQ_ROUTE_RESET_DETECT_MIN_LEN)
                lane_fanout(ln, buf, (size_t)got, &from, &to, if_index, ecn,
                            fanout);
            break;
        }
        case PQ_ROUTE_LANE: {
            struct pq_lane *owner = &g->lanes[r.lane];
            if (pq_inbox_push(&owner->inbox, buf, (size_t)got, &from, &to,
                              if_index, ecn)) {
                st_add(ln, ST_FWD_OUT, 1);
                lane_signal(owner);
            }
            break;
        }
        case PQ_ROUTE_DETECT:
            lane_inject(ln, buf, (size_t)got, &from, &to, if_index, ecn, now,
                        true);
            lane_fanout(ln, buf, (size_t)got, &from, &to, if_index, ecn,
                        fanout);
            break;
        case PQ_ROUTE_DROP:
        default:
            break;
        }
    }
    return n;
}

static int lane_send(struct pq_lane *ln, uint64_t now, size_t *bytes_sent)
{
    picoquic_quic_t *quic = ln->facade->quic;
    uint8_t buf[PICOQUIC_MAX_PACKET_SIZE];
    int n = 0;
    while (n < PQ_GROUP_SEND_BUDGET) {
        size_t len = 0;
        struct sockaddr_storage peer, local;
        int if_index = 0;
        picoquic_connection_id_t log_cid;
        picoquic_cnx_t *last = NULL;
        memset(&peer, 0, sizeof(peer));
        memset(&local, 0, sizeof(local));
        if (picoquic_prepare_next_packet_ex(quic, now, buf, sizeof(buf), &len,
                                            &peer, &local, &if_index, &log_cid,
                                            &last, NULL) != 0 || len == 0)
            break;
        struct pq_lane_socket *s = NULL;
        for (int i = 0; i < ln->nsock; i++) {
            if (ln->sock[i].af == peer.ss_family) {
                s = &ln->sock[i];
                break;
            }
        }
        if (!s) break;   /* no socket of that family: the family is not served */
        /* A socket bound to a specific address must send from it. picoquic's
         * normalization fills the source from the bound address where the
         * platform carries the source in a control message (Linux IP_PKTINFO)
         * and clears it where a bound socket rejects that message (BSD
         * IP_SENDSRCADDR, which fails with EINVAL), so the kernel then sends
         * from the bound address. A wildcard socket is left as prepared. */
        picoquic_socket_ctx_t s_ctx = { .bound_addr = s->bound,
                                        .n_port = s->n_port };
        picoquic_packet_loop_set_send_source(&s_ctx, &local);
        (void)picoquic_packet_loop_do_udp_send(quic, last, s->fd,
                                               &ln->send_param, buf, len,
                                               &peer, &local, if_index, 0, NULL,
                                               &log_cid, now);
        n++;
        *bytes_sent += len;
    }
    return n;
}

/* One receive turn: read up to the per-turn budget across the lane's ready
 * sockets, divided fairly by pq_recv_share so no one socket starves another. */
static int lane_recv_turn(struct pq_lane *ln, const struct pollfd *pfd,
                          uint64_t now, size_t *bytes_recv, int *fanout)
{
    int received = 0;
    int ready = 0;
    for (int i = 0; i < ln->nsock; i++)
        if (pfd[i].revents & POLLIN) ready++;
    int share = pq_recv_share(ready, PQ_GROUP_RECV_BUDGET);
    for (int i = 0; i < ln->nsock; i++) {
        if (!(pfd[i].revents & POLLIN)) continue;
        int remaining = PQ_GROUP_RECV_BUDGET - received;
        int b = share < remaining ? share : remaining;
        if (b > 0)
            received += lane_receive(ln, &ln->sock[i], b, now,
                                     bytes_recv, fanout);
    }
    return received;
}

/* Shorten this turn's wait to a pending per-lane deadline, bounded so a far
 * future never wraps the wait negative (which would busy-poll the loop); an
 * instant already served does not wake the lane again. */
static int64_t lane_deadline_delta(int64_t delta, uint64_t deadline,
                                   uint64_t served, uint64_t now)
{
    if (deadline != UINT64_MAX && deadline > served) {
        int64_t until = deadline_until_us(deadline, now);
        if (until < delta) delta = until;
    }
    return delta;
}

#ifdef MOQ_PQ_THREADED_TESTING
/* Fail the Nth lane-thread spawn (1-based; 0 disables), and count the lane
 * worker bodies in flight. A test drives the group's construction unwind from
 * a failure after an earlier lane's thread is already running and confirms no
 * lane thread survives it. Defined only in the test-internals archive, never
 * the shipped library. */
_Atomic int moq_pq_threaded_test_fail_spawn_at;
_Atomic int moq_pq_threaded_test_live_threads;
/* Set when the chosen spawn failure is taken, with the count of lane workers
 * already inside their bodies at that moment, so the test can confirm the
 * failure came from the injection and that the earlier workers were running. */
_Atomic int moq_pq_threaded_test_spawn_fail_reached;
_Atomic int moq_pq_threaded_test_live_at_spawn_fail;

/* Drive one receive turn over a lane whose only populated fields are the given
 * ready socket fds, so a test can confirm the per-turn budget is shared fairly
 * among them. The caller queues oversized datagrams: each is read and counted
 * toward the budget but discarded before routing, so no connection context is
 * needed. Returns the datagrams read. At most two sockets, as a lane has. */
int moq_pq_threaded_test_recv_turn(const int *fds, int nsock)
{
    struct pq_lane ln;
    struct pollfd pfd[2];
    size_t bytes = 0;
    int fanout = 0;
    memset(&ln, 0, sizeof(ln));
    if (nsock > 2) nsock = 2;
    ln.nsock = nsock;
    for (int i = 0; i < nsock; i++) {
        ln.sock[i].fd = fds[i];
        pfd[i].fd = fds[i];
        pfd[i].events = POLLIN;
        pfd[i].revents = POLLIN;
    }
    return lane_recv_turn(&ln, pfd, 0, &bytes, &fanout);
}

/* The group loop's per-lane deadline-to-wait fold, so a test can confirm a
 * far-future deadline bounds the wait rather than busy-polling it. */
int64_t moq_pq_threaded_test_lane_deadline_delta(int64_t delta, uint64_t deadline,
                                                 uint64_t served, uint64_t now)
{
    return lane_deadline_delta(delta, deadline, served, now);
}

/* Preset a group lane's detect-ring drop counters, so a test can read their
 * fold through the public lane-stats getter at the counters' limit. */
int moq_pq_threaded_test_set_detect_drops(moq_pq_threaded_lane_t *lane,
                                          uint64_t full, uint64_t closed)
{
    moq_pq_threaded_t *t = lane ? lane->owner : NULL;
    struct pq_lane_group *g = t && t->group ? t->group->group_ctx : NULL;
    if (!g) return -1;
    struct pq_lane *ln = &g->lanes[lane->index];
    atomic_store(&ln->detect.full_drops, full);
    atomic_store(&ln->detect.closed_drops, closed);
    return 0;
}
#endif

static void *lane_main(void *arg)
{
    struct pq_lane *ln = arg;
#ifdef MOQ_PQ_THREADED_TESTING
    atomic_fetch_add(&moq_pq_threaded_test_live_threads, 1);
#endif
    struct pq_lane_group *g = ln->g;
    moq_pq_threaded_t *t = ln->facade;
    picoquic_quic_t *quic = t->quic;
    picoquic_packet_loop_options_t options;
    memset(&options, 0, sizeof(options));
    int ret = moq_pq_threaded_loop_callback(quic, picoquic_packet_loop_ready, t,
                                            &options);
    bool rearm = false;
    bool failed = false;

    while (ret == 0) {
        uint64_t now = picoquic_get_quic_time(quic);
        packet_loop_time_check_arg_t tc;
        tc.current_time = now;
        tc.delta_t = picoquic_get_next_wake_delay(quic, now, PQ_GROUP_MAX_WAIT_US);
        ret = moq_pq_threaded_loop_callback(quic, picoquic_packet_loop_time_check,
                                            t, &tc);
        if (ret != 0) break;
        int64_t delta = tc.delta_t;
        if (g->app_lane_deadline_us) {
            uint64_t ad = g->app_lane_deadline_us(g->app_lane_deadline_ctx,
                                                  ln->index);
            delta = lane_deadline_delta(delta, ad, ln->deadline_served, now);
        }
        if (rearm || atomic_load(&g->closing)) delta = 0;
        if (delta < 0) delta = 0;
        if (delta > PQ_GROUP_MAX_WAIT_US) delta = PQ_GROUP_MAX_WAIT_US;
        int timeout_ms = (int)((delta + 999) / 1000);

        struct pollfd pfd[3];
        int nfd = 0;
        for (int i = 0; i < ln->nsock; i++) {
            pfd[nfd].fd = ln->sock[i].fd;
            pfd[nfd].events = POLLIN;
            pfd[nfd].revents = 0;
            nfd++;
        }
        pfd[nfd].fd = ln->wake_r;
        pfd[nfd].events = POLLIN;
        pfd[nfd].revents = 0;
        int wake_ix = nfd++;
        int pr = poll(pfd, (nfds_t)nfd, timeout_ms);
        if (pr < 0 && errno != EINTR && errno != EAGAIN) {
            failed = true;
            break;
        }
        now = picoquic_get_quic_time(quic);

        /* The pipe: an application wake, a producer's signal, or both. The
         * flag is cleared before the rings are read, so a push after the
         * clear signals again. */
        bool woken = false;
        if (pfd[wake_ix].revents & POLLIN) {
            drain_pipe(ln->wake_r);
            atomic_store_explicit(&ln->signaled, 0, memory_order_release);
            pthread_mutex_lock(&t->mutex);
            woken = t->wake_pending;
            pthread_mutex_unlock(&t->mutex);
        }
        if (woken) {
            ret = moq_pq_threaded_loop_callback(quic, picoquic_packet_loop_wake_up,
                                                t, NULL);
            if (ret != 0) break;
        }

        size_t bytes_recv = 0;
        int fanout = 0;
        int received = lane_recv_turn(ln, pfd, now, &bytes_recv, &fanout);
        int injected = 0;
        {
            pq_inbox_slot_t slot;
            while (injected < PQ_GROUP_INJECT_BUDGET &&
                   pq_inbox_pop(&ln->inbox, &slot)) {
                picoquic_cnx_t *first =
                    lane_inject(ln, slot.bytes, slot.len, &slot.from, &slot.to,
                                slot.if_index, slot.ecn, now, false);
                st_add(ln, ST_FWD_IN, 1);
                bytes_recv += slot.len;
                injected++;
                /* Forwarded here by its CID but tied to no connection: as in
                 * the local case, it may be a reset another lane owns. */
                if (!first && slot.len >= PQ_ROUTE_RESET_DETECT_MIN_LEN)
                    lane_fanout(ln, slot.bytes, slot.len, &slot.from, &slot.to,
                                slot.if_index, slot.ecn, &fanout);
            }
        }
        int detected = 0;
        {
            pq_inbox_slot_t slot;
            while (detected < PQ_GROUP_DETECT_BUDGET &&
                   pq_inbox_pop(&ln->detect, &slot)) {
                lane_inject(ln, slot.bytes, slot.len, &slot.from, &slot.to,
                            slot.if_index, slot.ecn, now, true);
                st_add(ln, ST_FANOUT_IN, 1);
                detected++;
            }
        }
        if (received + injected + detected > 0) {
            size_t b = bytes_recv;
            ret = moq_pq_threaded_loop_callback(quic,
                                                picoquic_packet_loop_after_receive,
                                                t, &b);
            if (ret != 0) break;
        }

        size_t bytes_sent = 0;
        int sent = lane_send(ln, now, &bytes_sent);
        ret = moq_pq_threaded_loop_callback(quic, picoquic_packet_loop_after_send,
                                            t, &bytes_sent);
        if (ret != 0) break;

        /* The pump ran: the lane deadline it was due for is served. */
        if (g->app_lane_deadline_us) {
            uint64_t ad = g->app_lane_deadline_us(g->app_lane_deadline_ctx,
                                                  ln->index);
            if (ad != UINT64_MAX && ad <= now && ad > ln->deadline_served)
                ln->deadline_served = ad;
        }

        rearm = received >= PQ_GROUP_RECV_BUDGET ||
                injected >= PQ_GROUP_INJECT_BUDGET ||
                detected >= PQ_GROUP_DETECT_BUDGET ||
                sent >= PQ_GROUP_SEND_BUDGET ||
                !pq_inbox_empty(&ln->inbox) || !pq_inbox_empty(&ln->detect);
        if (rearm) st_add(ln, ST_REARMS, 1);
    }

    lane_loop_exited(ln);
    if (!atomic_load(&g->closing))
        group_request_stop(g, failed);
#ifdef MOQ_PQ_THREADED_TESTING
    atomic_fetch_sub(&moq_pq_threaded_test_live_threads, 1);
#endif
    return NULL;
}

/* Start a lane's worker thread. A test build can fail a chosen spawn, so the
 * construction unwind can be driven from a partly-started group. */
static int lane_spawn(struct pq_lane *ln, uint32_t index)
{
#ifdef MOQ_PQ_THREADED_TESTING
    if (atomic_load(&moq_pq_threaded_test_fail_spawn_at) == (int)index + 1) {
        /* Fail only once every earlier lane's worker has entered its body
         * (bounded at 5 s), so the unwind runs against workers that are
         * actually running, not merely created. */
        for (int i = 0; i < 5000 &&
             atomic_load(&moq_pq_threaded_test_live_threads) < (int)index; i++) {
            struct timespec ms = { 0, 1000000 };
            nanosleep(&ms, NULL);
        }
        atomic_store(&moq_pq_threaded_test_live_at_spawn_fail,
                     atomic_load(&moq_pq_threaded_test_live_threads));
        atomic_store(&moq_pq_threaded_test_spawn_fail_reached, 1);
        return -1;
    }
#else
    (void)index;
#endif
    return pthread_create(&ln->thread, NULL, lane_main, ln);
}

/* ------------------------------------------------------------------ */
/* lifecycle                                                           */
/* ------------------------------------------------------------------ */

static bool cfg_has(const moq_pq_threaded_cfg_t *cfg, size_t off, size_t size)
{
    return cfg->struct_size >= off && size <= cfg->struct_size - off;
}
#define GCFG_HAS(cfg, field) \
    cfg_has(cfg, offsetof(moq_pq_threaded_cfg_t, field), sizeof((cfg)->field))

static void lane_teardown(struct pq_lane_group *g, struct pq_lane *ln)
{
    /* After the thread, if it ran, has been joined. */
    pq_inbox_discard(&ln->inbox);
    pq_inbox_discard(&ln->detect);
    lane_close_sockets(ln);
    if (ln->wake_r >= 0) { close(ln->wake_r); ln->wake_r = -1; }
    if (ln->wake_w >= 0) { close(ln->wake_w); ln->wake_w = -1; }
    if (ln->facade) {
        moq_pq_threaded_destroy(ln->facade);
        ln->facade = NULL;
    }
    pq_inbox_free(&ln->inbox, &g->alloc);
    pq_inbox_free(&ln->detect, &g->alloc);
    pq_group_crypto_lane_free(&ln->crypto);
}

/* Stop a lane that was started: the facade's stop drives the bounded local
 * close through the loop, then the thread is joined. */
static void lane_stop_and_join(struct pq_lane *ln)
{
    if (!ln->facade) return;
    if (ln->thread_started) {
        (void)moq_pq_threaded_stop(ln->facade);
        pthread_join(ln->thread, NULL);
        ln->thread_started = false;
    }
}

static void group_free(struct pq_lane_group *g)
{
    moq_alloc_t alloc = g->alloc;
    if (g->lanes) {
        for (uint32_t i = 0; i < g->count; i++)
            lane_teardown(g, &g->lanes[i]);
        alloc.free(g->lanes, (size_t)g->count * sizeof(*g->lanes), alloc.ctx);
    }
    pq_group_keys_erase(&g->keys);
    alloc.free(g, sizeof(*g), alloc.ctx);
}

moq_result_t pq_lane_group_create(const moq_pq_threaded_cfg_t *cfg,
                                  moq_pq_threaded_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (!GCFG_HAS(cfg, lane_count) || cfg->lane_count < 2)
        return MOQ_ERR_INVAL;
    if (cfg->lane_count > MOQ_PQ_THREADED_MAX_LANES)
        return MOQ_ERR_INVAL;
    if (!GCFG_HAS(cfg, perspective) || cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_UNSUPPORTED;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc)
        return MOQ_ERR_INVAL;
    int port = GCFG_HAS(cfg, port) ? cfg->port : 0;
    if (port <= 0 || port > 65535)
        return MOQ_ERR_INVAL;
    if (GCFG_HAS(cfg, socket_buffer_bytes) &&
        cfg->socket_buffer_bytes > (uint32_t)INT32_MAX)
        return MOQ_ERR_INVAL;
#if !defined(SO_REUSEPORT)
    return MOQ_ERR_UNSUPPORTED;
#endif

    const moq_alloc_t *alloc = cfg->alloc;
    moq_pq_threaded_t *gf = alloc->alloc(sizeof(*gf), alloc->ctx);
    if (!gf) return MOQ_ERR_NOMEM;
    memset(gf, 0, sizeof(*gf));
    gf->alloc = *alloc;
    gf->perspective = MOQ_PERSPECTIVE_SERVER;
    gf->port = port;
    gf->wake_fd = -1;
    gf->lane.owner = gf;
    if (pthread_mutex_init(&gf->mutex, NULL) != 0) {
        alloc->free(gf, sizeof(*gf), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }
    if (pthread_cond_init(&gf->cond, NULL) != 0) {
        pthread_mutex_destroy(&gf->mutex);
        alloc->free(gf, sizeof(*gf), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }

    struct pq_lane_group *g = alloc->alloc(sizeof(*g), alloc->ctx);
    if (!g) {
        pthread_cond_destroy(&gf->cond);
        pthread_mutex_destroy(&gf->mutex);
        alloc->free(gf, sizeof(*gf), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(g, 0, sizeof(*g));
    g->facade = gf;
    g->alloc = *alloc;
    g->count = cfg->lane_count;
    g->max_connections =
        (GCFG_HAS(cfg, max_connections) && cfg->max_connections)
            ? cfg->max_connections : 1024u;
    if (GCFG_HAS(cfg, app_lane_deadline_ctx)) {
        g->app_lane_deadline_us = cfg->app_lane_deadline_us;
        g->app_lane_deadline_ctx = cfg->app_lane_deadline_ctx;
    }
    gf->group_ctx = g;
    moq_result_t rc = MOQ_ERR_INTERNAL;

    if (!pq_group_keys_make(&g->keys)) goto fail;
    g->lanes = alloc->alloc((size_t)g->count * sizeof(*g->lanes), alloc->ctx);
    if (!g->lanes) { rc = MOQ_ERR_NOMEM; goto fail; }
    memset(g->lanes, 0, (size_t)g->count * sizeof(*g->lanes));
    for (uint32_t i = 0; i < g->count; i++) {
        g->lanes[i].g = g;
        g->lanes[i].index = i;
        g->lanes[i].wake_r = g->lanes[i].wake_w = -1;
    }

    struct sockaddr_storage bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    if (GCFG_HAS(cfg, bind_address) && cfg->bind_address) {
        size_t alen = cfg->bind_address->sa_family == AF_INET
                          ? sizeof(struct sockaddr_in)
                      : cfg->bind_address->sa_family == AF_INET6
                          ? sizeof(struct sockaddr_in6) : 0;
        if (alen == 0) { rc = MOQ_ERR_INVAL; goto fail; }
        memcpy(&bind_addr, cfg->bind_address, alen);
    }
    uint32_t buffer_bytes =
        GCFG_HAS(cfg, socket_buffer_bytes) ? cfg->socket_buffer_bytes : 0;

    /* Every lane's context, sockets, rings and pipe exist before any lane
     * thread starts: the group is published to its lanes all at once. */
    for (uint32_t i = 0; i < g->count; i++) {
        struct pq_lane *ln = &g->lanes[i];
        if (!pq_group_crypto_lane_init(&ln->crypto, &g->keys)) goto fail;
        if (!pq_inbox_init(&ln->inbox, alloc, PQ_GROUP_INBOX_SLOTS) ||
            !pq_inbox_init(&ln->detect, alloc, PQ_GROUP_DETECT_SLOTS)) {
            rc = MOQ_ERR_NOMEM;
            goto fail;
        }
        if (open_wake_pipe(ln) != 0) goto fail;
        pq_lane_opts_t opts = {
            .group = gf,
            .index = i,
            .wake_fd = ln->wake_w,
            .cnx_id_cb = group_cnx_id_cb,
            .cnx_id_ctx = ln,
            .cid_len = PQ_GROUP_CID_LEN,
            .ticket_key = g->keys.ticket,
            .ticket_key_len = sizeof(g->keys.ticket),
        };
        rc = moq_pq_threaded_create_lane(cfg, &opts, &ln->facade);
        if (rc != MOQ_OK) goto fail;
        rc = MOQ_ERR_INTERNAL;
        memset(&ln->send_param, 0, sizeof(ln->send_param));
        if (bind_addr.ss_family == 0) {
            if (lane_open_socket(ln, AF_INET, port, &bind_addr, buffer_bytes) != 0 ||
                lane_open_socket(ln, AF_INET6, port, &bind_addr, buffer_bytes) != 0)
                goto fail;
        } else if (lane_open_socket(ln, (int)bind_addr.ss_family, port,
                                    &bind_addr, buffer_bytes) != 0) {
            goto fail;
        }
    }

    for (uint32_t i = 0; i < g->count; i++) {
        struct pq_lane *ln = &g->lanes[i];
        if (lane_spawn(ln, i) != 0) goto fail;
        ln->thread_started = true;
    }
    /* Each loop's ready callback latches loop_came_up on its facade. */
    for (uint32_t i = 0; i < g->count; i++) {
        moq_pq_threaded_t *t = g->lanes[i].facade;
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += PQ_GROUP_READY_WAIT_S;
        pthread_mutex_lock(&t->mutex);
        while (!t->loop_came_up) {
            if (pthread_cond_timedwait(&t->cond, &t->mutex, &deadline) != 0)
                break;
        }
        bool up = t->loop_came_up;
        pthread_mutex_unlock(&t->mutex);
        if (!up) goto fail;
    }

    *out = gf;
    return MOQ_OK;

fail:
    /* Lanes already running are stopped and joined, then everything is
     * released; the group facade last. */
    atomic_store(&g->closing, 1);
    if (g->lanes) {
        for (uint32_t i = 0; i < g->count; i++) {
            pq_inbox_close(&g->lanes[i].inbox);
            pq_inbox_close(&g->lanes[i].detect);
        }
        for (uint32_t i = 0; i < g->count; i++)
            if (g->lanes[i].thread_started) lane_signal(&g->lanes[i]);
        for (uint32_t i = 0; i < g->count; i++)
            lane_stop_and_join(&g->lanes[i]);
    }
    group_free(g);
    pthread_cond_destroy(&gf->cond);
    pthread_mutex_destroy(&gf->mutex);
    alloc->free(gf, sizeof(*gf), alloc->ctx);
    return rc;
}

uint32_t pq_lane_group_count(const struct pq_lane_group *g)
{
    return g->count;
}

moq_pq_threaded_t *pq_lane_group_lane(struct pq_lane_group *g, uint32_t i)
{
    return i < g->count ? g->lanes[i].facade : NULL;
}

size_t pq_lane_group_conn_count(const struct pq_lane_group *g)
{
    size_t n = 0;
    for (uint32_t i = 0; i < g->count; i++)
        n += moq_pq_threaded_conn_count(g->lanes[i].facade);
    return n;
}

int pq_lane_group_drain_state(struct pq_lane_group *g)
{
    for (uint32_t i = 0; i < g->count; i++) {
        moq_pq_threaded_t *t = g->lanes[i].facade;
        pthread_mutex_lock(&t->mutex);
        bool own = t->network_thread_id_set &&
                   pthread_equal(pthread_self(), t->network_thread_id);
        pthread_mutex_unlock(&t->mutex);
        if (own) return -2;
    }
    for (uint32_t i = 0; i < g->count; i++) {
        if (moq_pq_threaded_drain_state(g->lanes[i].facade) != 1)
            return 0;
    }
    return 1;
}

bool pq_lane_group_is_fatal(const struct pq_lane_group *g)
{
    /* The group facade's own flag, read directly: its public accessor
     * dispatches here. */
    moq_pq_threaded_t *gf = g->facade;
    pthread_mutex_lock(&gf->mutex);
    bool f = gf->fatal;
    pthread_mutex_unlock(&gf->mutex);
    if (f) return true;
    for (uint32_t i = 0; i < g->count; i++)
        if (moq_pq_threaded_is_fatal(g->lanes[i].facade)) return true;
    return false;
}

uint64_t pq_lane_group_fatal_code(const struct pq_lane_group *g)
{
    for (uint32_t i = 0; i < g->count; i++) {
        uint64_t c = moq_pq_threaded_fatal_code(g->lanes[i].facade);
        if (c) return c;
    }
    return 0;
}

moq_result_t pq_lane_group_wake_all(struct pq_lane_group *g)
{
    moq_result_t rc = MOQ_OK;
    for (uint32_t i = 0; i < g->count; i++) {
        moq_result_t r = moq_pq_threaded_wake(g->lanes[i].facade);
        if (r != MOQ_OK) rc = r;
    }
    return rc;
}

void pq_lane_group_lane_stop(moq_pq_threaded_t *lane)
{
    struct pq_lane_group *g = lane->group ? lane->group->group_ctx : NULL;
    if (!g) return;
    struct pq_lane *ln = &g->lanes[lane->lane.index];
    lane_signal(ln);
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += PQ_GROUP_EXIT_WAIT_S;
    pthread_mutex_lock(&lane->mutex);
    while (lane->loop_running) {
        if (pthread_cond_timedwait(&lane->cond, &lane->mutex, &deadline) ==
            ETIMEDOUT)
            break;
    }
    pthread_mutex_unlock(&lane->mutex);
}

moq_result_t pq_lane_group_stop(struct pq_lane_group *g)
{
    moq_pq_threaded_t *gf = g->facade;
    for (uint32_t i = 0; i < g->count; i++) {
        moq_pq_threaded_t *t = g->lanes[i].facade;
        pthread_mutex_lock(&t->mutex);
        bool own = t->network_thread_id_set &&
                   pthread_equal(pthread_self(), t->network_thread_id);
        pthread_mutex_unlock(&t->mutex);
        if (own) return MOQ_ERR_WRONG_STATE;
    }
    pthread_mutex_lock(&gf->mutex);
    if (gf->stopped) {
        pthread_mutex_unlock(&gf->mutex);
        return pq_lane_group_is_fatal(g) ? MOQ_ERR_CLOSED : MOQ_OK;
    }
    gf->stopped = true;
    gf->activity_pending = true;
    pthread_cond_broadcast(&gf->cond);
    pthread_mutex_unlock(&gf->mutex);

    /* Close admission everywhere, then wake every lane: a lane that was
     * blocked finds the stop; one forwarding finds the rings closed. */
    atomic_store(&g->closing, 1);
    for (uint32_t i = 0; i < g->count; i++) {
        pq_inbox_close(&g->lanes[i].inbox);
        pq_inbox_close(&g->lanes[i].detect);
    }
    for (uint32_t i = 0; i < g->count; i++)
        lane_signal(&g->lanes[i]);
    for (uint32_t i = 0; i < g->count; i++)
        lane_stop_and_join(&g->lanes[i]);
    for (uint32_t i = 0; i < g->count; i++) {
        pq_inbox_discard(&g->lanes[i].inbox);
        pq_inbox_discard(&g->lanes[i].detect);
    }
    return pq_lane_group_is_fatal(g) ? MOQ_ERR_CLOSED : MOQ_OK;
}

void pq_lane_group_destroy(struct pq_lane_group *g)
{
    pthread_mutex_lock(&g->facade->mutex);
    bool stopped = g->facade->stopped;
    pthread_mutex_unlock(&g->facade->mutex);
    if (!stopped) (void)pq_lane_group_stop(g);
    group_free(g);
}

/* ------------------------------------------------------------------ */
/* admission                                                           */
/* ------------------------------------------------------------------ */

bool pq_lane_group_admit(moq_pq_threaded_t *lane)
{
    struct pq_lane_group *g = lane->group->group_ctx;
    size_t cur = atomic_load(&g->conn_total);
    for (;;) {
        if (cur >= g->max_connections) return false;
        if (atomic_compare_exchange_weak(&g->conn_total, &cur, cur + 1))
            return true;
    }
}

void pq_lane_group_release(moq_pq_threaded_t *lane)
{
    struct pq_lane_group *g = lane->group->group_ctx;
    size_t cur = atomic_load(&g->conn_total);
    while (cur > 0 &&
           !atomic_compare_exchange_weak(&g->conn_total, &cur, cur - 1))
        ;
}

/* ------------------------------------------------------------------ */
/* stats                                                               */
/* ------------------------------------------------------------------ */

moq_result_t moq_picoquic_threaded_lane_get_stats(
    const moq_pq_threaded_lane_t *lane,
    moq_picoquic_threaded_lane_stats_t *out, size_t out_size)
{
    if (!lane || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_PICOQUIC_THREADED_LANE_STATS_V0_SIZE) return MOQ_ERR_INVAL;
    moq_pq_threaded_t *t = lane->owner;
    struct pq_lane_group *g = t && t->group ? t->group->group_ctx : NULL;
    if (!g) return MOQ_ERR_UNSUPPORTED;
    struct pq_lane *ln = &g->lanes[lane->index];
    moq_picoquic_threaded_lane_stats_t rec;
    memset(&rec, 0, sizeof(rec));
    size_t n = out_size < sizeof(rec) ? out_size : sizeof(rec);
    rec.struct_size = (uint32_t)n;
    rec.datagrams_received = atomic_load_explicit(&ln->st[ST_RECEIVED], memory_order_relaxed);
    rec.forwarded_out = atomic_load_explicit(&ln->st[ST_FWD_OUT], memory_order_relaxed);
    rec.forwarded_in = atomic_load_explicit(&ln->st[ST_FWD_IN], memory_order_relaxed);
    rec.inbox_full_drops = atomic_load_explicit(&ln->inbox.full_drops, memory_order_relaxed);
    rec.inbox_closed_drops = atomic_load_explicit(&ln->inbox.closed_drops, memory_order_relaxed);
    rec.inbox_discarded = atomic_load_explicit(&ln->inbox.discarded, memory_order_relaxed);
    rec.oversize_drops = atomic_load_explicit(&ln->st[ST_OVERSIZE], memory_order_relaxed);
    rec.reset_fanout_out = atomic_load_explicit(&ln->st[ST_FANOUT_OUT], memory_order_relaxed);
    rec.reset_fanout_in = atomic_load_explicit(&ln->st[ST_FANOUT_IN], memory_order_relaxed);
    rec.reset_fanout_skipped = atomic_load_explicit(&ln->st[ST_FANOUT_SKIPPED], memory_order_relaxed);
    uint64_t df = atomic_load_explicit(&ln->detect.full_drops, memory_order_relaxed);
    uint64_t dc = atomic_load_explicit(&ln->detect.closed_drops, memory_order_relaxed);
    rec.detection_drops = pq_u64_sat_add(df, dc);
    rec.detection_discarded = atomic_load_explicit(&ln->detect.discarded, memory_order_relaxed);
    rec.budget_rearms = atomic_load_explicit(&ln->st[ST_REARMS], memory_order_relaxed);
    memcpy(out, &rec, n);
    return MOQ_OK;
}
