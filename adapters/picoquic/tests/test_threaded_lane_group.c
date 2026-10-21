/*
 * The PicoQUIC lane group over loopback: a server facade with
 * cfg.lane_count > 1, real clients, bounded waits only.
 *
 * Rows (argv[1]):
 *   spread     48 clients against 4 lanes: every client is served, every
 *              lane owns connections, every lane processed its own traffic
 *              (received directly or forwarded to it), nothing was dropped
 *   rebind     one client behind a UDP proxy that moves to a new source port
 *              mid-stream (a NAT rebinding): the stream completes on the
 *              same server connection, on the same lane
 *   reset      8 clients behind the proxy: a stateless reset for each, sent to
 *              the server from the client's tuple, ends the connection on the
 *              lane that owns it, whichever lane received the reset; the
 *              clients' next packets are answered with the server's resets
 *   stop_load  16 clients streaming when the server stops: the stop completes
 *              within its bound
 *   deadline   a per-lane deadline wakes its lane alone, and once
 *   admission  max_connections is the group's total across lanes
 *   limits     the lane-count bounds, the group's facade accessors, and the
 *              stats record's size contract
 */

#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#include <picoquic.h>
#include <picoquic_internal.h>   /* white-box: the context settings the group
                                  * re-asserts have no public getters */
#include <picotls.h>   /* ptls_context_t, to read max_early_data_size */
#include <tls_api.h>   /* picoquic_create_cnxid_reset_secret */
#include "../pq_threaded_internal.h"   /* white-box: a connection's cnx, to read
                                        * the server CID that routes to its lane */

#define PQ_GROUP_CID_LEN 17u   /* adapter-internal; restated for the assert */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            failures++;                                                 \
        }                                                               \
    } while (0)

#define LANES     4u
#define OBJ_LEN   1200u

static struct timespec deadline_in(int seconds)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += seconds;
    return t;
}

static int time_before(const struct timespec *dl)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec < dl->tv_sec ||
           (now.tv_sec == dl->tv_sec && now.tv_nsec < dl->tv_nsec);
}

static uint64_t mono_us(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000u + (uint64_t)t.tv_nsec / 1000u;
}

static void nap_ms(int ms)
{
    struct timespec ts = { 0, (long)ms * 1000000L };
    nanosleep(&ts, NULL);
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

/* -- the server: accept SUBSCRIBE, write objects ---------------------------- */

/* Per-connection state, in the connection's user slot. Static storage: the
 * server's lanes pump concurrently and a record outlives its connection. */
typedef struct {
    moq_session_t     *session;
    moq_subscription_t sub;
    bool               subscribed;
    unsigned           written;
    /* The lane that owns this connection and the current server-issued CID
     * that routes to it, refreshed each pump (the cross-lane reset row maps a
     * verifiable CID to its lane through these). */
    _Atomic uint32_t   lane;
    _Atomic int        server_cid_len;
    uint8_t            server_cid[PICOQUIC_CONNECTION_ID_MAX_SIZE];
} conn_rec_t;

#define MAX_RECS 256
static conn_rec_t g_recs[MAX_RECS];
static _Atomic unsigned g_rec_next;

typedef struct {
    _Atomic int accepted[MOQ_PQ_THREADED_MAX_LANES];
    _Atomic int pumps[MOQ_PQ_THREADED_MAX_LANES];
    _Atomic int objects;             /* written, all lanes */
    unsigned    objects_per_sub;     /* 1, or many for the streaming rows */
    _Atomic uint64_t deadline_at;    /* the lane deadline row */
    _Atomic uint64_t due_now[MOQ_PQ_THREADED_MAX_LANES];
} srv_ctx_t;

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *vctx)
{
    (void)t;
    srv_ctx_t *s = vctx;
    uint32_t li = moq_pq_threaded_lane_index(lane);
    s->pumps[li]++;
    uint64_t dl = atomic_load(&s->deadline_at);
    if (dl != UINT64_MAX && now >= dl && atomic_load(&s->due_now[li]) == 0)
        atomic_store(&s->due_now[li], now);
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL);
         c; c = moq_pq_threaded_lane_next_conn(lane, c)) {
        conn_rec_t *rec = moq_picoquic_threaded_conn_user(c);
        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (!sess) continue;
        if (!rec) {
            unsigned ix = atomic_fetch_add(&g_rec_next, 1);
            if (ix >= MAX_RECS) continue;
            rec = &g_recs[ix];
            memset(rec, 0, sizeof(*rec));
            rec->session = sess;
            moq_picoquic_threaded_conn_set_user(c, rec);
            s->accepted[li]++;
        }
        /* Record where this connection lives and the first server CID that
         * routes to it, once. The owning lane is the only writer of its
         * records; the release pairs with the test's acquire so the bytes are
         * safe to read after. This first CID is retired once the client
         * rebinds, which is exactly the "verifiable CID its lane no longer
         * owns" the cross-lane row needs. */
        if (atomic_load_explicit(&rec->server_cid_len, memory_order_relaxed) == 0) {
            picoquic_cnx_t *cnx = ((struct moq_pq_threaded_conn *)c)->cnx;
            if (cnx) {
                picoquic_connection_id_t lcid = picoquic_get_local_cnxid(cnx);
                if (lcid.id_len > 0 &&
                    lcid.id_len <= PICOQUIC_CONNECTION_ID_MAX_SIZE) {
                    memcpy(rec->server_cid, lcid.id, lcid.id_len);
                    atomic_store_explicit(&rec->lane, li, memory_order_relaxed);
                    atomic_store_explicit(&rec->server_cid_len, (int)lcid.id_len,
                                          memory_order_release);
                }
            }
        }
        moq_event_t ev[8];
        size_t ne = 0;
        moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !rec->subscribed) {
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                rec->sub = ev[i].u.subscribe_request.sub;
                if (moq_session_accept_subscribe(sess, rec->sub, &acfg, now) >= 0)
                    rec->subscribed = true;
            }
            moq_event_cleanup(&ev[i]);
        }
        if (rec->subscribed && rec->written < s->objects_per_sub) {
            static const uint8_t body[OBJ_LEN] = { 0x5a };
            moq_subgroup_cfg_t sg;
            moq_subgroup_cfg_init(&sg);
            sg.group_id = rec->written;
            moq_subgroup_handle_t h;
            moq_rcbuf_t *b = NULL;
            if (moq_session_open_subgroup(sess, rec->sub, &sg, now, &h) >= 0 &&
                moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &b) == MOQ_OK) {
                if (moq_session_write_object(sess, h, 0, b, now) >= 0) {
                    rec->written++;
                    s->objects++;
                }
                moq_rcbuf_decref(b);
                moq_session_close_subgroup(sess, h, now);
            }
        }
    }
    return 0;
}

static uint64_t lane_deadline_fn(void *vctx, uint32_t lane)
{
    srv_ctx_t *s = vctx;
    return lane == 1 ? atomic_load(&s->deadline_at) : UINT64_MAX;
}

static uint64_t lane0_deadline_fn(void *vctx, uint32_t lane)
{
    srv_ctx_t *s = vctx;
    return lane == 0 ? atomic_load(&s->deadline_at) : UINT64_MAX;
}

static moq_result_t server_create(int port, uint32_t lanes, uint32_t max_conns,
                                  srv_ctx_t *s, bool with_lane_deadline,
                                  moq_pq_threaded_t **out)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
    cfg.port = port;
    cfg.lane_count = lanes;
    cfg.max_connections = max_conns;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_ctx = s;
    cfg.send_request_capacity = true;
    cfg.streaming_objects = false;
    if (with_lane_deadline) {
        cfg.app_lane_deadline_us = lane_deadline_fn;
        cfg.app_lane_deadline_ctx = s;
    }
    *out = NULL;
    return moq_pq_threaded_create(&cfg, out);
}

static moq_pq_threaded_t *server_on_free_port(uint32_t lanes, uint32_t max_conns,
                                              srv_ctx_t *s, bool with_deadline,
                                              int *port_out)
{
    for (int attempt = 0; attempt < 16; attempt++) {
        int port = pick_free_udp_port();
        moq_pq_threaded_t *srv = NULL;
        if (port && server_create(port, lanes, max_conns, s, with_deadline, &srv) == MOQ_OK) {
            *port_out = port;
            return srv;
        }
    }
    return NULL;
}

/* -- clients: subscribe, count objects --------------------------------------- */

typedef struct {
    _Atomic int subscribed;
    _Atomic int whole;
    _Atomic int established;
    _Atomic int closed;          /* the session ended after being established */
    _Atomic int keep_alive_wanted; /* set before create: PING the server every 200 ms */
    bool keep_alive;             /* the pump's: keep-alives enabled */
    picoquic_quic_t *quic;       /* captured at create; its reset seed signs
                                  * the tokens this client advertises */
} cli_ctx_t;

static int capture_quic(picoquic_quic_t *quic, void *vctx)
{
    ((cli_ctx_t *)vctx)->quic = quic;
    return 0;
}

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *vctx)
{
    (void)lane;
    cli_ctx_t *c = vctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess || moq_session_state(sess) == MOQ_SESS_CLOSED) {
        if (atomic_load(&c->established)) atomic_store(&c->closed, 1);
        return 0;
    }
    if (moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        c->established = 1;
        if (atomic_load(&c->keep_alive_wanted) && !c->keep_alive && c->quic) {
            picoquic_cnx_t *cnx = picoquic_get_first_cnx(c->quic);
            if (cnx) {
                picoquic_enable_keep_alive(cnx, 200000);
                c->keep_alive = true;
            }
        }
    }
    if (!c->subscribed && moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[1] = { MOQ_BYTES_LITERAL("ns") };
        sc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
        sc.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        if (moq_session_subscribe(sess, &sc, now, &sub) == MOQ_OK) c->subscribed = 1;
    }
    moq_event_t ev[8];
    size_t ne = 0;
    moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) c->whole++;
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

static moq_pq_threaded_t *client_create(int port, cli_ctx_t *c)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_ctx = c;
    cfg.configure_quic = capture_quic;
    cfg.configure_quic_ctx = c;
    moq_pq_threaded_t *cli = NULL;
    return moq_pq_threaded_create(&cfg, &cli) == MOQ_OK ? cli : NULL;
}

static void stop(moq_pq_threaded_t **f)
{
    if (!*f) return;
    moq_pq_threaded_stop(*f);
    moq_pq_threaded_destroy(*f);
    *f = NULL;
}

static int sum_lanes(_Atomic int *a, uint32_t n)
{
    int s = 0;
    for (uint32_t i = 0; i < n; i++) s += atomic_load(&a[i]);
    return s;
}

/* Per-lane routing counters, summed and printed. */
static void print_stats(moq_pq_threaded_t *srv, uint32_t lanes,
                        moq_picoquic_threaded_lane_stats_t *per_lane)
{
    for (uint32_t i = 0; i < lanes; i++) {
        moq_pq_threaded_lane_t *l = moq_pq_threaded_lane(srv, i);
        CHECK(l != NULL);
        CHECK(moq_picoquic_threaded_lane_get_stats(l, &per_lane[i], sizeof(per_lane[i])) == MOQ_OK);
        printf("  lane %u: received %llu fwd_out %llu fwd_in %llu discarded %llu "
               "full_drops %llu closed_drops %llu fanout_out %llu fanout_in %llu "
               "rearms %llu\n", i,
               (unsigned long long)per_lane[i].datagrams_received,
               (unsigned long long)per_lane[i].forwarded_out,
               (unsigned long long)per_lane[i].forwarded_in,
               (unsigned long long)per_lane[i].inbox_discarded,
               (unsigned long long)per_lane[i].inbox_full_drops,
               (unsigned long long)per_lane[i].inbox_closed_drops,
               (unsigned long long)per_lane[i].reset_fanout_out,
               (unsigned long long)per_lane[i].reset_fanout_in,
               (unsigned long long)per_lane[i].budget_rearms);
    }
}

/* -- rows -------------------------------------------------------------------- */

static void row_spread(void)
{
    enum { N = 48 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 1;
    atomic_store(&s.deadline_at, UINT64_MAX);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 128, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    CHECK(moq_pq_threaded_lane_count(srv) == LANES);
    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    for (int i = 0; i < N; i++) cli[i] = client_create(port, &c[i]);
    for (struct timespec dl = deadline_in(30); time_before(&dl);) {
        int done = 0;
        for (int i = 0; i < N; i++) {
            if (!cli[i]) { done++; continue; }
            if (atomic_load(&c[i].whole) >= 1) done++;
            else { moq_pq_threaded_wake(cli[i]); }
        }
        if (done == N) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }
    int served = 0;
    for (int i = 0; i < N; i++) served += atomic_load(&c[i].whole) >= 1;
    printf("spread: served %d/%d; owned per lane:", served, N);
    for (uint32_t i = 0; i < LANES; i++) printf(" %d", atomic_load(&s.accepted[i]));
    printf("\n");
    CHECK(served == N);
    CHECK(sum_lanes(s.accepted, LANES) == N);
    for (uint32_t i = 0; i < LANES; i++) CHECK(atomic_load(&s.accepted[i]) > 0);
    /* The counters are read once every lane has exited: a forwarded datagram
     * was then injected or discarded, and a reset candidate offered to the
     * other lanes was injected, refused, or discarded; nothing is in flight. */
    for (int i = 0; i < N; i++) stop(&cli[i]);
    CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);
    static moq_picoquic_threaded_lane_stats_t st[LANES];
    print_stats(srv, LANES, st);
    uint64_t received = 0, fwd_out = 0, fwd_in = 0, discarded = 0, full = 0,
             oversize = 0, fanout_out = 0, fanout_in = 0, det_drops = 0,
             det_discarded = 0;
    for (uint32_t i = 0; i < LANES; i++) {
        CHECK(st[i].datagrams_received + st[i].forwarded_in > 0);
        received += st[i].datagrams_received;
        fwd_out += st[i].forwarded_out;
        fwd_in += st[i].forwarded_in;
        discarded += st[i].inbox_discarded;
        full += st[i].inbox_full_drops;
        oversize += st[i].oversize_drops;
        fanout_out += st[i].reset_fanout_out;
        fanout_in += st[i].reset_fanout_in;
        det_drops += st[i].detection_drops;
        det_discarded += st[i].detection_discarded;
    }
    CHECK(received > 0);
    CHECK(fwd_out > 0);
    CHECK(fwd_out == fwd_in + discarded);
    CHECK(fanout_out * (LANES - 1) == fanout_in + det_drops + det_discarded);
    CHECK(full == 0);
    CHECK(oversize == 0);
    stop(&srv);
}

/* A UDP proxy on loopback: clients dial its port; it relays each client to
 * the server from a socket of that client's own, so the server sees one
 * tuple per client. The first client's socket is replaced after
 * `switch_after` server-to-client datagrams (a NAT rebinding), so the server
 * then sees that client from a new port. */
#define PROXY_MAX_CLIENTS 8

typedef struct {
    struct sockaddr_in addr;    /* the client, on the proxy's client side */
    int sfd;                    /* the socket the server knows it by */
    int from_server;            /* datagrams relayed to it (the proxy thread's) */
    _Atomic int switched;       /* its socket was replaced */
    uint8_t cid_len;            /* its connection ID length: its Initial's source ID */
    /* The first ID the server addressed it with -- after the switch, when
     * the proxy switches: the server takes a fresh client ID for the new
     * tuple, one the client advertised a stateless reset token for. */
    uint8_t dcid[PICOQUIC_CONNECTION_ID_MAX_SIZE];
    _Atomic int dcid_seen;      /* dcid is set (read it after) */
    /* The verifiable group CID the client addresses the server with (the
     * destination ID of its short headers towards the server): the prefix
     * that routes a datagram to the lane that owns this connection. */
    uint8_t server_dcid[PICOQUIC_CONNECTION_ID_MAX_SIZE];
    _Atomic int server_dcid_seen;
} proxy_client_t;

typedef struct {
    int client_port;     /* the proxy's client-facing port */
    int server_port;
    int switch_after;    /* each client's socket is replaced after this many
                          * server datagrams to it; 0: never */
    _Atomic int switched;   /* clients whose socket was replaced */
    _Atomic int stop;
    _Atomic int relayed_from_server;
    pthread_t thread;
    int cfd;
    proxy_client_t clients[PROXY_MAX_CLIENTS];
    _Atomic int nclients;   /* records below it are whole */
} proxy_t;

static int udp_loopback_socket(int port)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) != 0) { close(fd); return -1; }
    return fd;
}

static void *proxy_main(void *arg)
{
    proxy_t *p = arg;
    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server.sin_port = htons((uint16_t)p->server_port);
    uint8_t buf[2048];
    while (!atomic_load(&p->stop)) {
        struct pollfd pfd[1 + PROXY_MAX_CLIENTS];
        int n = atomic_load(&p->nclients);
        int polled = n;
        pfd[0].fd = p->cfd;
        pfd[0].events = POLLIN;
        pfd[0].revents = 0;
        for (int i = 0; i < n; i++) {
            pfd[1 + i].fd = p->clients[i].sfd;
            pfd[1 + i].events = POLLIN;
            pfd[1 + i].revents = 0;
        }
        if (poll(pfd, (nfds_t)(1 + n), 50) <= 0) continue;
        if (pfd[0].revents & POLLIN) {
            struct sockaddr_in from;
            socklen_t fl = sizeof(from);
            ssize_t got = recvfrom(p->cfd, buf, sizeof(buf), 0, (struct sockaddr *)&from, &fl);
            if (got > 0) {
                proxy_client_t *c = NULL;
                for (int i = 0; i < n && !c; i++)
                    if (p->clients[i].addr.sin_port == from.sin_port) c = &p->clients[i];
                if (!c && n < PROXY_MAX_CLIENTS) {
                    c = &p->clients[n];
                    c->addr = from;
                    c->sfd = udp_loopback_socket(0);
                    atomic_store(&p->nclients, ++n);
                }
                if (c && c->sfd >= 0) {
                    /* The client's connection ID length, from the source ID
                     * of its Initial: the server's short headers carry it. */
                    if (c->cid_len == 0 && got > 6 && (buf[0] & 0x80)) {
                        size_t dl = buf[5];
                        if (6 + dl < (size_t)got) c->cid_len = buf[6 + dl];
                    }
                    /* The first verifiable group CID the client addresses the
                     * server with (the destination ID of a short header),
                     * latched before any rebinding so it matches the server's
                     * first record for this connection. */
                    if (!atomic_load(&c->server_dcid_seen) && !(buf[0] & 0x80) &&
                        (size_t)got > 1u + PQ_GROUP_CID_LEN) {
                        memcpy(c->server_dcid, buf + 1, PQ_GROUP_CID_LEN);
                        atomic_store(&c->server_dcid_seen, 1);
                    }
                    (void)sendto(c->sfd, buf, (size_t)got, 0, (struct sockaddr *)&server, sizeof(server));
                }
            }
        }
        for (int i = 0; i < polled; i++) {
            if (!(pfd[1 + i].revents & POLLIN)) continue;
            proxy_client_t *c = &p->clients[i];
            ssize_t got = recv(c->sfd, buf, sizeof(buf), 0);
            if (got <= 0) continue;
            c->from_server++;
            if (!atomic_load(&c->dcid_seen) && !(buf[0] & 0x80) && c->cid_len &&
                (size_t)got > 1u + c->cid_len &&
                (p->switch_after == 0 || atomic_load(&c->switched))) {
                memcpy(c->dcid, buf + 1, c->cid_len);
                atomic_store(&c->dcid_seen, 1);
            }
            (void)sendto(p->cfd, buf, (size_t)got, 0, (struct sockaddr *)&c->addr, sizeof(c->addr));
            atomic_fetch_add(&p->relayed_from_server, 1);
            if (p->switch_after && c->from_server == p->switch_after) {
                /* The rebinding: a new source port towards the server. */
                int nfd = udp_loopback_socket(0);
                if (nfd >= 0) {
                    close(c->sfd);
                    c->sfd = nfd;
                    atomic_store(&c->switched, 1);
                    atomic_fetch_add(&p->switched, 1);
                }
            }
        }
    }
    for (int i = 0; i < atomic_load(&p->nclients); i++)
        if (p->clients[i].sfd >= 0) close(p->clients[i].sfd);
    return NULL;
}

static void row_rebind(void)
{
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 40;
    atomic_store(&s.deadline_at, UINT64_MAX);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 16, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    proxy_t p;
    memset(&p, 0, sizeof(p));
    p.server_port = port;
    p.switch_after = 12;
    p.client_port = pick_free_udp_port();
    p.cfd = udp_loopback_socket(p.client_port);
    CHECK(p.cfd >= 0);
    CHECK(pthread_create(&p.thread, NULL, proxy_main, &p) == 0);
    cli_ctx_t c;
    memset(&c, 0, sizeof(c));
    moq_pq_threaded_t *cli = client_create(p.client_port, &c);
    CHECK(cli != NULL);
    for (struct timespec dl = deadline_in(30); cli && time_before(&dl) &&
         atomic_load(&c.whole) < 40;) {
        moq_pq_threaded_wake(cli);
        moq_pq_threaded_wait(cli, 10000);
    }
    printf("rebind: objects %d/40, proxy switched %d after %d server datagrams, "
           "owned per lane:", atomic_load(&c.whole), atomic_load(&p.switched),
           atomic_load(&p.relayed_from_server));
    for (uint32_t i = 0; i < LANES; i++) printf(" %d", atomic_load(&s.accepted[i]));
    printf("\n");
    CHECK(atomic_load(&c.whole) == 40);
    CHECK(atomic_load(&p.switched) == 1);
    CHECK(sum_lanes(s.accepted, LANES) == 1);   /* the same connection throughout */
    CHECK(moq_pq_threaded_conn_count(srv) == 1);
    stop(&cli);
    atomic_store(&p.stop, 1);
    pthread_join(p.thread, NULL);
    close(p.cfd);
    CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);
    static moq_picoquic_threaded_lane_stats_t st[LANES];
    print_stats(srv, LANES, st);
    uint64_t fwd_out = 0, fwd_in = 0, discarded = 0;
    for (uint32_t i = 0; i < LANES; i++) {
        fwd_out += st[i].forwarded_out;
        fwd_in += st[i].forwarded_in;
        discarded += st[i].inbox_discarded;
    }
    CHECK(fwd_out == fwd_in + discarded);
    stop(&srv);
}

/* A peer that lost state answers the server's packets with stateless
 * resets. Each client's reset reaches the server on whichever lane the
 * kernel picks for that client's tuple; the lane that owns the connection
 * detects it -- through the detection ring when another lane received it --
 * and the connection ends. The clients then learn the same way: their
 * keep-alives reach the owner lane, which no longer knows them and answers
 * each with a reset of its own.
 *
 * A client advertises reset tokens only for the connection IDs it issues
 * after the handshake (RFC 9000 Section 10.3: the token of the initial ID
 * comes from the server's transport parameter alone), so the server must
 * be addressing the client by one of those: every client rebinds through
 * the proxy first, and the server takes a fresh ID for the new tuple. */
static void row_reset(void)
{
    enum { N = PROXY_MAX_CLIENTS, OBJECTS = 20, SWITCH_AFTER = 8 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = OBJECTS;
    atomic_store(&s.deadline_at, UINT64_MAX);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 16, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    static proxy_t p;
    memset(&p, 0, sizeof(p));
    p.server_port = port;
    p.switch_after = SWITCH_AFTER;
    p.client_port = pick_free_udp_port();
    p.cfd = udp_loopback_socket(p.client_port);
    CHECK(p.cfd >= 0);
    CHECK(pthread_create(&p.thread, NULL, proxy_main, &p) == 0);
    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    /* One at a time, so the proxy's record i is client i. */
    for (int i = 0; i < N; i++) {
        atomic_store(&c[i].keep_alive_wanted, 1);
        cli[i] = client_create(p.client_port, &c[i]);
        CHECK(cli[i] != NULL);
        for (struct timespec dl = deadline_in(10);
             cli[i] && time_before(&dl) && atomic_load(&p.nclients) < i + 1;)
            nap_ms(5);
        CHECK(atomic_load(&p.nclients) == i + 1);
    }
    for (struct timespec dl = deadline_in(30); time_before(&dl);) {
        int done = 0;
        for (int i = 0; i < N; i++) {
            if (!cli[i]) { done++; continue; }
            if (atomic_load(&c[i].whole) >= OBJECTS &&
                atomic_load(&p.clients[i].dcid_seen)) done++;
            else moq_pq_threaded_wake(cli[i]);
        }
        if (done == N) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }
    int served = 0;
    for (int i = 0; i < N; i++) served += atomic_load(&c[i].whole) >= OBJECTS;
    CHECK(served == N);
    CHECK(atomic_load(&p.switched) == N);
    CHECK(moq_pq_threaded_conn_count(srv) == N);

    /* One reset per client, from the socket the server knows the client by:
     * a random first byte and padding, then the token the client advertised
     * for the ID the server addresses it with. */
    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server.sin_port = htons((uint16_t)port);
    int sent = 0;
    for (int i = 0; i < N; i++) {
        proxy_client_t *pc = &p.clients[i];
        if (!cli[i] || !atomic_load(&pc->dcid_seen)) continue;
        picoquic_connection_id_t cid;
        memset(&cid, 0, sizeof(cid));
        cid.id_len = pc->cid_len;
        memcpy(cid.id, pc->dcid, pc->cid_len);
        uint8_t reset[64];
        for (size_t k = 0; k < sizeof(reset); k++) reset[k] = (uint8_t)rand();
        reset[0] = 0x40 | (reset[0] & 0x3F);
        CHECK(picoquic_create_cnxid_reset_secret(
                  c[i].quic, &cid,
                  reset + sizeof(reset) - PICOQUIC_RESET_SECRET_SIZE) == 0);
        if (sendto(pc->sfd, reset, sizeof(reset), 0, (struct sockaddr *)&server,
                   sizeof(server)) == (ssize_t)sizeof(reset))
            sent++;
    }
    CHECK(sent == N);

    /* The owner lanes drop the connections. */
    for (struct timespec dl = deadline_in(15);
         time_before(&dl) && moq_pq_threaded_conn_count(srv) > 0;)
        nap_ms(10);
    printf("reset: %d resets sent, %zu server connections left\n", sent,
           moq_pq_threaded_conn_count(srv));
    CHECK(moq_pq_threaded_conn_count(srv) == 0);

    /* The clients' keep-alives find no connection on the owner lane and
     * are answered with a reset: every client's session ends. */
    int closed = 0;
    for (struct timespec dl = deadline_in(15); time_before(&dl);) {
        closed = 0;
        for (int i = 0; i < N; i++) {
            if (!cli[i]) continue;
            if (atomic_load(&c[i].closed) ||
                moq_pq_threaded_wait(cli[i], 0) == MOQ_ERR_CLOSED)
                closed++;
        }
        if (closed == N) break;
        nap_ms(10);
    }
    printf("reset: %d/%d clients closed by the server's resets\n", closed, N);
    CHECK(closed == N);

    for (int i = 0; i < N; i++) stop(&cli[i]);
    atomic_store(&p.stop, 1);
    pthread_join(p.thread, NULL);
    close(p.cfd);
    CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);
    static moq_picoquic_threaded_lane_stats_t st[LANES];
    print_stats(srv, LANES, st);
    uint64_t fwd_out = 0, fwd_in = 0, discarded = 0, fanout_out = 0,
             fanout_in = 0, det_drops = 0, det_discarded = 0;
    for (uint32_t i = 0; i < LANES; i++) {
        fwd_out += st[i].forwarded_out;
        fwd_in += st[i].forwarded_in;
        discarded += st[i].inbox_discarded;
        fanout_out += st[i].reset_fanout_out;
        fanout_in += st[i].reset_fanout_in;
        det_drops += st[i].detection_drops;
        det_discarded += st[i].detection_discarded;
    }
    CHECK(fwd_out == fwd_in + discarded);
    /* Every crafted reset was a candidate on the lane that received it, offered
     * to each other lane; a datagram on a verifiable CID that no longer
     * associates (a client's keep-alive after its connection was reset) also
     * becomes a candidate now, so the count is at least the resets crafted. */
    CHECK(fanout_out >= (uint64_t)sent);
    CHECK(fanout_out * (LANES - 1) == fanout_in + det_drops + det_discarded);
    CHECK(det_drops == 0);
    stop(&srv);
}

/* A stateless reset whose CID prefix verifies to one lane but whose token
 * belongs to a connection another lane owns still ends that connection. The
 * routed lane cannot associate the datagram -- the prefix is a CID it issued
 * but retired once the client rebound -- so it offers it to the other lanes
 * for detection, and the owner recognises its token. Without that, the reset
 * reaches only the routed lane and the real connection lingers. */
static void row_reset_crosslane(void)
{
    enum { N = PROXY_MAX_CLIENTS, OBJECTS = 20, SWITCH_AFTER = 8 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = OBJECTS;
    atomic_store(&s.deadline_at, UINT64_MAX);
    atomic_store(&g_rec_next, 0);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 16, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    static proxy_t p;
    memset(&p, 0, sizeof(p));
    p.server_port = port;
    p.switch_after = SWITCH_AFTER;
    p.client_port = pick_free_udp_port();
    p.cfd = udp_loopback_socket(p.client_port);
    CHECK(p.cfd >= 0);
    CHECK(pthread_create(&p.thread, NULL, proxy_main, &p) == 0);
    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    for (int i = 0; i < N; i++) {
        atomic_store(&c[i].keep_alive_wanted, 1);
        cli[i] = client_create(p.client_port, &c[i]);
        CHECK(cli[i] != NULL);
        for (struct timespec dl = deadline_in(10);
             cli[i] && time_before(&dl) && atomic_load(&p.nclients) < i + 1;)
            nap_ms(5);
        CHECK(atomic_load(&p.nclients) == i + 1);
    }
    /* Establish and rebind every client: each then has a client-issued ID
     * (so a token) and a retired first server CID. */
    for (struct timespec dl = deadline_in(30); time_before(&dl);) {
        int done = 0;
        for (int i = 0; i < N; i++) {
            if (!cli[i]) { done++; continue; }
            if (atomic_load(&c[i].whole) >= OBJECTS &&
                atomic_load(&p.clients[i].dcid_seen) &&
                atomic_load(&p.clients[i].server_dcid_seen)) done++;
            else moq_pq_threaded_wake(cli[i]);
        }
        if (done == N) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }

    /* Map each client to the lane that owns it, by matching the first server
     * CID it addressed to the one the server recorded for that connection. */
    uint32_t cli_lane[N];
    bool known[N];
    for (int i = 0; i < N; i++) {
        known[i] = false;
        if (!atomic_load(&p.clients[i].server_dcid_seen)) continue;
        unsigned nrec = atomic_load(&g_rec_next);
        for (unsigned r = 0; r < nrec; r++) {
            if (atomic_load_explicit(&g_recs[r].server_cid_len,
                                     memory_order_acquire) == (int)PQ_GROUP_CID_LEN &&
                memcmp(g_recs[r].server_cid, p.clients[i].server_dcid,
                       PQ_GROUP_CID_LEN) == 0) {
                cli_lane[i] = atomic_load_explicit(&g_recs[r].lane,
                                                   memory_order_relaxed);
                known[i] = true;
                break;
            }
        }
    }

    /* A victim B and a prefix from a client A on a different lane. */
    int vb = -1, pa = -1;
    for (int i = 0; i < N && vb < 0; i++)
        if (cli[i] && known[i] && atomic_load(&p.clients[i].dcid_seen)) vb = i;
    if (vb >= 0)
        for (int i = 0; i < N; i++)
            if (cli[i] && known[i] && cli_lane[i] != cli_lane[vb]) { pa = i; break; }
    printf("reset_crosslane: victim=%d(lane %d) prefix=%d(lane %d)\n",
           vb, vb >= 0 ? (int)cli_lane[vb] : -1, pa,
           pa >= 0 ? (int)cli_lane[pa] : -1);
    CHECK(vb >= 0 && pa >= 0);   /* two clients on different lanes */

    if (vb >= 0 && pa >= 0) {
        struct sockaddr_in server;
        memset(&server, 0, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        server.sin_port = htons((uint16_t)port);
        picoquic_connection_id_t bcid;
        memset(&bcid, 0, sizeof(bcid));
        bcid.id_len = p.clients[vb].cid_len;
        memcpy(bcid.id, p.clients[vb].dcid, p.clients[vb].cid_len);
        uint8_t reset[64];
        for (size_t k = 0; k < sizeof(reset); k++) reset[k] = (uint8_t)rand();
        reset[0] = 0x40 | (reset[0] & 0x3F);
        /* The prefix verifies to lane A, which retired it; the token is the
         * one B advertised for the ID the server addresses it by. */
        memcpy(reset + 1, p.clients[pa].server_dcid, PQ_GROUP_CID_LEN);
        CHECK(picoquic_create_cnxid_reset_secret(
                  c[vb].quic, &bcid,
                  reset + sizeof(reset) - PICOQUIC_RESET_SECRET_SIZE) == 0);
        ssize_t w = sendto(p.clients[vb].sfd, reset, sizeof(reset), 0,
                           (struct sockaddr *)&server, sizeof(server));
        CHECK(w == (ssize_t)sizeof(reset));

        int closed = 0;
        for (struct timespec dl = deadline_in(15); time_before(&dl);) {
            if (atomic_load(&c[vb].closed) ||
                moq_pq_threaded_wait(cli[vb], 0) == MOQ_ERR_CLOSED) {
                closed = 1;
                break;
            }
            moq_pq_threaded_wake(cli[vb]);
            nap_ms(10);
        }
        printf("reset_crosslane: victim closed=%d\n", closed);
        CHECK(closed == 1);
    }

    for (int i = 0; i < N; i++) stop(&cli[i]);
    atomic_store(&p.stop, 1);
    pthread_join(p.thread, NULL);
    close(p.cfd);
    stop(&srv);
}

/* stop_load's placement: each client-chosen connection ID goes, on first
 * sight, to the next lane in turn, and every later sighting to the same lane,
 * so N clients cover every lane whatever IDs they pick. */
extern uint32_t (*_Atomic moq_pq_threaded_test_owner_override)(
    const uint8_t *dcid, size_t len, uint32_t lanes);
static pthread_mutex_t rr_mu = PTHREAD_MUTEX_INITIALIZER;
static struct { uint8_t id[20]; size_t len; uint32_t lane; } rr_seen[64];
static int rr_n;

static uint32_t rr_owner(const uint8_t *dcid, size_t len, uint32_t lanes)
{
    uint32_t lane;
    pthread_mutex_lock(&rr_mu);
    int i = 0;
    while (i < rr_n && !(rr_seen[i].len == len &&
                         memcmp(rr_seen[i].id, dcid, len) == 0))
        i++;
    if (i < rr_n) {
        lane = rr_seen[i].lane;
    } else if (rr_n < 64 && len <= sizeof(rr_seen[0].id)) {
        lane = (uint32_t)rr_n % lanes;
        memcpy(rr_seen[rr_n].id, dcid, len);
        rr_seen[rr_n].len = len;
        rr_seen[rr_n].lane = lane;
        rr_n++;
    } else {
        lane = (len ? dcid[0] : 0) % lanes;   /* still one lane per ID */
    }
    pthread_mutex_unlock(&rr_mu);
    return lane;
}

static void row_stop_load(void)
{
    enum { N = 16 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 100000;   /* streams until stopped */
    atomic_store(&s.deadline_at, UINT64_MAX);
    pthread_mutex_lock(&rr_mu);
    rr_n = 0;
    pthread_mutex_unlock(&rr_mu);
    atomic_store(&moq_pq_threaded_test_owner_override, rr_owner);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 64, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    for (int i = 0; i < N; i++) cli[i] = client_create(port, &c[i]);
    /* Enough clients streaming, placed on every lane, that the stop finds
     * real work mid-flight: the bounded local close is the invariant, not
     * that all N ramped (N client threads plus the lanes oversubscribe a
     * sanitizer's cores; `spread` is where every client being served is
     * pinned). Placement is the round-robin control above, not the keyed
     * hash, so every lane is given clients; the per-lane accept counts confirm
     * each lane took its share. */
    int streaming = 0;
    for (struct timespec dl = deadline_in(30); time_before(&dl);) {
        streaming = 0;
        for (int i = 0; i < N; i++) {
            if (cli[i] && atomic_load(&c[i].whole) >= 5) streaming++;
            else if (cli[i]) moq_pq_threaded_wake(cli[i]);
        }
        bool every_lane = true;
        for (uint32_t l = 0; l < LANES; l++)
            every_lane &= atomic_load(&s.accepted[l]) > 0;
        if (streaming >= N / 2 && every_lane) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }
    CHECK(streaming >= N / 2);
    for (uint32_t l = 0; l < LANES; l++) CHECK(atomic_load(&s.accepted[l]) > 0);
    uint64_t t0 = mono_us();
    moq_result_t rc = moq_pq_threaded_stop(srv);
    uint64_t took = mono_us() - t0;
    printf("stop_load: %d/%d streaming, stop rc %d in %llu us, objects "
           "written %d, accepted per lane", streaming, N, (int)rc,
           (unsigned long long)took, atomic_load(&s.objects));
    for (uint32_t l = 0; l < LANES; l++)
        printf(" %d", (int)atomic_load(&s.accepted[l]));
    printf("\n");
    CHECK(rc == MOQ_OK);
    CHECK(took < 3000000u);
    moq_pq_threaded_destroy(srv);
    for (int i = 0; i < N; i++) stop(&cli[i]);
    atomic_store(&moq_pq_threaded_test_owner_override, NULL);
}

static void row_deadline(void)
{
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 1;
    atomic_store(&s.deadline_at, UINT64_MAX);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(2, 8, &s, true, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    nap_ms(50);
    int p0 = atomic_load(&s.pumps[0]), p1 = atomic_load(&s.pumps[1]);
    /* picoquic's clock is the monotonic microsecond clock on this platform;
     * the lane deadline is announced with that lane's wake. */
    uint64_t at = mono_us() + 300000u;
    atomic_store(&s.deadline_at, at);
    CHECK(moq_pq_threaded_lane_wake(moq_pq_threaded_lane(srv, 1)) == MOQ_OK);
    for (struct timespec dl = deadline_in(3); time_before(&dl) &&
         atomic_load(&s.due_now[1]) == 0;)
        nap_ms(10);
    uint64_t due = atomic_load(&s.due_now[1]);
    nap_ms(300);
    int p1_after = atomic_load(&s.pumps[1]);
    printf("deadline: lane 1 due pump %llu us after the instant; lane 1 pumps %d -> %d; "
           "lane 0 pumps %d -> %d\n", due ? (unsigned long long)(due - at) : 0ull,
           p1, p1_after, p0, atomic_load(&s.pumps[0]));
    CHECK(due != 0 && due >= at);
    CHECK(atomic_load(&s.due_now[0]) == 0);          /* lane 0 never pumped for it */
    CHECK(p1_after - p1 <= 4);                        /* once, not without pause */
    stop(&srv);
}

/* The one-lane facade (no routing plane) honours app_lane_deadline_us for its
 * single lane, index 0, exactly as the header promises. */
static void row_deadline_one(void)
{
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    atomic_store(&s.deadline_at, UINT64_MAX);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (!port) continue;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.port = port;
        cfg.lane_count = 1;                 /* one-lane facade, not a group */
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = &s;
        cfg.app_lane_deadline_us = lane0_deadline_fn;
        cfg.app_lane_deadline_ctx = &s;
        if (moq_pq_threaded_create(&cfg, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;
    CHECK(moq_pq_threaded_lane_count(srv) == 1);
    nap_ms(50);
    int p0 = atomic_load(&s.pumps[0]);
    uint64_t at = mono_us() + 300000u;
    atomic_store(&s.deadline_at, at);
    CHECK(moq_pq_threaded_wake(srv) == MOQ_OK);
    for (struct timespec dl = deadline_in(3); time_before(&dl) &&
         atomic_load(&s.due_now[0]) == 0;)
        nap_ms(10);
    uint64_t due = atomic_load(&s.due_now[0]);
    nap_ms(300);
    int p0_after = atomic_load(&s.pumps[0]);
    printf("deadline_one: lane 0 due pump %llu us after the instant; pumps %d -> %d\n",
           due ? (unsigned long long)(due - at) : 0ull, p0, p0_after);
    CHECK(due != 0 && due >= at);        /* the one-lane facade woke for it */
    CHECK(p0_after - p0 <= 16);          /* a few idle pumps, not a spin (a
                                          * past deadline re-offered every turn
                                          * pumps thousands of times) */
    stop(&srv);
}

static void row_admission(void)
{
    enum { N = 6 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 1;
    atomic_store(&s.deadline_at, UINT64_MAX);
    int port = 0;
    moq_pq_threaded_t *srv = server_on_free_port(LANES, 3, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    for (int i = 0; i < N; i++) cli[i] = client_create(port, &c[i]);
    for (struct timespec dl = deadline_in(10); time_before(&dl);) {
        int served = 0;
        for (int i = 0; i < N; i++) {
            if (cli[i] && atomic_load(&c[i].whole) >= 1) served++;
            else if (cli[i]) moq_pq_threaded_wake(cli[i]);
        }
        if (served >= 3) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }
    nap_ms(300);
    int served = 0;
    for (int i = 0; i < N; i++) served += atomic_load(&c[i].whole) >= 1;
    printf("admission: served %d of %d with a total of 3; accepted %d; live %zu\n",
           served, N, sum_lanes(s.accepted, LANES), moq_pq_threaded_conn_count(srv));
    CHECK(served == 3);
    CHECK(sum_lanes(s.accepted, LANES) == 3);
    CHECK(moq_pq_threaded_conn_count(srv) <= 3);
    for (int i = 0; i < N; i++) stop(&cli[i]);
    stop(&srv);
}

/* -- oom: construction unwinds cleanly however far it gets ------------------ */

typedef struct {
    const moq_alloc_t *base;
    _Atomic long count;   /* allocation attempts so far */
    _Atomic long live;    /* blocks currently held */
    long fail_at;         /* fail the Nth attempt; 0 never fails */
} fp_alloc_t;

static void *fp_alloc(size_t size, void *ctx)
{
    fp_alloc_t *f = ctx;
    if (f->fail_at && atomic_fetch_add(&f->count, 1) + 1 == f->fail_at)
        return NULL;
    void *p = f->base->alloc(size, f->base->ctx);
    if (p) atomic_fetch_add(&f->live, 1);
    return p;
}

static void *fp_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    fp_alloc_t *f = ctx;
    if (f->fail_at && atomic_fetch_add(&f->count, 1) + 1 == f->fail_at)
        return NULL;
    void *p = f->base->realloc(ptr, old_size, new_size, f->base->ctx);
    if (p && !ptr) atomic_fetch_add(&f->live, 1);                 /* was alloc */
    else if (!p && ptr && new_size == 0) atomic_fetch_sub(&f->live, 1); /* freed */
    return p;
}

static void fp_free(void *ptr, size_t size, void *ctx)
{
    fp_alloc_t *f = ctx;
    if (ptr) atomic_fetch_sub(&f->live, 1);
    f->base->free(ptr, size, f->base->ctx);
}

/* Open descriptors below a small ceiling; a clean unwind of a failed
 * construction returns to the count before it, leaking no socket or pipe. */
static int open_fds(void)
{
    int n = 0;
    for (int fd = 0; fd < 256; fd++)
        if (fcntl(fd, F_GETFD) != -1) n++;
    return n;
}

static void row_oom(void)
{
    static srv_ctx_t s;
    /* Fail construction's Nth allocation for every N until it stops failing:
     * each attempt must report the failure, leave no facade behind (*out
     * NULL), start no thread, free every block it took, and leak no
     * descriptor. The lane array, and every lane's inbox and detect ring, must
     * unwind even when some were never initialised. */
    int fd_before = open_fds();
    for (long n = 1; n <= 64; n++) {
        memset(&s, 0, sizeof(s));
        atomic_store(&s.deadline_at, UINT64_MAX);
        fp_alloc_t fp;
        fp.base = moq_alloc_default();
        atomic_store(&fp.count, 0);
        atomic_store(&fp.live, 0);
        fp.fail_at = n;
        moq_alloc_t alloc = { .ctx = &fp, .alloc = fp_alloc,
                              .realloc = fp_realloc, .free = fp_free };

        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.port = pick_free_udp_port();
        cfg.lane_count = 2;
        cfg.max_connections = 8;
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = &s;

        moq_pq_threaded_t *srv = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &srv);
        if (rc == MOQ_OK) {
            /* N is past the last allocation: a real group came up. Tear it
             * down and confirm the sweep covered every failure point. */
            CHECK(srv != NULL);
            stop(&srv);
            CHECK(atomic_load(&fp.live) == 0);
            CHECK(open_fds() == fd_before);
            printf("oom: clean unwind through %ld allocations\n", n - 1);
            return;
        }
        CHECK(srv == NULL);
        CHECK(atomic_load(&fp.live) == 0);
        CHECK(open_fds() == fd_before);
    }
    CHECK(!"construction never succeeded within the sweep");
}

/* -- invariants: the group re-asserts its context invariants after a hook ---- */

/* A configure hook that moves every group-owned invariant the wrong way; the
 * group must re-assert each after it returns. Runs on the create thread
 * (before any lane thread), so the capture is single-threaded. */
static int sabotage_quic(picoquic_quic_t *quic, void *vctx)
{
    picoquic_set_default_connection_id_length(quic, 8);
    picoquic_set_stateless_reset_sending(quic, 0);
    picoquic_set_accept_0rtt(quic, 1);
    picoquic_set_default_tp_value(quic, picoquic_tp_max_packet_size, 4096);
    *(picoquic_quic_t **)vctx = quic;   /* last lane wins; all are identical */
    return 0;
}

static void row_invariants(void)
{
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    atomic_store(&s.deadline_at, UINT64_MAX);
    picoquic_quic_t *quic = NULL;
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (!port) continue;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.port = port;
        cfg.lane_count = 2;                 /* a group: the invariants apply */
        cfg.max_connections = 8;
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = &s;
        cfg.configure_quic = sabotage_quic;
        cfg.configure_quic_ctx = &quic;
        if (moq_pq_threaded_create(&cfg, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;
    CHECK(quic != NULL);
    /* No client connects: the lanes are idle, so these write-once context
     * settings are not being touched by a lane thread while we read them. */
    if (quic) {
        unsigned early = (unsigned)((ptls_context_t *)quic->tls_master_ctx)
                             ->max_early_data_size;
        printf("invariants: cid_len=%u reset_sending=%d early_data=%u "
               "max_packet=%u\n",
               quic->local_cnxid_length, !quic->is_stateless_reset_disabled,
               early, quic->default_tp.max_packet_size);
        CHECK(quic->local_cnxid_length == PQ_GROUP_CID_LEN);  /* routed length */
        CHECK(quic->is_stateless_reset_disabled == 0);        /* reset sending on */
        CHECK(early == 0);                                    /* early data refused */
        CHECK(quic->default_tp.max_packet_size == PICOQUIC_MAX_PACKET_SIZE);
    }
    stop(&srv);
}

static void row_limits(void)
{
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    atomic_store(&s.deadline_at, UINT64_MAX);
    moq_pq_threaded_t *srv = NULL;
    int port = pick_free_udp_port();
    CHECK(server_create(port, MOQ_PQ_THREADED_MAX_LANES + 1, 8, &s, false, &srv) == MOQ_ERR_INVAL);
    CHECK(srv == NULL);
    {
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.lane_count = 2;
        cfg.on_lane_pump = client_pump;
        moq_pq_threaded_t *cli = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &cli) == MOQ_ERR_UNSUPPORTED);
    }
    srv = server_on_free_port(LANES, 8, &s, false, &port);
    CHECK(srv != NULL);
    if (!srv) return;
    CHECK(moq_pq_threaded_lane_count(srv) == LANES);
    CHECK(moq_pq_threaded_lane(srv, LANES) == NULL);
    CHECK(moq_pq_threaded_lane_index(moq_pq_threaded_lane(srv, 3)) == 3);
    CHECK(moq_pq_threaded_session(srv) == NULL);
    CHECK(moq_pq_threaded_conn(srv) == NULL);
    CHECK(moq_pq_threaded_negotiated_version(srv) == 0);
    CHECK(moq_pq_threaded_conn_count(srv) == 0);
    /* drained: every lane has pumped since the last wake, with nothing to flush */
    CHECK(moq_pq_threaded_wake(srv) == MOQ_OK);
    for (struct timespec dl = deadline_in(3); time_before(&dl) &&
         moq_pq_threaded_drain_state(srv) != 1;)
        nap_ms(10);
    CHECK(moq_pq_threaded_drain_state(srv) == 1);
    printf("limits: group of %u up, drained\n", moq_pq_threaded_lane_count(srv));
    /* the stats record: the floor, the stamp, and an untouched tail */
    struct {
        moq_picoquic_threaded_lane_stats_t rec;
        uint64_t tail;
    } big;
    memset(&big, 0xAB, sizeof(big));
    moq_pq_threaded_lane_t *l = moq_pq_threaded_lane(srv, 0);
    CHECK(moq_picoquic_threaded_lane_get_stats(l, &big.rec, MOQ_PICOQUIC_THREADED_LANE_STATS_V0_SIZE - 1) == MOQ_ERR_INVAL);
    CHECK(big.rec.struct_size == 0xABABABABu);
    CHECK(moq_picoquic_threaded_lane_get_stats(l, &big.rec, sizeof(big)) == MOQ_OK);
    CHECK(big.rec.struct_size == sizeof(moq_picoquic_threaded_lane_stats_t));
    CHECK(big.rec.reserved0 == 0);
    CHECK(big.tail == 0xABABABABABABABABull);
    stop(&srv);
    /* a one-lane facade has no routing plane */
    srv = server_on_free_port(1, 8, &s, false, &port);
    CHECK(srv != NULL);
    if (srv) {
        CHECK(moq_picoquic_threaded_lane_get_stats(moq_pq_threaded_lane(srv, 0), &big.rec, sizeof(big.rec)) == MOQ_ERR_UNSUPPORTED);
        stop(&srv);
    }
}

/* -- bind_specific: a lane bound to one address sends from it (BSD source) ---- */

/* A server group bound to a specific 127.0.0.1 address, not the wildcard, so its
 * lane sockets are "specific" and the lane send path must set the packet source
 * the way the host accepts: a socket bound to one address rejects an explicit
 * source control message on BSD (IP_SENDSRCADDR, EINVAL). Clients must still
 * complete the handshake and receive whole objects; a wrongly set source drops
 * every server datagram and nothing arrives. */
static void row_bind_specific(void)
{
    enum { N = 8 };
    static srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.objects_per_sub = 1;
    atomic_store(&s.deadline_at, UINT64_MAX);

    struct sockaddr_in loopback;
    memset(&loopback, 0, sizeof(loopback));
    loopback.sin_family = AF_INET;
    loopback.sin_addr.s_addr = htonl(INADDR_LOOPBACK);   /* 127.0.0.1 */

    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (!port) continue;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.port = port;
        cfg.lane_count = 2;                 /* a group: the lane send path applies */
        cfg.max_connections = 16;
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = &s;
        cfg.send_request_capacity = true;
        cfg.streaming_objects = false;
        cfg.bind_address = (struct sockaddr *)&loopback;   /* a specific bind */
        if (moq_pq_threaded_create(&cfg, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;

    static cli_ctx_t c[N];
    moq_pq_threaded_t *cli[N];
    memset(c, 0, sizeof(c));
    for (int i = 0; i < N; i++) cli[i] = client_create(port, &c[i]);
    for (struct timespec dl = deadline_in(30); time_before(&dl);) {
        int done = 0;
        for (int i = 0; i < N; i++) {
            if (!cli[i]) { done++; continue; }
            if (atomic_load(&c[i].whole) >= 1) done++;
            else moq_pq_threaded_wake(cli[i]);
        }
        if (done == N) break;
        moq_pq_threaded_wait(cli[0], 10000);
    }
    int served = 0;
    for (int i = 0; i < N; i++) served += atomic_load(&c[i].whole) >= 1;
    printf("bind_specific: served %d/%d over a specific 127.0.0.1 bind\n", served, N);
    CHECK(served == N);
    for (int i = 0; i < N; i++) stop(&cli[i]);
    stop(&srv);
}

int main(int argc, char **argv)
{
    const char *row = argc > 1 ? argv[1] : "spread";
    setvbuf(stdout, NULL, _IONBF, 0);
    if (strcmp(row, "spread") == 0) row_spread();
    else if (strcmp(row, "rebind") == 0) row_rebind();
    else if (strcmp(row, "reset") == 0) row_reset();
    else if (strcmp(row, "reset_crosslane") == 0) row_reset_crosslane();
    else if (strcmp(row, "stop_load") == 0) row_stop_load();
    else if (strcmp(row, "deadline") == 0) row_deadline();
    else if (strcmp(row, "deadline_one") == 0) row_deadline_one();
    else if (strcmp(row, "admission") == 0) row_admission();
    else if (strcmp(row, "limits") == 0) row_limits();
    else if (strcmp(row, "invariants") == 0) row_invariants();
    else if (strcmp(row, "oom") == 0) row_oom();
    else if (strcmp(row, "bind_specific") == 0) row_bind_specific();
    else {
        fprintf(stderr, "unknown row %s\n", row);
        return 2;
    }
    if (failures) {
        printf("FAILED: %d check(s) [threaded_lane_group %s]\n", failures, row);
        return 1;
    }
    printf("PASS: threaded_lane_group %s\n", row);
    return 0;
}
