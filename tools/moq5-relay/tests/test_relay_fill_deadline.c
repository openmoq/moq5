/*
 * A held fill on a quiet track ends at its deadline, over the real serve.
 *
 * On draft-22 the relay holds a fill at a gap it has evidence for until the
 * request's FILL_TIMEOUT runs out, then writes an End of Timed-Out Range and
 * ends the stream. A relay may wait less than the subscriber asked, never
 * more. When the track has gone quiet nothing else wakes the lane that holds
 * the fill, so the deadline itself has to: this test runs the PRODUCTION
 * serve composition (cmd_serve at one lane, cmd_serve_lanes at several, and
 * the dual listener with the fill on the WebTransport facade) over real
 * localhost QUIC and checks that the fill ends inside its timeout plus a
 * small tolerance, with no other traffic to carry it there.
 *
 * The scenario: a publisher sends Objects {0,0} and {0,2} of group 0 and then
 * falls silent, so {0,1} is a hole the later Object evidences. A priming
 * subscriber pulls the track through the relay so it is retained. A second
 * subscriber then subscribes at the live edge with a fill over {0,0}..Largest
 * and FILL_TIMEOUT = FD_TIMEOUT_MS. The fill must deliver {0,0} at once, then
 * after the timeout one Timed-Out marker at {0,1}, then {0,2}, then FIN.
 *
 * The answer does not depend on which shard the fill subscriber lands on.
 * Two scenarios put it on a shard that has ingested nothing of the track
 * (the publisher and the priming subscriber elsewhere). That shard retrieves
 * the range from the shard that owns the track and answers exactly as the
 * owner would: {0,0} at once, {0,1} Timed-Out at the timeout, {0,2}, FIN.
 *
 * Clients are managed clients whose protocol logic runs inside their own lane
 * pumps; the main thread only correlates atomics. Once the fill subscriber is
 * released nothing pokes the relay or the clients, so the only way the fill
 * can end on time is the relay's own wait honouring the deadline.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "../cli/config.h"

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#ifdef MOQR_DUAL_LISTENER
#include <moq/wtquic_msquic_managed.h>
#endif

#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "logevent.h"

extern int moqr_test_cmd_serve(const moqr_cli_config_t *cfg);
extern int moqr_test_cmd_serve_lanes(const moqr_cli_config_t *cfg);
extern void moqr_test_serve_log_set_io(const moqr_cli_log_io_t *io);
extern void moqr_test_raise_stop(void);

static int g_failures;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); \
    g_failures++; } } while (0)

/* The requested FILL_TIMEOUT, the slack allowed past it for the relay's
 * wake, the write and the delivery over loopback, and how long the test
 * waits before calling the fill stuck. The last is several times the timeout:
 * a relay that ends the fill only at some later unrelated pump misses it. */
#define FD_TIMEOUT_MS   300u
#define FD_TOLERANCE_MS 250u
#define FD_STUCK_MS     (6u * FD_TIMEOUT_MS)
/* Setup phases (handshakes, the namespace, the priming subscription) are not
 * what is measured; their bound only stops a broken run from hanging. */
#define FD_SETUP_MS     15000u

#define FD_NS    "filldl"
#define FD_TRACK "t"

static uint64_t
mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static void
nap_ms(unsigned ms)
{
    struct timespec ts = { .tv_sec = 0, .tv_nsec = (long)ms * 1000000L };
    nanosleep(&ts, NULL);
}

/* -- the serve, on its own thread ------------------------------------------ */

/* The serve log's readiness flush is the point every listener is up: in text
 * mode it is the first flush the sink makes. */
static atomic_int g_ready;

static size_t
io_write(void *ctx, FILE *f, const char *p, size_t n)
{
    (void)ctx;
    return fwrite(p, 1, n, f);
}

static int
io_flush(void *ctx, FILE *f)
{
    (void)ctx;
    int rc = fflush(f);
    atomic_store(&g_ready, 1);
    return rc;
}

static const moqr_cli_log_io_t kServeIo = {
    .clock_us = NULL,
    .write = io_write,
    .flush = io_flush,
    .ctx = NULL,
};

typedef struct serve_run {
    moqr_cli_config_t cfg;
    bool              lanes;
    int               rc;
} serve_run_t;

static void *
serve_main(void *arg)
{
    serve_run_t *r = arg;
    r->rc = r->lanes ? moqr_test_cmd_serve_lanes(&r->cfg)
                     : moqr_test_cmd_serve(&r->cfg);
    return NULL;
}

/* A UDP port no IPv4 socket holds on ANY local address. The serve config
 * names fixed ports; asking the kernel for one keeps parallel test runs from
 * colliding. The relay's MsQuic listener binds the wildcard whatever host it
 * is configured with (adapters/common/moq_listen_port.h), so the pick must be
 * free on the wildcard: a port free on loopback alone can still be held by a
 * socket bound to another local address, and an explicit port is never
 * replaced or retried. */
static uint16_t
free_udp_port(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return 0;
    }
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = 0;
    socklen_t len = sizeof(a);
    uint16_t port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0) {
        port = ntohs(a.sin_port);
    }
    close(fd);
    return port;
}

/* -- the client peers ------------------------------------------------------ */

typedef enum { ROLE_PUB, ROLE_PRIME, ROLE_SPACER, ROLE_FILL } role_t;

typedef struct peer {
    role_t         role;
    /* lane-thread state */
    bool           announced;
    bool           accepted;
    bool           wrote;
    bool           subscribed;
    moq_subscription_t up_sub;
    /* main thread -> lane */
    atomic_int     go;               /* ROLE_FILL: subscribe now          */
    /* lane -> main thread */
    atomic_int     established;
    atomic_int     ns_accepted;      /* ROLE_PUB                          */
    atomic_int     published;        /* ROLE_PUB: both Objects written    */
    atomic_int     objects;          /* ROLE_PRIME: subscription Objects  */
    atomic_int     fatal;
    /* ROLE_FILL */
    atomic_uint_least64_t t_sub_us;
    atomic_uint_least64_t t_first_us;   /* first fill Object              */
    atomic_uint_least64_t t_term_us;    /* fill terminal                  */
    atomic_int     fill_objects;
    atomic_int     objects_at_gap;   /* fill Objects seen before the gap */
    atomic_int     fill_gaps;
    atomic_uint    gap_kind;
    atomic_uint_least64_t gap_group, gap_object;
    atomic_uint_least64_t last_obj_group, last_obj_id;
    atomic_int     terminals;
    atomic_uint    term_reason;
    atomic_int     live_objects;
} peer_t;

static moq_namespace_t
fd_ns(moq_bytes_t *part)
{
    *part = MOQ_BYTES_LITERAL(FD_NS);
    return (moq_namespace_t){ .parts = part, .count = 1 };
}

static void
pub_write(peer_t *p, moq_session_t *s, uint64_t now_us)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(s, p->up_sub, &sgc, now_us, &sg) !=
        MOQ_OK) {
        atomic_store(&p->fatal, 1);
        return;
    }
    /* {0,0} and {0,2}: Object 1 is never sent, and Object 2 is what makes
     * its absence evidence the relay can wait on */
    static const uint64_t ids[2] = { 0, 2 };
    for (size_t i = 0; i < 2; i++) {
        uint8_t body[8];
        memset(body, (int)(0xA0 + ids[i]), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) !=
                MOQ_OK ||
            moq_session_write_object(s, sg, ids[i], pl, now_us) != MOQ_OK) {
            atomic_store(&p->fatal, 1);
        }
        if (pl != NULL) {
            moq_rcbuf_decref(pl);
        }
    }
    if (moq_session_close_subgroup(s, sg, now_us) != MOQ_OK) {
        atomic_store(&p->fatal, 1);
    }
    p->wrote = true;
    atomic_store(&p->published, 1);
}

static void
peer_subscribe(peer_t *p, moq_session_t *s, uint64_t now_us)
{
    moq_bytes_t part;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = fd_ns(&part);
    sc.track_name = MOQ_BYTES_LITERAL(FD_TRACK);
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    moq_location_filter_cfg_init(&floc);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    if (p->role == ROLE_PRIME) {
        /* everything from {0,0}: pulls the track through the relay */
        loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
        sc.location = &loc;
    } else {
        /* the live edge, with a fill over {0,0}..Largest */
        loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
        floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
        fill.location = &floc;
        fill.has_fill_timeout = true;
        fill.fill_timeout_ms = FD_TIMEOUT_MS;
        sc.location = &loc;
        sc.fill = &fill;
    }
    moq_subscription_t h;
    moq_result_t rc = moq_session_subscribe(s, &sc, now_us, &h);
    if (rc == MOQ_OK) {
        p->subscribed = true;
        if (p->role == ROLE_FILL) {
            atomic_store(&p->t_sub_us, now_us);
        }
    } else if (rc != MOQ_ERR_WOULD_BLOCK) {
        atomic_store(&p->fatal, 1);
    }
}

static void
peer_drive(peer_t *p, moq_session_t *s, uint64_t now_us)
{
    if (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        return;
    }
    atomic_store(&p->established, 1);
    if (p->role == ROLE_PUB && !p->announced) {
        moq_bytes_t part;
        moq_publish_namespace_cfg_t pc;
        moq_publish_namespace_cfg_init(&pc);
        pc.track_namespace = fd_ns(&part);
        moq_announcement_t ann;
        moq_result_t rc = moq_session_publish_namespace(s, &pc, now_us, &ann);
        if (rc == MOQ_OK) {
            p->announced = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK) {
            atomic_store(&p->fatal, 1);
        }
    }
    if (p->role == ROLE_PRIME && !p->subscribed) {
        peer_subscribe(p, s, now_us);
    }
    if (p->role == ROLE_FILL && !p->subscribed && atomic_load(&p->go)) {
        peer_subscribe(p, s, now_us);
    }

    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                atomic_store(&p->ns_accepted, 1);
                break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                if (p->role == ROLE_PUB && !p->accepted) {
                    moq_accept_subscribe_cfg_t ac;
                    moq_accept_subscribe_cfg_init(&ac);
                    if (moq_session_accept_subscribe(
                            s, ev->u.subscribe_request.sub, &ac, now_us) ==
                        MOQ_OK) {
                        p->up_sub = ev->u.subscribe_request.sub;
                        p->accepted = true;
                    } else {
                        atomic_store(&p->fatal, 1);
                    }
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                /* the relay refuses until the namespace has a route; the
                 * priming subscriber asks again on its next pump */
                if (p->role == ROLE_PRIME) {
                    p->subscribed = false;
                } else {
                    atomic_store(&p->fatal, 1);
                }
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                if (p->role == ROLE_PRIME) {
                    atomic_fetch_add(&p->objects, 1);
                } else {
                    atomic_fetch_add(&p->live_objects, 1);
                }
                break;
            case MOQ_EVENT_FILL_OBJECT:
                if (atomic_fetch_add(&p->fill_objects, 1) == 0) {
                    atomic_store(&p->t_first_us, now_us);
                }
                atomic_store(&p->last_obj_group, ev->u.fill_object.group_id);
                atomic_store(&p->last_obj_id, ev->u.fill_object.object_id);
                break;
            case MOQ_EVENT_FILL_GAP:
                atomic_store(&p->objects_at_gap,
                             atomic_load(&p->fill_objects));
                atomic_fetch_add(&p->fill_gaps, 1);
                atomic_store(&p->gap_kind, ev->u.fill_gap.kind);
                atomic_store(&p->gap_group, ev->u.fill_gap.group_id);
                atomic_store(&p->gap_object, ev->u.fill_gap.object_id);
                break;
            case MOQ_EVENT_FILL_TERMINAL:
                atomic_store(&p->term_reason, ev->u.fill_terminal.reason);
                atomic_store(&p->t_term_us, now_us);
                atomic_fetch_add(&p->terminals, 1);
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
    if (p->role == ROLE_PUB && p->accepted && !p->wrote) {
        pub_write(p, s, now_us);
    }
}

static int
raw_client_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *vctx)
{
    (void)lane;
    peer_drive(vctx, moq_msquic_managed_session(m), now_us);
    return 0;
}

static moq_msquic_managed_t *
raw_client(uint16_t port, peer_t *p)
{
    moq_msquic_managed_cfg_t c;
    moq_msquic_managed_cfg_init_sized(&c, sizeof(c));
    c.alloc = moq_alloc_default();
    c.perspective = MOQ_PERSPECTIVE_CLIENT;
    c.host = "127.0.0.1";
    c.port = port;
    c.version = MOQ_VERSION_DRAFT_22;
    c.insecure_skip_verify = true;
    c.on_lane_pump = raw_client_pump;
    c.on_lane_pump_user = p;
    moq_msquic_managed_t *t = NULL;
    if (moq_msquic_managed_create(&c, &t) != MOQ_OK) {
        return NULL;
    }
    return t;
}

#ifdef MOQR_DUAL_LISTENER
static int
wt_client_pump(moq_wtquic_msquic_managed_t *m,
               moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
               void *vctx)
{
    (void)lane;
    peer_drive(vctx, moq_wtquic_msquic_managed_session(m), now_us);
    return 0;
}

static moq_wtquic_msquic_managed_t *
wt_client(uint16_t port, peer_t *p)
{
    static const char *const kProtos[1] = { "moqt-22" };
    moq_wtquic_msquic_managed_cfg_t c;
    moq_wtquic_msquic_managed_cfg_init_sized(&c, sizeof(c));
    c.alloc = moq_alloc_default();
    c.perspective = MOQ_PERSPECTIVE_CLIENT;
    c.host = "127.0.0.1";
    c.port = port;
    c.insecure_skip_verify = true;
    c.wt_path = "/moq";
    c.wt_protocols = kProtos;
    c.wt_protocol_count = 1;
    c.on_lane_pump = wt_client_pump;
    c.on_lane_pump_user = p;
    moq_wtquic_msquic_managed_t *t = NULL;
    if (moq_wtquic_msquic_managed_create(&c, &t) != MOQ_OK) {
        return NULL;
    }
    return t;
}
#endif

/* One client connection, on whichever facade the scenario puts it. */
typedef struct client {
    moq_msquic_managed_t *raw;
#ifdef MOQR_DUAL_LISTENER
    moq_wtquic_msquic_managed_t *wt;
#endif
} client_t;

static bool
client_open(client_t *c, bool webtransport, uint16_t port, peer_t *p)
{
    memset(c, 0, sizeof(*c));
    if (!webtransport) {
        c->raw = raw_client(port, p);
        return c->raw != NULL;
    }
#ifdef MOQR_DUAL_LISTENER
    c->wt = wt_client(port, p);
    return c->wt != NULL;
#else
    return false;
#endif
}

static void
client_wake(client_t *c)
{
    if (c->raw != NULL) {
        (void)moq_msquic_managed_wake(c->raw);
    }
#ifdef MOQR_DUAL_LISTENER
    if (c->wt != NULL) {
        (void)moq_wtquic_msquic_managed_wake(c->wt);
    }
#endif
}

static void
client_close(client_t *c)
{
    if (c->raw != NULL) {
        (void)moq_msquic_managed_stop(c->raw);
        moq_msquic_managed_destroy(c->raw);
        c->raw = NULL;
    }
#ifdef MOQR_DUAL_LISTENER
    if (c->wt != NULL) {
        (void)moq_wtquic_msquic_managed_stop(c->wt);
        moq_wtquic_msquic_managed_destroy(c->wt);
        c->wt = NULL;
    }
#endif
}

/* Poll `flag` until it is set or `ms` pass, waking the given clients so their
 * setup logic keeps moving. Used only before the measured interval. */
static bool
await_flag(atomic_int *flag, unsigned ms, client_t *cl, size_t n)
{
    uint64_t until = mono_us() + (uint64_t)ms * 1000u;
    while (atomic_load(flag) == 0) {
        if (mono_us() >= until) {
            return false;
        }
        for (size_t i = 0; i < n; i++) {
            client_wake(&cl[i]);
        }
        nap_ms(5);
    }
    return true;
}

/* -- the scenario ---------------------------------------------------------- */

typedef enum {
    MODE_SINGLE,
    MODE_LANES,
    MODE_DUAL_WT,
    /* The fill subscriber's shard does not hold the track. */
    MODE_DUAL_SPLIT,        /* publisher + priming on raw, fill on WT     */
    MODE_LANES_FILL_FIRST   /* two raw lanes, fill accepted before priming */
} fd_mode_t;

typedef enum {
    SETUP_NORMAL,
    SETUP_REFUSE_CONFIG,
    SETUP_REFUSE_THREAD
} fd_setup_t;

static bool
fd_dual(fd_mode_t mode)
{
    return mode == MODE_DUAL_WT || mode == MODE_DUAL_SPLIT;
}

static void
dump_peer(const char *name, peer_t *p)
{
    fprintf(stderr,
            "  %s: established=%d ns_accepted=%d published=%d objects=%d "
            "fill_objects=%d fill_gaps=%d terminals=%d fatal=%d\n",
            name, atomic_load(&p->established), atomic_load(&p->ns_accepted),
            atomic_load(&p->published), atomic_load(&p->objects),
            atomic_load(&p->fill_objects), atomic_load(&p->fill_gaps),
            atomic_load(&p->terminals), atomic_load(&p->fatal));
}

static int
run(const char *cert, const char *key, fd_mode_t mode, fd_setup_t setup)
{
    /* Refusal controls stop before creating any listener or worker. */
    uint16_t raw_port = setup == SETUP_NORMAL ? free_udp_port() : 12345;
    uint16_t wt_port = fd_dual(mode) ? free_udp_port() : 0;
    CHECK(raw_port != 0);
    CHECK(!fd_dual(mode) || (wt_port != 0 && wt_port != raw_port));
    if (g_failures != 0) {
        return 1;
    }

    static serve_run_t sr;
    memset(&sr, 0, sizeof(sr));
    char json[2048];
    if (fd_dual(mode)) {
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":%u,"
                 "\"cert\":\"%s\",\"key\":\"%s\",\"lanes\":1,"
                 "\"versions\":[22]},"
                 "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":%u,"
                 "\"cert\":\"%s\",\"key\":\"%s\",\"lanes\":1,"
                 "\"versions\":[22]}}",
                 (unsigned)raw_port, cert, key, (unsigned)wt_port, cert, key);
    } else {
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":%u,"
                 "\"cert\":\"%s\",\"key\":\"%s\",\"lanes\":%u,"
                 "\"versions\":[22]}}",
                 (unsigned)raw_port, cert, key,
                 mode == MODE_SINGLE ? 1u : 2u);
    }
    char err[256] = { 0 };
    const char *config_json = setup == SETUP_REFUSE_CONFIG ? "{" : json;
    if (moqr_cli_config_parse(config_json, strlen(config_json), &sr.cfg, err,
                              sizeof(err)) != MOQR_OK) {
        fprintf(stderr, "FAIL: config refused: %s\n", err);
        return 1;
    }
    /* the composition under test is the one main() would pick */
    sr.lanes = moqr_cli_total_lanes(&sr.cfg) > 1;
    CHECK(sr.lanes == (mode != MODE_SINGLE));

    atomic_store(&g_ready, 0);
    moqr_test_serve_log_set_io(&kServeIo);
    pthread_t th;
    if (setup == SETUP_REFUSE_THREAD ||
        pthread_create(&th, NULL, serve_main, &sr) != 0) {
        fprintf(stderr, "FAIL: serve thread\n");
        moqr_test_serve_log_set_io(NULL);
        return 1;
    }
    bool ready = await_flag(&g_ready, FD_SETUP_MS, NULL, 0);
    CHECK(ready);

    static peer_t pub, prime, spacer, filler;
    memset(&pub, 0, sizeof(pub));
    memset(&prime, 0, sizeof(prime));
    memset(&spacer, 0, sizeof(spacer));
    memset(&filler, 0, sizeof(filler));
    pub.role = ROLE_PUB;
    prime.role = ROLE_PRIME;
    spacer.role = ROLE_SPACER;
    filler.role = ROLE_FILL;

    /*
     * Placement. The raw facade hands accepted connections to its lanes
     * round-robin, so the clients connect one at a time, each after the one
     * before it is established, and the order decides the shards.
     *
     * In the first three scenarios the fill subscriber's shard holds the
     * track's retained Objects itself: at one lane, and on the dual listener's single
     * WebTransport lane, every client shares one shard; at two lanes the
     * publisher lands on shard 0, the priming subscriber on shard 1 (pulling
     * a copy of the track there), the idle spacer on shard 0 and the fill
     * subscriber on shard 1 -- a slot other than the first. In the dual
     * composition every client arrives over WebTransport, so the fill is held
     * on that facade's lane while the raw listener is up beside it.
     *
     * In the last two it does not. dual_split keeps the publisher and the
     * priming subscriber on the raw listener and puts the fill subscriber on
     * the WebTransport lane. lanes_fill_first accepts the fill subscriber's
     * connection before the priming one: publisher on shard 0, fill
     * subscriber on shard 1, priming subscriber on shard 0 with the
     * publisher. Either way the fill subscriber's shard has ingested nothing
     * of the track when its fill arrives, and the range is retrieved from
     * the owner shard.
     */
    client_t cl[4];
    memset(cl, 0, sizeof(cl));
    const bool wt = mode == MODE_DUAL_WT;
    const uint16_t port = wt ? wt_port : raw_port;
#ifndef MOQR_DUAL_LISTENER
    if (fd_dual(mode)) {
        fprintf(stderr, "FAIL: %s needs the WebTransport build\n",
                mode == MODE_DUAL_WT ? "dual_wt" : "dual_split");
        g_failures++;
        ready = false;
    }
#endif
    if (ready && mode == MODE_DUAL_SPLIT) {
        CHECK(client_open(&cl[0], false, raw_port, &pub));
        CHECK(await_flag(&pub.ns_accepted, FD_SETUP_MS, cl, 1));
        CHECK(client_open(&cl[1], false, raw_port, &prime));
        CHECK(await_flag(&prime.objects, FD_SETUP_MS, cl, 2));
        CHECK(client_open(&cl[3], true, wt_port, &filler));
    } else if (ready && mode == MODE_LANES_FILL_FIRST) {
        CHECK(client_open(&cl[0], false, raw_port, &pub));
        CHECK(await_flag(&pub.ns_accepted, FD_SETUP_MS, cl, 1));
        CHECK(client_open(&cl[3], false, raw_port, &filler));
        CHECK(await_flag(&filler.established, FD_SETUP_MS, cl, 4));
        CHECK(client_open(&cl[1], false, raw_port, &prime));
        CHECK(await_flag(&prime.objects, FD_SETUP_MS, cl, 4));
    } else if (ready) {
        CHECK(client_open(&cl[0], wt, port, &pub));
        CHECK(await_flag(&pub.ns_accepted, FD_SETUP_MS, cl, 1));
        CHECK(client_open(&cl[1], wt, port, &prime));
        CHECK(await_flag(&prime.objects, FD_SETUP_MS, cl, 2));
        CHECK(client_open(&cl[2], wt, port, &spacer));
        CHECK(await_flag(&spacer.established, FD_SETUP_MS, cl, 3));
        CHECK(client_open(&cl[3], wt, port, &filler));
    }
    /* Retained: the priming subscriber has both Objects, so the relay's log
     * holds {0,0} and {0,2} and Largest is {0,2}. */
    bool primed = false;
    if (g_failures == 0) {
        uint64_t until = mono_us() + (uint64_t)FD_SETUP_MS * 1000u;
        while (mono_us() < until &&
               !(atomic_load(&prime.objects) >= 2 &&
                 atomic_load(&filler.established))) {
            for (size_t i = 0; i < 4; i++) {
                client_wake(&cl[i]);
            }
            nap_ms(5);
        }
        primed = atomic_load(&prime.objects) == 2 &&
                 atomic_load(&filler.established);
        CHECK(primed);
    }

    if (primed) {
        /* Release the fill subscriber with one wake. From here nothing pokes
         * the relay or any client: the publisher is silent, the priming
         * subscriber has everything, and the fill subscriber only reacts to
         * what arrives. */
        atomic_store(&filler.go, 1);
        client_wake(&cl[3]);
        uint64_t start = mono_us();
        while (atomic_load(&filler.terminals) == 0 &&
               atomic_load(&filler.fatal) == 0 &&
               mono_us() - start < (uint64_t)FD_STUCK_MS * 1000u) {
            nap_ms(2);
        }
        /* settle: anything delivered after the terminal would show here */
        nap_ms(50);

        uint64_t t_sub = atomic_load(&filler.t_sub_us);
        uint64_t t_first = atomic_load(&filler.t_first_us);
        uint64_t t_term = atomic_load(&filler.t_term_us);
        int terms = atomic_load(&filler.terminals);
        CHECK(t_sub != 0);
        if (terms == 0) {
            fprintf(stderr,
                    "FAIL: the fill did not end within %u ms of a %u ms "
                    "FILL_TIMEOUT (fill_objects=%d fill_gaps=%d)\n",
                    FD_STUCK_MS, FD_TIMEOUT_MS,
                    atomic_load(&filler.fill_objects),
                    atomic_load(&filler.fill_gaps));
            g_failures++;
        } else {
            uint64_t elapsed = t_term - t_sub;
            printf("  fill ended %" PRIu64 " us after the subscribe "
                   "(FILL_TIMEOUT %u ms, tolerance %u ms)\n",
                   elapsed, FD_TIMEOUT_MS, FD_TOLERANCE_MS);
            /* never early: the relay starts its clock after this subscribe */
            CHECK(elapsed >= (uint64_t)FD_TIMEOUT_MS * 1000u);
            /* and on time: the deadline itself ended it */
            CHECK(elapsed <=
                  (uint64_t)(FD_TIMEOUT_MS + FD_TOLERANCE_MS) * 1000u);
            /* what the relay holds was delivered before the wait, on
             * whichever shard the fill subscriber landed */
            CHECK(t_first != 0 &&
                  t_first - t_sub < (uint64_t)FD_TIMEOUT_MS * 1000u);
        }
        CHECK(terms == 1);
        CHECK(atomic_load(&filler.term_reason) == MOQ_FILL_COMPLETE);
        CHECK(atomic_load(&filler.fill_gaps) == 1);
        CHECK(atomic_load(&filler.gap_kind) == MOQ_FETCH_RANGE_TIMED_OUT);
        /* {0,0}, then {0,1} Timed-Out, then {0,2} */
        CHECK(atomic_load(&filler.fill_objects) == 2);
        CHECK(atomic_load(&filler.objects_at_gap) == 1);
        CHECK(atomic_load(&filler.last_obj_group) == 0 &&
              atomic_load(&filler.last_obj_id) == 2);
        CHECK(atomic_load(&filler.gap_group) == 0 &&
              atomic_load(&filler.gap_object) == 1);
        /* a Next Object subscription delivers nothing retained */
        CHECK(atomic_load(&filler.live_objects) == 0);
        CHECK(atomic_load(&filler.fatal) == 0);
    }
    if (g_failures != 0) {
        dump_peer("publisher", &pub);
        dump_peer("prime", &prime);
        dump_peer("fill", &filler);
    }

    for (size_t i = 4; i-- > 0;) {
        client_close(&cl[i]);
    }
    moqr_test_raise_stop();
    pthread_join(th, NULL);
    moqr_test_serve_log_set_io(NULL);
    CHECK(sr.rc == 0);
    return g_failures;
}

int
main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr,
                "usage: %s <cert> <key> "
                "<single|lanes|dual_wt|dual_split|lanes_fill_first|"
                "setup_config_refusal|setup_thread_refusal>\n",
                argv[0]);
        return 2;
    }
    fd_mode_t mode;
    fd_setup_t setup = SETUP_NORMAL;
    if (strcmp(argv[3], "single") == 0) {
        mode = MODE_SINGLE;
    } else if (strcmp(argv[3], "lanes") == 0) {
        mode = MODE_LANES;
    } else if (strcmp(argv[3], "dual_wt") == 0) {
        mode = MODE_DUAL_WT;
    } else if (strcmp(argv[3], "dual_split") == 0) {
        mode = MODE_DUAL_SPLIT;
    } else if (strcmp(argv[3], "lanes_fill_first") == 0) {
        mode = MODE_LANES_FILL_FIRST;
    } else if (strcmp(argv[3], "setup_config_refusal") == 0) {
        mode = MODE_SINGLE;
        setup = SETUP_REFUSE_CONFIG;
    } else if (strcmp(argv[3], "setup_thread_refusal") == 0) {
        mode = MODE_SINGLE;
        setup = SETUP_REFUSE_THREAD;
    } else {
        fprintf(stderr, "unknown scenario: %s\n", argv[3]);
        return 2;
    }
    const int result = run(argv[1], argv[2], mode, setup);
    if (result == 0 && g_failures == 0) {
        printf("PASS: relay_fill_deadline %s\n", argv[3]);
        return 0;
    }
    return 1;
}
