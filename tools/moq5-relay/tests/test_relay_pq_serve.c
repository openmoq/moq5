/*
 * The production relay served over PicoQUIC (listener.transport "picoquic").
 *
 * The real serve composition (cmd_serve_lanes, whose raw lanes are then
 * PicoQUIC facades) runs on its own thread over real localhost QUIC, with
 * real clients on either transport. A publisher announces a namespace; a live
 * subscriber pulls the track through the relay; once every Object has reached
 * it a late subscriber asks for the track from its start and is answered from
 * what the relay retained; then the publisher ends the subscription and both
 * subscribers see the end. The relay's admin endpoint is up throughout: its
 * metrics, shards and info documents are fetched over HTTP and must name the
 * PicoQUIC backend. The publisher publishes two tracks and each
 * subscriber subscribes to both, so on draft-18 and later several request
 * streams are open at once on every connection. Every Object is checked byte
 * for byte, one of them larger than a QUIC packet so it crosses the relay in
 * chunks.
 *
 * argv: cert key draft lanes publisher-backend subscriber-backend [early],
 * where a backend is "pq" (the threaded PicoQUIC client) or "msquic" (the
 * managed MsQuic client); `early` connects the publisher while lane 0's
 * facade runs but is not yet published to the wakers. Clients run their
 * protocol logic in their own pumps; the main thread only correlates atomics.
 *
 * argv: capacity -- the capacity serve prints at one lane follows the
 * composition that runs: the shard runtime (with its serve context) for
 * PicoQUIC, the direct composition for MsQuic.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "../cli/config.h"

#include <moq/msquic_managed.h>
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "logevent.h"

#include <moq/relay/wire_codes.h>

extern int moqr_test_cmd_serve_lanes(const moqr_cli_config_t *cfg);
extern void moqr_test_serve_log_set_io(const moqr_cli_log_io_t *io);
extern void moqr_test_raise_stop(void);
extern void moqr_test_clear_stop(void);
extern void (*g_test_pq_before_lane_publish)(uint32_t lane);
extern _Atomic unsigned g_test_pq_wakes_unpublished;
extern int moqr_test_serve_print_capacity(const moqr_cli_config_t *cfg,
                                          FILE *out);
extern size_t moqr_test_serve_lanes_ctx_bytes(void);
extern bool moqr_test_pq_bind_addr(const char *host,
                                   struct sockaddr_storage *out);


/* early: serve pauses once lane 0's facade runs, before its handle is
 * published to the wakers, until the test releases it. */
static atomic_int g_early_lane0;
static atomic_int g_early_release;

static void
early_before_lane_publish(uint32_t lane)
{
    if (lane != 0) {
        return;
    }
    atomic_store(&g_early_lane0, 1);
    for (int i = 0; i < 4000 && !atomic_load(&g_early_release); i++) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 5000000L };
        nanosleep(&ts, NULL);
    }
}

static int g_failures;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); \
    g_failures++; } } while (0)

/* listener.host as the PicoQUIC bind address: a literal keeps its family,
 * wildcards included; an empty host binds both families; a name is refused. */
/* One field of the capacity print, as the integer after its label. */
static bool
capacity_field(const char *text, const char *label, unsigned long long *out)
{
    const char *at = strstr(text, label);
    return at != NULL && sscanf(at + strlen(label), "%llu", out) == 1;
}

/* The capacity print at one lane, through serve's own helper, for each
 * listener transport: PicoQUIC runs the shard runtime and is described with
 * its serve context and cross-shard container; MsQuic runs the direct
 * composition and is described without them. */
static int
check_capacity(void)
{
    static const char *docs[2] = {
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\","
        "\"transport\":\"picoquic\"}}",
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"}}",
    };
    for (int pq = 1; pq >= 0; pq--) {
        static moqr_cli_config_t cfg;
        char err[256] = { 0 };
        const char *doc = docs[pq ? 0 : 1];
        CHECK(moqr_cli_config_parse(doc, strlen(doc), &cfg, err, sizeof(err)) ==
              MOQR_OK);
        CHECK(moqr_cli_total_lanes(&cfg) == 1u);
        FILE *f = tmpfile();
        CHECK(f != NULL);
        if (f == NULL) {
            continue;
        }
        CHECK(moqr_test_serve_print_capacity(&cfg, f) == 0);
        static char text[8192];
        rewind(f);
        size_t n = fread(text, 1, sizeof(text) - 1u, f);
        text[n] = '\0';
        fclose(f);
        moqr_cli_capacity_t cap;
        CHECK(moqr_cli_describe_capacity(&cfg, moq_alloc_default(),
                                         pq ? moqr_test_serve_lanes_ctx_bytes()
                                            : 0u,
                                         &cap) == MOQR_OK);
        unsigned long long total = 0, runtime = 0, cross = 0;
        CHECK(capacity_field(text, "allocation-request ceiling: ", &total));
        CHECK(capacity_field(text, "cli runtime: ", &runtime));
        CHECK(total == cap.total_bytes);
        CHECK(runtime == cap.cli_runtime_bytes);
        if (pq) {
            CHECK(capacity_field(text, "cross-shard: ", &cross) &&
                  cross == cap.cross_shard_bytes && cross > 0u);
            CHECK(runtime >= moqr_test_serve_lanes_ctx_bytes());
        } else {
            CHECK(strstr(text, "cross-shard: ") == NULL);
        }
        CHECK(strstr(text, "MsQuic/session") == NULL);
        printf("capacity %s: ceiling %llu, cli runtime %llu, cross-shard "
               "%llu\n", pq ? "picoquic" : "msquic", total, runtime, cross);
    }
    return g_failures != 0;
}

static int
check_bind_addr(void)
{
    struct sockaddr_storage ss;
    CHECK(moqr_test_pq_bind_addr("0.0.0.0", &ss) && ss.ss_family == AF_INET &&
          ((struct sockaddr_in *)&ss)->sin_addr.s_addr == htonl(INADDR_ANY));
    CHECK(moqr_test_pq_bind_addr("::", &ss) && ss.ss_family == AF_INET6 &&
          memcmp(&((struct sockaddr_in6 *)&ss)->sin6_addr, &in6addr_any,
                 sizeof(in6addr_any)) == 0);
    CHECK(moqr_test_pq_bind_addr("127.0.0.1", &ss) && ss.ss_family == AF_INET);
    CHECK(moqr_test_pq_bind_addr("::1", &ss) && ss.ss_family == AF_INET6);
    CHECK(moqr_test_pq_bind_addr("", &ss) && ss.ss_family == 0);
    CHECK(!moqr_test_pq_bind_addr("localhost", &ss));
    return g_failures != 0;
}

#define PQS_NS     "pqserve"
#define PQS_TRACKS 2u
static const char *const k_tracks[PQS_TRACKS] = { "t", "u" };
#define PQS_GROUPS 3u
#define PQS_OBJS   4u
#define PQS_TOTAL  (PQS_TRACKS * PQS_GROUPS * PQS_OBJS)
#define PQS_BIG    40000u   /* Object {1,1}: many packets, many chunks */
#define PQS_SETUP_MS 20000u

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

/* An Object's length and its every byte, so a delivery is checked exactly. */
static size_t
obj_len(unsigned tr, uint64_t g, uint64_t o)
{
    return g == 1 && o == 1 ? PQS_BIG + tr
                            : 100u + tr * 11u + (size_t)(g * PQS_OBJS + o) * 37u;
}

static uint8_t
obj_byte(unsigned tr, uint64_t g, uint64_t o, size_t i)
{
    return (uint8_t)(tr * 101u + g * 31u + o * 7u + i);
}

/* -- the serve, on its own thread ------------------------------------------ */

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
    int               rc;
} serve_run_t;

static void *
serve_main(void *arg)
{
    serve_run_t *r = arg;
    r->rc = moqr_test_cmd_serve_lanes(&r->cfg);
    return NULL;
}

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
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(a);
    uint16_t port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0) {
        port = ntohs(a.sin_port);
    }
    close(fd);
    return port;
}

/* One HTTP GET to the admin endpoint on loopback: the status code (0 on any
 * transport failure) and the body, NUL-terminated, in `body`. */
static int
http_get(uint16_t port, const char *path, char *body, size_t cap)
{
    body[0] = '\0';
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    char req[256];
    int rn = snprintf(req, sizeof(req),
                      "GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                      "Connection: close\r\n\r\n", path);
    size_t got = 0;
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        send(fd, req, (size_t)rn, 0) == rn) {
        ssize_t r;
        while (got + 1 < cap &&
               (r = recv(fd, body + got, cap - 1 - got, 0)) > 0) {
            got += (size_t)r;
        }
    }
    close(fd);
    body[got] = '\0';
    int status = 0;
    if (sscanf(body, "HTTP/1.1 %d", &status) != 1) {
        status = 0;
    }
    return status;
}

/* -- the client peers ------------------------------------------------------ */

typedef enum { ROLE_PUB, ROLE_LIVE, ROLE_LATE } role_t;

typedef struct peer {
    role_t   role;
    /* lane-thread state */
    bool     announced;
    moq_version_t version;
    /* per track */
    bool     accepted[PQS_TRACKS];
    bool     wrote[PQS_TRACKS];
    bool     done_sent[PQS_TRACKS];
    bool     subscribed[PQS_TRACKS];
    moq_subscription_t up_sub[PQS_TRACKS];    /* PUB: the relay's subscription */
    moq_subscription_t down_sub[PQS_TRACKS];  /* subscribers: their own */
    /* The Object in progress on each track's group subgroup: chunks of
     * different subgroups interleave, and each chunk names its own location. */
    size_t   chunk_got[PQS_TRACKS][PQS_GROUPS];
    bool     chunk_bad[PQS_TRACKS][PQS_GROUPS];
    /* main thread -> lane */
    atomic_int go;                 /* LATE: subscribe now; PUB: end now */
    /* lane -> main thread */
    atomic_int established;
    atomic_int ns_accepted;
    atomic_int published;
    atomic_int objects;            /* complete, byte-exact Objects */
    atomic_int bad;                /* any Object wrong in length or bytes */
    atomic_int done;               /* SUBSCRIBE_DONEs seen */
    atomic_int fatal;
} peer_t;

static moq_namespace_t
pqs_ns(moq_bytes_t *part)
{
    *part = MOQ_BYTES_LITERAL(PQS_NS);
    return (moq_namespace_t){ .parts = part, .count = 1 };
}

static void
pub_write(peer_t *p, unsigned tr, moq_session_t *s, uint64_t now_us)
{
    for (uint64_t g = 0; g < PQS_GROUPS; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(s, p->up_sub[tr], &sgc, now_us, &sg) !=
            MOQ_OK) {
            atomic_store(&p->fatal, 1);
            return;
        }
        for (uint64_t o = 0; o < PQS_OBJS; o++) {
            size_t n = obj_len(tr, g, o);
            uint8_t *body = malloc(n);
            if (body == NULL) {
                atomic_store(&p->fatal, 1);
                return;
            }
            for (size_t i = 0; i < n; i++) {
                body[i] = obj_byte(tr, g, o, i);
            }
            moq_rcbuf_t *pl = NULL;
            if (moq_rcbuf_create(moq_alloc_default(), body, n, &pl) != MOQ_OK ||
                moq_session_write_object(s, sg, o, pl, now_us) != MOQ_OK) {
                atomic_store(&p->fatal, 1);
            }
            if (pl != NULL) {
                moq_rcbuf_decref(pl);
            }
            free(body);
        }
        if (moq_session_close_subgroup(s, sg, now_us) != MOQ_OK) {
            atomic_store(&p->fatal, 1);
        }
    }
    p->wrote[tr] = true;
    atomic_fetch_add(&p->published, 1);
}

static void
peer_subscribe(peer_t *p, unsigned tr, moq_session_t *s, uint64_t now_us)
{
    moq_bytes_t part;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = pqs_ns(&part);
    sc.track_name = (moq_bytes_t){ .data = (const uint8_t *)k_tracks[tr],
                                   .len = strlen(k_tracks[tr]) };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.start_group = 0;
    sc.start_object = 0;
    moq_subscription_t h;
    moq_result_t rc = moq_session_subscribe(s, &sc, now_us, &h);
    if (rc == MOQ_OK) {
        p->subscribed[tr] = true;
        p->down_sub[tr] = h;
    } else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_REQUEST_BLOCKED) {
        atomic_store(&p->fatal, 1);
    }
}

/* One received Object (whole, or the end of its chunks): count it if it is
 * exactly the Object the publisher wrote at that location. */
static void
note_object(peer_t *p, unsigned tr, uint64_t g, uint64_t o, size_t len,
            bool bad)
{
    if (bad || g >= PQS_GROUPS || o >= PQS_OBJS || len != obj_len(tr, g, o)) {
        fprintf(stderr, "role %d: bad object {%llu,%llu} len %zu want %zu "
                    "bytes wrong %d\n", (int)p->role, (unsigned long long)g,
                    (unsigned long long)o, len,
                    g < PQS_GROUPS && o < PQS_OBJS ? obj_len(tr, g, o) : 0,
                    (int)bad);
        atomic_store(&p->bad, 1);
        return;
    }
    atomic_fetch_add(&p->objects, 1);
}

static bool
bytes_ok(const moq_rcbuf_t *b, unsigned tr, uint64_t g, uint64_t o, size_t at)
{
    const uint8_t *d = moq_rcbuf_data(b);
    size_t n = moq_rcbuf_len(b);
    for (size_t i = 0; i < n; i++) {
        if (d[i] != obj_byte(tr, g, o, at + i)) {
            return false;
        }
    }
    return true;
}

/* Which of this subscriber's tracks a subscription is, or PQS_TRACKS. */
static unsigned
down_track(const peer_t *p, moq_subscription_t sub)
{
    for (unsigned tr = 0; tr < PQS_TRACKS; tr++) {
        if (p->subscribed[tr] && moq_subscription_eq(p->down_sub[tr], sub)) {
            return tr;
        }
    }
    return PQS_TRACKS;
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
        pc.track_namespace = pqs_ns(&part);
        moq_announcement_t ann;
        moq_result_t rc = moq_session_publish_namespace(s, &pc, now_us, &ann);
        if (rc == MOQ_OK) {
            p->announced = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK &&
                   rc != MOQ_ERR_REQUEST_BLOCKED) {
            atomic_store(&p->fatal, 1);
        }
    }
    if (p->role == ROLE_LIVE ||
        (p->role == ROLE_LATE && atomic_load(&p->go))) {
        for (unsigned tr = 0; tr < PQS_TRACKS; tr++) {
            if (!p->subscribed[tr]) {
                peer_subscribe(p, tr, s, now_us);
            }
        }
    }

    /* One event per poll: a request's borrowed fields are invalidated by the
     * next advancing call, and handling an event (accepting a SUBSCRIBE) is
     * one, so a batch's later events could not be read after it. */
    moq_event_t evs[1];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 1)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                atomic_store(&p->ns_accepted, 1);
                break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                const moq_bytes_t *name = &ev->u.subscribe_request.track_name;
                unsigned tr = PQS_TRACKS;
                for (unsigned i = 0; i < PQS_TRACKS; i++) {
                    if (name->len == strlen(k_tracks[i]) &&
                        memcmp(name->data, k_tracks[i], name->len) == 0) {
                        tr = i;
                    }
                }
                if (p->role == ROLE_PUB && tr < PQS_TRACKS && !p->accepted[tr]) {
                    moq_accept_subscribe_cfg_t ac;
                    moq_accept_subscribe_cfg_init(&ac);
                    if (moq_session_accept_subscribe(
                            s, ev->u.subscribe_request.sub, &ac, now_us) ==
                        MOQ_OK) {
                        p->up_sub[tr] = ev->u.subscribe_request.sub;
                        p->accepted[tr] = true;
                    } else {
                        atomic_store(&p->fatal, 1);
                    }
                } else if (p->role == ROLE_PUB) {
                    fprintf(stderr, "publisher: unexpected SUBSCRIBE for "
                            "\"%.*s\" (track %u, accepted %d)\n",
                            (int)name->len, (const char *)name->data, tr,
                            tr < PQS_TRACKS ? (int)p->accepted[tr] : -1);
                    atomic_store(&p->fatal, 1);   /* unknown or repeated */
                }
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_ERROR: {
                /* refused until the namespace has a route: ask again */
                unsigned tr = down_track(p, ev->u.subscribe_error.sub);
                if (tr < PQS_TRACKS) {
                    p->subscribed[tr] = false;
                }
                break;
            }
            case MOQ_EVENT_OBJECT_RECEIVED: {
                const moq_object_received_event_t *o = &ev->u.object_received;
                unsigned tr = down_track(p, o->sub);
                if (tr >= PQS_TRACKS) {
                    atomic_store(&p->bad, 1);
                    break;
                }
                size_t len = o->payload ? moq_rcbuf_len(o->payload) : 0;
                bool bad = o->payload && !bytes_ok(o->payload, tr, o->group_id,
                                                   o->object_id, 0);
                note_object(p, tr, o->group_id, o->object_id, len, bad);
                break;
            }
            case MOQ_EVENT_OBJECT_CHUNK: {
                const moq_object_chunk_event_t *c = &ev->u.object_chunk;
                uint64_t g = c->group_id;
                unsigned tr = down_track(p, c->sub);
                if (g >= PQS_GROUPS || tr >= PQS_TRACKS) {
                    atomic_store(&p->bad, 1);
                    break;
                }
                if (c->begin) {
                    p->chunk_got[tr][g] = 0;
                    p->chunk_bad[tr][g] = false;
                }
                if (c->chunk != NULL) {
                    if (!bytes_ok(c->chunk, tr, g, c->object_id,
                                  p->chunk_got[tr][g])) {
                        p->chunk_bad[tr][g] = true;
                    }
                    p->chunk_got[tr][g] += moq_rcbuf_len(c->chunk);
                }
                if (c->end) {
                    note_object(p, tr, g, c->object_id, p->chunk_got[tr][g],
                                p->chunk_bad[tr][g] ||
                                    c->terminal != MOQ_OBJECT_TERMINAL_NORMAL);
                }
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_DONE:
                if (down_track(p, ev->u.subscribe_done.sub) < PQS_TRACKS) {
                    atomic_fetch_add(&p->done, 1);
                }
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
    for (unsigned tr = 0; p->role == ROLE_PUB && tr < PQS_TRACKS; tr++) {
        if (p->accepted[tr] && !p->wrote[tr]) {
            pub_write(p, tr, s, now_us);
        }
        if (!p->wrote[tr] || p->done_sent[tr] || !atomic_load(&p->go)) {
            continue;
        }
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        uint64_t ended = 0;
        if (moqr_pd_encode(p->version, MOQR_PD_TRACK_ENDED, &ended) != MOQR_OK) {
            atomic_store(&p->fatal, 1);
            return;
        }
        dc.status_code = ended;
        dc.stream_count = PQS_GROUPS;   /* one subgroup stream per group */
        moq_result_t rc = moq_session_done_subscribe(s, p->up_sub[tr], &dc,
                                                     now_us);
        if (rc == MOQ_OK) {
            p->done_sent[tr] = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK) {
            atomic_store(&p->fatal, 1);
        }
    }
}

/* PicoQUIC client */
static int
pq_client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
               uint64_t now_us, void *vctx)
{
    (void)lane;
    peer_drive(vctx, moq_pq_threaded_session(t), now_us);
    return 0;
}

/* MsQuic client */
static int
msq_client_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *vctx)
{
    (void)lane;
    peer_drive(vctx, moq_msquic_managed_session(m), now_us);
    return 0;
}

typedef struct client {
    moq_pq_threaded_t    *pq;
    moq_msquic_managed_t *msq;
} client_t;

static const char *
draft_alpn(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "moqt-16"
           : v == MOQ_VERSION_DRAFT_18 ? "moqt-18"
                                       : "moqt-22";
}

static bool
client_open(client_t *c, bool pq, uint16_t port, moq_version_t v, peer_t *p)
{
    memset(c, 0, sizeof(*c));
    if (pq) {
        static const char *alpn[1];
        alpn[0] = draft_alpn(v);
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.insecure_skip_verify = true;
        cfg.alpn_list = alpn;
        cfg.alpn_count = 1;
        cfg.send_request_capacity = true;
        cfg.streaming_objects = p->role != ROLE_PUB && v != MOQ_VERSION_DRAFT_16;
        cfg.on_lane_pump = pq_client_pump;
        cfg.on_lane_pump_ctx = p;
        return moq_pq_threaded_create(&cfg, &c->pq) == MOQ_OK;
    }
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.version = v;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.on_lane_pump = msq_client_pump;
    cfg.on_lane_pump_user = p;
    return moq_msquic_managed_create(&cfg, &c->msq) == MOQ_OK;
}

static void
client_wake(client_t *c)
{
    if (c->pq != NULL) {
        (void)moq_pq_threaded_wake(c->pq);
    }
    if (c->msq != NULL) {
        (void)moq_msquic_managed_wake(c->msq);
    }
}

static void
client_close(client_t *c)
{
    if (c->pq != NULL) {
        moq_pq_threaded_stop(c->pq);
        moq_pq_threaded_destroy(c->pq);
    }
    if (c->msq != NULL) {
        (void)moq_msquic_managed_stop(c->msq);
        moq_msquic_managed_destroy(c->msq);
    }
    memset(c, 0, sizeof(*c));
}

/* Wake every client until *flag reaches want or the bound passes. */
static bool
await_at_least(atomic_int *flag, int want, unsigned ms, client_t *cl, size_t n)
{
    uint64_t until = mono_us() + (uint64_t)ms * 1000u;
    while (mono_us() < until) {
        if (atomic_load(flag) >= want) {
            return true;
        }
        for (size_t i = 0; i < n; i++) {
            client_wake(&cl[i]);
        }
        nap_ms(5);
    }
    return atomic_load(flag) >= want;
}

static int
run(const char *cert, const char *key, moq_version_t v, unsigned lanes,
    bool pub_pq, bool sub_pq, bool early)
{
    uint16_t port = free_udp_port();
    CHECK(port != 0);
    uint16_t admin_port = 0;
    for (int i = 0; i < 16 && admin_port == 0; i++) {
        /* a free TCP port: bind, read back, release */
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in a;
        memset(&a, 0, sizeof(a));
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(a);
        if (fd >= 0 && bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
            getsockname(fd, (struct sockaddr *)&a, &len) == 0) {
            admin_port = ntohs(a.sin_port);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
    CHECK(admin_port != 0);
    static serve_run_t sr;
    memset(&sr, 0, sizeof(sr));
    char json[2048];
    snprintf(json, sizeof(json),
             "{\"listener\":{\"transport\":\"picoquic\",\"host\":\"127.0.0.1\","
             "\"port\":%u,\"cert\":\"%s\",\"key\":\"%s\",\"lanes\":%u,"
             "\"versions\":[%u]},"
             "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1\",\"port\":%u}}}",
             (unsigned)port, cert, key, lanes,
             v == MOQ_VERSION_DRAFT_16 ? 16u
             : v == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
             (unsigned)admin_port);
    char err[256] = { 0 };
    if (moqr_cli_config_parse(json, strlen(json), &sr.cfg, err, sizeof(err)) !=
        MOQR_OK) {
        fprintf(stderr, "FAIL: config refused: %s\n", err);
        return 1;
    }
    CHECK(sr.cfg.transport == MOQR_CLI_TRANSPORT_PICOQUIC);

    moqr_test_clear_stop();
    atomic_store(&g_ready, 0);
    atomic_store(&g_early_lane0, 0);
    atomic_store(&g_early_release, 0);
    atomic_store(&g_test_pq_wakes_unpublished, 0u);
    g_test_pq_before_lane_publish = early ? early_before_lane_publish : NULL;
    moqr_test_serve_log_set_io(&kServeIo);
    pthread_t th;
    if (pthread_create(&th, NULL, serve_main, &sr) != 0) {
        fprintf(stderr, "FAIL: serve thread\n");
        moqr_test_serve_log_set_io(NULL);
        return 1;
    }
    static peer_t pub, live, late;
    memset(&pub, 0, sizeof(pub));
    memset(&live, 0, sizeof(live));
    memset(&late, 0, sizeof(late));
    pub.role = ROLE_PUB;
    live.role = ROLE_LIVE;
    late.role = ROLE_LATE;
    pub.version = live.version = late.version = v;
    client_t cl[3];
    memset(cl, 0, sizeof(cl));

    uint64_t until = mono_us() + (uint64_t)PQS_SETUP_MS * 1000u;
    if (early) {
        /* Traffic before the facade is published: the publisher connects
         * and its lane pumps in that window. At one lane nothing wakes the
         * lane then (the admin endpoint and the coordinator start after
         * publication, and one shard pushes to no other), so the row counts
         * the wakes that found it unpublished rather than relying on a
         * replay; it proves the pumps of an unpublished lane are served. */
        while (!atomic_load(&g_early_lane0) && mono_us() < until) {
            nap_ms(5);
        }
        CHECK(atomic_load(&g_early_lane0));
        CHECK(client_open(&cl[0], pub_pq, port, v, &pub));
        CHECK(await_at_least(&pub.ns_accepted, 1, PQS_SETUP_MS, cl, 1));
        atomic_store(&g_early_release, 1);
    }
    while (!atomic_load(&g_ready) && mono_us() < until) {
        nap_ms(5);
    }
    CHECK(atomic_load(&g_ready));

    if (g_failures == 0) {
        if (!early) {
            CHECK(client_open(&cl[0], pub_pq, port, v, &pub));
            CHECK(await_at_least(&pub.ns_accepted, 1, PQS_SETUP_MS, cl, 1));
        }
        CHECK(client_open(&cl[1], sub_pq, port, v, &live));
        CHECK(client_open(&cl[2], sub_pq, port, v, &late));
        /* Live: every Object, byte-exact, as it is published. */
        CHECK(await_at_least(&live.objects, (int)PQS_TOTAL, PQS_SETUP_MS, cl,
                             3));
        /* Late: the same Objects, from what the relay retained. */
        atomic_store(&late.go, 1);
        CHECK(await_at_least(&late.objects, (int)PQS_TOTAL, PQS_SETUP_MS, cl,
                             3));
        /* The admin documents, rendered from the PicoQUIC lanes' rows. */
        static char body[1 << 18];
        int st_metrics = http_get(admin_port, "/metrics", body, sizeof(body));
        if (st_metrics != 200) {
            fprintf(stderr, "metrics: status %d: %.300s\n", st_metrics, body);
        }
        CHECK(st_metrics == 200);
        CHECK(strstr(body, "transport=\"picoquic\"") != NULL);
        CHECK(http_get(admin_port, "/api/v1/shards", body, sizeof(body)) == 200);
        if (getenv("PQS_SHOW_SHARDS")) {
            fprintf(stderr, "%s\n", body);
        }
        CHECK(strstr(body, "\"transport\":\"picoquic\"") != NULL);
        CHECK(strstr(body, "\"listener\":\"raw\"") != NULL);
        /* One shard has no cross-shard plane: absent, not zeros as valid. */
        if (lanes == 1) {
            CHECK(strstr(body, "\"capability\":\"absent\"") != NULL);
            CHECK(strstr(body, "\"shard_plane\"") == NULL);
        } else {
            CHECK(strstr(body, "\"capability\":\"valid\"") != NULL);
        }
        CHECK(http_get(admin_port, "/api/v1/info", body, sizeof(body)) == 200);
        CHECK(strstr(body, "\"transport\":\"picoquic\"") != NULL);
        /* The end reaches both. */
        atomic_store(&pub.go, 1);
        CHECK(await_at_least(&live.done, (int)PQS_TRACKS, PQS_SETUP_MS, cl, 3));
        CHECK(await_at_least(&late.done, (int)PQS_TRACKS, PQS_SETUP_MS, cl, 3));
    }
    printf("pq_serve: draft %u lanes %u pub=%s sub=%s: live %d/%u late %d/%u "
           "done %d/%d bad %d/%d fatal %d/%d/%d\n",
           v == MOQ_VERSION_DRAFT_16 ? 16u : v == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
           lanes, pub_pq ? "pq" : "msquic", sub_pq ? "pq" : "msquic",
           atomic_load(&live.objects), PQS_TOTAL, atomic_load(&late.objects),
           PQS_TOTAL, atomic_load(&live.done), atomic_load(&late.done),
           atomic_load(&live.bad), atomic_load(&late.bad),
           atomic_load(&pub.fatal), atomic_load(&live.fatal),
           atomic_load(&late.fatal));
    if (early) {
        printf("pq_serve: wakes that found lane 0 unpublished: %u\n",
               atomic_load(&g_test_pq_wakes_unpublished));
    }
    CHECK(atomic_load(&live.objects) == (int)PQS_TOTAL);
    CHECK(atomic_load(&late.objects) == (int)PQS_TOTAL);
    CHECK(!atomic_load(&live.bad) && !atomic_load(&late.bad));
    CHECK(!atomic_load(&pub.fatal) && !atomic_load(&live.fatal) &&
          !atomic_load(&late.fatal));

    for (size_t i = 0; i < 3; i++) {
        client_close(&cl[i]);
    }
    moqr_test_raise_stop();
    pthread_join(th, NULL);
    moqr_test_serve_log_set_io(NULL);
    CHECK(sr.rc == 0);
    return g_failures != 0;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "capacity") == 0) {
        int rc = check_capacity();
        printf(rc ? "FAILED: relay_pq_capacity\n" : "PASS: relay_pq_capacity\n");
        return rc;
    }
    if (argc == 2 && strcmp(argv[1], "bind_addr") == 0) {
        int rc = check_bind_addr();
        printf(rc ? "FAILED: relay_pq_bind_addr\n" : "PASS: relay_pq_bind_addr\n");
        return rc;
    }
    if (argc != 7 && argc != 8) {
        fprintf(stderr, "usage: %s cert key draft lanes pq|msquic pq|msquic "
                        "[early]\n", argv[0]);
        return 2;
    }
    unsigned d = (unsigned)atoi(argv[3]);
    moq_version_t v = d == 16 ? MOQ_VERSION_DRAFT_16
                      : d == 18 ? MOQ_VERSION_DRAFT_18
                      : d == 22 ? MOQ_VERSION_DRAFT_22 : (moq_version_t)0;
    unsigned lanes = (unsigned)atoi(argv[4]);
    if (v == 0 || lanes < 1 || lanes > 4) {
        fprintf(stderr, "bad draft or lanes\n");
        return 2;
    }
    int rc = run(argv[1], argv[2], v, lanes, strcmp(argv[5], "pq") == 0,
                 strcmp(argv[6], "pq") == 0,
                 argc == 8 && strcmp(argv[7], "early") == 0);
    if (rc == 0) {
        printf("PASS: relay_pq_serve\n");
    }
    return rc;
}
