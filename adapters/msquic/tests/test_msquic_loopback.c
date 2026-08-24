/*
 * The retained physical residue of the managed MoQ-over-MsQuic loopback.
 *
 * Everything here needs a real transport to mean anything; the facade
 * behaviour that does not is proven deterministically over the fake API
 * table (msquic_cfg_prefix, msquic_facade_control, msquic_confinement,
 * msquic_no_spin, msquic_child_forwarding, msquic_lanes, msquic_lane_stats,
 * msquic_close_feed, msquic_terminal_ack, msquic_unit).
 *
 * What remains:
 *   - the streaming-object boundary in BOTH dispositions, each on its own
 *     freshly built pair;
 *   - a real second client refused at the admission cap, which is what a
 *     refused PEER observes rather than what the local listener decided;
 *   - a connect that can never complete, ending in a bounded fatal.
 *
 * Confinement: every moq_session_* call lives inside on_lane_pump. The main
 * thread only waits on terminal predicates and asserts recorded
 * observations after teardown. Timed waits are fail-closed hang guards,
 * never verdicts.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

enum { OBJECT_SIZE = 96 * 1024 }; /* > the 64 KiB budget floor */

static void payload_fill(uint8_t *dst, size_t size)
{
    for (size_t i = 0; i < size; i++)
        dst[i] = (uint8_t)(i * 131u + 47u);
}

struct side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    int sub_ok;
    int session_closed;
    int objects;
    uint64_t object_bytes;
    int object_errors;
    int publish_errors;
    bool subscribed;
    bool published;
    bool close_sent;
    bool hold_open;            /* never close from the pump */
    moq_subscription_t sub;
    bool have_sub;
    /* streaming-objects receive (cfg.streaming_objects = true) */
    int chunk_events;          /* OBJECT_CHUNK events seen */
    uint64_t chunk_bytes;      /* payload bytes across chunks */
    int chunk_errors;          /* byte-pattern mismatches */
    int chunk_objects;         /* objects completed by end + terminal NORMAL */
};

static void observe_object(struct side *sd, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;

    if (payload == NULL) {
        sd->object_errors++;
        return;
    }
    size_t len = moq_rcbuf_len(payload);
    const uint8_t *data = moq_rcbuf_data(payload);

    sd->object_bytes += len;
    if (len != OBJECT_SIZE) {
        sd->object_errors++;
        return;
    }
    for (size_t i = 0; i < len; i++)
        if (data[i] != (uint8_t)(i * 131u + 47u)) {
            sd->object_errors++;
            return;
        }
}

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct side *sd = ctx;
    /* a client's single session is the client lane/session path */
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;

    pthread_mutex_lock(&sd->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sd->setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            sd->sub_ok++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            observe_object(sd, &ev);
            sd->objects++;
            break;
        case MOQ_EVENT_OBJECT_CHUNK: {
            /* streaming receive (cfg.streaming_objects): verify each slice
             * continues the payload_fill pattern at the running offset */
            const moq_object_chunk_event_t *oc = &ev.u.object_chunk;

            sd->chunk_events++;
            if (oc->chunk != NULL) {
                size_t len = moq_rcbuf_len(oc->chunk);
                const uint8_t *data = moq_rcbuf_data(oc->chunk);

                for (size_t i = 0; i < len; i++)
                    if (data[i] !=
                        (uint8_t)((sd->chunk_bytes + i) * 131u + 47u)) {
                        sd->chunk_errors++;
                        break;
                    }
                sd->chunk_bytes += len;
            }
            if (oc->end && oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL)
                sd->chunk_objects++;
            break;
        }
        case MOQ_EVENT_SESSION_CLOSED:
            sd->session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->setup > 0 && !sd->subscribed) {
        static const moq_bytes_t ns[] = {
            { (const uint8_t *)"msq", 3 },
        };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        sd->subscribed = true;
        if (moq_session_subscribe(s, &sc, now, &sub) < 0)
            sd->publish_errors++;
    }
    if (s != NULL && (sd->objects >= 1 || sd->chunk_objects >= 1) &&
        !sd->close_sent && !sd->hold_open) {
        sd->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct side *sd = ctx;
    /* a server drives its accepted session through lane iteration; this
     * scenario runs exactly one connection */
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);

    (void)m;
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c)
                                 : NULL;
    moq_event_t ev;

    pthread_mutex_lock(&sd->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sd->setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            sd->sub = ev.u.subscribe_request.sub;
            sd->have_sub = true;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            sd->session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->have_sub && !sd->published) {
        sd->published = true;

        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, sd->sub, &ac, now) < 0)
            sd->publish_errors++;

        moq_subgroup_cfg_t sgc;
        moq_subgroup_handle_t sg;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, sd->sub, &sgc, now, &sg) < 0) {
            sd->publish_errors++;
        } else {
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;

            if (bytes != NULL)
                payload_fill(bytes, OBJECT_SIZE);
            if (bytes == NULL ||
                moq_rcbuf_create(moq_alloc_default(), bytes,
                                 OBJECT_SIZE, &buf) < 0) {
                sd->publish_errors++;
            } else {
                if (moq_session_write_object(s, sg, 0, buf, now) < 0)
                    sd->publish_errors++;
                moq_rcbuf_decref(buf);
                if (moq_session_close_subgroup(s, sg, now) < 0)
                    sd->publish_errors++;
            }
            free(bytes);
        }
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}


static void side_init(struct side *sd)
{
    memset(sd, 0, sizeof(*sd));
    pthread_mutex_init(&sd->mu, NULL);
}

static void side_destroy(struct side *sd)
{
    pthread_mutex_destroy(&sd->mu);
}

/* Bounded wait for a terminal state on a managed handle. */
static bool wait_terminal(moq_msquic_managed_t *m, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (moq_msquic_managed_is_closed(m) ||
            moq_msquic_managed_is_fatal(m))
            return true;
        (void)moq_msquic_managed_wait(m, 10 * 1000);
    }
    return moq_msquic_managed_is_closed(m) ||
           moq_msquic_managed_is_fatal(m);
}

static void t_streaming_objects(const char *cert, const char *key,
                                bool streaming)
{
    int before = failures;
    struct side sv, cl;

    side_init(&sv);
    side_init(&cl);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0; /* ephemeral */
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = client_pump;
    ccfg.on_lane_pump_user = &cl;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_events = 64;
    ccfg.streaming_objects = streaming;
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    CHECK(wait_terminal(cl.m, 30000));
    CHECK(wait_terminal(sv.m, 30000));

    CHECK(moq_msquic_managed_is_closed(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(cl.m));
    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    /* observations, read after teardown */
    CHECK(cl.setup == 1);
    CHECK(cl.sub_ok == 1);
    CHECK(sv.publish_errors == 0);
    if (streaming) {
        CHECK(cl.objects == 0);            /* never a whole-object event */
        CHECK(cl.chunk_events >= 1);       /* the object arrived as slices */
        CHECK(cl.chunk_bytes == OBJECT_SIZE);
        CHECK(cl.chunk_errors == 0);       /* byte-exact across chunks */
        CHECK(cl.chunk_objects == 1);      /* one object, terminal NORMAL */
    } else {
        CHECK(cl.chunk_events == 0);       /* no slice without the flag */
        CHECK(cl.objects == 1);            /* exactly one whole object */
        CHECK(cl.object_bytes == OBJECT_SIZE);
        CHECK(cl.object_errors == 0);      /* byte-exact */
    }

    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == before)
        printf("PASS: streaming_objects[streaming=%d]\n", (int)streaming);
}

/* Server-side state for multi-connection scenarios: one slot per
 * accepted session, keyed by session pointer, driven entirely from
 * inside on_lane_pump via lane iteration. */
struct msrv {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    struct {
        moq_subscription_t sub;
        bool have_sub;
        bool published;
    } slots[4];
    int next_slot;
    int published;
    int session_closed;
    size_t first_closed_conn_count;
};

static int multi_server_pump(moq_msquic_managed_t *m,
                             moq_msquic_managed_lane_t *lane, uint64_t now,
                             void *ctx)
{
    struct msrv *sv = ctx;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    for (moq_msquic_managed_conn_t *conn =
             moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        /* per-connection state rides the conn's user slot: pointer
         * values (session or conn) are NOT stable identities across a
         * reclaimed child and its successor */
        int slot = (int)(intptr_t)moq_msquic_managed_conn_user(conn) - 1;

        if (slot < 0) {
            if (sv->next_slot >= 4)
                continue;
            slot = sv->next_slot++;
            moq_msquic_managed_conn_set_user(
                conn, (void *)(intptr_t)(slot + 1));
        }

        moq_event_t ev;
        bool closed = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv->slots[slot].sub = ev.u.subscribe_request.sub;
                sv->slots[slot].have_sub = true;
            } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                sv->session_closed++;
                closed = true;
                if (sv->first_closed_conn_count == 0)
                    sv->first_closed_conn_count =
                        moq_msquic_managed_conn_count(m);
            }
            moq_event_cleanup(&ev);
        }
        /* terminal consumed: release the child so it can be reclaimed */
        if (closed)
            (void)moq_msquic_managed_conn_ack_terminal(conn);
        if (sv->slots[slot].have_sub && !sv->slots[slot].published) {
            sv->slots[slot].published = true;
            sv->published++;

            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            (void)moq_session_accept_subscribe(s, sv->slots[slot].sub,
                                               &ac, now);

            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, sv->slots[slot].sub, &sgc,
                                          now, &sg) == 0) {
                uint8_t *bytes = malloc(OBJECT_SIZE);
                moq_rcbuf_t *buf = NULL;

                if (bytes != NULL) {
                    payload_fill(bytes, OBJECT_SIZE);
                    if (moq_rcbuf_create(moq_alloc_default(), bytes,
                                         OBJECT_SIZE, &buf) == 0) {
                        (void)moq_session_write_object(s, sg, 0, buf,
                                                       now);
                        moq_rcbuf_decref(buf);
                    }
                    free(bytes);
                }
                (void)moq_session_close_subgroup(s, sg, now);
            }
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static moq_msquic_managed_t *make_multi_server(struct msrv *sv,
                                               const char *cert,
                                               const char *key,
                                               uint32_t max_conns)
{
    moq_msquic_managed_cfg_t cfg;

    memset(sv, 0, sizeof(*sv));
    pthread_mutex_init(&sv->mu, NULL);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = multi_server_pump;
    cfg.on_lane_pump_user = sv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = max_conns;
    if (moq_msquic_managed_create(&cfg, &sv->m) != MOQ_OK)
        return NULL;
    return sv->m;
}

static moq_msquic_managed_t *make_client(struct side *sd, uint16_t port,
                                         bool hold_open)

{
    moq_msquic_managed_cfg_t cfg;

    side_init(sd);
    sd->hold_open = hold_open;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_user = sd;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    if (moq_msquic_managed_create(&cfg, &sd->m) != MOQ_OK)
        return NULL;
    return sd->m;
}

static void t_cap_refusal(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 1) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, true) != NULL); /* holds open */
    if (c1.m == NULL)
        return;
    /* first client fully up (it received its object and idles) */
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&c1.mu);
        bool got = c1.objects >= 1;
        pthread_mutex_unlock(&c1.mu);
        if (got)
            break;
        (void)moq_msquic_managed_wait(c1.m, 10 * 1000);
    }

    CHECK(make_client(&c2, port, false) != NULL);
    if (c2.m == NULL)
        return;
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_fatal(c2.m));
    CHECK(c2.setup == 0);

    /* the first client is still healthy: close it from its pump */
    pthread_mutex_lock(&c1.mu);
    c1.hold_open = false;
    pthread_mutex_unlock(&c1.mu);
    (void)moq_msquic_managed_wake(c1.m);
    CHECK(wait_terminal(c1.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(!moq_msquic_managed_is_fatal(c1.m));

    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    moq_msquic_managed_destroy(c2.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(c1.setup == 1);
    CHECK(c1.objects == 1);
    CHECK(c1.object_errors == 0);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: cap_refusal\n");
}

static int idle_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now,
                     void *ctx)
{
    (void)m;
    (void)lane;
    (void)now;
    (void)ctx;
    return 0;
}

/*
 * A connect that can never complete ends in a bounded FATAL terminal, with
 * no setup, and stop/destroy stay clean.
 *
 * The destination is a TEST-OWNED silent endpoint: a UDP socket this test
 * binds on loopback to an ephemeral port and holds open without ever
 * speaking QUIC. Binding it is what makes the case self-contained -- it
 * guarantees nothing else occupies the port, where the old fixed port 1 was
 * an assumption about the host. Because the port IS bound, the peer sends
 * no ICMP refusal, so the failure is the handshake/idle timeout bounded by
 * cfg.idle_timeout_ms rather than an immediate reject. Both satisfy what is
 * asserted here, which is the CLASSIFICATION -- fatal, not a clean close --
 * and never a particular code. The wait is a fail-closed hang guard.
 */
static void t_connect_failure_fatal(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    struct sockaddr_in sa;
    socklen_t slen = sizeof(sa);
    int silent = socket(AF_INET, SOCK_DGRAM, 0);

    CHECK(silent >= 0);
    if (silent < 0)
        return;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0; /* ephemeral */
    CHECK(bind(silent, (struct sockaddr *)&sa, sizeof(sa)) == 0);
    CHECK(getsockname(silent, (struct sockaddr *)&sa, &slen) == 0);
    CHECK(ntohs(sa.sin_port) != 0);

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = ntohs(sa.sin_port); /* bound, and forever silent */
    cfg.insecure_skip_verify = true;
    cfg.idle_timeout_ms = 2000;
    cfg.on_lane_pump = idle_pump;
    CHECK(moq_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL) {
        close(silent);
        return;
    }

    CHECK(wait_terminal(m, 20000));
    CHECK(moq_msquic_managed_is_fatal(m));
    CHECK(!moq_msquic_managed_is_closed(m));
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    /* closed only after destroy, so the socket outlives every retransmit */
    close(silent);

    if (failures == before)
        printf("PASS: connect_failure_fatal\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }

    /* one library reference for the whole process: managed facades
     * open/close MsQuic per instance, and letting the global refcount
     * bounce to zero between scenarios re-initializes MsQuic's
     * datapath each time — churn that has shown rare transient
     * connect failures. Never released; reachable until exit. */
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    /* two independent pairs: chunks iff configured */
    t_streaming_objects(argv[1], argv[2], true);
    t_streaming_objects(argv[1], argv[2], false);
    t_cap_refusal(argv[1], argv[2]);
    t_connect_failure_fatal();

    if (failures == 0)
        printf("PASS: msquic_loopback\n");
    else
        fprintf(stderr, "FAIL: msquic_loopback (%d)\n", failures);
    return failures;
}
