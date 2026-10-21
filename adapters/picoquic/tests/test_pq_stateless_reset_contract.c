/*
 * The stateless-reset contract LibMoQ requires of picoquic, carried by
 * patches/picoquic/0009-stateless-reset-ingress.patch (RFC 9000 section
 * 10.3.1, and the no-send policy of section 10.3): public API only, simulated
 * time, a fixed-latency in-process link between a server context and a client
 * context.
 *
 * A genuine reset is produced by picoquic itself: the client context is freed
 * ("crash") and recreated with the same reset seed at the same address; the
 * server's next packet for the old connection reaches the new client context,
 * which answers it with a stateless reset carrying the token the client issued
 * for the connection ID the server was using.
 *
 *   case 0   the server is still on the client's initial CID, which carries no
 *            reset token: the reset must NOT be detected. A control.
 *   case 0b  the server moved to a token-bearing client CID; same address:
 *            detected. The positive control.
 *   case 1   NAT rebinding: the client's apparent port changes; the server
 *            validates the new path and sends there on a new token-bearing
 *            client CID; the reset arrives from the new address: detected.
 *   case 1c  case 1, then a CID refresh on the new path: detected. A control.
 *   case 2   connections A and B from one client address, both on token-bearing
 *            CIDs; B's genuine reset with its leading bytes replaced by A's live
 *            server CID, so the server associates it with A and cannot decrypt
 *            it: B detected, A unaffected. Then B's unmodified reset: detected.
 *   case 3   the restarted client context has reset sending disabled: no reset
 *            is produced, and the server connection stays up.
 *
 * Exit status: 0 when every case holds; 1 when a case fails with every
 * prerequisite and control holding; 2 when a prerequisite or control did not
 * hold, so the run proves nothing.
 */

#include <picoquic.h>
#include <picoquic_utils.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#define ALPN "repro"
#define MAX_CNX 4

static uint64_t g_now = 1000000;
static const char *g_cert, *g_key;

/* -- addresses -------------------------------------------------------------- */

static struct sockaddr_storage mk_addr(const char *ip, uint16_t port)
{
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    struct sockaddr_in *a = (struct sockaddr_in *)&ss;
    a->sin_family = AF_INET;
    a->sin_port = htons(port);
    inet_pton(AF_INET, ip, &a->sin_addr);
#ifdef __APPLE__
    a->sin_len = sizeof(*a);
#endif
    return ss;
}

static bool same_addr(const struct sockaddr_storage *x,
                      const struct sockaddr_storage *y)
{
    const struct sockaddr_in *a = (const void *)x, *b = (const void *)y;
    return a->sin_family == b->sin_family && a->sin_port == b->sin_port &&
           a->sin_addr.s_addr == b->sin_addr.s_addr;
}

static uint16_t port_of(const struct sockaddr_storage *x)
{
    return ntohs(((const struct sockaddr_in *)x)->sin_port);
}

/* -- the server's observations ---------------------------------------------- */

typedef struct {
    picoquic_cnx_t *cnx;
    bool reset_cb;          /* picoquic_callback_stateless_reset seen */
    bool closed_cb;         /* any close callback */
} srv_cnx_t;

static srv_cnx_t g_srv[MAX_CNX];
static int g_nsrv;

static srv_cnx_t *srv_rec(picoquic_cnx_t *c)
{
    for (int i = 0; i < g_nsrv; i++)
        if (g_srv[i].cnx == c) return &g_srv[i];
    if (g_nsrv < MAX_CNX) {
        g_srv[g_nsrv].cnx = c;
        return &g_srv[g_nsrv++];
    }
    return NULL;
}

static int server_cb(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                     size_t length, picoquic_call_back_event_t ev, void *ctx,
                     void *sctx)
{
    (void)stream_id; (void)bytes; (void)length; (void)ctx; (void)sctx;
    srv_cnx_t *r = srv_rec(cnx);
    if (r && ev == picoquic_callback_stateless_reset) r->reset_cb = true;
    if (r && (ev == picoquic_callback_close ||
              ev == picoquic_callback_application_close))
        r->closed_cb = true;
    return 0;
}

static int client_cb(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                     size_t length, picoquic_call_back_event_t ev, void *ctx,
                     void *sctx)
{
    (void)cnx; (void)stream_id; (void)bytes; (void)length; (void)ev;
    (void)ctx; (void)sctx;
    return 0;
}

/* -- the network ------------------------------------------------------------ */

typedef struct {
    picoquic_quic_t *server, *client;
    struct sockaddr_storage server_addr;
    struct sockaddr_storage client_local; /* the client's own address */
    struct sockaddr_storage client_seen;  /* the client as the server sees it */
    /* last server datagram destination, for the report */
    struct sockaddr_storage last_server_dst;
    /* captured client datagrams (the reset), when capture is on */
    bool capture;
    uint8_t cap[PICOQUIC_MAX_PACKET_SIZE];
    size_t cap_len;
} net_t;

static uint8_t g_seed_client[PICOQUIC_RESET_SECRET_SIZE];
static uint8_t g_seed_server[PICOQUIC_RESET_SECRET_SIZE];

static bool g_client_sends_resets = true;

static picoquic_quic_t *mk_client(void)
{
    picoquic_quic_t *q = picoquic_create(MAX_CNX, NULL, NULL, NULL, ALPN,
        client_cb, NULL, NULL, NULL, g_seed_client, g_now, &g_now, NULL,
        NULL, 0);
    if (q) {
        picoquic_set_null_verifier(q);
        if (!g_client_sends_resets) picoquic_set_stateless_reset_sending(q, 0);
    }
    return q;
}

/* A link with fixed one-way latency: datagrams in flight, each with its
 * arrival time, delivered in send order. */
#define LAT_US 10000u
#define MAX_FLIGHT 4096

typedef struct {
    uint64_t arrive;
    bool to_server;
    size_t len;
    struct sockaddr_storage dst;   /* where the sender addressed it */
    uint8_t bytes[PICOQUIC_MAX_PACKET_SIZE];
} flight_t;

static flight_t g_flight[MAX_FLIGHT];
static size_t g_head, g_tail;     /* ring of in-flight datagrams */

static void deliver_due(net_t *n)
{
    while (g_head != g_tail && g_flight[g_head % MAX_FLIGHT].arrive <= g_now) {
        flight_t *f = &g_flight[g_head % MAX_FLIGHT];
        g_head++;
        picoquic_cnx_t *first = NULL;
        if (f->to_server) {
            picoquic_incoming_packet_ex(n->server, f->bytes, f->len,
                (struct sockaddr *)&n->client_seen,
                (struct sockaddr *)&n->server_addr, 0, 0, &first, g_now);
        } else if (n->client && same_addr(&f->dst, &n->client_seen)) {
            /* Only the current NAT mapping reaches the client. */
            picoquic_incoming_packet_ex(n->client, f->bytes, f->len,
                (struct sockaddr *)&n->server_addr,
                (struct sockaddr *)&n->client_local, 0, 0, &first, g_now);
        }
    }
}

static void send_side(net_t *n, int side)
{
    picoquic_quic_t *q = side == 0 ? n->client : n->server;
    if (!q) return;
    for (int k = 0; k < 64; k++) {
        uint8_t buf[PICOQUIC_MAX_PACKET_SIZE];
        size_t len = 0;
        struct sockaddr_storage to, from;
        int ifx = 0;
        picoquic_connection_id_t lcid;
        picoquic_cnx_t *last = NULL;
        memset(&to, 0, sizeof(to));
        memset(&from, 0, sizeof(from));
        if (picoquic_prepare_next_packet_ex(q, g_now, buf, sizeof(buf), &len,
                &to, &from, &ifx, &lcid, &last, NULL) != 0 || len == 0)
            return;
        if (side == 0 && n->capture && n->cap_len == 0) {
            memcpy(n->cap, buf, len);         /* held for the case */
            n->cap_len = len;
            continue;
        }
        if (side == 1) n->last_server_dst = to;
        if (g_tail - g_head >= MAX_FLIGHT) return;
        flight_t *f = &g_flight[g_tail % MAX_FLIGHT];
        g_tail++;
        f->arrive = g_now + LAT_US;
        f->to_server = side == 0;
        f->len = len;
        f->dst = to;
        memcpy(f->bytes, buf, len);
    }
}

static void step(net_t *n)
{
    deliver_due(n);
    send_side(n, 0);
    send_side(n, 1);
}

static void run_for(net_t *n, uint64_t dur_us)
{
    uint64_t end = g_now + dur_us;
    while (g_now < end) {
        step(n);
        uint64_t next = end;
        if (g_head != g_tail && g_flight[g_head % MAX_FLIGHT].arrive < next)
            next = g_flight[g_head % MAX_FLIGHT].arrive;
        if (n->server) {
            uint64_t w = picoquic_get_next_wake_time(n->server, g_now);
            if (w < next) next = w;
        }
        if (n->client) {
            uint64_t w = picoquic_get_next_wake_time(n->client, g_now);
            if (w < next) next = w;
        }
        g_now = next > g_now ? next : g_now + 1;
    }
}

static picoquic_cnx_t *connect_one(net_t *n)
{
    picoquic_cnx_t *c = picoquic_create_cnx(n->client,
        picoquic_null_connection_id, picoquic_null_connection_id,
        (struct sockaddr *)&n->server_addr, g_now, 0, "test.example.com",
        ALPN, 1);
    if (!c || picoquic_start_client_cnx(c) != 0) return NULL;
    run_for(n, 2000000);
    return picoquic_get_cnx_state(c) == picoquic_state_ready ? c : NULL;
}

/* The server's connection for client connection `cc`: the server's local CID
 * is the CID the client sends to. */
static picoquic_cnx_t *server_for(net_t *n, picoquic_cnx_t *cc)
{
    picoquic_connection_id_t want = picoquic_get_remote_cnxid(cc);
    for (picoquic_cnx_t *c = picoquic_get_first_cnx(n->server); c;
         c = picoquic_get_next_cnx(c)) {
        picoquic_connection_id_t id = picoquic_get_local_cnxid(c);
        if (picoquic_compare_connection_id(&id, &want) == 0) return c;
    }
    return NULL;
}

/* The server moves its default path to a fresh client CID, one the client
 * issued with a stateless reset token (public API). */
static bool server_refresh(net_t *n, picoquic_cnx_t *sc)
{
    picoquic_connection_id_t before = picoquic_get_remote_cnxid(sc);
    int rc = picoquic_refresh_path_connection_id(sc, 0);
    static const uint8_t data[32] = { 3 };
    picoquic_add_to_stream(sc, 1, data, sizeof(data), 0);
    run_for(n, 100000);
    picoquic_connection_id_t after = picoquic_get_remote_cnxid(sc);
    return rc == 0 && picoquic_compare_connection_id(&before, &after) != 0;
}

static void setup(net_t *n)
{
    memset(n, 0, sizeof(*n));
    g_head = g_tail = 0;
    memset(g_srv, 0, sizeof(g_srv));
    g_nsrv = 0;
    n->server_addr = mk_addr("10.0.0.2", 443);
    n->client_local = mk_addr("192.168.1.10", 5000);
    n->client_seen = mk_addr("203.0.113.7", 40000);
    n->server = picoquic_create(MAX_CNX, g_cert, g_key, NULL, ALPN, server_cb,
        NULL, NULL, NULL, g_seed_server, g_now, &g_now, NULL, NULL, 0);
    n->client = mk_client();
}

/* The client crashes and restarts with the same seed and address. */
static void client_restart(net_t *n)
{
    picoquic_free(n->client);
    n->client = mk_client();
}

/* Make the server send on `sc`, let the restarted client answer with a reset,
 * and capture that reset (not delivered). */
static bool capture_reset(net_t *n, picoquic_cnx_t *sc)
{
    static const uint8_t data[64] = { 1 };
    picoquic_add_to_stream(sc, 1, data, sizeof(data), 0);
    n->capture = true;
    n->cap_len = 0;
    for (int i = 0; i < 200 && n->cap_len == 0; i++)
        run_for(n, 1000);
    n->capture = false;
    return n->cap_len > 0;
}

static void deliver_captured(net_t *n, const uint8_t *bytes, size_t len)
{
    uint8_t buf[PICOQUIC_MAX_PACKET_SIZE];
    memcpy(buf, bytes, len);
    picoquic_cnx_t *first = NULL;
    picoquic_incoming_packet_ex(n->server, buf, len,
        (struct sockaddr *)&n->client_seen, (struct sockaddr *)&n->server_addr,
        0, 0, &first, g_now);
}

static const char *state_name(picoquic_state_enum s)
{
    switch (s) {
    case picoquic_state_ready: return "ready";
    case picoquic_state_disconnecting: return "disconnecting";
    case picoquic_state_closing_received: return "closing_received";
    case picoquic_state_closing: return "closing";
    case picoquic_state_draining: return "draining";
    case picoquic_state_disconnected: return "disconnected";
    default: return "other";
    }
}

static void report(const char *what, picoquic_cnx_t *c)
{
    srv_cnx_t *r = srv_rec(c);
    printf("  %s: server state %s, stateless_reset callback %s\n", what,
           state_name(picoquic_get_cnx_state(c)),
           r && r->reset_cb ? "YES" : "no");
}

static int g_harness_failures;

/* A prerequisite or control that did not hold: the run proves nothing. */
#define REQUIRE(cond, what)                                               \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("  HARNESS: %s did not hold\n", what);                 \
            g_harness_failures++;                                         \
        }                                                                 \
    } while (0)

/* The smallest stateless reset (§10.3): 5 unpredictable bytes and a 16-byte
 * token. */
#define RESET_MIN_LEN (5u + PICOQUIC_RESET_SECRET_SIZE)

/* A captured datagram that has a stateless reset's shape: short header form
 * (first bit 0) with the fixed bit set, and at least RESET_MIN_LEN bytes. */
static bool reset_shape(const uint8_t *b, size_t len)
{
    return len >= RESET_MIN_LEN && (b[0] & 0x80) == 0 && (b[0] & 0x40) != 0;
}

static bool detected(picoquic_cnx_t *c)
{
    srv_cnx_t *r = srv_rec(c);
    return r && r->reset_cb;
}

static bool untouched(picoquic_cnx_t *c)
{
    srv_cnx_t *r = srv_rec(c);
    return picoquic_get_cnx_state(c) == picoquic_state_ready &&
           !(r && r->reset_cb);
}

int main(int argc, char **argv)
{
    if (argc != 1 && argc != 3) {
        fprintf(stderr, "usage: %s [cert key]\n", argv[0]);
        return 2;
    }
    setvbuf(stdout, NULL, _IONBF, 0);
    g_cert = argc == 3 ? argv[1] : MOQ_TEST_CERT_PATH;
    g_key = argc == 3 ? argv[2] : MOQ_TEST_KEY_PATH;
    memset(g_seed_client, 0xC1, sizeof(g_seed_client));
    memset(g_seed_server, 0x5E, sizeof(g_seed_server));
    net_t n;
    bool f1 = false, f2 = false, no_send_violated = false;

    /* -- case 0: the client's initial CID carries no token -------------------- */
    setup(&n);
    picoquic_cnx_t *c0 = connect_one(&n);
    picoquic_cnx_t *s0 = c0 ? server_for(&n, c0) : NULL;
    printf("case 0 (no path change, server still on the client's initial CID, "
           "which has no token): established %s\n", s0 ? "yes" : "NO");
    REQUIRE(s0 != NULL, "case 0 connection");
    if (s0) {
        client_restart(&n);
        bool got = capture_reset(&n, s0);
        printf("  reset produced: %s (%zu bytes)\n", got ? "yes" : "NO",
               n.cap_len);
        REQUIRE(got && reset_shape(n.cap, n.cap_len), "case 0 reset capture");
        if (got) {
            deliver_captured(&n, n.cap, n.cap_len);
            report("after reset (RFC: no eligible token; not detected)", s0);
            REQUIRE(!detected(s0), "case 0 control (not detected)");
        }
    }
    picoquic_free(n.client);
    picoquic_free(n.server);

    /* -- case 0b: positive control ----------------------------------------- */
    setup(&n);
    c0 = connect_one(&n);
    s0 = c0 ? server_for(&n, c0) : NULL;
    printf("case 0b (no path change, server refreshed to a token-bearing "
           "client CID): established %s\n", s0 ? "yes" : "NO");
    REQUIRE(s0 != NULL, "case 0b connection");
    if (s0) {
        bool ref = server_refresh(&n, s0);
        printf("  server CID refresh: %s\n", ref ? "yes" : "NO");
        REQUIRE(ref, "case 0b CID refresh");
        client_restart(&n);
        bool got = capture_reset(&n, s0);
        printf("  reset produced: %s (%zu bytes)\n", got ? "yes" : "NO",
               n.cap_len);
        REQUIRE(got && reset_shape(n.cap, n.cap_len), "case 0b reset capture");
        if (got) {
            deliver_captured(&n, n.cap, n.cap_len);
            report("after reset (RFC: detected)", s0);
            REQUIRE(detected(s0), "case 0b control (detected)");
        }
    }
    picoquic_free(n.client);
    picoquic_free(n.server);

    /* -- case 1 and 1c: NAT rebinding ---------------------------------------- */
    for (int refresh_after = 0; refresh_after < 2; refresh_after++) {
        setup(&n);
        picoquic_cnx_t *c1 = connect_one(&n);
        picoquic_cnx_t *s1 = c1 ? server_for(&n, c1) : NULL;
        printf("case %s (NAT rebinding%s): established %s\n",
               refresh_after ? "1c" : "1",
               refresh_after ? ", then a server CID refresh on the new path" : "",
               s1 ? "yes" : "NO");
        REQUIRE(s1 != NULL, "case 1 connection");
        if (!s1) { picoquic_free(n.client); picoquic_free(n.server); continue; }
        picoquic_connection_id_t before = picoquic_get_remote_cnxid(s1);
        uint16_t old_port = port_of(&n.client_seen);
        n.client_seen = mk_addr("203.0.113.7", 40001);
        static const uint8_t data[200] = { 2 };
        for (int i = 0; i < 10; i++) {
            picoquic_add_to_stream(c1, 0, data, sizeof(data), 0);
            run_for(&n, 100000);
        }
        picoquic_connection_id_t after = picoquic_get_remote_cnxid(s1);
        printf("  server sends to port %u (was %u); its remote CID %s\n",
               port_of(&n.last_server_dst), old_port,
               picoquic_compare_connection_id(&before, &after) == 0
                   ? "unchanged" : "changed (a token-bearing client CID)");
        REQUIRE(port_of(&n.last_server_dst) == 40001,
                "case 1 server sending on the new path");
        REQUIRE(picoquic_compare_connection_id(&before, &after) != 0,
                "case 1 server on a token-bearing client CID");
        if (refresh_after) {
            bool ref = server_refresh(&n, s1);
            printf("  server CID refresh: %s\n", ref ? "yes" : "NO");
            REQUIRE(ref, "case 1c CID refresh");
        }
        client_restart(&n);
        bool got = capture_reset(&n, s1);
        printf("  reset produced: %s (%zu bytes)\n", got ? "yes" : "NO",
               n.cap_len);
        REQUIRE(got && reset_shape(n.cap, n.cap_len), "case 1 reset capture");
        if (got) {
            deliver_captured(&n, n.cap, n.cap_len);
            report("after reset from the new address (RFC: detected)", s1);
            if (refresh_after)
                REQUIRE(detected(s1), "case 1c control (detected)");
            else
                f1 = !detected(s1);
        }
        picoquic_free(n.client);
        picoquic_free(n.server);
    }

    /* -- case 2: associated but undecryptable -------------------------------- */
    setup(&n);
    picoquic_cnx_t *ca = connect_one(&n);
    picoquic_cnx_t *cb = ca ? connect_one(&n) : NULL;
    picoquic_cnx_t *sa = ca ? server_for(&n, ca) : NULL;
    picoquic_cnx_t *sb = cb ? server_for(&n, cb) : NULL;
    printf("case 2 (one client address; B's reset with A's live server CID as "
           "its prefix): established %s\n",
           (sa && sb && sa != sb) ? "yes" : "NO");
    REQUIRE(sa && sb && sa != sb, "case 2 connections");
    if (sa && sb && sa != sb) {
        bool ra = server_refresh(&n, sa), rb2 = server_refresh(&n, sb);
        printf("  server CID refresh: A %s, B %s\n", ra ? "yes" : "NO",
               rb2 ? "yes" : "NO");
        REQUIRE(ra && rb2, "case 2 CID refreshes");
        picoquic_connection_id_t a_cid = picoquic_get_local_cnxid(sa);
        picoquic_connection_id_t b_cid = picoquic_get_local_cnxid(sb);
        REQUIRE(picoquic_compare_connection_id(&a_cid, &b_cid) != 0 &&
                    server_for(&n, ca) == sa && server_for(&n, cb) == sb,
                "case 2 distinct connections A and B");
        client_restart(&n);
        bool got = capture_reset(&n, sb);
        printf("  B's reset produced: %s (%zu bytes)\n", got ? "yes" : "NO",
               n.cap_len);
        REQUIRE(got && reset_shape(n.cap, n.cap_len), "case 2 reset capture");
        /* The prefix may be replaced only where it cannot reach the token. */
        REQUIRE(n.cap_len >= 1u + a_cid.id_len + PICOQUIC_RESET_SECRET_SIZE,
                "case 2 reset long enough for A's CID and the token");
        if (got && n.cap_len >= 1u + a_cid.id_len + PICOQUIC_RESET_SECRET_SIZE) {
            uint8_t rb[PICOQUIC_MAX_PACKET_SIZE], mod[PICOQUIC_MAX_PACKET_SIZE];
            size_t rb_len = n.cap_len;
            memcpy(rb, n.cap, rb_len);
            memcpy(mod, rb, rb_len);
            memcpy(mod + 1, a_cid.id, a_cid.id_len);
            REQUIRE(memcmp(mod + rb_len - PICOQUIC_RESET_SECRET_SIZE,
                           rb + rb_len - PICOQUIC_RESET_SECRET_SIZE,
                           PICOQUIC_RESET_SECRET_SIZE) == 0,
                    "case 2 genuine token unchanged by the prefix");
            deliver_captured(&n, mod, rb_len);
            report("B after the A-prefixed reset (RFC: detected)", sb);
            report("A after the A-prefixed reset (RFC: unaffected)", sa);
            f2 = !detected(sb);
            REQUIRE(untouched(sa), "case 2 A unaffected");
            deliver_captured(&n, rb, rb_len);
            report("B after the unmodified reset (control: detected)", sb);
            REQUIRE(detected(sb), "case 2 control (detected)");
        }
    }
    picoquic_free(n.client);
    picoquic_free(n.server);

    /* -- case 3: a context that never sends stateless resets ---------------- */
    setup(&n);
    picoquic_cnx_t *c3 = connect_one(&n);
    picoquic_cnx_t *s3 = c3 ? server_for(&n, c3) : NULL;
    printf("case 3 (restarted client with reset sending disabled): established %s\n",
           s3 ? "yes" : "NO");
    REQUIRE(s3 != NULL, "case 3 connection");
    if (s3) {
        g_client_sends_resets = false;
        client_restart(&n);
        g_client_sends_resets = true;
        bool got = capture_reset(&n, s3);
        printf("  reset produced: %s (sending disabled: none expected)\n",
               got ? "YES" : "no");
        no_send_violated = got;
        REQUIRE(picoquic_get_cnx_state(s3) == picoquic_state_ready,
                "case 3 server connection still up");
    }
    picoquic_free(n.client);
    picoquic_free(n.server);

    if (g_harness_failures) {
        printf("RESULT: harness invalid (%d prerequisite/control failures); "
               "this run proves nothing\n", g_harness_failures);
        return 2;
    }
    printf("RESULT: rebinding detection %s; other-connection prefix detection %s; "
           "no-send policy %s\n", f1 ? "FAILED" : "ok", f2 ? "FAILED" : "ok",
           no_send_violated ? "FAILED" : "ok");
    if (f1 || f2 || no_send_violated) {
        printf("FAILED: picoquic stateless-reset contract\n");
        return 1;
    }
    printf("PASS: pq_stateless_reset_contract\n");
    return 0;
}
