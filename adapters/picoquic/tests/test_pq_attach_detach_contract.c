/*
 * What moq_pq_conn_destroy leaves on a caller-owned connection that is still
 * live. Public API only, simulated time, a fixed-latency in-process link
 * between a picoquic server context and a picoquic client context.
 *
 * The adapter is attached to a ready client connection and destroyed. Destroy
 * clears the connection's callback, so picoquic never calls into the freed
 * adapter (the client_unbind row of test_threaded_terminal_controls proves
 * that under ASan). picoquic's stand-in for a missing callback rejects the
 * events it is given; for stream data, that closes the connection. The two
 * rows pin what picoquic.h tells a caller that keeps the connection:
 *
 *   keep   the caller installs its own callback after destroy. The server's
 *          later stream data and FIN reach it, and the connection stays up
 *          with no local error.
 *   none   the caller installs nothing. The server's later stream data
 *          closes the connection with INTERNAL_ERROR instead of being
 *          queued.
 */
#include <moq/picoquic.h>
#include <moq/session.h>

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

#define MAX_CNX 2
#define LAT_US 10000u
#define MAX_FLIGHT 1024
#define SERVER_UNI_STREAM 3u   /* the first server-initiated unidirectional stream */

static int failures;
#define CHECK(cond) do { if (!(cond)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); failures++; } } while (0)

static uint64_t g_now = 1000000;

/* -- the link ---------------------------------------------------------------- */

typedef struct {
    uint64_t arrive;
    bool to_server;
    size_t len;
    uint8_t bytes[PICOQUIC_MAX_PACKET_SIZE];
} flight_t;

typedef struct {
    picoquic_quic_t *server, *client;
    struct sockaddr_storage server_addr, client_addr;
    flight_t flight[MAX_FLIGHT];
    size_t head, tail;
} net_t;

static struct sockaddr_storage mk_addr(const char *ip, uint16_t port)
{
    struct sockaddr_storage ss;
    struct sockaddr_in *a = (struct sockaddr_in *)&ss;
    memset(&ss, 0, sizeof(ss));
    a->sin_family = AF_INET;
    a->sin_port = htons(port);
    (void)inet_pton(AF_INET, ip, &a->sin_addr);
    return ss;
}

static void deliver_due(net_t *n)
{
    while (n->head != n->tail && n->flight[n->head % MAX_FLIGHT].arrive <= g_now) {
        flight_t *f = &n->flight[n->head % MAX_FLIGHT];
        picoquic_cnx_t *first = NULL;
        n->head++;
        if (f->to_server)
            (void)picoquic_incoming_packet_ex(n->server, f->bytes, f->len,
                (struct sockaddr *)&n->client_addr, (struct sockaddr *)&n->server_addr,
                0, 0, &first, g_now);
        else
            (void)picoquic_incoming_packet_ex(n->client, f->bytes, f->len,
                (struct sockaddr *)&n->server_addr, (struct sockaddr *)&n->client_addr,
                0, 0, &first, g_now);
    }
}

static void send_side(net_t *n, bool from_client)
{
    picoquic_quic_t *q = from_client ? n->client : n->server;
    for (int k = 0; k < 64; k++) {
        uint8_t buf[PICOQUIC_MAX_PACKET_SIZE];
        size_t len = 0;
        struct sockaddr_storage to, from;
        int ifx = 0;
        picoquic_connection_id_t lcid;
        picoquic_cnx_t *last = NULL;
        if (picoquic_prepare_next_packet_ex(q, g_now, buf, sizeof(buf), &len,
                &to, &from, &ifx, &lcid, &last, NULL) != 0 || len == 0)
            return;
        if (n->tail - n->head >= MAX_FLIGHT)
            return;
        flight_t *f = &n->flight[n->tail % MAX_FLIGHT];
        n->tail++;
        f->arrive = g_now + LAT_US;
        f->to_server = from_client;
        f->len = len;
        memcpy(f->bytes, buf, len);
    }
}

static void run_for(net_t *n, uint64_t dur_us)
{
    uint64_t end = g_now + dur_us;
    while (g_now < end) {
        deliver_due(n);
        send_side(n, true);
        send_side(n, false);
        uint64_t next = end;
        if (n->head != n->tail && n->flight[n->head % MAX_FLIGHT].arrive < next)
            next = n->flight[n->head % MAX_FLIGHT].arrive;
        uint64_t w = picoquic_get_next_wake_time(n->server, g_now);
        if (w < next) next = w;
        w = picoquic_get_next_wake_time(n->client, g_now);
        if (w < next) next = w;
        g_now = next > g_now ? next : g_now + 1;
    }
}

/* -- callbacks --------------------------------------------------------------- */

/* Both sides accept every event until the adapter is attached: a context
 * created without a callback refuses every event, the handshake's included. */
static int accept_cb(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                     size_t length, picoquic_call_back_event_t event,
                     void *ctx, void *stream_ctx)
{
    (void)cnx; (void)stream_id; (void)bytes; (void)length; (void)event;
    (void)ctx; (void)stream_ctx;
    return 0;
}

typedef struct {
    size_t bytes;
    bool fin;
    uint64_t stream_id;
    int events;
} app_t;

/* The caller's own callback, installed after the adapter is destroyed. */
static int app_cb(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                  size_t length, picoquic_call_back_event_t event,
                  void *ctx, void *stream_ctx)
{
    app_t *app = ctx;
    (void)cnx; (void)bytes; (void)stream_ctx;
    app->events++;
    if (event == picoquic_callback_stream_data || event == picoquic_callback_stream_fin) {
        app->bytes += length;
        app->stream_id = stream_id;
        if (event == picoquic_callback_stream_fin)
            app->fin = true;
    }
    return 0;
}

/* -- one row ----------------------------------------------------------------- */

static void row(const char *name, bool install_app_cb, const char *cert, const char *key)
{
    net_t *n = calloc(1, sizeof(*n));
    CHECK(n != NULL);
    if (!n)
        return;
    n->server_addr = mk_addr("10.0.0.2", 443);
    n->client_addr = mk_addr("10.0.0.1", 5000);
    n->server = picoquic_create(MAX_CNX, cert, key, NULL, MOQ_PQ_ALPN_DEFAULT,
        accept_cb, NULL, NULL, NULL, NULL, g_now, &g_now, NULL, NULL, 0);
    n->client = picoquic_create(MAX_CNX, NULL, NULL, NULL, MOQ_PQ_ALPN_DEFAULT,
        accept_cb, NULL, NULL, NULL, NULL, g_now, &g_now, NULL, NULL, 0);
    CHECK(n->server != NULL && n->client != NULL);
    if (!n->server || !n->client)
        goto out;
    picoquic_set_null_verifier(n->client);

    picoquic_cnx_t *cc = picoquic_create_cnx(n->client, picoquic_null_connection_id,
        picoquic_null_connection_id, (struct sockaddr *)&n->server_addr, g_now, 0,
        "test.example.com", MOQ_PQ_ALPN_DEFAULT, 1);
    CHECK(cc != NULL && picoquic_start_client_cnx(cc) == 0);
    run_for(n, 2000000);
    picoquic_cnx_t *sc = picoquic_get_first_cnx(n->server);
    CHECK(picoquic_get_cnx_state(cc) == picoquic_state_ready);
    CHECK(sc != NULL);
    if (picoquic_get_cnx_state(cc) != picoquic_state_ready || !sc)
        goto out;

    /* Attach the adapter to the caller's ready connection, then destroy it
     * while the connection is still live. */
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *sess = NULL;
    CHECK(moq_session_create(&scfg, g_now, &sess) == MOQ_OK);
    moq_pq_conn_cfg_t ccfg;
    moq_pq_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.session = sess;
    ccfg.cnx = cc;
    ccfg.alloc = moq_alloc_default();
    moq_pq_conn_t *conn = NULL;
    CHECK(moq_pq_conn_create(&ccfg, &conn) == 0 && conn != NULL);
    CHECK(picoquic_get_callback_context(cc) == (void *)conn);
    if (conn)
        moq_pq_conn_destroy(conn);
    if (sess)
        moq_session_destroy(sess);
    CHECK(picoquic_get_callback_context(cc) == NULL);
    CHECK(picoquic_get_cnx_state(cc) == picoquic_state_ready);

    app_t app;
    memset(&app, 0, sizeof(app));
    if (install_app_cb)
        picoquic_set_callback(cc, app_cb, &app);

    /* The server sends on its first unidirectional stream. */
    static const uint8_t payload[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    CHECK(picoquic_add_to_stream(sc, SERVER_UNI_STREAM, payload, sizeof(payload), 1) == 0);
    run_for(n, 300000);

    picoquic_state_enum st = picoquic_get_cnx_state(cc);
    uint64_t lerr = picoquic_get_local_error(cc);
    printf("%s: client state %d, local error 0x%llx; app callback: %d events, %zu bytes, fin %s\n",
           name, (int)st, (unsigned long long)lerr, app.events, app.bytes, app.fin ? "yes" : "no");
    if (install_app_cb) {
        CHECK(app.bytes == sizeof(payload));
        CHECK(app.fin);
        CHECK(app.stream_id == SERVER_UNI_STREAM);
        CHECK(st == picoquic_state_ready);
        CHECK(lerr == 0);
    } else {
        CHECK(st >= picoquic_state_disconnecting);
        CHECK(lerr == PICOQUIC_TRANSPORT_INTERNAL_ERROR);
    }
out:
    if (n->client) picoquic_free(n->client);
    if (n->server) picoquic_free(n->server);
    free(n);
}

int main(int argc, char **argv)
{
    if (argc != 1 && argc != 3) {
        fprintf(stderr, "usage: %s [cert key]\n", argv[0]);
        return 2;
    }
    setvbuf(stdout, NULL, _IONBF, 0);
    const char *cert = argc == 3 ? argv[1] : MOQ_TEST_CERT_PATH;
    const char *key = argc == 3 ? argv[2] : MOQ_TEST_KEY_PATH;
    row("keep", true, cert, key);
    row("none", false, cert, key);
    printf("%s (%d failures)\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
