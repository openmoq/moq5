/*
 * The lane group refuses 0-RTT: every lane shares one session-ticket key, so
 * 0-RTT early data -- which a replay could resend to any lane -- is not allowed,
 * while ordinary 1-RTT session resumption is. This proves it with real traffic
 * rather than by reading a config field. A raw picoquic client (so it, not the
 * managed facade, controls resumption) handshakes with a real two-lane group
 * server over loopback and keeps the session ticket the server issues; it then
 * resumes on a second connection. The resumption succeeds -- the ticket is used
 * (resumed_ticket_id is set) -- but 0-RTT is not available on it, because the
 * ticket the refusing server issued carries max_early_data_size 0. Had the
 * group accepted 0-RTT, the ticket would permit it and is_0rtt_available would
 * be true, failing the check.
 *
 * White-box: links the threaded adapter's test-internals archive and reads the
 * client connection's resumption fields, which have no public getters.
 */
#include <moq/picoquic_threaded.h>

#include <picoquic.h>
#include <picoquic_internal.h>   /* cnx->resumed_ticket_id */
#include <picoquic_utils.h>      /* picoquic_null_connection_id */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define ALPN "moqt-16"   /* MOQ_PQ_ALPN_DEFAULT */

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

/* Drive one client connection to readiness, then keep pumping a short while so a
 * post-handshake NEW_SESSION_TICKET is received. Returns 0 when ready. */
static int drive(picoquic_quic_t *quic, picoquic_cnx_t *cnx, int fd,
                 const struct sockaddr *local, uint64_t settle_us)
{
    uint8_t buf[1536];
    uint64_t start = picoquic_current_time();
    uint64_t ready_at = 0;
    while (picoquic_current_time() - start < 5000000) {   /* 5s hard bound */
        uint64_t now = picoquic_current_time();
        /* Send everything the stack has ready this turn. */
        for (int k = 0; k < 16; k++) {
            size_t len = 0;
            struct sockaddr_storage to, from;
            int if_index = 0;
            if (picoquic_prepare_packet(cnx, now, buf, sizeof(buf), &len,
                                        &to, &from, &if_index) != 0)
                return -1;
            if (len == 0) break;
            socklen_t tl = to.ss_family == AF_INET ? sizeof(struct sockaddr_in)
                                                   : sizeof(struct sockaddr_in6);
            (void)sendto(fd, buf, len, 0, (struct sockaddr *)&to, tl);
        }
        if (picoquic_get_cnx_state(cnx) == picoquic_state_disconnected)
            return -1;
        if (picoquic_get_cnx_state(cnx) == picoquic_state_ready) {
            if (ready_at == 0) ready_at = picoquic_current_time();
            else if (picoquic_current_time() - ready_at >= settle_us)
                return 0;
        }
        struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
        if (poll(&pfd, 1, 5) > 0 && (pfd.revents & POLLIN)) {
            struct sockaddr_storage sfrom;
            socklen_t sl = sizeof(sfrom);
            ssize_t got = recvfrom(fd, buf, sizeof(buf), 0,
                                   (struct sockaddr *)&sfrom, &sl);
            if (got > 0)
                (void)picoquic_incoming_packet(quic, buf, (size_t)got,
                                               (struct sockaddr *)&sfrom,
                                               (struct sockaddr *)local, 0, 0,
                                               picoquic_current_time());
        }
    }
    return picoquic_get_cnx_state(cnx) == picoquic_state_ready ? 0 : -1;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    /* A real two-lane group server (it refuses 0-RTT). */
    int port = pick_free_udp_port();
    CHECK(port > 0);
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
    cfg.port = port;
    cfg.lane_count = 2;
    cfg.max_connections = 8;
    cfg.on_lane_pump = noop_pump;
    moq_pq_threaded_t *srv = NULL;
    CHECK(moq_pq_threaded_create(&cfg, &srv) == MOQ_OK);
    CHECK(srv != NULL);
    if (!srv) { printf("FAILED: no server [pq_zero_rtt_refused]\n"); return 1; }

    /* A raw client with its own in-memory ticket store, so the ticket from the
     * first connection is there to resume the second. */
    uint64_t now = picoquic_current_time();
    /* A ticket file name is what makes picoquic install the client's
     * save-ticket callback, so the ticket the server issues is kept for the
     * resumption below. The file is this run's alone and removed at the end. */
    char ticket_file[80];
    snprintf(ticket_file, sizeof(ticket_file), "moq_0rtt_tickets_%d.bin",
             (int)getpid());
    (void)unlink(ticket_file);
    picoquic_quic_t *cq = picoquic_create(2, NULL, NULL, NULL, NULL, NULL, NULL,
                                          NULL, NULL, NULL, now, NULL,
                                          ticket_file, NULL, 0);
    CHECK(cq != NULL);
    if (!cq) {
        (void)moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        printf("FAILED: no client quic [pq_zero_rtt_refused]\n");
        return 1;
    }
    picoquic_set_null_verifier(cq);

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(fd >= 0);
    struct sockaddr_in local;
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(fd, (struct sockaddr *)&local, sizeof(local)) == 0);
    socklen_t ll = sizeof(local);
    CHECK(getsockname(fd, (struct sockaddr *)&local, &ll) == 0);

    struct sockaddr_in server;
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server.sin_port = htons((uint16_t)port);

    /* First connection: handshake, then settle so the session ticket arrives. */
    picoquic_cnx_t *c1 = picoquic_create_cnx(cq, picoquic_null_connection_id,
        picoquic_null_connection_id, (struct sockaddr *)&server,
        picoquic_current_time(), 0, "test", ALPN, 1);
    CHECK(c1 != NULL);
    if (c1) {
        /* Offer early data on the first connection too, so the session the
         * server tickets is one the client could resume with 0-RTT; whether it
         * may is then the server's policy, tested on the second connection. */
        static const uint8_t early[8] = { 't', 'e', 's', 't', '0', 'r', 't', 't' };
        (void)picoquic_add_to_stream(c1, 0, early, sizeof(early), 0);
        CHECK(picoquic_start_client_cnx(c1) == 0);
    }
    int r1 = c1 ? drive(cq, c1, fd, (struct sockaddr *)&local, 800000) : -1;
    CHECK(r1 == 0);   /* reached ready */

    /* Second connection: picoquic finds the stored ticket for this server and
     * resumes. 0-RTT is offered only if the ticket permits it. */
    picoquic_cnx_t *c2 = picoquic_create_cnx(cq, picoquic_null_connection_id,
        picoquic_null_connection_id, (struct sockaddr *)&server,
        picoquic_current_time(), 0, "test", ALPN, 1);
    CHECK(c2 != NULL);
    int zero_rtt = 0;
    if (c2) {
        /* Offer early data so the client sets up its 0-RTT send key when the
         * ticket permits it. The refusing server's ticket carries
         * max_early_data_size 0, so the client cannot, and 0-RTT is unavailable.
         * The key is established at start, for the early flight, and may be
         * dropped once the handshake confirms, so observe it here. */
        static const uint8_t early[8] = { 't', 'e', 's', 't', '0', 'r', 't', 't' };
        (void)picoquic_add_to_stream(c2, 0, early, sizeof(early), 0);
        CHECK(picoquic_start_client_cnx(c2) == 0);
        zero_rtt = picoquic_is_0rtt_available(c2);
    }
    int r2 = c2 ? drive(cq, c2, fd, (struct sockaddr *)&local, 50000) : -1;
    CHECK(r2 == 0);

    /* resumed_ticket_id is stamped when the ticket is offered, before the
     * server accepts it, so it is only a prerequisite; is_psk_handshake is the
     * proof the server accepted the ticket rather than falling back to a full
     * handshake. 0-RTT is refused: no send key was set up (checked at start,
     * before it is dropped post-handshake) and no early data was accepted. */
    int ticket_offered = c2 && c2->resumed_ticket_id != 0;
    int psk_resumed = c2 && picoquic_tls_is_psk_handshake(c2);
    int early_accepted = c2 && c2->is_0RTT_accepted;
    printf("zero_rtt_refused: ticket_offered=%d psk_resumed=%d "
           "zero_rtt_available=%d early_accepted=%d\n",
           ticket_offered, psk_resumed, zero_rtt, early_accepted);
    CHECK(ticket_offered);
    CHECK(psk_resumed);
    CHECK(!zero_rtt);
    CHECK(!early_accepted);

    close(fd);
    picoquic_free(cq);
    (void)unlink(ticket_file);
    (void)moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);

    if (failures) {
        printf("FAILED: %d check(s) [pq_zero_rtt_refused]\n", failures);
        return 1;
    }
    printf("PASS: pq_zero_rtt_refused\n");
    return 0;
}
