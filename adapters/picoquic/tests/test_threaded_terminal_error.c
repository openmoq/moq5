/*
 * A client failure before SETUP preserves the QUIC transport error: local
 * first, otherwise remote. Application errors have a separate code space.
 *
 * Rows: deferred/eager session x peer transport/TLS, local precedence,
 * no transport error, application-only error, and an already-latched fatal.
 * The real packet-loop callback and public observers run synchronously over
 * a real provider connection. No socket, thread, handshake or sleep is used.
 */
#include "../pq_threaded_internal.h"
#include <picoquic_internal.h>

#include <stdio.h>
#include <string.h>

static int failures;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    failures++; \
} } while (0)

typedef struct {
    const char *name;
    uint64_t local_error;
    uint64_t remote_error;
    uint64_t application_error;
    uint64_t expected_error;
    bool already_fatal;
} row_t;

static int observe_terminal(moq_pq_threaded_t *t,
                            moq_pq_threaded_lane_t *lane,
                            uint64_t now_us, void *ctx)
{
    (void)lane;
    (void)now_us;
    unsigned *calls = ctx;
    (*calls)++;
    CHECK(moq_pq_threaded_is_fatal(t));
    return 0;
}

static void run_row(const row_t *row, bool eager)
{
    int before = failures;
    uint64_t now_us = 0;
    picoquic_quic_t *quic = picoquic_create(1, NULL, NULL, NULL,
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL, NULL, NULL, NULL,
        now_us, &now_us, NULL, NULL, 0);
    CHECK(quic != NULL);
    if (!quic) return;

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    peer.sin_port = htons(4433);
    picoquic_cnx_t *cnx = picoquic_create_client_cnx(quic,
        (struct sockaddr *)&peer, now_us, 0, "localhost",
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL);
    CHECK(cnx != NULL);
    if (!cnx) { picoquic_free(quic); return; }

    moq_pq_threaded_t t;
    memset(&t, 0, sizeof(t));
    t.alloc = *moq_alloc_default();
    t.perspective = MOQ_PERSPECTIVE_CLIENT;
    t.quic = quic;
    t.active_cnx = cnx;
    t.lane.owner = &t;
    t.client_deferred = !eager;
    unsigned pumps = 0;
    t.on_lane_pump = observe_terminal;
    t.on_lane_pump_ctx = &pumps;
    int rc = pthread_mutex_init(&t.mutex, NULL);
    CHECK(rc == 0);
    if (rc != 0) { picoquic_free(quic); return; }
    rc = pthread_cond_init(&t.cond, NULL);
    CHECK(rc == 0);
    if (rc != 0) {
        pthread_mutex_destroy(&t.mutex);
        picoquic_free(quic);
        return;
    }

    if (eager) {
        moq_session_cfg_t scfg;
        moq_session_cfg_init(&scfg, moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        moq_result_t mr = moq_session_create(&scfg, now_us, &t.session);
        CHECK(mr == MOQ_OK);
        if (mr != MOQ_OK) goto cleanup;
        moq_pq_conn_cfg_t ccfg;
        moq_pq_conn_cfg_init(&ccfg);
        ccfg.session = t.session;
        ccfg.cnx = cnx;
        ccfg.alloc = moq_alloc_default();
        rc = moq_pq_conn_create(&ccfg, &t.conn);
        CHECK(rc == 0);
        if (rc != 0) goto cleanup;
    }

    cnx->local_error = row->local_error;
    cnx->remote_error = row->remote_error;
    cnx->remote_application_error = row->application_error;
    cnx->cnx_state = picoquic_state_disconnected;
    if (row->already_fatal) {
        t.fatal = true;
        t.fatal_code = 17;
        t.terminal_error = row->expected_error;
    }
    CHECK(moq_pq_threaded_loop_callback(quic,
        picoquic_packet_loop_after_receive, &t, NULL) ==
        PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP);
    CHECK(moq_pq_threaded_is_fatal(&t));
    CHECK(moq_pq_threaded_fatal_code(&t) == (row->already_fatal ? 17u : 0u));
    CHECK(moq_pq_threaded_terminal_error(&t) == row->expected_error);
    CHECK(moq_pq_threaded_wait(&t, 0) == MOQ_ERR_CLOSED);
    CHECK(pumps == 1);
    CHECK(t.loop_exited);

cleanup:
    moq_pq_conn_destroy(t.conn);
    moq_session_destroy(t.session);
    picoquic_free(quic);
    CHECK(pthread_cond_destroy(&t.cond) == 0);
    CHECK(pthread_mutex_destroy(&t.mutex) == 0);
    printf("%s: %s %s\n", failures == before ? "PASS" : "FAIL",
           eager ? "eager" : "deferred", row->name);
}

int main(void)
{
    static const row_t rows[] = {
        { "peer transport close", 0, 0x0c, 0, 0x0c, false },
        { "peer certificate alert", 0, 0x130, 0, 0x130, false },
        { "peer TLS alert", 0, 0x178, 0, 0x178, false },
        { "local error precedence", 0x12a, 0x0c, 0, 0x12a, false },
        { "local transport error", 0x01, 0, 0, 0x01, false },
        { "no recorded error", 0, 0, 0, 0, false },
        { "application code is not a TLS alert", 0, 0, 0x130, 0, false },
        { "fatal latch preserved", 0x01, 0x0c, 0, 0x178, true },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        run_row(&rows[i], false);
        run_row(&rows[i], true);
    }
    return failures ? 1 : 0;
}
