/*
 * One WebTransport session per QUIC connection, over the real h3zero stack
 * in the deterministic simulation.
 *
 * picoquic's WebTransport layer counts sessions per HTTP/3 context: a client
 * cannot open a second session on a connection while one is counted, and a
 * server refuses a second WebTransport CONNECT on the same connection before
 * its path callback runs. The slot is released when the session's control
 * stream is deregistered, which the attach adapter does when it detaches.
 *
 * Rows:
 *   client-second-connect-refused  picowt_connect on the established
 *                                  connection fails; the live session is
 *                                  untouched.
 *   server-second-connect-refused  a second CONNECT that reaches the server
 *                                  is answered with a refusal; the server's
 *                                  path callback never sees it and the live
 *                                  session is untouched.
 *   slot-released-on-detach        destroying the client adapter releases the
 *                                  slot, so a new session may be opened.
 */
#include "pico_wt_harness.h"
#include <moq/moq.h>

#include <stdio.h>
#include <string.h>

static int failures;
static const char *row = "";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", row, __FILE__, __LINE__, \
                #cond); \
        failures++; \
    } } while (0)

typedef struct {
    int connecting;
    int accepted;
    int refused;
} second_ctx_t;

static int second_cb(picoquic_cnx_t *cnx, uint8_t *bytes, size_t length,
                     picohttp_call_back_event_t event,
                     h3zero_stream_ctx_t *stream_ctx, void *app_ctx)
{
    second_ctx_t *s = (second_ctx_t *)app_ctx;
    (void)cnx; (void)bytes; (void)length; (void)stream_ctx;
    if (event == picohttp_callback_connecting) s->connecting++;
    else if (event == picohttp_callback_connect_accepted) s->accepted++;
    else if (event == picohttp_callback_connect_refused) s->refused++;
    return 0;
}

static int setup(pico_wt_harness_t *h, uint8_t cid_byte)
{
    pico_wt_harness_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.cid_byte = cid_byte;
    if (pico_wt_harness_setup(h, &cfg) != 0) return -1;
    return pico_wt_harness_handshake(h);
}

static void check_first_session_alive(pico_wt_harness_t *h)
{
    CHECK(h->client_conn && !moq_pico_wt_conn_is_fatal(h->client_conn));
    CHECK(h->server_conn && !moq_pico_wt_conn_is_fatal(h->server_conn));
    CHECK(h->client_conn && !moq_pico_wt_conn_is_closed(h->client_conn));
    CHECK(h->server_conn && !moq_pico_wt_conn_is_closed(h->server_conn));
    CHECK(moq_session_state(h->client_session) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(h->server_session) == MOQ_SESS_ESTABLISHED);
}

static void client_second_connect_refused(void)
{
    row = "client-second-connect-refused";
    pico_wt_harness_t h;
    CHECK(setup(&h, 0x61) == 0);
    if (h.client_conn && h.server_conn) {
        CHECK(h.client_h3_ctx->nb_wt_connections == 1);
        h3zero_stream_ctx_t *ctrl2 = picowt_set_control_stream(
            h.test_ctx->cnx_client, h.client_h3_ctx);
        CHECK(ctrl2 != NULL);
        second_ctx_t s2;
        memset(&s2, 0, sizeof(s2));
        if (ctrl2 != NULL)
            CHECK(picowt_connect(h.test_ctx->cnx_client, h.client_h3_ctx,
                                 ctrl2, PICOQUIC_TEST_SNI, "/moq",
                                 second_cb, &s2, NULL) != 0);
        CHECK(s2.connecting == 0);
        CHECK(h.client_h3_ctx->nb_wt_connections == 1);
        pico_wt_harness_pump(&h, 200);
        check_first_session_alive(&h);
    }
    pico_wt_harness_cleanup(&h);
}

static void server_second_connect_refused(void)
{
    row = "server-second-connect-refused";
    pico_wt_harness_t h;
    CHECK(setup(&h, 0x62) == 0);
    if (h.client_conn && h.server_conn) {
        int server_connects = h.server_wt.connect_received;
        /* The client's own limit would stop this CONNECT before it is sent;
         * lift it so the request reaches the server. */
        h.client_h3_ctx->nb_wt_connections = 0;
        h3zero_stream_ctx_t *ctrl2 = picowt_set_control_stream(
            h.test_ctx->cnx_client, h.client_h3_ctx);
        CHECK(ctrl2 != NULL);
        second_ctx_t s2;
        memset(&s2, 0, sizeof(s2));
        if (ctrl2 != NULL) {
            CHECK(picowt_connect(h.test_ctx->cnx_client, h.client_h3_ctx,
                                 ctrl2, PICOQUIC_TEST_SNI, "/moq",
                                 second_cb, &s2, NULL) == 0);
            pico_wt_harness_pump(&h, 300);
        }
        CHECK(s2.connecting == 1);
        CHECK(s2.refused == 1 && s2.accepted == 0);
        CHECK(h.server_wt.connect_received == server_connects);
        check_first_session_alive(&h);
    }
    pico_wt_harness_cleanup(&h);
}

static void slot_released_on_detach(void)
{
    row = "slot-released-on-detach";
    pico_wt_harness_t h;
    CHECK(setup(&h, 0x63) == 0);
    if (h.client_conn && h.server_conn) {
        CHECK(h.client_h3_ctx->nb_wt_connections == 1);
        moq_pico_wt_conn_destroy(h.client_conn);
        h.client_conn = NULL;
        CHECK(h.client_h3_ctx->nb_wt_connections == 0);
        h3zero_stream_ctx_t *ctrl2 = picowt_set_control_stream(
            h.test_ctx->cnx_client, h.client_h3_ctx);
        CHECK(ctrl2 != NULL);
        second_ctx_t s2;
        memset(&s2, 0, sizeof(s2));
        if (ctrl2 != NULL) {
            CHECK(picowt_connect(h.test_ctx->cnx_client, h.client_h3_ctx,
                                 ctrl2, PICOQUIC_TEST_SNI, "/moq",
                                 second_cb, &s2, NULL) == 0);
            CHECK(h.client_h3_ctx->nb_wt_connections == 1);
            /* The second session is local bookkeeping only: release it
             * before teardown, as the adapter would on detach. */
            uint64_t ctrl2_id = ctrl2->stream_id;
            picowt_deregister(h.test_ctx->cnx_client, h.client_h3_ctx, ctrl2);
            pico_wt_h3_delete_prefix(h.test_ctx->cnx_client,
                                     h.client_h3_ctx, ctrl2_id);
            CHECK(h.client_h3_ctx->nb_wt_connections == 0);
        }
        CHECK(s2.connecting == 1);
    }
    pico_wt_harness_cleanup(&h);
}

int main(void)
{
    client_second_connect_refused();
    server_second_connect_refused();
    slot_released_on_detach();
    if (failures == 0)
        printf("PASS: pico_wt_one_session\n");
    return failures != 0;
}
