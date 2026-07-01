/*
 * WebTransport protocol-token negotiation through pico_wt / vendored
 * h3zero, end-to-end in the deterministic sim.
 *
 * Verifies the mechanics the endpoint version negotiation stands on:
 *   - the client emits WT-Available-Protocols via the existing
 *     picowt_connect() parameter (e.g. "moqt-18, moqt-16");
 *   - a STRICT server (the moxygen/moqx class that refuses a missing or
 *     non-overlapping offer) selects a token with the vendored
 *     picowt_select_wt_protocol() helper, which writes the selection into
 *     the CONNECT response's WT-Protocol header;
 *   - the client reads the selected token back at
 *     picohttp_callback_connect_accepted from
 *     stream_ctx->ps.stream_state.header.wt_protocol -- i.e. BEFORE any MoQ
 *     session exists (pico_wt_managed creates the session inside this same
 *     callback, after the token is readable), so setting
 *     moq_session_cfg_t.version from the token is a contained facade change;
 *   - selection honors the client's offer order (preference order), and a
 *     no-overlap or absent offer is a deterministic refusal
 *     (picohttp_callback_connect_refused), not a hang.
 *
 * No moq session is created here on purpose: this test isolates the WT
 * negotiation mechanics from the managed-facade session lifecycle.
 */

#include "../pico_wt_adapter.h"
#include <picoquictest_internal.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>
#include <demoserver.h>
#include <picoquic_utils.h>

#include <stdio.h>
#include <string.h>

#ifndef PICOQUIC_SOURCE_DIR
#define PICOQUIC_SOURCE_DIR "."
#endif

static int failures = 0;
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        failures++; \
    } } while (0)

/* -- negotiation callback state ------------------------------------- */

typedef struct {
    const char *server_supported;   /* the strict server's protocol list */
    int connect_received;           /* server: CONNECT admitted */
    int strict_rejected;            /* server: offer missing / no overlap */
    int offer_seen;                 /* server: WT-Available-Protocols present */
    int connect_accepted;           /* client: 2xx observed */
    int connect_refused;            /* client: non-2xx observed */
    char selected[64];              /* client: WT-Protocol readback at accept */
} nego_ctx_t;

static int nego_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes, size_t length,
                      picohttp_call_back_event_t event,
                      h3zero_stream_ctx_t *stream_ctx, void *app_ctx)
{
    nego_ctx_t *n = (nego_ctx_t *)app_ctx;
    (void)bytes; (void)length;

    switch (event) {
    case picohttp_callback_connect: {
        /* SERVER. Strict-relay behavior: the offer header MUST be present
         * and MUST overlap our supported list; otherwise refuse the CONNECT
         * (return -1 => h3zero answers an explicit non-2xx). On a match,
         * picowt_select_wt_protocol stores the selection in
         * stream_state.wt_protocol; h3zero's response frame carries it. */
        if (stream_ctx->ps.stream_state.header.wt_available_protocols != NULL)
            n->offer_seen = 1;
        if (!n->offer_seen ||
            picowt_select_wt_protocol(stream_ctx, n->server_supported) != 0) {
            n->strict_rejected = 1;
            return -1;
        }
        h3zero_callback_ctx_t *h3 =
            (h3zero_callback_ctx_t *)picoquic_get_callback_context(cnx);
        if (h3zero_declare_stream_prefix(h3, stream_ctx->stream_id,
                                         nego_wt_cb, n) != 0)
            return -1;
        n->connect_received = 1;
        break;
    }
    case picohttp_callback_connect_accepted: {
        /* CLIENT. The selected token is readable RIGHT HERE, before any MoQ
         * session would be created (pico_wt_managed builds its session
         * later in this same callback) -- the readback point the endpoint
         * version contract needs. */
        const char *tok =
            (const char *)stream_ctx->ps.stream_state.header.wt_protocol;
        if (tok)
            snprintf(n->selected, sizeof(n->selected), "%s", tok);
        n->connect_accepted = 1;
        break;
    }
    case picohttp_callback_connect_refused:
        n->connect_refused = 1;
        break;
    default:
        break;
    }
    return 0;
}

/* -- one sim scenario -------------------------------------------------- *
 * offer == NULL means the client sends no WT-Available-Protocols header. */

static int run_scenario(uint8_t cid_byte, const char *offer,
                        const char *server_supported,
                        nego_ctx_t *cli, nego_ctx_t *srv)
{
    picoquic_test_tls_api_ctx_t *test_ctx = NULL;
    h3zero_callback_ctx_t *client_h3 = NULL;
    h3zero_stream_ctx_t *client_ctrl = NULL;
    picohttp_server_parameters_t server_param;
    picohttp_server_path_item_t path_table[1];
    uint64_t now = 0;
    uint64_t loss = 0;
    int ret = 0;

    memset(cli, 0, sizeof(*cli));
    memset(srv, 0, sizeof(*srv));
    srv->server_supported = server_supported;

    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    picoquic_connection_id_t cid = {
        {0x77, 0x74, 0x9e, cid_byte, 0, 0, 0, 0}, 8};

    path_table[0] = (picohttp_server_path_item_t){
        .path = "/moq", .path_length = 4,
        .path_callback = nego_wt_cb, .path_app_ctx = srv};
    memset(&server_param, 0, sizeof(server_param));
    server_param.path_table = path_table;
    server_param.path_table_nb = 1;

    if (tls_api_init_ctx_ex(&test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, "h3", &now, NULL, NULL,
                            0, 1, 0, &cid) != 0)
        return -1;

    picoquic_set_default_idle_timeout(test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(test_ctx->qserver, 30000);
    picowt_set_default_transport_parameters(test_ctx->qserver);
    picowt_set_transport_parameters(test_ctx->cnx_client);

    if (picowt_prepare_client_cnx(test_ctx->qclient, NULL,
            &test_ctx->cnx_client, &client_h3, &client_ctrl,
            now, PICOQUIC_TEST_SNI) != 0) {
        ret = -1;
        goto done;
    }

    if (picowt_connect(test_ctx->cnx_client, client_h3, client_ctrl,
                       PICOQUIC_TEST_SNI, "/moq", nego_wt_cb, cli,
                       offer) != 0) {
        ret = -1;
        goto done;
    }

    picoquic_set_alpn_select_fn_v2(test_ctx->qserver,
        picoquic_demo_server_callback_select_alpn);
    picoquic_set_default_callback(test_ctx->qserver,
        h3zero_callback, &server_param);

    if (picoquic_start_client_cnx(test_ctx->cnx_client) != 0) {
        ret = -1;
        goto done;
    }
    if (tls_api_connection_loop(test_ctx, &loss, 0, &now) != 0) {
        ret = -1;
        goto done;
    }

    /* Drive the sim until the client observed a terminal CONNECT outcome. */
    for (int i = 0; i < 2048; i++) {
        if (cli->connect_accepted || cli->connect_refused)
            break;
        int was_active = 0;
        tls_api_one_sim_round(test_ctx, &now, now + 25000, &was_active);
    }

done:
    if (test_ctx)
        tls_api_delete_ctx(test_ctx);
    return ret;
}

int main(void)
{
    nego_ctx_t cli, srv;

    /* == A. Offer {18, 16}; strict server supports only 16 ============ */
    CHECK(run_scenario(0x01, "moqt-18, moqt-16", "moqt-16", &cli, &srv) == 0);
    CHECK(srv.offer_seen);
    CHECK(srv.connect_received);
    CHECK(!srv.strict_rejected);
    CHECK(cli.connect_accepted);
    CHECK(!cli.connect_refused);
    CHECK(strcmp(cli.selected, "moqt-16") == 0);

    /* == B. Offer order is client preference: server supports both ===== *
     * picowt_select_wt_protocol walks the OFFERED list in order, so the
     * first offered token the server supports wins -- the property the
     * endpoint's "AUTO offers newest-preferred" contract relies on. */
    CHECK(run_scenario(0x02, "moqt-18, moqt-16",
                       "moqt-16, moqt-18", &cli, &srv) == 0);
    CHECK(cli.connect_accepted);
    CHECK(strcmp(cli.selected, "moqt-18") == 0);

    /* == C. No offer header: the strict server refuses ================= */
    CHECK(run_scenario(0x03, NULL, "moqt-16", &cli, &srv) == 0);
    CHECK(!srv.offer_seen);
    CHECK(srv.strict_rejected);
    CHECK(cli.connect_refused);
    CHECK(!cli.connect_accepted);
    CHECK(cli.selected[0] == '\0');

    /* == D. No overlap: deterministic refusal, not a hang ============== */
    CHECK(run_scenario(0x04, "moqt-99", "moqt-16, moqt-18", &cli, &srv) == 0);
    CHECK(srv.offer_seen);
    CHECK(srv.strict_rejected);
    CHECK(cli.connect_refused);
    CHECK(!cli.connect_accepted);

    if (failures == 0)
        printf("PASS: pico_wt_protocol_nego\n");
    return failures != 0;
}
