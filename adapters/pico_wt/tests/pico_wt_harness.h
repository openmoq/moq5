#ifndef MOQ_PICO_WT_HARNESS_H
#define MOQ_PICO_WT_HARNESS_H

/*
 * Internal test/demo harness for the picoquic WebTransport adapter.
 *
 * Owns the full deterministic-sim lifecycle a pico WT pair needs:
 * the picoquic test context, h3zero contexts, the WebTransport
 * CONNECT handshake, a MoQ session pair, and a moq_pico_wt_conn_t
 * adapter pair — plus sim-round pumping and ASAN-safe teardown.
 *
 * Shared by the loopback test and the conformance pair so the
 * lifecycle lives in exactly one place. It is also the readable
 * reference for what a future real example / managed facade would
 * have to own (minus the deterministic sim).
 *
 * Test/demo only. NOT installed. Depends on picoquic's test
 * simulation (picoquic-test / tls_api), which is source-tree only.
 */

#include "../pico_wt_adapter.h"
#include <picoquictest_internal.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>

/* Per-endpoint WT CONNECT state captured by the control-stream
 * callback (CONNECT received / accepted + the h3zero handles). */
typedef struct {
    h3zero_callback_ctx_t *h3_ctx;
    h3zero_stream_ctx_t   *ctrl_ctx;
    int connect_received;
    int connect_accepted;
} pico_wt_wt_cb_ctx_t;

typedef struct {
    picoquic_test_tls_api_ctx_t *test_ctx;
    uint64_t now;
    uint64_t loss_mask;

    pico_wt_wt_cb_ctx_t client_wt;
    pico_wt_wt_cb_ctx_t server_wt;
    h3zero_callback_ctx_t *client_h3_ctx;
    h3zero_stream_ctx_t   *client_ctrl_ctx;

    picohttp_server_path_item_t  path_table[1];
    picohttp_server_parameters_t server_param;

    moq_version_t       version;
    moq_session_t      *client_session;
    moq_session_t      *server_session;
    moq_pico_wt_conn_t *client_conn;
    moq_pico_wt_conn_t *server_conn;
} pico_wt_harness_t;

typedef struct {
    uint8_t  cid_byte;                 /* CID discriminator (4th byte) */
    uint64_t server_goaway_timeout_us; /* server drain timeout; 0=off */
    uint32_t request_capacity;         /* both sessions; 0 → 16 */
    uint32_t max_events;               /* session event queue; 0 -> default */
    moq_version_t version;             /* both sessions; 0 -> draft-16 */
} pico_wt_harness_cfg_t;

/* picohttp callback for the WT control stream on both ends:
 * declares the stream prefix on CONNECT and records accept state. */
int pico_wt_harness_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes,
    size_t length, picohttp_call_back_event_t event,
    h3zero_stream_ctx_t *stream_ctx, void *app_ctx);

/*
 * Establish QUIC + H3 + WT CONNECT and create the MoQ session +
 * adapter pair. Zeroes *h first, so the caller need not pre-init it.
 * Returns 0 on success, -1 on failure; on failure the partial state
 * is safe to pass to pico_wt_harness_cleanup.
 */
int pico_wt_harness_setup(pico_wt_harness_t *h,
                          const pico_wt_harness_cfg_t *cfg);

/*
 * One sim round: service both adapters, advance time up to
 * `time_limit`, service both again. Returns the tls_api rc (0 = ok);
 * sets *was_active when packets moved. Does NOT check fatal.
 */
int pico_wt_harness_sim_round(pico_wt_harness_t *h,
                              uint64_t time_limit, int *was_active);

/* Pump up to `ms` of sim time or until 10 consecutive idle rounds. */
int pico_wt_harness_pump(pico_wt_harness_t *h, uint64_t ms);

/* Pump up to `ms` of sim time or until `target`'s bridge closes. */
int pico_wt_harness_pump_until_closed(pico_wt_harness_t *h, uint64_t ms,
                                      moq_pico_wt_conn_t *target);

/*
 * Start the client session and drive the MoQ setup handshake to
 * SETUP_COMPLETE on both ends, draining events. Returns 0 on
 * success, -1 on timeout.
 */
int pico_wt_harness_handshake(pico_wt_harness_t *h);

/*
 * ASAN-safe teardown: adapters (detach from picoquic/h3zero) →
 * sessions → tls context. Does NOT free *h. Safe on partial setup.
 */
void pico_wt_harness_cleanup(pico_wt_harness_t *h);

#endif /* MOQ_PICO_WT_HARNESS_H */
