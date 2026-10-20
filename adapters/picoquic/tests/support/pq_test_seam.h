/* TEST-ONLY seams into the raw picoquic adapter. Compiled only into test
 * executables that build moq_picoquic.c with MOQ_PICOQUIC_TESTING; never
 * installed, never exported from the shipped library. */
#ifndef MOQ_PQ_TEST_SEAM_H
#define MOQ_PQ_TEST_SEAM_H

#include <moq/picoquic.h>
#include <moq/transport_bridge.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Declare or withdraw MOQ_TRANSPORT_CAP_HOLD_INPUT on the attached endpoint. */
void moq_pq_test_set_hold_input(moq_pq_conn_t *conn, bool on);

/* Connections created while set also declare
 * MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT, so their bridge keeps held-stream
 * records (age, cancellation). Test builds only; no shipped adapter declares
 * that capability. */
void moq_pq_test_create_with_hold_bidi(bool on);

/* Snapshot of one inbound stream's receive state (false if untracked). */
typedef struct moq_pq_test_rx {
    bool           active;
    bool           paused;
    bool           held_input;   /* the adapter owes a refused chunk */
    bool           buf_fin;
    bool           fin_blocked;
    uint64_t       blocked;
    uint64_t       delivered;
    uint64_t       granted;
    uint64_t       budget;
    size_t         held_len;     /* length of the refused chunk (front of buf) */
    bool           held_fin;     /* that chunk's FIN flag */
    bool           release_watch;/* watched for picoquic's stream release */
    bool           released;     /* released while retained work is owed */
    size_t         buf_len;
    const uint8_t *buf;          /* borrowed; valid until the next adapter call */
} moq_pq_test_rx_t;

bool moq_pq_test_rx_state(moq_pq_conn_t *conn, uint64_t stream_id,
                          moq_pq_test_rx_t *out);

/* Inbound streams the adapter currently tracks. */
size_t moq_pq_test_rx_active(moq_pq_conn_t *conn);

/* The endpoint vtable and context the adapter gave its bridge. */
void moq_pq_test_endpoint(moq_pq_conn_t *conn,
                          const moq_transport_endpoint_ops_t **ops,
                          void **ctx);

/* The next `n` per-stream context assignments the adapter makes fail as
 * picoquic would refuse them. */
void moq_pq_test_fail_stream_ctx(int n);

/* Whole-stream terminals the adapter reported from picoquic's stream
 * release. */
uint64_t moq_pq_test_release_terminals(moq_pq_conn_t *conn);

/* Called as each such terminal is reported, before it reaches the bridge. */
extern void (*moq_pq_test_terminal_observer)(moq_pq_conn_t *conn,
                                             uint64_t stream_id);

/* The base of the adapter's receive table (moves when the table grows). */
const void *moq_pq_test_rx_base(moq_pq_conn_t *conn);

/* Whether the bridge still retains work for the stream. */
bool moq_pq_test_bridge_pending(moq_pq_conn_t *conn, uint64_t stream_id);

#endif
