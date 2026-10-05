/* TEST-ONLY seams into the raw picoquic adapter. Compiled only into test
 * executables that build moq_picoquic.c with MOQ_PICOQUIC_TESTING; never
 * installed, never exported from the shipped library. */
#ifndef MOQ_PQ_TEST_SEAM_H
#define MOQ_PQ_TEST_SEAM_H

#include <moq/picoquic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Declare or withdraw MOQ_TRANSPORT_CAP_HOLD_INPUT on the attached endpoint. */
void moq_pq_test_set_hold_input(moq_pq_conn_t *conn, bool on);

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
    size_t         buf_len;
    const uint8_t *buf;          /* borrowed; valid until the next adapter call */
} moq_pq_test_rx_t;

bool moq_pq_test_rx_state(moq_pq_conn_t *conn, uint64_t stream_id,
                          moq_pq_test_rx_t *out);

#endif
