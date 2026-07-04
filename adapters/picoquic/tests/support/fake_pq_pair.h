#ifndef FAKE_PQ_PAIR_H
#define FAKE_PQ_PAIR_H

/*
 * Fake picoquic pair for deterministic adapter conformance testing.
 *
 * Provides link-time stubs for all picoquic functions called by
 * moq_picoquic.c, routed through per-connection outbox queues.
 * pump_once services one side, then delivers its outbox to the
 * peer's moq_pq_callback.
 */

#include <moq/picoquic.h>
#include <moq/session.h>
#include "../../../../tests/conformance/moq_adapter_pair.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Test-only accessors (defined in moq_picoquic.c, not public) */
size_t moq_pq_conn_active_stream_count(const moq_pq_conn_t *c);
size_t moq_pq_conn_tombstone_count(const moq_pq_conn_t *c);

#define FAKE_PQ_MAX_OPS 256

typedef enum {
    FAKE_PQ_OP_STREAM_WRITE,
    FAKE_PQ_OP_DATAGRAM,
    FAKE_PQ_OP_RESET,
    FAKE_PQ_OP_STOP_SENDING,
    FAKE_PQ_OP_CLOSE,
} fake_pq_op_kind_t;

typedef struct {
    fake_pq_op_kind_t kind;
    uint64_t stream_id;
    uint8_t  data[4096];
    size_t   len;
    bool     fin;
    uint64_t error_code;
} fake_pq_op_t;

typedef struct {
    fake_pq_op_t ops[FAKE_PQ_MAX_OPS];
    size_t       count;
    uint64_t     next_uni_id;
    uint64_t     next_bidi_id;
    size_t       opened_bidi;
    bool         block_write;
    bool         drop_datagram;
    uint32_t     datagram_max;   /* negotiated peer max_datagram_frame_size
                                    (0 = DATAGRAM not negotiated) */
    /* Streams the endpoint marked active for pull sending; the pump drains them
     * through prepare_to_send into STREAM_WRITE ops. */
    uint64_t     active[FAKE_PQ_MAX_OPS];
    size_t       active_count;
} fake_pq_side_t;

typedef struct {
    moq_session_t    *client_session;
    moq_session_t    *server_session;
    moq_pq_conn_t    *client_conn;
    moq_pq_conn_t    *server_conn;
    fake_pq_side_t    client_side;
    fake_pq_side_t    server_side;
    uint64_t          now;
    bool              closed;
    bool              fatal;
    uint64_t          fatal_code;
} fake_pq_pair_t;

/* Create a pair with two sessions and two adapters wired together. */
int fake_pq_pair_create(fake_pq_pair_t *pair);
void fake_pq_pair_destroy(fake_pq_pair_t *pair);

/* Pump one round: service both sides and deliver queued ops. */
bool fake_pq_pair_pump_once(fake_pq_pair_t *pair);

/* Create a moq_adapter_pair_t backed by this fake pair. */
moq_adapter_pair_t fake_pq_conformance_create(void);

#ifdef __cplusplus
}
#endif

#endif /* FAKE_PQ_PAIR_H */
