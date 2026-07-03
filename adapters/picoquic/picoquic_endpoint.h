#ifndef MOQ_PICOQUIC_ENDPOINT_H
#define MOQ_PICOQUIC_ENDPOINT_H

/*
 * Private header for the picoquic endpoint binding.
 * Shared between picoquic_endpoint.c and moq_picoquic.c.
 * Not installed.
 */

#include <moq/transport_bridge.h>
#include <picoquic.h>
#include "../common/moq_pq_send_gate.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    picoquic_cnx_t *cnx;
    /* Bounds the bytes buffered in picoquic's send_queue (avoids the O(n^2)
     * tail-walk on deep streams). See moq_pq_send_gate.h. */
    moq_pq_send_gate_t send_gate;
} pq_endpoint_ctx_t;

void pq_endpoint_init(moq_transport_endpoint_ops_t *ops,
                       pq_endpoint_ctx_t *ep_ctx,
                       picoquic_cnx_t *cnx);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_ENDPOINT_H */
