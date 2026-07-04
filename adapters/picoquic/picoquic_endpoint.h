#ifndef MOQ_PICOQUIC_ENDPOINT_H
#define MOQ_PICOQUIC_ENDPOINT_H

/*
 * Private header for the picoquic endpoint binding.
 * Shared between picoquic_endpoint.c and moq_picoquic.c.
 * Not installed.
 */

#include <moq/transport_bridge.h>
#include <picoquic.h>
#include "../common/moq_pq_send_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    picoquic_cnx_t      *cnx;
    /* Adapter-owned outbound queue: MoQ stream bytes are held here and copied
     * into picoquic's packet buffer from prepare_to_send (pull model). */
    moq_pq_send_queue_t *queue;
} pq_endpoint_ctx_t;

/* Returns 0 on success, -1 on allocation failure (queue create). */
int pq_endpoint_init(moq_transport_endpoint_ops_t *ops,
                     pq_endpoint_ctx_t *ep_ctx,
                     picoquic_cnx_t *cnx,
                     const moq_alloc_t *alloc);

/* Release the outbound queue (decref retained rcbufs, free copies). */
void pq_endpoint_cleanup(pq_endpoint_ctx_t *ep_ctx);

/* Service a picoquic_callback_prepare_to_send for `stream_id`: copy up to
 * `max` queued bytes into picoquic's buffer via `provide_ctx`, setting FIN and
 * still-active as the queue dictates. Reneges (0,0,0) when nothing is queued. */
void pq_endpoint_on_prepare_to_send(pq_endpoint_ctx_t *ep_ctx,
                                    uint64_t stream_id,
                                    void *provide_ctx, size_t max);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_ENDPOINT_H */
