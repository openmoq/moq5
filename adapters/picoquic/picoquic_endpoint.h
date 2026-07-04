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

typedef struct moq_pq_conn moq_pq_conn_t;

typedef struct {
    picoquic_cnx_t      *cnx;
    /* Adapter-owned outbound queue: MoQ stream bytes are held here and copied
     * into picoquic's packet buffer from prepare_to_send (pull model). */
    moq_pq_send_queue_t *queue;
    /* Telemetry (characterization): prepare_to_send calls and bytes provided. */
    uint64_t             prepare_count;
    uint64_t             provided_bytes;
} pq_endpoint_ctx_t;

/* Send-path telemetry snapshot (characterization; not product API). */
typedef struct {
    uint64_t prepare_count;    /* prepare_to_send callbacks serviced */
    uint64_t provided_bytes;   /* bytes copied into picoquic buffers */
    uint64_t queue_high_water; /* peak adapter-queue backlog bytes */
    uint64_t queue_would_block;/* pushes refused for the queue cap */
} moq_pq_send_stats_t;

/* Read the endpoint's send-path telemetry (endpoint counters + its queue). */
void pq_endpoint_get_stats(const pq_endpoint_ctx_t *ep_ctx,
                           moq_pq_send_stats_t *out);

/* Read a connection's send-path telemetry (implemented in moq_picoquic.c;
 * declared here so the threaded adapter can aggregate across connections). */
void moq_pq_conn_get_send_stats(const moq_pq_conn_t *conn,
                                moq_pq_send_stats_t *out);

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
