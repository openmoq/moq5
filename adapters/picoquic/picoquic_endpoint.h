#ifndef MOQ_PICOQUIC_ENDPOINT_H
#define MOQ_PICOQUIC_ENDPOINT_H

/*
 * Private header for the picoquic endpoint binding.
 * Shared between picoquic_endpoint.c and moq_picoquic.c.
 * Not installed.
 */

#include <moq/transport_bridge.h>
#include <picoquic.h>
#include <stdbool.h>
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

/*
 * True once picoquic has handed this connection's picoquic_cnx_t back and the
 * adapter has dropped every copy of the pointer (implemented in
 * moq_picoquic.c). PRIVATE: deliberately not in <moq/picoquic.h>, because it
 * describes an internal ownership fact, not application API.
 *
 * This is NOT moq_pq_conn_is_closed(), which reports MoQ session/bridge
 * terminal state. The two are independent: a session can be closed while the
 * cnx is alive (a ready-state APPLICATION_CLOSE leaves picoquic in
 * picoquic_state_closing_received), and a released cnx is one that no longer
 * exists at all.
 *
 * The threaded adapter keeps its own duplicate cnx pointer in step with this
 * fact via moq_pq_conn_cfg_t.after_callback. A NULL conn reports true.
 */
bool moq_pq_conn_cnx_released(const moq_pq_conn_t *conn);

/* Returns 0 on success, -1 on allocation failure (queue create). queue_cap
 * bounds the outbound queue (0 = MOQ_PQ_SEND_QUEUE_CAP_DEFAULT). */
int pq_endpoint_init(moq_transport_endpoint_ops_t *ops,
                     pq_endpoint_ctx_t *ep_ctx,
                     picoquic_cnx_t *cnx,
                     const moq_alloc_t *alloc,
                     uint64_t queue_cap);

/* Release the outbound queue (decref retained rcbufs, free copies). */
void pq_endpoint_cleanup(pq_endpoint_ctx_t *ep_ctx);

/* Service a picoquic_callback_prepare_to_send for `stream_id`: copy up to
 * `max` queued bytes into picoquic's buffer via `provide_ctx`, setting FIN and
 * still-active as the queue dictates. Reneges (0,0,0) when nothing is queued.
 * Returns true when this call emptied the stream's queue. */
bool pq_endpoint_on_prepare_to_send(pq_endpoint_ctx_t *ep_ctx,
                                    uint64_t stream_id,
                                    void *provide_ctx, size_t max);

/* Apply the stream priorities recorded since the last call; call before
 * picoquic sends. Returns 0, or -1 when picoquic refuses one (fatal). */
int pq_endpoint_apply_priorities(pq_endpoint_ctx_t *ep_ctx);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_ENDPOINT_H */
