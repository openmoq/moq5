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

/* A RESET_STREAM_AT accepted from the bridge whose reliable prefix picoquic
 * has not pulled from the send queue yet (picoquic refuses a reliable size
 * beyond the bytes it has sent). */
typedef struct {
    uint64_t sid;
    uint64_t error_code;
    uint64_t reliable_size;
} pq_deferred_reset_t;

typedef struct {
    picoquic_cnx_t      *cnx;
    moq_alloc_t          alloc;
    /* Adapter-owned outbound queue: MoQ stream bytes are held here and copied
     * into picoquic's packet buffer from prepare_to_send (pull model). */
    moq_pq_send_queue_t *queue;
    /* Telemetry (characterization): prepare_to_send calls and bytes provided. */
    uint64_t             prepare_count;
    uint64_t             provided_bytes;
    /* Reliable resets waiting for their prefix to be sent, and the number of
     * RESET_STREAM_AT frames issued (telemetry and test witness). */
    pq_deferred_reset_t *deferred;
    size_t               deferred_count;
    size_t               deferred_cap;
    uint64_t             reliable_resets;
    /* Receive-side ownership hooks of the attaching adapter (NULL: none).
     * rx_terminal_discard: asked before a STOP_SENDING; true when the
     * adapter's own record shows the stream's receive half already terminal
     * at the transport, in which case the adapter has discarded what it held
     * for the stream and no STOP_SENDING is sent. rx_stopped: told after an
     * accepted STOP_SENDING, so the adapter drops what it held or arranges
     * to learn how the stopped receive half ends; nonzero when it cannot,
     * and the STOP is then a failure. rx_rebind: told after any call that
     * clears picoquic's per-stream context (a reset, an active mark), so the
     * adapter restores what a stopped receive half needs; nonzero likewise
     * fails the call. */
    bool               (*rx_terminal_discard)(void *owner, uint64_t stream_id);
    /* Provider-confirmed receive-half terminal on STOP (not whole-stream
     * closure): discard only held/refused input, without installing a watch. */
    void               (*rx_stop_terminal)(void *owner, uint64_t stream_id);
    int                (*rx_stopped)(void *owner, uint64_t stream_id);
    int                (*rx_rebind)(void *owner, uint64_t stream_id);
    void                *rx_owner;
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

/*
 * True while the connection's bridge holds an outbound action the transport
 * has not taken (moq_transport_bridge_has_outbound_pending), which a graceful
 * drain must wait out (implemented in moq_picoquic.c). PRIVATE, like
 * moq_pq_conn_cnx_released. A NULL conn reports false. Network thread only.
 */
bool moq_pq_conn_outbound_pending(const moq_pq_conn_t *conn);

/*
 * How many picoquic callbacks other than prepare_to_send the connection has
 * received: stream data, terminals, datagrams, closes. A count unchanged
 * since a service pass means the connection has no new input for the next
 * one. PRIVATE, like moq_pq_conn_cnx_released. A NULL conn reports 0.
 * Network thread only.
 */
uint64_t moq_pq_conn_event_count(const moq_pq_conn_t *conn);

/*
 * True once the session's MOQ_EVENT_SESSION_CLOSED has been transferred to the
 * application by a poll (moq_transport_bridge_terminal_facts' observed fact),
 * not merely queued. PRIVATE, like moq_pq_conn_cnx_released. A NULL conn
 * reports false. Network thread only.
 */
bool moq_pq_conn_terminal_observed(const moq_pq_conn_t *conn);

/*
 * Stop routing picoquic callbacks to this adapter and drop its copy of the
 * cnx, leaving the bridge and session in place. For a connection whose
 * transport the owner has finished with -- closed by the peer, or closed with
 * picoquic_close() by the owner -- while the session must stay readable.
 * Idempotent; a no-op once the cnx is released. PRIVATE. Network thread only.
 */
void moq_pq_conn_release_transport(moq_pq_conn_t *conn);

/* Returns 0 on success, -1 on allocation failure (queue create). queue_cap
 * bounds the outbound queue (0 = MOQ_PQ_SEND_QUEUE_CAP_DEFAULT). */
int pq_endpoint_init(moq_transport_endpoint_ops_t *ops,
                     pq_endpoint_ctx_t *ep_ctx,
                     picoquic_cnx_t *cnx,
                     const moq_alloc_t *alloc,
                     uint64_t queue_cap);

/* Release the outbound queue (decref retained rcbufs, free copies). */
void pq_endpoint_cleanup(pq_endpoint_ctx_t *ep_ctx);

/* Issue every deferred reliable reset whose prefix picoquic has now taken.
 * Called outside picoquic's prepare_to_send (the stream's sent offset only
 * advances once that callback returns): from moq_pq_service and at the start
 * of the other picoquic callbacks. Each issued entry is removed, never
 * retried. Returns 0, or -1 when picoquic refused both a RESET_STREAM_AT and
 * its plain RESET_STREAM fallback, so the stream could not be ended; the
 * caller then fails the transport, as for a reset refused on the spot. */
int pq_endpoint_flush_resets(pq_endpoint_ctx_t *ep_ctx);

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
