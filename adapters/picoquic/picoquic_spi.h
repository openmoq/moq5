#ifndef MOQ_PICOQUIC_SPI_H
#define MOQ_PICOQUIC_SPI_H

/*
 * Private adapter SPI of libmoq-adapter-picoquic.
 *
 * Stability: this is NOT application API and NOT a stable third-party ABI. The
 * header is not installed. Its MOQ_PQ_API functions are exported from the
 * shared libmoq-adapter-picoquic only so that the separately built
 * libmoq-adapter-picoquic-threaded can link them; the two libraries are built
 * and versioned in lockstep and MAY change these together at any release.
 * Application code uses <moq/picoquic.h> / <moq/picoquic_threaded.h>, never
 * these symbols. The exact exported set of the shared adapter is pinned by the
 * adapter_picoquic_symbol_policy test
 * (adapters/picoquic/tests/check_adapter_symbols.cmake).
 */

#include <moq/export.h>
#include <moq/picoquic.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Send-path telemetry snapshot (characterization; not product API). */
typedef struct {
    uint64_t prepare_count;    /* prepare_to_send callbacks serviced */
    uint64_t provided_bytes;   /* bytes copied into picoquic buffers */
    uint64_t queue_high_water; /* peak adapter-queue backlog bytes */
    uint64_t queue_would_block;/* pushes refused for the queue cap */
} moq_pq_send_stats_t;

/* Read a connection's send-path telemetry (implemented in moq_picoquic.c), so
 * the threaded adapter can aggregate across connections. */
MOQ_PQ_API void moq_pq_conn_get_send_stats(const moq_pq_conn_t *conn,
                                           moq_pq_send_stats_t *out);

/*
 * True once picoquic has handed this connection's picoquic_cnx_t back and the
 * adapter has dropped every copy of the pointer (implemented in
 * moq_picoquic.c). Deliberately not in <moq/picoquic.h>, because it describes
 * an internal ownership fact, not application API.
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
MOQ_PQ_API bool moq_pq_conn_cnx_released(const moq_pq_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_SPI_H */
