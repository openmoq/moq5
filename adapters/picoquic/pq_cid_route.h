#ifndef MOQ_PQ_CID_ROUTE_H
#define MOQ_PQ_CID_ROUTE_H

/*
 * Which lane a datagram belongs to, read from its first packet's
 * destination connection ID and nothing else: no address, socket or arrival
 * order enters it, so the same datagram routes the same way on every lane
 * (RFC 9000 section 5.2 matches packets by connection ID; section 12.2
 * forbids coalescing packets of different connection IDs, so the first
 * packet decides for the datagram). Pure: bounds first, no allocation, no
 * QUIC parsing beyond the invariant header. PRIVATE to adapters/picoquic.
 *
 *   - A connection ID the group issued (PQ_GROUP_CID_LEN bytes carrying a
 *     verifiable lane tag) names its owner.
 *   - Any other long-header destination ID is client-chosen, before the
 *     server has answered (section 7.2 keeps it the same on every packet
 *     until then, 0-RTT included): a keyed hash of it names the owner, so
 *     every copy, retransmission and concurrent arrival lands on one lane.
 *   - A short-header datagram whose ID does not verify cannot be attributed.
 *     One long enough to be a stateless reset (RFC 9000 section 10.3 receive
 *     minimum) is a candidate every lane compares with its own tokens; a
 *     shorter one is left to picoquic.
 *   - Anything malformed is left to picoquic, which drops it.
 *
 * The tag is a routing hint: picoquic still authenticates every packet.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#if defined(__GNUC__) || defined(__clang__)
/* The lane group's internals are not part of the adapter's ABI. */
#  pragma GCC visibility push(hidden)
#endif

#define PQ_GROUP_CID_LEN 17u

/* The smallest datagram picoquic will *send* as a stateless reset:
 * PICOQUIC_RESET_PACKET_MIN_SIZE, restated here so this module needs no
 * picoquic header; the group checks the two agree. This is a send policy,
 * not the receive floor below. */
#define PQ_ROUTE_RESET_MIN_LEN 39u

/* The smallest datagram a *received* packet must be to possibly be a
 * stateless reset, so that detection does not miss a short one: RFC 9000
 * section 10.3, a minimal short header (at least 5 bytes) carrying the
 * 16-byte reset token. A peer may send a reset this small even though
 * picoquic pads its own larger; eligibility is separate from send policy. */
#define PQ_ROUTE_RESET_DETECT_MIN_LEN 21u

typedef enum pq_route_kind {
    PQ_ROUTE_LOCAL = 0,   /* this lane's, or picoquic's to refuse */
    PQ_ROUTE_LANE,        /* another lane's: forward to `lane` */
    PQ_ROUTE_DETECT,      /* no owner: a stateless-reset candidate */
    PQ_ROUTE_DROP         /* empty */
} pq_route_kind_t;

typedef struct pq_route {
    pq_route_kind_t kind;
    uint32_t        lane;
} pq_route_t;

typedef struct pq_route_ops {
    /* True when `cid` (PQ_GROUP_CID_LEN bytes) was issued by the group;
     * *lane is then its owner, below `lanes`. */
    bool (*verify)(void *ctx, const uint8_t *cid, uint32_t lanes,
                   uint32_t *lane);
    /* The owner of a client-chosen destination ID: a keyed hash of its
     * bytes, below `lanes`. */
    uint32_t (*owner)(void *ctx, const uint8_t *dcid, size_t len,
                      uint32_t lanes);
} pq_route_ops_t;

pq_route_t pq_cid_route(const uint8_t *dg, size_t len, uint32_t self,
                        uint32_t lanes, const pq_route_ops_t *ops, void *ctx);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC visibility pop
#endif
#endif /* MOQ_PQ_CID_ROUTE_H */
