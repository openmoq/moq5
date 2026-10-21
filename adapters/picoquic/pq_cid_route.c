#include "pq_cid_route.h"

/* RFC 9000 section 17.2: a long header is form bit 1, then a 4-byte version
 * and a 1-byte destination ID length; section 17.3: a short header is form
 * bit 0 followed directly by the destination ID. */
#define PQ_LONG_HEADER_FORM 0x80u
#define PQ_LONG_HEADER_MIN  6u
#define PQ_MAX_CID_LEN      20u

static pq_route_t to_lane(uint32_t self, uint32_t lane)
{
    pq_route_t r;
    r.kind = lane == self ? PQ_ROUTE_LOCAL : PQ_ROUTE_LANE;
    r.lane = lane;
    return r;
}

pq_route_t pq_cid_route(const uint8_t *dg, size_t len, uint32_t self,
                        uint32_t lanes, const pq_route_ops_t *ops, void *ctx)
{
    pq_route_t local = { PQ_ROUTE_LOCAL, self };
    uint32_t lane = 0;

    if (len == 0 || !dg || lanes == 0) {
        pq_route_t drop = { PQ_ROUTE_DROP, self };
        return drop;
    }
    if (dg[0] & PQ_LONG_HEADER_FORM) {
        if (len < PQ_LONG_HEADER_MIN)
            return local;
        uint32_t version = ((uint32_t)dg[1] << 24) | ((uint32_t)dg[2] << 16) |
                           ((uint32_t)dg[3] << 8) | (uint32_t)dg[4];
        size_t dcid_len = dg[5];
        if (version == 0)
            return local;                 /* Version Negotiation: stateless */
        if (dcid_len > PQ_MAX_CID_LEN || dcid_len > len - PQ_LONG_HEADER_MIN)
            return local;
        if (dcid_len == PQ_GROUP_CID_LEN &&
            ops->verify(ctx, dg + PQ_LONG_HEADER_MIN, lanes, &lane))
            return to_lane(self, lane);
        if (dcid_len == 0)
            return local;
        return to_lane(self, ops->owner(ctx, dg + PQ_LONG_HEADER_MIN,
                                        dcid_len, lanes) % lanes);
    }
    if (len < 1u + PQ_GROUP_CID_LEN)
        return local;
    if (ops->verify(ctx, dg + 1, lanes, &lane))
        return to_lane(self, lane);
    if (len >= PQ_ROUTE_RESET_DETECT_MIN_LEN) {
        pq_route_t detect = { PQ_ROUTE_DETECT, self };
        return detect;
    }
    return local;
}
