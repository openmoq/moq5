/*
 * pq_cid_route: the PicoQUIC lane group's datagram router under arbitrary
 * input. The first two bytes pick the lane count (1..64) and the receiving
 * lane; the rest is the datagram. Verify and owner are fakes (a tag byte and
 * a byte sum), as in the unit test, so the harness exercises the header walk
 * and the length rules, not the crypto. Pinned on every input:
 *   - a LANE result names another lane below the lane count, never with one
 *     lane;
 *   - the same datagram names the same owner from every lane: LOCAL on the
 *     owner, LANE elsewhere; a datagram that is picoquic's or a reset
 *     candidate on one lane is so on every lane;
 *   - DROP is exactly the empty datagram.
 */

#include "pq_cid_route.h"

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

static bool fake_verify(void *ctx, const uint8_t *cid, uint32_t lanes,
                        uint32_t *lane)
{
    (void)ctx;
    if (cid[0] != 0x5A || cid[1] >= lanes) return false;
    *lane = cid[1];
    return true;
}

static uint32_t fake_owner(void *ctx, const uint8_t *dcid, size_t len,
                           uint32_t lanes)
{
    (void)ctx;
    uint32_t sum = 0;
    for (size_t i = 0; i < len; i++) sum += dcid[i];
    return sum % lanes;
}

static const pq_route_ops_t ops = { fake_verify, fake_owner };

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 2) return 0;
    uint32_t lanes = 1u + (data[0] % 64u);
    uint32_t self = data[1] % lanes;
    const uint8_t *dg = data + 2;
    size_t len = size - 2;

    pq_route_t r = pq_cid_route(dg, len, self, lanes, &ops, NULL);
    switch (r.kind) {
    case PQ_ROUTE_LANE:
        if (r.lane >= lanes || r.lane == self || lanes == 1) abort();
        break;
    case PQ_ROUTE_LOCAL:
    case PQ_ROUTE_DETECT:
        break;
    case PQ_ROUTE_DROP:
        if (len != 0) abort();
        break;
    default:
        abort();
    }
    if (len == 0 && r.kind != PQ_ROUTE_DROP) abort();

    /* The same bytes from every other lane. A LOCAL on this lane is either
     * ownership (the others forward here) or picoquic's to handle (the
     * others keep it too); the lanes must agree on which. */
    bool forwarded_here = false, kept_everywhere = false;
    for (uint32_t other = 0; other < lanes; other++) {
        if (other == self) continue;
        pq_route_t o = pq_cid_route(dg, len, other, lanes, &ops, NULL);
        if (r.kind == PQ_ROUTE_LANE) {
            if (other == r.lane) {
                if (o.kind != PQ_ROUTE_LOCAL) abort();
            } else if (o.kind != PQ_ROUTE_LANE || o.lane != r.lane) {
                abort();
            }
        } else if (r.kind == PQ_ROUTE_LOCAL) {
            if (o.kind == PQ_ROUTE_LANE && o.lane == self) {
                forwarded_here = true;
            } else if (o.kind == PQ_ROUTE_LOCAL) {
                kept_everywhere = true;
            } else {
                abort();
            }
            if (forwarded_here && kept_everywhere) abort();
        } else if (o.kind != r.kind) {
            abort();
        }
    }
    return 0;
}
