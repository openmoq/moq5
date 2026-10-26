/*
 * pq_cid_route: which lane a datagram belongs to, from its first packet's
 * destination connection ID alone. Driven with fake owner and verify
 * functions, so every rule is pinned without crypto:
 *
 *   - a group-issued ID (verifies) names its owner, long or short header;
 *   - a client-chosen long-header ID hashes to its owner;
 *   - a short header that does not verify is a reset candidate when long
 *     enough to be one, else picoquic's;
 *   - malformed or stateless input is picoquic's, never forwarded;
 *   - the same bytes route the same way from every lane.
 */

#include "../pq_cid_route.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            failures++;                                                 \
        }                                                               \
    } while (0)

/* A fake tag: byte 0 is 0x5A and byte 1 the lane. */
static bool fake_verify(void *ctx, const uint8_t *cid, uint32_t lanes,
                        uint32_t *lane)
{
    (void)ctx;
    if (cid[0] != 0x5A || cid[1] >= lanes) return false;
    *lane = cid[1];
    return true;
}

/* A fake owner hash: the byte sum. */
static uint32_t fake_owner(void *ctx, const uint8_t *dcid, size_t len,
                           uint32_t lanes)
{
    (void)ctx;
    uint32_t sum = 0;
    for (size_t i = 0; i < len; i++) sum += dcid[i];
    return sum % lanes;
}

static const pq_route_ops_t ops = { fake_verify, fake_owner };

static pq_route_t route(const uint8_t *dg, size_t len, uint32_t self,
                        uint32_t lanes)
{
    return pq_cid_route(dg, len, self, lanes, &ops, NULL);
}

/* A long header: form bit, version, dcid length, dcid, filler to `len`. */
static size_t long_header(uint8_t *dg, uint32_t version, const uint8_t *dcid,
                          size_t dcid_len, size_t len)
{
    memset(dg, 0xEE, len);
    dg[0] = 0xC0;
    dg[1] = (uint8_t)(version >> 24);
    dg[2] = (uint8_t)(version >> 16);
    dg[3] = (uint8_t)(version >> 8);
    dg[4] = (uint8_t)version;
    dg[5] = (uint8_t)dcid_len;
    memcpy(dg + 6, dcid, dcid_len);
    return len;
}

int main(void)
{
    uint8_t dg[1500] = { 0 };
    uint8_t group_cid[PQ_GROUP_CID_LEN];
    memset(group_cid, 0x11, sizeof(group_cid));
    group_cid[0] = 0x5A;
    group_cid[1] = 2;
    uint8_t client_cid[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };   /* sum 36: lane 0 of 4 */

    /* nothing, or no lanes, is dropped */
    CHECK(route(dg, 0, 0, 4).kind == PQ_ROUTE_DROP);
    CHECK(route(dg, 10, 0, 0).kind == PQ_ROUTE_DROP);

    /* long header: too short, Version Negotiation, bad ID length -> local */
    long_header(dg, 1, client_cid, 8, 1200);
    CHECK(route(dg, 5, 1, 4).kind == PQ_ROUTE_LOCAL);
    long_header(dg, 0, client_cid, 8, 1200);
    CHECK(route(dg, 1200, 1, 4).kind == PQ_ROUTE_LOCAL);
    long_header(dg, 1, client_cid, 8, 1200);
    dg[5] = 21;
    CHECK(route(dg, 1200, 1, 4).kind == PQ_ROUTE_LOCAL);
    dg[5] = 20;
    CHECK(route(dg, 20, 1, 4).kind == PQ_ROUTE_LOCAL);   /* ID runs past the end */
    long_header(dg, 1, client_cid, 0, 1200);
    CHECK(route(dg, 1200, 1, 4).kind == PQ_ROUTE_LOCAL); /* zero-length ID */

    /* long header, client-chosen ID: the owner hash, from every lane */
    long_header(dg, 1, client_cid, 8, 1200);
    pq_route_t r = route(dg, 1200, 1, 4);
    CHECK(r.kind == PQ_ROUTE_LANE && r.lane == 0);
    r = route(dg, 1200, 0, 4);
    CHECK(r.kind == PQ_ROUTE_LOCAL);
    /* a copy from any socket routes identically */
    for (uint32_t self = 0; self < 4; self++) {
        r = route(dg, 1200, self, 4);
        CHECK(r.lane == 0 && r.kind == (self == 0 ? PQ_ROUTE_LOCAL : PQ_ROUTE_LANE));
    }

    /* long header, a group-issued ID: its owner, whatever the hash says */
    long_header(dg, 1, group_cid, PQ_GROUP_CID_LEN, 1200);
    r = route(dg, 1200, 0, 4);
    CHECK(r.kind == PQ_ROUTE_LANE && r.lane == 2);
    CHECK(route(dg, 1200, 2, 4).kind == PQ_ROUTE_LOCAL);
    /* a 17-byte ID that does not verify is client-chosen: the hash */
    long_header(dg, 1, group_cid, PQ_GROUP_CID_LEN, 1200);
    dg[6] = 0x00;
    r = route(dg, 1200, 3, 4);
    CHECK(r.kind == PQ_ROUTE_LANE || r.kind == PQ_ROUTE_LOCAL);
    CHECK(r.lane == fake_owner(NULL, dg + 6, PQ_GROUP_CID_LEN, 4));

    /* short header: too short for a group ID -> local */
    memset(dg, 0x44, sizeof(dg));
    dg[0] = 0x40;
    CHECK(route(dg, 17, 0, 4).kind == PQ_ROUTE_LOCAL);
    /* a group ID names its owner */
    memcpy(dg + 1, group_cid, PQ_GROUP_CID_LEN);
    r = route(dg, 1200, 0, 4);
    CHECK(r.kind == PQ_ROUTE_LANE && r.lane == 2);
    CHECK(route(dg, 1200, 2, 4).kind == PQ_ROUTE_LOCAL);
    CHECK(route(dg, 18, 1, 4).kind == PQ_ROUTE_LANE);    /* exactly one ID long */
    /* no owner: a candidate when it is long enough to be a stateless reset
     * (RFC 9000 section 10.3 receive minimum: a minimal short header plus a
     * 16-byte token), else local. Eligibility is the receive floor, well
     * below the backend's larger send minimum. */
    dg[1] = 0x00;
    CHECK(route(dg, PQ_ROUTE_RESET_DETECT_MIN_LEN, 0, 4).kind == PQ_ROUTE_DETECT);
    CHECK(route(dg, PQ_ROUTE_RESET_DETECT_MIN_LEN - 1, 0, 4).kind == PQ_ROUTE_LOCAL);
    CHECK(route(dg, PQ_ROUTE_RESET_MIN_LEN, 0, 4).kind == PQ_ROUTE_DETECT);
    CHECK(route(dg, PQ_ROUTE_RESET_MIN_LEN - 1, 0, 4).kind == PQ_ROUTE_DETECT);
    CHECK(route(dg, 1200, 3, 4).kind == PQ_ROUTE_DETECT);
    /* a tag naming a lane the group does not have does not verify */
    dg[1] = 0x5A;
    dg[2] = 4;
    CHECK(route(dg, 1200, 0, 4).kind == PQ_ROUTE_DETECT);

    /* one lane: everything is local */
    long_header(dg, 1, client_cid, 8, 1200);
    CHECK(route(dg, 1200, 0, 1).kind == PQ_ROUTE_LOCAL);
    dg[0] = 0x40;
    memset(dg + 1, 0, 17);
    CHECK(route(dg, 1200, 0, 1).kind == PQ_ROUTE_DETECT);  /* still a candidate */

    if (failures) {
        printf("FAILED: %d check(s) [pq_cid_route]\n", failures);
        return 1;
    }
    printf("PASS: pq_cid_route\n");
    return 0;
}
