/*
 * pq_group_crypto: the lane group's connection IDs and owner hash over the
 * adapter's TLS backend.
 *
 *   - an issued ID verifies to its lane, at every lane count it fits, and
 *     from a second lane's contexts over the same keys;
 *   - a lane the group does not have, a damaged ID, or another group's
 *     keys do not verify;
 *   - the first byte's low three bits are zero; different nonces give
 *     different IDs; the same nonce gives the same ID;
 *   - the owner hash is deterministic, below the lane count, and spreads.
 */

#include "../pq_group_crypto.h"

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

int main(void)
{
    pq_group_keys_t keys, other_keys;
    pq_group_crypto_lane_t a, b, foreign;
    CHECK(pq_group_keys_make(&keys));
    CHECK(pq_group_keys_make(&other_keys));
    CHECK(memcmp(keys.cid, other_keys.cid, sizeof(keys.cid)) != 0);
    CHECK(pq_group_crypto_lane_init(&a, &keys));
    CHECK(pq_group_crypto_lane_init(&b, &keys));
    CHECK(pq_group_crypto_lane_init(&foreign, &other_keys));

    uint8_t random[PQ_GROUP_CID_LEN], cid[PQ_GROUP_CID_LEN], cid2[PQ_GROUP_CID_LEN];
    uint32_t lane = 99;
    for (uint32_t lanes = 2; lanes <= 64; lanes *= 2) {
        for (uint32_t l = 0; l < lanes; l++) {
            pq_group_random(random, sizeof(random));
            pq_group_cid_make(&a, l, random, cid);
            CHECK((cid[0] & 0x07) == 0);
            CHECK(pq_group_cid_verify(&a, cid, lanes, &lane) && lane == l);
            CHECK(pq_group_cid_verify(&b, cid, lanes, &lane) && lane == l);
            /* the lane tag is not visible */
            CHECK(cid[1] != l || cid[2] != 0);
        }
    }
    /* an ID of lane 5 in a group of 4 does not verify */
    pq_group_random(random, sizeof(random));
    pq_group_cid_make(&a, 5, random, cid);
    CHECK(!pq_group_cid_verify(&a, cid, 4, &lane));
    CHECK(pq_group_cid_verify(&a, cid, 8, &lane) && lane == 5);
    /* damaged, or another group's */
    pq_group_cid_make(&a, 1, random, cid);
    for (size_t i = 0; i < PQ_GROUP_CID_LEN; i++) {
        memcpy(cid2, cid, sizeof(cid2));
        cid2[i] ^= 0x10;
        if (i == 0) {
            CHECK(pq_group_cid_verify(&a, cid2, 4, &lane) && lane == 1); /* random bits */
        } else {
            CHECK(!pq_group_cid_verify(&a, cid2, 4, &lane));
        }
    }
    CHECK(!pq_group_cid_verify(&foreign, cid, 4, &lane));
    memcpy(cid2, cid, sizeof(cid2));
    cid2[0] |= 0x01;
    CHECK(!pq_group_cid_verify(&a, cid2, 4, &lane));
    /* nonce-bound */
    pq_group_cid_make(&a, 1, random, cid2);
    CHECK(memcmp(cid, cid2, sizeof(cid)) == 0);
    random[3] ^= 0x80;
    pq_group_cid_make(&a, 1, random, cid2);
    CHECK(memcmp(cid + 1, cid2 + 1, PQ_GROUP_CID_LEN - 1) != 0);

    /* the owner hash */
    uint8_t dcid[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    uint32_t o = pq_group_owner_of(&a, dcid, sizeof(dcid), 4);
    CHECK(o < 4);
    CHECK(pq_group_owner_of(&a, dcid, sizeof(dcid), 4) == o);
    CHECK(pq_group_owner_of(&b, dcid, sizeof(dcid), 4) == o);
    CHECK(pq_group_owner_of(&a, dcid, sizeof(dcid), 1) == 0);
    unsigned seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < 256; i++) {
        pq_group_random(dcid, sizeof(dcid));
        seen[pq_group_owner_of(&a, dcid, sizeof(dcid), 4)]++;
    }
    CHECK(seen[0] > 20 && seen[1] > 20 && seen[2] > 20 && seen[3] > 20);
    CHECK(pq_group_owner_of(&foreign, dcid, sizeof(dcid), 4) < 4);

    pq_group_crypto_lane_free(&a);
    pq_group_crypto_lane_free(&b);
    pq_group_crypto_lane_free(&foreign);
    pq_group_keys_erase(&keys);
    CHECK(keys.cid[0] == 0 && keys.ticket[31] == 0);

    if (failures) {
        printf("FAILED: %d check(s) [pq_group_crypto]\n", failures);
        return 1;
    }
    printf("PASS: pq_group_crypto\n");
    return 0;
}
