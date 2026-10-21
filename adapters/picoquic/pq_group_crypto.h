#ifndef MOQ_PQ_GROUP_CRYPTO_H
#define MOQ_PQ_GROUP_CRYPTO_H

/*
 * The lane group's keyed primitives, from the TLS backend the adapter already
 * links through picotls's public API: AES-128 for the lane tag inside every
 * connection ID the group issues, HMAC-SHA256 for the owner of a
 * client-chosen destination ID, and the backend's CSPRNG for the keys. Each
 * lane holds its own contexts, so routing allocates nothing and shares no
 * mutable state across threads. PRIVATE to adapters/picoquic.
 *
 * A connection ID the group issues is PQ_GROUP_CID_LEN bytes: a first byte
 * with five random bits and three zero bits, then one AES block,
 * AES-128-ECB_k(lane || 0x00000000 || nonce[11]) with the nonce picoquic's
 * own random bytes. Every issued ID is a fresh pseudorandom block, so no ID's
 * contents correlate it with another of the same connection (RFC 9000
 * section 5.1); the four zero bytes make a stray ID verify with probability
 * 2^-32 x lanes/256, and a verifying ID still only names a lane whose
 * picoquic authenticates the packet.
 */

#include "pq_cid_route.h"

#include <picotls.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#if defined(__GNUC__) || defined(__clang__)
/* The lane group's internals are not part of the adapter's ABI. */
#  pragma GCC visibility push(hidden)
#endif

typedef struct pq_group_keys {
    uint8_t cid[16];     /* the lane tag cipher */
    uint8_t owner[32];   /* the client-chosen ID owner hash */
    uint8_t ticket[32];  /* shared ticket and token key of every lane */
} pq_group_keys_t;

typedef struct pq_group_crypto_lane {
    ptls_cipher_context_t *enc;
    ptls_cipher_context_t *dec;
    ptls_hash_context_t   *hmac;
} pq_group_crypto_lane_t;

/* The backend's CSPRNG. */
void pq_group_random(uint8_t *buf, size_t len);

bool pq_group_keys_make(pq_group_keys_t *k);
void pq_group_keys_erase(pq_group_keys_t *k);

/* One lane's contexts over the group's keys; false on allocation failure. */
bool pq_group_crypto_lane_init(pq_group_crypto_lane_t *c,
                               const pq_group_keys_t *k);
void pq_group_crypto_lane_free(pq_group_crypto_lane_t *c);

/* The connection ID for `lane` from picoquic's random `random` bytes
 * (PQ_GROUP_CID_LEN of them): out gets PQ_GROUP_CID_LEN bytes. */
void pq_group_cid_make(pq_group_crypto_lane_t *c, uint32_t lane,
                       const uint8_t *random, uint8_t *out);

/* pq_route_ops_t over a lane's contexts (ctx = pq_group_crypto_lane_t *). */
bool pq_group_cid_verify(void *ctx, const uint8_t *cid, uint32_t lanes,
                         uint32_t *lane);
uint32_t pq_group_owner_of(void *ctx, const uint8_t *dcid, size_t len,
                           uint32_t lanes);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC visibility pop
#endif
#endif /* MOQ_PQ_GROUP_CRYPTO_H */
