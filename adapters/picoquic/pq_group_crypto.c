#include "pq_group_crypto.h"

#include <string.h>

#if defined(MOQ_PICOQUIC_VERIFY_OPENSSL)
#include <picotls/openssl.h>
#define PQ_AES128ECB   ptls_openssl_aes128ecb
#define PQ_SHA256      ptls_openssl_sha256
#define PQ_RANDOM      ptls_openssl_random_bytes
#elif defined(MOQ_PICOQUIC_VERIFY_MBEDTLS)
#include <picotls/mbedtls.h>
#define PQ_AES128ECB   ptls_mbedtls_aes128ecb
#define PQ_SHA256      ptls_mbedtls_sha256
#define PQ_RANDOM      ptls_mbedtls_random_bytes
#else
#include <picotls/minicrypto.h>
#define PQ_AES128ECB   ptls_minicrypto_aes128ecb
#define PQ_SHA256      ptls_minicrypto_sha256
#define PQ_RANDOM      ptls_minicrypto_random_bytes
#endif

#define PQ_CID_BLOCK 16u

void pq_group_random(uint8_t *buf, size_t len)
{
    PQ_RANDOM(buf, len);
}

bool pq_group_keys_make(pq_group_keys_t *k)
{
    PQ_RANDOM(k->cid, sizeof(k->cid));
    PQ_RANDOM(k->owner, sizeof(k->owner));
    PQ_RANDOM(k->ticket, sizeof(k->ticket));
    return true;
}

void pq_group_keys_erase(pq_group_keys_t *k)
{
    ptls_clear_memory(k, sizeof(*k));
}

bool pq_group_crypto_lane_init(pq_group_crypto_lane_t *c,
                               const pq_group_keys_t *k)
{
    memset(c, 0, sizeof(*c));
    c->enc = ptls_cipher_new(&PQ_AES128ECB, 1, k->cid);
    c->dec = ptls_cipher_new(&PQ_AES128ECB, 0, k->cid);
    c->hmac = ptls_hmac_create(&PQ_SHA256, k->owner, sizeof(k->owner));
    if (!c->enc || !c->dec || !c->hmac) {
        pq_group_crypto_lane_free(c);
        return false;
    }
    return true;
}

void pq_group_crypto_lane_free(pq_group_crypto_lane_t *c)
{
    if (c->enc) ptls_cipher_free(c->enc);
    if (c->dec) ptls_cipher_free(c->dec);
    if (c->hmac) c->hmac->final(c->hmac, NULL, PTLS_HASH_FINAL_MODE_FREE);
    memset(c, 0, sizeof(*c));
}

void pq_group_cid_make(pq_group_crypto_lane_t *c, uint32_t lane,
                       const uint8_t *random, uint8_t *out)
{
    uint8_t block[PQ_CID_BLOCK];
    block[0] = (uint8_t)lane;
    memset(block + 1, 0, 4);
    memcpy(block + 5, random + 1, PQ_CID_BLOCK - 5);
    out[0] = random[0] & 0xF8u;
    ptls_cipher_encrypt(c->enc, out + 1, block, PQ_CID_BLOCK);
    ptls_clear_memory(block, sizeof(block));
}

bool pq_group_cid_verify(void *ctx, const uint8_t *cid, uint32_t lanes,
                         uint32_t *lane)
{
    pq_group_crypto_lane_t *c = ctx;
    uint8_t block[PQ_CID_BLOCK];
    if (cid[0] & 0x07u)
        return false;
    ptls_cipher_encrypt(c->dec, block, cid + 1, PQ_CID_BLOCK);
    bool ok = (block[1] | block[2] | block[3] | block[4]) == 0 &&
              block[0] < lanes;
    if (ok) *lane = block[0];
    ptls_clear_memory(block, sizeof(block));
    return ok;
}

uint32_t pq_group_owner_of(void *ctx, const uint8_t *dcid, size_t len,
                           uint32_t lanes)
{
    pq_group_crypto_lane_t *c = ctx;
    uint8_t md[PTLS_SHA256_DIGEST_SIZE];
    c->hmac->update(c->hmac, dcid, len);
    c->hmac->final(c->hmac, md, PTLS_HASH_FINAL_MODE_RESET);
    uint32_t h = ((uint32_t)md[0] << 24) | ((uint32_t)md[1] << 16) |
                 ((uint32_t)md[2] << 8) | (uint32_t)md[3];
    ptls_clear_memory(md, sizeof(md));
    return lanes ? h % lanes : 0;
}
