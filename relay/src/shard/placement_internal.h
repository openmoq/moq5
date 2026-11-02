#ifndef MOQR_SHARDS_PLACEMENT_INTERNAL_H
#define MOQR_SHARDS_PLACEMENT_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t
r_splitmix64(uint64_t x);

moqr_owner_t
r_rendezvous_place(const moqr_place_key_t *key,
                   const moqr_placement_state_t *state);

int32_t
r_winner_of(uint64_t candidates, uint64_t hash);

uint32_t
rd32(const uint8_t *p);

void
wr32(uint8_t *p, uint32_t v);

uint8_t *
canon_build(const moq_bytes_t *parts, uint32_t count, uint32_t *out_len,
            const moq_alloc_t *a);

uint8_t *
canon_dup(const uint8_t *canon, uint32_t len, const moq_alloc_t *a);

uint64_t
canon_hash(const uint8_t *canon, uint32_t len);

int
canon_cmp(const uint8_t *a, uint32_t alen, const uint8_t *b, uint32_t blen);

moqr_ns_t
canon_to_ns(const uint8_t *canon, moq_bytes_t *parts);

uint8_t *
canon2_build(const moq_bytes_t *parts, uint32_t count, moq_bytes_t name,
             uint32_t *out_len, const moq_alloc_t *a);

uint32_t
canon2_ns_len(const uint8_t *canon);

moqr_ns_t
canon2_decode(const uint8_t *canon, moq_bytes_t *parts, moq_bytes_t *name);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_PLACEMENT_INTERNAL_H */
