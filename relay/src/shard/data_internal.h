#ifndef MOQR_SHARDS_DATA_INTERNAL_H
#define MOQR_SHARDS_DATA_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

r_psg_t *
psg_find(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg);

r_psg_t *
psg_claim(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg);

void
psg_row_clear(r_psg_t *row, uint32_t slots);

bool
dp_admit(const dp_budget_t *b, uint64_t lb);

void
dp_spend(dp_budget_t *b, uint64_t lb);

void
psub_free(r_mgr *m, r_psub *p);

void
shard_data_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                 uint64_t *pushed_dst_mask);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_DATA_INTERNAL_H */
