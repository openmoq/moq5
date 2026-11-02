#ifndef MOQR_SHARDS_FILL_INTERNAL_H
#define MOQR_SHARDS_FILL_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

r_fret_t *
fret_find(r_mgr *m, uint64_t id);

void
fret_demand_gone(r_mgr *m, uint64_t demand_id);

moqr_result_t
shards_fill_open(void *ctx, const moqr_bind_fill_query_t *q,
                 moqr_bind_fill_claim_t *claim);

bool
shard_delivery_subgroup_props(r_mgr *m, uint16_t src);

moqr_result_t
shards_sa_open(void *ctx, const moqr_retrieve_standalone_query_t *q,
               uint64_t *handle);

moqr_result_t
shards_sa_plan(void *ctx, uint64_t handle, moqr_retrieve_standalone_plan_t *out);

moqr_result_t
shards_fill_retrieve_peek(void *ctx, uint64_t handle, uint64_t now_us,
                          moqr_retrieve_item_t *out);

moqr_result_t
shards_fill_peek(void *ctx, uint64_t handle, uint64_t now_us,
                 moqr_fetch_item_t *out);

moqr_result_t
shards_fill_commit(void *ctx, uint64_t handle);

void
shards_fill_close(void *ctx, uint64_t handle);

uint64_t
shards_fill_next_deadline(void *ctx);

r_oret_t *
oret_find(r_mgr *m, uint16_t src, uint64_t id);

void
oret_free(r_mgr *m, r_oret_t *o);

void
oret_demand_gone(r_mgr *m, uint16_t src, uint64_t demand_id);

void
requester_fill_phase(moqr_shards_t *s, uint16_t shard,
                     uint64_t *pushed_dst_mask);

bool
owner_fill_pass(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                dp_budget_t *bud, uint64_t *pushed_dst_mask, bool *attempted,
                uint64_t *pending);

bool
owner_fill_accept(moqr_shards_t *s, uint16_t shard, uint16_t src,
                  const r_demand_msg_t *msg, uint64_t *pushed_dst_mask,
                  uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_FILL_INTERNAL_H */
