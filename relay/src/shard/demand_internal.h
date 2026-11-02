#ifndef MOQR_SHARDS_DEMAND_INTERNAL_H
#define MOQR_SHARDS_DEMAND_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t
dmsg_logical_bytes(const r_demand_msg_t *m);

void
dmsg_release(const moq_alloc_t *a, r_demand_msg_t *m);

bool
dch_push_arb(moqr_shards_t *s, uint16_t src, uint16_t dst,
             const r_demand_msg_t *m, bool is_data, bool other_eligible);

r_demand_msg_t *
dch_head(moqr_shards_t *s, uint16_t src, uint16_t dst);

void
dch_release_held(moqr_shards_t *s, uint16_t src, uint16_t dst,
                 uint32_t entries, uint64_t bytes);

void
psub_metadata_release(r_mgr *m, r_psub *p);

void
shards_metadata_changed(void *ctx, moqr_track_t track, uint64_t track_gen,
                        const moqr_bind_meta_ref_t *latest);

bool
shards_router(void *ctx, const moqr_intent_t *it, uint64_t now_us);

moqr_pd_desc_t
shard_local_done(moqr_pd_status_t status);

r_pdemand *
pend_find(r_mgr *m, uint64_t demand_id);

void
pend_retire(r_mgr *m, r_pdemand *d);

bool
ctrl_work_pending(const r_mgr *m, uint16_t dst);

bool
owner_metadata_pending(const r_mgr *m, uint16_t dst);

bool
data_work_pending(const r_mgr *m, uint16_t dst);

void
shard_demand_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                   uint64_t *pushed_dst_mask);

void
shard_demand_inbound(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                     uint64_t *pushed_dst_mask);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_DEMAND_INTERNAL_H */
