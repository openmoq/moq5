#ifndef MOQR_SHARDS_MAILBOX_INTERNAL_H
#define MOQR_SHARDS_MAILBOX_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void
shard_inbound_batch(moqr_shards_t *s, uint16_t shard);

void
shard_outbound(moqr_shards_t *s, uint16_t shard, uint64_t *pushed_dst_mask);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_MAILBOX_INTERNAL_H */
