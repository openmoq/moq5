#ifndef MOQR_SHARDS_JOURNAL_INTERNAL_H
#define MOQR_SHARDS_JOURNAL_INTERNAL_H

#include "shards_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

r_jentry *
jrn_find(r_mgr *m, const uint8_t *canon, uint32_t len, uint32_t *ins);

r_jentry *
jrn_get(r_mgr *m, uint8_t *canon, uint32_t len);

void
jrn_remove(r_mgr *m, uint32_t i);

void
jrn_set_candidate(r_mgr *m, r_jentry *e, uint64_t bit, bool present);

bool
shard_apply_slot(r_mgr *m, r_mailbox *mb, r_ctrl_msg *slot);

void
shard_reconcile(moqr_shards_t *s, uint16_t shard, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_SHARDS_JOURNAL_INTERNAL_H */
