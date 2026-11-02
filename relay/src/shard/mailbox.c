#include "shards_internal.h"
#include "mailbox_internal.h"
#include "journal_internal.h"
#include "placement_internal.h"

/* -- control mailboxes ------------------------------------------------------ */

/* Publish a namespace's latest net state to a directed mailbox. Coalesces onto an
 * existing slot for the same namespace — announce+withdraw collapse in place, so a
 * pending message is never duplicated and can never head-of-line another. Returns
 * false only when a NEW namespace finds every slot occupied (backpressure: the
 * sender leaves its sent bit clear and re-exports next round). */
static bool
mbox_push(moqr_shards_t *s, uint16_t src, uint16_t dst, uint8_t op,
          uint16_t origin, const uint8_t *canon, uint32_t len)
{
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    bool pushed = false;
    pthread_mutex_lock(&mb->mu);
    int32_t free_slot = -1;
    for (uint32_t i = 0; i < mb->cap; i++) {
        if (!mb->slots[i].used) {
            if (free_slot < 0) {
                free_slot = (int32_t)i;
            }
            continue;
        }
        if (canon_cmp(mb->slots[i].canon, mb->slots[i].canon_len, canon, len) == 0) {
            mb->slots[i].op = op;          /* coalesce: latest state wins */
            mb->slots[i].round = s->round;
            pushed = true;
            goto out;
        }
    }
    if (free_slot < 0) {
        goto out;                          /* full of other namespaces */
    }
    {
        uint8_t *dup = canon_dup(canon, len, &s->alloc);
        if (dup == NULL) {
            goto out;
        }
        r_ctrl_msg *slot = &mb->slots[free_slot];
        slot->used = true;
        slot->round = s->round;
        slot->op = op;
        slot->origin = origin;
        slot->canon = dup;
        slot->canon_len = len;
        mb->count++;
        pushed = true;
    }
out:
    pthread_mutex_unlock(&mb->mu);
    return pushed;
}


/* Publish a whole original-source snapshot under one leaf lock. A reused
 * source slot replaces its predecessor, including while the consumer lags. */
static bool
mbox_push_cluster(moqr_shards_t *s, uint16_t src, uint16_t dst,
                  uint32_t index, const r_cluster_snapshot *snapshot,
                  bool *allocation_failed)
{
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    uint8_t *canon = NULL;
    uint64_t *hops = NULL;
    size_t hop_bytes = snapshot->present
        ? snapshot->record.route.metadata.hop_count * sizeof(uint64_t) : 0;
    if (snapshot->present) {
        canon = canon_dup(snapshot->canon, snapshot->canon_len, &s->alloc);
        if (canon == NULL) { *allocation_failed = true; return false; }
        hops = sh_alloc(&s->alloc, hop_bytes);
        if (hops == NULL) {
            sh_free(&s->alloc, canon, snapshot->canon_len);
            *allocation_failed = true;
            return false;
        }
        memcpy(hops, snapshot->hops, hop_bytes);
    }
    bool pushed = false;
    pthread_mutex_lock(&mb->mu);
    r_ctrl_msg *slot = NULL;
    bool waiting_withdraw = mb->withdrawal->used && mb->withdrawal->source_index == index;
    /* Do not let a reannouncement overtake an already published withdrawal.
     * The source snapshot remains unsent and retries after its durable pop. */
    if (waiting_withdraw) {
        if (!snapshot->present) slot = mb->withdrawal;
    } else {
        for (uint32_t i = 0; i < mb->cap; ++i) {
            if (mb->slots[i].used && mb->slots[i].cluster &&
                mb->slots[i].source_index == index) {
                slot = &mb->slots[i];
                break;
            }
            if (!mb->slots[i].used && slot == NULL) slot = &mb->slots[i];
        }
        if (slot == NULL && !snapshot->present && !mb->withdrawal->used)
            slot = mb->withdrawal;
    }
    if (slot != NULL) {
        if (slot->used) {
            sh_free(&s->alloc, slot->canon, slot->canon_len);
            sh_free(&s->alloc, slot->hops,
                    slot->record.route.metadata.hop_count * sizeof(uint64_t));
        } else {
            mb->count++;
        }
        memset(slot, 0, sizeof(*slot));
        slot->used = true;
        slot->cluster = true;
        slot->round = s->round;
        slot->origin = src;
        slot->source_index = index;
        slot->op = snapshot->present ? R_OP_ANNOUNCE : R_OP_UNANNOUNCE;
        slot->canon = canon;
        slot->canon_len = snapshot->present ? snapshot->canon_len : 0;
        slot->hops = hops;
        if (snapshot->present) slot->record = snapshot->record;
        slot->record.source_index = index;
        slot->record.route.metadata.hop_ids = hops;
        pushed = true;
    }
    pthread_mutex_unlock(&mb->mu);
    if (!pushed) {
        sh_free(&s->alloc, canon, snapshot->canon_len);
        sh_free(&s->alloc, hops, hop_bytes);
    }
    return pushed;
}


/* Capture after all local mutations. Comparisons include source identity and
 * charged/warm cost, so an alternative changing below the best path is still
 * replicated. Imports are excluded by the core exporter. */
static void
shard_cluster_outbound(moqr_shards_t *s, uint16_t shard, uint64_t *wake)
{
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; i < s->cluster_routes; ++i) {
        moqr_cluster_export_t record;
        memset(&record, 0, sizeof(record));
        moqr_result_t rc = moqr_core_internal_cluster_export(m->core, i, &record);
        if (rc != MOQR_OK && rc != MOQR_DONE) {
            m->cluster_error = rc;
            return;
        }
        bool present = rc == MOQR_OK;
        r_cluster_snapshot *snapshot = &m->cluster[i];
        if (present) {
            uint32_t canon_len = 0;
            uint8_t *canon = canon_build(record.ns.parts, record.ns.count,
                                        &canon_len, &m->alloc);
            if (canon == NULL) { m->oom = true; return; }
            const moqr_cluster_route_t *a = &snapshot->record.route;
            const moqr_cluster_route_t *b = &record.route;
            size_t hop_bytes = b->metadata.hop_count * sizeof(uint64_t);
            bool same = snapshot->present && snapshot->canon_len == canon_len &&
                memcmp(snapshot->canon, canon, canon_len) == 0 &&
                moqr_cluster_advertisement_id_eq(a->advertisement, b->advertisement) &&
                snapshot->record.source_session_cookie == record.source_session_cookie &&
                snapshot->record.revision == record.revision &&
                a->accumulated_cost == b->accumulated_cost &&
                a->warm == b->warm &&
                a->metadata.has_route_cost == b->metadata.has_route_cost &&
                a->metadata.route_cost == b->metadata.route_cost &&
                a->metadata.hop_count == b->metadata.hop_count &&
                snapshot->record.source_peer_hop_id == record.source_peer_hop_id &&
                snapshot->record.source_correlation_id == record.source_correlation_id &&
                memcmp(snapshot->hops, b->metadata.hop_ids, hop_bytes) == 0;
            if (same) {
                sh_free(&m->alloc, canon, canon_len);
            } else {
                uint64_t *hops = sh_alloc(&m->alloc, hop_bytes);
                if (hops == NULL) {
                    sh_free(&m->alloc, canon, canon_len);
                    m->oom = true;
                    return;
                }
                memcpy(hops, b->metadata.hop_ids, hop_bytes);
                sh_free(&m->alloc, snapshot->canon, snapshot->canon_len);
                sh_free(&m->alloc, snapshot->hops,
                        snapshot->record.route.metadata.hop_count * sizeof(uint64_t));
                snapshot->canon = canon;
                snapshot->canon_len = canon_len;
                snapshot->hops = hops;
                snapshot->record = record;
                snapshot->record.ns.parts = NULL;
                snapshot->record.ns.count = 0;
                memset(snapshot->record.ns_parts, 0,
                       sizeof(snapshot->record.ns_parts));
                snapshot->record.route.metadata.hop_ids = hops;
                snapshot->present = true;
                snapshot->sent = 0;
            }
        } else if (snapshot->present) {
            snapshot->present = false;
            snapshot->sent = 0;
        }
        for (uint16_t d = 0; d < s->shard_count; ++d) {
            if (d == shard || ((snapshot->sent >> d) & 1u) != 0) continue;
            bool allocation_failed = false;
            if (mbox_push_cluster(s, shard, d, i, snapshot, &allocation_failed)) {
                snapshot->sent |= 1ull << d;
                if (wake != NULL) *wake |= 1ull << d;
            } else if (allocation_failed) {
                /* No queued message can return credit for an allocation
                 * refusal. Surface it instead of silently stranding state. */
                m->oom = true;
                return;
            }
        }
    }
}


/* Phase 1 — apply every prior-round inbound control slot into the candidate set,
 * marking namespaces dirty. No core-trie mutation here. Each namespace's slot is
 * independent (the mailbox coalesces per namespace), so a slot that cannot be
 * recorded because the journal is full is simply left for the next round while the
 * others — including unannounces that free journal slots — still apply. Capacity is
 * lag, never loss, and never head-of-line. */
void
shard_inbound_batch(moqr_shards_t *s, uint16_t shard)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    for (uint16_t src = 0; src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&mb->mu);
        if (mb->withdrawal != NULL && mb->withdrawal->used &&
            (s->live_visibility || mb->withdrawal->round < s->round))
            (void)shard_apply_slot(m, mb, mb->withdrawal);
        for (uint32_t i = 0; i < mb->cap; i++) {
            r_ctrl_msg *slot = &mb->slots[i];
            if (!slot->used) {
                continue;
            }
            if (!s->live_visibility && slot->round >= s->round) {
                continue;   /* pushed this round: the deterministic barrier */
            }
            (void)shard_apply_slot(m, mb, slot);
        }
        pthread_mutex_unlock(&mb->mu);
    }
}


/* Phase 4 — export ONLY this shard's own local-origin state to each destination
 * (never remote candidate bits), converging each mailbox to the current state
 * as space allows; collapse intermediate churn; reclaim fully-quiesced entries.
 * Walks in the same lexicographic order as reconcile. `pushed_dst_mask`
 * (optional) accumulates bit d for every destination whose mailbox accepted a
 * push (the demand/data phases fold their channel pushes into the same mask)
 * — the seam a runtime uses to wake exactly the shards with new inbound
 * state waiting. */
void
shard_outbound(moqr_shards_t *s, uint16_t shard, uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    if (s->cluster_enabled) {
        shard_cluster_outbound(s, shard, pushed_dst_mask);
        return;
    }
    uint32_t i = 0;
    while (i < m->jrn_len) {
        r_jentry *e = &m->jrn[i];
        uint64_t local = (e->candidates >> shard) & 1u;
        for (uint16_t d = 0; d < s->shard_count; d++) {
            if (d == shard) {
                continue;
            }
            uint64_t told = (e->sent >> d) & 1u;
            if (local == told) {
                continue;
            }
            uint8_t op = local ? R_OP_ANNOUNCE : R_OP_UNANNOUNCE;
            if (mbox_push(s, shard, d, op, shard, e->canon, e->canon_len)) {
                if (local) {
                    e->sent |= (1ull << d);
                } else {
                    e->sent &= ~(1ull << d);
                }
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d;
                }
            }
            /* full: leave `sent` unchanged, retry next round (cursor lag). */
        }
        /* Reclaim a fully-quiesced entry: no live candidate, no mirror, every
         * destination already told "absent", and no self-echo still owed to it (a
         * pending mirror echo must find its entry, or it would be read as a real
         * local event). */
        if (e->candidates == 0 && e->mirror == R_MIRROR_NONE && e->sent == 0 &&
            !e->dirty && e->echo_ann == 0 && e->echo_unann == 0) {
            jrn_remove(m, i);
            continue;   /* i now indexes the next entry */
        }
        i++;
    }
}
