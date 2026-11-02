#include "shards_internal.h"
#include "journal_internal.h"
#include "placement_internal.h"

/* Find the sorted-array index of `canon` (binary search). Returns the entry
 * when present; else sets *ins to the insertion index and returns NULL. */
r_jentry *
jrn_find(r_mgr *m, const uint8_t *canon, uint32_t len, uint32_t *ins)
{
    uint32_t lo = 0, hi = m->jrn_len;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2u;
        int c = canon_cmp(m->jrn[mid].canon, m->jrn[mid].canon_len, canon, len);
        if (c == 0) {
            if (ins != NULL) {
                *ins = mid;
            }
            return &m->jrn[mid];
        }
        if (c < 0) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    if (ins != NULL) {
        *ins = lo;
    }
    return NULL;
}


/* Get-or-insert an entry for `canon` (ADOPTS canon on insert; the caller must
 * not free it after). Returns NULL on capacity/OOM (caller frees canon). */
r_jentry *
jrn_get(r_mgr *m, uint8_t *canon, uint32_t len)
{
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    if (e != NULL) {
        return e;   /* existing; caller frees the dup canon */
    }
    if (m->jrn_len >= m->jrn_cap) {
        return NULL;
    }
    memmove(&m->jrn[ins + 1u], &m->jrn[ins],
            (size_t)(m->jrn_len - ins) * sizeof(m->jrn[0]));
    e = &m->jrn[ins];
    memset(e, 0, sizeof(*e));
    e->used = true;
    e->canon = canon;
    e->canon_len = len;
    e->hash = canon_hash(canon, len);
    e->mirror = R_MIRROR_NONE;
    m->jrn_len++;
    return e;
}


/* Remove a fully-quiesced entry (index i) and free its key. The entry
 * leaves the printed journal projection, so the projection generation
 * advances. */
void
jrn_remove(r_mgr *m, uint32_t i)
{
    sh_free(&m->alloc, m->jrn[i].canon, m->jrn[i].canon_len);
    memmove(&m->jrn[i], &m->jrn[i + 1u],
            (size_t)(m->jrn_len - i - 1u) * sizeof(m->jrn[0]));
    m->jrn_len--;
    m->journal_epoch++;
}


/* Record an origin's candidate bit as present/absent, marking the entry
 * dirty for reconcile. The projection generation advances ONLY when the bit
 * actually changes — a redelivered/no-op observation is not a projection
 * change. */
void
jrn_set_candidate(r_mgr *m, r_jentry *e, uint64_t bit, bool present)
{
    uint64_t next = present ? (e->candidates | bit) : (e->candidates & ~bit);
    if (next != e->candidates) {
        e->candidates = next;
        m->journal_epoch++;
    }
    e->dirty = true;
}


/* -- phase 3: dirty-reconcile ----------------------------------------------- */

static void
mgr_update_holdout(r_mgr *m, r_jentry *e, bool holdout)
{
    if (holdout && !e->holdout) {
        m->holdouts++;
        m->journal_epoch++;   /* holdout transition: a projection change */
    } else if (!holdout && e->holdout) {
        m->holdouts--;
        m->journal_epoch++;
    }
    e->holdout = holdout;
}


static void
mgr_reconcile_entry(r_mgr *m, r_jentry *e, uint64_t now_us)
{
    int32_t winner = r_winner_of(e->candidates, e->hash);
    uint64_t local_bit = (e->candidates >> m->shard) & 1u;

    moq_bytes_t parts[R_NS_MAX_PARTS];
    moqr_ns_t ns = canon_to_ns(e->canon, parts);

    /* A local publisher that lost to a remote winner is withdrawn before any
     * mirror work: force-withdraw purges its announce and every track it
     * sourced (SUB_DONE terminals carry PUBLISH_DONE TRACK_ENDED; the
     * publisher cancel and parked rejects carry UNINTERESTED) and queues
     * the publisher cancel.
     * On success the local candidate bit clears HERE, so the winner's mirror
     * installs in this same pass; the withdrawal's own NS_GONE reaches our
     * watcher next round as a REAL event (force-withdraw is not a mirror op,
     * so no self-echo is minted) and re-clears the already-clear bit — an
     * idempotent no-op. WOULD_BLOCK means the withdrawal is INCOMPLETE, not
     * that nothing happened: the purge is resumable, so some of the loser's
     * tracks may already be gone. What is guaranteed is that its announce is
     * still installed — the namespace effects are the withdrawal's final unit
     * — so the entry stays dirty, the loser stays counted as a holdout, and
     * the winner is NOT installed over an announce that still stands. Retried
     * next round, resuming from the tracks that remain. Any other result fail-stops the step (never
     * silently converge over a loser that could not be withdrawn). */
    if (local_bit != 0 && winner >= 0 && (uint16_t)winner != m->shard) {
        moqr_result_t rc =
            moqr_core_force_withdraw(m->core, ns, R_ERR_UNINTERESTED, now_us);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            /* Resumable, not inert: the purge got part way and queued that
             * much fan-out, so the drain is owed exactly as on success. */
            m->reconcile_owed_local_drain = true;
            mgr_update_holdout(m, e, true);
            return;   /* stay dirty; retry next round */
        }
        if (rc != MOQR_OK) {
            m->oom = true;   /* permanent: nothing mutated, nothing owed */
            return;
        }
        m->reconcile_owed_local_drain = true;
        e->candidates &= ~(1ull << m->shard);
        m->journal_epoch++;   /* candidate removal (the withdrawn loser) */
        local_bit = 0;
        winner = r_winner_of(e->candidates, e->hash);
    }

    /* Desired mirror: a remote winner with no local publisher here. */
    int32_t desired = R_MIRROR_NONE;
    if (winner >= 0 && (uint16_t)winner != m->shard && local_bit == 0) {
        desired = winner;
    }

    /* Holdout gauge: with enforcement above, a holdout persists only while its
     * withdrawal defers on WOULD_BLOCK (handled there); on every other path the
     * condition is re-derived from the possibly-just-cleared candidate set. */
    mgr_update_holdout(m, e, local_bit != 0 && winner >= 0 &&
                                  (uint16_t)winner != m->shard);

    if (desired == e->mirror) {
        e->dirty = false;   /* already converged */
        return;
    }

    /* Withdraw the stale mirror first. A transient failure (WOULD_BLOCK — the
     * intent ring is full) keeps the mirror installed and the entry dirty so we
     * retry next round; we never install a replacement over a mirror we failed to
     * remove. The echo counter is bumped only after the op succeeds — the echo is
     * queued to our watcher and observed a round later, never synchronously. */
    if (e->mirror != R_MIRROR_NONE) {
        if (moqr_core_unannounce(m->core, m->pb[e->mirror], ns) != MOQR_OK) {
            return;   /* stay dirty; retry next round */
        }
        m->reconcile_owed_local_drain = true;
        e->echo_unann++;
        e->mirror = R_MIRROR_NONE;
        m->journal_epoch++;   /* mirror teardown */
    }

    /* Install the new mirror. WRONG_STATE is permanent — a local publisher already
     * occupies the trie node (a late local announce, or this shard is the local
     * loser); stop retrying and hold on NONE (the local publisher is the holdout).
     * WOULD_BLOCK is transient: stay dirty and retry next round. */
    bool was_blocked = e->cap_blocked;
    if (e->cap_blocked) {
        e->cap_blocked = false;
        m->cap_blocked_count--;
    }
    if (desired != R_MIRROR_NONE && was_blocked &&
        moqr_core_announce_room(m->core, ns) == MOQR_ERR_CAPACITY) {
        /* Still no room: stay parked without another counted refusal. The
         * episode was counted once, by the install that first failed. */
        e->cap_blocked = true;
        m->cap_blocked_count++;
        e->dirty = false;
        return;
    }
    if (desired != R_MIRROR_NONE) {
        moqr_result_t rc = moqr_core_announce(m->core, m->pb[desired], ns);
        if (rc == MOQR_OK) {
            m->reconcile_owed_local_drain = true;
            e->echo_ann++;
            e->mirror = desired;
            m->journal_epoch++;   /* mirror install */
        } else if (rc == MOQR_ERR_WOULD_BLOCK) {
            return;   /* stay dirty; retry next round */
        } else if (rc == MOQR_ERR_CAPACITY) {
            /* The core holds only live namespaces right now. Room appears
             * only when capacity is released or becomes reclaimable (a
             * withdrawal, a freed track or reclaimed node), which moves the
             * core's room epoch, so park the entry until that epoch moves
             * past this refusal: bounded, no self-wake. Room
             * that appears is noticed at this shard's next step (any wake);
             * there is no timer for it. Retries ask moqr_core_announce_room
             * first, so one parked episode counts one refusal. */
            e->cap_blocked = true;
            m->cap_blocked_count++;
        }
        /* else WRONG_STATE / permanent: leave mirror NONE, clear dirty below. */
    }

    e->dirty = false;
}


/* -- the six-phase stepper ---------------------------------------------------- */

/* Apply one ready mailbox slot to the journal and release it. Returns false and
 * leaves the slot in place ONLY when an announce cannot be recorded because the
 * journal is full — the slot is retried next round (cursor lag). All other cases
 * (announce recorded, any unannounce) consume the slot. */
bool
shard_apply_slot(r_mgr *m, r_mailbox *mb, r_ctrl_msg *slot)
{
    if (slot->cluster) {
        moqr_cluster_export_t record = slot->record;
        moq_bytes_t parts[R_NS_MAX_PARTS];
        if (slot->op == R_OP_ANNOUNCE)
            record.ns = canon_to_ns(slot->canon, parts);
        record.route.metadata.hop_ids = slot->hops;
        moqr_result_t rc = moqr_core_internal_cluster_import(m->core,
            m->pb[slot->origin], &record, slot->op == R_OP_UNANNOUNCE);
        if (rc == MOQR_ERR_WRONG_STATE && slot->op == R_OP_ANNOUNCE) {
            /* A source pool slot was reused for a different namespace. The
             * old lifetime withdraws before its replacement is admitted. */
            rc = moqr_core_internal_cluster_import(m->core, m->pb[slot->origin],
                                          &record, true);
            if (rc == MOQR_OK || rc == MOQR_DONE) {
                m->reconcile_owed_local_drain = true;
                rc = moqr_core_internal_cluster_import(m->core, m->pb[slot->origin],
                                              &record, false);
            }
        }
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            m->cluster_inbound_retry = true;
            return false;
        }
        if (rc == MOQR_ERR_CAPACITY) return false;
        if (rc != MOQR_OK && rc != MOQR_DONE) {
            if (rc == MOQR_ERR_NOMEM) m->oom = true;
            else m->cluster_error = rc == MOQR_INTERNAL_LIMIT ? MOQR_ERR_CAPACITY : rc;
            return false;
        }
        m->reconcile_owed_local_drain = true;
        sh_free(&m->alloc, slot->canon, slot->canon_len);
        sh_free(&m->alloc, slot->hops,
                slot->record.route.metadata.hop_count * sizeof(uint64_t));
        slot->used = false;
        slot->hops = NULL;
        mb->count--;
        /* A full control mailbox may have held the producer on an unsent
         * alternative. Return credit on this same durable pop; concurrent
         * steppers must not depend on an unrelated future session event. */
        m->s->shards[m->shard].pop_wake |= 1ull << slot->origin;
        return true;
    }
    uint64_t bit = 1ull << slot->origin;
    if (slot->op == R_OP_ANNOUNCE) {
        r_jentry *e = jrn_get(m, slot->canon, slot->canon_len);
        if (e == NULL) {
            return false;   /* journal full: leave the slot, retry next round */
        }
        /* jrn_get adopts the canon on insert; on an existing entry the mailbox's
         * copy is redundant and freed below. */
        bool adopted = (e->canon == slot->canon);
        jrn_set_candidate(m, e, bit, true);
        if (!adopted) {
            sh_free(&m->alloc, slot->canon, slot->canon_len);
        }
    } else {
        r_jentry *e = jrn_find(m, slot->canon, slot->canon_len, NULL);
        if (e != NULL) {
            jrn_set_candidate(m, e, bit, false);
        }
        sh_free(&m->alloc, slot->canon, slot->canon_len);
    }
    slot->used = false;
    mb->count--;
    return true;
}


/* Phase 3 — reconcile every dirty namespace in lexicographic key order (the
 * journal is kept sorted, so array order IS lexicographic). This phase alone
 * mutates the core trie (mirror ops and loser force-withdrawals). */
void
shard_reconcile(moqr_shards_t *s, uint16_t shard, uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    if (s->cluster_enabled) {
        return; /* import applies atomic source-slot snapshots */
    }
    uint64_t room_epoch = moqr_core_room_epoch(m->core);
    if (m->cap_blocked_count != 0 && room_epoch != m->cap_epoch) {
        /* Room may have appeared in the core since a mirror install was
         * refused for room: retry those installs. */
        for (uint32_t i = 0; i < m->jrn_len; i++) {
            if (m->jrn[i].cap_blocked) {
                m->jrn[i].dirty = true;
            }
        }
    }
    /* A later withdrawal may free room for an entry already visited. Only
     * acknowledge this pass's starting epoch, so that entry retries next time. */
    m->cap_epoch = room_epoch;
    for (uint32_t i = 0; i < m->jrn_len; i++) {
        if (m->jrn[i].dirty) {
            mgr_reconcile_entry(m, &m->jrn[i], now_us);
        }
    }
}
