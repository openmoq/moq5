#include "shards_internal.h"
#include "demand_internal.h"
#include "data_internal.h"
#include "fill_internal.h"
#include "journal_internal.h"
#include "placement_internal.h"

/* Whether a message kind is DATA (byte-gated, counted against the data
 * pump) rather than control. */
static bool
dmsg_is_data(uint8_t kind)
{
    return (kind >= D_MSG_OBJ && kind <= D_MSG_SG_SEAL) ||
           (kind >= D_MSG_FILL_OPEN && kind <= D_MSG_FILL_END);
}


/* Logical bytes a queued message contributes to its channel's byte gauge:
 * payload + properties + the control canonical key. */
uint64_t
dmsg_logical_bytes(const r_demand_msg_t *m)
{
    return (uint64_t)moq_rcbuf_len(m->payload) +
           (uint64_t)moq_rcbuf_len(m->props) + m->canon_len;
}


/* Release everything a queued message still owns (clone refs + canon). */
void
dmsg_release(const moq_alloc_t *a, r_demand_msg_t *m)
{
    if (m->canon != NULL) {
        a->free(m->canon, m->canon_len, a->ctx);
        m->canon = NULL;
    }
    moq_rcbuf_decref(m->payload);
    m->payload = NULL;
    moq_rcbuf_decref(m->props);
    m->props = NULL;
}


/* Append a message to a directed channel. The message's owned memory (canon
 * + payload/props clones) TRANSFERS to the channel on success (the consumer
 * releases it at pop); on a refused push the caller keeps ownership and
 * retries later. Every message's logical bytes count into the gauge; DATA
 * kinds are additionally GATED on the byte cap. Control keys and bounded
 * Track Properties are separately covered by the allocation ceiling and
 * must not starve behind that data cap. */
static bool
dch_push(moqr_shards_t *s, uint16_t src, uint16_t dst, const r_demand_msg_t *m)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    uint64_t lb = dmsg_logical_bytes(m);
    bool is_data = dmsg_is_data(m->kind);
    bool pushed = false;
    pthread_mutex_lock(&ch->mu);
    if (ch->count + ch->held < ch->cap &&
        (!is_data || ch->bytes + ch->held_bytes + lb <= s->dch_byte_cap)) {
        r_demand_msg_t *slot =
            &ch->slots[(ch->head + ch->count) % ch->cap];
        *slot = *m;
        slot->round = s->round;
        slot->bytes_accounted = lb;
        ch->count++;
        ch->bytes += lb;
        /* High-water marks: this channel's own maxima, maintained inside
         * the same critical section that made the push durable. */
        if (ch->count + ch->held > ch->count_hwm) {
            ch->count_hwm = ch->count + ch->held;
        }
        if (ch->bytes + ch->held_bytes > ch->bytes_hwm) {
            ch->bytes_hwm = ch->bytes + ch->held_bytes;
        }
        /* pair accounting: data uses the SAME logical-byte currency the
         * producer spends against pump_bytes, so the cross-pair sums close
         * exactly against the per-shard aggregates */
        if (is_data) {
            ch->pair_data_msgs++;
            ch->pair_data_bytes += lb;
        } else if (m->kind <= D_MSG_SG_SEAL) {
            ch->pair_ctrl_msgs++;
        }
        pushed = true;
    } else if (ch->count + ch->held >= ch->cap) {
        /* refusal split mirrors the admission predicate's own evaluation
         * order: entries first, one increment per refusal event */
        ch->refused_entries++;
    } else {
        ch->refused_bytes++;
    }
    pthread_mutex_unlock(&ch->mu);
    return pushed;
}


/* Class-arbitrated push. The sticky token is PRODUCER-owned (only the src
 * shard touches its row) and transfers ONLY on a successful preferred-class
 * enqueue: a refused/full push never moves it, and the non-preferred class
 * may enqueue only when the preferred class has no eligible work — in which
 * case the token still does not move. Eligibility is computed by the caller
 * OUTSIDE the channel lock. */
bool
dch_push_arb(moqr_shards_t *s, uint16_t src, uint16_t dst,
             const r_demand_msg_t *m, bool is_data, bool other_eligible)
{
    uint8_t *tok = &s->arb[(uint32_t)src * s->shard_count + dst];
    uint8_t cls = is_data ? R_ARB_DATA : R_ARB_CTRL;
    if (*tok != cls && other_eligible) {
        r_mgr *pm = s->shards[src].mgr;
        if (pm != NULL) {
            pm->arb_refusals++;   /* producer-owned, like enq[] below */
        }
        return false;   /* the preferred class holds the claim on the slot */
    }
    if (!dch_push(s, src, dst, m)) {
        return false;   /* full: the token does not move on failure */
    }
    /* The push is durable: count it for the PRODUCER, by exact kind. Every
     * demand-channel enqueue funnels through here, and only success counts
     * — a refused attempt above never reaches this line. */
    {
        r_mgr *pm = s->shards[src].mgr;
        if (pm != NULL && m->kind >= D_MSG_DEMAND && m->kind <= D_MSG_SG_SEAL) {
            pm->enq[m->kind - 1u]++;
        }
    }

    if (*tok == cls) {
        *tok = (cls == R_ARB_DATA) ? R_ARB_CTRL : R_ARB_DATA;
    }
    return true;
}


/* The channel head, or NULL (empty, or the head is not yet visible under the
 * deterministic round barrier). The pointer stays valid until dch_pop —
 * there is exactly one consumer per channel and a published slot is never
 * mutated by the producer. */
r_demand_msg_t *
dch_head(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    r_demand_msg_t *m = NULL;
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        r_demand_msg_t *head = &ch->slots[ch->head];
        if (s->live_visibility || head->round < s->round) {
            m = head;
        }
    }
    pthread_mutex_unlock(&ch->mu);
    return m;
}


/* Consume the head (call only after its outcome is durable). Releases
 * whatever the message still owns — a consumer that took ownership of a
 * clone (ingest transfer) must null the ref first. */
static void
dch_pop(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        /* Credit the producer: this durable pop frees one src->dst slot, so
         * src may have work it was holding on channel-full. Only dst's own
         * stepper runs this (single writer on the accumulator); a held head
         * never pops and therefore never wakes anyone. */
        s->shards[dst].pop_wake |= 1ull << src;
        r_demand_msg_t *head = &ch->slots[ch->head];
        /* The gauge tracked the bytes as PUSHED; an ownership transfer since
         * then (nulled refs) does not change what the channel accounted. */
        ch->bytes -= head->bytes_accounted;
        dmsg_release(&s->alloc, head);
        ch->head = (ch->head + 1u) % ch->cap;
        ch->count--;
    }
    pthread_mutex_unlock(&ch->mu);
}


/* Take the head out of the ring while keeping its entry and bytes charged
 * to the channel (a fill-retrieval answer item moving into its window). The
 * caller has already taken every reference the head owned. No producer
 * credit: nothing was freed toward src yet. */
static uint64_t
dch_hold_head(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    uint64_t charged = 0;
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        r_demand_msg_t *head = &ch->slots[ch->head];
        charged = head->bytes_accounted;
        ch->bytes -= charged;
        ch->held++;
        ch->held_bytes += charged;
        dmsg_release(&s->alloc, head);   /* only the canon, if any */
        ch->head = (ch->head + 1u) % ch->cap;
        ch->count--;
    }
    pthread_mutex_unlock(&ch->mu);
    return charged;
}


/* Release held charge: `entries` items worth `bytes` have been served (or
 * dropped), so their room returns to the src->dst channel and src is
 * credited exactly as a durable pop credits it. dst's stepper only. */
void
dch_release_held(moqr_shards_t *s, uint16_t src, uint16_t dst,
                 uint32_t entries, uint64_t bytes)
{
    if (entries == 0) {
        return;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    ch->held -= entries;
    ch->held_bytes -= bytes;
    s->shards[dst].pop_wake |= 1ull << src;
    pthread_mutex_unlock(&ch->mu);
}


/* -- the intent router (phase-2 observation only) --------------------------- */

void
psub_metadata_release(r_mgr *m, r_psub *p)
{
    moqr_bind_t *b = m->s->shards[m->shard].bind;
    moqr_bind_meta_release(b, &p->ack_metadata);
    moqr_bind_meta_release(b, &p->latest_metadata);
    p->metadata_pending = false;
}


/* A synchronous owner-lane observation only: retain/coalesce, without core,
 * session or channel calls. ACK A is a separate pin even if B arrives before
 * the channel can carry A. A failed retain invalidates, never fabricates EMPTY. */
void
shards_metadata_changed(void *ctx, moqr_track_t track, uint64_t track_gen,
                        const moqr_bind_meta_ref_t *latest)
{
    r_mgr *m = ctx;
    moqr_bind_t *b = m->s->shards[m->shard].bind;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        r_psub *p = &m->psub[i];
        if (!p->used || !p->has_otrack || p->undemanding ||
            p->terminating || p->done_pending ||
            p->otrack._opaque != track._opaque || p->otrack_gen != track_gen ||
            (p->ack_sent && latest->revision <= p->metadata_sent_revision)) {
            continue;
        }
        moqr_bind_meta_release(b, &p->latest_metadata);
        if (moqr_bind_meta_retain(b, latest, &p->latest_metadata) != MOQR_OK) {
            p->latest_metadata = *latest;
            p->latest_metadata.state = MOQR_BIND_META_UNAVAILABLE;
            p->latest_metadata.storage = NULL;
            p->latest_metadata.bytes = (moq_bytes_t){ NULL, 0 };
        }
        p->metadata_pending = true;
    }
}


/* Called from inside moqr_bind_pump for the manager binding's reserved cookie.
 * An NS_FOUND/NS_GONE that is one of the manager's own mirror echoes consumes a
 * pending per-entry echo and is dropped; the rest update this shard's local
 * candidate bit and mark the namespace dirty. An UPSTREAM_SUBSCRIBE is downstream
 * demand for a namespace this shard only MIRRORS — the core emitted it to the
 * mirror's reserved pseudo-binding cookie, so only remote-owned demand reaches
 * here (a local publisher's demand targets a real cookie and never routes). It
 * is recorded for FORWARDING (the demand phase round-trips it to the owner,
 * who answers over the reply channel). It must NOT mutate the core trie (that
 * is phase 3) or reenter the core. */
bool
shards_router(void *ctx, const moqr_intent_t *it, uint64_t now_us)
{
    (void)now_us;
    r_mgr *m = ctx;
    if (it->kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
        /* Borrowed + non-deferrable: consume in-call, deep-copying the
         * ns+name into an owned key (the intent's spans die with this call)
         * plus the owned scalars. A full array or a failed copy can't park a
         * borrowed intent → fail-stop (never a silent drop). */
        if (m->pend_len >= m->pend_cap) {
            m->oom = true;
            return true;
        }
        uint64_t base = MOQR_SHARD_COOKIE_BASE;
        if (it->binding_cookie < base ||
            it->binding_cookie - base >= m->s->shard_count) {
            m->oom = true;   /* not a mirror pseudo-binding: unroutable */
            return true;
        }
        uint32_t clen = 0;
        uint8_t *canon = canon2_build(it->ns_parts, it->ns_count, it->name,
                                      &clen, &m->alloc);
        if (canon == NULL) {
            m->oom = true;
            return true;
        }
        r_pdemand *d = &m->pend[m->pend_len++];
        memset(d, 0, sizeof(*d));
        d->track = it->track;
        d->track_gen = it->track_gen;
        d->demand_id = ++m->next_demand_id;
        d->canon = canon;
        d->canon_len = clen;
        d->origin = (uint16_t)(it->binding_cookie - base);
        if (m->s->cluster_enabled) {
            moqr_cluster_advertisement_id_t route;
            moqr_result_t rc = moqr_core_internal_cluster_upstream_route(m->core,
                it->track, it->track_gen, &route, &d->requester_hop_id,
                &d->requester_correlation_id);
            if (rc != MOQR_OK) {
                /* The source can withdraw after this intent was queued and
                 * before the binding drains it. Its generation is then moot;
                 * never turn it into a fresh lookup on a replacement route. */
                sh_free(&m->alloc, d->canon, d->canon_len);
                m->pend_len--;
                if (rc != MOQR_DONE && rc != MOQR_ERR_STALE_HANDLE &&
                    rc != MOQR_ERR_WRONG_STATE) m->oom = true;
                return true;
            }
            if (moq_handle_session_tag(route.source._opaque) != (uint32_t)d->origin + 1u) {
                sh_free(&m->alloc, d->canon, d->canon_len);
                m->pend_len--;
                m->cluster_error = MOQR_ERR_INTERNAL;
                return true;
            }
            d->source_advertisement = route;
        }
        d->state = D_ST_RECORDED;
        d->term_overrun = false;
        d->term_quiet = false;
        d->popen_row = UINT32_MAX;
        d->ack_has_largest = false;
        d->ack_largest_group = 0;
        d->ack_largest_object = 0;
        d->feed_seen = false;
        d->feed_min_group = 0;
        return true;
    }
    if (it->kind == MOQR_INTENT_ACCEPT_SUB ||
        it->kind == MOQR_INTENT_REJECT_SUB ||
        it->kind == MOQR_INTENT_SUB_DONE) {
        /* Owner role: the pump-sub's lifecycle intents come back on the
         * requester's pseudo-binding with cookie = demand_id. A demand id is
         * only monotonic per REQUESTER, so the entry key is {src, demand_id}
         * — src recovered from the pseudo-binding cookie the intent rode in
         * on. Only reply OBLIGATIONS are recorded here (flags on the
         * admitted-demand entry); the demand phase pushes the actual channel
         * messages, so the bind pump never touches a channel mutex. An
         * intent with no matching entry is moot (the demand was torn down
         * first). */
        uint64_t base = MOQR_SHARD_COOKIE_BASE;
        if (it->binding_cookie < base ||
            it->binding_cookie - base >= m->s->shard_count) {
            return true;   /* not a per-origin pseudo-binding: not ours */
        }
        uint16_t src = (uint16_t)(it->binding_cookie - base);
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            r_psub *p = &m->psub[i];
            if (!p->used || p->src != src || p->demand_id != it->cookie) {
                continue;
            }
            if (it->kind == MOQR_INTENT_ACCEPT_SUB) {
                /* A cancelled demand never ACKs — the requester is gone and
                 * the staged teardown will retire the pump-sub. */
                if (!p->undemanding && !p->ack_pending && !p->ack_sent &&
                    p->has_otrack && moqr_sub_eq(p->sub, it->sub)) {
                    /* No lookup here: the binding may already hold a newer
                     * directory set than this scalar reply was pinned to. */
                    (void)moqr_bind_meta_retain_executing(
                        m->s->shards[m->shard].bind, it, &p->ack_metadata);
                    p->ack_pending = true;
                    p->has_largest = it->has_largest;
                    p->largest_group = it->largest_group;
                    p->largest_object = it->largest_object;
                }
            } else {
                p->done_pending = true;
                p->done_pre_ack = (it->kind == MOQR_INTENT_REJECT_SUB);
                p->done_code = it->error_code;
                p->done_pd = it->pd;
                /* The terminal already retired the pump-sub in the core;
                 * a racing teardown must not re-run stage 1. */
                p->pump_sub_retired = true;
            }
            break;
        }
        return true;
    }
    if (it->kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
        /* Requester role: an acknowledged remote demand went idle — the
         * ACTIVE linger elapsed with no subscribers, and the core emitted the
         * unsubscribe toward the mirror pseudo-binding carrying
         * upstream_cookie = demand_id. Flip the entry to UNDEMANDING; the
         * demand phase owns pushing the UNDEMAND (durable retry on a full
         * channel). A probe-emitted unsubscribe carries cookie 0 and matches
         * nothing; ids start at 1. */
        for (uint32_t i = 0; i < m->pend_len; i++) {
            if (m->pend[i].demand_id == it->cookie &&
                m->pend[i].state == D_ST_ACKED) {
                /* Route through the staged terminal, quietly: the track is
                 * already WARM, but partially ingested OPEN records must be
                 * abandoned (STOPPED) before the demand and its progress row
                 * disappear — a WARM log keeps its records, and a later
                 * rejoin must never meet stale partial state. */
                m->pend[i].state = D_ST_TERMINATING;
                m->pend[i].term_code = 0;
                m->pend[i].term_overrun = false;
                m->pend[i].term_quiet = true;
                break;
            }
        }
        return true;
    }
    if (it->kind != MOQR_INTENT_NS_FOUND && it->kind != MOQR_INTENT_NS_GONE) {
        return true;
    }
    if (m->s->cluster_enabled) {
        return true; /* route snapshots observe all alternatives, not fan-out */
    }
    uint8_t op = (it->kind == MOQR_INTENT_NS_FOUND) ? R_OP_ANNOUNCE
                                                    : R_OP_UNANNOUNCE;
    uint32_t len = 0;
    uint8_t *canon = canon_build(it->ns_parts, it->ns_count, &len, &m->alloc);
    if (canon == NULL) {
        m->oom = true;
        return true;   /* borrowed view can't be parked; bounded + counted */
    }
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    if (e != NULL) {
        uint16_t *echo = (op == R_OP_ANNOUNCE) ? &e->echo_ann : &e->echo_unann;
        if (*echo > 0) {
            (*echo)--;                    /* our own mirror echo — suppress */
            sh_free(&m->alloc, canon, len);
            return true;
        }
    }
    uint64_t bit = 1ull << m->shard;
    if (op == R_OP_ANNOUNCE) {
        if (e == NULL) {
            e = jrn_get(m, canon, len);   /* adopts canon on insert */
            if (e == NULL) {
                m->oom = true;
                sh_free(&m->alloc, canon, len);
                return true;
            }
        } else {
            sh_free(&m->alloc, canon, len);   /* existing entry; drop the dup */
        }
        jrn_set_candidate(m, e, bit, true);
    } else {
        sh_free(&m->alloc, canon, len);       /* find-only; never adopted */
        if (e != NULL) {
            jrn_set_candidate(m, e, bit, false);
        }
    }
    return true;
}


/* -- fill retrieval: requester ----------------------------------------------- */

/* A record of `group` reached this shard's log through demand d's feed. */
static void
pend_note_feed(r_pdemand *d, uint64_t group)
{
    if (!d->feed_seen || group < d->feed_min_group) {
        d->feed_min_group = group;
    }
    d->feed_seen = true;
}


/* Drop a demand entry (frees its owned key) with keep-compaction handled by
 * the caller's loop. */
static void
pend_free_entry(r_mgr *m, r_pdemand *d)
{
    fret_demand_gone(m, d->demand_id);
    sh_free(&m->alloc, d->canon, d->canon_len);
    d->canon = NULL;
    if (d->popen_row != UINT32_MAX) {
        psg_row_clear(&m->popen[(size_t)d->popen_row * m->s->sg_slots],
                      m->s->sg_slots);
        m->popen_row_used[d->popen_row] = 0;
        d->popen_row = UINT32_MAX;
    }
}


/* The demand's progress row, claimed lazily on its first open object; NULL
 * when none is free (a demand terminal, never an untracked stream). */
static r_psg_t *
pend_popen_row(r_mgr *m, r_pdemand *d)
{
    if (d->popen_row != UINT32_MAX) {
        return &m->popen[(size_t)d->popen_row * m->s->sg_slots];
    }
    for (uint32_t i = 0; i < m->pend_cap; i++) {
        if (!m->popen_row_used[i]) {
            m->popen_row_used[i] = 1;
            d->popen_row = i;
            return &m->popen[(size_t)i * m->s->sg_slots];
        }
    }
    return NULL;
}


/* The retryable requester-side terminal: abandon every open object this
 * demand still has with reset_code (each commit clears its slot's object
 * fields, so a retry never repeats a completed abandon; discarding objects
 * have no record to abandon), then source_done(done_code). Every DONE-shaped
 * teardown of an ACKed demand — owner DONE, capacity terminal, re-target,
 * idle unsubscribe — funnels through here so no path can strand an orphan
 * OPEN record, WARM or ACTIVE. Returns false while a stage holds
 * (WOULD_BLOCK, or a hard error after latching fail-stop) — the caller
 * keeps the entry and its channel head durable. */
/* The shard's own terminals are meanings, not any draft's numbers: the
 * connection that reads them may speak either draft. */
moqr_pd_desc_t
shard_local_done(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}


static moqr_reset_desc_t
shard_local_reset(moqr_reset_cause_t cause)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_local(cause, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}


/* A terminal the owner forwarded from its publisher (a wire status), as
 * opposed to one a relay chose (a local meaning: capacity, withdrawal,
 * re-target). Only the publisher's own end lets local subscribers first
 * receive what this shard already holds. */
static bool
shard_pd_forwarded(moqr_pd_desc_t pd)
{
    return pd.tag == MOQR_CODE_WIRE_D16 || pd.tag == MOQR_CODE_WIRE_D18 ||
           pd.tag == MOQR_CODE_WIRE_D22;
}


/* A track handle identifies retained storage; a source transition can keep
 * that handle and change only track_gen. Every queued response must match
 * the current source generation before touching that storage. */
static bool
pend_source_current(const r_mgr *m, const r_pdemand *d)
{
    uint64_t generation = 0;
    return moqr_core_track_current_generation(m->core, d->track, &generation)
               == MOQR_OK && generation == d->track_gen;
}


static bool
pend_terminal_step(moqr_shards_t *s, r_mgr *m, r_pdemand *d,
                   moqr_pd_desc_t done, moqr_reset_desc_t reset,
                   bool graceful, uint64_t now_us)
{
    if (!pend_source_current(m, d) &&
        !moqr_core_internal_source_idle(m->core, d->track, d->track_gen)) {
        return true; /* never abandon records belonging to a replacement */
    }
    /* The terminal this shard is resolving into its own core, recorded where
     * it is actually consumed rather than where it arrived. */
    m->last_done_pd = done;
    if (d->popen_row != UINT32_MAX) {
        r_psg_t *row = &m->popen[(size_t)d->popen_row * s->sg_slots];
        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
            if (!row[sl].used || !row[sl].object_open) {
                continue;
            }
            if (!row[sl].discarding) {
                moqr_result_t ar = moqr_core_abandon_record(
                    m->core, d->track, row[sl].group, row[sl].subgroup,
                    row[sl].object_id, reset);
                if (ar == MOQR_ERR_WOULD_BLOCK) {
                    return false;
                }
                if (ar != MOQR_OK && ar != MOQR_ERR_TOO_OLD &&
                    ar != MOQR_ERR_STALE_HANDLE && ar != MOQR_DONE) {
                    /* WRONG_STATE/INVAL: the record survives in a state
                     * this teardown cannot scrub — clearing only the
                     * bookkeeping would strand a partial OPEN record, so
                     * fail-stop and HOLD instead. */
                    m->oom = true;
                    return false;
                }
                /* OK, or moot (evicted/raced): the stage is done. */
            }
            row[sl].object_open = false;
            row[sl].discarding = false;
            row[sl].object_id = 0;
            row[sl].next_chunk = 0;
        }
    }
    /* The owner's own DONE lets each local subscriber receive what this
     * shard already holds first; a terminal this shard chooses ends them at
     * once. */
    moqr_result_t tr = graceful
        ? moqr_core_source_finish(m->core, d->track, d->track_gen, done, now_us)
        : moqr_core_source_done(m->core, d->track, d->track_gen, done, now_us);
    if (tr == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    if (tr != MOQR_OK && tr != MOQR_ERR_STALE_HANDLE &&
        tr != MOQR_ERR_WRONG_STATE) {
        m->oom = true;
        return false;
    }
    return true;
}


/* The pending-demand entry for a demand id, or NULL. */
r_pdemand *
pend_find(r_mgr *m, uint64_t demand_id)
{
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].demand_id == demand_id) {
            return &m->pend[i];
        }
    }
    return NULL;
}


static bool
demand_reply_matches(const r_pdemand *d, uint16_t src,
                      const r_demand_msg_t *msg)
{
    return d != NULL && d->origin == src && d->demand_id == msg->demand_id &&
           moqr_track_eq(d->track, msg->track) && d->track_gen == msg->track_gen;
}


/* The message remains the owner of its clone. replace makes an independent
 * requester-budget copy (or publishes UNAVAILABLE on copy refusal) before
 * the ACK/fill behind it can be consumed. Record the REMOTE revision only
 * after that outcome is durable; a held initial ACK must not allocate twice. */
static moqr_result_t
demand_metadata_install(r_mgr *m, r_pdemand *d, const r_demand_msg_t *msg,
                         uint64_t now_us)
{
    moqr_bind_meta_ref_t input;
    memset(&input, 0, sizeof(input));
    input.state = msg->metadata.state;
    input.wire_version = msg->metadata.wire_version;
    input.source_binding = msg->metadata.source_binding;
    input.source_handle = msg->metadata.source_handle;
    input.source_generation = msg->metadata.source_generation;
    input.bytes = (moq_bytes_t){ moq_rcbuf_data(msg->props),
                                moq_rcbuf_len(msg->props) };
    uint64_t revision = 0;
    moqr_result_t rc = moqr_bind_meta_replace(
        m->s->shards[m->shard].bind, d->track, d->track_gen, &input,
        now_us, &revision);
    if (revision != 0) {
        d->metadata_bound = true;
        d->metadata_track = msg->metadata.track;
        d->metadata_track_gen = msg->metadata.track_gen;
        d->metadata_revision = msg->metadata.revision;
        return MOQR_OK;   /* includes the durable CAPACITY/NOMEM invalidation */
    }
    return rc;
}


/* Retire one pending-demand entry in place (swap-remove; order-free). */
void
pend_retire(r_mgr *m, r_pdemand *d)
{
    pend_free_entry(m, d);
    *d = m->pend[m->pend_len - 1u];
    m->pend_len--;
}


/* Control work this manager has pending toward `dst` — the DATA class's
 * eligibility input for the sticky arbiter (a data push may take a contended
 * slot only when no control is waiting). Durable retry state only, computed
 * outside any channel lock: unsent demands and cancel notices (requester
 * role) and unpushed ACK/latest-metadata/DONE replies (owner role). An inbound
 * request awaiting its refusal is deliberately not counted — its reply retries
 * behind a durable head, so missing it costs control one round, never a
 * message. */
bool
ctrl_work_pending(const r_mgr *m, uint16_t dst)
{
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].origin == dst &&
            (m->pend[i].state == D_ST_RECORDED ||
             m->pend[i].state == D_ST_UNDEMANDING)) {
            return true;
        }
    }
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == dst &&
            (p->ack_pending || p->done_pending ||
             (p->ack_sent && p->metadata_pending))) {
            return true;
        }
    }
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        const r_fret_t *f = &m->fret[i];
        if (f->used && f->origin == dst &&
            (f->req_owed || f->cancel_owed || f->more_owed)) {
            return true;
        }
    }
    return false;
}


/* A latest-state obligation is a causal prerequisite for data on this pair.
 * Hold the pair until phase 5 queues it: the old DATA eligibility bit must
 * not let a sticky arbiter wait for data that itself waits for metadata. */
bool
owner_metadata_pending(const r_mgr *m, uint16_t dst)
{
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == dst && !p->undemanding &&
            p->ack_sent && p->metadata_pending) {
            return true;
        }
    }
    return false;
}


/* Whether this manager's data phase reported unpushed extraction work toward
 * dst — the CTRL class's eligibility input (recomputed every data phase, so
 * at most one step stale). */
bool
data_work_pending(const r_mgr *m, uint16_t dst)
{
    return ((m->data_pending_mask >> dst) & 1u) != 0 &&
           !owner_metadata_pending(m, dst);
}


/* One bounded clone is staged at a time. Failure becomes a durable scalar
 * UNAVAILABLE obligation even if the channel then refuses it; no allocation
 * retry can turn that failed copy into EMPTY or leave requester A available. */
static void
demand_metadata_message(r_mgr *m, r_psub *p, uint8_t kind,
                         moqr_bind_meta_ref_t *ref, r_demand_msg_t *msg)
{
    memset(msg, 0, sizeof(*msg));
    msg->kind = kind;
    msg->demand_id = p->demand_id;
    msg->track = p->rtrack;
    msg->track_gen = p->rtrack_gen;
    if (ref->state == MOQR_BIND_META_PRESENT &&
        (ref->bytes.len == 0 || ref->bytes.data == NULL ||
         ref->bytes.len > MOQR_BIND_META_SET_MAX ||
         moq_rcbuf_create(&m->alloc, ref->bytes.data, ref->bytes.len,
                          &msg->props) != MOQ_OK)) {
        moqr_bind_meta_ref_t unavailable = *ref;
        unavailable.state = MOQR_BIND_META_UNAVAILABLE;
        unavailable.storage = NULL;
        unavailable.bytes = (moq_bytes_t){ NULL, 0 };
        moqr_bind_meta_release(m->s->shards[m->shard].bind, ref);
        *ref = unavailable;
    }
    msg->metadata.track = p->otrack;
    msg->metadata.track_gen = p->otrack_gen;
    msg->metadata.state = ref->state;
    msg->metadata.wire_version = ref->wire_version;
    msg->metadata.source_binding = ref->source_binding;
    msg->metadata.source_handle = ref->source_handle;
    msg->metadata.source_generation = ref->source_generation;
    msg->metadata.revision = ref->revision;
}


/* Push a DONE reply answering `req` back to its requester. Returns false on
 * a refused push (full channel, or the arbiter holding the slot for data);
 * the caller leaves its durable record in place and retries. */
static bool
demand_reply_done(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                  const r_demand_msg_t *req, bool pre_ack, uint64_t code,
                  uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    r_demand_msg_t reply;
    memset(&reply, 0, sizeof(reply));
    reply.kind = D_MSG_DONE;
    reply.pre_ack = pre_ack;
    reply.error_code = code;
    reply.demand_id = req->demand_id;
    reply.track = req->track;
    reply.track_gen = req->track_gen;
    if (!dch_push_arb(s, shard, dst, &reply, false,
                      data_work_pending(m, dst))) {
        return false;
    }
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}


/* Phase 5 — the demand phase: probe local liveness, then forward. Per entry:
 * - The PROBE (moqr_core_upstream_cancel): a downstream subscriber closing
 *   while the track is PENDING fires no intent, so this is the only way to
 *   learn a demand died locally. WRONG_STATE = still wanted (or already
 *   active) — proceed; OK/STALE = locally dead — drop a RECORDED entry
 *   silently (nothing was sent) or turn a SENT one into UNDEMANDING;
 *   WOULD_BLOCK = hold the entry untouched this round.
 * - The RE-TARGET check: a SENT/ACKED demand whose namespace mirror now
 *   points at a DIFFERENT shard fails terminally (GOING_AWAY) rather than
 *   silently migrating — the subscriber's retry rides the new mirror. A
 *   mirror that is merely gone is NOT a re-target: the owner still holds the
 *   demand and its answer (or terminal) is guaranteed to arrive.
 * - RECORDED entries push DEMAND toward the recorded owner; a full channel
 *   keeps them RECORDED (the entry is the durable record). On push: SENT.
 * - UNDEMANDING entries push UNDEMAND, then drop; a full channel retries.
 * The owner's answer resolves the requester in the phase-1 inbound drain —
 * refusal is counted THERE, when the round-trip actually completes.
 * After the requester walk, the OWNER walk services the admitted-demand
 * table: pending ACK/DONE replies push here (FIFO order per demand is the
 * channel's), and a cancelled demand runs the staged teardown — stage 1
 * retires the pump-sub exactly once, stage 2 cancels the owner track's
 * upstream attempt if this was its last interest; a WOULD_BLOCK in stage 2
 * holds the entry WITHOUT re-running stage 1 (the retired flag is the
 * stage boundary). */
void
shard_demand_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                   uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    uint32_t keep = 0;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        r_pdemand *d = &m->pend[i];
        bool drop = false;
        if (!pend_source_current(m, d)) {
            if (d->state == D_ST_RECORDED) {
                drop = true;
            } else if ((d->state == D_ST_ACKED || d->state == D_ST_TERMINATING) &&
                       moqr_core_internal_source_idle(m->core, d->track,
                                                       d->track_gen)) {
                /* An idle release keeps this old log, so its unfinished
                 * records still need ordinary quiet cancellation cleanup. */
                if (d->state == D_ST_ACKED) {
                    d->state = D_ST_TERMINATING;
                    d->term_code = 0;
                    d->term_overrun = false;
                    d->term_quiet = true;
                }
            } else {
                d->state = D_ST_UNDEMANDING;
            }
        }
        if (d->state == D_ST_TERMINATING) {
            /* Commit the local terminal, one retryable stage at a time; the
             * triggering channel head stays durable (and keeps every later
             * message behind it) until this lands. */
            /* A quiet terminal is this relay stopping the stream; otherwise
             * the demand's terminal ends it. Both are relay-chosen causes
             * here — the PUBLISH_DONE status rides `term_code` separately and
             * no longer doubles as the reset number. */
            moqr_reset_desc_t rdesc =
                d->term_quiet ? shard_local_reset(MOQR_RESET_CANCELLED)
                              : shard_local_reset(MOQR_RESET_CANCELLED);
            if (!pend_terminal_step(s, m, d,
                                    shard_local_done(MOQR_PD_TRACK_ENDED),
                                    rdesc, false, now_us)) {
                m->pend[keep++] = *d;
                continue;
            }
            if (d->term_quiet) {
                /* An ordinary local stop, not a loss: no terminal metric. */
            } else if (d->term_overrun) {
                m->remote_demand_term_overrun++;
            } else {
                m->remote_demand_term_capacity++;
            }
            d->state = D_ST_UNDEMANDING;   /* the owner is owed the notice */
        }
        if (d->state == D_ST_RECORDED || d->state == D_ST_SENT) {
            moqr_result_t pr = moqr_core_upstream_cancel(m->core, d->track,
                                                         d->track_gen, now_us);
            if (pr == MOQR_ERR_WOULD_BLOCK) {
                m->pend[keep++] = *d;   /* hold untouched; retry next round */
                continue;
            }
            if (pr == MOQR_OK || pr == MOQR_ERR_STALE_HANDLE) {
                if (d->state == D_ST_RECORDED) {
                    drop = true;        /* never sent: nothing to undo */
                } else {
                    d->state = D_ST_UNDEMANDING;
                }
            } else if (pr != MOQR_ERR_WRONG_STATE) {
                m->oom = true;          /* unexpected; never silently converge */
                m->pend[keep++] = *d;
                continue;
            }
        }
        if (!drop && (d->state == D_ST_SENT || d->state == D_ST_ACKED)) {
            r_jentry *e = jrn_find(m, d->canon, canon2_ns_len(d->canon), NULL);
            if (e != NULL && e->mirror != R_MIRROR_NONE &&
                e->mirror != (int32_t)d->origin) {
                if (d->state == D_ST_ACKED) {
                    /* The full requester terminal: partially open records
                     * from the OLD owner are abandoned before the demand
                     * moves on — stale state must never meet the new
                     * owner's track generation. */
                    if (!pend_terminal_step(s, m, d,
                                            shard_local_done(
                                                MOQR_PD_TRACK_ENDED),
                                            shard_local_reset(
                                                MOQR_RESET_CANCELLED),
                                            false, now_us)) {
                        m->pend[keep++] = *d;   /* retry next round */
                        continue;
                    }
                } else {
                    moqr_result_t rc = moqr_core_upstream_error(
                        m->core, d->track, d->track_gen, R_ERR_INTERNAL,
                        now_us);
                    if (rc == MOQR_ERR_WOULD_BLOCK) {
                        m->pend[keep++] = *d;   /* retry next round */
                        continue;
                    }
                    if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE &&
                        rc != MOQR_ERR_WRONG_STATE) {
                        m->oom = true;
                        m->pend[keep++] = *d;
                        continue;
                    }
                }
                /* Terminated locally (or already moot); the OLD owner is
                 * still owed the cancel notice. */
                d->state = D_ST_UNDEMANDING;
            }
        }
        if (!drop && d->state == D_ST_RECORDED) {
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_DEMAND;
            msg.demand_id = d->demand_id;
            msg.track = d->track;
            msg.track_gen = d->track_gen;
            msg.source_advertisement = d->source_advertisement;
            msg.requester_hop_id = d->requester_hop_id;
            msg.requester_correlation_id = d->requester_correlation_id;
            msg.canon = canon_dup(d->canon, d->canon_len, &m->alloc);
            msg.canon_len = d->canon_len;
            if (msg.canon != NULL &&
                dch_push_arb(s, shard, d->origin, &msg, false,
                             data_work_pending(m, d->origin))) {
                d->state = D_ST_SENT;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d->origin;
                }
            } else {
                sh_free(&m->alloc, msg.canon, msg.canon_len);
                /* full channel (or dup OOM): stay RECORDED, retry */
            }
        }
        if (!drop && d->state == D_ST_UNDEMANDING) {
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_UNDEMAND;
            msg.demand_id = d->demand_id;
            msg.track = d->track;
            msg.track_gen = d->track_gen;
            if (dch_push_arb(s, shard, d->origin, &msg, false,
                             data_work_pending(m, d->origin))) {
                drop = true;            /* notice delivered to the channel */
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d->origin;
                }
            }
        }
        if (drop) {
            pend_free_entry(m, d);
        } else {
            m->pend[keep++] = *d;
        }
    }
    m->pend_len = keep;

    requester_fill_phase(s, shard, pushed_dst_mask);

    /* Owner walk: service the admitted-demand table. */
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        r_psub *p = &m->psub[i];
        if (!p->used) {
            continue;
        }
        if (p->undemanding) {
            if (!p->pump_sub_retired) {
                /* Stage 1: retire the pump-sub. OK and STALE both mean it no
                 * longer exists (a terminal may have retired it first);
                 * unsubscribe emits no intent, so it cannot block. */
                (void)moqr_core_unsubscribe(m->core, p->sub, now_us);
                p->pump_sub_retired = true;
            }
            if (p->has_otrack) {
                /* Stage 2: if the pump-sub was the owner track's last
                 * interest and its upstream attempt is still PENDING, cancel
                 * it — otherwise the track would sit in PENDING limbo with
                 * nobody left to resolve it. WRONG_STATE = the track is
                 * alive for other subscribers (or already active); STALE =
                 * its identity already moved on — a terminal resolved it.
                 * Both are moot here BECAUSE stage 1 has run: the captured
                 * generation was current until the pump-sub retired, so a
                 * mismatch can only mean another path resolved the track. */
                moqr_result_t rc = moqr_core_upstream_cancel(
                    m->core, p->otrack, p->otrack_gen, now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    continue;   /* hold at stage 2; stage 1 never re-runs */
                }
            }
            psub_free(m, p);
            continue;
        }
        if (p->terminating && !p->term_cancelled) {
            /* The captured-track cancellation stage of the owner-side
             * capacity terminal: WOULD_BLOCK holds HERE without re-running
             * the (already confirmed) STREAM_ERROR release, and the DONE
             * below waits for it. */
            if (p->has_otrack) {
                moqr_result_t rc = moqr_core_upstream_cancel(
                    m->core, p->otrack, p->otrack_gen, now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    continue;
                }
            }
            p->term_cancelled = true;
        }
        if (p->ack_pending) {
            r_demand_msg_t msg;
            demand_metadata_message(m, p, D_MSG_ACK, &p->ack_metadata, &msg);
            msg.has_largest = p->has_largest;
            msg.largest_group = p->largest_group;
            msg.largest_object = p->largest_object;
            if (!dch_push_arb(s, shard, p->src, &msg, false,
                              data_work_pending(m, p->src))) {
                dmsg_release(&s->alloc, &msg);
                continue;   /* refused: the flag is the durable record */
            }
            p->ack_pending = false;
            p->ack_sent = true;
            p->metadata_sent_revision = p->ack_metadata.revision;
            moqr_bind_meta_release(s->shards[shard].bind, &p->ack_metadata);
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << p->src;
            }
        }
        if (p->ack_sent && p->metadata_pending) {
            if (p->latest_metadata.revision > p->metadata_sent_revision) {
                r_demand_msg_t msg;
                demand_metadata_message(m, p, D_MSG_METADATA,
                                         &p->latest_metadata, &msg);
                if (!dch_push_arb(s, shard, p->src, &msg, false,
                                  data_work_pending(m, p->src))) {
                    dmsg_release(&s->alloc, &msg);
                    continue;
                }
                p->metadata_sent_revision = p->latest_metadata.revision;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << p->src;
                }
            }
            moqr_bind_meta_release(s->shards[shard].bind, &p->latest_metadata);
            p->metadata_pending = false;
        }
        if (p->done_pending) {
            /* Reached only with no ACK owed (a still-full ACK push moved to
             * the next entry above), and the FIFO channel preserves
             * ACK-before-DONE when both push in this same pass. */
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_DONE;
            msg.pre_ack = p->done_pre_ack;
            msg.error_code = p->done_code;
            msg.pd = p->done_pd;
            msg.demand_id = p->demand_id;
            msg.track = p->rtrack;
            msg.track_gen = p->rtrack_gen;
            if (!dch_push_arb(s, shard, p->src, &msg, false,
                              data_work_pending(m, p->src))) {
                continue;   /* refused: the flag is the durable record */
            }
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << p->src;
            }
            /* Terminal delivered: the demand is closed; its progress row is
             * freed only now, after the DONE is durably queued. */
            psub_free(m, p);
        }
    }
}


/* Owner role: admit one DEMAND as an ordinary subscribe on the requester's
 * pseudo-binding, recording the pump-sub in the admitted-demand table so the
 * router can correlate its lifecycle intents back to the requester. Returns
 * false when the DEMAND must stay at the channel head (a full reply channel
 * or a transiently-full core); true once its outcome is durable (a pump-sub
 * entry exists, or a refusing DONE reached the reply channel). */
static bool
owner_admit_demand(moqr_shards_t *s, uint16_t shard, uint16_t src,
                   const r_demand_msg_t *msg, uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    r_psub *p = NULL;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        if (!m->psub[i].used) {
            p = &m->psub[i];
            break;
        }
    }
    if (p == NULL) {
        /* Pump-sub table full: a capacity refusal, which the requester's
         * binding encodes per draft with a Retry Interval. The durable
         * outcome is the refusing DONE itself. */
        return demand_reply_done(s, shard, src, msg, true,
                                 MOQR_REQUEST_ERROR_CAPACITY, pushed_dst_mask);
    }
    moq_bytes_t parts[R_NS_MAX_PARTS];
    moq_bytes_t name;
    moqr_ns_t ns = canon2_decode(msg->canon, parts, &name);
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.cookie = msg->demand_id;
    moqr_sub_t sub;
    memset(&sub, 0, sizeof(sub));
    if (s->cluster_enabled && moq_handle_session_tag(msg->source_advertisement.source._opaque) != (uint32_t)shard + 1u)
        return demand_reply_done(s, shard, src, msg, true, R_ERR_INTERNAL, pushed_dst_mask);
    moqr_result_t rc = s->cluster_enabled
        ? moqr_core_internal_subscribe_cluster(m->core, m->pb[src], &req,
            msg->source_advertisement,
            msg->requester_hop_id, msg->requester_correlation_id, &sub)
        : moqr_core_subscribe(m->core, m->pb[src], &req, &sub);
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        return false;   /* intent ring full: DEMAND stays durable */
    }
    if (rc != MOQR_OK) {
        return demand_reply_done(s, shard, src, msg, true,
                                 rc == MOQR_ERR_CAPACITY
                                     ? MOQR_REQUEST_ERROR_CAPACITY
                                     : R_ERR_INTERNAL,
                                 pushed_dst_mask);
    }
    memset(p, 0, sizeof(*p));
    p->used = true;
    p->src = src;
    p->demand_id = msg->demand_id;
    p->rtrack = msg->track;
    p->rtrack_gen = msg->track_gen;
    p->sub = sub;
    /* Capture the owner track NOW, while the pump-sub is live — the staged
     * teardown's stage 2 needs it after stage 1 has retired the sub. A
     * subscribe that rejected synchronously (no publisher) creates no sub:
     * the REJECT_SUB intent is already queued with cookie = demand_id and
     * the router turns it into the DONE reply. */
    p->has_otrack = (moqr_core_sub_track(m->core, sub, &p->otrack,
                                         &p->otrack_gen) == MOQR_OK);
    if (!p->has_otrack) {
        p->pump_sub_retired = true;   /* the sub never lived */
    } else {
        /* Admission may join an already ACTIVE/WARM track after its last
         * observer notification. This seeds only latest, never ACK A. */
        if (moqr_bind_meta_latest(s->shards[shard].bind, p->otrack,
                                  &p->latest_metadata) == MOQR_OK) {
            p->metadata_pending = p->latest_metadata.revision != 0;
        }
    }
    return true;
}


/* Inbound demand drain (joins phase 1): each source channel's head is
 * processed until it can't be — pop-only-when-durable throughout.
 * - DEMAND (owner role): with admission off, answer DONE(NOT_SUPPORTED,
 *   pre_ack); with admission on, admit via owner_admit_demand. Either way
 *   the message pops only once the outcome is durable (the reply pushed, or
 *   the pump-sub entry recorded).
 * - UNDEMAND: mark the admitted demand for the staged teardown (the entry is
 *   the durable record); with no matching entry it is moot (never admitted,
 *   or already terminally resolved).
 * - ACK (requester role): first re-probe local liveness — an ACK for a
 *   demand whose last subscriber already left must not activate a zombie —
 *   then install its exact metadata and resolve via upstream_ok with
 *   upstream_cookie = demand_id (the correlation key the ACTIVE linger path
 *   later echoes back).
 * - METADATA: apply a newer, source-qualified complete set or UNAVAILABLE
 *   before the fill/data behind it, without generating another wire ACK.
 * - DONE (requester role): resolve by the entry's own state — pre-ACK
 *   through upstream_error, post-ACK through source_done — with the OWNER's
 *   code verbatim; OK records the resolution (and counts a pre-ACK refusal);
 *   WOULD_BLOCK leaves the DONE at the head; STALE/WRONG_STATE is moot (the
 *   probe, the linger path, or a re-target resolved the track first). */
void
shard_demand_inbound(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                     uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    for (uint16_t src = 0; src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_demand_msg_t *msg;
        while ((msg = dch_head(s, src, shard)) != NULL) {
            if ((msg->kind >= D_MSG_DONE && msg->kind <= D_MSG_SG_SEAL) ||
                msg->kind == D_MSG_METADATA) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg)) {
                    dch_pop(s, src, shard);
                    continue; /* a reply for another retained/source identity */
                }
                if (!pend_source_current(m, d)) {
                    if ((d->state == D_ST_ACKED || d->state == D_ST_TERMINATING) &&
                        moqr_core_internal_source_idle(m->core, d->track,
                                                        d->track_gen)) {
                        if (d->state == D_ST_ACKED) {
                            d->state = D_ST_TERMINATING;
                            d->term_code = 0;
                            d->term_overrun = false;
                            d->term_quiet = true;
                        }
                        break; /* quiet old-log cleanup precedes later replies */
                    }
                    /* Source changes run before this FIFO drain. Old queued
                     * objects, chunks, resets, metadata and terminals must
                     * neither write nor scrub the replacement's log. Keep
                     * the old owner's cancel obligation, without a local
                     * source terminal that could end the replacement. */
                    d->state = D_ST_UNDEMANDING;
                    if (msg->kind == D_MSG_DONE) pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
            }
            if (msg->kind == D_MSG_DEMAND) {
                bool durable =
                    s->admit_remote
                        ? owner_admit_demand(s, shard, src, msg,
                                             pushed_dst_mask)
                        : demand_reply_done(s, shard, src, msg, true,
                                            R_ERR_NOT_SUPPORTED,
                                            pushed_dst_mask);
                if (!durable) {
                    break;   /* DEMAND stays durable at the head */
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_UNDEMAND) {
                /* Entry key is {src, demand_id}: ids from different
                 * requesters collide, the channel they arrive on does not. */
                for (uint32_t i = 0; i < m->psub_cap; i++) {
                    if (m->psub[i].used && m->psub[i].src == src &&
                        m->psub[i].demand_id == msg->demand_id &&
                        moqr_track_eq(m->psub[i].rtrack, msg->track) &&
                        m->psub[i].rtrack_gen == msg->track_gen) {
                        m->psub[i].undemanding = true;
                        break;
                    }
                }
                dch_pop(s, src, shard);   /* entry (or moot) is durable */
            } else if (msg->kind == D_MSG_ACK) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg) ||
                    d->state != D_ST_SENT) {
                    dch_pop(s, src, shard);   /* cancel crossed in flight */
                    continue;
                }
                moqr_result_t pr = moqr_core_upstream_cancel(
                    m->core, d->track, d->track_gen, now_us);
                if (pr == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* ACK stays durable at the head */
                }
                if (pr == MOQR_OK || pr == MOQR_ERR_STALE_HANDLE) {
                    /* Locally dead (or identity moved on): the owner now
                     * holds a live pump-sub that must be torn down. */
                    d->state = D_ST_UNDEMANDING;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (pr != MOQR_ERR_WRONG_STATE) {
                    m->oom = true;   /* unexpected; never silently converge */
                    dch_pop(s, src, shard);
                    continue;
                }
                if (!d->metadata_bound) {
                    moqr_result_t mr = demand_metadata_install(m, d, msg,
                                                               now_us);
                    if (mr != MOQR_OK) {
                        /* A stale local attempt must not install on its
                         * replacement. Other non-publishing failures become
                         * a request-local refusal, never metadata fail-stop. */
                        if (mr != MOQR_ERR_STALE_HANDLE &&
                            mr != MOQR_ERR_WRONG_STATE &&
                            moqr_core_upstream_error(
                                m->core, d->track, d->track_gen,
                                R_ERR_INTERNAL, now_us) == MOQR_ERR_WOULD_BLOCK) {
                            break;
                        }
                        d->state = D_ST_UNDEMANDING;
                        dch_pop(s, src, shard);
                        continue;
                    }
                }
                moqr_result_t rc = moqr_core_upstream_ok(
                    m->core, d->track, d->track_gen, msg->demand_id,
                    msg->has_largest, msg->largest_group,
                    msg->largest_object);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* ACK stays durable at the head */
                }
                if (rc == MOQR_OK) {
                    d->state = D_ST_ACKED;
                    d->ack_has_largest = msg->has_largest;
                    d->ack_largest_group = msg->largest_group;
                    d->ack_largest_object = msg->largest_object;
                } else {
                    m->oom = true;
                    d->state = D_ST_UNDEMANDING;
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_METADATA) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg) ||
                    d->state != D_ST_ACKED || !d->metadata_bound ||
                    !moqr_track_eq(d->metadata_track, msg->metadata.track) ||
                    d->metadata_track_gen != msg->metadata.track_gen ||
                    msg->metadata.revision <= d->metadata_revision) {
                    dch_pop(s, src, shard);   /* old/moot demand or revision */
                    continue;
                }
                moqr_result_t mr = demand_metadata_install(m, d, msg, now_us);
                if (mr != MOQR_OK && d->state == D_ST_ACKED) {
                    /* A moved identity still retires partial OPEN records.
                     * replace's drain may already have staged a terminal;
                     * never overwrite that outcome. Copy refusal already
                     * published UNAVAILABLE and returns OK above. */
                    bool moot = mr == MOQR_ERR_STALE_HANDLE ||
                                mr == MOQR_ERR_WRONG_STATE;
                    d->state = D_ST_TERMINATING;
                    d->term_code = moot ? 0 : R_ERR_INTERNAL;
                    d->term_overrun = false;
                    d->term_quiet = moot;
                }
                dch_pop(s, src, shard);
                if (d->state == D_ST_TERMINATING) {
                    break;   /* phase 5 settles teardown before later results */
                }
            } else if (msg->kind == D_MSG_DONE) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg)) {
                    dch_pop(s, src, shard);   /* cancel completed first */
                    continue;
                }
                if (d->state == D_ST_UNDEMANDING) {
                    /* The owner's terminal supersedes the pending cancel
                     * notice: its per-demand state is already gone, so the
                     * UNDEMAND is no longer owed and the local track was
                     * already resolved when the entry went UNDEMANDING. */
                    pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    /* A staged local terminal owns this demand's teardown;
                     * the DONE waits behind it (its trigger is the retained
                     * head, so this arm is normally unreachable — kept as a
                     * guard, never an erase). */
                    break;
                }
                if (d->state == D_ST_ACKED) {
                    /* Post-ACK DONE is a full requester terminal: abandon
                     * any still-open objects with the DONE's code before
                     * source_done, one retryable stage at a time. */
                    if (!pend_terminal_step(s, m, d, msg->pd,
                                            shard_local_reset(
                                                MOQR_RESET_CANCELLED),
                                            shard_pd_forwarded(msg->pd),
                                            now_us)) {
                        break;   /* stage held: the DONE stays durable */
                    }
                    m->remote_demand_resolved++;
                    m->last_done_code = msg->error_code;
                    m->last_done_pre_ack = false;
                    pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
                moqr_result_t rc = moqr_core_upstream_error(
                    m->core, d->track, d->track_gen, msg->error_code,
                    now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* intent ring full: DONE stays durable */
                }
                if (rc == MOQR_OK) {
                    m->remote_demand_refused++;
                    m->remote_demand_resolved++;
                    m->last_done_code = msg->error_code;
                    /* Pre-ACK refusal is a REQUEST_ERROR: no terminal at all. */
                    m->last_done_pd = moqr_pd_desc_none();
                    m->last_done_pre_ack = true;
                } else if (rc != MOQR_ERR_STALE_HANDLE &&
                           rc != MOQR_ERR_WRONG_STATE) {
                    m->oom = true;
                }
                pend_retire(m, d);
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_OBJ) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot: pop releases clones */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    /* The trigger (this head, or one before it) stays until
                     * the staged terminal commits — popping would let later
                     * messages jump the uncommitted teardown. */
                    break;
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    /* Data for a demand that never ACKed (or from the wrong
                     * channel) is an invariant violation — the owner gates
                     * extraction on the ACK being in the same FIFO ahead of
                     * it. Consume safely (clones released at pop) and
                     * fail-stop. */
                    m->oom = true;
                    dch_pop(s, src, shard);
                    continue;
                }
                moqr_log_append_desc_t desc;
                moqr_log_append_desc_init(&desc);
                desc.group_id = msg->group_id;
                desc.subgroup_id = msg->subgroup_id;
                desc.object_id = msg->object_id;
                desc.publisher_priority = msg->prio;
                desc.status = (moqr_obj_status_t)msg->status;
                desc.datagram_pref = msg->datagram_pref;
                desc.end_of_group = msg->end_of_group;
                desc.first_object = msg->first_object;
                desc.payload = msg->payload;
                desc.properties = msg->props;
                desc.properties_codec = msg->props_codec;
                desc.subgroup_properties = msg->subgroup_props;
                desc.now_us = now_us;
                moqr_result_t rc = moqr_core_ingest(m->core, d->track, &desc);
                if (rc == MOQR_OK) {
                    /* Ownership transferred to the log: null the refs so the
                     * pop releases nothing twice. */
                    msg->payload = NULL;
                    msg->props = NULL;
                    pend_note_feed(d, msg->group_id);
                    m->inbound_data_applied = true;
                    dch_pop(s, src, shard);
                } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* head durable, refs intact: retry next round */
                } else if (rc == MOQR_ERR_NOMEM) {
                    m->oom = true;   /* fail-stop; head stays durable */
                    break;
                } else if (rc == MOQR_ERR_CAPACITY) {
                    /* Local capacity edge: terminate THIS demand (INTERNAL
                     * 0x0) through the staged path — the head stays durable
                     * until the terminal commits, after which it pops as
                     * moot. Loss-visible, never a hidden drop. */
                    d->state = D_ST_TERMINATING;
                    d->term_code = R_ERR_INTERNAL;
                    break;
                } else if (rc == MOQR_ERR_TOO_OLD) {
                    /* Below the requester's OWN retention horizon — the same
                     * outcome local eviction produces; counted, never a
                     * hidden drop. */
                    m->remote_data_rejected++;
                    dch_pop(s, src, shard);
                } else {
                    /* Ordering INVAL / desync: consume safely, fail-stop. */
                    m->oom = true;
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_OBJ_OPEN ||
                       msg->kind == D_MSG_OBJ_CHUNK ||
                       msg->kind == D_MSG_OBJ_END) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot: pop releases clones */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    break;   /* hold everything behind the staged terminal */
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    m->oom = true;   /* data before ACK: invariant broken */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_psg_t *row = pend_popen_row(m, d);
                if (row == NULL) {
                    /* No progress row left: loss-visible demand terminal. */
                    d->state = D_ST_TERMINATING;
                    d->term_code = R_ERR_INTERNAL;
                    d->term_overrun = true;
                    break;   /* head durable until the terminal commits */
                }
                if (msg->kind == D_MSG_OBJ_OPEN) {
                    r_psg_t *slot = psg_claim(row, s->sg_slots,
                                              msg->group_id,
                                              msg->subgroup_id);
                    if (slot == NULL) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = true;
                        break;
                    }
                    if (slot->object_open) {
                        m->oom = true;   /* re-open without END: desync */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_log_append_desc_t desc;
                    moqr_log_append_desc_init(&desc);
                    desc.group_id = msg->group_id;
                    desc.subgroup_id = msg->subgroup_id;
                    desc.object_id = msg->object_id;
                    desc.publisher_priority = msg->prio;
                    desc.end_of_group = msg->end_of_group;
                    desc.first_object = msg->first_object;
                    desc.obj_state = MOQR_OBJ_OPEN;
                    desc.declared_len = msg->declared_len;
                    desc.properties = msg->props;
                    desc.properties_codec = msg->props_codec;
                    desc.subgroup_properties = msg->subgroup_props;
                    desc.now_us = now_us;
                    moqr_result_t rc =
                        moqr_core_ingest(m->core, d->track, &desc);
                    if (rc == MOQR_OK) {
                        msg->props = NULL;   /* transferred to the log */
                        pend_note_feed(d, msg->group_id);
                        slot->object_open = true;
                        slot->object_id = msg->object_id;
                        slot->next_chunk = 0;
                        m->inbound_data_applied = true;
                        dch_pop(s, src, shard);
                    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;   /* head durable, refs intact */
                    } else if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;   /* head durable; fail-stop */
                    } else if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    } else if (rc == MOQR_ERR_TOO_OLD) {
                        /* Below the local horizon: no record, but the slot
                         * still tracks the object so its chunk/END sequence
                         * consumes as the same loss instead of desync. */
                        m->remote_data_rejected++;
                        slot->object_open = true;
                        slot->discarding = true;
                        slot->object_id = msg->object_id;
                        slot->next_chunk = 0;
                        dch_pop(s, src, shard);
                    } else {
                        m->oom = true;   /* ordering INVAL / desync */
                        dch_pop(s, src, shard);
                    }
                } else if (msg->kind == D_MSG_OBJ_CHUNK) {
                    r_psg_t *slot = psg_find(row, s->sg_slots,
                                             msg->group_id,
                                             msg->subgroup_id);
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* chunk for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (slot->discarding) {
                        m->remote_data_rejected++;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc = moqr_core_append_chunk(
                        m->core, d->track, msg->group_id, msg->subgroup_id,
                        msg->object_id, msg->payload);
                    if (rc == MOQR_OK) {
                        /* append_chunk increfs — the channel keeps its own
                         * clone ref and releases it at pop as usual. */
                        slot->next_chunk++;
                        m->inbound_data_applied = true;
                        dch_pop(s, src, shard);
                    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;
                    } else if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;
                    } else if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    } else if (rc == MOQR_ERR_TOO_OLD) {
                        /* The record fell below the local horizon; every
                         * later message for it consumes the same way. */
                        m->remote_data_rejected++;
                        dch_pop(s, src, shard);
                    } else {
                        m->oom = true;
                        dch_pop(s, src, shard);
                    }
                } else {   /* D_MSG_OBJ_END */
                    r_psg_t *slot = psg_find(row, s->sg_slots,
                                             msg->group_id,
                                             msg->subgroup_id);
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* end for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (slot->discarding) {
                        m->remote_data_rejected++;
                        slot->object_open = false;
                        slot->discarding = false;
                        slot->object_id = 0;
                        slot->next_chunk = 0;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc = moqr_core_complete_record(
                        m->core, d->track, msg->group_id, msg->subgroup_id,
                        msg->object_id);
                    if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;
                    }
                    if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;
                    }
                    if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    }
                    if (rc == MOQR_ERR_TOO_OLD) {
                        m->remote_data_rejected++;
                    } else if (rc != MOQR_OK) {
                        m->oom = true;
                    } else {
                        m->inbound_data_applied = true;
                    }
                    /* Only the OBJECT fields reset — the subgroup slot
                     * lives on for this (group, subgroup)'s next object. */
                    slot->object_open = false;
                    slot->discarding = false;
                    slot->object_id = 0;
                    slot->next_chunk = 0;
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_OBJ_RESET ||
                       msg->kind == D_MSG_GRP_RESET ||
                       msg->kind == D_MSG_GRP_EVICT ||
                       msg->kind == D_MSG_SG_SEAL) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    break;   /* hold everything behind the staged terminal */
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    m->oom = true;   /* terminal before ACK: invariant broken */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_psg_t *row =
                    (d->popen_row != UINT32_MAX)
                        ? &m->popen[(size_t)d->popen_row * s->sg_slots]
                        : NULL;
                if (msg->kind == D_MSG_SG_SEAL) {
                    /* The durable subgroup FIN. FIFO puts every OBJ_END
                     * before it, so an open (non-discarding) slot here is a
                     * desync, never a race. */
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               msg->group_id,
                                               msg->subgroup_id)
                                    : NULL;
                    if (slot != NULL && slot->object_open &&
                        !slot->discarding) {
                        m->oom = true;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc =
                        msg->seal_reset
                            ? moqr_core_reset_subgroup(
                                  m->core, d->track, msg->group_id,
                                  msg->subgroup_id, msg->reset)
                            : moqr_core_seal_subgroup(
                                  m->core, d->track, msg->group_id,
                                  msg->subgroup_id);
                    if (rc != MOQR_OK && rc != MOQR_DONE &&
                        rc != MOQR_ERR_STALE_HANDLE) {
                        m->oom = true;   /* INVAL contradiction: fail closed */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (rc == MOQR_OK) {
                        /* OK (not DONE/STALE) is the one outcome that marks
                         * local readiness — the seal notice is deliverable. */
                        m->inbound_data_applied = true;
                    }
#ifdef MOQR_BIND_TESTING
                    if (rc == MOQR_OK || rc == MOQR_DONE) {
                        uint32_t si = (uint32_t)(m->seal_seq %
                                                 (sizeof(m->seal_log) /
                                                  sizeof(m->seal_log[0])));
                        m->seal_log[si].seq = m->seal_seq;
                        m->seal_log[si].src = src;
                        m->seal_log[si].demand_id = msg->demand_id;
                        m->seal_log[si].group_id = msg->group_id;
                        m->seal_log[si].subgroup_id = msg->subgroup_id;
                        m->seal_seq++;
                    }
#endif
                    if (slot != NULL) {
                        memset(slot, 0, sizeof(*slot));
                    }
                    dch_pop(s, src, shard);
                } else if (msg->kind == D_MSG_OBJ_RESET) {
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               msg->group_id,
                                               msg->subgroup_id)
                                    : NULL;
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* reset for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (!slot->discarding) {
                        moqr_result_t rc = moqr_core_abandon_record(
                            m->core, d->track, msg->group_id,
                            msg->subgroup_id, msg->object_id,
                            msg->reset);
                        if (rc != MOQR_OK && rc != MOQR_ERR_TOO_OLD &&
                            rc != MOQR_ERR_STALE_HANDLE) {
                            m->oom = true;
                            dch_pop(s, src, shard);
                            continue;
                        }
                        if (rc == MOQR_OK) {
                            m->inbound_data_applied = true;
                        }
                    }
                    /* Reset terminates the subgroup: free the whole slot. */
                    memset(slot, 0, sizeof(*slot));
                    dch_pop(s, src, shard);
                } else if (msg->kind == D_MSG_GRP_RESET) {
                    /* The group was evicted at the owner while begun. Like
                     * the local bind's evict-reset, this stage touches ONLY
                     * begun/open subgroups — completed-but-unsealed slots
                     * survive so the GRP_EVICT behind it can still find and
                     * reset those streams. Each cleared slot is the retry
                     * cursor — a fail-stop retry never redoes one. */
                    bool fault = false;
                    if (row != NULL) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (!row[sl].used ||
                                row[sl].group != msg->group_id ||
                                !row[sl].object_open) {
                                continue;
                            }
                            if (!row[sl].discarding) {
                                moqr_result_t rc = moqr_core_abandon_record(
                                    m->core, d->track, row[sl].group,
                                    row[sl].subgroup, row[sl].object_id,
                                    msg->reset);
                                if (rc != MOQR_OK &&
                                    rc != MOQR_ERR_TOO_OLD &&
                                    rc != MOQR_ERR_STALE_HANDLE) {
                                    fault = true;
                                    break;
                                }
                                if (rc == MOQR_OK) {
                                    m->inbound_data_applied = true;
                                }
                            }
                            memset(&row[sl], 0, sizeof(row[sl]));
                        }
                    }
                    /* The slot sweep covers every partial that was applied
                     * through a tracked slot — normally all of them. The
                     * group is dead at its source, so belt-and-suspenders:
                     * abandon any OPEN tail the walk could not see, so a
                     * dead group never retains an open record. */
                    {
                        moqr_result_t gr = moqr_core_abandon_group_open(
                            m->core, d->track, msg->group_id,
                            msg->reset);
                        if (gr != MOQR_OK && gr != MOQR_DONE &&
                            gr != MOQR_ERR_STALE_HANDLE) {
                            fault = true;
                        }
                    }
                    if (fault) {
                        m->oom = true;
                    }
                    dch_pop(s, src, shard);
                } else {   /* D_MSG_GRP_EVICT: group_id = the watermark */
                    /* Everything below the owner's oldest retained group is
                     * gone there: what this destination holds of it stays
                     * servable, but nothing more of it will cross. A slot
                     * still here never got its SG_SEAL, and the owner may
                     * have evicted the subgroup's remaining objects too:
                     * its completeness is unknown, so its still-open object
                     * is abandoned and the subgroup is reset, never sealed
                     * (a FIN says every object was delivered; same rule as
                     * the bind's own watermark close). Each slot is settled
                     * once, then freed. */
                    bool fault = false;
                    if (row != NULL) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (!row[sl].used ||
                                row[sl].group >= msg->group_id) {
                                continue;
                            }
                            moqr_reset_desc_t evict_desc;
                            if (moqr_reset_desc_internal(
                                    MOQR_RESET_CODE_EVICTED, &evict_desc) !=
                                MOQR_OK) {
                                evict_desc = moqr_reset_desc_none();
                            }
                            if (row[sl].object_open &&
                                !row[sl].discarding) {
                                moqr_result_t rc = moqr_core_abandon_record(
                                    m->core, d->track, row[sl].group,
                                    row[sl].subgroup, row[sl].object_id,
                                    evict_desc);
                                if (rc != MOQR_OK &&
                                    rc != MOQR_ERR_TOO_OLD &&
                                    rc != MOQR_ERR_STALE_HANDLE) {
                                    fault = true;
                                    break;
                                }
                                if (rc == MOQR_OK) {
                                    m->inbound_data_applied = true;
                                }
                            }
                            moqr_result_t sr = moqr_core_reset_subgroup(
                                m->core, d->track, row[sl].group,
                                row[sl].subgroup, evict_desc);
                            if (sr != MOQR_OK && sr != MOQR_DONE &&
                                sr != MOQR_ERR_STALE_HANDLE) {
                                fault = true;
                                break;
                            }
                            if (sr == MOQR_OK) {
                                m->inbound_data_applied = true;
                            }
                            memset(&row[sl], 0, sizeof(row[sl]));
                        }
                    }
                    /* The slot sweep above can only reach subgroups this
                     * destination streamed through a slot; whole-object
                     * subgroups and ones a GRP_RESET already settled have
                     * none. Recording the watermark on the local journal
                     * resets every retained subgroup below it that had not
                     * ended, the same way, and turns "not yet" into "never
                     * will" for the absent range — without this, a subgroup
                     * whose seal died with the owner's eviction stays open
                     * downstream until this shard evicts the group too. */
                    {
                        moqr_result_t wr = moqr_core_note_track_evicted_below(
                            m->core, d->track, msg->group_id);
                        if (wr != MOQR_OK && wr != MOQR_ERR_STALE_HANDLE) {
                            fault = true;
                        }
                        if (wr == MOQR_OK) {
                            m->inbound_data_applied = true;
                        }
                    }
                    if (fault) {
                        m->oom = true;
                    }
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_FILL_REQ) {
                if (!owner_fill_accept(s, shard, src, msg, pushed_dst_mask,
                                       now_us)) {
                    break;   /* the REQ stays durable at the head */
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_FILL_MORE ||
                       msg->kind == D_MSG_FILL_CANCEL) {
                /* No entry: the retrieval already ended here (moot). The
                 * data phase re-reads every retrieval, so a grant takes
                 * effect in this same step. */
                r_oret_t *o = oret_find(m, src, msg->fill_id);
                if (o != NULL && o->demand_id != msg->demand_id) o = NULL;
                if (o != NULL && msg->kind == D_MSG_FILL_CANCEL) {
                    oret_free(m, o);
                } else if (o != NULL) {
                    o->credit += msg->fill_credit;
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_FILL_OPEN ||
                       msg->kind == D_MSG_FILL_ITEM ||
                       msg->kind == D_MSG_FILL_END) {
                r_fret_t *f = fret_find(m, msg->fill_id);
                if (f == NULL || f->origin != src ||
                    f->demand_id != msg->demand_id || f->closed ||
                    f->abandoned || f->ended) {
                    /* Late or no longer wanted: ignored (pop releases). */
                    dch_pop(s, src, shard);
                    continue;
                }
                if (msg->kind == D_MSG_FILL_OPEN) {
                    if (f->standalone != msg->fetch_standalone) {
                        m->oom = true;   /* answers a different request kind */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (f->standalone) {
                        memset(&f->plan, 0, sizeof(f->plan));
                        f->plan.wire.admit = msg->fetch_admit;
                        if (msg->fetch_admit == MOQR_FETCH_ACCEPT) {
                            f->plan.wire.end_of_track = msg->fetch_eot;
                            f->plan.wire.end_group = msg->fill_end_group;
                            f->plan.wire.end_object = msg->fetch_end_raw_object;
                            f->plan.start = (moqr_retrieve_location_t){
                                msg->group_id, msg->object_id };
                            f->plan.end = (moqr_retrieve_location_t){
                                msg->fill_end_group, msg->fill_end_object };
                            f->plan.meta_state = msg->metadata.state;
                            f->plan.meta_wire_version = msg->metadata.wire_version;
                            if (msg->props != NULL) {
                                /* Moved out of the message before its pop. */
                                f->meta_props = msg->props;
                                msg->props = NULL;
                                f->plan.meta_bytes = (moq_bytes_t){
                                    moq_rcbuf_data(f->meta_props),
                                    moq_rcbuf_len(f->meta_props) };
                            }
                            f->start_group = msg->group_id;
                            f->start_object = msg->object_id;
                            f->end_group = msg->fill_end_group;
                            f->end_object = msg->fill_end_object;
                        } else {
                            f->plan.wire.error_code = msg->error_code;
                            f->plan.capacity = msg->fetch_capacity;
                            f->ended = true;
                            f->more_owed = false;
                        }
                    }
                    f->begun = true;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (msg->kind == D_MSG_FILL_END) {
                    f->ended = true;
                    f->terminal = msg->fill_result;
                    f->more_owed = false;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (f->count >= R_FILL_WINDOW || f->granted == 0) {
                    m->oom = true;   /* beyond the granted window: desync */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_fitem_t *it =
                    &f->win[(f->head + f->count) % R_FILL_WINDOW];
                memset(it, 0, sizeof(*it));
                it->result = msg->fill_result;
                it->group_id = msg->group_id;
                it->subgroup_id = msg->subgroup_id;
                it->object_id = msg->object_id;
                it->prio = msg->prio;
                it->status = msg->status;
                it->datagram_pref = msg->datagram_pref;
                it->end_of_group = msg->end_of_group;
                it->declared_len = msg->declared_len;
                it->payload = msg->payload;
                it->props = msg->props;
                it->props_codec = msg->props_codec;
                msg->payload = NULL;   /* moved into the window */
                msg->props = NULL;
                it->charged = dch_hold_head(s, src, shard);
                f->count++;
                f->granted--;
                if (it->result.kind == MOQR_RETRIEVE_ERROR) {
                    f->ended = true;
                    f->terminal = it->result;
                    f->more_owed = false;
                }
            } else {
                m->oom = true;   /* unknown kind: never silently converge */
                dch_pop(s, src, shard);
            }
        }
    }
}
