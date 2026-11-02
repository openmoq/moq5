#include "shards_internal.h"
#include "data_internal.h"
#include "demand_internal.h"
#include "fill_internal.h"

r_psg_t *
psg_find(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg)
{
    for (uint32_t i = 0; i < slots; i++) {
        if (row[i].used && row[i].group == g && row[i].subgroup == sg) {
            return &row[i];
        }
    }
    return NULL;
}


/* Find-or-claim the (g, sg) slot in a demand's progress row; NULL when the
 * row is exhausted (a demand terminal, never an untracked stream). */
r_psg_t *
psg_claim(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg)
{
    r_psg_t *e = psg_find(row, slots, g, sg);
    if (e != NULL) {
        return e;
    }
    for (uint32_t i = 0; i < slots; i++) {
        if (!row[i].used) {
            memset(&row[i], 0, sizeof(row[i]));
            row[i].used = true;
            row[i].group = g;
            row[i].subgroup = sg;
            return &row[i];
        }
    }
    return NULL;
}


void
psg_row_clear(r_psg_t *row, uint32_t slots)
{
    memset(row, 0, (size_t)slots * sizeof(row[0]));
}


bool
dp_admit(const dp_budget_t *b, uint64_t lb)
{
    if (b->msgs_left == 0) {
        return false;
    }
    if (b->pushed_any && lb > b->bytes_left) {
        return false;
    }
    return true;
}


void
dp_spend(dp_budget_t *b, uint64_t lb)
{
    if (!b->pushed_any && lb > b->bytes_left) {
        b->turn_over = true;
    }
    b->pushed_any = true;
    b->msgs_left--;
    b->bytes_left = lb >= b->bytes_left ? 0 : b->bytes_left - lb;
}


/* Free an admitted-demand entry AND its subgroup-progress row (the row is
 * indexed by the psub slot, so a recycled slot must never inherit stale
 * cursors). */
void
psub_free(r_mgr *m, r_psub *p)
{
    oret_demand_gone(m, p->src, p->demand_id);
    psub_metadata_release(m, p);
    uint32_t idx = (uint32_t)(p - m->psub);
    psg_row_clear(&m->psg[(size_t)idx * m->s->sg_slots], m->s->sg_slots);
    memset(p, 0, sizeof(*p));
}


/* The ONE turn-classification point: every turn-ending path funnels here,
 * so the four outcome counters partition pump_turns by construction.
 * Precedence is pre-registered (never data-dependent): message budget
 * beats byte budget beats blocked beats drained — a one-message budget
 * spent on an oversize-first delivery classifies as MSG_BUDGET even though
 * it also crossed the byte budget. "Blocked" reads data_pending_mask AFTER
 * this turn's updates: budget remained but at least one source still has
 * eligible data behind a channel/byte-cap/arbiter/un-ACKed refusal. */
static void
dp_turn_classify(r_mgr *m, const dp_budget_t *b)
{
    if (b->msgs_left == 0) {
        m->turns_msg_budget++;
    } else if (b->turn_over) {
        m->turns_byte_budget++;
    } else if (m->data_pending_mask != 0) {
        m->turns_blocked++;
    } else {
        m->turns_drained++;
    }
    if (b->pushed_any) {
        m->turns_with_messages++;
    }
}


/* End the data phase after the soft-first exception (or any mid-object turn
 * boundary): the source stays conservatively pending and the turn/rotation
 * bookkeeping happens exactly once. */
static void
dp_turn_end(r_mgr *m, uint16_t src, const dp_budget_t *b)
{
    m->data_pending_mask |= 1ull << src;
    dp_turn_classify(m, b);
    m->pump_turns++;
    m->data_rr = (uint16_t)((m->data_rr + 1u) % m->s->shard_count);
}


/* Confirm a delivery outcome on an owner pseudo-binding. A refused
 * confirmation means the core's outstanding-delivery state is no longer
 * what this phase believes — latch fail-stop rather than treat it as a
 * successful hold, release, or advance. */
static bool
data_done(r_mgr *m, uint16_t src, moqr_delivery_outcome_t oc, uint64_t now_us)
{
    if (moqr_core_delivery_done(m->core, m->pb[src], oc, now_us) != MOQR_OK) {
        m->oom = true;
        return false;
    }
    return true;
}


/* Phase 6 — the data phase: pull admitted deliveries from the per-origin
 * pseudo-bindings and pump each over the same ordered channel that carried
 * its ACK — a whole/status/datagram record as one D_MSG_OBJ, a chunked or
 * live-edge record as OBJ_OPEN / OBJ_CHUNK... / OBJ_END with the per-demand
 * subgroup slot as the resume cursor. Discipline:
 * - Extraction requires ack_sent: the ACK is already durably enqueued in the
 *   SAME FIFO, so an object can never overtake it.
 * - Correlation is {src, demand_id} via the delivery's sub_cookie; a
 *   delivery this table cannot name is a fail-stop, never a guess.
 * - Clone first (independent ownership — never a borrowed pointer or a
 *   shared rcbuf across shards), then enqueue, and only then acknowledge
 *   DELIVERED (or STALLED past a live edge, retaining progress). A refused
 *   push frees the clones and reports WOULD_BLOCK (the delivery re-derives
 *   and the slot cursor resumes without duplication); clone OOM releases
 *   the outstanding delivery with STREAM_ERROR — retiring the pump-sub so
 *   nothing stays pinned — and latches fail-stop.
 * - Shapes still without vocabulary (abandoned-after-begin resets, seal and
 *   eviction notices) are HELD via WOULD_BLOCK — replayed verbatim,
 *   acknowledged by nothing — so nothing is lost or faked; that source lags
 *   until those shapes can cross.
 * - Turn budgets bound each phase: pump_turn_msgs messages and
 *   pump_turn_bytes logical bytes, except that the FIRST message of an
 *   otherwise-empty turn may exceed the byte budget (a legal record is never
 *   permanently unsendable) — spending that exception ENDS the turn
 *   immediately, so nothing (not even a zero-byte status object) rides the
 *   same turn behind it; no debt carries forward. Extraction round-robins
 *   across sources between turns. */
void
shard_data_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                 uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL || !s->admit_remote) {
        return;
    }
    dp_budget_t bud = { s->pump_turn_msgs, s->pump_turn_bytes, false,
                        false };
    bool attempted = false;
    uint16_t k = s->shard_count;
    /* Fill-retrieval answers go first: each is bounded by the credit its
     * requester granted (one window per round trip), so they cannot starve
     * live data, while live data behind a busy channel could starve them
     * indefinitely. */
    uint64_t fill_pending = 0;
    bool fill_turn_open = owner_fill_pass(s, shard, now_us, &bud,
                                          pushed_dst_mask, &attempted,
                                          &fill_pending);
    m->data_pending_mask |= fill_pending;
    if (!fill_turn_open) {
        dp_turn_classify(m, &bud);
        m->pump_turns++;
        m->data_rr = (uint16_t)((m->data_rr + 1u) % k);
        return;
    }
    for (uint16_t step = 0; step < k; step++) {
        uint16_t src = (uint16_t)((m->data_rr + step) % k);
        if (src == shard) {
            continue;
        }
        /* Only pull a pseudo-binding that has an admitted, acknowledged
         * demand — pb[src] carries no deliverable work otherwise, and a
         * not-yet-acked demand must wait for its ACK to enqueue first. */
        if (owner_metadata_pending(m, src)) {
            m->data_pending_mask &= ~(1ull << src);
            continue;
        }
        bool live = false, unacked = false;
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            const r_psub *p = &m->psub[i];
            if (!p->used || p->src != src || p->undemanding ||
                p->done_pending) {
                continue;
            }
            if (p->ack_sent) {
                live = true;
            } else {
                unacked = true;
            }
        }
        if (!live) {
            if (!unacked && ((fill_pending >> src) & 1u) == 0) {
                m->data_pending_mask &= ~(1ull << src);
            }
            continue;
        }
        attempted = true;
        bool ctrl_waiting = ctrl_work_pending(m, src);
        bool src_pending = false;
        for (;;) {
            if (bud.msgs_left == 0) {
                src_pending = true;   /* budget spent with work remaining */
                break;
            }
            moqr_delivery_t d;
            moqr_result_t rc =
                moqr_core_next_delivery(m->core, m->pb[src], now_us, &d);
            if (rc == MOQR_DONE) {
                break;   /* drained: nothing pending toward src */
            }
            if (rc != MOQR_OK) {
                m->oom = true;   /* unexpected; never silently converge */
                break;
            }
            r_psub *p = NULL;
            for (uint32_t i = 0; i < m->psub_cap; i++) {
                if (m->psub[i].used && m->psub[i].src == src &&
                    m->psub[i].demand_id == d.sub_cookie) {
                    p = &m->psub[i];
                    break;
                }
            }
            if (p == NULL) {
                /* A delivery this table cannot correlate: fail-stop. The
                 * WOULD_BLOCK keeps the delivery outstanding rather than
                 * asserting anything about it. */
                (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us);
                m->oom = true;
                return;
            }
            if (!p->ack_sent) {
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            if (d.notice != MOQR_DELIVERY_NOTICE_NONE || d.evicted_reset ||
                d.rec.obj_state == MOQR_OBJ_ABANDONED) {
                /* Terminal / notice shapes: one zero-byte message each,
                 * confirmed only after the push is durable — a notice is
                 * never acknowledged (and no owner slot is touched) while
                 * its channel message could still be lost. */
                r_psg_t *row =
                    m->psg != NULL
                        ? &m->psg[(size_t)(p - m->psub) * s->sg_slots]
                        : NULL;
                r_demand_msg_t tm;
                memset(&tm, 0, sizeof(tm));
                tm.demand_id = p->demand_id;
                tm.track = p->rtrack;
                tm.track_gen = p->rtrack_gen;
                moqr_delivery_outcome_t confirm = MOQR_DELIVERY_DELIVERED;
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
                    tm.kind = D_MSG_SG_SEAL;
                    tm.group_id = d.rec.group_id;
                    tm.subgroup_id = d.rec.subgroup_id;
                    tm.seal_reset = d.seal_reset;
                    tm.reset = d.seal_reset_desc;
                } else if (d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                    /* group_id carries the watermark: the oldest RETAINED
                     * group (everything below is gone). */
                    tm.kind = D_MSG_GRP_EVICT;
                    tm.group_id = d.oldest_group;
                } else if (d.evicted_reset) {
                    tm.kind = D_MSG_GRP_RESET;
                    tm.group_id = d.rec.group_id;
                    if (moqr_reset_desc_internal(
                            MOQR_RESET_CODE_EVICTED, &tm.reset) !=
                        MOQR_OK) {
                        tm.reset = moqr_reset_desc_none();
                    }
                    confirm = MOQR_DELIVERY_ABANDONED;
                } else {
                    /* Ordinary begun-abandoned object: exact progress
                     * identity required — this pump began it, so its slot
                     * must name it. */
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               d.rec.group_id,
                                               d.rec.subgroup_id)
                                    : NULL;
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != d.rec.object_id) {
                        (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                        now_us);
                        m->oom = true;   /* desync: never guess */
                        return;
                    }
                    tm.kind = D_MSG_OBJ_RESET;
                    tm.group_id = d.rec.group_id;
                    tm.subgroup_id = d.rec.subgroup_id;
                    tm.object_id = d.rec.object_id;
                    tm.reset = d.rec.reset;
                    confirm = MOQR_DELIVERY_ABANDONED;
                }
                if (!dp_admit(&bud, 0)) {
                    if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                   now_us)) {
                        return;
                    }
                    src_pending = true;
                    break;
                }
                if (!dch_push_arb(s, shard, src, &tm, true, ctrl_waiting)) {
                    if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                   now_us)) {
                        return;
                    }
                    src_pending = true;
                    break;
                }
                dp_spend(&bud, 0);
                m->pump_messages++;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << src;
                }
                if (!data_done(m, src, confirm, now_us)) {
                    return;   /* the message is durable; the core faulted */
                }
                /* Progress-slot cleanup, only after the acknowledged
                 * confirmation above. */
                if (row != NULL) {
                    if (tm.kind == D_MSG_SG_SEAL) {
                        r_psg_t *slot = psg_find(row, s->sg_slots,
                                                 tm.group_id,
                                                 tm.subgroup_id);
                        if (slot != NULL) {
                            if (slot->object_open) {
                                /* A seal surfaces only after this sub
                                 * consumed the list — an open object here
                                 * is a core/pump desync. */
                                m->oom = true;
                                return;
                            }
                            memset(slot, 0, sizeof(*slot));
                        }
                        /* Absence is valid: whole-object subgroups never
                         * claim a slot. */
                    } else if (tm.kind == D_MSG_GRP_EVICT) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (row[sl].used &&
                                row[sl].group < tm.group_id) {
                                memset(&row[sl], 0, sizeof(row[sl]));
                            }
                        }
                    } else if (tm.kind == D_MSG_GRP_RESET) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (row[sl].used &&
                                row[sl].group == tm.group_id) {
                                memset(&row[sl], 0, sizeof(row[sl]));
                            }
                        }
                    } else {   /* OBJ_RESET: reset terminates the subgroup */
                        r_psg_t *slot = psg_find(row, s->sg_slots,
                                                 tm.group_id,
                                                 tm.subgroup_id);
                        if (slot != NULL) {
                            memset(slot, 0, sizeof(*slot));
                        }
                    }
                }
                if (bud.turn_over) {
                    dp_turn_end(m, src, &bud);
                    return;
                }
                ctrl_waiting = ctrl_work_pending(m, src);
                continue;
            }
            if (d.rec.chunk_count > 0 || d.rec.obj_state == MOQR_OBJ_OPEN) {
                /* Chunked / live-edge record: OBJ_OPEN exactly once, each
                 * exposed chunk in order, OBJ_END on completion — the
                 * subgroup-progress slot is the resume cursor across every
                 * hold (channel-full, budgets, STALLED regrowth), so nothing
                 * duplicates and nothing skips. */
                r_psg_t *row =
                    &m->psg[(size_t)(p - m->psub) * s->sg_slots];
                r_psg_t *slot = psg_claim(row, s->sg_slots, d.rec.group_id,
                                          d.rec.subgroup_id);
                if (slot == NULL) {
                    /* Distinct live subgroups beyond the progress table: a
                     * loss-visible owner-side demand terminal. Stage 1
                     * releases the outstanding delivery and retires the
                     * pump-sub in the one confirmed core step; the cancel
                     * and DONE stages retry from the reply walk. */
                    if (!data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                   now_us)) {
                        return;   /* nothing claimed released */
                    }
                    p->pump_sub_retired = true;
                    p->terminating = true;
                    p->done_pending = true;
                    p->done_pre_ack = false;
                    p->done_code = R_ERR_INTERNAL;
                    p->done_pd = shard_local_done(MOQR_PD_INTERNAL_ERROR);
                    m->remote_demand_term_capacity++;
                    src_pending = true;   /* the DONE reply is owed */
                    break;
                }
                if (slot->object_open &&
                    slot->object_id != d.rec.object_id) {
                    /* A new object before the open one ended: desync —
                     * never guess. */
                    (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                    now_us);
                    m->oom = true;
                    return;
                }
                if (!slot->object_open) {
                    uint64_t plb =
                        (uint64_t)moq_rcbuf_len(d.rec.properties);
                    if (!dp_admit(&bud, plb)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    r_demand_msg_t om;
                    memset(&om, 0, sizeof(om));
                    om.kind = D_MSG_OBJ_OPEN;
                    om.demand_id = p->demand_id;
                    om.track = p->rtrack;
                    om.track_gen = p->rtrack_gen;
                    om.group_id = d.rec.group_id;
                    om.subgroup_id = d.rec.subgroup_id;
                    om.object_id = d.rec.object_id;
                    om.declared_len = d.rec.declared_len;
                    om.prio = d.rec.publisher_priority;
                    om.end_of_group = d.rec.end_of_group;
                    om.first_object = d.rec.first_object;
                    om.props_codec = d.rec.properties_codec;
                    om.subgroup_props = shard_delivery_subgroup_props(m, src);
                    if (d.rec.properties != NULL &&
                        moq_rcbuf_clone(&s->alloc, d.rec.properties,
                                        &om.props) != MOQ_OK) {
                        dmsg_release(&s->alloc, &om);
                        if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                      now_us)) {
                            psub_free(m, p);
                        }
                        m->oom = true;
                        return;
                    }
                    if (!dch_push_arb(s, shard, src, &om, true,
                                      ctrl_waiting)) {
                        dmsg_release(&s->alloc, &om);
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    dp_spend(&bud, plb);
                    m->pump_messages++;
                    m->pump_bytes += plb;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    slot->object_open = true;
                    slot->object_id = d.rec.object_id;
                    slot->next_chunk = 0;
                    if (bud.turn_over) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                    ctrl_waiting = ctrl_work_pending(m, src);
                }
                bool held = false;
                while (slot->next_chunk < d.rec.chunk_count) {
                    const moq_rcbuf_t *cb = NULL;
                    uint64_t clen = 0;
                    if (moqr_core_delivery_chunk(m->core, m->pb[src],
                                                 slot->next_chunk, &cb,
                                                 &clen) != MOQR_OK) {
                        (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                        now_us);
                        m->oom = true;   /* pinned chunk missing: desync */
                        return;
                    }
                    if (!dp_admit(&bud, clen)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        held = true;
                        break;
                    }
                    r_demand_msg_t cm;
                    memset(&cm, 0, sizeof(cm));
                    cm.kind = D_MSG_OBJ_CHUNK;
                    cm.demand_id = p->demand_id;
                    cm.track = p->rtrack;
                    cm.track_gen = p->rtrack_gen;
                    cm.group_id = d.rec.group_id;
                    cm.subgroup_id = d.rec.subgroup_id;
                    cm.object_id = d.rec.object_id;
                    if (moq_rcbuf_clone(&s->alloc, cb, &cm.payload) !=
                        MOQ_OK) {
                        dmsg_release(&s->alloc, &cm);
                        if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                      now_us)) {
                            psub_free(m, p);
                        }
                        m->oom = true;
                        return;
                    }
                    if (!dch_push_arb(s, shard, src, &cm, true,
                                      ctrl_waiting)) {
                        dmsg_release(&s->alloc, &cm);
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        held = true;
                        break;
                    }
                    dp_spend(&bud, clen);
                    m->pump_messages++;
                    m->pump_bytes += clen;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    slot->next_chunk++;
                    if (bud.turn_over) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                    ctrl_waiting = ctrl_work_pending(m, src);
                }
                if (held) {
                    break;
                }
                if (d.rec.obj_state == MOQR_OBJ_COMPLETE) {
                    if (!dp_admit(&bud, 0)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    r_demand_msg_t em;
                    memset(&em, 0, sizeof(em));
                    em.kind = D_MSG_OBJ_END;
                    em.demand_id = p->demand_id;
                    em.track = p->rtrack;
                    em.track_gen = p->rtrack_gen;
                    em.group_id = d.rec.group_id;
                    em.subgroup_id = d.rec.subgroup_id;
                    em.object_id = d.rec.object_id;
                    em.end_of_group = d.rec.end_of_group;
                    em.adv_subgroup_end = d.subgroup_end;
                    if (!dch_push_arb(s, shard, src, &em, true,
                                      ctrl_waiting)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    dp_spend(&bud, 0);
                    m->pump_messages++;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    if (!data_done(m, src, MOQR_DELIVERY_DELIVERED,
                                   now_us)) {
                        return;
                    }
                    /* Only the OBJECT fields reset: the subgroup slot lives
                     * on for the next object of this (group, subgroup). */
                    slot->object_open = false;
                    slot->object_id = 0;
                    slot->next_chunk = 0;
                    if (bud.turn_over) {
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                } else {
                    /* Live edge drained: release fairly WITHOUT advancing —
                     * the progress slot resumes at next_chunk when the
                     * record grows or completes. Nothing is pushable toward
                     * this record until then, so no pending bit. */
                    if (!data_done(m, src, MOQR_DELIVERY_STALLED, now_us)) {
                        return;
                    }
                }
                ctrl_waiting = ctrl_work_pending(m, src);
                continue;
            }
            uint64_t lb = (uint64_t)moq_rcbuf_len(d.rec.payload) +
                          (uint64_t)moq_rcbuf_len(d.rec.properties);
            if (!dp_admit(&bud, lb)) {
                /* Budget spent; only an otherwise-empty turn's FIRST
                 * message may exceed the byte budget. */
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_OBJ;
            msg.demand_id = p->demand_id;
            msg.track = p->rtrack;
            msg.track_gen = p->rtrack_gen;
            msg.group_id = d.rec.group_id;
            msg.subgroup_id = d.rec.subgroup_id;
            msg.object_id = d.rec.object_id;
            msg.prio = d.rec.publisher_priority;
            msg.status = (uint8_t)d.rec.status;
            msg.datagram_pref = d.rec.datagram_pref;
            msg.end_of_group = d.rec.end_of_group;
            msg.first_object = d.rec.first_object;
            msg.adv_subgroup_end = d.subgroup_end;
            msg.props_codec = d.rec.properties_codec;
            msg.subgroup_props = shard_delivery_subgroup_props(m, src);
            bool oom = false;
            if (d.rec.payload != NULL &&
                moq_rcbuf_clone(&s->alloc, d.rec.payload, &msg.payload) !=
                    MOQ_OK) {
                oom = true;
            }
            if (!oom && d.rec.properties != NULL &&
                moq_rcbuf_clone(&s->alloc, d.rec.properties, &msg.props) !=
                    MOQ_OK) {
                oom = true;
            }
            if (oom) {
                /* Allocation failure is NOT backpressure: release the
                 * outstanding delivery and its pins by retiring the pump-sub
                 * (the one core-sanctioned combined step), then fail-stop —
                 * the shard enters the failed state with nothing pinned. The
                 * entry is claimed released ONLY when the core confirmed it. */
                dmsg_release(&s->alloc, &msg);
                if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR, now_us)) {
                    psub_free(m, p);
                }
                m->oom = true;
                return;
            }
            if (!dch_push_arb(s, shard, src, &msg, true, ctrl_waiting)) {
                dmsg_release(&s->alloc, &msg);
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            if (!data_done(m, src, MOQR_DELIVERY_DELIVERED, now_us)) {
                return;
            }
            dp_spend(&bud, lb);
            m->pump_messages++;
            m->pump_bytes += lb;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << src;
            }
            if (bud.turn_over) {
                /* The soft-first exception is spent: the turn ends NOW —
                 * nothing, not even a zero-byte status object, rides the
                 * same turn behind it. Work may remain here, so stay
                 * conservatively pending for the next turn. */
                dp_turn_end(m, src, &bud);
                return;
            }
            ctrl_waiting = ctrl_work_pending(m, src);
        }
        if (src_pending || ((fill_pending >> src) & 1u) != 0) {
            m->data_pending_mask |= 1ull << src;
        } else {
            m->data_pending_mask &= ~(1ull << src);
        }
        if (bud.msgs_left == 0) {
            break;
        }
    }
    if (attempted) {
        dp_turn_classify(m, &bud);
        m->pump_turns++;
        m->data_rr = (uint16_t)((m->data_rr + 1u) % k);
    }
}
