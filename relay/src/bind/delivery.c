#include "bind_internal.h"
#include "delivery_internal.h"
#include "discovery_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "subscription_update_internal.h"
#include "testing_internal.h"

/* Whole 64-bit words for one scheduling bit per conn slot — widened before
 * the +63 round-up so the uint32_t maximum cannot wrap (mirrors the core's
 * ready_word_count; the two layers size independent pools). */
uint32_t
bind_dl_word_count(uint32_t max_conns)
{
    return (uint32_t)(((uint64_t)max_conns + 63u) >> 6);
}


void
bind_dl_set(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] |= 1ull << (slot & 63u);
}


void
bind_dl_clear(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] &= ~(1ull << (slot & 63u));
}


bool
bind_dl_test(const uint64_t *words, uint32_t slot)
{
    return (words[slot >> 6] & (1ull << (slot & 63u))) != 0;
}


/* Re-arm a connection's delivery readiness from a bind-local recovery edge
 * (a subgroup-slot release on a still-used, still-open connection). Spurious
 * re-arms cost one empty probe and dedup in the bitset; teardown paths CLEAR
 * scheduling state instead — a dead or reused slot must never re-arm itself. */
static void
bind_dl_rearm(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->used && !cn->closed) {
        bind_dl_set(b->dl_ready, (uint32_t)(cn - b->conns));
    }
}


/* Re-arm on a freed subgroup-pool slot ONLY when a delivery is actually waiting
 * on that pool (bind_sg_waiting, set by the last pass's B_DELIVER_BLOCKED_BIND_SG
 * and cleared by any other outcome). Ordinary seal-close / reset traffic, where
 * nothing is pool-blocked, re-arms nothing — so a fully-draining pass leaves the
 * ready set empty and earns no spurious self-wake. */
void
bind_dl_rearm_on_slot_release(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->bind_sg_waiting) {
        bind_dl_rearm(b, cn);
    }
}


void
bind_dl_clear_conn(moqr_bind_t *b, b_conn_t *cn)
{
    uint32_t slot = (uint32_t)(cn - b->conns);
    bind_dl_clear(b->dl_ready, slot);
    bind_dl_clear(b->dl_parked, slot);
    cn->park_reason = B_PARK_NONE;
    cn->park_cap = 0;
    cn->bind_sg_waiting = false;
}


/* Stream one COMPLETE chunked record downstream: begin_object once, then
 * write_object_data for each retained chunk in order, then end_object. Progress
 * (obj_begun / obj_id / next_chunk) lives on the sg slot so a WOULD_BLOCK at any
 * step HOLDS and the next pump resumes the SAME chunk — no duplicate, no skip
 * (each write_object_data is retry-safe: capacity is gated before any incref).
 * Returns MOQ_OK only when the whole object shipped (begin + all chunks + end);
 * MOQ_ERR_WOULD_BLOCK to hold; another error to fail the delivery. Chunks are
 * read from the core's pinned array — never from the log directly. */
/* Map a single session write result to a delivery outcome (whole-object,
 * status, and datagram paths, which each emit exactly one action). */
static moqr_delivery_outcome_t
oc_from_result(moq_result_t wrc)
{
    return wrc == MOQ_OK              ? MOQR_DELIVERY_DELIVERED
           : wrc == MOQ_ERR_WOULD_BLOCK ? MOQR_DELIVERY_WOULD_BLOCK
                                        : MOQR_DELIVERY_STREAM_ERROR;
}


/* A live delivery whose Object Properties cannot be written on this
 * connection's draft (bind_props_out refused with `why`): its subscription
 * alone ends, by the next delivery scan before anything else of it is
 * delivered -- with MALFORMED_TRACK when the block is malformed in its own
 * codec, else INTERNAL_ERROR (the done's registry has no status naming a
 * destination that cannot carry valid properties); the record, of which
 * nothing went downstream, is released. */
static moqr_delivery_outcome_t
bind_props_refused(moqr_bind_t *b, const moqr_delivery_t *d, moq_result_t why)
{
    (void)moqr_core_internal_sub_fail(
        b->core, d->sub,
        bind_local_pd(why == MOQ_ERR_PROTO ? MOQR_PD_MALFORMED_TRACK
                                           : MOQR_PD_INTERNAL_ERROR));
    return MOQR_DELIVERY_REFUSED_UNBEGUN;
}


moqr_delivery_outcome_t
bind_write_chunked(moqr_bind_t *b, b_conn_t *cn, const moqr_delivery_t *d,
                   b_sg_t *sg, uint64_t now_us)
{
    if (!sg->obj_begun || sg->obj_id != d->rec.object_id) {
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = d->rec.object_id;
        bc.payload_length = d->rec.declared_len;   /* known at open (OPEN too) */
        moq_rcbuf_t *props_own = NULL;
        moq_result_t prc = bind_props_out(b, cn, d->rec.properties,
                                          d->rec.properties_codec, &bc.properties,
                                          &props_own);
        if (prc != MOQ_OK)
            return bind_props_refused(b, d, prc);
        moq_result_t brc =
            moq_session_begin_object_ex(cn->session, sg->h, &bc, now_us);
        if (props_own != NULL) moq_rcbuf_decref(props_own);
        if (brc == MOQ_ERR_WOULD_BLOCK) {
            return MOQR_DELIVERY_WOULD_BLOCK;   /* subgroup stays OPEN, retry */
        }
        if (brc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->obj_begun = true;
        sg->obj_id = d->rec.object_id;
        sg->next_chunk = 0;
    }
    while (sg->next_chunk < d->rec.chunk_count) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_core_delivery_chunk(b->core, cn->binding, sg->next_chunk, &cb,
                                     &clen) != MOQR_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;   /* pinned chunk missing */
        }
        moq_result_t wrc = moq_session_write_object_data(
            cn->session, sg->h, (moq_rcbuf_t *)(void *)cb, now_us);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {
            /* Hold: checkpoint that the object is begun downstream and how many
             * chunks have shipped (sg->next_chunk, which is <= chunk_count and
             * may be 0 when begin_object succeeded but this first chunk blocked).
             * This keeps the core's begun watermark accurate so that, if a
             * Forward pause releases this partial and the group is then
             * abandoned/evicted, the required RESET still surfaces. SAME chunk
             * retries next pump. next_chunk is in range, so INVAL here is a
             * consistency break, not a normal hold. */
            if (moqr_core_delivery_note_emitted(b->core, cn->binding,
                                                sg->next_chunk, now_us) !=
                MOQR_OK) {
                return MOQR_DELIVERY_STREAM_ERROR;
            }
            return MOQR_DELIVERY_WOULD_BLOCK;
        }
        if (wrc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->next_chunk++;
    }
    if (d->rec.obj_state != MOQR_OBJ_COMPLETE) {
        /* OPEN live edge: every currently-exposed chunk shipped but the
         * object is not complete yet — HOLD here without ending it. The core
         * records the emitted watermark on STALLED and re-selects this head only
         * when it grows or flips terminal, so other subs are not HOL-blocked. */
        return MOQR_DELIVERY_STALLED;
    }
    moq_result_t erc = moq_session_end_object(cn->session, sg->h, now_us);
    if (erc == MOQ_ERR_WOULD_BLOCK) {
        if (moqr_core_delivery_note_emitted(b->core, cn->binding,
                                            sg->next_chunk, now_us) != MOQR_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        return MOQR_DELIVERY_WOULD_BLOCK;
    }
    if (erc != MOQ_OK) {
        return MOQR_DELIVERY_STREAM_ERROR;
    }
    sg->obj_begun = false;   /* object complete; the slot can begin the next */
    return MOQR_DELIVERY_DELIVERED;
}


/* Confirm a delivery outcome to the core. delivery_done's error vocabulary
 * (INVAL / STALE_HANDLE / WRONG_STATE) is all consistency failures — never
 * transient — so any non-OK confirmation fails the session closed and
 * detaches; a binding whose confirmations misfire must not stay live with
 * an unacknowledged delivery. Returns false when the connection was
 * detached (the caller returns B_DELIVER_CLOSED). */
bool
bind_confirm_delivery(moqr_bind_t *b, b_conn_t *cn,
                      moqr_delivery_outcome_t oc, uint64_t now_us)
{
    moqr_result_t crc;
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_confirms) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_confirms, 1) == 1) {
        /* Injected consistency failure: the core is NOT called, so the
         * outstanding delivery stays live — teardown must cope with it. */
        crc = MOQR_ERR_WRONG_STATE;
    } else
#endif
    {
        crc = moqr_core_delivery_done(b->core, cn->binding, oc, now_us);
    }
    if (crc == MOQR_OK) {
        return true;
    }
    b->session_errors++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "delivery confirm failed", now_us);
    conn_detach(b, cn, now_us);
    return false;
}


/* Bounded exact-key work is independent of Forward, live-edge growth and
 * ordinary record priority. It cannot consume a sibling's outstanding item. */
bool
bind_discard_stopped(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_sgs; i++) {
        b_sg_t *sg = &cn->sgs[i];
        if (!sg->used || !sg->stopped) continue;
        bool retired = false, more = false;
        moqr_result_t rc = moqr_core_internal_discard_subgroup(
            b->core, cn->binding, sg->rsub, sg->group, sg->subgroup,
            sg->core_list, 256u, now_us, &retired, &more);
        if (rc == MOQR_ERR_CAPACITY) more = true;
        else if (rc != MOQR_OK && !(rc == MOQR_ERR_STALE_HANDLE && retired)) {
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "stopped cursor mismatch", now_us);
            conn_detach(b, cn, now_us);
            return false;
        }
        if (retired) {
            memset(sg, 0, sizeof(*sg));
            bind_dl_rearm_on_slot_release(b, cn);
        }
        if (more) {
            b->budget_pending = true;
            bind_dl_set(b->dl_ready, (uint32_t)(cn - b->conns));
        }
    }
    return true;
}


bool
bind_delivery_eligible(void *ctx, uint64_t cookie, const moqr_record_view_t *rec)
{
    const b_delivery_capacity_t *cap = ctx;
    if (rec->datagram_pref || rec->obj_state == MOQR_OBJ_ABANDONED) return true;
    for (uint32_t i = 0; i < cap->bind->n_sgs; i++) {
        const b_sg_t *sg = &cap->conn->sgs[i];
        if (!sg->used || (sg->sub_raw == cookie && sg->group == rec->group_id &&
                         sg->subgroup == rec->subgroup_id)) return true;
    }
    return false;
}


b_deliver_outcome_t
bind_deliver(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    b_delivery_capacity_t cap = { .bind = b, .conn = cn };
    for (uint32_t guard = 0; guard < 256; guard++) {
        moqr_delivery_t d;
        moqr_result_t drc;
#ifdef MOQR_BIND_TESTING
        atomic_fetch_add(&bind_dbg_probes, 1);
        if (atomic_load(&bind_dbg_fail_probes) > 0 &&
            atomic_fetch_sub(&bind_dbg_fail_probes, 1) == 1) {
            drc = bind_dbg_probe_result;   /* injected; d stays unused */
        } else
#endif
        {
            if (cn->bind_sg_waiting) {
                for (uint32_t si = 0; si < b->n_sgs; si++) {
                    if (!cn->sgs[si].used) {
                        cn->bind_sg_waiting = false;
                        break;
                    }
                }
            }
            drc = cn->bind_sg_waiting
                ? moqr_core_internal_next_eligible(b->core, cn->binding,
                                                    now_us, &d,
                                                    bind_delivery_eligible, &cap)
                : moqr_core_next_delivery(b->core, cn->binding, now_us, &d);
        }
        if (drc != MOQR_OK) {
            if (drc == MOQR_DONE) {
                if (cn->bind_sg_waiting) return B_DELIVER_BLOCKED_BIND_SG;
                return B_DELIVER_DRAINED;   /* nothing NOW; a mark re-arms */
            }
            if (drc == MOQR_ERR_CAPACITY) {
                /* Transient scheduler capacity (pin array / table growth):
                 * nothing was consumed; self-rearm so sibling work is never
                 * stranded waiting for a mark that will not come. */
                return B_DELIVER_RETRY;
            }
            /* STALE_HANDLE (the core binding vanished under a live conn) or
             * any other error is a consistency failure: fail closed rather
             * than strand an unschedulable connection. */
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "delivery probe failed", now_us);
            conn_detach(b, cn, now_us);
            return B_DELIVER_CLOSED;
        }
        moq_subscription_t ssub = { d.sub_cookie };
        /* Before any notice, open or write for this request: if the session
         * already ended it, nothing more goes out for it. The outstanding
         * delivery is released as a stream error through the checked
         * confirmation (its pins dropped once, nothing counted as delivered;
         * the core retires the subscription) -- a confirmation that fails
         * has detached the connection, and nothing of it is touched again --
         * then the whole request is cleaned up -- its slots, its owner --
         * leaving the session's own resets and terminal to the session. */
        {
            const b_dsub_t *gds = dsub_find(b, cn, d.sub_cookie);
            if (gds != NULL && bind_request_ended_by_session(cn, gds)) {
                if (!bind_confirm_delivery(b, cn, MOQR_DELIVERY_STREAM_ERROR,
                                           now_us)) {
                    return B_DELIVER_CLOSED;
                }
                b_dsub_t *eds = dsub_find(b, cn, d.sub_cookie);
                if (eds != NULL) {
                    bind_dsub_retire(b, cn, eds, now_us);
                }
                continue;
            }
        }
        bool rec_props = false, rec_gap = false;
        uint32_t rec_list = 0;
        moqr_reset_desc_t rec_gap_reset = moqr_reset_desc_none();
        if (d.notice == MOQR_DELIVERY_NOTICE_NONE) {
            (void)moqr_core_internal_delivery_flags(b->core, cn->binding,
                                                    &rec_props, &rec_gap,
                                                    &rec_gap_reset, &rec_list);
        }
        /* Recordless acknowledged notices precede record work. The close IS
         * the payload here, so the notice is acknowledged (DELIVERED) only
         * once every close it names is on the wire; a WOULD_BLOCK holds the
         * notice and the next pump retries exactly where it left off (closed
         * slots are already cleared). A hard close error fails closed — the
         * peer must not be left with a stream that will never FIN. */
        if (d.notice != MOQR_DELIVERY_NOTICE_NONE) {
            bool blocked = false, faulted = false;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie) {
                    continue;
                }
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL
                        ? (cn->sgs[i].group != d.rec.group_id ||
                           cn->sgs[i].subgroup != d.rec.subgroup_id)
                        /* EVICT_WATERMARK: any open subgroup of a group BELOW
                         * the track's oldest RETAINED group is gone (its
                         * records are evicted; the group id is never
                         * re-admitted) and can never receive more objects.
                         * The watermark is the oldest retained group, NOT a
                         * delivered record's group: the schedule is not
                         * strictly ascending, so a lower group can still be
                         * live only at/above it. */
                        : cn->sgs[i].group >= d.oldest_group) {
                    continue;
                }
                if (cn->sgs[i].stopped) {
                    /* This stream already has its session-owned terminal.
                     * A SEAL ack leaves suppression intact for later source
                     * lifetimes. Only authoritative eviction frees the key. */
                    if (d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                        memset(&cn->sgs[i], 0, sizeof(cn->sgs[i]));
                        bind_dl_rearm_on_slot_release(b, cn);
                    }
                    continue;
                }
                /* A reset-flavored seal closes abnormally, forwarding the
                 * upstream RESET code — the subgroup's content is truncated
                 * at the source, and a clean FIN would misrepresent it as
                 * complete. Same retry discipline either way. */
                uint64_t seal_wire = 0;
                bool seal_is_reset = d.notice == MOQR_DELIVERY_NOTICE_SEAL &&
                                     d.seal_reset;
                /* An eviction watermark orphans the subgroup with objects the
                 * downstream never got (or may not have got): it is reset,
                 * never FINed, since a FIN says every object was delivered
                 * (§11.3.2; §7.4). */
                moqr_reset_desc_t seal_desc = d.seal_reset_desc;
                if (d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                    seal_is_reset = moqr_reset_desc_internal(
                        MOQR_RESET_CODE_EVICTED, &seal_desc) == MOQR_OK;
                }
                if (cn->sgs[i].reset_owed) {
                    seal_is_reset = true;   /* the owed reset, as recorded */
                    seal_wire = cn->sgs[i].reset_wire;
                } else if (seal_is_reset &&
                    !bind_reset_wire(b, cn, seal_desc, &seal_wire)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t crc =
                    seal_is_reset
                        ? moq_session_reset_subgroup(cn->session,
                                                     cn->sgs[i].h, seal_wire,
                                                     now_us)
                        : moq_session_close_subgroup(cn->session,
                                                     cn->sgs[i].h, now_us);
                if (crc == MOQ_ERR_WOULD_BLOCK) {
                    blocked = true;
                    break;
                }
                /* STALE_HANDLE on this exact stream (its full handle, so
                 * never a later stream that reused its pool slot): the
                 * session already ended it on its own -- the peer's
                 * STOP_SENDING, a delivery timeout, its own reset -- and owns
                 * that stream's terminal. The request is live (checked
                 * above), and a single subgroup's end does not touch the
                 * others or the subscription (draft-22 section 11.3.2,
                 * draft-18 section 11.4.3), so only this slot is released
                 * and the notice acknowledged. Nothing is sent in its place:
                 * a stale stream is not turned into a FIN, nor reset again,
                 * and is no evidence the peer received its objects. Any
                 * other refusal -- WRONG_STATE on a stream mid-object
                 * included -- is a hard error. */
                if (crc != MOQ_OK && crc != MOQ_ERR_STALE_HANDLE) {
                    faulted = true;
                    break;
                }
                cn->sgs[i].used = false;
                cn->sgs[i].obj_begun = false;
                cn->sgs[i].reset_owed = false;
                bind_dl_rearm_on_slot_release(b, cn);
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
                    break;   /* a seal names exactly one subgroup */
                }
            }
            if (faulted) {
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "notice close failed", now_us);
                conn_detach(b, cn, now_us);
                return B_DELIVER_CLOSED;
            }
            if (!bind_confirm_delivery(b, cn,
                                       blocked ? MOQR_DELIVERY_WOULD_BLOCK
                                               : MOQR_DELIVERY_DELIVERED,
                                       now_us)) {
                return B_DELIVER_CLOSED;
            }
            if (blocked) {
                /* held: the close is an ordinary session write, so this is
                 * action-queue backpressure — resume above the floor */
                return B_DELIVER_BLOCKED_ACTION_CAP;
            }
            continue;
        }
        moqr_delivery_outcome_t oc;
        if (d.rec.datagram_pref) {
            moq_result_t wrc;
            if (d.rec.payload == NULL) {
                wrc = bind_cookie_is_publication(d.sub_cookie)
                    ? moq_session_send_pub_status_datagram(
                          cn->session,
                          (moq_publication_t){ ._opaque = d.sub_cookie },
                          d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority,
                          (moq_object_status_t)d.rec.status, now_us)
                    : moq_session_send_status_datagram(
                          cn->session, ssub, d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority,
                          (moq_object_status_t)d.rec.status, now_us);
            } else {
                const uint8_t *pb = NULL;
                size_t pl = 0;
                moq_rcbuf_t *wp = NULL, *props_own = NULL;
                wrc = bind_props_out(b, cn, d.rec.properties, d.rec.properties_codec,
                                     &wp, &props_own);
                if (wrc != MOQ_OK) {
                    if (props_own != NULL) moq_rcbuf_decref(props_own);
                    oc = bind_props_refused(b, &d, wrc);
                    goto confirm;
                }
                if (wp != NULL) {
                    pb = moq_rcbuf_data(wp);
                    pl = moq_rcbuf_len(wp);
                }
                wrc = bind_cookie_is_publication(d.sub_cookie)
                    ? moq_session_send_pub_object_datagram(
                          cn->session,
                          (moq_publication_t){ ._opaque = d.sub_cookie },
                          d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority, d.rec.end_of_group,
                          (moq_rcbuf_t *)(void *)d.rec.payload, pb, pl, now_us)
                    : moq_session_send_object_datagram(
                          cn->session, ssub, d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority, d.rec.end_of_group,
                          (moq_rcbuf_t *)(void *)d.rec.payload, pb, pl, now_us);
                if (props_own != NULL) moq_rcbuf_decref(props_own);
            }
            oc = oc_from_result(wrc);
        } else {
            int sgi = -1, freei = -1;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (cn->sgs[i].used && cn->sgs[i].sub_raw == d.sub_cookie &&
                    cn->sgs[i].group == d.rec.group_id &&
                    cn->sgs[i].subgroup == d.rec.subgroup_id) {
                    sgi = (int)i;
                    break;
                }
                if (!cn->sgs[i].used && freei < 0) {
                    freei = (int)i;
                }
            }
            if (sgi >= 0 && cn->sgs[sgi].stopped && !d.evicted_reset) {
                oc = MOQR_DELIVERY_DISCARDED;
            } else if (d.rec.obj_state == MOQR_OBJ_ABANDONED && d.evicted_reset) {
                /* The whole group was evicted while begun downstream: reset EVERY
                 * begun subgroup of the group so no peer stream is left with a
                 * begun object and no terminal. Retry-safe — a reset subgroup is
                 * cleared (used=false), so a WOULD_BLOCK mid-way holds the
                 * delivery and the next pump resumes with the remaining ones. A
                 * hard reset error fails closed (terminate the connection). */
                bool blocked = false, faulted = false;
                for (uint32_t i = 0; i < b->n_sgs; i++) {
                    if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie ||
                        cn->sgs[i].group != d.rec.group_id ||
                        cn->sgs[i].stopped ||
                        !cn->sgs[i].obj_begun) {
                        continue;
                    }
                    uint64_t rec_wire = 0;
                    if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire)) {
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "untranslatable reset", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    moq_result_t rrc = moq_session_reset_subgroup(
                        cn->session, cn->sgs[i].h, rec_wire, now_us);
                    if (rrc == MOQ_ERR_WOULD_BLOCK) {
                        blocked = true;
                        break;
                    }
                    if (rrc != MOQ_OK) {
                        faulted = true;
                        break;
                    }
                    cn->sgs[i].used = false;
                    cn->sgs[i].obj_begun = false;
                    bind_dl_rearm_on_slot_release(b, cn);
                }
                if (faulted) {
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "evict reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                if (blocked) {
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                oc = MOQR_DELIVERY_ABANDONED;
            } else if (d.rec.obj_state == MOQR_OBJ_ABANDONED) {
                /* Abandoned head, selected only because this sub already began
                 * the object downstream: RESET the existing subgroup so
                 * the peer sees an abnormal end, not a stuck object — never open
                 * a fresh one. The reset forwards the record's stored terminal
                 * code (d.rec.reset_code): the publisher's RESET code, or a
                 * relay-defined code for STOP / eviction. The core reports
                 * ABANDONED only after the reset is on the wire, so a missing
                 * begun subgroup or a failed reset FAILS CLOSED (terminate the
                 * connection) rather than silently advancing. */
                if (sgi < 0 || !cn->sgs[sgi].obj_begun ||
                    cn->sgs[sgi].obj_id != d.rec.object_id) {
                    /* Desync: core scheduled chunks (emitted>0) but the bind has
                     * no matching begun subgroup. Do not advance without a wire
                     * terminal. */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon desync", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                uint64_t rec_wire2 = 0;
                if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire2)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t rrc = moq_session_reset_subgroup(
                    cn->session, cn->sgs[sgi].h, rec_wire2, now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) {
                    /* Retry the reset next pump; the core holds the delivery. */
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                if (rrc != MOQ_OK) {
                    /* Reset could not be put on the wire: fail closed — the peer
                     * must see a terminal for the begun object, so terminate the
                     * connection (its close IS the terminal). */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                cn->sgs[sgi].used = false;   /* reset shipped: stream finished */
                cn->sgs[sgi].obj_begun = false;
                bind_dl_rearm_on_slot_release(b, cn);
                oc = MOQR_DELIVERY_ABANDONED;
            } else {
                /* The open downstream stream cannot carry this record: an
                 * object of the subgroup before it was not forwarded
                 * (refused at ingest, or expired from cache), so it is not
                 * known to be the next object (§11.3.2: the relay "MUST
                 * reset the Subgroup stream and open a new one to forward
                 * it"); or it has properties the stream was opened without.
                 * A record already begun on the stream continues there, and
                 * one whose first write the session refused right after
                 * opening the stream for it starts there: the stream has
                 * carried nothing it could fail to continue. */
                if (sgi >= 0 &&
                    !(cn->sgs[sgi].obj_begun &&
                      cn->sgs[sgi].obj_id == d.rec.object_id) &&
                    !(cn->sgs[sgi].unwritten &&
                      cn->sgs[sgi].open_obj == d.rec.object_id) &&
                    (rec_gap ||
                     (d.rec.properties != NULL && !cn->sgs[sgi].props))) {
                    moqr_reset_desc_t rd = moqr_reset_desc_none();
                    uint64_t rwire = 0;
                    /* The stream the record does not continue ends as its
                     * upstream stream ended when that was a RESET (the
                     * upstream code), else as a truncated one. */
                    moqr_result_t drc = !rec_gap
                        ? moqr_reset_desc_local(MOQR_RESET_INTERNAL_ERROR, &rd)
                        : rec_gap_reset.tag != MOQR_CODE_NONE
                            ? (rd = rec_gap_reset, MOQR_OK)
                            : moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED, &rd);
                    if (drc != MOQR_OK || !bind_reset_wire(b, cn, rd, &rwire)) {
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "untranslatable reset", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    moq_result_t rrc = moq_session_reset_subgroup(
                        cn->session, cn->sgs[sgi].h, rwire, now_us);
                    if (rrc == MOQ_ERR_WOULD_BLOCK) {
                        if (!bind_confirm_delivery(b, cn,
                                                   MOQR_DELIVERY_REFUSED_UNBEGUN,
                                                   now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_BLOCKED_ACTION_CAP;
                    }
                    if (rrc != MOQ_OK) {
                        b->session_errors++;
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "gap reset failed", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    cn->sgs[sgi].used = false;
                    cn->sgs[sgi].obj_begun = false;
                    bind_dl_rearm_on_slot_release(b, cn);
                    freei = sgi;
                    sgi = -1;
                }
                if (sgi < 0) {
                    moq_subgroup_cfg_t sc;
                    moq_subgroup_cfg_init(&sc);
                    sc.group_id = d.rec.group_id;
                    sc.subgroup_id = d.rec.subgroup_id;
                    sc.publisher_priority = d.rec.publisher_priority;
                    /* The upstream stream's form, per record: a record
                     * appended without it opens with properties exactly
                     * when it carries some. */
                    sc.object_properties = rec_props ||
                                           d.rec.properties != NULL;
                    /* Header-EOG evidence rides the subgroup header on every
                     * record of the upstream subgroup, so the first record
                     * (which opens the downstream stream) preserves the bit. */
                    sc.end_of_group = d.rec.end_of_group;
                    /* §2.2: a relay forwarding a subgroup that begins with
                     * the first object ever published in it MUST set
                     * FIRST_OBJECT; only that record carries the mark, so a
                     * stream opened at any later record leaves it clear. */
                    sc.first_object = d.rec.first_object;
                    moq_subgroup_handle_t h;
                    if (freei < 0) {
                        /* Bind subgroup slots exhausted — never reaches the
                         * session, and provably BEFORE any downstream write
                         * (a mid-object batch would have found its subgroup
                         * open, sgi >= 0). Confirmed as REFUSED_UNBEGUN so
                         * the core releases a chunked outstanding delivery
                         * instead of holding it — a held pre-begin record
                         * would starve the SEAL notices whose closes free
                         * the slot it needs. No parked state: every
                         * slot-release site on this still-open connection
                         * re-arms readiness. */
#ifdef MOQR_BIND_TESTING
                        /* Attribute the refusal to the OFFENDING track
                         * (first latches; a different track — a different
                         * demand — or an unresolvable handle marks the
                         * conn ambiguous: the join must fail closed, never
                         * pick between candidates). */
                        {
                            moqr_track_t dtr;
                            uint64_t dtg;
                            if (moqr_core_sub_track(b->core, d.sub, &dtr,
                                                    &dtg) != MOQR_OK) {
                                cn->dbg_bind_sg_ambiguous = true;
                            } else if (!cn->dbg_bind_sg_track_set) {
                                cn->dbg_bind_sg_track = dtr._opaque;
                                cn->dbg_bind_sg_track_gen = dtg;
                                cn->dbg_bind_sg_track_set = true;
                            } else if (cn->dbg_bind_sg_track !=
                                           dtr._opaque ||
                                       cn->dbg_bind_sg_track_gen != dtg) {
                                cn->dbg_bind_sg_ambiguous = true;
                            }
                        }
#endif
                        if (!bind_confirm_delivery(
                                b, cn, MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        if (!cn->bind_sg_waiting) {
                            cn->bind_sg_waiting = true;
                            continue;   /* maintenance gets the next selection */
                        }
                        return B_DELIVER_BLOCKED_BIND_SG;
                    }
                    b_dsub_t *ds = dsub_find(b, cn, d.sub_cookie);
                    if (ds == NULL) {
                        /* Delivery cannot overtake a parked ACCEPT. If its
                         * request owner is gone, retire only this core sub;
                         * no unaccounted stream may be opened for it. */
                        b->session_errors++;
                        if (!bind_confirm_delivery(
                                b, cn, MOQR_DELIVERY_STREAM_ERROR, now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_RETRY;
                    }
                    /* Snapshot BEFORE the call: open_subgroup returns one
                     * WOULD_BLOCK for two distinct causes. A full action
                     * queue (cap == 0) recovers on the capacity gate; a
                     * queue with room means the SUBGROUP POOL refused —
                     * its frees are observable only through the reap each
                     * open's own prologue performs, so that set gets one
                     * bounded re-attempt per pump. */
                    size_t precap =
                        moq_session_action_capacity(cn->session);
                    moq_result_t orc = bind_cookie_is_publication(d.sub_cookie)
                        ? moq_session_open_pub_subgroup(
                              cn->session,
                              (moq_publication_t){ ._opaque = d.sub_cookie },
                              &sc, now_us, &h)
                        : moq_session_open_subgroup(cn->session, ssub, &sc,
                                                    now_us, &h);
                    if (orc == MOQ_ERR_WOULD_BLOCK) {
                        /* The open itself failed, so the downstream object
                         * provably never began. The SESSION-pool refusal
                         * (queue had room) confirms REFUSED_UNBEGUN so a
                         * chunked outstanding delivery RELEASES instead of
                         * starving the SEAL notices whose closes free the
                         * pool; the zero-capacity ACTION_CAP branch keeps
                         * plain WOULD_BLOCK (its capacity-gated park and
                         * recovery are validated as-is, and a whole-object
                         * release there is already the behavior). */
                        if (!bind_confirm_delivery(
                                b, cn,
                                precap == 0
                                    ? MOQR_DELIVERY_WOULD_BLOCK
                                    : MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return precap == 0 ? B_DELIVER_BLOCKED_ACTION_CAP
                                           : B_DELIVER_BLOCKED_SESSION_SG;
                    }
                    if (orc != MOQ_OK) {
                        /* A deterministic contract violation can never
                         * succeed on retry: fail closed. */
                        b->session_errors++;
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "subgroup open failed",
                                                now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    ds->streams_opened = bind_stream_count_next(
                        ds->streams_opened, cn->wire_draft);
                    sgi = freei;
                    cn->sgs[sgi].used = true;
                    cn->sgs[sgi].stopped = false;
                    cn->sgs[sgi].sub_raw = d.sub_cookie;
                    cn->sgs[sgi].rsub = d.sub;
                    cn->sgs[sgi].core_list = rec_list;
                    cn->sgs[sgi].group = d.rec.group_id;
                    cn->sgs[sgi].subgroup = d.rec.subgroup_id;
                    cn->sgs[sgi].h = h;
                    cn->sgs[sgi].obj_begun = false;   /* fresh write progress  */
                    cn->sgs[sgi].obj_id = 0;
                    cn->sgs[sgi].next_chunk = 0;
                    cn->sgs[sgi].props = sc.object_properties;
                    cn->sgs[sgi].unwritten = true;
                    cn->sgs[sgi].open_obj = d.rec.object_id;
                    cn->sgs[sgi].reset_owed = false;
                    cn->sgs[sgi].reset_wire = 0;
                    cn->bind_sg_waiting = false;
                }
                if (d.rec.chunk_count > 0 || d.rec.obj_state == MOQR_OBJ_OPEN ||
                    cn->sgs[sgi].obj_begun) {
                    /* Chunked record: stream the batch. Returns DELIVERED after
                     * end_object (COMPLETE), STALLED at the live edge (OPEN),
                     * WOULD_BLOCK to hold, or STREAM_ERROR. */
                    oc = bind_write_chunked(b, cn, &d, &cn->sgs[sgi], now_us);
                } else if (d.rec.payload == NULL) {
                    oc = oc_from_result(moq_session_write_status_object(
                        cn->session, cn->sgs[sgi].h, d.rec.object_id,
                        (moq_object_status_t)d.rec.status, now_us));
                } else {
                    moq_object_cfg_t ocfg;
                    moq_object_cfg_init(&ocfg);
                    ocfg.object_id = d.rec.object_id;
                    ocfg.payload = (moq_rcbuf_t *)(void *)d.rec.payload;
                    moq_rcbuf_t *props_own = NULL;
                    moq_result_t prc = bind_props_out(b, cn, d.rec.properties,
                                                      d.rec.properties_codec,
                                                      &ocfg.properties, &props_own);
                    if (prc != MOQ_OK) {
                        oc = bind_props_refused(b, &d, prc);
                    } else {
                        oc = oc_from_result(moq_session_write_object_ex(
                            cn->session, cn->sgs[sgi].h, &ocfg, now_us));
                    }
                    if (props_own != NULL) moq_rcbuf_decref(props_own);
                }
                if (oc == MOQR_DELIVERY_DELIVERED || cn->sgs[sgi].obj_begun) {
                    cn->sgs[sgi].unwritten = false;   /* the stream carries it */
                }
                /* NO inline graceful close here — every graceful subgroup
                 * closure rides the acknowledged SEAL notice, whose close
                 * retries under WOULD_BLOCK. An inline close swallowed on
                 * WOULD_BLOCK after a delivered record would clear the slot
                 * and lose the FIN forever (the later notice would find
                 * nothing to close). The seal itself comes only from the
                 * upstream's actual graceful terminal (SUBGROUP_FINISHED,
                 * emitted after the FIN and every buffered object) — the
                 * header EOG bit is subgroup metadata preserved on the
                 * downstream open, never a per-record terminal. */
            }
        }
    confirm:
        if (!bind_confirm_delivery(b, cn, oc, now_us)) {
            return B_DELIVER_CLOSED;
        }
        if (oc == MOQR_DELIVERY_WOULD_BLOCK) {
            /* Write-path backpressure: ordinary writes document only the
             * action queue, and a property-bearing write needs TWO slots —
             * so the observed remaining capacity is the honest floor (the
             * write retries only once the queue drains PAST it). The held
             * (chunked/abandoned) delivery resumes on the recovery probe. */
            return B_DELIVER_BLOCKED_ACTION_CAP;
        }
        if (oc == MOQR_DELIVERY_STREAM_ERROR) {
            /* Terminal write error retired ONE subscription; sibling subs on
             * this binding may hold deliverable work no future mark
             * re-signals — self-rearm rather than strand them. */
            return B_DELIVER_RETRY;
        }
        if (oc != MOQR_DELIVERY_DELIVERED) {
            /* STALLED (live edge) or ABANDONED (reset done): nothing to observe,
             * and the head is no longer selectable — keep serving OTHER subs on
             * this binding so one publisher-stalled object never HOL-blocks them. */
            continue;
        }
        /* Every successful downstream write records exactly one latency
         * observation, so the histogram count tracks deliveries_written. A
         * regressed clock (now_us < arrival) saturates to 0us rather than
         * skipping — the observation is never dropped. */
        uint64_t latency_us =
            now_us >= d.rec.arrival_us ? now_us - d.rec.arrival_us : 0;
        moqr_latency_observe(&b->forward_latency, latency_us);
        b->deliveries_written++;
    }
    return B_DELIVER_BUDGET;   /* guard expired with work remaining */
}
