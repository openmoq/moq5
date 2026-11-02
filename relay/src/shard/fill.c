#include "shards_internal.h"
#include "fill_internal.h"
#include "data_internal.h"
#include "demand_internal.h"
#include "placement_internal.h"

static int
r_loc_cmp(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo)
{
    if (ag != bg) {
        return ag < bg ? -1 : 1;
    }
    if (ao != bo) {
        return ao < bo ? -1 : 1;
    }
    return 0;
}


r_fret_t *
fret_find(r_mgr *m, uint64_t id)
{
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        if (m->fret[i].used && m->fret[i].id == id) {
            return &m->fret[i];
        }
    }
    return NULL;
}


/* Drop the window's items: their clones now, their channel charge in the
 * next demand phase (which owns the channel side). */
static void
fret_drop_window(r_fret_t *f)
{
    while (f->count > 0) {
        r_fitem_t *it = &f->win[f->head];
        moq_rcbuf_decref(it->payload);
        moq_rcbuf_decref(it->props);
        f->release_entries++;
        f->release_bytes += it->charged;
        memset(it, 0, sizeof(*it));
        f->head = (f->head + 1u) % R_FILL_WINDOW;
        f->count--;
    }
    f->head = 0;
}


/* The demand a retrieval rides is ending: the owner drops its side with the
 * demand, so nothing more will come and nothing is owed to it. */
void
fret_demand_gone(r_mgr *m, uint64_t demand_id)
{
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        r_fret_t *f = &m->fret[i];
        if (f->used && f->demand_id == demand_id) {
            f->ended = true;
            memset(&f->terminal, 0, sizeof(f->terminal));
            f->terminal.kind = MOQR_RETRIEVE_ERROR;
            f->terminal.error_kind = MOQR_RETRIEVE_SOURCE_LOST;
            f->terminal.local_result = MOQR_ERR_STALE_HANDLE;
            f->req_owed = false;
            f->more_owed = false;
            f->cancel_owed = false;
        }
    }
}


/* The fill source's open (moqr_bind_fill_source_t): claim the part of the
 * range this shard's log cannot hold. The feed of an accepted demand carries
 * what follows the Largest the owner reported in its ACK, so the owner must
 * answer everything at or below it from cache plus upstream retrieval -- and,
 * until the feed has brought anything, the whole range. Once it has, the claim
 * runs through the groups before the
 * feed's first group, whole: the log takes over at a group it holds, so it
 * never reports the groups the feed never carried as an evicted prefix, and
 * an Object of those groups still on its way through the feed is answered
 * by the owner instead of being skipped. A track not fed by an accepted
 * demand, an ACK without a Largest (the feed carries everything), or a range
 * that starts after the claimable part is not the source's. */
moqr_result_t
shards_fill_open(void *ctx, const moqr_bind_fill_query_t *q,
                 moqr_bind_fill_claim_t *claim)
{
    r_mgr *m = ctx;
    if (m == NULL || q == NULL || claim == NULL ||
        q->struct_size < offsetof(moqr_bind_fill_query_t, deadline_us) +
                             sizeof(q->deadline_us)) {
        return MOQR_ERR_INVAL;
    }
    const r_pdemand *d = NULL;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        const r_pdemand *p = &m->pend[i];
        if (p->state == D_ST_ACKED && moqr_track_eq(p->track, q->track) &&
            p->track_gen == q->track_gen) {
            d = p;
            break;
        }
    }
    if (d == NULL || !d->ack_has_largest || m->fret == NULL) {
        return MOQR_DONE;
    }
    uint64_t eg = q->end_group, eo = q->end_object;
    if (d->feed_seen) {
        uint64_t cg = d->ack_largest_group, co = d->ack_largest_object;
        if (d->feed_min_group > d->ack_largest_group) {
            cg = d->feed_min_group - 1u;
            co = UINT64_MAX;
        }
        if (r_loc_cmp(q->start_group, q->start_object, cg, co) > 0) {
            return MOQR_DONE;
        }
        if (r_loc_cmp(eg, eo, cg, co) > 0) {
            eg = cg;
            eo = co;
        }
    }
    r_fret_t *f = NULL;
    for (uint32_t i = 0; i < m->fill_cap; i++) {
        if (!m->fret[i].used) {
            f = &m->fret[i];
            break;
        }
    }
    if (f == NULL) {
        m->fill_starved = true;
        return MOQR_ERR_WOULD_BLOCK;
    }
    memset(f, 0, sizeof(*f));
    f->used = true;
    f->req_owed = true;
    f->origin = d->origin;
    f->id = ++m->next_fill_id;
    f->demand_id = d->demand_id;
    f->start_group = q->start_group;
    f->start_object = q->start_object;
    f->next_group = q->start_group;
    f->next_object = q->start_object;
    f->end_group = eg;
    f->end_object = eo;
    f->deadline_us = q->deadline_us;
    f->order = (uint8_t)q->group_order;
    f->prio = q->subscriber_priority;
    claim->handle = f->id;
    claim->end_group = eg;
    claim->end_object = eo;
    return MOQR_OK;
}


/* Whether the owner's outstanding delivery toward `src` came from an
 * upstream subgroup whose header set PROPERTIES (false for a notice). */
bool
shard_delivery_subgroup_props(r_mgr *m, uint16_t src)
{
    bool sgp = false, follows_gap = false;
    moqr_reset_desc_t gap_reset;
    uint32_t list;
    if (moqr_core_internal_delivery_flags(m->core, m->pb[src], &sgp, &follows_gap,
                                          &gap_reset, &list) != MOQR_OK) {
        return false;
    }
    return sgp;
}


/* The standalone FETCH source's open: ask the shard that owns the track
 * (named by its binding cookie here) to admit and answer the request. The
 * key is copied now; the REQ carries it. No free retrieval slot refuses this
 * request alone. A Joining FETCH names the accepted demand that feeds the
 * joined track here: the owner answers it only while that demand's
 * subscription there still reads the track the name resolves to. With no
 * such demand, the subscription it joins is no longer fed (STALE_HANDLE). */
moqr_result_t
shards_sa_open(void *ctx, const moqr_retrieve_standalone_query_t *q,
               uint64_t *handle)
{
    r_mgr *m = ctx;
    if (m == NULL || q == NULL || handle == NULL || m->fret == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (m->s->cluster_enabled && !q->cluster_selected) {
        return MOQR_ERR_WRONG_STATE; /* never dispatch an unfiltered mirror */
    }
    uint64_t base = MOQR_SHARD_COOKIE_BASE;
    if (q->remote_cookie < base || q->remote_cookie - base >= m->s->shard_count ||
        q->remote_cookie - base == m->shard) {
        return MOQR_ERR_INVAL;
    }
    r_fret_t *f = NULL;
    for (uint32_t i = 0; i < m->fill_cap; i++) {
        if (!m->fret[i].used) {
            f = &m->fret[i];
            break;
        }
    }
    if (f == NULL) {
        return MOQR_ERR_CAPACITY;
    }
    uint64_t join_demand = 0;
    if (q->join) {
        for (uint32_t i = 0; i < m->pend_len && join_demand == 0; i++) {
            const r_pdemand *d = &m->pend[i];
            if (d->state == D_ST_ACKED && moqr_track_eq(d->track, q->join_track) &&
                d->origin == (uint16_t)(q->remote_cookie - base)) {
                join_demand = d->demand_id;
            }
        }
        if (join_demand == 0) {
            return MOQR_ERR_STALE_HANDLE;
        }
    }
    uint32_t clen = 0;
    uint8_t *canon = canon2_build(q->ns.parts, (uint32_t)q->ns.count, q->name,
                                  &clen, &m->alloc);
    if (canon == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(f, 0, sizeof(*f));
    f->used = true;
    f->standalone = true;
    f->cluster_selected = q->cluster_selected;
    f->source_advertisement = q->cluster_source_advertisement;
    f->requester_hop_id = q->cluster_requester_hop_id;
    f->requester_correlation_id = q->cluster_requester_correlation_id;
    f->req_owed = true;
    f->origin = (uint16_t)(q->remote_cookie - base);
    f->id = ++m->next_fill_id;
    f->canon = canon;
    f->canon_len = clen;
    f->start_group = q->start_group;
    f->start_object = q->start_object;
    f->next_group = q->start_group;
    f->next_object = q->start_object;
    f->end_group = q->end_group;
    f->end_object = q->end_object;
    f->range_form = q->range_form;
    f->relative_start = q->relative_start;
    f->join_demand_id = join_demand;
    f->deadline_us = q->deadline_us;
    f->order = (uint8_t)q->group_order;
    f->prio = q->subscriber_priority;
    *handle = f->id;
    return MOQR_OK;
}


/* The owner's admission, once its OPEN has crossed. An answer that ended
 * before admitting (no slot there, or the owner gone) is an error. */
moqr_result_t
shards_sa_plan(void *ctx, uint64_t handle, moqr_retrieve_standalone_plan_t *out)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || !f->standalone || f->closed || out == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (f->begun) {
        *out = f->plan;
        return MOQR_OK;
    }
    if (f->ended) {
        return f->terminal.local_result != MOQR_OK ? f->terminal.local_result
                                                   : MOQR_ERR_INTERNAL;
    }
    return MOQR_ERR_WOULD_BLOCK;
}


/* Stop expecting the answer: whatever has not begun by the deadline is the
 * fill's Timed-Out run, and an answer arriving later is dropped. A request
 * never pushed is simply not asked; otherwise retain one cancellation for
 * the ordinary demand phase, even when OPEN is still blocked at the owner. */
static void
fret_abandon(r_fret_t *f)
{
    f->abandoned = true;
    f->cancel_owed = !f->req_owed && !f->ended;
    f->req_owed = false;
    f->more_owed = false;
}


static void
fill_result_store(r_fill_result_t *out, const moqr_retrieve_item_t *in)
{
    *out = (r_fill_result_t){
        .kind = in->kind, .start = in->start, .end = in->end,
        .gap_kind = in->gap_kind, .error_kind = in->error_kind,
        .local_result = in->local_result, .session_result = in->session_result,
        .error_code = in->error_code, .retry_after_ms = in->retry_after_ms,
        .cause = in->cause, .can_retry = in->can_retry,
        .data_stream = in->data_stream, .stop_sending = in->stop_sending,
    };
}


static void
fill_result_load(moqr_retrieve_item_t *out, const r_fill_result_t *in)
{
    *out = (moqr_retrieve_item_t){
        .kind = in->kind, .start = in->start, .end = in->end,
        .gap_kind = in->gap_kind, .error_kind = in->error_kind,
        .local_result = in->local_result, .session_result = in->session_result,
        .error_code = in->error_code, .retry_after_ms = in->retry_after_ms,
        .cause = in->cause, .can_retry = in->can_retry,
        .data_stream = in->data_stream, .stop_sending = in->stop_sending,
    };
}


moqr_result_t
shards_fill_retrieve_peek(void *ctx, uint64_t handle, uint64_t now_us,
                          moqr_retrieve_item_t *out)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed || out == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    memset(out, 0, sizeof(*out));
    if (f->count > 0) {
        const r_fitem_t *it = &f->win[f->head];
        fill_result_load(out, &it->result);
        if (it->result.kind == MOQR_RETRIEVE_OBJECT) {
            out->object.group_id = it->group_id;
            out->object.subgroup_id = it->subgroup_id;
            out->object.object_id = it->object_id;
            out->object.publisher_priority = it->prio;
            out->object.datagram_pref = it->datagram_pref;
            out->object.status = (moqr_obj_status_t)it->status;
            out->object.end_of_group = it->end_of_group;
            out->object.declared_len = it->declared_len;
            out->object.obj_state = MOQR_OBJ_COMPLETE;
            out->object.payload = it->payload;
            out->object.properties = it->props;
            out->object.properties_codec = it->props_codec;
        } else if (it->result.kind == MOQR_RETRIEVE_ERROR && it->props != NULL) {
            out->reason = (moq_bytes_t){ moq_rcbuf_data(it->props),
                                         moq_rcbuf_len(it->props) };
        }
        return MOQR_OK;
    }
    if (f->ended) {
        fill_result_load(out, &f->terminal);
        return MOQR_OK;
    }
    if (f->exhausted) {
        out->kind = MOQR_RETRIEVE_END;
        out->start = (moqr_retrieve_location_t){ f->start_group, f->start_object };
        out->end = (moqr_retrieve_location_t){ f->end_group, f->end_object };
        return MOQR_OK;
    }
    if (!f->standalone && !f->abandoned && !f->begun &&
        now_us >= f->deadline_us) {
        fret_abandon(f);
    }
    if (f->abandoned) {
        out->kind = MOQR_RETRIEVE_GAP;
        out->start = (moqr_retrieve_location_t){ f->next_group, f->next_object };
        out->end = (moqr_retrieve_location_t){ f->end_group, f->end_object };
        out->gap_kind = MOQ_FETCH_RANGE_TIMED_OUT;
        f->synth_peeked = true;
        return MOQR_OK;
    }
    /* Once OPEN has crossed, only the owner's original deadline classifies
     * missing spans. Complete queued coverage is never discarded by time. */
    return MOQR_ERR_WOULD_BLOCK;
}


/* The public vtable still requires a cache reader. Typed gaps/errors cannot
 * be represented by it; the built-in source always installs the adjunct. */
moqr_result_t
shards_fill_peek(void *ctx, uint64_t handle, uint64_t now_us,
                 moqr_fetch_item_t *out)
{
    if (out == NULL) return MOQR_ERR_INVAL;
    moqr_retrieve_item_t it;
    moqr_result_t rc = shards_fill_retrieve_peek(ctx, handle, now_us, &it);
    if (rc != MOQR_OK) return rc;
    memset(out, 0, sizeof(*out));
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        out->kind = MOQR_FETCH_ITEM_OBJECT;
        out->rec = it.object;
    } else if (it.kind == MOQR_RETRIEVE_END) {
        out->kind = MOQR_FETCH_ITEM_END;
    } else {
        return MOQR_ERR_UNSUPPORTED;
    }
    return MOQR_OK;
}


moqr_result_t
shards_fill_commit(void *ctx, uint64_t handle)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (f->count > 0) {
        r_fitem_t *it = &f->win[f->head];
        if (it->result.kind == MOQR_RETRIEVE_ERROR) return MOQR_ERR_WRONG_STATE;
        if (r_loc_cmp(it->result.end.group, it->result.end.object,
                      f->end_group, f->end_object) < 0) {
            f->next_group = it->result.end.group +
                (it->result.end.object == UINT64_MAX ? 1u : 0u);
            f->next_object = it->result.end.object == UINT64_MAX
                                 ? 0 : it->result.end.object + 1u;
        }
        moq_rcbuf_decref(it->payload);
        moq_rcbuf_decref(it->props);
        f->release_entries++;
        f->release_bytes += it->charged;
        memset(it, 0, sizeof(*it));
        f->head = (f->head + 1u) % R_FILL_WINDOW;
        f->count--;
        if (f->count == 0 && f->granted == 0 && !f->ended &&
            !f->abandoned) {
            f->more_owed = true;
        }
        return MOQR_OK;
    }
    if (f->synth_peeked) {
        f->exhausted = true;
        return MOQR_OK;
    }
    return MOQR_ERR_WRONG_STATE;
}


void
shards_fill_close(void *ctx, uint64_t handle)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed) {
        return;
    }
    fret_drop_window(f);
    moq_rcbuf_decref(f->meta_props);
    f->meta_props = NULL;
    f->plan.meta_bytes = (moq_bytes_t){ NULL, 0 };
    if (f->req_owed) {
        f->req_owed = false;
    } else if (!f->ended && !f->abandoned) {
        f->cancel_owed = true;
    }
    f->more_owed = false;
    f->closed = true;
}


/* -- fill retrieval: owner ---------------------------------------------------- */

/* The requester times only unopened claims. Typed GAPs are already classified
 * and must not wait again. The owner contributes cursor no-origin waits;
 * admitted collector/cancellation timers remain in its bind at zero credit. */
static bool
fret_needs_deadline(const r_fret_t *f)
{
    /* A standalone FETCH waits for its admission, not for time: the owner
     * answers its REQ at once and times the retrieval itself. */
    return f->used && !f->standalone && !f->closed && !f->abandoned &&
           !f->exhausted && !f->ended && !f->begun && f->count == 0;
}


uint64_t
shards_fill_next_deadline(void *ctx)
{
    const r_mgr *m = ctx;
    uint64_t next = UINT64_MAX;
    for (uint32_t i = 0; m != NULL && m->fret != NULL && i < m->fill_cap;
         i++) {
        const r_fret_t *f = &m->fret[i];
        if (fret_needs_deadline(f) && f->deadline_us < next) {
            next = f->deadline_us;
        }
    }
    for (uint32_t i = 0; m != NULL && m->oret != NULL && i < m->fill_cap;
         i++) {
        const r_oret_t *o = &m->oret[i];
        uint64_t d = moqr_bind_retrieve_next_deadline(&o->cursor);
        if (o->used && d < next) {
            next = d;
        }
    }
    return next;
}


r_oret_t *
oret_find(r_mgr *m, uint16_t src, uint64_t id)
{
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        if (m->oret[i].used && m->oret[i].src == src && m->oret[i].id == id) {
            return &m->oret[i];
        }
    }
    return NULL;
}


void
oret_free(r_mgr *m, r_oret_t *o)
{
    moqr_bind_retrieve_close(m->s->shards[m->shard].bind, &o->cursor);
    if (o->standalone && moqr_fetch_is_valid(o->owner)) {
        (void)moqr_core_fetch_close(m->core, o->owner);
    }
    moq_rcbuf_decref(o->meta_props);
    memset(o, 0, sizeof(*o));
}


/* An admitted demand is closing: its retrievals close with it (the
 * requester resolves them from the demand's own end). */
void
oret_demand_gone(r_mgr *m, uint16_t src, uint64_t demand_id)
{
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        r_oret_t *o = &m->oret[i];
        if (o->used && o->src == src && o->demand_id == demand_id) {
            oret_free(m, o);
        }
    }
}


/* The admitted demand a retrieval answers for, while it still serves. */
static const r_psub *
owner_fill_psub(const r_mgr *m, uint16_t src, uint64_t demand_id)
{
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == src && p->demand_id == demand_id &&
            p->ack_sent && p->has_otrack && !p->undemanding &&
            !p->terminating && !p->done_pending) {
            return p;
        }
    }
    return NULL;
}


/* Initialization sends nothing. Only the owner data phase calls this shared
 * cursor; its accepted pseudo-subscription is the authorization authority. */
static moqr_result_t
owner_fill_cursor(r_mgr *m, r_oret_t *o)
{
    if (o->standalone) {
        /* The request's own admission authorizes it, as for a local FETCH:
         * the requester authorized the FETCH before asking. */
        moqr_retrieve_request_t sa = {
            .binding = m->pb[o->src], .standalone = true, .owner = o->owner,
            .track = o->plan.track, .track_gen = o->plan.track_gen,
            .source = o->src, .child = o->id,
            .start = { o->plan.start_group, o->plan.start_object },
            .end = { o->plan.end_group, o->plan.end_object },
            .group_order = (moqr_group_order_t)o->order,
            .subscriber_priority = o->prio, .deadline_us = o->deadline_us,
        };
        return moqr_bind_retrieve_init(m->s->shards[m->shard].bind, &o->cursor,
                                       &sa);
    }
    const r_psub *p = owner_fill_psub(m, o->src, o->demand_id);
    if (p == NULL) return MOQR_ERR_STALE_HANDLE;
    moqr_retrieve_request_t request = {
        .binding = m->pb[o->src], .parent = p->sub,
        .track = p->otrack, .track_gen = p->otrack_gen,
        .source = o->src, .demand = o->demand_id, .child = o->id,
        .start = { o->start_group, o->start_object },
        .end = { o->end_group, o->end_object },
        .group_order = (moqr_group_order_t)o->order,
        .subscriber_priority = o->prio, .deadline_us = o->deadline_us,
    };
    return moqr_bind_retrieve_init(m->s->shards[m->shard].bind, &o->cursor,
                                   &request);
}


/* Detach upstream ownership before parking an allocation-free local terminal.
 * The bind retains any owed cancellation until its actual child settles. */
static void
owner_fill_fail(r_mgr *m, r_oret_t *o, moqr_result_t failure)
{
    moqr_bind_retrieve_close(m->s->shards[m->shard].bind, &o->cursor);
    o->failed = true;
    o->failure = failure;
}


static void
fill_local_error(r_fill_result_t *out, moqr_result_t failure)
{
    memset(out, 0, sizeof(*out));
    out->kind = MOQR_RETRIEVE_ERROR;
    out->error_kind = failure == MOQR_ERR_STALE_HANDLE
                          ? MOQR_RETRIEVE_SOURCE_LOST : MOQR_RETRIEVE_LOCAL;
    out->local_result = failure;
}


/* Requester side of the demand phase for fill retrievals: return the channel
 * charge of window items the fill has consumed, push whatever the retrieval
 * owes the owner -- REQ, then CANCEL or a new credit window -- and free a
 * claim the binding has closed once nothing more is owed. A refused push
 * keeps its flag: the flag is the durable record. */
void
requester_fill_phase(moqr_shards_t *s, uint16_t shard,
                     uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        r_fret_t *f = &m->fret[i];
        if (!f->used) {
            continue;
        }
        if (f->release_entries > 0) {
            dch_release_held(s, f->origin, shard, f->release_entries,
                             f->release_bytes);
            f->release_entries = 0;
            f->release_bytes = 0;
        }
        r_demand_msg_t msg;
        memset(&msg, 0, sizeof(msg));
        msg.demand_id = f->demand_id;
        msg.fill_id = f->id;
        bool data_waiting = data_work_pending(m, f->origin);
        if (f->req_owed) {
            msg.kind = D_MSG_FILL_REQ;
            msg.group_id = f->start_group;
            msg.object_id = f->start_object;
            msg.fill_end_group = f->end_group;
            msg.fill_end_object = f->end_object;
            msg.fill_deadline_us = f->deadline_us;
            msg.fill_order = f->order;
            msg.prio = f->prio;
            msg.fill_credit = R_FILL_WINDOW;
            if (f->standalone) {
                /* The key moves to the channel with the REQ. */
                msg.fetch_standalone = true;
                msg.cluster_selected = f->cluster_selected;
                msg.source_advertisement = f->source_advertisement;
                msg.requester_hop_id = f->requester_hop_id;
                msg.requester_correlation_id = f->requester_correlation_id;
                msg.fetch_range_form = f->range_form;
                msg.fetch_relative_start = f->relative_start;
                msg.fetch_join_demand = f->join_demand_id;
                msg.canon = f->canon;
                msg.canon_len = f->canon_len;
            }
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->canon = NULL;
            f->canon_len = 0;
            f->req_owed = false;
            f->granted = R_FILL_WINDOW;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        }
        if (f->cancel_owed) {
            msg.kind = D_MSG_FILL_CANCEL;
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->cancel_owed = false;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        } else if (f->more_owed) {
            msg.kind = D_MSG_FILL_MORE;
            msg.fill_credit = R_FILL_WINDOW;
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->granted += msg.fill_credit;
            f->more_owed = false;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        }
        if (f->closed && !f->req_owed && !f->cancel_owed) {
            sh_free(&m->alloc, f->canon, f->canon_len);
            memset(f, 0, sizeof(*f));
            if (m->fill_starved) {
                m->fill_starved = false;
                m->fill_wake_owed = true;
            }
        }
    }
}


/* Push one owner answer message as DATA under this turn's budget. A refused
 * push releases what the message owned, marks src pending and leaves the
 * cursor where it was, so the same item is re-read next time. */
static bool
owner_fill_push(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                r_demand_msg_t *msg, dp_budget_t *bud,
                uint64_t *pushed_dst_mask, uint64_t *pending)
{
    r_mgr *m = s->shards[shard].mgr;
    uint64_t lb = dmsg_logical_bytes(msg);
    if (!dp_admit(bud, lb) ||
        !dch_push_arb(s, shard, dst, msg, true, ctrl_work_pending(m, dst))) {
        dmsg_release(&s->alloc, msg);
        *pending |= 1ull << dst;
        return false;
    }
    dp_spend(bud, lb);
    m->pump_messages++;
    m->pump_bytes += lb;
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}


/* owner_fill_push with *props (an owned reference, or NULL) moved into the
 * message as its properties: on success the channel owns it and *props is
 * NULL; on a refused push it is handed back to *props, never released. */
static bool
owner_fill_push_props(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                      r_demand_msg_t *msg, moq_rcbuf_t **props, dp_budget_t *bud,
                      uint64_t *pushed_dst_mask, uint64_t *pending)
{
    msg->props = *props;
    *props = NULL;
    r_mgr *m = s->shards[shard].mgr;
    uint64_t lb = dmsg_logical_bytes(msg);
    if (!dp_admit(bud, lb) ||
        !dch_push_arb(s, shard, dst, msg, true, ctrl_work_pending(m, dst))) {
        *props = msg->props;
        msg->props = NULL;
        dmsg_release(&s->alloc, msg);
        *pending |= 1ull << dst;
        return false;
    }
    dp_spend(bud, lb);
    m->pump_messages++;
    m->pump_bytes += lb;
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}


/* Shared coverage runs only in the owner data phase. Peeking at zero credit
 * still observes hard errors/deadlines; successful FIFO transfer is the sole
 * permission to commit. All cursor allocations remain bind-owned. */
bool
owner_fill_pass(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                dp_budget_t *bud, uint64_t *pushed_dst_mask, bool *attempted,
                uint64_t *pending)
{
    r_mgr *m = s->shards[shard].mgr;
    moqr_bind_t *bind = s->shards[shard].bind;
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        r_oret_t *o = &m->oret[i];
        if (!o->used) continue;
        *attempted = true;
        uint16_t dst = o->src;
        if (owner_metadata_pending(m, dst)) continue;
        for (;;) {
            if (bud->turn_over) {
                *pending |= 1ull << dst;
                return false;
            }
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.demand_id = o->demand_id;
            msg.fill_id = o->id;
            if (o->standalone && o->plan.wire.admit != MOQR_FETCH_ACCEPT) {
                /* Refused here: the admission is the whole answer. */
                msg.kind = D_MSG_FILL_OPEN;
                msg.fetch_standalone = true;
                msg.fetch_admit = MOQR_FETCH_REJECT;
                msg.error_code = o->plan.wire.error_code;
                msg.fetch_capacity = o->capacity;
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                    pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (!o->cursor.live && !o->failed) {
                moqr_result_t rc = owner_fill_cursor(m, o);
                if (rc != MOQR_OK) owner_fill_fail(m, o, rc);
            }
            if (o->failed) {
                msg.kind = D_MSG_FILL_END;
                fill_local_error(&msg.fill_result, o->failure);
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                     pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (!o->open_sent) {
                msg.kind = D_MSG_FILL_OPEN;
                if (o->standalone) {
                    msg.fetch_standalone = true;
                    msg.fetch_admit = MOQR_FETCH_ACCEPT;
                    msg.fetch_eot = o->plan.wire.end_of_track;
                    msg.group_id = o->plan.start_group;
                    msg.object_id = o->plan.start_object;
                    msg.fill_end_group = o->plan.end_group;
                    msg.fill_end_object = o->plan.end_object;
                    msg.fetch_end_raw_object = o->plan.wire.end_object;
                    msg.metadata.state = o->meta_state;
                    msg.metadata.wire_version = o->meta_version;
                }
                /* The clone moves into the message whole: once pushed, this
                 * lane never touches it again; refused, it comes back. */
                if (!owner_fill_push_props(s, shard, dst, &msg, &o->meta_props, bud,
                                           pushed_dst_mask, pending)) break;
                o->open_sent = true;
                continue;
            }
            moqr_retrieve_item_t it;
            moqr_result_t rc = moqr_bind_retrieve_peek(bind, &o->cursor,
                                                       now_us, &it);
            if (rc == MOQR_ERR_WOULD_BLOCK) break;
            if (rc != MOQR_OK) {
                owner_fill_fail(m, o, rc);
                continue;
            }
            fill_result_store(&msg.fill_result, &it);
            if (it.kind == MOQR_RETRIEVE_END) {
                msg.kind = D_MSG_FILL_END;
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                     pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (it.kind != MOQR_RETRIEVE_OBJECT &&
                it.kind != MOQR_RETRIEVE_GAP && it.kind != MOQR_RETRIEVE_ERROR) {
                owner_fill_fail(m, o, MOQR_ERR_INVAL);
                continue;
            }
            msg.kind = D_MSG_FILL_ITEM;
            uint64_t bytes = it.kind == MOQR_RETRIEVE_OBJECT
                ? cap_add(moq_rcbuf_len(it.object.payload),
                           moq_rcbuf_len(it.object.properties))
                : it.kind == MOQR_RETRIEVE_ERROR ? it.reason.len : 0;
            if (bytes > s->dch_byte_cap) {
                owner_fill_fail(m, o, MOQR_ERR_CAPACITY);
                continue;
            }
            if (o->credit == 0) break;
            if (it.kind == MOQR_RETRIEVE_OBJECT) {
                msg.group_id = it.object.group_id;
                msg.subgroup_id = it.object.subgroup_id;
                msg.object_id = it.object.object_id;
                msg.prio = it.object.publisher_priority;
                msg.status = (uint8_t)it.object.status;
                msg.datagram_pref = it.object.datagram_pref;
                msg.end_of_group = it.object.end_of_group;
                msg.declared_len = it.object.declared_len;
                msg.props_codec = it.object.properties_codec;
                if ((it.object.payload != NULL &&
                     moq_rcbuf_clone(&s->alloc, it.object.payload,
                                     &msg.payload) != MOQ_OK) ||
                    (it.object.properties != NULL &&
                     moq_rcbuf_clone(&s->alloc, it.object.properties,
                                     &msg.props) != MOQ_OK)) {
                    dmsg_release(&s->alloc, &msg);
                    owner_fill_fail(m, o, MOQR_ERR_NOMEM);
                    continue;
                }
            } else if (it.kind == MOQR_RETRIEVE_ERROR && it.reason.len != 0) {
                if (it.reason.data == NULL) {
                    owner_fill_fail(m, o, MOQR_ERR_INVAL);
                    continue;
                }
                if (moq_rcbuf_create(&s->alloc, it.reason.data, it.reason.len,
                                     &msg.props) != MOQ_OK) {
                    owner_fill_fail(m, o, MOQR_ERR_NOMEM);
                    continue;
                }
            }
            if (!owner_fill_push(s, shard, dst, &msg, bud,
                                 pushed_dst_mask, pending)) break;
            o->credit--;
            if (it.kind == MOQR_RETRIEVE_ERROR) {
                oret_free(m, o);
                break;
            }
            rc = moqr_bind_retrieve_commit(bind, &o->cursor);
            if (rc != MOQR_OK) owner_fill_fail(m, o, rc);
        }
    }
    return true;
}


/* Owner role: take on one retrieval. Missing admission or a full owner table
 * produces a child-local ERROR, never successful END or a fabricated gap.
 * Returns false when that answer cannot be pushed yet (the REQ stays at the
 * head). */
/* Owner role: admit a standalone FETCH another shard routed here, exactly as
 * a FETCH this shard received would be admitted (the requester's pseudo-
 * binding stands for its connection). The answer, OPEN with the admission,
 * is pushed by the data phase. A Joining FETCH is answered only from the
 * track its feeding demand's pump-sub reads: once that demand no longer
 * serves, or the name resolves to a replacement, the join is refused, never
 * served the replacement's Objects. */
static bool
owner_sa_accept(moqr_shards_t *s, uint16_t shard, uint16_t src,
                const r_demand_msg_t *msg, r_oret_t *o, uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    moq_bytes_t parts[R_NS_MAX_PARTS], name;
    moqr_fetch_req_t req;
    moqr_fetch_req_init(&req);
    req.ns = canon2_decode(msg->canon, parts, &name);
    req.name = name;
    req.start_group = msg->group_id;
    req.start_object = msg->object_id;
    req.end_group = msg->fill_end_group;
    req.end_object = msg->fill_end_object;
    req.group_order = (moqr_group_order_t)msg->fill_order;
    req.subscriber_priority = msg->prio;
    req.cookie = msg->fill_id;
    req.report_gaps = true;
    req.report_unknown = true;
    req.report_pending = true;
    req.range_form = msg->fetch_range_form;
    req.relative_start = msg->fetch_relative_start;
    memset(o, 0, sizeof(*o));
    o->used = true;
    o->standalone = true;
    o->src = src;
    o->id = msg->fill_id;
    o->deadline_us = msg->fill_deadline_us;
    o->order = msg->fill_order;
    o->prio = msg->prio;
    o->credit = msg->fill_credit;
    const r_psub *joined = msg->fetch_join_demand != 0
                               ? owner_fill_psub(m, src, msg->fetch_join_demand)
                               : NULL;
    if (msg->fetch_join_demand != 0 && joined == NULL) {
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INVALID_JOINING;
        return true;
    }
    if (s->cluster_enabled && msg->cluster_selected &&
        moq_handle_session_tag(msg->source_advertisement.source._opaque) != (uint32_t)shard + 1u) {
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INTERNAL;
        return true;
    }
    moqr_result_t rc = s->cluster_enabled && msg->cluster_selected
        ? moqr_core_internal_retrieve_open_cluster(m->core, m->pb[src], &req,
            msg->source_advertisement,
            msg->requester_hop_id, msg->requester_correlation_id,
            now_us, &o->owner, &o->plan)
        : moqr_core_internal_retrieve_open(m->core, m->pb[src], &req,
                                           now_us, &o->owner, &o->plan);
    if (rc == MOQR_OK && joined != NULL && o->plan.wire.admit == MOQR_FETCH_ACCEPT &&
        !moqr_track_eq(o->plan.track, joined->otrack)) {
        (void)moqr_core_fetch_close(m->core, o->owner);
        memset(&o->plan, 0, sizeof(o->plan));
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INVALID_JOINING;
    } else if (rc != MOQR_OK) {
        memset(&o->plan, 0, sizeof(o->plan));
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INTERNAL;
        o->capacity = rc == MOQR_ERR_CAPACITY;   /* the owner's FETCH pool */
    } else if (o->plan.wire.admit == MOQR_FETCH_ACCEPT) {
        /* The answer's Track Properties are fixed here, at admission. A copy
         * that cannot be made is UNAVAILABLE: the requester refuses rather
         * than answer without them. */
        moqr_bind_meta_ref_t pin;
        moqr_bind_t *bind = s->shards[shard].bind;
        (void)moqr_bind_meta_reply(bind, o->plan.track, &pin);
        o->meta_state = pin.state;
        o->meta_version = pin.wire_version;
        if (pin.state == MOQR_BIND_META_PRESENT &&
            (pin.bytes.len == 0 || pin.bytes.data == NULL ||
             pin.bytes.len > MOQR_BIND_META_SET_MAX ||
             moq_rcbuf_create(&m->alloc, pin.bytes.data, pin.bytes.len,
                              &o->meta_props) != MOQ_OK)) {
            o->meta_state = MOQR_BIND_META_UNAVAILABLE;
            o->meta_props = NULL;
        }
        moqr_bind_meta_release(bind, &pin);
        o->start_group = o->plan.start_group;
        o->start_object = o->plan.start_object;
        o->end_group = o->plan.end_group;
        o->end_object = o->plan.end_object;
    } else if (o->plan.remote_cookie != 0) {
        /* Not this shard's either: never routed further. */
        o->plan.wire.error_code = R_ERR_INTERNAL;
    }
    return true;
}


bool
owner_fill_accept(moqr_shards_t *s, uint16_t shard, uint16_t src,
                  const r_demand_msg_t *msg, uint64_t *pushed_dst_mask,
                  uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    r_oret_t *o = NULL;
    if (m->oret != NULL && msg->fetch_standalone && msg->demand_id == 0 &&
        msg->canon != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            if (!m->oret[i].used) {
                o = &m->oret[i];
                break;
            }
        }
        if (o != NULL) {
            return owner_sa_accept(s, shard, src, msg, o, now_us);
        }
    } else if (m->oret != NULL && !msg->fetch_standalone &&
        owner_fill_psub(m, src, msg->demand_id) != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            if (!m->oret[i].used) {
                o = &m->oret[i];
                break;
            }
        }
    }
    if (o == NULL) {
        if (owner_metadata_pending(m, src)) {
            return false;   /* even a refusal follows latest metadata */
        }
        r_demand_msg_t reply;
        memset(&reply, 0, sizeof(reply));
        reply.kind = D_MSG_FILL_END;
        reply.demand_id = msg->demand_id;
        reply.fill_id = msg->fill_id;
        fill_local_error(&reply.fill_result,
            !msg->fetch_standalone &&
                    owner_fill_psub(m, src, msg->demand_id) == NULL
                ? MOQR_ERR_STALE_HANDLE : MOQR_ERR_CAPACITY);
        if (!dch_push_arb(s, shard, src, &reply, true,
                          ctrl_work_pending(m, src))) {
            return false;
        }
        m->pump_messages++;
        if (pushed_dst_mask != NULL) {
            *pushed_dst_mask |= 1ull << src;
        }
        return true;
    }
    memset(o, 0, sizeof(*o));
    o->used = true;
    o->src = src;
    o->id = msg->fill_id;
    o->demand_id = msg->demand_id;
    o->start_group = msg->group_id;
    o->start_object = msg->object_id;
    o->end_group = msg->fill_end_group;
    o->end_object = msg->fill_end_object;
    o->deadline_us = msg->fill_deadline_us;
    o->order = msg->fill_order;
    o->prio = msg->prio;
    o->credit = msg->fill_credit;
    return true;
}
