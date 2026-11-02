#include "bind_internal.h"
#include "fetch_joining_internal.h"
#include "authorization_internal.h"
#include "fetch_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "testing_internal.h"
#include "upstream_internal.h"

void fwd_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

bool fwd_result_is_gone(moq_result_t rc);

void fwd_admit(moqr_bind_t *b, b_conn_t *cn,
               const moq_fetch_request_event_t *fq,
               const moq_fetch_request_options_t *fo, uint64_t now_us);

bool fwd_on_up_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
                     uint64_t now_us);

bool fwd_on_down_cancelled(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);

void bind_fwd_pump(moqr_bind_t *b, uint64_t now_us);


uint64_t
conn_cookie(const moqr_bind_t *b, const b_conn_t *cn)
{
    return (uint64_t)(cn - b->conns) + 1u;
}


/* The connection a transaction side names, or NULL when that slot has been
 * closed or reused since (cookie + core binding must both still match). */
b_conn_t *
fwd_conn(moqr_bind_t *b, uint64_t cookie, moqr_binding_t binding)
{
    b_conn_t *cn = conn_by_cookie(b, cookie);
    if (cn == NULL || !cn->used || cn->closed || cn->session == NULL ||
        !moqr_binding_eq(cn->binding, binding)) {
        return NULL;
    }
    return cn;
}


bool
fwd_bytes_charge(moqr_bind_t *b, uint64_t *charged, uint64_t n)
{
    if (n > b->fwd_bytes_cap - b->fwd_bytes_used) {
        return false;
    }
    b->fwd_bytes_used += n;
    *charged += n;
#ifdef MOQR_BIND_TESTING
    if (b->fwd_bytes_used > b->dbg_fwd.bytes_high) {
        b->dbg_fwd.bytes_high = b->fwd_bytes_used;
    }
#endif
    return true;
}


bool
fwd_charge(moqr_bind_t *b, b_fwd_t *t, uint64_t n)
{
    return fwd_bytes_charge(b, &t->charged, n);
}


static void
fwd_uncharge(moqr_bind_t *b, b_fwd_t *t, uint64_t n)
{
    b->fwd_bytes_used -= n;
    t->charged -= n;
}


/* Owned, charged copy of a borrowed byte span. Empty spans copy nothing. */
static bool
fwd_copy(moqr_bind_t *b, b_fwd_t *t, const uint8_t *src, size_t len,
         uint8_t **dst, size_t *dst_len)
{
    *dst = NULL;
    *dst_len = 0;
    if (len == 0) {
        return true;
    }
    if (src == NULL || !fwd_charge(b, t, len)) {
        return false;
    }
    uint8_t *p = b->alloc.alloc(len, b->alloc.ctx);
    if (p == NULL) {
        fwd_uncharge(b, t, len);
        return false;
    }
    memcpy(p, src, len);
    *dst = p;
    *dst_len = len;
    return true;
}


void
fwd_item_release(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it)
{
    if (it->payload != NULL) {
        moq_rcbuf_decref(it->payload);
    }
    if (it->properties != NULL) {
        moq_rcbuf_decref(it->properties);
    }
    fwd_uncharge(b, t, it->charged);
    /* Append-only slots retain their Location until transaction retirement:
     * data can precede FETCH_OK, even after live coverage consumes this copy. */
    it->payload = NULL;
    it->properties = NULL;
    it->charged = 0;
}


/* Release every owned reference and copy and free the slot. Only called once
 * BOTH sides are terminal: nothing is owed on either session any more. */
void
fwd_release(moqr_bind_t *b, b_fwd_t *t)
{
    for (uint32_t i = t->next_write; i < t->n_items; i++) {
        fwd_item_release(b, t, &t->items[i]);
    }
    if (t->props != NULL) {
        b->alloc.free(t->props, t->props_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->props_len);
    }
    if (t->reason != NULL) {
        b->alloc.free(t->reason, t->reason_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->reason_len);
    }
    if (t->key != NULL) {
        b->alloc.free(t->key, t->key_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->key_len);
    }
    /* Invariant: every charge was matched by its release. */
    b->fwd_bytes_used -= t->charged;
    memset(t, 0, sizeof(*t));
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live--;
#endif
}


static b_fwd_t *
fwd_find_up(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (t->used && t->ufetch_live && t->u_cookie == cookie &&
            moqr_binding_eq(t->u_binding, cn->binding) &&
            t->ufetch._opaque == handle) {
            return t;
        }
    }
    return NULL;
}


static b_fwd_t *
fwd_find_down(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (t->used && t->consumer == FW_CONSUMER_JOIN &&
            t->d != FW_D_TERMINAL && t->d_cookie == cookie &&
            moqr_binding_eq(t->d_binding, cn->binding) &&
            t->sfetch._opaque == handle) {
            return t;
        }
    }
    return NULL;
}


/* The upstream side ends for a reason other than its own COMPLETE/ERROR/RESET:
 * if a request is still live upstream it is cancelled (retried until the
 * session commits it); the recorded `end` is the first one, never overwritten. */
void
fwd_end_up(b_fwd_t *t, uint8_t end)
{
    if (t->end == FW_END_NONE) {
        t->end = end;
    }
    if (t->u == FW_U_PENDING) {
        t->u = FW_U_CANCELLING;
    } else if (t->u == FW_U_UNSENT) {
        t->u = FW_U_TERMINAL;
    }
}


/* A connection is going away: every transaction naming it on either side
 * stops addressing its session. Idempotent (a deferred detach retries). */
void
fwd_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used) {
            continue;
        }
        if (t->u_cookie == cookie && moqr_binding_eq(t->u_binding, cn->binding) &&
            t->u != FW_U_TERMINAL) {
            if (t->end == FW_END_NONE) {
                t->end = FW_END_LOST;
            }
            t->u = FW_U_TERMINAL;      /* no cancel: the session is gone */
            t->ufetch_live = false;
            FWD_DBG(b, lost);
        }
        if (t->d_cookie == cookie && moqr_binding_eq(t->d_binding, cn->binding) &&
            t->d != FW_D_TERMINAL) {
            t->d = FW_D_TERMINAL;      /* nothing more may be written to it */
            fwd_end_up(t, FW_END_CANCELLED);
            FWD_DBG(b, down_lost);
        }
    }
}


/* Admit a downstream Joining FETCH, or refuse it on the wire. The session has
 * resolved its range against the joined subscription's saved Largest
 * (draft-16 9.16.2.1); that range is never widened or resolved again here.
 * The joined subscription is resolved ONCE, and only to the retained
 * identity its SUBSCRIBE_OK answered for: a track whose retained identity has
 * been replaced since is no longer that subscription's, so the join is
 * refused rather than answered from the replacement. The fetch is authorized
 * against that track's full name (fail-closed).
 *
 * Through an upstream subscription that kept its track key and carries the
 * track's source generation, the join is one bounded upstream transaction:
 * the source answers the whole range. Any other join -- a track that arrived
 * by PUBLISH, an upstream subscription that could not keep its key, a track
 * with no live source, one another shard owns -- is answered as a standalone
 * FETCH of that range is (bind_standalone_request): retained Objects from the
 * cache, each run the cache does not hold confirmed with the track's live
 * source within the request's wait budget, and a run there is no source to
 * confirm marked Unknown (9.16.3). A missing upstream subscription is not
 * evidence that the track does not exist. */
void
fwd_admit(moqr_bind_t *b, b_conn_t *cn, const moq_fetch_request_event_t *fq,
          const moq_fetch_request_options_t *fo, uint64_t now_us)
{
    /* 1. Resolve the joined subscription to its retained identity. */
    const b_dsub_t *ds = NULL;
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used &&
            cn->dsubs[i].sub_raw == fq->joining_sub._opaque) {
            ds = &cn->dsubs[i];
            break;
        }
    }
    moqr_track_t track = { 0 };
    uint64_t track_gen = 0;
    bool resolved = ds != NULL && ds->has_ident && moqr_sub_is_valid(ds->rsub) &&
                    moqr_core_sub_track(b->core, ds->rsub, &track, &track_gen) ==
                        MOQR_OK &&
                    moqr_track_eq(track, ds->ident_track);
    /* Its full name, borrowed from the core until its next mutating call. */
    moq_bytes_t kparts[32];
    moqr_ns_t kns = { NULL, 0 };
    moq_bytes_t kname = { NULL, 0 };
    if (resolved &&
        moqr_core_track_key(b->core, track, track_gen, kparts, 32, &kns,
                            &kname) != MOQR_OK) {
        resolved = false;
    }
    /* 2. Authorize on the RESOLVED identity (fail-closed). An unresolvable
     * join is authorized on the empty identity, so the request's existence
     * is never a pre-auth oracle. */
    uint64_t ferr = 0;
    uint64_t ftkt = 0;
    if (bind_authorize(b, cn, MOQR_AUTH_FETCH, resolved ? kns : (moqr_ns_t){ NULL, 0 },
                       resolved ? kname : (moq_bytes_t){ NULL, 0 }, fq->tokens,
                       fq->token_count, now_us, &ferr, &ftkt, NULL) !=
        BIND_AUTH_ALLOW) {
        bind_fetch_refuse(b, cn, fq->fetch,
                          ferr != 0 ? ferr : MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
        return;
    }
    if (!resolved) {
        bind_fetch_refuse(b, cn, fq->fetch,
                          MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID, now_us);
        return;
    }
    b_conn_t *ucn = NULL;
    const b_usub_t *us = NULL;
    for (uint32_t ci = 0; ci < b->max_conns && us == NULL; ci++) {
        b_conn_t *c = &b->conns[ci];
        if (!c->used || c->closed || c->session == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (c->usubs[i].used && c->usubs[i].has_key &&
                moqr_track_eq(c->usubs[i].track, track) &&
                c->usubs[i].track_gen == track_gen) {
                ucn = c;
                us = &c->usubs[i];
                break;
            }
        }
    }
    if (us == NULL) {
        moqr_fetch_req_t freq;
        moqr_fetch_req_init(&freq);
        freq.ns = kns;
        freq.name = kname;
        freq.start_group = fq->start_group;
        freq.start_object = fq->start_object;
        freq.end_group = fq->end_group;
        freq.end_object = fq->end_object;
        freq.group_order = MOQR_GROUP_ORDER_ASCENDING;
        freq.subscriber_priority = fq->subscriber_priority;
        freq.cookie = fq->fetch._opaque;
        bind_standalone_request(b, cn, fq, &freq, fo, &track, now_us);
        FWD_DBG(b, joins_retrieved);
        return;
    }
    /* 3. A transaction slot. */
    b_fwd_t *t = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (!b->fwd[i].used) {
            t = &b->fwd[i];
            break;
        }
    }
    if (t == NULL) {
        /* The transaction table is a bounded pool: a capacity refusal. */
        bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                              MOQR_ERR_CAPACITY, now_us);
        FWD_DBG(b, rejected_no_slot);
        return;
    }
    memset(t, 0, sizeof(*t));
    t->used = true;
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live++;
#endif
    /* 4. Owned, charged copy of the track key (one buffer; parts + name). */
    if (!fwd_copy(b, t, us->key, us->key_len, &t->key, &t->key_len)) {
        fwd_release(b, t);
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        FWD_DBG(b, rejected_oom);
        return;
    }
    t->ns_count = us->ns_count;
    {
        size_t off = 0;
        for (uint32_t i = 0; i < us->ns_count; i++) {
            t->parts[i] = (moq_bytes_t){ t->key + off, us->parts[i].len };
            off += us->parts[i].len;
        }
        t->name = (moq_bytes_t){ t->key + off, us->name.len };
    }
    t->d_cookie = conn_cookie(b, cn);
    t->d_binding = cn->binding;
    t->sfetch = fq->fetch;
    t->u_cookie = conn_cookie(b, ucn);
    t->u_binding = ucn->binding;
    t->track = track;
    t->track_gen = track_gen;
    t->start_group = fq->start_group;
    t->start_object = fq->start_object;
    t->end_group = fq->end_group;
    t->end_object = fq->end_object;
    t->has_prio = true;
    t->prio = fq->subscriber_priority;
    t->u = FW_U_UNSENT;
    t->d = FW_D_PENDING;
    FWD_DBG(b, admitted);
}


/* An item refused by the counts or the pool: the collection is abandoned
 * before OK (overflow terminal) and the upstream request cancelled. */
static void
fwd_overflow(moqr_bind_t *b, b_fwd_t *t)
{
    (void)b;
    if (t->consumer == FW_CONSUMER_FILL) {
        t->failure = MOQR_RETRIEVE_LOCAL;
        t->local_result = MOQR_ERR_CAPACITY;
    }
    fwd_end_up(t, FW_END_OVERFLOW);
    FWD_DBG(b, overflow);
}


/* Whether (group, object) lies inside the transaction's normalized range
 * [start, end) -- end_object 0 meaning "through the end of end_group". */
static bool
fwd_in_range(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    if (group < t->start_group ||
        (group == t->start_group && object < t->start_object)) {
        return false;
    }
    if (t->end_object == 0) {
        return group <= t->end_group;
    }
    return group < t->end_group ||
           (group == t->end_group && object < t->end_object);
}


/* Whether an upstream FETCH_OK's End Location stays within the request's
 * end (same end_object 0 convention on both sides). */
static bool
fwd_end_within(const b_fwd_t *t, uint64_t end_group, uint64_t end_object)
{
    if (t->end_object == 0) {
        return end_group <= t->end_group;
    }
    if (end_object == 0) {
        return end_group < t->end_group;
    }
    return end_group < t->end_group ||
           (end_group == t->end_group && end_object <= t->end_object);
}


/* The upstream answer does not fit the pinned range: abandoned before OK
 * (never relayed as a narrower or wider success) and cancelled upstream. */
static void
fwd_mismatch(moqr_bind_t *b, b_fwd_t *t)
{
    (void)b;
    if (t->consumer == FW_CONSUMER_FILL) {
        t->failure = MOQR_RETRIEVE_MALFORMED;
        t->local_result = MOQR_ERR_INVAL;
    }
    fwd_end_up(t, FW_END_MISMATCH);
    FWD_DBG(b, mismatch);
}


static bool
retrieve_response_position(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    moqr_retrieve_location_t at = retrieve_loc(group, object);
    if (!fwd_in_range(t, group, object) ||
        (t->ok_seen && retrieve_loc_cmp(at, retrieve_inclusive_end(
             t->ok_end_group, t->ok_end_object)) > 0)) return false;
    if (t->n_items != 0) {
        const b_fwd_item_t *last = &t->items[t->n_items - 1u];
        if (retrieve_loc_cmp(at, retrieve_loc(last->group, last->object)) <= 0) {
            return false;
        }
    }
    return true;
}


static bool
retrieve_response_end(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    moqr_retrieve_location_t end = retrieve_inclusive_end(group, object);
    if (!fwd_end_within(t, group, object) ||
        retrieve_loc_cmp(end, retrieve_loc(t->start_group, t->start_object)) < 0) {
        return false;
    }
    if (t->n_items != 0) {
        const b_fwd_item_t *last = &t->items[t->n_items - 1u];
        if (retrieve_loc_cmp(end, retrieve_loc(last->group, last->object)) < 0) {
            return false;
        }
    }
    return true;
}


/* Fetcher-role events on an upstream connection. Returns true when the event
 * belonged to a transaction (its owned references, if any, were taken). An
 * event naming no live transaction handle is left for moq_event_cleanup. */
bool
fwd_on_up_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev, uint64_t now_us)
{
    uint64_t h;
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_OK:       h = ev->u.fetch_ok.fetch._opaque; break;
    case MOQ_EVENT_FETCH_OBJECT:   h = ev->u.fetch_object.fetch._opaque; break;
    case MOQ_EVENT_FETCH_GAP:      h = ev->u.fetch_gap.fetch._opaque; break;
    case MOQ_EVENT_FETCH_COMPLETE: h = ev->u.fetch_complete.fetch._opaque; break;
    case MOQ_EVENT_FETCH_ERROR:    h = ev->u.fetch_error.fetch._opaque; break;
    case MOQ_EVENT_FETCH_RESET:    h = ev->u.fetch_reset.fetch._opaque; break;
    default:                       return false;
    }
    b_fwd_t *t = fwd_find_up(b, cn, h);
    if (t == NULL) {
        FWD_DBG(b, stale_events_ignored);
        return false;   /* retired handle: cleaned by the pump, never matched */
    }
    if (t->consumer == FW_CONSUMER_FILL) {
        if (t->end == FW_END_NONE && !retrieve_fwd_current(b, t)) {
            fwd_end_up(t, FW_END_LOST);
        }
        retrieve_fwd_expire(t, now_us);
    }
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_OK: {
        const moq_fetch_ok_event_t *ok = &ev->u.fetch_ok;
        if (t->u != FW_U_PENDING || t->ok_seen) {
            if (t->consumer == FW_CONSUMER_FILL && t->u == FW_U_PENDING) {
                fwd_mismatch(b, t);
            }
            FWD_DBG(b, drained_events);
            return true;
        }
        if (!fwd_end_within(t, ok->end_group, ok->end_object) ||
            (t->consumer == FW_CONSUMER_FILL &&
             !retrieve_response_end(t, ok->end_group, ok->end_object))) {
            fwd_mismatch(b, t);
            return true;
        }
        moqr_bind_meta_ref_t input = bind_meta_input(cn, h, t->track_gen,
                                                     ok->track_properties);
        if (t->consumer == FW_CONSUMER_FILL ||
            fwd_copy(b, t, ok->track_properties.data,
                     ok->track_properties.len, &t->props, &t->props_len)) {
            t->props_codec = bind_props_codec(cn);
            t->ok_seen = true;
            t->ok_eot = ok->end_of_track;
            t->ok_end_group = ok->end_group;
            t->ok_end_object = ok->end_object;
        } else {
            fwd_overflow(b, t);
        }
        /* Fill has no second ACK and needs no transaction property copy;
         * the separate bounded directory owns its newest complete set.
         * A lawful new complete set replaces the directory even if this
         * separate Joining FETCH cannot retain its transaction. Its owned
         * reply remains unchanged. A failed directory copy publishes
         * UNAVAILABLE; a stale source generation cannot update a new source.
         * This callback precedes processing any following FETCH objects. */
        (void)moqr_bind_meta_replace(b, t->track, t->track_gen, &input,
                                     now_us, NULL);
        return true;
    }
    case MOQ_EVENT_FETCH_OBJECT: {
        moq_fetch_object_event_t *o = &ev->u.fetch_object;
        if (t->u != FW_U_PENDING) {
            FWD_DBG(b, drained_events);
            return true;   /* draining a cancelled/overflowed response */
        }
        if (!fwd_in_range(t, o->group_id, o->object_id) ||
            (t->consumer == FW_CONSUMER_FILL &&
             !retrieve_response_position(t, o->group_id, o->object_id))) {
            fwd_mismatch(b, t);
            return true;
        }
        if (t->n_items >= BF_FWD_MAX_ITEMS || t->n_objects >= BF_FWD_MAX_OBJECTS) {
            fwd_overflow(b, t);
            return true;
        }
        b_fwd_item_t *it = &t->items[t->n_items];
        memset(it, 0, sizeof(*it));
        if (t->consumer == FW_CONSUMER_FILL) {
            moqr_result_t rc = retrieve_copy_object(b, t, it, o);
            if (rc != MOQR_OK) {
                fwd_overflow(b, t);
                t->local_result = rc;
                return true;
            }
        } else {
            uint64_t bytes = (o->payload != NULL ? moq_rcbuf_len(o->payload) : 0) +
                             (o->properties != NULL ? moq_rcbuf_len(o->properties) : 0);
            if (!fwd_charge(b, t, bytes)) {
                fwd_overflow(b, t);
                return true;
            }
            it->payload = o->payload;
            it->properties = o->properties;
            it->charged = bytes;
            o->payload = NULL;
            o->properties = NULL;
        }
        it->group = o->group_id;
        it->subgroup = o->subgroup_id;
        it->object = o->object_id;
        it->properties_codec = bind_props_codec(cn);
        it->publisher_priority = o->publisher_priority;
        it->datagram = o->datagram;
        t->n_items++;
        t->n_objects++;
        return true;
    }
    case MOQ_EVENT_FETCH_GAP: {
        const moq_fetch_gap_event_t *g = &ev->u.fetch_gap;
        if (t->u != FW_U_PENDING) {
            FWD_DBG(b, drained_events);
            return true;
        }
        if (t->consumer == FW_CONSUMER_FILL &&
            (!retrieve_response_position(t, g->group_id, g->object_id) ||
             (g->range_kind != MOQ_FETCH_RANGE_NON_EXISTENT &&
              g->range_kind != MOQ_FETCH_RANGE_UNKNOWN &&
              g->range_kind != MOQ_FETCH_RANGE_TIMED_OUT))) {
            fwd_mismatch(b, t);
            return true;
        }
        if (t->n_items >= BF_FWD_MAX_ITEMS || t->n_gaps >= BF_FWD_MAX_GAPS) {
            fwd_overflow(b, t);
            return true;
        }
        b_fwd_item_t *it = &t->items[t->n_items++];
        memset(it, 0, sizeof(*it));
        it->is_gap = true;
        it->range_kind = (uint8_t)g->range_kind;
        it->group = g->group_id;
        it->object = g->object_id;
        t->n_gaps++;
        return true;
    }
    case MOQ_EVENT_FETCH_COMPLETE:
        /* The session frees the fetcher entry with this terminal. */
        t->ufetch_live = false;
        if (t->u == FW_U_PENDING && t->end == FW_END_NONE) {
            t->end = t->ok_seen ? FW_END_COMPLETE : FW_END_ERROR;
            if (!t->ok_seen) {
                t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                if (t->consumer == FW_CONSUMER_FILL) {
                    t->failure = MOQR_RETRIEVE_MALFORMED;
                    t->local_result = MOQR_ERR_INVAL;
                }
            }
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, completed);
        return true;
    case MOQ_EVENT_FETCH_ERROR: {
        const moq_fetch_error_event_t *e = &ev->u.fetch_error;
        t->ufetch_live = false;
        if (t->end == FW_END_NONE) {
            t->end = FW_END_ERROR;
            if (fwd_copy(b, t, e->reason.data, e->reason.len, &t->reason,
                         &t->reason_len)) {
                t->err_code = e->error_code;
                t->err_can_retry = e->can_retry;
                t->err_retry_after_ms = e->retry_after_ms;
            } else {
                /* The complete upstream error cannot be retained: answer
                 * with the relay's own failure, never a trimmed copy. */
                t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                t->err_can_retry = false;
                t->err_retry_after_ms = 0;
                t->reason = NULL;
                t->reason_len = 0;
                if (t->consumer == FW_CONSUMER_FILL) {
                    t->failure = MOQR_RETRIEVE_LOCAL;
                    t->local_result = MOQR_ERR_CAPACITY;
                    t->err_code = e->error_code;
                }
                FWD_DBG(b, reason_unretained);
            }
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, errors);
        return true;
    }
    case MOQ_EVENT_FETCH_RESET:
        t->ufetch_live = false;
        if (t->end == FW_END_NONE) {
            t->end = FW_END_RESET;
            t->reset_code = ev->u.fetch_reset.error_code;
            t->reset_cause = ev->u.fetch_reset.cause;
            t->reset_data_stream = ev->u.fetch_reset.data_stream;
            t->reset_stop_sending = ev->u.fetch_reset.stop_sending;
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, resets);
        return true;
    default:
        return false;
    }
}


/* Publisher-role: the downstream subscriber cancelled its (forwarded) fetch.
 * The session already retired its entry and reset any open data stream. */
bool
fwd_on_down_cancelled(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    b_fwd_t *t = fwd_find_down(b, cn, handle);
    if (t == NULL) {
        return false;
    }
    t->d = FW_D_TERMINAL;
    fwd_end_up(t, FW_END_CANCELLED);
    FWD_DBG(b, cancelled_down);
    return true;
}


bool
fwd_result_is_gone(moq_result_t rc)
{
    return rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_CLOSED;
}


/* Drive every transaction as far as the sessions allow. Each step HOLDs on
 * WOULD_BLOCK with no state advance and is retried by the next pump; no step
 * loops on a refusal. */
void
bind_fwd_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used) {
            continue;
        }
        b_conn_t *pending_up = conn_by_cookie(b, t->u_cookie);
        b_conn_t *pending_down = conn_by_cookie(b, t->d_cookie);
        if ((pending_up != NULL && pending_up->events_pending) ||
            (pending_down != NULL && pending_down->events_pending)) continue;
        /* -- upstream side -- */
        if (t->consumer == FW_CONSUMER_FILL) {
            if (t->end == FW_END_NONE && !retrieve_fwd_current(b, t)) {
                fwd_end_up(t, FW_END_LOST);
            }
            retrieve_fwd_expire(t, now_us);
        }
        if (t->u == FW_U_UNSENT) {
            b_conn_t *ucn = fwd_conn(b, t->u_cookie, t->u_binding);
            if (ucn == NULL) {
                t->end = t->end == FW_END_NONE ? FW_END_LOST : t->end;
                t->u = FW_U_TERMINAL;
                FWD_DBG(b, lost);
            } else {
                moq_fetch_cfg_t fc;
                /* D22 section 9.20 parameters apply to this hop. Enforce the
                 * consumer's absolute deadline locally rather than forward
                 * FILL_TIMEOUT; expiry above prevents an unsent dispatch
                 * and retains cancellation of any request already sent. */
                moq_fetch_cfg_init(&fc);
                fc.track_namespace = (moq_namespace_t){ .parts = t->parts,
                                                        .count = t->ns_count };
                fc.track_name = t->name;
                fc.start_group = t->start_group;
                fc.start_object = t->start_object;
                fc.end_group = t->end_group;
                fc.end_object = t->end_object;
                /* A retrieval asks in its consumer's order; each span lies in
                 * one group whenever that order is descending. */
                fc.group_order = t->consumer == FW_CONSUMER_FILL &&
                        t->retrieval.group_order == MOQR_GROUP_ORDER_DESCENDING
                    ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING;
                fc.has_subscriber_priority = t->has_prio;
                fc.subscriber_priority = t->prio;
                moq_result_t rc = moq_session_fetch(ucn->session, &fc, now_us,
                                                    &t->ufetch);
                if (rc == MOQ_OK) {
                    t->ufetch_live = true;
                    t->u = FW_U_PENDING;
                } else if (rc == MOQ_ERR_WOULD_BLOCK ||
                           rc == MOQ_ERR_REQUEST_BLOCKED) {
                    FWD_DBG(b, retried_send);   /* HOLD */
                } else {
                    t->end = t->end == FW_END_NONE ? FW_END_ERROR : t->end;
                    t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                    if (t->consumer == FW_CONSUMER_FILL) {
                        t->failure = MOQR_RETRIEVE_SESSION;
                        t->session_result = rc;
                    }
                    t->u = FW_U_TERMINAL;
                    b->session_errors++;
                }
            }
        }
        if (t->u == FW_U_CANCELLING) {
            b_conn_t *ucn = fwd_conn(b, t->u_cookie, t->u_binding);
            if (ucn == NULL || !t->ufetch_live) {
                t->u = FW_U_TERMINAL;
                t->ufetch_live = false;
            } else {
                moq_result_t rc = moq_session_fetch_cancel(ucn->session,
                                                           t->ufetch, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_cancel);   /* HOLD: ownership retained */
                } else if (t->consumer == FW_CONSUMER_FILL && rc != MOQ_OK &&
                           !fwd_result_is_gone(rc)) {
                    /* A hard local refusal is not proof the upstream request
                     * retired. Keep this bounded slot and retry cancellation;
                     * neither consumer teardown nor slot reuse may erase it. */
                    if (t->session_result == MOQ_OK) {
                        b->session_errors++;
                        t->session_result = rc;
                    }
                    if (t->end == FW_END_TIMEOUT) {
                        t->end = FW_END_ERROR;
                        t->failure = MOQR_RETRIEVE_SESSION;
                    }
                } else {
                    /* Committed, already gone, or a local failure that can
                     * never cancel it: the upstream session is then closed
                     * (bind_cancel_settled), so the FETCH cannot go on. */
                    (void)bind_cancel_settled(b, ucn, rc, "fetch cancellation failed",
                                              now_us);
                    t->u = FW_U_TERMINAL;
                    t->ufetch_live = false;
                    FWD_DBG(b, cancelled_up);
                }
            }
        }
        if (t->consumer == FW_CONSUMER_FILL) {
            if (t->u == FW_U_TERMINAL && t->d == FW_D_TERMINAL) {
                fwd_release(b, t);
            }
            continue; /* the private cursor owns replay, not a session FETCH */
        }
        /* -- downstream side -- */
        b_conn_t *dcn = t->d != FW_D_TERMINAL
                            ? fwd_conn(b, t->d_cookie, t->d_binding) : NULL;
        if (t->d != FW_D_TERMINAL && dcn == NULL) {
            t->d = FW_D_TERMINAL;   /* its session is gone: nothing to write */
            fwd_end_up(t, FW_END_CANCELLED);
            FWD_DBG(b, down_lost);
        }
        for (uint32_t guard = 0; guard < 256 && t->d != FW_D_TERMINAL; guard++) {
            if (t->d == FW_D_PENDING) {
                if (t->u != FW_U_TERMINAL) {
                    break;   /* still collecting */
                }
                if (t->end == FW_END_COMPLETE && t->ok_seen && !t->malformed) {
                    t->d = FW_D_ACCEPTING;
                    continue;
                }
                moq_reject_fetch_cfg_t rj;
                moq_reject_fetch_cfg_init(&rj);
                if (t->malformed) {
                    rj.error_code = MOQ_REQUEST_ERROR_MALFORMED_TRACK;
                } else if (t->end == FW_END_ERROR) {
                    rj.error_code = t->err_code;
                    rj.can_retry = t->err_can_retry;
                    rj.retry_after_ms = t->err_retry_after_ms;
                    rj.reason = (moq_bytes_t){ t->reason, t->reason_len };
                } else {
                    rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                }
                moq_result_t rc = moq_session_reject_fetch(dcn->session,
                                                           t->sfetch, &rj,
                                                           now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_reject);
                    break;
                }
                if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
                    b->session_errors++;
                }
                t->d = FW_D_TERMINAL;
                FWD_DBG(b, rejected_down);
                break;
            }
            if (t->d == FW_D_ACCEPTING && t->malformed) {
                t->d = FW_D_PENDING;   /* not accepted yet: refused instead */
                continue;
            }
            if (t->d == FW_D_STREAMING && t->malformed) {
                t->d = FW_D_ABORTING;  /* accepted: reset, never FINed */
                continue;
            }
            if (t->d == FW_D_ACCEPTING) {
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_of_track = t->ok_eot;
                ac.end_group = t->ok_end_group;
                ac.end_object = t->ok_end_object;
                ac.empty = (t->n_items == 0);
                b_props_tmp_t ptmp = { NULL, 0 };
                if (!bind_track_props_out(b, t->props_codec, bind_props_codec(dcn),
                                          (moq_bytes_t){ t->props, t->props_len },
                                          &ac.track_properties, &ptmp)) {
                    /* The upstream's Track Properties cannot be stated on
                     * this downstream's draft: nothing reached the wire, so
                     * the request is refused rather than answered without
                     * them. */
                    t->end = FW_END_ERROR;
                    t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                    t->err_can_retry = false;
                    t->err_retry_after_ms = 0;
                    t->d = FW_D_PENDING;
                    continue;
                }
                moq_result_t rc = moq_session_accept_fetch(dcn->session,
                                                           t->sfetch, &ac,
                                                           now_us);
                bind_track_props_release(b, &ptmp);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_accept);
                    break;
                }
                if (fwd_result_is_gone(rc)) {
                    t->d = FW_D_TERMINAL;   /* peer cancelled / session closed */
                    FWD_DBG(b, down_gone);
                    break;
                }
                if (rc != MOQ_OK) {
                    /* Locally unencodable before anything reached the wire:
                     * the request is still open downstream, so it is answered
                     * with a rejection rather than left pending. */
                    b->session_errors++;
                    t->end = FW_END_ERROR;
                    t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                    t->err_can_retry = false;
                    t->err_retry_after_ms = 0;
                    t->d = FW_D_PENDING;
                    continue;
                }
                t->d = ac.empty ? FW_D_TERMINAL : FW_D_STREAMING;
                if (ac.empty) {
                    FWD_DBG(b, delivered);
                }
                continue;
            }
            if (t->d == FW_D_STREAMING) {
                if (t->next_write < t->n_items) {
                    b_fwd_item_t *it = &t->items[t->next_write];
                    moq_result_t rc;
                    if (it->is_gap) {
                        rc = moq_session_write_fetch_range(
                            dcn->session, t->sfetch,
                            (moq_fetch_range_kind_t)it->range_kind, it->group,
                            it->object, now_us);
                    } else {
                        moq_fetch_object_cfg_t oc;
                        moq_fetch_object_cfg_init(&oc);
                        oc.group_id = it->group;
                        oc.subgroup_id = it->subgroup;
                        oc.object_id = it->object;
                        oc.publisher_priority = it->publisher_priority;
                        oc.datagram = moq_session_supports_fetch_datagram(
                                          dcn->session) ? it->datagram : false;
                        oc.payload = it->payload;
                        moq_rcbuf_t *props_own = NULL;
                        rc = bind_props_out(b, dcn, it->properties, it->properties_codec,
                                            &oc.properties, &props_own);
#ifdef MOQR_BIND_TESTING
                        if (rc == MOQ_OK && atomic_load(&bind_dbg_fail_fwd_writes) > 0 &&
                            atomic_fetch_sub(&bind_dbg_fail_fwd_writes, 1) == 1) {
                            rc = MOQ_ERR_INVAL;   /* injected: unencodable */
                        } else
#endif
                        if (rc == MOQ_OK) {
                            rc = moq_session_write_fetch_object(
                                dcn->session, t->sfetch, &oc, now_us);
                        }
                        if (props_own != NULL) moq_rcbuf_decref(props_own);
                    }
                    if (rc == MOQ_ERR_WOULD_BLOCK) {
                        FWD_DBG(b, retried_write);
                        break;   /* HOLD: the item is kept, retried as is */
                    }
                    if (rc == MOQ_OK) {
                        fwd_item_release(b, t, it);
                        t->next_write++;
                        b->deliveries_written++;
                        continue;
                    }
                    if (fwd_result_is_gone(rc)) {
                        t->d = FW_D_TERMINAL;
                        FWD_DBG(b, down_gone);
                        break;
                    }
                    t->d = FW_D_ABORTING;   /* locally unencodable after OK */
                    continue;
                }
                moq_result_t rc = moq_session_end_fetch(dcn->session, t->sfetch,
                                                        now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_fin);
                    break;
                }
                if (rc == MOQ_OK) {
                    t->d = FW_D_TERMINAL;
                    FWD_DBG(b, delivered);
                    break;
                }
                if (fwd_result_is_gone(rc)) {
                    t->d = FW_D_TERMINAL;
                    FWD_DBG(b, down_gone);
                    break;
                }
                t->d = FW_D_ABORTING;
                continue;
            }
            if (t->d == FW_D_ABORTING) {
                moq_result_t rc = moq_session_abort_fetch(dcn->session, t->sfetch,
                                                          t->malformed
                                                              ? t->malformed_wire
                                                              : BIND_RESET_INTERNAL,
                                                          now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_abort);
                    break;
                }
                if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
                    b->session_errors++;
                }
                t->d = FW_D_TERMINAL;
                FWD_DBG(b, aborted);
                break;
            }
        }
        if (t->u == FW_U_TERMINAL && t->d == FW_D_TERMINAL) {
            fwd_release(b, t);
        }
    }
}
