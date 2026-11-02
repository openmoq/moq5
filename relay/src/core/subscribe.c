#include "relay_internal.h"
#include "subscribe_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

bool
moqr_sub_is_valid(moqr_sub_t h)
{
    return moq_handle_pool_tag(h._opaque) == MOQR_HANDLE_POOL_SUB &&
           moq_handle_session_tag(h._opaque) != 0 &&
           (moq_handle_generation(h._opaque) & 1u) != 0;
}


bool
moqr_sub_eq(moqr_sub_t a, moqr_sub_t b)
{
    return a._opaque == b._opaque;
}


r_sub_t *
sub_resolve(moqr_core_t *c, moqr_sub_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_SUB ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_subs ||
        c->subs[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->subs[slot];
}


/* -- ops: subscribe ------------------------------------------------------------------------------- */

/* The sizeof of the layout through `forward`: what a caller built before
 * range_end_holds was appended allocates and passes. */
#define MOQR_SUBSCRIBE_REQ_SIZE_THROUGH_FORWARD                               \
    ((offsetof(moqr_subscribe_req_t, forward) +                               \
      sizeof(((moqr_subscribe_req_t *)0)->forward) +                          \
      (_Alignof(moqr_subscribe_req_t) - 1)) &                                 \
     ~(_Alignof(moqr_subscribe_req_t) - 1))


/* Clear and stamp `n` bytes (at least through `forward`) and set the defaults,
 * every one of which lies inside that layout. */
static void
subscribe_req_clear(moqr_subscribe_req_t *req, size_t n)
{
    memset(req, 0, n);
    req->struct_size = (uint32_t)n;
    req->filter.type = MOQR_FILTER_LARGEST_OBJECT;
    req->forward = true;   /* deliver unless the request explicitly pauses */
    req->subscriber_priority = 128;
}


/* Parenthesized: defines the symbol, not the header's sized macro. */
void
(moqr_subscribe_req_init)(moqr_subscribe_req_t *req)
{
    if (req == NULL) {
        return;
    }
    subscribe_req_clear(req, MOQR_SUBSCRIBE_REQ_SIZE_THROUGH_FORWARD);
}


void
moqr_subscribe_req_init_sized(moqr_subscribe_req_t *req, size_t req_size)
{
    /* Anything shorter than the layout through `forward` is not a request
     * the relay accepts, and could not hold the defaults. */
    if (req == NULL || req_size < MOQR_SUBSCRIBE_REQ_SIZE_THROUGH_FORWARD) {
        return;
    }
    subscribe_req_clear(req, req_size < sizeof(*req) ? req_size : sizeof(*req));
}


/* A free subscription slot, leaving the ones reserved for later requests. */
static uint32_t
sub_slot_find(moqr_core_t *c)
{
    uint32_t found = UINT32_MAX, free_slots = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if (r_gen_openable(c->subs[i].gen)) {
            if (found == UINT32_MAX) found = i;
            if (c->subs_reserved == 0) return found;
            if (++free_slots > c->subs_reserved) return found;
        }
    }
    return UINT32_MAX;
}


moqr_result_t
moqr_core_internal_sub_reserve(moqr_core_t *c)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (sub_slot_find(c) == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_SUBS);
    }
    c->subs_reserved++;
    return MOQR_OK;
}


void
moqr_core_internal_sub_release_reserve(moqr_core_t *c)
{
    if (c != NULL && c->subs_reserved > 0) {
        c->subs_reserved--;
    }
}


/* Create a sourceless track holding one rendezvous subscription (§9.20.6).
 * No intent: the request is answered when a publisher appears (ordinary
 * admission) or at its deadline (TIMEOUT). */
static moqr_result_t
rendezvous_hold(moqr_core_t *c, uint32_t bslot, r_binding_t *b,
                const moqr_subscribe_req_t *req, uint64_t hash,
                bool forward, bool holds,
                const moq_location_filter_cfg_t *location, uint64_t deadline_us,
                moqr_sub_t *out)
{
    uint32_t sslot = sub_slot_find(c);
    if (sslot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_SUBS);
    }
    uint32_t tslot = UINT32_MAX;
    moqr_result_t rc = track_create(c, req->ns, req->name, hash, &tslot);
    if (rc != MOQR_OK) {
        return rc;
    }
    r_track_t *t = &c->tracks[tslot];
    t->state = R_TRACK_RENDEZVOUS;
    t->source_kind = R_TRACK_SOURCE_NONE;
    r_sub_t *s = &c->subs[sslot];
    uint32_t gen = r_gen_open(s->gen);
    memset(s, 0, sizeof(*s));
    s->gen = gen;
    s->binding = bslot;
    s->binding_gen = b->gen;
    s->cluster_requester_hop = b->peer_hop_id;
    s->cluster_requester_correlation = b->correlation_id;
    s->track = tslot;
    s->track_gen_slot = t->gen;
    s->state = R_SUB_PARKED;
    s->forward = forward;
    s->range_end_holds = holds;
    s->subscriber_priority = req->subscriber_priority;
    s->group_order = req->group_order;
    s->cookie = req->cookie;
    s->filter_type = req->filter.type;
    s->start_group = req->filter.start_group;
    s->start_object = req->filter.start_object;
    s->end_group = req->filter.end_group_delta;
    s->rendezvous = true;
    s->rendezvous_deadline_us = deadline_us;
    if (location != NULL) {
        s->has_location = true;
        s->location = *location;
    }
    rc = gpos_alloc(c, s, t->log);
    if (rc != MOQR_OK) {
        s->gen = r_gen_close(s->gen);
        track_free_slot(c, tslot);
        return rc;
    }
    sub_list_push(c, bslot, sslot);
    track_sub_link(c, tslot, sslot);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 5 /* rendezvous hold */, tslot, bslot,
            req->cookie, deadline_us);
    *out = sub_handle(c, sslot);
    return MOQR_OK;
}


static moqr_result_t core_subscribe(moqr_core_t *c, moqr_binding_t bh,
                                    const moqr_subscribe_req_t *req,
                                    moqr_sub_t *out);


moqr_result_t
moqr_core_subscribe(moqr_core_t *c, moqr_binding_t bh,
                    const moqr_subscribe_req_t *req, moqr_sub_t *out)
{
    /* A request made with a reservation takes the slot it held: released for
     * this call, and held again when the request fails. */
    bool reserved = c != NULL && req != NULL &&
        req->struct_size >= offsetof(moqr_subscribe_req_t, reserved) +
                                sizeof(req->reserved) &&
        req->reserved && c->subs_reserved > 0;
    if (reserved) c->subs_reserved--;
    moqr_result_t rc = core_subscribe(c, bh, req, out);
    if (reserved && rc != MOQR_OK) c->subs_reserved++;
    return rc;
}


moqr_result_t
moqr_core_internal_subscribe_cluster(moqr_core_t *c, moqr_binding_t bh,
    const moqr_subscribe_req_t *req, moqr_cluster_advertisement_id_t source,
    uint64_t requester_hop_id,
    uint64_t requester_correlation_id, moqr_sub_t *out)
{
    if (c == NULL || !c->cluster_enabled || source.serial == 0 || req == NULL ||
        req->struct_size < offsetof(moqr_subscribe_req_t, forward) ||
        !ftn_view_ok(req->ns, req->name)) return MOQR_ERR_INVAL;
    if (cluster_owner_node(c, source, req->ns) == UINT32_MAX)
        return MOQR_ERR_STALE_HANDLE;
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    uint64_t hop = b->peer_hop_id, corr = b->correlation_id;
    b->peer_hop_id = requester_hop_id;
    b->correlation_id = requester_correlation_id;
    c->cluster_forced = source;
    moqr_result_t rc = moqr_core_subscribe(c, bh, req, out);
    memset(&c->cluster_forced, 0, sizeof(c->cluster_forced));
    b->peer_hop_id = hop;
    b->correlation_id = corr;
    return rc;
}


static moqr_result_t
core_subscribe(moqr_core_t *c, moqr_binding_t bh,
               const moqr_subscribe_req_t *req, moqr_sub_t *out)
{
    /* Prefix-size (ABI) contract: `forward` is a trailing field, so a caller
     * that predates it (struct_size ending before `forward`) is still valid and
     * defaults to forward=true. The minimum is everything up to `forward`. */
    if (c == NULL || req == NULL || out == NULL ||
        req->struct_size < offsetof(moqr_subscribe_req_t, forward) ||
        !ftn_view_ok(req->ns, req->name)) {
        return MOQR_ERR_INVAL;
    }
    const moq_location_filter_cfg_t *location = NULL;
    if (req->struct_size >= offsetof(moqr_subscribe_req_t, location) +
                                sizeof(req->location)) {
        location = req->location;
    }
    if (location != NULL ? !location_valid(location)
                         : (req->filter.type < MOQR_FILTER_NEXT_GROUP_START ||
                            req->filter.type > MOQR_FILTER_RELATIVE_START)) {
        return MOQR_ERR_INVAL;
    }
    bool req_forward = true;   /* default when the caller omits the field */
    if (req->struct_size >=
        offsetof(moqr_subscribe_req_t, forward) + sizeof(req->forward)) {
        req_forward = req->forward;
    }
    bool req_holds = false;    /* default when the caller omits the field */
    if (req->struct_size >= offsetof(moqr_subscribe_req_t, range_end_holds) +
                                sizeof(req->range_end_holds)) {
        req_holds = req->range_end_holds;
    }
    /* Read as one block: a torn tail never holds a request. */
    bool req_wait = false;
    uint64_t req_deadline = 0;
    if (req->struct_size >= offsetof(moqr_subscribe_req_t, rendezvous) +
                                sizeof(req->rendezvous)) {
        req_wait = req->rendezvous;
        req_deadline = req->rendezvous_deadline_us;
    }
    if (location == NULL && req->filter.type == MOQR_FILTER_ABSOLUTE_RANGE &&
        req->filter.start_group + req->filter.end_group_delta <
            req->filter.start_group) {
        return MOQR_ERR_INVAL;   /* end-group overflow (spec: violation) */
    }
    *out = MOQR_SUB_INVALID;
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 2)) {   /* worst case: ACCEPT + UPSTREAM_SUBSCRIBE */
        return MOQR_ERR_WOULD_BLOCK;
    }

    uint64_t hash = 0;
    uint32_t tslot = (c->cluster_enabled ? cluster_track_lookup(c, req->ns, req->name, bslot, b->gen, &hash) : track_lookup(c, req->ns, req->name, &hash));
    moqr_result_t rc;
    if (tslot != UINT32_MAX && c->cluster_enabled &&
        !cluster_track_eligible(c, &c->tracks[tslot], bslot, b->gen) &&
        track_sub_count(c, tslot) == 0 && track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
        tslot = UINT32_MAX;
    }


    if (tslot == UINT32_MAX) {
        /* New track: needs an announced publisher. */
        uint32_t pub_node = (c->cluster_enabled ? cluster_select_node(c, req->ns, bslot, b->gen) : trie_longest_announce(c, req->ns));
        if (pub_node == UINT32_MAX && req_wait) {
            /* §9.20.6: hold the subscription, without an answer, until a
             * publisher appears or its one deadline passes. */
            return rendezvous_hold(c, bslot, b, req, hash, req_forward,
                                   req_holds, location, req_deadline, out);
        }
        if (pub_node == UINT32_MAX) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
            it->binding_cookie = b->cookie;
            it->cookie = req->cookie;
            it->error_code = R_ERR_DOES_NOT_EXIST;
            return MOQR_OK;
        }
        uint32_t sslot = sub_slot_find(c);
        if (sslot == UINT32_MAX) {
            return r_refuse(c, MOQR_REFUSE_SUBS);
        }
        rc = track_create(c, req->ns, req->name, hash, &tslot);
        if (rc != MOQR_OK) {
            return rc;
        }
        /* Making room may have compacted the trie; announced nodes are
         * never freed, only moved, so look the publisher up again. */
        pub_node = (c->cluster_enabled ? cluster_select_node(c, req->ns, bslot, b->gen) : trie_longest_announce(c, req->ns));
        r_track_t *t = &c->tracks[tslot];
        t->state = R_TRACK_PENDING;
        t->source_kind = R_TRACK_SOURCE_PULL;
        t->track_gen++;
        t->has_upstream_binding = true;
        t->up_binding = c->nodes[pub_node].ann_binding;
        t->up_binding_gen = c->nodes[pub_node].ann_binding_gen;
        track_src_set(c, t, pub_node);
        t->src_ann_binding = c->nodes[pub_node].ann_binding;
        t->src_ann_binding_gen = c->nodes[pub_node].ann_binding_gen;
        cluster_pin_track(c, t);
        t->cluster_requester_hop = b->peer_hop_id;
        t->cluster_requester_correlation = b->correlation_id;

        t->metadata_managed = c->bindings[t->up_binding].metadata_managed;
        r_sub_t *s = &c->subs[sslot];
        uint32_t gen = r_gen_open(s->gen);
        memset(s, 0, sizeof(*s));
        s->gen = gen;
        s->binding = bslot;
        s->binding_gen = b->gen;
    s->cluster_requester_hop = b->peer_hop_id;
    s->cluster_requester_correlation = b->correlation_id;
        s->track = tslot;
        s->track_gen_slot = t->gen;
        s->state = R_SUB_PARKED;
        s->forward = req_forward;   /* initial Forward, carried atomically */
        s->range_end_holds = req_holds;
        s->subscriber_priority = req->subscriber_priority;
        s->group_order = req->group_order;
        s->cookie = req->cookie;
        /* Filter resolves at upstream_ok; remember the request. */
        s->filter_type = req->filter.type;
        s->start_group = req->filter.start_group;
        s->start_object = req->filter.start_object;
        s->end_group = req->filter.end_group_delta;   /* delta until resolve */
        if (location != NULL) {
            s->has_location = true;
            s->location = *location;
        }
        rc = gpos_alloc(c, s, t->log);
        if (rc != MOQR_OK) {
            s->gen = r_gen_close(s->gen);
            track_free_slot(c, tslot);
            return rc;
        }
        sub_list_push(c, bslot, sslot);
        track_sub_link(c, tslot, sslot);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        it->binding_cookie = c->bindings[t->up_binding].cookie;
        it->track = track_handle(c, tslot);
        it->track_gen = t->track_gen;
        intent_set_ns_from_key(it, &t->key);
        r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 0, tslot, t->track_gen,
                bslot, 0);
        *out = sub_handle(c, sslot);
        return MOQR_OK;
    }

    /* Existing track. */
    r_track_t *t = &c->tracks[tslot];
    if (!cluster_track_eligible(c, t, bslot, b->gen) && t->state != R_TRACK_RENDEZVOUS) {
        moqr_intent_t *deny = intent_push(c, MOQR_INTENT_REJECT_SUB);
        deny->binding_cookie = b->cookie;
        deny->cookie = req->cookie;
        deny->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    if (t->state == R_TRACK_RENDEZVOUS && !req_wait) {
        /* Others wait for this track's publisher; this request does not. */
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
        it->binding_cookie = b->cookie;
        it->cookie = req->cookie;
        it->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    uint32_t sslot = sub_slot_find(c);
    if (sslot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_SUBS);
    }
    r_sub_t *s = &c->subs[sslot];
    uint32_t gen = r_gen_open(s->gen);
    memset(s, 0, sizeof(*s));
    s->gen = gen;
    s->binding = bslot;
    s->binding_gen = b->gen;
    s->cluster_requester_hop = b->peer_hop_id;
    s->cluster_requester_correlation = b->correlation_id;
    s->track = tslot;
    s->track_gen_slot = t->gen;
    s->forward = req_forward;   /* initial Forward, carried atomically */
    s->range_end_holds = req_holds;
    s->subscriber_priority = req->subscriber_priority;
    s->group_order = req->group_order;
    s->cookie = req->cookie;
    if (location != NULL) {
        s->has_location = true;
        s->location = *location;
    }
    moqr_result_t crc = gpos_alloc(c, s, t->log);
    if (crc != MOQR_OK) {
        s->gen = r_gen_close(s->gen);
        return crc;
    }
    sub_list_push(c, bslot, sslot);
    track_sub_link(c, tslot, sslot);
    t->linger_deadline_us = 0;   /* subscriber present again */

    if (t->state == R_TRACK_PENDING || t->state == R_TRACK_RENDEZVOUS) {
        s->state = R_SUB_PARKED;
        s->filter_type = req->filter.type;
        s->start_group = req->filter.start_group;
        s->start_object = req->filter.start_object;
        s->end_group = req->filter.end_group_delta;
        if (t->state == R_TRACK_RENDEZVOUS) {
            s->rendezvous = true;
            s->rendezvous_deadline_us = req_deadline;
        }
        *out = sub_handle(c, sslot);
        return MOQR_OK;
    }

    /* ACTIVE or WARM: accept immediately from track state. */
    s->state = R_SUB_ACTIVE;
    ready_mark(c, bslot);   /* retained content is deliverable at once */
    if (s->has_location) {
        sub_resolve_location(s, t->has_largest, t->largest_group, t->largest_object);
        sub_exclude_retained(s, t->log);
    } else {
        sub_resolve_filter(s, &req->filter, t->has_largest, t->largest_group,
                           t->largest_object);
    }
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_SUB);
    it->binding_cookie = b->cookie;
    it->cookie = req->cookie;
    it->sub = sub_handle(c, sslot);
    it->has_largest = t->has_largest;
    it->largest_group = t->largest_group;
    it->largest_object = t->largest_object;

    if (t->state == R_TRACK_WARM) {
        /* Re-establish upstream if a publisher is announced. */
        uint32_t pub_node = (c->cluster_enabled ? cluster_select_node(c, req->ns, bslot, b->gen) : trie_longest_announce(c, req->ns));
        if (pub_node != UINT32_MAX) {
            t->state = R_TRACK_PENDING;
            t->source_kind = R_TRACK_SOURCE_PULL;
            t->track_gen++;
            t->has_upstream_binding = true;
            t->up_binding = c->nodes[pub_node].ann_binding;
            t->up_binding_gen = c->nodes[pub_node].ann_binding_gen;
            track_src_set(c, t, pub_node);
            t->src_ann_binding = c->nodes[pub_node].ann_binding;
            t->src_ann_binding_gen = c->nodes[pub_node].ann_binding_gen;
        cluster_pin_track(c, t);
        t->cluster_requester_hop = b->peer_hop_id;
        t->cluster_requester_correlation = b->correlation_id;
            t->metadata_managed = t->metadata_managed ||
                c->bindings[t->up_binding].metadata_managed;
            moqr_intent_t *up = intent_push(c,
                                            MOQR_INTENT_UPSTREAM_SUBSCRIBE);
            up->binding_cookie = c->bindings[t->up_binding].cookie;
            up->track = track_handle(c, tslot);
            up->track_gen = t->track_gen;
            intent_set_ns_from_key(up, &t->key);
            r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 1 /* warm rejoin */,
                    tslot, t->track_gen, bslot, 0);
        }
    }
    *out = sub_handle(c, sslot);
    return MOQR_OK;
}


moqr_result_t
moqr_core_unsubscribe(moqr_core_t *c, moqr_sub_t sh, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    sub_retire(c, slot, now_us);
    return MOQR_OK;
}


moqr_result_t
moqr_core_sub_find(const moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                   moqr_sub_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_SUB_INVALID;
    uint32_t bslot = 0;
    const r_binding_t *b = binding_resolve((moqr_core_t *)c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    for (uint32_t i = 0; i < c->max_subs; i++) {
        const r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) != 0 && s->binding == bslot &&
            s->binding_gen == b->gen && s->cookie == cookie) {
            *out = sub_handle((moqr_core_t *)c, i);
            return MOQR_OK;
        }
    }
    return MOQR_ERR_STALE_HANDLE;
}


moqr_result_t
moqr_core_sub_set_forward(moqr_core_t *c, moqr_sub_t sh, bool forward,
                          uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;   /* retired/unknown/foreign: fail closed */
    }
    if (s->forward == forward) {
        return MOQR_OK;   /* idempotent: no state change, no spurious re-arm */
    }
    s->forward = forward;
    r_binding_t *b = &c->bindings[s->binding];
    if (!forward) {
        /* Pausing: release this sub's own held ordinary-RECORD delivery NOW so
         * its pins free and the binding never sits monopolized while paused
         * (its siblings keep flowing). The cursor is left unadvanced — resume
         * re-derives. A held MAINTENANCE delivery (recordless notice or an
         * abandoned/evict reset) is NOT dropped: stream cleanup stays eligible
         * while paused so a downstream FIN/RESET is not deferred to resume. */
        if (b->out_active && b->out_sub == slot &&
            b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned) {
            binding_drop_held(b);
        }
    } else {
        /* Resuming: re-arm the owning binding so the released/held work is
         * reconsidered on the next pump and delivered exactly once. */
        ready_mark(c, s->binding);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_sub_set_filter(moqr_core_t *c, moqr_sub_t sh,
                         const moqr_filter_t *filter, uint64_t now_us)
{
    (void)now_us;
    if (c == NULL || filter == NULL ||
        filter->type < MOQR_FILTER_NEXT_GROUP_START ||
        filter->type > MOQR_FILTER_RELATIVE_START) {
        return MOQR_ERR_INVAL;
    }
    if (filter->type == MOQR_FILTER_ABSOLUTE_RANGE &&
        filter->start_group + filter->end_group_delta < filter->start_group) {
        return MOQR_ERR_INVAL;   /* end-group overflow (spec: violation) */
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (s->state == R_SUB_PARKED) {
        /* Resolves at upstream acceptance, like the request's own filter. */
        s->has_location = false;
        s->filter_type = filter->type;
        s->start_group = filter->start_group;
        s->start_object = filter->start_object;
        s->end_group = filter->end_group_delta;
        return MOQR_OK;
    }
    const r_track_t *t = &c->tracks[s->track];
    sub_exclude_retained(s, t->log);
    uint64_t old_sg = s->start_group, old_so = s->start_object;
    bool old_empty = s->location_empty;
    sub_resolve_filter(s, filter, t->has_largest, t->largest_group,
                       t->largest_object);
    /* The frontier never moves back: records the scan has passed (delivered,
     * or consumed below the former start) are not revisited, so a lowered
     * start keeps the former one. */
    if (s->start_group < old_sg ||
        (s->start_group == old_sg && s->start_object < old_so)) {
        s->start_group = old_sg;
        s->start_object = old_so;
    }
    /* Beyond-last is also a lower bound: only typed replacement may reopen it. */
    s->location_empty = s->location_empty || old_empty;
    sub_exclude_retained(s, t->log);
    r_binding_t *b = &c->bindings[s->binding];
    if (s->location_empty && b->out_active && b->out_sub == slot &&
        b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned) {
        uint32_t pos = gpos_hash_find(s, b->out_group);
        bool begun = pos != R_GPOS_NIL &&
                     gpos_emit(s, gpos_at(s, pos))[b->out_list] != 0;
        if (!begun) binding_drop_held(b);
    }
    /* The scan reads the window live; re-arm the owning binding so what the
     * new window admits is considered at once. */
    ready_mark(c, s->binding);
    return MOQR_OK;
}


moqr_result_t
moqr_core_sub_set_location_filter(moqr_core_t *c, moqr_sub_t sh,
                                  const moq_location_filter_cfg_t *location,
                                  uint64_t now_us)
{
    (void)now_us;
    if (c == NULL || !location_valid(location)) return MOQR_ERR_INVAL;
    r_sub_t *s = sub_resolve(c, sh, NULL);
    if (s == NULL) return MOQR_ERR_STALE_HANDLE;
    const r_track_t *t = &c->tracks[s->track];
    if (s->excluded == NULL) {
        size_t n = t->log->max_records / 8u + (t->log->max_records % 8u != 0);
        uint8_t *bits = r_alloc(c, n);
        if (bits == NULL) return MOQR_ERR_NOMEM;
        memset(bits, 0, n);
        s->excluded = bits;
        s->excluded_cap = t->log->max_records;
    }
    if (s->state != R_SUB_PARKED) sub_exclude_retained(s, t->log);
    s->location = *location;
    s->has_location = true;
    if (s->state == R_SUB_PARKED) return MOQR_OK;
    sub_resolve_location(s, t->has_largest, t->largest_group, t->largest_object);
    sub_exclude_retained(s, t->log);
    r_binding_t *b = &c->bindings[s->binding];
    if (b->out_active && b->out_sub == (uint32_t)(s - c->subs) &&
        b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned &&
        sub_excluded(s, b->out_view.rec.arrival_seq)) {
        uint32_t slot = gpos_hash_find(s, b->out_group);
        bool begun = slot != R_GPOS_NIL &&
                     gpos_emit(s, gpos_at(s, slot))[b->out_list] != 0;
        if (!begun) binding_drop_held(b);
    }
    ready_mark(c, s->binding);
    return MOQR_OK;
}


moqr_result_t
moqr_core_delivery_note_emitted(moqr_core_t *c, moqr_binding_t bh,
                                uint32_t emitted, uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Only a live outstanding chunked delivery carries a begun watermark;
     * anything else (idle, whole-object, notice, abandoned-reset) has no
     * per-chunk progress to record. The CALL itself means "the object header is
     * begun downstream" — including emitted==0 (begin_object shipped, first
     * chunk not yet), which MUST still be recorded as begun so an abandon/evict
     * while paused surfaces the required RESET rather than being skipped as
     * never-begun. */
    if (!b->out_active || !b->out_chunked) {
        return MOQR_OK;
    }
    /* Contract: emitted is 0..chunk_count. An out-of-range count would become a
     * bogus pin base / re-selection watermark (re-derivation could pin zero
     * chunks and silently advance) — reject it and leave state unchanged. */
    if (emitted > b->out_view.rec.chunk_count) {
        return MOQR_ERR_INVAL;
    }
    r_sub_t *s = &c->subs[b->out_sub];
    if ((s->gen & 1u) == 0) {
        return MOQR_OK;   /* sub retired underneath the hold: nothing to mark */
    }
    r_track_t *t = &c->tracks[s->track];
    if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot || s->gpos == NULL) {
        return MOQR_OK;   /* group evicted/track gone: pins keep it whole */
    }
    uint32_t slot = gpos_hash_find(s, b->out_group);
    if (slot == R_GPOS_NIL) {
        return MOQR_OK;
    }
    r_gpos_t *pe = gpos_at(s, slot);
    /* Count every accepted checkpoint call (once per WOULD_BLOCK hold — this is
     * a call counter, NOT a per-begun-object transition). */
    c->note_emitted_total++;
    /* Mark this position begun (top bit) — decoupled from the chunk count so a
     * begin-with-zero-chunks is begun without faking a count of one (which would
     * skip chunk 0 on re-derivation). The begun TRANSITION is counted exactly
     * once (word 0 -> nonzero) into the begun accounting; the eventual
     * DELIVERED/ABANDONED balances it. Record the exact emitted count in the low
     * bits, monotonically. */
    uint32_t *w = &gpos_emit(s, pe)[b->out_list];
    if (*w == 0) {
        sub_begun_add(c, s);
    }
    uint32_t cnt = *w & GPOS_EMIT_MASK;
    if (emitted > cnt) {
        cnt = emitted;   /* monotonic: never rewind the count */
    }
    *w = GPOS_BEGUN_BIT | cnt;
    return MOQR_OK;
}


moqr_result_t
moqr_core_sub_track(moqr_core_t *c, moqr_sub_t sh, moqr_track_t *track,
                    uint64_t *track_gen)
{
    if (c == NULL || track == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    *track = track_handle(c, s->track);
    if (track_gen != NULL) {
        *track_gen = c->tracks[s->track].track_gen;
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_track_current_generation(const moqr_core_t *c, moqr_track_t th,
                                   uint64_t *out_track_gen)
{
    if (c == NULL || out_track_gen == NULL) return MOQR_ERR_INVAL;
    const r_track_t *t = track_resolve((moqr_core_t *)(void *)c, th, NULL);
    if (t == NULL) return MOQR_ERR_STALE_HANDLE;
    *out_track_gen = t->track_gen;
    return MOQR_OK;
}


bool
moqr_core_internal_source_idle(const moqr_core_t *c, moqr_track_t th,
    uint64_t old_source_gen)
{
    if (c == NULL || old_source_gen == UINT64_MAX) return false;
    const r_track_t *t = track_resolve((moqr_core_t *)(uintptr_t)c, th, NULL);
    return t != NULL && t->state == R_TRACK_WARM &&
        t->source_kind == R_TRACK_SOURCE_NONE && !t->has_upstream_binding &&
        t->track_gen == old_source_gen + 1;
}


moqr_result_t
moqr_core_track_metadata_managed(const moqr_core_t *c, moqr_track_t th,
                                bool *out_managed)
{
    if (c == NULL || out_managed == NULL) return MOQR_ERR_INVAL;
    const r_track_t *t = track_resolve((moqr_core_t *)(void *)c, th, NULL);
    if (t == NULL) return MOQR_ERR_STALE_HANDLE;
    *out_managed = t->metadata_managed;
    return MOQR_OK;
}


moqr_result_t
moqr_core_track_key(const moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                    moq_bytes_t *parts, size_t parts_cap, moqr_ns_t *ns,
                    moq_bytes_t *name)
{
    if (c == NULL || ns == NULL || name == NULL ||
        (parts == NULL && parts_cap != 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    const r_track_t *t = track_resolve((moqr_core_t *)(void *)c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t count = key_part_count(&t->key);
    if ((size_t)count > parts_cap) {
        return MOQR_ERR_CAPACITY;
    }
    for (uint32_t i = 0; i < count; i++) {
        parts[i] = key_part(&t->key, i);
    }
    ns->parts = parts;
    ns->count = count;
    *name = key_name(&t->key);
    return MOQR_OK;
}


moqr_result_t
moqr_core_track_largest(const moqr_core_t *c, moqr_track_t th,
                        uint64_t track_gen, bool *has_largest,
                        uint64_t *largest_group, uint64_t *largest_object)
{
    if (c == NULL || has_largest == NULL || largest_group == NULL ||
        largest_object == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    const r_track_t *t = track_resolve((moqr_core_t *)(void *)c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    *has_largest = t->has_largest;
    *largest_group = t->has_largest ? t->largest_group : 0;
    *largest_object = t->has_largest ? t->largest_object : 0;
    return MOQR_OK;
}


moqr_result_t
moqr_core_sub_state(const moqr_core_t *c, moqr_sub_t sh,
                    moqr_sub_state_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The same generation-guarded resolution as sub_resolve, on a const
     * view: pool tag, shard tag, live (odd) generation, slot bounds, and
     * the slot's current generation must all match — anything else is a
     * retired or foreign handle, reported STALE, never a stale value. */
    if (moq_handle_pool_tag(sh._opaque) != MOQR_HANDLE_POOL_SUB ||
        moq_handle_session_tag(sh._opaque) != c->shard_tag ||
        (moq_handle_generation(sh._opaque) & 1u) == 0) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = moq_handle_slot(sh._opaque);
    if (slot >= c->max_subs ||
        c->subs[slot].gen != moq_handle_generation(sh._opaque)) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Exact mapping — a live sub is PARKED or ACTIVE by construction; any
     * other value is a corrupted slot and is refused, never guessed. */
    if (c->subs[slot].state == R_SUB_PARKED) {
        *out = MOQR_SUB_PARKED;
    } else if (c->subs[slot].state == R_SUB_ACTIVE) {
        *out = MOQR_SUB_ACTIVE;
    } else {
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_revoke_sub(moqr_core_t *c, moqr_sub_t sh, moqr_pd_desc_t status,
                     uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt terminal before retiring anything: this subscription must stay live. */
    if (!moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Reserve intent space BEFORE mutating: a relay-initiated termination must
     * tell the subscriber (SUBSCRIBE_DONE), so the sub is never retired unless
     * the DONE is queued. Unlike moqr_core_unsubscribe (silent local retire),
     * this pushes MOQR_INTENT_SUB_DONE -> moq_session_done_subscribe. */
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint64_t bcookie = 0;
    if (s->binding < c->max_bindings &&
        c->bindings[s->binding].gen == s->binding_gen) {
        bcookie = c->bindings[s->binding].cookie;
    }
    uint64_t scookie = s->cookie;
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
    it->binding_cookie = bcookie;
    it->cookie = scookie;
    it->sub = sh;
    it->pd = status;   /* already tagged by the caller; never re-derived */
    sub_retire(c, slot, now_us);
    return MOQR_OK;
}
