#include "relay_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

/* -- ops: publish push / ingest / status / capacity ---------------------------------------------------- */

/* Assign a pushed track's source-announce identity: the longest announce for the
 * pushed namespace, but ONLY when it is owned by the pushing binding itself. An
 * unannounced push — or one whose longest announce belongs to a different binding
 * — has no announce source (UINT32_MAX) and is not coupled to any announce
 * withdrawal, while a publisher that both announced and pushed is still purged. */
static void
track_set_push_source(moqr_core_t *c, r_track_t *t, moqr_ns_t ns, uint32_t bslot,
                      uint32_t bgen)
{
    if (c->cluster_enabled) {
        uint32_t node = cluster_longest_node(c, ns);
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            const r_cluster_route_t *r = &c->cluster_routes[ri];
            if (r->used && r->node == node && r->binding == bslot && r->binding_gen == bgen) {
                track_src_set(c, t, node);
                t->src_ann_binding = bslot;
                t->src_ann_binding_gen = bgen;
                t->cluster_route_lifetime = r->lifetime;
                t->cluster_origin = cluster_path(c, ri)[0];
                return;
            }
        }
        return;
    }
    uint32_t node = trie_longest_announce(c, ns);
    if (node != UINT32_MAX && c->nodes[node].ann_binding == bslot &&
        c->nodes[node].ann_binding_gen == bgen) {
        track_src_set(c, t, node);
        t->src_ann_binding = bslot;
        t->src_ann_binding_gen = bgen;
        cluster_pin_track(c, t);
    } else {
        track_src_set(c, t, UINT32_MAX);
    }
}


static moqr_result_t
rendezvous_publish(moqr_core_t *c, uint32_t tslot, uint32_t bslot,
                   r_binding_t *b, moqr_ns_t ns, uint64_t cookie,
                   moqr_track_t *out)
{
    r_track_t *t = &c->tracks[tslot];
    uint32_t held = track_sub_count(c, tslot);
    if (!intent_space(c, held + 1u) || rendezvous_timeout_owed(c, tslot)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    t->state = R_TRACK_ACTIVE;
    t->source_kind = R_TRACK_SOURCE_PUSH;
    t->metadata_managed = b->metadata_managed;
    t->track_gen++;
    t->has_upstream_binding = true;
    t->up_binding = bslot;
    t->up_binding_gen = b->gen;
    track_set_push_source(c, t, ns, bslot, b->gen);
    moqr_intent_t *acc = intent_push(c, MOQR_INTENT_ACCEPT_PUBLISH);
    acc->binding_cookie = b->cookie;
    acc->cookie = cookie;
    acc->track = track_handle(c, tslot);
    acc->track_gen = t->track_gen;
    *out = acc->track;
    for (uint32_t k = t->subs_head; k != R_SUB_NIL; k = c->subs[k].track_next) {
        r_sub_t *s = &c->subs[k];
        if ((s->gen & 1u) == 0 || s->state != R_SUB_PARKED) {
            continue;
        }
        s->rendezvous = false;
        s->state = R_SUB_ACTIVE;
        ready_mark(c, s->binding);
        if (s->has_location) {
            sub_resolve_location(s, t->has_largest, t->largest_group,
                                 t->largest_object);
            sub_exclude_retained(s, t->log);
        } else {
            moqr_filter_t f = {
                .type = s->filter_type,
                .start_group = s->start_group,
                .start_object = s->start_object,
                .end_group_delta = s->end_group,
            };
            sub_resolve_filter(s, &f, t->has_largest, t->largest_group,
                               t->largest_object);
        }
        discovery_suppress(c, s->binding, s->binding_gen, tslot);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_SUB);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        it->sub = sub_handle(c, k);
        it->has_largest = t->has_largest;
        it->largest_group = t->largest_group;
        it->largest_object = t->largest_object;
    }
    discovery_mark_all(c);
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 2 /* rendezvous publish */, tslot,
            t->track_gen, held, 0);
    return MOQR_OK;
}


moqr_result_t
moqr_core_publish_open(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                       moq_bytes_t name, uint64_t cookie, moqr_track_t *out)
{
    if (c == NULL || out == NULL || !ftn_view_ok(ns, name)) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_TRACK_INVALID;
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint64_t hash = 0;
    uint32_t tslot = track_lookup(c, ns, name, &hash);
    if (c->cluster_enabled) {
        tslot = UINT32_MAX;
        for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
            const r_track_t *t = &c->tracks[ti];
            bool held_eligible = t->state == R_TRACK_RENDEZVOUS;
            if (held_eligible && (t->gen & 1u) != 0) {
                uint32_t node = cluster_longest_node(c, ns), own_route = UINT32_MAX;
                for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
                    const r_cluster_route_t *r = &c->cluster_routes[ri];
                    if (r->used && r->node == node && r->binding == bslot && r->binding_gen == b->gen)
                        own_route = ri;
                }
                for (uint32_t si = t->subs_head; si != R_SUB_NIL && held_eligible; si = c->subs[si].track_next) {
                    const r_sub_t *sub = &c->subs[si];
                    held_eligible = own_route != UINT32_MAX
                        ? cluster_eligible(c, own_route, sub->binding, sub->binding_gen, UINT32_MAX, 0)
                        : (sub->binding != bslot && (b->correlation_id == 0 ||
                            b->correlation_id != c->bindings[sub->binding].correlation_id));
                }
            }
            if ((t->gen & 1u) != 0 && t->hash == hash &&
                key_view_eq(&t->key, ns, name) &&
                ((t->up_binding == bslot && t->up_binding_gen == b->gen) ||
                 held_eligible)) { tslot = ti; break; }
        }
    }
    moqr_result_t rc;
    if (tslot != UINT32_MAX) {
        r_track_t *t = &c->tracks[tslot];
        if (t->state == R_TRACK_RENDEZVOUS) {
            return rendezvous_publish(c, tslot, bslot, b, ns, cookie, out);
        }
        if (t->state != R_TRACK_WARM) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_PUBLISH);
            it->binding_cookie = b->cookie;
            it->cookie = cookie;
            it->error_code = R_ERR_INTERNAL;   /* duplicate active source */
            return MOQR_OK;
        }

        /* A warm pushed track can still carry retained content from a previous
         * source. Reacquiring the same full track name with PUBLISH creates a
         * new source generation: old readers are terminated/staled, the log is
         * replaced, and group 0 is compared against an empty generation. */
        uint32_t old_subs = track_sub_count(c, tslot);
        if (!intent_space(c, old_subs + 1u)) {
            return MOQR_ERR_WOULD_BLOCK;
        }
        /* The new generation normally keeps the slot. A slot on its last
         * live generation cannot renew; the new source then moves to another
         * slot, found (or reclaimed) before anything changes, and when none
         * exists the publish is refused with the retained track untouched.
         * The slot on its last generation is never a reclaim candidate for a
         * slot, so making room cannot free the track being replaced. */
        bool renew = r_gen_renewable(t->gen);
        uint32_t dst = tslot;
        if (!renew) {
            rc = r_make_room(c, true, 0, 0, NULL);
            if (rc != MOQR_OK) {
                return rc;
            }
            dst = track_free_slot_find(c);
        }
        moqr_log_t *new_log = NULL;
        rc = track_make_log(c, dst, &new_log);
        if (rc != MOQR_OK) {
            return rc;
        }
        uint32_t old_track_gen = t->gen;
        for (uint32_t i = t->subs_head; i != R_SUB_NIL;) {
            uint32_t next = c->subs[i].track_next;
            r_sub_t *s = &c->subs[i];
            if ((s->gen & 1u) != 0 && s->track == tslot &&
                s->track_gen_slot == old_track_gen) {
                uint64_t bcookie = 0;
                if (s->binding < c->max_bindings &&
                    c->bindings[s->binding].gen == s->binding_gen) {
                    bcookie = c->bindings[s->binding].cookie;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = bcookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, i, 0);
            }
            i = next;
        }
        if (renew) {
            track_replace_retained_generation(c, tslot, new_log);
            track_touch(c, tslot);
        } else {
            /* The name moves with its interned key (no new bytes charged);
             * the old slot retires and every handle to it goes stale. */
            r_key_t key = t->key;
            uint64_t thash = t->hash;
            uint32_t source_gen = t->track_gen;
            t->key = (r_key_t){ NULL, 0 };
            track_fetch_invalidate(c, tslot, old_track_gen);
            track_free_slot(c, tslot);
            track_install(c, dst, key, thash, new_log);
            tslot = dst;
            t = &c->tracks[tslot];
            t->track_gen = source_gen;
        }
        t->state = R_TRACK_ACTIVE;
        t->source_kind = R_TRACK_SOURCE_PUSH;
        t->metadata_managed = b->metadata_managed;
        t->track_gen++;
        t->has_upstream_binding = true;
        t->up_binding = bslot;
        t->up_binding_gen = b->gen;
        track_set_push_source(c, t, ns, bslot, b->gen);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_PUBLISH);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->track = track_handle(c, tslot);
        it->track_gen = t->track_gen;
        *out = it->track;
        discovery_mark_all(c);
        return MOQR_OK;
    }
    rc = track_create(c, ns, name, hash, &tslot);
    if (rc != MOQR_OK) {
        return rc;
    }
    r_track_t *t = &c->tracks[tslot];
    t->state = R_TRACK_ACTIVE;
    t->source_kind = R_TRACK_SOURCE_PUSH;
    t->metadata_managed = b->metadata_managed;
    t->track_gen = 1;
    t->has_upstream_binding = true;
    t->up_binding = bslot;
    t->up_binding_gen = b->gen;
    track_set_push_source(c, t, ns, bslot, b->gen);
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_PUBLISH);
    it->binding_cookie = b->cookie;
    it->cookie = cookie;
    it->track = track_handle(c, tslot);
    it->track_gen = t->track_gen;
    *out = it->track;
    discovery_mark_all(c);
    return MOQR_OK;
}


/* Raise the track's Largest Object to (g, o) if that is past it. */
static void
track_note_largest(r_track_t *t, uint64_t g, uint64_t o)
{
    if (!t->has_largest || g > t->largest_group ||
        (g == t->largest_group && o > t->largest_object)) {
        t->has_largest = true;
        t->largest_group = g;
        t->largest_object = o;
    }
}


moqr_result_t
moqr_core_ingest(moqr_core_t *c, moqr_track_t th,
                 const moqr_log_append_desc_t *desc)
{
    if (c == NULL || desc == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (desc->struct_size < offsetof(moqr_log_append_desc_t, declared_len) +
                            sizeof(desc->declared_len)) return MOQR_ERR_INVAL;
    if (t->cluster_group_floor_set && desc->group_id <= t->cluster_group_floor) {
        if (desc->payload != NULL) moq_rcbuf_decref(desc->payload);
        if (desc->properties != NULL) moq_rcbuf_decref(desc->properties);
        return MOQR_OK;
    }
    /* OPEN begins an incrementally-filled record (chunk-through). The Largest
     * Object advances when the first byte of an Object past it arrives
     * (draft-22 3.1.4), so an Object still arriving can be the Largest; what
     * is served up to the Largest waits for it to complete (fetch peek). */
    /* Groups evicted to make room owe their watermark and terminal progress
     * even when the record is then refused. */
    const uint64_t evicted = moqr_log_evicted_groups_total(t->log);
    if (desc->obj_state == MOQR_OBJ_OPEN) {
        moqr_result_t orc = moqr_log_open_record(t->log, desc);
        if (orc == MOQR_OK) {
            track_note_largest(t, desc->group_id, desc->object_id);
            track_location_ingest(c, t, desc);
            ready_mark_track(c, tslot);   /* a live-edge head is selectable */
        } else if (moqr_log_evicted_groups_total(t->log) != evicted) {
            ready_mark_track(c, tslot);
        }
        return orc;
    }
    moqr_result_t rc = moqr_log_append(t->log, desc);
    if (rc != MOQR_OK) {
        if (moqr_log_evicted_groups_total(t->log) != evicted) {
            ready_mark_track(c, tslot);
        }
        return rc;
    }
    track_note_largest(t, desc->group_id, desc->object_id);
    track_location_ingest(c, t, desc);
    c->ingested_total++;
    ready_mark_track(c, tslot);
    return MOQR_OK;
}


/* Chunk-through ingest wrappers: resolve the track (must be ACTIVE) and forward
 * to the log's chunk-through APIs. Continuation chunks are routed by (group, subgroup,
 * object) -- the identity every OBJECT_CHUNK now carries. */
moqr_result_t
moqr_core_append_chunk(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                       uint64_t subgroup_id, uint64_t object_id,
                       moq_rcbuf_t *chunk)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (t->cluster_group_floor_set && group_id <= t->cluster_group_floor)
        return MOQR_OK; /* append_chunk borrows the caller's buffer */
    const uint64_t evicted = moqr_log_evicted_groups_total(t->log);
    moqr_result_t rc =
        moqr_log_append_chunk(t->log, group_id, subgroup_id, object_id, chunk);
    /* Growth resumes a stalled delivery; the groups evicted to make room owe
     * their watermark and terminal progress even when the chunk is then
     * refused. */
    if (rc == MOQR_OK || moqr_log_evicted_groups_total(t->log) != evicted) {
        ready_mark_track(c, tslot);
    }
    return rc;
}


moqr_result_t
moqr_core_complete_record(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                          uint64_t subgroup_id, uint64_t object_id)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (t->cluster_group_floor_set && group_id <= t->cluster_group_floor)
        return MOQR_OK;
    moqr_result_t rc =
        moqr_log_complete_record(t->log, group_id, subgroup_id, object_id);
    if (rc != MOQR_OK) {
        return rc;
    }
    /* Now a delivered/fetchable object. The Largest already counts it from its
     * open; noting it again never lowers it. */
    track_note_largest(t, group_id, object_id);
    c->ingested_total++;
    ready_mark_track(c, tslot);
    return MOQR_OK;
}


moqr_result_t
moqr_core_abandon_record(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                         uint64_t subgroup_id, uint64_t object_id,
                         moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the record must stay OPEN if we cannot state its terminal. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* ACTIVE or WARM: a pure log mutation. WARM matters — the idle
     * ACTIVE->WARM transition happens before the upstream unsubscribe is
     * even visible to the source, and a WARM log KEEPS its records, so the
     * source's teardown must still be able to scrub a partially ingested
     * OPEN record (a later rejoin must never meet stale partial state).
     * The handle's pool generation resolved above rejects a freed-and-reused
     * slot. Nothing more is enforced here: correlating the abandon with the
     * current source attempt (and serializing it against rejoins) is the
     * caller's job — the shard teardown runs it in the same single-writer
     * step that observed the WARM transition. */
    if (t->state != R_TRACK_ACTIVE && t->state != R_TRACK_WARM) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (t->cluster_group_floor_set && group_id <= t->cluster_group_floor)
        return MOQR_OK;
    moqr_result_t rc = moqr_log_abandon_record(t->log, group_id, subgroup_id,
                                               object_id, reset);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the reset notice is deliverable */
    }
    return rc;
}


moqr_result_t
moqr_core_seal_subgroup(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                        uint64_t subgroup_id)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The handle's pool generation rejects a freed-and-reused slot. Nothing
     * more is enforced here: correlating the seal with the current source
     * attempt (and serializing it against rejoins) is the caller's job. */
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->cluster_group_floor_set && group_id <= t->cluster_group_floor)
        return MOQR_OK;
    moqr_result_t rc = moqr_log_seal_subgroup(t->log, group_id, subgroup_id);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the seal notice is deliverable */
    }
    return rc;
}


moqr_result_t
moqr_core_reset_subgroup(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                         uint64_t subgroup_id, moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the subgroup must keep its own terminal if we cannot state this one. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->cluster_group_floor_set && group_id <= t->cluster_group_floor)
        return MOQR_OK;
    moqr_result_t rc =
        moqr_log_reset_subgroup(t->log, group_id, subgroup_id, reset);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the reset notice is deliverable */
    }
    return rc;
}


moqr_result_t
moqr_core_internal_note_gap(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                   uint64_t subgroup_id)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    return moqr_log_note_gap(t->log, group_id, subgroup_id);
}


uint32_t
moqr_core_internal_duplicate(moqr_core_t *c, moqr_track_t th,
                    const moqr_log_append_desc_t *desc)
{
    if (c == NULL || desc == NULL) {
        return MOQR_LOG_DUP_NONE;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_LOG_DUP_NONE;
    }
    return moqr_log_duplicate(t->log, desc);
}


uint32_t
moqr_core_internal_duplicate_range(moqr_core_t *c, moqr_track_t th,
                                   uint64_t group_id, uint64_t subgroup_id,
                                   uint64_t object_id, uint64_t offset,
                                   const uint8_t *data, size_t len)
{
    if (c == NULL || (data == NULL && len > 0)) {
        return MOQR_LOG_RANGE_UNKNOWN;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_LOG_RANGE_UNKNOWN;
    }
    return moqr_log_duplicate_range(t->log, group_id, subgroup_id, object_id,
                                    offset, data, len);
}


moqr_result_t
moqr_core_internal_set_opener(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                              uint64_t subgroup_id, uint64_t object_id,
                              uint32_t conn, uint64_t src)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    return moqr_log_set_opener(t->log, group_id, subgroup_id, object_id, conn, src);
}


bool
moqr_core_internal_open_by_other(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                                 uint64_t subgroup_id, uint64_t object_id,
                                 uint32_t conn, uint64_t src)
{
    if (c == NULL) {
        return false;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return false;
    }
    return moqr_log_open_by_other(t->log, group_id, subgroup_id, object_id, conn, src);
}


moqr_result_t
moqr_core_internal_set_max_cache_duration(moqr_core_t *c, moqr_track_t th,
                                 bool has_duration, uint64_t duration_ms)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    t->has_max_cache = has_duration;
    t->max_cache_us = !has_duration ? 0
                      : duration_ms > UINT64_MAX / 1000u ? UINT64_MAX
                                                          : duration_ms * 1000u;
    ready_mark_track(c, tslot);   /* an expiry may change what is deliverable */
    return MOQR_OK;
}


moqr_result_t
moqr_core_note_track_evicted_below(moqr_core_t *c, moqr_track_t th,
                                   uint64_t oldest_retained)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint64_t floor = moqr_log_evicted_floor(t->log);
    if (moqr_log_note_evicted_below(t->log, oldest_retained) > 0 ||
        moqr_log_evicted_floor(t->log) != floor) {
        /* The reset notices are deliverable, and a subscription waiting on
         * the closed range may now retire. */
        ready_mark_track(c, tslot);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_abandon_group_open(moqr_core_t *c, moqr_track_t th,
                             uint64_t group_id, moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the group's open records must stay completable. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE && t->state != R_TRACK_WARM) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_result_t rc =
        moqr_log_abandon_group_open(t->log, group_id, reset, NULL);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* any reset notice is deliverable */
    }
    return rc;
}


moqr_result_t
moqr_core_track_status(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                       moq_bytes_t name, uint64_t cookie)
{
    if (c == NULL || !ftn_view_ok(ns, name)) {
        return MOQR_ERR_INVAL;
    }
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint32_t tslot = track_lookup(c, ns, name, NULL);
    if (tslot == UINT32_MAX) {
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_ERROR);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    r_track_t *t = &c->tracks[tslot];
    if (t->state == R_TRACK_PENDING || t->state == R_TRACK_RENDEZVOUS) {
        /* No established knowledge yet; answering OK would fabricate
         * status (forwarding the request upstream is future work). */
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_ERROR);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_OK);
    it->binding_cookie = b->cookie;
    it->cookie = cookie;
    /* The answer states the track's properties: the binding resolves them
     * from this identity. */
    it->track = track_handle(c, tslot);
    it->track_gen = t->track_gen;
    it->has_largest = t->has_largest;
    it->largest_group = t->largest_group;
    it->largest_object = t->largest_object;
    return MOQR_OK;
}
