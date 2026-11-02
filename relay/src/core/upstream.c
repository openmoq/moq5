#include "relay_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

/* -- ops: upstream resolution ------------------------------------------------------------------------ */

moqr_result_t
moqr_core_upstream_ok(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                      uint64_t upstream_cookie, bool has_largest,
                      uint64_t lg, uint64_t lo)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_PENDING) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* Reserve: one ACCEPT per parked sub. */
    uint32_t parked = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0 && c->subs[i].track == tslot &&
            c->subs[i].track_gen_slot == t->gen &&
            c->subs[i].state == R_SUB_PARKED) {
            parked++;
        }
    }
    if (!intent_space(c, parked)) {
        return MOQR_ERR_WOULD_BLOCK;
    }

    t->state = R_TRACK_ACTIVE;
    t->upstream_cookie = upstream_cookie;
    discovery_mark_all(c);
    if (has_largest &&
        (!t->has_largest || lg > t->largest_group ||
         (lg == t->largest_group && lo > t->largest_object))) {
        t->has_largest = true;
        t->largest_group = lg;
        t->largest_object = lo;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 0 /* ok */, tslot, track_gen,
            parked, 0);

    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != t->gen || s->state != R_SUB_PARKED) {
            continue;
        }
        moqr_filter_t f = {
            .type = s->filter_type,
            .start_group = s->start_group,
            .start_object = s->start_object,
            .end_group_delta = s->end_group,
        };
        s->state = R_SUB_ACTIVE;
        ready_mark(c, s->binding);
        if (s->has_location) {
            sub_resolve_location(s, t->has_largest, t->largest_group,
                                  t->largest_object);
            sub_exclude_retained(s, t->log);
        } else {
            sub_resolve_filter(s, &f, t->has_largest, t->largest_group,
                               t->largest_object);
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_SUB);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        it->sub = sub_handle(c, i);
        it->has_largest = t->has_largest;
        it->largest_group = t->largest_group;
        it->largest_object = t->largest_object;
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_upstream_error(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                         uint64_t error_code, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_PENDING) {
        return MOQR_ERR_WRONG_STATE;
    }
    uint32_t parked = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0 && c->subs[i].track == tslot &&
            c->subs[i].track_gen_slot == t->gen &&
            c->subs[i].state == R_SUB_PARKED) {
            parked++;
        }
    }
    if (!intent_space(c, parked)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 1 /* error */, tslot, track_gen,
            parked, error_code);
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != t->gen || s->state != R_SUB_PARKED) {
            continue;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        it->error_code = error_code;
        sub_retire(c, i, now_us);
    }
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_sub_count(c, tslot) == 0 &&
        track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
        track_touch(c, tslot);   /* source released: last use */
        r_room_changed(c);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_upstream_cancel(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                          uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Valid only on a PENDING track NOBODY subscribes to — in any sub state:
     * a WARM rejoin ACCEPTs its subscriber and only then re-enters PENDING,
     * so live subscribers can exist on a track with zero parked ones. */
    if (t->state != R_TRACK_PENDING || track_sub_count(c, tslot) != 0) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* Preflight the single unsubscribe intent BEFORE any mutation. */
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 2 /* cancelled */, tslot,
            track_gen, 0, 0);
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
    it->track = th;
    it->track_gen = t->track_gen;
    it->cookie = t->upstream_cookie;
    if (t->has_upstream_binding && (c->bindings[t->up_binding].gen & 1u) != 0) {
        it->binding_cookie = c->bindings[t->up_binding].cookie;
    }
    /* Release: bump identity so a late upstream answer is STALE, then mirror
     * the source-release rules — free when nothing is retained, else WARM
     * (a cancelled rejoin must keep the retained log it was rejoining for). */
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
        track_touch(c, tslot);   /* source released: last use */
        r_room_changed(c);
    }
    return MOQR_OK;
}
