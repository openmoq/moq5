#include "relay_internal.h"
#include "namespace_internal.h"
#include "auth_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "tracks_internal.h"

uint32_t cluster_failover_node(moqr_core_t *c, moqr_ns_t ns,
    const r_track_t *track, uint32_t excluded, uint32_t excluded_gen,
    uint64_t excluded_lifetime);

void cluster_reparent_sweep(moqr_core_t *c);

uint32_t cluster_track_lookup(moqr_core_t *c, moqr_ns_t ns,
    moq_bytes_t name, uint32_t requester, uint32_t requester_gen,
    uint64_t *hash_out);

static uint32_t cluster_best_at(const moqr_core_t *c, uint32_t node,
    uint32_t requester, uint32_t requester_gen, uint32_t excluded, uint32_t excluded_gen);

uint32_t cluster_select_node(moqr_core_t *c, moqr_ns_t ns,
    uint32_t requester, uint32_t requester_gen);

moqr_result_t cluster_unannounce(moqr_core_t *c, uint32_t bslot,
    uint32_t bgen, uint32_t node, uint64_t lifetime);

bool cluster_track_eligible(const moqr_core_t *c, const r_track_t *t,
    uint32_t requester, uint32_t requester_gen);

void cluster_pin_track(moqr_core_t *c, r_track_t *t);

void cluster_warm_flush(moqr_core_t *c);

void tsub_free(moqr_core_t *c, uint32_t i);


/* -- ops: announce / ns discovery -------------------------------------------------------------- */

/* The trie nodes and name bytes announcing ns would add, and the deepest
 * node of its path that already exists. */
static void
announce_need(const moqr_core_t *c, moqr_ns_t ns, uint32_t *deep,
              uint32_t *missing, uint64_t *missing_bytes)
{
    *deep = 0;
    *missing = 0;
    *missing_bytes = 0;
    for (size_t i = 0; i < ns.count; i++) {
        uint32_t child = *missing == 0 ? trie_find_child(c, *deep, ns.parts[i])
                                       : UINT32_MAX;
        if (child != UINT32_MAX) {
            *deep = child;
        } else {
            (*missing)++;
            *missing_bytes += ns.parts[i].len;
        }
    }
}


bool cluster_route_warm(const moqr_core_t *c, uint32_t ri);


/* Cluster route ownership is independent of trie indexing; all storage is
 * reserved at core creation. */
uint64_t *
cluster_path(const moqr_core_t *c, uint32_t ri)
{
    return c->cluster_hops + (size_t)ri * c->max_cluster_hops;
}


static bool
cluster_contains(const uint64_t *ids, uint32_t count, uint64_t id)
{
    if (id == 0) return false;
    for (uint32_t i = 0; i < count; i++) if (ids[i] == id) return true;
    return false;
}


static bool
cluster_known(const moqr_core_t *c, uint32_t ri)
{
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    const uint64_t *ids = cluster_path(c, ri);
    for (uint32_t i = 0; i < r->hop_count; i++) if (ids[i] == 0) return false;
    return true;
}


static bool
cluster_eligible_with_path(const moqr_core_t *c, uint32_t ri, uint32_t requester,
    uint32_t requester_gen, uint64_t requester_hop, uint64_t requester_correlation,
    uint32_t excluded, uint32_t excluded_gen, const uint64_t *ids, uint32_t count)
{
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    if (!r->used || c->bindings[r->binding].gen != r->binding_gen ||
        (r->binding == excluded && r->binding_gen == excluded_gen)) return false;
    if (requester == UINT32_MAX) return true;
    if (requester >= c->max_bindings ||
        c->bindings[requester].gen != requester_gen) return false;
    if (r->binding == requester && r->binding_gen == requester_gen) return false;
    if (cluster_contains(ids, count, requester_hop)) return false;
    if (requester_correlation != 0 && requester_correlation == r->source_correlation_id) return false;
    if (requester_hop != 0 && requester_hop == r->source_peer_hop_id) return false;
    return true;
}


bool
cluster_eligible(const moqr_core_t *c, uint32_t ri, uint32_t requester,
    uint32_t requester_gen, uint32_t excluded, uint32_t excluded_gen)
{
    const r_binding_t *b = requester < c->max_bindings ? &c->bindings[requester] : NULL;
    return cluster_eligible_with_path(c, ri, requester, requester_gen,
        b != NULL ? b->peer_hop_id : 0, b != NULL ? b->correlation_id : 0,
        excluded, excluded_gen, cluster_path(c, ri), c->cluster_routes[ri].hop_count);
}


static moqr_cluster_advertisement_id_t
cluster_advertisement(const moqr_core_t *c, const r_cluster_route_t *r)
{
    if (r->mirrored) return r->original;
    return (moqr_cluster_advertisement_id_t){
        {r_pack(c, MOQR_HANDLE_POOL_BINDING, r->binding_gen, r->binding)},
        r->lifetime};
}


static bool
cluster_forced_match(const moqr_core_t *c, const r_cluster_route_t *r)
{
    return c->cluster_forced.serial == 0 || (!r->mirrored &&
        moqr_cluster_advertisement_id_eq(cluster_advertisement(c, r), c->cluster_forced));
}


/* Resolve only an original local owner. A public snapshot is not an
 * authorization token, and a mirror must never satisfy forced dispatch. */
uint32_t
cluster_owner_node(const moqr_core_t *c, moqr_cluster_advertisement_id_t id,
                   moqr_ns_t ns)
{
    uint32_t bs;
    const r_binding_t *b = binding_resolve((moqr_core_t *)(uintptr_t)c, id.source, &bs);
    if (b == NULL || id.serial == 0) return UINT32_MAX;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (!r->used || r->mirrored || r->binding != bs ||
            r->binding_gen != b->gen || r->lifetime != id.serial) continue;
        uint32_t node = 0;
        if (r->node == node) return node;
        for (size_t p = 0; p < ns.count; p++) {
            node = trie_find_child(c, node, ns.parts[p]);
            if (node == UINT32_MAX) break;
            if (node == r->node) return node;
        }
    }
    return UINT32_MAX;
}


static uint32_t
cluster_best_at(const moqr_core_t *c, uint32_t node, uint32_t requester,
    uint32_t requester_gen, uint32_t excluded, uint32_t excluded_gen)
{
    uint32_t best = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->node != node || !cluster_eligible(c, i, requester, requester_gen,
                                                excluded, excluded_gen)) continue;
        if (!cluster_forced_match(c, r)) continue;
        if (best == UINT32_MAX) { best = i; continue; }
        const r_cluster_route_t *p = &c->cluster_routes[best];
        bool known = cluster_known(c, i), pk = cluster_known(c, best);
        uint64_t cost = cluster_route_warm(c, i) ? 0 : r->cost;
        uint64_t prior_cost = cluster_route_warm(c, best) ? 0 : p->cost;
        if ((known && !pk) || (known == pk &&
            (cost < prior_cost || (cost == prior_cost &&
            (r->hop_count < p->hop_count || (r->hop_count == p->hop_count &&
             r->arrival > p->arrival)))))) best = i;
    }
    return best;
}


void
cluster_project(moqr_core_t *c, uint32_t node, uint32_t ri)
{
    r_trie_node_t *n = &c->nodes[node];
    n->has_announce = ri != UINT32_MAX;
    n->cluster_route_index = ri;
    if (ri != UINT32_MAX) {
        const r_cluster_route_t *r = &c->cluster_routes[ri];
        n->ann_binding = r->binding;
        n->ann_binding_gen = r->binding_gen;
        n->session_cookie = r->session_cookie;
    }
}


uint32_t
cluster_longest_node(const moqr_core_t *c, moqr_ns_t ns)
{
    if (c->cluster_forced.serial != 0)
        return cluster_owner_node(c, c->cluster_forced, ns);
    uint32_t node = 0, best = c->nodes[0].has_announce ? 0 : UINT32_MAX;
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) break;
        if (c->nodes[node].has_announce) best = node;
    }
    return best;
}


uint32_t
cluster_select_node(moqr_core_t *c, moqr_ns_t ns, uint32_t requester,
    uint32_t requester_gen)
{
    uint32_t node = cluster_longest_node(c, ns);
    if (node == UINT32_MAX) return node;
    uint32_t ri = cluster_best_at(c, node, requester, requester_gen,
                                  UINT32_MAX, 0);
    if (ri == UINT32_MAX) return UINT32_MAX;
    /* Legacy ingress helpers consume this projection. Every choice is made
     * anew from the independent route pool; this is never route ownership. */
    cluster_project(c, node, ri);
    return node;
}


bool
cluster_route_warm(const moqr_core_t *c, uint32_t ri)
{
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    if (r->inherited_warm) return true;
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        const r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) != 0 && t->state == R_TRACK_ACTIVE &&
            t->has_upstream_binding && t->cluster_route_lifetime == r->lifetime &&
            track_sub_count(c, ti) != 0) return true;
    }
    return false;
}


moq_result_t
moqr_core_cluster_info(const moqr_core_t *c, moqr_cluster_info_t *out, size_t out_size)
{
    if (c == NULL || out == NULL || out_size < MOQR_CLUSTER_INFO_V0_SIZE)
        return MOQ_ERR_INVAL;
    moqr_cluster_info_t value = {0};
    value.enabled = c->cluster_enabled;
    value.local_hop_id = c->cluster_hop_id;
    value.max_routes = c->max_cluster_routes;
    value.max_hops = c->max_cluster_hops;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++)
        if (c->cluster_routes[i].used) value.routes_used++;
    moqr_cluster_write(out, out_size, &value, sizeof(value));
    return MOQ_OK;
}


void
moqr_cluster_peer_cfg_init_sized(moqr_cluster_peer_cfg_t *cfg, size_t size)
{
    if (cfg == NULL || size < sizeof(uint32_t)) return;
    moqr_cluster_peer_cfg_t value = {0};
    if (size >= offsetof(moqr_cluster_peer_cfg_t, effective_relay_cost) + sizeof(uint64_t))
        value.effective_relay_cost = 1;
    moqr_cluster_write(cfg, size, &value, sizeof(value));
}


moq_result_t
moqr_core_binding_set_cluster(moqr_core_t *c, moqr_binding_t bh,
    const moqr_cluster_peer_cfg_t *cfg)
{
    moqr_cluster_peer_cfg_t value = {0};
    value.effective_relay_cost = 1;
    if (cfg != NULL && !moqr_cluster_read(cfg, MOQR_CLUSTER_PEER_CFG_V0_SIZE,
                                         &value, sizeof(value))) return MOQ_ERR_INVAL;
    if (!value.negotiated && value.peer_hop_id != 0) return MOQ_ERR_INVAL;
    if (c == NULL) return MOQ_ERR_INVAL;
    if (!c->cluster_enabled) return MOQ_ERR_WRONG_STATE;
    uint32_t bs;
    r_binding_t *b = binding_resolve(c, bh, &bs);
    if (b == NULL) return MOQ_ERR_STALE_HANDLE;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++)
        if (c->cluster_routes[i].used && c->cluster_routes[i].binding == bs &&
            c->cluster_routes[i].binding_gen == b->gen) return MOQ_ERR_WRONG_STATE;
    b->cluster_negotiated = value.negotiated;
    b->peer_hop_id = value.peer_hop_id;
    b->relay_cost = value.effective_relay_cost;
    b->correlation_id = value.correlation_id;
    return MOQ_OK;
}


static void
cluster_fill(const moqr_core_t *c, uint32_t ri, moqr_cluster_route_t *out)
{
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->advertisement = cluster_advertisement(c, r);
    out->origin_id = cluster_path(c, ri)[0];
    out->accumulated_cost = r->cost;
    uint32_t n = r->node;
    while (n != 0) { out->prefix_count++; n = c->nodes[n].parent; }
    out->warm = cluster_route_warm(c, ri);
    out->metadata.hop_ids = cluster_path(c, ri);
    out->metadata.hop_count = r->hop_count;
    out->metadata.has_route_cost = true;
    out->metadata.route_cost = out->warm ? 0 : r->cost;
}


moq_result_t
moqr_core_cluster_route(const moqr_core_t *c, moqr_binding_t requester,
    moqr_ns_t ns, moqr_cluster_route_t *out, size_t out_size)
{
    if (c == NULL || out == NULL || out_size < MOQR_CLUSTER_ROUTE_V0_SIZE || !ns_view_ok(ns, 0)) return MOQ_ERR_INVAL;
    if (!c->cluster_enabled) return MOQ_ERR_WRONG_STATE;
    uint32_t bs = UINT32_MAX, bg = 0;
    if (moqr_binding_is_valid(requester)) {
        r_binding_t *b = binding_resolve((moqr_core_t *)(uintptr_t)c, requester, &bs);
        if (b == NULL) return MOQ_ERR_STALE_HANDLE;
        bg = b->gen;
    }
    uint32_t node = cluster_longest_node(c, ns);
    if (node == UINT32_MAX) return MOQ_DONE;
    uint32_t ri = cluster_best_at(c, node, bs, bg, UINT32_MAX, 0);
    if (ri == UINT32_MAX) return MOQ_DONE;
    moqr_cluster_route_t value;
    cluster_fill(c, ri, &value);
    moqr_cluster_write(out, out_size, &value, sizeof(value));
    return MOQ_OK;
}


static uint32_t
cluster_notify_count(const moqr_core_t *c, uint32_t node)
{
    uint32_t count = 0;
    for (uint32_t i = 0; i < c->max_ns_subs; i++)
        if (c->ns_subs[i].used && prefix_matches_node(c, &c->ns_subs[i], node)) count++;
    return count;
}


static void
cluster_notify(moqr_core_t *c, uint32_t node)
{
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_ns_sub_t *s = &c->ns_subs[i];
        if (!s->used || !prefix_matches_node(c, s, node)) continue;
        uint32_t ri = cluster_best_at(c, node, s->binding, s->binding_gen,
                                      UINT32_MAX, 0);
        moqr_intent_t *it = intent_push(c, ri == UINT32_MAX ? MOQR_INTENT_NS_GONE : MOQR_INTENT_NS_FOUND);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        intent_set_ns_from_node(c, it, node);
        it->value = key_rd32(s->prefix);
    }
}


moqr_result_t
cluster_unannounce(moqr_core_t *c, uint32_t bs, uint32_t bg, uint32_t node,
    uint64_t lifetime)
{
    uint32_t found = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->used && r->node == node && r->binding == bs && r->binding_gen == bg &&
            (lifetime == 0 || r->lifetime == lifetime)) { found = i; break; }
    }
    if (found == UINT32_MAX) return MOQR_ERR_WRONG_STATE;
    uint64_t lifetime_id = c->cluster_routes[found].lifetime;
    uint64_t need = cluster_notify_count(c, node);
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        const r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) != 0 && t->cluster_route_lifetime == lifetime_id)
            need += (uint64_t)track_sub_count(c, ti) + 2u;
    }
    if (need > UINT32_MAX || !intent_space(c, (uint32_t)need)) return MOQR_ERR_WOULD_BLOCK;
    /* Retire or re-establish every captured source before its route is freed.
     * All possible intent units were reserved above, so no partial teardown. */
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) == 0 || t->cluster_route_lifetime != lifetime_id) continue;
        bool retargeted = false;
        if (t->has_upstream_binding) {
            moqr_intent_t *old = intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
            old->binding_cookie = c->bindings[t->up_binding].cookie;
            old->track = track_handle(c, ti);
            old->track_gen = t->track_gen;
            old->cookie = t->upstream_cookie;
        }
        if (t->state == R_TRACK_ACTIVE) {
            (void)track_source_failover(c, ti, bs, bg, lifetime_id, 0, &retargeted);
        } else {
            for (uint32_t si = t->subs_head; si != R_SUB_NIL;) {
                uint32_t next = c->subs[si].track_next;
                r_sub_t *sub = &c->subs[si];
                bool active = sub->state == R_SUB_ACTIVE;
                moqr_intent_t *it = intent_push(c, active ? MOQR_INTENT_SUB_DONE : MOQR_INTENT_REJECT_SUB);
                it->binding_cookie = c->bindings[sub->binding].cookie;
                it->cookie = sub->cookie;
                it->sub = sub_handle(c, si);
                it->error_code = active ? R_DONE_TRACK_ENDED : R_ERR_DOES_NOT_EXIST;
                if (active) it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, si, 0);
                si = next;
            }
        }
        if (!retargeted) {
            track_fetch_invalidate(c, ti, t->gen);
            t->has_upstream_binding = false;
            t->state = R_TRACK_WARM;
            t->source_kind = R_TRACK_SOURCE_NONE;
            t->track_gen++;
            track_touch(c, ti);
        }
    }
    uint64_t session_cookie = c->cluster_routes[found].session_cookie;
    c->cluster_routes[found].used = false;
    for (uint32_t gi = 0; gi < c->max_grants; gi++) {
        r_grant *g = &c->grants[gi];
        if ((g->gen & 1u) != 0 && g->action == MOQR_AUTH_PUBLISH_NAMESPACE &&
            g->binding_cookie == c->bindings[bs].cookie &&
            g->session_cookie == session_cookie && !(g->revoked && g->unannounced))
            grant_free_slot(c, gi);
    }
    cluster_project(c, node, cluster_best_at(c, node, UINT32_MAX, 0, UINT32_MAX, 0));
    r_bump_route_epoch(c);
    r_room_changed(c);
    cluster_notify(c, node);
    return MOQR_OK;
}


/* Count before commit, then retire after commit. Stored per-request identities
 * survive owner-shard pseudo-bindings and changing temporary admission views. */
static uint32_t
cluster_revalidate_requests(moqr_core_t *c, uint32_t ri,
    const uint64_t *ids, uint32_t count, bool commit)
{
    const r_cluster_route_t *route = &c->cluster_routes[ri];
    uint32_t terminals = 0;
    for (uint32_t si = 0; si < c->max_subs; si++) {
        r_sub_t *sub = &c->subs[si];
        if ((sub->gen & 1u) == 0 || sub->track >= c->max_tracks) continue;
        const r_track_t *t = &c->tracks[sub->track];
        if (t->gen != sub->track_gen_slot || t->cluster_route_lifetime != route->lifetime ||
            cluster_eligible_with_path(c, ri, sub->binding, sub->binding_gen,
                sub->cluster_requester_hop, sub->cluster_requester_correlation,
                UINT32_MAX, 0, ids, count)) continue;
        terminals++;
        if (!commit) continue;
        bool active = sub->state == R_SUB_ACTIVE;
        moqr_intent_t *it = intent_push(c, active ? MOQR_INTENT_SUB_DONE : MOQR_INTENT_REJECT_SUB);
        it->binding_cookie = c->bindings[sub->binding].cookie;
        it->cookie = sub->cookie;
        it->sub = sub_handle(c, si);
        it->error_code = active ? R_DONE_TRACK_ENDED : R_ERR_DOES_NOT_EXIST;
        if (active) it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
        sub_retire(c, si, 0);
    }
    if (commit) {
        for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
            r_fetch_t *f = &c->fetches[fi];
            if ((f->gen & 1u) == 0 || f->track >= c->max_tracks) continue;
            const r_track_t *t = &c->tracks[f->track];
            if (t->gen == f->track_gen_slot && t->cluster_route_lifetime == route->lifetime &&
                !cluster_eligible_with_path(c, ri, f->binding, f->binding_gen,
                    f->cluster_requester_hop, f->cluster_requester_correlation,
                    UINT32_MAX, 0, ids, count)) fetch_retire(c, f);
        }
    }
    return terminals;
}


static moqr_result_t
cluster_announce(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
    uint64_t session_cookie, const moq_cluster_metadata_t *metadata, bool update)
{
    if (c == NULL || !ns_view_ok(ns, 0)) return MOQR_ERR_INVAL;
    if (!c->cluster_enabled) return MOQR_ERR_WRONG_STATE;
    uint32_t bs;
    r_binding_t *b = binding_resolve(c, bh, &bs);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    uint32_t node = trie_walk(c, ns), ri = UINT32_MAX, free_slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (!r->used) { if (free_slot == UINT32_MAX) free_slot = i; continue; }
        if (r->node == node && r->binding == bs && r->binding_gen == b->gen && !r->mirrored) ri = i;
    }
    if (update != (ri != UINT32_MAX)) return MOQR_ERR_WRONG_STATE;
    if (update && c->cluster_routes[ri].session_cookie != session_cookie) return MOQR_ERR_WRONG_STATE;
    static const uint64_t zero = 0;
    const uint64_t *ids = metadata != NULL ? metadata->hop_ids : &zero;
    size_t count = metadata != NULL ? metadata->hop_count : 1;
    if (update && (metadata == NULL || count == 0)) {
        ids = cluster_path(c, ri);
        count = c->cluster_routes[ri].hop_count;
    }
    if (count == 0 || ids == NULL) return MOQR_ERR_INVAL;
    if (count > c->max_cluster_hops) return MOQR_ERR_CAPACITY;
    for (size_t i = 0; i < count; i++) {
        if (ids[i] == 0) continue;
        for (size_t j = 0; j < i; j++) if (ids[i] == ids[j]) return MOQR_ERR_INVAL;
    }
    if (update && ids[0] != cluster_path(c, ri)[0]) return MOQR_ERR_WRONG_STATE;
    if (cluster_contains(ids, (uint32_t)count, c->cluster_hop_id)) {
        return update ? cluster_unannounce(c, bs, b->gen, node, 0) : MOQR_OK;
    }
    if (ri == UINT32_MAX && free_slot == UINT32_MAX) return MOQR_ERR_CAPACITY;
    if (c->cluster_sequence > UINT64_MAX - (update ? 1u : 2u))
        return MOQR_INTERNAL_LIMIT;
    /* Reserve fanout before creating nodes or replacing any route bytes. */
    uint32_t need = 0;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_ns_sub_t *s = &c->ns_subs[i];
        if (s->used) {
            uint32_t pc = key_rd32(s->prefix), off = 4u * (1u + pc);
            bool match = pc <= ns.count;
            for (uint32_t p = 0; p < pc && match; p++) {
                uint32_t len = key_rd32(s->prefix + 4u * (1u + p));
                if (ns.parts[p].len != len || memcmp(ns.parts[p].data, s->prefix + off, len) != 0) match = false;
                off += len;
            }
            if (match) need++;
        }
    }
    if (update) need += cluster_revalidate_requests(c, ri, ids, (uint32_t)count, false);
    if (!intent_space(c, need)) return MOQR_ERR_WOULD_BLOCK;
    if (node == UINT32_MAX) {
        uint32_t deep, missing;
        uint64_t bytes;
        announce_need(c, ns, &deep, &missing, &bytes);
        moqr_result_t rc = r_make_room(c, false, missing, bytes, &deep);
        if (rc != MOQR_OK) return rc;
        node = 0;
        for (size_t i = 0; i < ns.count; i++) {
            rc = trie_child_get_or_create(c, node, ns.parts[i], &node);
            if (rc != MOQR_OK) return rc;
        }
    }
    if (ri == UINT32_MAX) ri = free_slot;
    r_cluster_route_t *r = &c->cluster_routes[ri];
    uint64_t seed = update ? r->seed_cost : 0;
    if (metadata != NULL && metadata->has_route_cost) seed = metadata->route_cost;
    if (!update) {
        memset(r, 0, sizeof(*r));
        r->lifetime = ++c->cluster_sequence;
    }
    /* memmove allows callers to feed back a borrowed route view. */
    memmove(cluster_path(c, ri), ids, count * sizeof(uint64_t));
    r->used = true;
    r->node = node;
    r->binding = bs;
    r->binding_gen = b->gen;
    r->session_cookie = session_cookie;
    r->hop_count = (uint32_t)count;
    r->seed_cost = seed;
    r->cost = seed > UINT64_MAX - b->relay_cost ? UINT64_MAX : seed + b->relay_cost;
    r->source_peer_hop_id = b->peer_hop_id;
    r->source_correlation_id = b->correlation_id;
    r->arrival = ++c->cluster_sequence;
    if (update) (void)cluster_revalidate_requests(c, ri, cluster_path(c, ri), r->hop_count, true);
    cluster_project(c, node, cluster_best_at(c, node, UINT32_MAX, 0, UINT32_MAX, 0));
    r_bump_route_epoch(c);
    cluster_notify(c, node);
    return MOQR_OK;
}


moq_result_t
moqr_core_announce_cluster(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
    uint64_t session_cookie, const moq_cluster_metadata_t *metadata, bool update)
{
    return moqr_result_public(cluster_announce(c, bh, ns, session_cookie, metadata, update));
}


void
cluster_pin_track(moqr_core_t *c, r_track_t *t)
{
    if (!c->cluster_enabled || t->src_ann_node == UINT32_MAX) return;
    const r_trie_node_t *node = &c->nodes[t->src_ann_node];
    uint32_t ri = node->cluster_route_index;
    if (!node->has_announce || ri >= c->max_cluster_routes) return;
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    if (r->used && r->node == t->src_ann_node &&
        r->binding == t->src_ann_binding && r->binding_gen == t->src_ann_binding_gen) {
        t->cluster_route_lifetime = r->lifetime;
        t->cluster_origin = cluster_path(c, ri)[0];
    }
}


static bool
cluster_direct_push(const moqr_core_t *c, const r_track_t *t,
    uint32_t requester, uint32_t requester_gen)
{
    if (c->cluster_forced.serial != 0 || t->source_kind != R_TRACK_SOURCE_PUSH ||
        t->state != R_TRACK_ACTIVE || t->cluster_route_lifetime != 0 ||
        !t->has_upstream_binding || t->up_binding >= c->max_bindings ||
        c->bindings[t->up_binding].gen != t->up_binding_gen) return false;
    if (requester == UINT32_MAX) return true;
    if (requester >= c->max_bindings || c->bindings[requester].gen != requester_gen ||
        (requester == t->up_binding && requester_gen == t->up_binding_gen)) return false;
    const r_binding_t *a = &c->bindings[requester], *b = &c->bindings[t->up_binding];
    return !(a->peer_hop_id != 0 && a->peer_hop_id == b->peer_hop_id) &&
           !(a->correlation_id != 0 && a->correlation_id == b->correlation_id);
}


bool
cluster_track_eligible(const moqr_core_t *c, const r_track_t *t,
    uint32_t requester, uint32_t requester_gen)
{
    if (!c->cluster_enabled) return true;
    if (cluster_direct_push(c, t, requester, requester_gen)) return true;
    moq_bytes_t parts[32];
    uint32_t count = key_part_count(&t->key);
    for (uint32_t i = 0; i < count; i++) parts[i] = key_part(&t->key, i);
    uint32_t node = cluster_longest_node(c, (moqr_ns_t){parts, count});
    if (node == UINT32_MAX) return false;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->used && r->lifetime == t->cluster_route_lifetime && r->node == node &&
            cluster_eligible(c, i, requester, requester_gen, UINT32_MAX, 0)) {
            if (cluster_forced_match(c, r)) return true;
        }
    }
    return false;
}


static uint64_t
cluster_order(moqr_ns_t ns, uint64_t id)
{
    uint64_t h = UINT64_C(14695981039346656037);
    for (size_t p = 0; p < ns.count; p++) {
        for (unsigned j = 0; j < 8; j++) { h ^= ((uint64_t)ns.parts[p].len >> (j * 8)) & 255u; h *= UINT64_C(1099511628211); }
        for (size_t i = 0; i < ns.parts[p].len; i++) { h ^= ns.parts[p].data[i]; h *= UINT64_C(1099511628211); }
    }
    for (unsigned j = 0; j < 8; j++) { h ^= (id >> (j * 8)) & 255u; h *= UINT64_C(1099511628211); }
    return h;
}


bool
cluster_should_reparent(moqr_ns_t ns, uint64_t local, uint64_t peer)
{
    if (local == peer || !ns_view_ok(ns, 0)) return false;
    uint64_t a = cluster_order(ns, local), b = cluster_order(ns, peer);
    return a == b ? local > peer : a > b;
}


void
cluster_warm_flush(moqr_core_t *c)
{
    if (!c->cluster_enabled) return;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        r_cluster_route_t *r = &c->cluster_routes[i];
        if (!r->used) continue;
        bool warm = cluster_route_warm(c, i);
        if (warm == r->warm_notified) continue;
        if (!intent_space(c, cluster_notify_count(c, r->node))) continue;
        r->warm_notified = warm;
        r_bump_route_epoch(c);
        cluster_notify(c, r->node);
    }
}


moqr_result_t
moqr_core_internal_cluster_export(const moqr_core_t *c, uint32_t index,
    moqr_cluster_export_t *out)
{
    if (c == NULL || out == NULL || !c->cluster_enabled) return MOQR_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    out->source_index = index;
    if (index >= c->max_cluster_routes || !c->cluster_routes[index].used)
        return MOQR_DONE;
    const r_cluster_route_t *r = &c->cluster_routes[index];
    /* Do not recursively mirror mirrors. */
    if (r->mirrored) return MOQR_DONE;
    out->revision = r->arrival;
    cluster_fill(c, index, &out->route);
    uint32_t node = r->node, count = out->route.prefix_count;
    while (node != 0) {
        out->ns_parts[--count] = (moq_bytes_t){c->nodes[node].part, c->nodes[node].part_len};
        node = c->nodes[node].parent;
    }
    out->ns.parts = out->ns_parts;
    out->ns.count = out->route.prefix_count;
    out->source_session_cookie = r->session_cookie;
    out->source_peer_hop_id = r->source_peer_hop_id;
    out->source_correlation_id = r->source_correlation_id;
    return MOQR_OK;
}


moqr_result_t
moqr_core_internal_cluster_import(moqr_core_t *c, moqr_binding_t bh,
    const moqr_cluster_export_t *record, bool withdraw)
{
    if (c == NULL || record == NULL || !c->cluster_enabled) return MOQR_ERR_INVAL;
    uint32_t bs;
    r_binding_t *b = binding_resolve(c, bh, &bs);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    if (b->cookie < MOQR_SHARD_COOKIE_BASE) return MOQR_ERR_INVAL;
    uint32_t ri = UINT32_MAX, free_slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (!r->used) { if (free_slot == UINT32_MAX) free_slot = i; continue; }
        if (r->mirrored && r->binding == bs && r->binding_gen == b->gen &&
            r->mirror_source_index == record->source_index) ri = i;
    }
    if (withdraw || record->route.advertisement.serial == 0) {
        return ri == UINT32_MAX ? MOQR_OK : cluster_unannounce(c, bs, b->gen,
            c->cluster_routes[ri].node, c->cluster_routes[ri].lifetime);
    }
    if (!moqr_binding_is_valid(record->route.advertisement.source) ||
        moq_handle_pool_tag(record->route.advertisement.source._opaque) != MOQR_HANDLE_POOL_BINDING ||
        moq_handle_session_tag(record->route.advertisement.source._opaque) != b->cookie - MOQR_SHARD_COOKIE_BASE + 1u)
        return MOQR_ERR_INVAL;
    if (ri != UINT32_MAX && !moqr_cluster_advertisement_id_eq(
            c->cluster_routes[ri].original, record->route.advertisement))
        return MOQR_ERR_WRONG_STATE; /* withdraw old lifetime before replacement */
    if (!ns_view_ok(record->ns, 0) || record->route.metadata.hop_count == 0 ||
        record->route.metadata.hop_ids == NULL) return MOQR_ERR_INVAL;
    if (record->route.metadata.hop_count > c->max_cluster_hops) return MOQR_ERR_CAPACITY;
    if (ri == UINT32_MAX && free_slot == UINT32_MAX) return MOQR_ERR_CAPACITY;
    if (c->cluster_sequence > UINT64_MAX - (ri != UINT32_MAX ? 1u : 2u))
        return MOQR_INTERNAL_LIMIT;
    uint32_t node = trie_walk(c, record->ns);
    /* A source index keeps its namespace for its advertisement lifetime. A
     * replacement is explicitly withdrawn first by the shard mailbox. */
    if (ri != UINT32_MAX && node != c->cluster_routes[ri].node) return MOQR_ERR_WRONG_STATE;
    uint32_t update_terminals = ri != UINT32_MAX
        ? cluster_revalidate_requests(c, ri, record->route.metadata.hop_ids,
            (uint32_t)record->route.metadata.hop_count, false) : 0;
    uint64_t update_intents = update_terminals + (uint64_t)(node == UINT32_MAX
        ? c->max_ns_subs : cluster_notify_count(c, node));
    if (update_intents > UINT32_MAX || !intent_space(c, (uint32_t)update_intents)) return MOQR_ERR_WOULD_BLOCK;
    if (node == UINT32_MAX) {
        uint32_t deep, missing;
        uint64_t bytes;
        announce_need(c, record->ns, &deep, &missing, &bytes);
        moqr_result_t rc = r_make_room(c, false, missing, bytes, &deep);
        if (rc != MOQR_OK) return rc;
        node = 0;
        for (size_t i = 0; i < record->ns.count; i++) {
            rc = trie_child_get_or_create(c, node, record->ns.parts[i], &node);
            if (rc != MOQR_OK) return rc;
        }
    }
    if (ri == UINT32_MAX) ri = free_slot;
    r_cluster_route_t *r = &c->cluster_routes[ri];
    bool same = r->used && moqr_cluster_advertisement_id_eq(r->original, record->route.advertisement);
    uint64_t lifetime = same ? r->lifetime : ++c->cluster_sequence;
    memmove(cluster_path(c, ri), record->route.metadata.hop_ids,
        record->route.metadata.hop_count * sizeof(uint64_t));
    memset(r, 0, sizeof(*r));
    r->used = true;
    r->mirrored = true;
    r->inherited_warm = record->route.warm;
    r->warm_notified = r->inherited_warm;
    r->node = node;
    r->binding = bs;
    r->binding_gen = b->gen;
    r->hop_count = (uint32_t)record->route.metadata.hop_count;
    r->mirror_source_index = record->source_index;
    r->lifetime = lifetime;
    r->original = record->route.advertisement;
    r->session_cookie = record->source_session_cookie;
    r->source_peer_hop_id = record->source_peer_hop_id;
    r->source_correlation_id = record->source_correlation_id;
    r->cost = record->route.accumulated_cost;
    r->seed_cost = record->route.metadata.route_cost;
    r->arrival = ++c->cluster_sequence;
    if (same) (void)cluster_revalidate_requests(c, ri, cluster_path(c, ri), r->hop_count, true);
    cluster_project(c, node, cluster_best_at(c, node, UINT32_MAX, 0, UINT32_MAX, 0));
    r_bump_route_epoch(c);
    cluster_notify(c, node);
    return MOQR_OK;
}


moqr_result_t
moqr_core_internal_cluster_upstream_route(const moqr_core_t *c, moqr_track_t th,
    uint64_t track_gen, moqr_cluster_advertisement_id_t *out, uint64_t *requester_hop_id,
    uint64_t *requester_correlation_id)
{
    if (c == NULL || out == NULL || requester_hop_id == NULL ||
        requester_correlation_id == NULL || !c->cluster_enabled) return MOQR_ERR_INVAL;
    r_track_t *t = track_resolve((moqr_core_t *)(uintptr_t)c, th, NULL);
    if (t == NULL || t->track_gen != track_gen) return MOQR_ERR_STALE_HANDLE;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->used && r->lifetime == t->cluster_route_lifetime) {
            *out = cluster_advertisement(c, r);
            if (t->subs_head != R_SUB_NIL) {
                const r_sub_t *requester = &c->subs[t->subs_head];
                *requester_hop_id = requester->cluster_requester_hop;
                *requester_correlation_id = requester->cluster_requester_correlation;
            } else {
                *requester_hop_id = t->cluster_requester_hop;
                *requester_correlation_id = t->cluster_requester_correlation;
            }
            return MOQR_OK;
        }
    }
    return MOQR_DONE;
}


uint32_t
cluster_failover_node(moqr_core_t *c, moqr_ns_t ns, const r_track_t *t,
    uint32_t excluded, uint32_t excluded_gen, uint64_t excluded_lifetime)
{
    if (t->cluster_origin == 0 || (t->has_largest && t->largest_group == UINT64_MAX))
        return UINT32_MAX;
    uint32_t node = cluster_longest_node(c, ns), best = UINT32_MAX;
    if (node == UINT32_MAX) return node;
    uint32_t excluded_binding = excluded_lifetime != 0 ? UINT32_MAX : excluded;
    for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
        const r_cluster_route_t *r = &c->cluster_routes[ri];
        if ((excluded_lifetime != 0 && r->lifetime == excluded_lifetime) ||
            r->node != node || !cluster_eligible(c, ri, UINT32_MAX, 0,
                                                excluded_binding, excluded_gen) ||
            cluster_path(c, ri)[0] != t->cluster_origin) continue;
        /* Current subscribers are authoritative. The track's original
         * requester may already have retired from a coalesced ingest. */
        bool eligible = t->subs_head != R_SUB_NIL ||
            (!cluster_contains(cluster_path(c, ri), r->hop_count,
                               t->cluster_requester_hop) &&
             (t->cluster_requester_correlation == 0 ||
              t->cluster_requester_correlation != r->source_correlation_id));
        for (uint32_t si = t->subs_head; si != R_SUB_NIL && eligible;
             si = c->subs[si].track_next) {
            const r_sub_t *s = &c->subs[si];
            eligible = cluster_eligible_with_path(c, ri, s->binding, s->binding_gen,
                s->cluster_requester_hop, s->cluster_requester_correlation,
                excluded_binding, excluded_gen, cluster_path(c, ri), r->hop_count);
        }
        if (!eligible) continue;
        if (best == UINT32_MAX) { best = ri; continue; }
        const r_cluster_route_t *old = &c->cluster_routes[best];
        bool known = cluster_known(c, ri), old_known = cluster_known(c, best);
        if ((known && !old_known) || (known == old_known &&
            (r->cost < old->cost || (r->cost == old->cost &&
             (r->hop_count < old->hop_count || (r->hop_count == old->hop_count &&
              r->arrival > old->arrival)))))) best = ri;
    }
    if (best == UINT32_MAX) return UINT32_MAX;
    cluster_project(c, node, best);
    return node;
}


void
cluster_reparent_sweep(moqr_core_t *c)
{
    if (!c->cluster_enabled) return;
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) == 0 || t->state != R_TRACK_ACTIVE ||
            !t->has_upstream_binding || track_sub_count(c, ti) == 0 ||
            t->cluster_origin == 0) continue;
        uint32_t current = UINT32_MAX;
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++)
            if (c->cluster_routes[ri].used && c->cluster_routes[ri].lifetime == t->cluster_route_lifetime)
                current = ri;
        if (current == UINT32_MAX) continue;
        moq_bytes_t parts[32];
        uint32_t count = key_part_count(&t->key);
        for (uint32_t p = 0; p < count; p++) parts[p] = key_part(&t->key, p);
        moqr_ns_t ns = {parts, count};
        uint32_t node = cluster_failover_node(c, ns, t, t->up_binding, t->up_binding_gen, t->cluster_route_lifetime);
        if (node == UINT32_MAX) continue;
        uint32_t target = c->nodes[node].cluster_route_index;
        if (target == UINT32_MAX) continue;
        const r_cluster_route_t *r = &c->cluster_routes[target];
        if (r->cost >= c->cluster_routes[current].cost) continue;
        const uint64_t *ids = cluster_path(c, target);
        if (r->seed_cost == 0 && r->hop_count >= 2 &&
            !cluster_should_reparent(ns, c->cluster_hop_id, ids[r->hop_count - 1])) continue;
        if (!intent_space(c, 2)) continue;
        moqr_intent_t *old = intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
        old->binding_cookie = c->bindings[t->up_binding].cookie;
        old->track = track_handle(c, ti);
        old->track_gen = t->track_gen;
        old->cookie = t->upstream_cookie;
        bool retargeted;
        (void)track_source_failover(c, ti, t->up_binding, t->up_binding_gen, t->cluster_route_lifetime, 0, &retargeted);
    }
}


uint32_t
cluster_track_lookup(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name,
    uint32_t requester, uint32_t requester_gen, uint64_t *hash_out)
{
    uint64_t hash = key_view_hash(ns, name);
    if (hash_out != NULL) *hash_out = hash;
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        const r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) != 0 && t->hash == hash && key_view_eq(&t->key, ns, name) &&
            cluster_direct_push(c, t, requester, requester_gen)) return ti;
    }
    uint32_t node = cluster_longest_node(c, ns);
    uint32_t ri = node != UINT32_MAX ? cluster_best_at(c, node, requester,
        requester_gen, UINT32_MAX, 0) : UINT32_MAX;
    if (ri == UINT32_MAX) return UINT32_MAX;
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        const r_track_t *t = &c->tracks[ti];
        if ((t->gen & 1u) != 0 && t->hash == hash &&
            key_view_eq(&t->key, ns, name) &&
            t->cluster_route_lifetime == c->cluster_routes[ri].lifetime &&
            cluster_track_eligible(c, t, requester, requester_gen)) return ti;
    }
    return UINT32_MAX;
}


moqr_result_t
moqr_core_announce_room(const moqr_core_t *c, moqr_ns_t ns)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    if (c->cluster_enabled) {
        bool free_route = false;
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++)
            if (!c->cluster_routes[ri].used) { free_route = true; break; }
        if (!free_route) return MOQR_ERR_CAPACITY;
    }
    uint32_t deep = 0, missing = 0;
    uint64_t missing_bytes = 0;
    announce_need(c, ns, &deep, &missing, &missing_bytes);
    if (missing == 0) {
        return MOQR_OK;
    }
    moqr_refuse_reason_t reason = MOQR_REFUSE_NS_NODES;
    return r_room_check(c, false, missing, missing_bytes, deep, &reason);
}


moqr_result_t
moqr_core_announce(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns)
{
    return moqr_core_announce_ex(c, bh, ns, 0);
}


moqr_result_t
moqr_core_announce_ex(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                      uint64_t session_cookie)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    if (c->cluster_enabled)
        return moqr_result_legacy(moqr_core_announce_cluster(c, bh, ns, session_cookie, NULL, false));
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Count matching ns-subs for fan-out reservation; the trie walk for
     * counting must not create nodes, so match against parts directly. */
    uint32_t need = 0;
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (!c->ns_subs[s].used) {
            continue;
        }
        uint32_t pcount = key_rd32(c->ns_subs[s].prefix);
        if (pcount > ns.count) {
            continue;
        }
        bool match = true;
        uint32_t off = 4u * (1u + pcount);
        for (uint32_t i = 0; i < pcount && match; i++) {
            uint32_t plen = key_rd32(c->ns_subs[s].prefix + 4u * (1u + i));
            if (ns.parts[i].len != plen ||
                (plen != 0 &&
                 memcmp(ns.parts[i].data, c->ns_subs[s].prefix + off,
                        plen) != 0)) {
                match = false;
            }
            off += plen;
        }
        if (match) {
            need++;
        }
    }
    if (!intent_space(c, need)) {
        return MOQR_ERR_WOULD_BLOCK;
    }

    /* The nodes and name bytes this announce would add. When they are not
     * free, make room before anything is created, keeping the deepest
     * existing node of this namespace's path (every shallower one is an
     * ancestor with a child). WOULD_BLOCK and CAPACITY leave state untouched. */
    {
        uint32_t deep = 0;
        uint32_t missing = 0;
        uint64_t missing_bytes = 0;
        announce_need(c, ns, &deep, &missing, &missing_bytes);
        if (missing != 0) {
            moqr_result_t rrc =
                r_make_room(c, false, missing, missing_bytes, &deep);
            if (rrc != MOQR_OK) {
                return rrc;
            }
        }
    }

    uint32_t node = 0;
    for (size_t i = 0; i < ns.count; i++) {
        moqr_result_t rc = trie_child_get_or_create(c, node, ns.parts[i],
                                                    &node);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    if (c->nodes[node].has_announce) {
        return MOQR_ERR_WRONG_STATE;   /* occupied namespace */
    }
    c->nodes[node].has_announce = true;
    c->nodes[node].ann_binding = bslot;
    c->nodes[node].ann_binding_gen = b->gen;
    c->nodes[node].session_cookie = session_cookie;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 0 /* announce */, node, bslot, 0, 0);

    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used && prefix_matches_node(c, &c->ns_subs[s],
                                                      node)) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_FOUND);
            it->binding_cookie = c->bindings[c->ns_subs[s].binding].cookie;
            it->cookie = c->ns_subs[s].cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(c->ns_subs[s].prefix);
        }
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_unannounce(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t node = trie_walk(c, ns);
    if (c->cluster_enabled)
        return cluster_unannounce(c, bslot, b->gen, node, 0);
    if (node == UINT32_MAX || !c->nodes[node].has_announce ||
        c->nodes[node].ann_binding != bslot ||
        c->nodes[node].ann_binding_gen != b->gen) {
        return MOQR_ERR_WRONG_STATE;
    }
    uint32_t need = 0;
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used &&
            prefix_matches_node(c, &c->ns_subs[s], node)) {
            need++;
        }
    }
    if (!intent_space(c, need)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint64_t ann_session_cookie = c->nodes[node].session_cookie;
    c->nodes[node].has_announce = false;
    r_room_changed(c);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0, node, bslot, 0, 0);
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used &&
            prefix_matches_node(c, &c->ns_subs[s], node)) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
            it->binding_cookie = c->bindings[c->ns_subs[s].binding].cookie;
            it->cookie = c->ns_subs[s].cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(c->ns_subs[s].prefix);
        }
    }
    /* A voluntary withdrawal retires the announce's OWN revalidation
     * grant: the authorization it re-checks no longer exists, and a grant
     * that outlives its announce could later revalidate to DENY and
     * withdraw a REPLACEMENT publisher's namespace. (A mid-teardown
     * cancel-vehicle grant — revoked + unannounced — is left to its own
     * peek/ack; its announce is already gone, so it cannot reach here.) */
    for (uint32_t gi = 0; gi < c->max_grants; gi++) {
        r_grant *g = &c->grants[gi];
        if ((g->gen & 1u) != 0 &&
            g->action == MOQR_AUTH_PUBLISH_NAMESPACE &&
            g->binding_cookie == c->bindings[bslot].cookie &&
            g->session_cookie == ann_session_cookie &&
            !(g->revoked && g->unannounced)) {
            grant_free_slot(c, gi);
        }
    }
    return MOQR_OK;
}


/* A wire cancel for {binding_cookie, session_cookie} is already peekable — from
 * either a revoked+unannounced grant or a queued pending cancel — so a fresh
 * force-withdraw must not enqueue a duplicate. */
static bool
cancel_already_pending(const moqr_core_t *c, uint64_t binding_cookie,
                       uint64_t session_cookie)
{
    for (uint32_t i = 0; i < c->max_grants; i++) {
        const r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced &&
            g->binding_cookie == binding_cookie &&
            g->session_cookie == session_cookie) {
            return true;
        }
    }
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        const r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used && pc->binding_cookie == binding_cookie &&
            pc->session_cookie == session_cookie) {
            return true;
        }
    }
    return false;
}


static uint32_t
cancel_free_slot(const moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        if (!c->pending_cancels[i].used) {
            return i;
        }
    }
    return UINT32_MAX;
}


/* A track is purged by force-withdraw iff its STORED source-announce identity is
 * the withdrawn node + owner (not a re-derived longest-prefix). */
static bool
track_sourced_by(const r_track_t *t, uint32_t node, uint32_t bslot, uint32_t bgen)
{
    return (t->gen & 1u) != 0 && t->src_ann_node == node &&
           t->src_ann_binding == bslot && t->src_ann_binding_gen == bgen;
}


/* Relay-topology force-withdraw of an announce: purge every track it sourced
 * (per-state terminals + fetch invalidation + slot free), clear the announce, fan
 * NS_GONE, and wire-cancel the publisher. Resumable, one track per reserved
 * unit: a short intent ring or a full cancel queue returns WOULD_BLOCK with the
 * tracks purged so far already committed, and the caller drains and retries to
 * completion. Only the FIRST unit is preflighted before any mutation, and the
 * final namespace effects wait for every unit. No-op OK on a missing /
 * no-announce / mirror-owned node.
 *
 * request_error is a REQUEST_ERROR-registry code and feeds exactly the two
 * wire messages that take that registry: the parked subscribers' REJECT_SUB
 * and the publisher-side cancel_namespace. Active subscribers' SUB_DONE is a
 * PUBLISH_DONE-registry message and always carries TRACK_ENDED — the track
 * stops being published, whatever prompted the withdrawal. */
moqr_result_t
moqr_core_force_withdraw(moqr_core_t *c, moqr_ns_t ns, uint64_t request_error,
                         uint64_t now_us)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t node = trie_walk(c, ns);
    if (node == UINT32_MAX || !c->nodes[node].has_announce) {
        return MOQR_OK;   /* nothing announced here */
    }
    uint32_t ann_bslot = c->nodes[node].ann_binding;
    uint32_t ann_bgen = c->nodes[node].ann_binding_gen;
    uint64_t session_cookie = c->nodes[node].session_cookie;
    uint64_t owner_cookie = c->bindings[ann_bslot].cookie;
    if (owner_cookie >= MOQR_SHARD_COOKIE_BASE) {
        return MOQR_OK;   /* mirror-owned pseudo-binding: never force-withdraw */
    }

    /* -- Resumable purge: one track per reserved unit -- *
     * Each unit (a track's subscriber terminals + its upstream release)
     * fits an empty ring under the create clamp; ring exhaustion returns
     * WOULD_BLOCK with PARTIAL progress — already-purged tracks are freed
     * and no longer match, so the caller drains and retries to
     * completion, exactly like binding_close. The announce clear + its
     * NS_GONE fan-out is the FINAL unit, reserved together with the
     * publisher cancel slot, so the namespace state never clears while a
     * sourced track still stands. */
    uint32_t ns_gone = 0;
    for (uint32_t si = 0; si < c->max_ns_subs; si++) {
        if (c->ns_subs[si].used &&
            prefix_matches_node(c, &c->ns_subs[si], node)) {
            ns_gone++;
        }
    }
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        r_track_t *t = &c->tracks[ti];
        if (!track_sourced_by(t, node, ann_bslot, ann_bgen)) {
            continue;
        }
        uint32_t unit = 0;
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) != 0 && s->track == ti &&
                    s->track_gen_slot == t->gen && s->state == R_SUB_PARKED) {
                    unit++;
                }
            }
        } else if (t->state == R_TRACK_ACTIVE) {
            unit += track_sub_count(c, ti);
        }
        if (t->has_upstream_binding &&
            (c->bindings[t->up_binding].gen & 1u) != 0) {
            unit++;
        }
        if (!intent_space(c, unit)) {
            return MOQR_ERR_WOULD_BLOCK;   /* partial; drain and retry */
        }
        uint32_t tgen = t->gen;
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) == 0 || s->track != ti ||
                    s->track_gen_slot != tgen || s->state != R_SUB_PARKED) {
                    continue;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
                it->binding_cookie = c->bindings[s->binding].cookie;
                it->cookie = s->cookie;
                it->error_code = request_error;
                sub_retire(c, si, now_us);
            }
        } else if (t->state == R_TRACK_ACTIVE) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) == 0 || s->track != ti ||
                    s->track_gen_slot != tgen || s->state != R_SUB_ACTIVE) {
                    continue;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = c->bindings[s->binding].cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, si);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, si, now_us);
            }
        }
        /* Fetches on this track: unpin + invalidate the handle so no held object
         * replays (next peek is STALE_HANDLE, before the track-gone guard). */
        for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
            r_fetch_t *f = &c->fetches[fi];
            if ((f->gen & 1u) != 0 && f->track == ti &&
                f->track_gen_slot == tgen) {
                fetch_retire(c, f);   /* invalidate the cursor handle */
            }
        }
        /* Release the upstream subscription: without this the binding's
         * upstream slot (and the publisher session's live subscription)
         * outlives the purged track, and the NEXT generation's fresh
         * demand dies as a duplicate — an accepted re-announcement that
         * can never carry data again. */
        if (t->has_upstream_binding &&
            (c->bindings[t->up_binding].gen & 1u) != 0) {
            moqr_intent_t *ui =
                intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
            ui->track = track_handle(c, ti);
            ui->track_gen = t->track_gen;
            ui->cookie = t->upstream_cookie;
            ui->binding_cookie = c->bindings[t->up_binding].cookie;
        }
        track_free_slot(c, ti);   /* pool-gen bump orphans any stragglers */
    }

    /* Final unit: the announce clear + NS_GONE fan-out + publisher cancel. */
    if (!intent_space(c, ns_gone)) {
        return MOQR_ERR_WOULD_BLOCK;   /* tracks already purged stay purged */
    }
    bool need_cancel = !cancel_already_pending(c, owner_cookie, session_cookie);
    uint32_t cancel_slot = UINT32_MAX;
    if (need_cancel) {
        cancel_slot = cancel_free_slot(c);
        if (cancel_slot == UINT32_MAX) {
            return MOQR_ERR_WOULD_BLOCK;   /* transient: queue drains on ack */
        }
    }

    /* Clear the announce (bypass the owner check) + NS_GONE fan-out. */
    if (c->cluster_enabled) {
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            r_cluster_route_t *r = &c->cluster_routes[ri];
            if (r->used && r->node == node && r->binding == ann_bslot &&
                r->binding_gen == ann_bgen) r->used = false;
        }
    }
    c->nodes[node].has_announce = false;
    r_room_changed(c);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0 /* announce */, node, ann_bslot, 0, 0);
    for (uint32_t si = 0; si < c->max_ns_subs; si++) {
        if (!c->ns_subs[si].used ||
            !prefix_matches_node(c, &c->ns_subs[si], node)) {
            continue;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
        it->binding_cookie = c->bindings[c->ns_subs[si].binding].cookie;
        it->cookie = c->ns_subs[si].cookie;
        intent_set_ns_from_node(c, it, node);
        it->value = key_rd32(c->ns_subs[si].prefix);
    }

    if (c->cluster_enabled)
        cluster_project(c, node, cluster_best_at(c, node, UINT32_MAX, 0, UINT32_MAX, 0));

    /* Wire-cancel the publisher (unless already pending) + retire its live grant. */
    if (need_cancel) {
        r_pending_cancel *pc = &c->pending_cancels[cancel_slot];
        pc->used = true;
        pc->binding_cookie = owner_cookie;
        pc->session_cookie = session_cookie;
        pc->error_code = request_error;
        /* A live / mid-teardown PUBLISH_NAMESPACE grant would keep revalidating a
         * withdrawn namespace: retire exactly that one grant (an already
         * revoked+unannounced grant is the dedupe case and is left to its own ack;
         * never retire_grants(binding) — that would kill other namespaces). */
        for (uint32_t gi = 0; gi < c->max_grants; gi++) {
            r_grant *g = &c->grants[gi];
            if ((g->gen & 1u) != 0 &&
                g->action == MOQR_AUTH_PUBLISH_NAMESPACE &&
                g->binding_cookie == owner_cookie &&
                g->session_cookie == session_cookie &&
                !(g->revoked && g->unannounced)) {
                grant_free_slot(c, gi);
            }
        }
    }
    return MOQR_OK;
}


/* Collect announces under the new ns-sub's prefix node (recursive).
 * Counting needs no allocated subscription prefix. */
static void
nsub_fanout_existing(moqr_core_t *c, const r_ns_sub_t *nsub, uint32_t node,
                     bool counting, uint32_t *count)
{
    if (c->nodes[node].has_announce) {
        if (counting) {
            (*count)++;
        } else {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_FOUND);
            it->binding_cookie = c->bindings[nsub->binding].cookie;
            it->cookie = nsub->cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(nsub->prefix);
        }
    }
    for (uint32_t k = c->nodes[node].first_child; k != R_NODE_NIL;
         k = c->nodes[k].next_sib) {
        nsub_fanout_existing(c, nsub, k, counting, count);
    }
}


moqr_result_t
moqr_core_ns_subscribe(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t prefix,
                       uint64_t cookie)
{
    if (c == NULL || !ns_view_ok(prefix, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            slot = i;
            break;
        }
    }
    if (slot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_NS_SUBS);
    }

    /* Reserve fan-out before reclaiming cache or allocating the prefix. */
    uint32_t node = trie_walk(c, prefix);
    uint32_t need = 0;
    if (node != UINT32_MAX) {
        nsub_fanout_existing(c, NULL, node, /*counting=*/true, &need);
    }
    if (!intent_space(c, need)) {
        return MOQR_ERR_WOULD_BLOCK;
    }

    /* Build the prefix buffer (key layout without name). */
    uint32_t plen = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        plen += (uint32_t)prefix.parts[i].len;
    }
    moqr_result_t mrc = r_make_room(c, false, 0, plen, NULL);
    if (mrc != MOQR_OK) {
        return mrc;
    }
    uint8_t *pbuf = r_alloc(c, plen);
    if (pbuf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    key_wr32(pbuf, (uint32_t)prefix.count);
    uint32_t off = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        key_wr32(pbuf + 4u * (1u + i), (uint32_t)prefix.parts[i].len);
        if (prefix.parts[i].len != 0) {
            memcpy(pbuf + off, prefix.parts[i].data, prefix.parts[i].len);
        }
        off += (uint32_t)prefix.parts[i].len;
    }

    r_ns_sub_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.binding = bslot;
    probe.binding_gen = b->gen;
    probe.cookie = cookie;
    probe.prefix = pbuf;
    probe.prefix_len = plen;

    c->ns_subs[slot] = probe;
    c->ns_subs[slot].used = true;
    c->intern_used += plen;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 2 /* ns_sub */, slot, bslot, cookie, 0);
    /* Reclaim may have compacted the trie; re-resolve its prefix node. */
    node = trie_walk(c, prefix);
    if (node != UINT32_MAX) {
        nsub_fanout_existing(c, &c->ns_subs[slot], node, /*counting=*/false,
                             NULL);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_ns_unsubscribe(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        r_ns_sub_t *s = &c->ns_subs[i];
        if (s->used && s->binding == bslot && s->binding_gen == b->gen &&
            s->cookie == cookie) {
            c->intern_used -= s->prefix_len;
            r_free(c, s->prefix, s->prefix_len);
            r_room_changed(c);
            memset(s, 0, sizeof(*s));
            r_bump_route_epoch(c);
            r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 2, i, bslot, cookie, 0);
            return MOQR_OK;
        }
    }
    return MOQR_ERR_STALE_HANDLE;
}


/* -- ops: namespace subscription update, track discovery, request tokens -- */

/* The canonical ns-only prefix buffer (u32 count | u32 lens | bytes) that
 * namespace and track subscriptions store. */
static uint32_t
prefix_buf_len(moqr_ns_t prefix)
{
    uint32_t len = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        len += (uint32_t)prefix.parts[i].len;
    }
    return len;
}


static void
prefix_buf_write(uint8_t *buf, moqr_ns_t prefix)
{
    key_wr32(buf, (uint32_t)prefix.count);
    uint32_t off = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        key_wr32(buf + 4u * (1u + i), (uint32_t)prefix.parts[i].len);
        if (prefix.parts[i].len != 0) {
            memcpy(buf + off, prefix.parts[i].data, prefix.parts[i].len);
        }
        off += (uint32_t)prefix.parts[i].len;
    }
}


/* Does the stored prefix buffer prefix the namespace parts? */
static bool
prefix_buf_matches(const uint8_t *buf, const moq_bytes_t *parts, uint32_t count)
{
    uint32_t pcount = key_rd32(buf);
    if (pcount > count) {
        return false;
    }
    uint32_t off = 4u * (1u + pcount);
    for (uint32_t i = 0; i < pcount; i++) {
        uint32_t plen = key_rd32(buf + 4u * (1u + i));
        if (parts[i].len != plen ||
            (plen != 0 && memcmp(parts[i].data, buf + off, plen) != 0)) {
            return false;
        }
        off += plen;
    }
    return true;
}


static uint32_t
ns_sub_find(const moqr_core_t *c, uint32_t bslot, uint32_t bgen, uint64_t cookie)
{
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_ns_sub_t *n = &c->ns_subs[i];
        if (n->used && n->binding == bslot && n->binding_gen == bgen &&
            n->cookie == cookie) {
            return i;
        }
    }
    return UINT32_MAX;
}


/* Announces under `probe`'s prefix that `old` (when non-NULL) did not match. */
static void
nsub_fanout_new(moqr_core_t *c, const r_ns_sub_t *probe, const r_ns_sub_t *old,
                uint32_t node, bool counting, uint32_t *count)
{
    if (c->nodes[node].has_announce && prefix_matches_node(c, probe, node) &&
        (old == NULL || !prefix_matches_node(c, old, node))) {
        if (counting) {
            (*count)++;
        } else {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_FOUND);
            it->binding_cookie = c->bindings[probe->binding].cookie;
            it->cookie = probe->cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(probe->prefix);
        }
    }
    for (uint32_t k = c->nodes[node].first_child; k != R_NODE_NIL;
         k = c->nodes[k].next_sib) {
        nsub_fanout_new(c, probe, old, k, counting, count);
    }
}


moqr_result_t
moqr_core_ns_update_prepare(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                            moqr_ns_t prefix, moqr_ns_update_prep_t *prep)
{
    if (prep != NULL) {
        memset(prep, 0, sizeof(*prep));
    }
    if (c == NULL || prep == NULL || !ns_view_ok(prefix, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = ns_sub_find(c, bslot, b->gen, cookie);
    if (slot == UINT32_MAX) {
        return MOQR_ERR_STALE_HANDLE;
    }
    r_ns_sub_t *n = &c->ns_subs[slot];
    uint32_t plen = prefix_buf_len(prefix);
    if ((uint64_t)c->intern_used - n->prefix_len + plen > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }
    uint8_t *pbuf = r_alloc(c, plen);
    if (pbuf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    prefix_buf_write(pbuf, prefix);
    r_ns_sub_t probe = *n;
    probe.prefix = pbuf;
    probe.prefix_len = plen;
    uint32_t need = 0;
    nsub_fanout_new(c, &probe, n, 0, /*counting=*/true, &need);
    if (!intent_space(c, need)) {
        r_free(c, pbuf, plen);
        return MOQR_ERR_WOULD_BLOCK;
    }
    c->intent_reserved += need;
    prep->staged = true;
    prep->binding = bh;
    prep->cookie = cookie;
    prep->prefix_buf = pbuf;
    prep->prefix_len = plen;
    prep->intents = need;
    return MOQR_OK;
}


void
moqr_core_ns_update_abort(moqr_core_t *c, moqr_ns_update_prep_t *prep)
{
    if (c == NULL || prep == NULL || !prep->staged) {
        return;
    }
    c->intent_reserved -= prep->intents;
    r_free(c, prep->prefix_buf, prep->prefix_len);
    memset(prep, 0, sizeof(*prep));
}


moqr_result_t
moqr_core_ns_update_commit(moqr_core_t *c, moqr_ns_update_prep_t *prep)
{
    if (c == NULL || prep == NULL || !prep->staged) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, prep->binding, &bslot);
    uint32_t slot = b != NULL ? ns_sub_find(c, bslot, b->gen, prep->cookie)
                              : UINT32_MAX;
    if (slot == UINT32_MAX) {
        moqr_core_ns_update_abort(c, prep);
        return MOQR_ERR_STALE_HANDLE;
    }
    /* The fan-out's intent room was held since prepare. */
    c->intent_reserved -= prep->intents;
    uint64_t cookie = prep->cookie;
    uint8_t *pbuf = prep->prefix_buf;
    uint32_t plen = prep->prefix_len;
    memset(prep, 0, sizeof(*prep));
    r_ns_sub_t *n = &c->ns_subs[slot];
    /* Queued notifications not yet polled are re-expressed against the new
     * prefix, or withdrawn with their namespace's membership. Order kept. */
    uint32_t pcount = key_rd32(pbuf);
    uint32_t w = 0;
    for (uint32_t r = 0; r < c->intent_count; r++) {
        moqr_intent_t *it = &c->intents[(c->intent_head + r) % c->max_intents];
        bool keep = true;
        if ((it->kind == MOQR_INTENT_NS_FOUND || it->kind == MOQR_INTENT_NS_GONE) &&
            it->binding_cookie == b->cookie && it->cookie == cookie) {
            keep = prefix_buf_matches(pbuf, it->ns_parts, it->ns_count);
            if (keep) {
                it->value = (uint64_t)pcount;
            }
        }
        if (keep) {
            if (w != r) {
                c->intents[(c->intent_head + w) % c->max_intents] = *it;
            }
            w++;
        }
    }
    c->intent_count = w;
    r_ns_sub_t old = *n;
    n->prefix = pbuf;
    n->prefix_len = plen;
    c->intern_used = c->intern_used - old.prefix_len + plen;
    /* What the old prefix already reported is not reported again. */
    nsub_fanout_new(c, n, &old, 0, /*counting=*/false, NULL);
    r_free(c, old.prefix, old.prefix_len);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 4 /* ns_sub update */, slot, bslot, cookie, 0);
    return MOQR_OK;
}


/* Every binding with a track subscription may now have an unoffered match. */
void
discovery_mark_all(moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_tsub_t *t = &c->tsubs[i];
        if (t->used && t->binding < c->max_bindings &&
            c->bindings[t->binding].gen == t->binding_gen) {
            c->bindings[t->binding].discovery_dirty = true;
        }
    }
}


static r_offer_t *
offer_at(moqr_core_t *c, uint32_t tsub, uint32_t tslot)
{
    return &c->offers[(size_t)tsub * c->max_tracks + tslot];
}


static void
offer_row_clear(moqr_core_t *c, uint32_t tsub)
{
    memset(offer_at(c, tsub, 0), 0, (size_t)c->max_tracks * sizeof(r_offer_t));
}


static uint32_t
tsub_find(const moqr_core_t *c, uint32_t bslot, uint32_t bgen, uint64_t cookie)
{
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_tsub_t *t = &c->tsubs[i];
        if (t->used && t->binding == bslot && t->binding_gen == bgen &&
            t->cookie == cookie) {
            return i;
        }
    }
    return UINT32_MAX;
}


void
tsub_free(moqr_core_t *c, uint32_t i)
{
    r_tsub_t *t = &c->tsubs[i];
    c->intern_used = c->intern_used >= t->prefix_len
        ? c->intern_used - t->prefix_len : 0;
    r_free(c, t->prefix, t->prefix_len);
    memset(t, 0, sizeof(*t));
    offer_row_clear(c, i);
}


#define MOQR_TRACKS_REQ_SIZE_THROUGH_GROUP_ORDER                             \
    (offsetof(moqr_tracks_req_t, group_order) +                             \
     sizeof(((moqr_tracks_req_t *)0)->group_order))


void
moqr_tracks_req_init_sized(moqr_tracks_req_t *req, size_t req_size)
{
    if (req == NULL || req_size < sizeof(req->struct_size)) {
        return;
    }
    size_t n = req_size < sizeof(*req) ? req_size : sizeof(*req);
    memset(req, 0, n);
    req->struct_size = (uint32_t)n;
    if (n >= offsetof(moqr_tracks_req_t, include_properties) +
                 sizeof(req->include_properties)) {
        req->forward = true;
        req->include_properties = true;
    }
}


moqr_result_t
moqr_core_tracks_subscribe(moqr_core_t *c, moqr_binding_t bh,
                           const moqr_tracks_req_t *req)
{
    if (c == NULL || req == NULL ||
        req->struct_size < MOQR_TRACKS_REQ_SIZE_THROUGH_GROUP_ORDER ||
        !ns_view_ok(req->prefix, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (tsub_find(c, bslot, b->gen, req->cookie) != UINT32_MAX) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->tsubs[i].used) {
            slot = i;
            break;
        }
    }
    if (slot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_NS_SUBS);
    }
    uint32_t plen = prefix_buf_len(req->prefix);
    if ((uint64_t)c->intern_used + plen > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }
    uint8_t *pbuf = r_alloc(c, plen);
    if (pbuf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    prefix_buf_write(pbuf, req->prefix);
    r_tsub_t *t = &c->tsubs[slot];
    memset(t, 0, sizeof(*t));
    t->used = true;
    t->binding = bslot;
    t->binding_gen = b->gen;
    t->cookie = req->cookie;
    t->prefix = pbuf;
    t->prefix_len = plen;
    t->forward = req->forward;
    t->include_properties = req->include_properties;
    t->group_order = req->group_order;
    t->held = req->struct_size >= offsetof(moqr_tracks_req_t, held) +
                                      sizeof(req->held) && req->held;
    offer_row_clear(c, slot);
    c->intern_used += plen;
    b->discovery_dirty = true;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 3 /* track sub */, slot, bslot,
            req->cookie, 0);
    return MOQR_OK;
}


moqr_result_t
moqr_core_tracks_update_prepare(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                                uint32_t flags, moqr_ns_t prefix, bool forward,
                                moqr_tracks_update_prep_t *prep)
{
    if (prep != NULL) {
        memset(prep, 0, sizeof(*prep));
    }
    if (c == NULL || prep == NULL ||
        (flags & ~(uint32_t)(MOQR_TRACKS_UPDATE_PREFIX |
                             MOQR_TRACKS_UPDATE_FORWARD |
                             MOQR_TRACKS_UPDATE_TOKENS)) != 0 ||
        ((flags & MOQR_TRACKS_UPDATE_PREFIX) && !ns_view_ok(prefix, 0))) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = tsub_find(c, bslot, b->gen, cookie);
    if (slot == UINT32_MAX) {
        return MOQR_ERR_STALE_HANDLE;
    }
    r_tsub_t *t = &c->tsubs[slot];
    if (flags & MOQR_TRACKS_UPDATE_PREFIX) {
        uint32_t plen = prefix_buf_len(prefix);
        if ((uint64_t)c->intern_used - t->prefix_len + plen > c->intern_budget) {
            return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
        }
        uint8_t *pbuf = r_alloc(c, plen);
        if (pbuf == NULL) {
            return MOQR_ERR_NOMEM;
        }
        prefix_buf_write(pbuf, prefix);
        prep->prefix_buf = pbuf;
        prep->prefix_len = plen;
    }
    prep->staged = true;
    prep->binding = bh;
    prep->cookie = cookie;
    prep->flags = flags;
    prep->forward = forward;
    return MOQR_OK;
}


void
moqr_core_tracks_update_abort(moqr_core_t *c, moqr_tracks_update_prep_t *prep)
{
    if (c == NULL || prep == NULL || !prep->staged) {
        return;
    }
    if (prep->prefix_buf != NULL) {
        r_free(c, prep->prefix_buf, prep->prefix_len);
    }
    memset(prep, 0, sizeof(*prep));
}


moqr_result_t
moqr_core_tracks_update_commit(moqr_core_t *c, moqr_tracks_update_prep_t *prep)
{
    if (c == NULL || prep == NULL || !prep->staged) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, prep->binding, &bslot);
    uint32_t slot = b != NULL ? tsub_find(c, bslot, b->gen, prep->cookie)
                              : UINT32_MAX;
    if (slot == UINT32_MAX) {
        moqr_core_tracks_update_abort(c, prep);
        return MOQR_ERR_STALE_HANDLE;
    }
    r_tsub_t *t = &c->tsubs[slot];
    if (prep->flags & MOQR_TRACKS_UPDATE_PREFIX) {
        c->intern_used = c->intern_used - t->prefix_len + prep->prefix_len;
        r_free(c, t->prefix, t->prefix_len);
        t->prefix = prep->prefix_buf;
        t->prefix_len = prep->prefix_len;
    }
    if (prep->flags & MOQR_TRACKS_UPDATE_FORWARD) {
        t->forward = prep->forward;
    }
    if (prep->flags & MOQR_TRACKS_UPDATE_TOKENS) {
        /* A decision made, or still pending, under the old tokens no longer
         * stands: each such match is decided again (§3.6, §9.5). */
        t->input_gen++;
        for (uint32_t k = 0; k < c->max_tracks; k++) {
            r_offer_t *o = offer_at(c, slot, k);
            if (o->state == MOQR_DISCOVERY_DECLINED ||
                o->state == MOQR_DISCOVERY_PENDING ||
                o->state == MOQR_DISCOVERY_ALLOWED) {
                memset(o, 0, sizeof(*o));
            }
        }
    }
    memset(prep, 0, sizeof(*prep));
    b->discovery_dirty = true;
    r_bump_route_epoch(c);
    return MOQR_OK;
}


moqr_result_t
moqr_core_tracks_answered(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    uint32_t slot = b != NULL ? tsub_find(c, bslot, b->gen, cookie) : UINT32_MAX;
    if (slot == UINT32_MAX) {
        return MOQR_ERR_STALE_HANDLE;
    }
    c->tsubs[slot].held = false;
    b->discovery_dirty = true;
    return MOQR_OK;
}


moqr_result_t
moqr_core_tracks_unsubscribe(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = tsub_find(c, bslot, b->gen, cookie);
    if (slot == UINT32_MAX) {
        return MOQR_ERR_STALE_HANDLE;
    }
    tsub_free(c, slot);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 3 /* track sub */, slot, bslot, cookie, 0);
    return MOQR_OK;
}


/* A track the binding may be offered: a live source that is not the
 * binding's own (§3.6 excludes the subscriber's own publications). */
static bool
track_offerable(const r_track_t *t, uint32_t bslot, uint32_t bgen)
{
    if ((t->gen & 1u) == 0 || t->state != R_TRACK_ACTIVE) {
        return false;
    }
    return !(t->has_upstream_binding && t->up_binding == bslot &&
             t->up_binding_gen == bgen);
}


moqr_result_t
moqr_core_next_discovery(moqr_core_t *c, moqr_binding_t bh, moqr_discovery_t *out,
                         size_t out_size)
{
    if (c == NULL || out == NULL || out_size < MOQR_DISCOVERY_V0_SIZE) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->discovery_dirty) {
        return MOQR_DONE;
    }
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_tsub_t *ts = &c->tsubs[i];
        if (!ts->used || ts->binding != bslot || ts->binding_gen != b->gen ||
            ts->held) {
            continue;
        }
        for (uint32_t k = 0; k < c->max_tracks; k++) {
            const r_track_t *t = &c->tracks[k];
            if (!track_offerable(t, bslot, b->gen) ||
                !cluster_track_eligible(c, t, bslot, b->gen)) {
                continue;
            }
            const r_offer_t *o = offer_at(c, i, k);
            bool current = o->track_slot_gen == t->gen &&
                           o->track_gen == t->track_gen;
            if (o->state != 0 && current &&
                o->state != MOQR_DISCOVERY_ALLOWED) {
                continue;
            }
            bool authorized = current && o->state == MOQR_DISCOVERY_ALLOWED;
            uint32_t count = key_part_count(&t->key);
            if (count > 32) {
                continue;
            }
            moq_bytes_t parts[32];
            for (uint32_t p = 0; p < count; p++) {
                parts[p] = key_part(&t->key, p);
            }
            if (!prefix_buf_matches(ts->prefix, parts, count)) {
                continue;
            }
            size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
            memset(out, 0, n);
            out->struct_size = (uint32_t)n;
            out->cookie = ts->cookie;
            out->track = track_handle(c, k);
            out->track_gen = t->track_gen;
            for (uint32_t p = 0; p < count; p++) {
                out->ns_parts[p] = parts[p];
            }
            out->ns.parts = out->ns_parts;
            out->ns.count = count;
            out->name = key_name(&t->key);
            out->prefix_count = key_rd32(ts->prefix);
            out->forward = ts->forward;
            out->include_properties = ts->include_properties;
            out->group_order = ts->group_order;
            out->has_largest = t->has_largest;
            out->largest_group = t->largest_group;
            out->largest_object = t->largest_object;
            if (n >= offsetof(moqr_discovery_t, authorized) +
                         sizeof(out->authorized)) {
                out->authorized = authorized;
            }
            if (n >= offsetof(moqr_discovery_t, input_gen) +
                         sizeof(out->input_gen)) {
                out->input_gen = ts->input_gen;
            }
            return MOQR_OK;
        }
    }
    b->discovery_dirty = false;
    return MOQR_DONE;
}


moqr_result_t
moqr_core_discovery_done(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                         moqr_track_t th, uint64_t track_gen, uint32_t input_gen,
                         uint32_t outcome)
{
    if (c == NULL || outcome < MOQR_DISCOVERY_OFFERED ||
        outcome > MOQR_DISCOVERY_ALLOWED) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = tsub_find(c, bslot, b->gen, cookie);
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (slot == UINT32_MAX || t == NULL || t->track_gen != track_gen ||
        c->tsubs[slot].input_gen != input_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    r_offer_t *o = offer_at(c, slot, tslot);
    o->track_slot_gen = t->gen;
    o->track_gen = t->track_gen;
    o->state = (uint8_t)outcome;
    if (outcome == MOQR_DISCOVERY_ALLOWED) {
        b->discovery_dirty = true;   /* offer it on the next pull */
    }
    return MOQR_OK;
}


/* Mark `tslot`'s current source as already served to every track
 * subscription of `bslot`: a SUBSCRIBE held there was resolved by it. */
void
discovery_suppress(moqr_core_t *c, uint32_t bslot, uint32_t bgen, uint32_t tslot)
{
    const r_track_t *t = &c->tracks[tslot];
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        const r_tsub_t *ts = &c->tsubs[i];
        if (ts->used && ts->binding == bslot && ts->binding_gen == bgen) {
            r_offer_t *o = offer_at(c, i, tslot);
            o->track_slot_gen = t->gen;
            o->track_gen = t->track_gen;
            o->state = MOQR_DISCOVERY_SUPPRESSED;
        }
    }
}


moqr_result_t
moqr_core_subscription_prefix(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                              bool tracks, moq_bytes_t *parts, size_t parts_cap,
                              moqr_ns_t *out)
{
    if (c == NULL || out == NULL || (parts_cap > 0 && parts == NULL)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    const uint8_t *buf = NULL;
    if (tracks) {
        uint32_t slot = tsub_find(c, bslot, b->gen, cookie);
        if (slot != UINT32_MAX) buf = c->tsubs[slot].prefix;
    } else {
        uint32_t slot = ns_sub_find(c, bslot, b->gen, cookie);
        if (slot != UINT32_MAX) buf = c->ns_subs[slot].prefix;
    }
    if (buf == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t count = key_rd32(buf);
    if (count > parts_cap) {
        return MOQR_ERR_CAPACITY;
    }
    uint32_t off = 4u * (1u + count);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t len = key_rd32(buf + 4u * (1u + i));
        parts[i].data = buf + off;
        parts[i].len = len;
        off += len;
    }
    out->parts = parts;
    out->count = count;
    return MOQR_OK;
}


static uint32_t
rtok_find(const moqr_core_t *c, uint32_t bslot, uint32_t bgen, uint64_t cookie)
{
    for (uint32_t i = 0; i < c->max_rtoks; i++) {
        const r_rtok_t *r = &c->rtoks[i];
        if (r->used && r->binding == bslot && r->binding_gen == bgen &&
            r->cookie == cookie) {
            return i;
        }
    }
    return UINT32_MAX;
}


moqr_result_t
moqr_core_request_tokens_set(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                             const moqr_auth_token_t *tokens, size_t token_count)
{
    if (c == NULL || (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = rtok_find(c, bslot, b->gen, cookie);
    if (token_count == 0) {
        if (slot != UINT32_MAX) {
            r_free_material(c, &c->rtoks[slot].mat, &c->intern_used);
            memset(&c->rtoks[slot], 0, sizeof(c->rtoks[slot]));
        }
        return MOQR_OK;
    }
    uint32_t dst = slot;
    if (dst == UINT32_MAX) {
        for (uint32_t i = 0; i < c->max_rtoks; i++) {
            if (!c->rtoks[i].used) {
                dst = i;
                break;
            }
        }
        if (dst == UINT32_MAX) {
            return r_refuse(c, MOQR_REFUSE_NS_SUBS);
        }
    }
    moqr_park_req_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.tokens = tokens;
    pr.token_count = token_count;
    /* The replacement is charged before the old copy is released, so a
     * budget refusal leaves the tokens in effect untouched. */
    r_material m;
    moqr_result_t rc = r_copy_material(c, &pr, &c->intern_used,
                                       c->intern_budget, &m);
    if (rc != MOQR_OK) {
        if (rc == MOQR_ERR_CAPACITY) {
            (void)r_refuse(c, MOQR_REFUSE_NAME_BYTES);
        }
        return rc;
    }
    r_rtok_t *r = &c->rtoks[dst];
    if (r->used) {
        r_free_material(c, &r->mat, &c->intern_used);
    }
    r->used = true;
    r->binding = bslot;
    r->binding_gen = b->gen;
    r->cookie = cookie;
    r->mat = m;
    return MOQR_OK;
}


moqr_result_t
moqr_core_request_tokens_get(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie,
                             const moqr_auth_token_t **tokens, size_t *token_count)
{
    if (c == NULL || tokens == NULL || token_count == NULL) {
        return MOQR_ERR_INVAL;
    }
    *tokens = NULL;
    *token_count = 0;
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = rtok_find(c, bslot, b->gen, cookie);
    if (slot == UINT32_MAX) {
        return MOQR_DONE;
    }
    *tokens = c->rtoks[slot].mat.tokens;
    *token_count = c->rtoks[slot].mat.token_count;
    return MOQR_OK;
}


void
moqr_core_request_tokens_clear(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie)
{
    if (c == NULL) {
        return;
    }
    (void)moqr_core_request_tokens_set(c, bh, cookie, NULL, 0);
}
