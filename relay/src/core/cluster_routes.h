static bool cluster_route_warm(const moqr_core_t *c, uint32_t ri);

/* Included by relay.c after trie and pool primitives. Route ownership is
 * independent of trie indexing; all storage is reserved at core creation. */
static uint64_t *
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

static bool
cluster_eligible(const moqr_core_t *c, uint32_t ri, uint32_t requester,
    uint32_t requester_gen, uint32_t excluded, uint32_t excluded_gen)
{
    const r_binding_t *b = requester < c->max_bindings ? &c->bindings[requester] : NULL;
    return cluster_eligible_with_path(c, ri, requester, requester_gen,
        b != NULL ? b->peer_hop_id : 0, b != NULL ? b->correlation_id : 0,
        excluded, excluded_gen, cluster_path(c, ri), c->cluster_routes[ri].hop_count);
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
        if (c->cluster_forced_lifetime != 0 &&
            !((r->mirrored ? r->external_lifetime : r->lifetime) == c->cluster_forced_lifetime &&
              (r->mirrored ? r->external_binding_cookie : c->bindings[r->binding].cookie) == c->cluster_forced_binding)) continue;
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

static void
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

static uint32_t
cluster_longest_node(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t node = 0, best = c->nodes[0].has_announce ? 0 : UINT32_MAX;
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) break;
        if (c->nodes[node].has_announce) best = node;
    }
    return best;
}

static uint32_t
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

static bool
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

void
moqr_core_cluster_info(const moqr_core_t *c, moqr_cluster_info_t *out)
{
    if (out == NULL) return;
    memset(out, 0, sizeof(*out));
    if (c == NULL) return;
    out->enabled = c->cluster_enabled;
    out->local_hop_id = c->cluster_hop_id;
    out->max_routes = c->max_cluster_routes;
    out->max_hops = c->max_cluster_hops;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++)
        if (c->cluster_routes[i].used) out->routes_used++;
}

moqr_result_t
moqr_core_binding_set_cluster(moqr_core_t *c, moqr_binding_t bh, bool negotiated,
    uint64_t peer_hop_id, uint64_t effective_relay_cost, uint64_t correlation_id)
{
    if (c == NULL || !c->cluster_enabled) return MOQR_ERR_WRONG_STATE;
    uint32_t bs;
    r_binding_t *b = binding_resolve(c, bh, &bs);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++)
        if (c->cluster_routes[i].used && c->cluster_routes[i].binding == bs &&
            c->cluster_routes[i].binding_gen == b->gen) return MOQR_ERR_WRONG_STATE;
    b->cluster_negotiated = negotiated;
    b->peer_hop_id = negotiated ? peer_hop_id : 0;
    b->relay_cost = effective_relay_cost;
    b->correlation_id = correlation_id;
    return MOQR_OK;
}

static void
cluster_fill(const moqr_core_t *c, uint32_t ri, moqr_cluster_route_t *out)
{
    const r_cluster_route_t *r = &c->cluster_routes[ri];
    memset(out, 0, sizeof(*out));
    out->source_binding_cookie = c->bindings[r->binding].cookie;
    out->source_session_cookie = r->session_cookie;
    out->origin_id = cluster_path(c, ri)[0];
    out->route_generation = r->lifetime;
    out->original_source_binding_cookie = r->mirrored ? r->external_binding_cookie : out->source_binding_cookie;
    out->original_route_generation = r->mirrored ? r->external_lifetime : r->lifetime;
    out->accumulated_cost = r->cost;
    uint32_t n = r->node;
    while (n != 0) { out->prefix_count++; n = c->nodes[n].parent; }
    out->warm = cluster_route_warm(c, ri);
    out->metadata.hop_ids = cluster_path(c, ri);
    out->metadata.hop_count = r->hop_count;
    out->metadata.has_route_cost = true;
    out->metadata.route_cost = out->warm ? 0 : r->cost;
}

moqr_result_t
moqr_core_cluster_route(const moqr_core_t *c, moqr_binding_t requester,
    moqr_ns_t ns, moqr_cluster_route_t *out)
{
    if (c == NULL || out == NULL || !ns_view_ok(ns, 0)) return MOQR_ERR_INVAL;
    if (!c->cluster_enabled) return MOQR_ERR_WRONG_STATE;
    uint32_t bs = UINT32_MAX, bg = 0;
    if (moqr_binding_is_valid(requester)) {
        r_binding_t *b = binding_resolve((moqr_core_t *)(uintptr_t)c, requester, &bs);
        if (b == NULL) return MOQR_ERR_STALE_HANDLE;
        bg = b->gen;
    }
    uint32_t node = cluster_longest_node(c, ns);
    if (node == UINT32_MAX) return MOQR_DONE;
    uint32_t ri = cluster_best_at(c, node, bs, bg, UINT32_MAX, 0);
    if (ri == UINT32_MAX) return MOQR_DONE;
    cluster_fill(c, ri, out);
    return MOQR_OK;
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

static moqr_result_t
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

moqr_result_t
moqr_core_announce_cluster(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
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

static void
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
cluster_track_eligible(const moqr_core_t *c, const r_track_t *t,
    uint32_t requester, uint32_t requester_gen)
{
    if (!c->cluster_enabled) return true;
    moq_bytes_t parts[32];
    uint32_t count = key_part_count(&t->key);
    for (uint32_t i = 0; i < count; i++) parts[i] = key_part(&t->key, i);
    uint32_t node = cluster_longest_node(c, (moqr_ns_t){parts, count});
    if (node == UINT32_MAX) return false;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->used && r->lifetime == t->cluster_route_lifetime && r->node == node &&
            cluster_eligible(c, i, requester, requester_gen, UINT32_MAX, 0)) {
            if (c->cluster_forced_lifetime == 0 ||
                ((r->mirrored ? r->external_lifetime : r->lifetime) == c->cluster_forced_lifetime &&
                 (r->mirrored ? r->external_binding_cookie : c->bindings[r->binding].cookie) == c->cluster_forced_binding)) return true;
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
moqr_cluster_should_reparent(moqr_ns_t ns, uint64_t local, uint64_t peer)
{
    if (local == peer || !ns_view_ok(ns, 0)) return false;
    uint64_t a = cluster_order(ns, local), b = cluster_order(ns, peer);
    return a == b ? local > peer : a > b;
}

static void
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
moqr_core_cluster_export(const moqr_core_t *c, uint32_t index,
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
    out->source_peer_hop_id = r->source_peer_hop_id;
    out->source_correlation_id = r->source_correlation_id;
    return MOQR_OK;
}

moqr_result_t
moqr_core_cluster_import(moqr_core_t *c, moqr_binding_t bh,
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
    if (withdraw || record->route.route_generation == 0) {
        return ri == UINT32_MAX ? MOQR_OK : cluster_unannounce(c, bs, b->gen,
            c->cluster_routes[ri].node, c->cluster_routes[ri].lifetime);
    }
    if (ri != UINT32_MAX &&
        (c->cluster_routes[ri].external_lifetime != record->route.route_generation ||
         c->cluster_routes[ri].external_binding_cookie != record->route.source_binding_cookie))
        return MOQR_ERR_WRONG_STATE; /* withdraw old lifetime before replacement */
    if (!ns_view_ok(record->ns, 0) || record->route.metadata.hop_count == 0 ||
        record->route.metadata.hop_ids == NULL) return MOQR_ERR_INVAL;
    if (record->route.metadata.hop_count > c->max_cluster_hops) return MOQR_ERR_CAPACITY;
    if (ri == UINT32_MAX && free_slot == UINT32_MAX) return MOQR_ERR_CAPACITY;
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
    bool same = r->used && r->external_lifetime == record->route.route_generation &&
                r->external_binding_cookie == record->route.source_binding_cookie;
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
    r->external_lifetime = record->route.route_generation;
    r->external_binding_cookie = record->route.source_binding_cookie;
    r->session_cookie = record->route.source_session_cookie;
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
moqr_core_cluster_upstream_route(const moqr_core_t *c, moqr_track_t th,
    uint64_t track_gen, moqr_cluster_route_t *out, uint64_t *requester_hop_id,
    uint64_t *requester_correlation_id)
{
    if (c == NULL || out == NULL || requester_hop_id == NULL ||
        requester_correlation_id == NULL || !c->cluster_enabled) return MOQR_ERR_INVAL;
    r_track_t *t = track_resolve((moqr_core_t *)(uintptr_t)c, th, NULL);
    if (t == NULL || t->track_gen != track_gen) return MOQR_ERR_STALE_HANDLE;
    for (uint32_t i = 0; i < c->max_cluster_routes; i++) {
        const r_cluster_route_t *r = &c->cluster_routes[i];
        if (r->used && r->lifetime == t->cluster_route_lifetime) {
            cluster_fill(c, i, out);
            if (r->mirrored) {
                out->source_binding_cookie = r->external_binding_cookie;
                out->route_generation = r->external_lifetime;
            }
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

static uint32_t
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

static void
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
            !moqr_cluster_should_reparent(ns, c->cluster_hop_id, ids[r->hop_count - 1])) continue;
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

static uint32_t
cluster_track_lookup(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name,
    uint32_t requester, uint32_t requester_gen, uint64_t *hash_out)
{
    uint64_t hash = key_view_hash(ns, name);
    if (hash_out != NULL) *hash_out = hash;
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
