#include "relay_internal.h"
#include "tracks_internal.h"
#include "delivery_internal.h"
#include "fetch_internal.h"
#include "intents_internal.h"

/* Borrowed-view validation: a non-zero length requires a data pointer,
 * and every namespace/name length is bounded by MOQ_FULL_TRACK_NAME_MAX
 * (per part, per name, and cumulatively) BEFORE anything narrows or
 * copies — the sizing math below runs on validated values only. */
static bool
bytes_view_ok(moq_bytes_t b)
{
    return (b.len == 0 || b.data != NULL) &&
           b.len <= MOQ_FULL_TRACK_NAME_MAX;
}


/* A PRESENT Track Namespace Field must contain at least one byte: draft-18 and
 * draft-16 Section 2.4.1 both require it. That is stricter than bytes_view_ok,
 * which exists for Track NAMES -- those may legitimately be empty in both
 * drafts. Keeping the two apart is what lets the core accept an empty Track
 * Name while rejecting an empty namespace field. */
static bool
ns_field_view_ok(moq_bytes_t b)
{
    return b.len > 0 && b.data != NULL && b.len <= MOQ_FULL_TRACK_NAME_MAX;
}


static bool
ns_view_len(moqr_ns_t ns, size_t min_count, size_t *out_total)
{
    if (ns.count < min_count || ns.count > 32) {
        return false;
    }
    if (ns.count > 0 && ns.parts == NULL) {
        return false;
    }
    size_t total = 0;
    for (size_t i = 0; i < ns.count; i++) {
        if (!ns_field_view_ok(ns.parts[i])) {
            return false;
        }
        total += ns.parts[i].len;
        if (total > MOQ_FULL_TRACK_NAME_MAX) {
            return false;
        }
    }
    *out_total = total;
    return true;
}


/* min_count is the protocol-neutral floor: draft-18 Section 2.4.1 allows a
 * Track Namespace of 0..32 fields (the ROOT namespace), draft-16 Section 2.4.1
 * allows 1..32, so the core represents both and the negotiated session profile
 * enforces its own minimum. */
bool
ns_view_ok(moqr_ns_t ns, size_t min_count)
{
    size_t total = 0;
    return ns_view_len(ns, min_count, &total);
}


/* Full Track Name: a Track Namespace plus a Track Name, sharing the combined
 * cap. draft-18 Section 2.4.1 builds it on the same 0..32-field namespace, so a
 * ROOT Full Track Name -- zero fields plus a name -- is valid here; the Track
 * Name itself may be empty in both drafts. The core stays protocol-neutral and
 * the session profile rejects a draft-16 zero-field namespace before it. */
bool
ftn_view_ok(moqr_ns_t ns, moq_bytes_t name)
{
    size_t total = 0;
    return ns_view_len(ns, 0, &total) && bytes_view_ok(name) &&
           total + name.len <= MOQ_FULL_TRACK_NAME_MAX;
}


r_track_t *
track_resolve(moqr_core_t *c, moqr_track_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_TRACK ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_tracks ||
        c->tracks[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->tracks[slot];
}


void track_free_slot(moqr_core_t *c, uint32_t slot);


/* -- key building -------------------------------------------------------------------- */

static moqr_result_t
key_build(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name, r_key_t *out)
{
    if (ns.count > 32) {
        return MOQR_ERR_INVAL;
    }
    uint32_t len = 4u * (2u + (uint32_t)ns.count);
    for (size_t i = 0; i < ns.count; i++) {
        if (ns.parts[i].len > MOQ_FULL_TRACK_NAME_MAX) {
            return MOQR_ERR_INVAL;
        }
        len += (uint32_t)ns.parts[i].len;
    }
    if (name.len > MOQ_FULL_TRACK_NAME_MAX) {
        return MOQR_ERR_INVAL;
    }
    len += (uint32_t)name.len;
    if (c->intern_used + len > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }

    uint8_t *buf = r_alloc(c, len);
    if (buf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    key_wr32(buf, (uint32_t)ns.count);
    uint32_t off = 4u * (2u + (uint32_t)ns.count);
    for (size_t i = 0; i < ns.count; i++) {
        key_wr32(buf + 4u * (1u + i), (uint32_t)ns.parts[i].len);
        if (ns.parts[i].len != 0) {
            memcpy(buf + off, ns.parts[i].data, ns.parts[i].len);
        }
        off += (uint32_t)ns.parts[i].len;
    }
    key_wr32(buf + 4u * (1u + ns.count), (uint32_t)name.len);
    if (name.len != 0) {
        memcpy(buf + off, name.data, name.len);
    }
    out->buf = buf;
    out->len = len;
    c->intern_used += len;
    return MOQR_OK;
}


static void
key_release(moqr_core_t *c, r_key_t *k)
{
    if (k->buf != NULL) {
        c->intern_used -= k->len;
        r_free(c, k->buf, k->len);
        k->buf = NULL;
        k->len = 0;
    }
}


/* The canonical key of (ns, name), computed from the views without building
 * it: its byte length, its hash, and equality with a stored key. Lookups use
 * these, so finding an existing track never allocates or charges name bytes
 * (and never makes room by evicting the very cache it is about to hit). */
static bool
key_view_len(moqr_ns_t ns, moq_bytes_t name, uint32_t *out)
{
    if (ns.count > 32 || name.len > MOQ_FULL_TRACK_NAME_MAX) {
        return false;
    }
    uint32_t len = 4u * (2u + (uint32_t)ns.count) + (uint32_t)name.len;
    for (size_t i = 0; i < ns.count; i++) {
        if (ns.parts[i].len > MOQ_FULL_TRACK_NAME_MAX) {
            return false;
        }
        len += (uint32_t)ns.parts[i].len;
    }
    *out = len;
    return true;
}


static uint64_t
key_hash_bytes(uint64_t h, const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= UINT64_C(0x100000001B3);
    }
    return h;
}


/* FNV-1a over exactly the bytes key_build() would write for (ns, name). */
uint64_t
key_view_hash(moqr_ns_t ns, moq_bytes_t name)
{
    uint64_t h = UINT64_C(0xCBF29CE484222325);
    uint8_t w[4];
    key_wr32(w, (uint32_t)ns.count);
    h = key_hash_bytes(h, w, 4);
    for (size_t i = 0; i < ns.count; i++) {
        key_wr32(w, (uint32_t)ns.parts[i].len);
        h = key_hash_bytes(h, w, 4);
    }
    key_wr32(w, (uint32_t)name.len);
    h = key_hash_bytes(h, w, 4);
    for (size_t i = 0; i < ns.count; i++) {
        h = key_hash_bytes(h, ns.parts[i].data, ns.parts[i].len);
    }
    return key_hash_bytes(h, name.data, name.len);
}


bool
key_view_eq(const r_key_t *k, moqr_ns_t ns, moq_bytes_t name)
{
    if (key_part_count(k) != ns.count) {
        return false;
    }
    for (uint32_t i = 0; i < ns.count; i++) {
        moq_bytes_t p = key_part(k, i);
        if (p.len != ns.parts[i].len ||
            (p.len != 0 && memcmp(p.data, ns.parts[i].data, p.len) != 0)) {
            return false;
        }
    }
    moq_bytes_t n = key_name(k);
    return n.len == name.len &&
           (n.len == 0 || memcmp(n.data, name.data, n.len) == 0);
}


/* -- trie ----------------------------------------------------------------------------- */

uint32_t
trie_find_child(const moqr_core_t *c, uint32_t node, moq_bytes_t part)
{
    for (uint32_t k = c->nodes[node].first_child; k != R_NODE_NIL;
         k = c->nodes[k].next_sib) {
        const r_trie_node_t *ch = &c->nodes[k];
        if (ch->part_len == part.len &&
            (part.len == 0 || memcmp(ch->part, part.data, part.len) == 0)) {
            return k;
        }
    }
    return UINT32_MAX;
}


moqr_result_t
trie_child_get_or_create(moqr_core_t *c, uint32_t node, moq_bytes_t part,
                         uint32_t *out)
{
    uint32_t found = trie_find_child(c, node, part);
    if (found != UINT32_MAX) {
        *out = found;
        return MOQR_OK;
    }
    if (c->node_count == c->max_ns_nodes) {
        return r_refuse(c, MOQR_REFUSE_NS_NODES);
    }
    if (c->intern_used + part.len > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }
    uint8_t *copy = NULL;
    if (part.len != 0) {
        copy = r_alloc(c, part.len);
        if (copy == NULL) {
            return MOQR_ERR_NOMEM;
        }
        memcpy(copy, part.data, part.len);
    }
    uint32_t idx = c->node_count++;
    r_trie_node_t *nn = &c->nodes[idx];
    memset(nn, 0, sizeof(*nn));
    nn->part = copy;
    nn->part_len = (uint32_t)part.len;
    nn->parent = node;
    nn->first_child = R_NODE_NIL;
    nn->last_child = R_NODE_NIL;
    nn->next_sib = R_NODE_NIL;
    r_trie_node_t *par = &c->nodes[node];
    nn->prev_sib = par->last_child;   /* append: fan-out keeps creation order */
    if (par->last_child != R_NODE_NIL) {
        c->nodes[par->last_child].next_sib = idx;
    } else {
        par->first_child = idx;
    }
    par->last_child = idx;
    c->intern_used += (uint32_t)part.len;
    *out = idx;
    return MOQR_OK;
}


/* Set a live track's source-announce node, keeping each node's src_refs
 * count of the tracks that name it. */
void
track_src_set(moqr_core_t *c, r_track_t *t, uint32_t node)
{
    if (t->src_ann_node != UINT32_MAX) {
        c->nodes[t->src_ann_node].src_refs--;
    }
    t->src_ann_node = node;
    if (node != UINT32_MAX) {
        c->nodes[node].src_refs++;
    }
}


/* Point every live track whose src_ann_node is `from` at `to`. */
static void
track_src_rewrite(moqr_core_t *c, uint32_t from, uint32_t to)
{
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        if ((c->tracks[t].gen & 1u) != 0 &&
            c->tracks[t].src_ann_node == from) {
            c->tracks[t].src_ann_node = to;
        }
    }
}


/* Free leaf node n and compact: the last node takes slot n. */
static void
trie_node_free(moqr_core_t *c, uint32_t n)
{
    r_trie_node_t *nd = &c->nodes[n];
    r_trie_node_t *par = &c->nodes[nd->parent];
    if (nd->prev_sib != R_NODE_NIL) {
        c->nodes[nd->prev_sib].next_sib = nd->next_sib;
    } else {
        par->first_child = nd->next_sib;
    }
    if (nd->next_sib != R_NODE_NIL) {
        c->nodes[nd->next_sib].prev_sib = nd->prev_sib;
    } else {
        par->last_child = nd->prev_sib;
    }
    if (nd->src_refs != 0) {
        track_src_rewrite(c, n, UINT32_MAX);   /* its announce is long gone */
    }
    c->intern_used -= nd->part_len;
    r_free(c, nd->part, nd->part_len);
    uint32_t last = c->node_count - 1u;
    if (n != last) {
        c->nodes[n] = c->nodes[last];
        r_trie_node_t *mv = &c->nodes[n];
        r_trie_node_t *mp = &c->nodes[mv->parent];
        if (mv->prev_sib != R_NODE_NIL) {
            c->nodes[mv->prev_sib].next_sib = n;
        } else {
            mp->first_child = n;
        }
        if (mv->next_sib != R_NODE_NIL) {
            c->nodes[mv->next_sib].prev_sib = n;
        } else {
            mp->last_child = n;
        }
        for (uint32_t k = mv->first_child; k != R_NODE_NIL;
             k = c->nodes[k].next_sib) {
            c->nodes[k].parent = n;
        }
        if (mv->src_refs != 0) {
            track_src_rewrite(c, last, n);
        }
    }
    if (c->cluster_enabled && n != last) {
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            if (c->cluster_routes[ri].used && c->cluster_routes[ri].node == last)
                c->cluster_routes[ri].node = n;
        }
    }
    memset(&c->nodes[last], 0, sizeof(c->nodes[last]));
    c->node_count--;
    c->ns_nodes_reclaimed++;
    r_room_changed(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 3 /* ns node */, n, last, 0, 0);
}


/* Walk node n's subtree, adding to *nodes / *bytes the size of every maximal
 * subtree that is entirely reclaimable (no announce anywhere in it, not on
 * the kept path, and, unless ignore_views, no part borrowed by an unpolled
 * intent). Returns whether n's own subtree qualifies, in which case its size
 * is included. Pure; recursion depth is bounded by the 32-part namespace
 * limit. */
static bool
trie_subtree_free(const moqr_core_t *c, uint32_t n, uint32_t keep,
                  bool ignore_views, bool check_views, uint32_t *nodes,
                  uint64_t *bytes)
{
    const r_trie_node_t *nd = &c->nodes[n];
    uint32_t below_n = 0;
    uint64_t below_b = 0;
    bool all = true;
    for (uint32_t k = nd->first_child; k != R_NODE_NIL;
         k = c->nodes[k].next_sib) {
        uint32_t sn = 0;
        uint64_t sb = 0;
        if (trie_subtree_free(c, k, keep, ignore_views, check_views, &sn,
                              &sb)) {
            below_n += sn;
            below_b += sb;
        } else {
            all = false;
            *nodes += sn;   /* qualifying subtrees under a kept child */
            *bytes += sb;
        }
    }
    if (all && n != 0 && n != keep && !nd->has_announce &&
        (ignore_views || !check_views || !intent_borrows_part(c, nd->part))) {
        *nodes += below_n + 1u;
        *bytes += below_b + nd->part_len;
        return true;
    }
    *nodes += below_n;
    *bytes += below_b;
    return false;
}


/* Room trie reclaim can supply right now (respecting intent views) and in
 * total (ignoring them). */
static void
trie_free_room(const moqr_core_t *c, uint32_t keep, uint32_t *nodes_now,
               uint64_t *bytes_now, uint32_t *nodes_all, uint64_t *bytes_all)
{
    bool views = intent_ns_count(c) != 0;
    *nodes_now = 0;
    *bytes_now = 0;
    (void)trie_subtree_free(c, 0, keep, false, views, nodes_now, bytes_now);
    if (views) {
        *nodes_all = 0;
        *bytes_all = 0;
        (void)trie_subtree_free(c, 0, keep, true, true, nodes_all, bytes_all);
    } else {
        *nodes_all = *nodes_now;
        *bytes_all = *bytes_now;
    }
}


/* Free every reclaimable node except *keep (a freed leaf can expose its
 * parent, so sweep until nothing changes; *keep follows its node if
 * compaction moves it). The route epoch is bumped before the first removal,
 * so each trace record carries the epoch of the state it produced. */
static uint32_t
trie_reclaim(moqr_core_t *c, uint32_t *keep)
{
    uint32_t freed = 0;
    bool views = intent_ns_count(c) != 0;
    bool changed = true;
    while (changed) {
        changed = false;
        for (uint32_t n = c->node_count; n-- > 1u;) {
            const r_trie_node_t *nd = &c->nodes[n];
            if (n == *keep || nd->has_announce ||
                nd->first_child != R_NODE_NIL ||
                (views && intent_borrows_part(c, nd->part))) {
                continue;
            }
            if (freed == 0) {
                r_bump_route_epoch(c);
            }
            if (*keep == c->node_count - 1u) {
                *keep = n;   /* compaction moves the kept node into n */
            }
            trie_node_free(c, n);
            freed++;
            changed = true;
        }
    }
    return freed;
}


/* Nodes a reclaim would free right now (stats gauge). */
uint32_t
trie_reclaimable_count(const moqr_core_t *c)
{
    uint32_t nodes = 0;
    uint64_t bytes = 0;
    (void)trie_subtree_free(c, 0, R_NODE_NIL, false,
                            intent_ns_count(c) != 0, &nodes, &bytes);
    return nodes;
}


/* Walk ns; return node index or UINT32_MAX (no create). */
uint32_t
trie_walk(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t node = 0;
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            return UINT32_MAX;
        }
    }
    return node;
}


/* Longest prefix of `ns` with a live announce; UINT32_MAX if none. */
uint32_t
trie_longest_announce(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t best = UINT32_MAX;
    uint32_t node = 0;
    if (c->nodes[0].has_announce) {
        best = 0;
    }
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            break;
        }
        if (c->nodes[node].has_announce) {
            best = node;
        }
    }
    return best;
}


/* Depth of a node (number of parts from root). */
uint32_t
node_depth(const moqr_core_t *c, uint32_t node)
{
    uint32_t d = 0;
    while (c->nodes[node].parent != UINT32_MAX) {
        d++;
        node = c->nodes[node].parent;
    }
    return d;
}


/* Does `node`'s path start with the ns-sub prefix? prefix buffer layout is
 * a key without name lens... stored as: u32 count | u32 lens | bytes. */
bool
prefix_matches_node(const moqr_core_t *c, const r_ns_sub_t *nsub,
                    uint32_t node)
{
    uint32_t pcount = key_rd32(nsub->prefix);
    uint32_t depth = node_depth(c, node);
    if (depth < pcount) {
        return false;
    }
    /* Collect the first pcount parts of node's path. */
    uint32_t chain[32];
    uint32_t n = node, d = depth;
    while (d > 0) {
        chain[d - 1] = n;
        n = c->nodes[n].parent;
        d--;
    }
    uint32_t off = 4u * (1u + pcount);
    for (uint32_t i = 0; i < pcount; i++) {
        uint32_t plen = key_rd32(nsub->prefix + 4u * (1u + i));
        const r_trie_node_t *pn = &c->nodes[chain[i]];
        if (pn->part_len != plen ||
            (plen != 0 && memcmp(pn->part, nsub->prefix + off, plen) != 0)) {
            return false;
        }
        off += plen;
    }
    return true;
}


/* -- tracks ------------------------------------------------------------------------------ */

/* Find the live track for (ns, name) from the views; *hash_out receives the
 * key hash either way. UINT32_MAX = none. */
uint32_t
track_lookup(const moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name,
             uint64_t *hash_out)
{
    uint64_t hash = key_view_hash(ns, name);
    if (hash_out != NULL) {
        *hash_out = hash;
    }
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        const r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) != 0 && t->hash == hash &&
            key_view_eq(&t->key, ns, name)) {
            return i;
        }
    }
    return UINT32_MAX;
}


static void
track_free_slot_reason(moqr_core_t *c, uint32_t slot, uint32_t reason);


void
track_free_slot(moqr_core_t *c, uint32_t slot)
{
    track_free_slot_reason(c, slot, 0);
}


static void
track_free_slot_reason(moqr_core_t *c, uint32_t slot, uint32_t reason)
{
    r_track_t *t = &c->tracks[slot];
    intent_scrub_track(c, slot);
    /* Fold this log's evictions into the monotonic accumulator before the
     * log (and its counters) vanish, so objects_evicted_total never drops. */
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    c->evicted_freed += ls.evicted_records_total;
    moqr_log_destroy(t->log);
    t->log = NULL;
    key_release(c, &t->key);
    track_src_set(c, t, UINT32_MAX);
    /* The gen bump below ends every remaining member's track membership at
     * once; clear their index links now so a later retire of a stale member
     * never unlinks through a dead track's list. */
    for (uint32_t i = t->subs_head; i != R_SUB_NIL;) {
        uint32_t next = c->subs[i].track_next;
        c->subs[i].track_next = R_SUB_NIL;
        c->subs[i].track_prev = R_SUB_NIL;
        i = next;
    }
    t->subs_head = R_SUB_NIL;
    t->gen = r_gen_close(t->gen);
    r_room_changed(c);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 1 /* track */, slot, reason, 0, 0);
}


/* Record an interest change on a live track: it becomes the most recently
 * used, and any idle-retention clock restarts. */
void
track_touch(moqr_core_t *c, uint32_t slot)
{
    c->tracks[slot].interest_seq = ++c->interest_seq;
    c->tracks[slot].idle_since_us = 0;
}


/* Reclaimable: the track's retained objects are pure cache. WARM means the
 * source is released (no upstream binding, no pending upstream request, no
 * cross-shard demand: a mirrored track is PENDING or ACTIVE while its demand
 * stands). No subscription may read it (an owner-side cross-shard pump
 * subscription is an ordinary subscription), and no fetch may hold it. */
bool
track_reclaimable(const moqr_core_t *c, uint32_t slot)
{
    const r_track_t *t = &c->tracks[slot];
    return (t->gen & 1u) != 0 && t->state == R_TRACK_WARM &&
           !t->has_upstream_binding && t->source_kind == R_TRACK_SOURCE_NONE &&
           track_sub_count(c, slot) == 0 && track_fetch_count(c, slot) == 0;
}


/* The least recently used reclaimable track, or UINT32_MAX when every live
 * track is in use. With for_slot, only a track whose release leaves its slot
 * openable qualifies: reclaiming a track on its last generation retires the
 * slot and frees no slot. Ties cannot occur: interest_seq values are unique. */
static uint32_t
track_lru_reclaimable(const moqr_core_t *c, bool for_slot)
{
    uint32_t best = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        if (track_reclaimable(c, i) &&
            (!for_slot || r_gen_reopens(c->tracks[i].gen)) &&
            (best == UINT32_MAX ||
             c->tracks[i].interest_seq < c->tracks[best].interest_seq)) {
            best = i;
        }
    }
    return best;
}


/* Free a reclaimable track and everything it owns (log, interned key; it has
 * no subscriber links and no fetches by definition). The slot generation bump
 * makes every outstanding handle to it MOQR_ERR_STALE_HANDLE. */
void
track_reclaim(moqr_core_t *c, uint32_t slot, uint32_t reason)
{
    moqr_log_stats_t ls;
    moqr_log_get_stats(c->tracks[slot].log, &ls);
    c->evicted_freed += ls.record_count;   /* discarded cache records */
    if (reason == R_RECLAIM_CAPACITY) {
        c->tracks_evicted_capacity++;
    } else {
        c->tracks_evicted_idle++;
    }
    track_free_slot_reason(c, slot, reason);
}


moqr_result_t
track_make_log(moqr_core_t *c, uint32_t slot, moqr_log_t **out)
{
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), &c->alloc);
    lc.budget = c->log_budget;
    lc.max_subgroups_per_group = c->log_max_subgroups;
    lc.max_objects_per_group = c->log_max_objects;
    lc.max_cursors = c->log_max_cursors;
    lc.max_chunk_nodes = c->log_max_chunk_nodes;
    lc.trace = c->trace;
    lc.trace_id = slot;
    return moqr_log_create(&lc, out);
}


/* Retire a live fetch cursor: unpin, drop its read reference on the track
 * it was opened on (when that track is still the same generation), and stale
 * the handle. The one place a fetch ends. */
void
fetch_retire(moqr_core_t *c, r_fetch_t *f)
{
    fetch_unpin(c, f);
    if (f->track < c->max_tracks &&
        c->tracks[f->track].gen == f->track_gen_slot &&
        c->tracks[f->track].fetch_count != 0) {
        c->tracks[f->track].fetch_count--;
    }
    f->gen = r_gen_close(f->gen);
    r_room_changed(c);   /* a fetch slot, and maybe a reclaimable track */
}


void
track_fetch_invalidate(moqr_core_t *c, uint32_t track_slot,
                       uint32_t track_gen)
{
    for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
        r_fetch_t *f = &c->fetches[fi];
        if ((f->gen & 1u) != 0 && f->track == track_slot &&
            f->track_gen_slot == track_gen) {
            fetch_retire(c, f);
        }
    }
}


void
track_replace_retained_generation(moqr_core_t *c, uint32_t slot,
                                  moqr_log_t *new_log)
{
    r_track_t *t = &c->tracks[slot];
    uint32_t old_gen = t->gen;

    track_fetch_invalidate(c, slot, old_gen);

    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    c->evicted_freed += ls.evicted_records_total;
    moqr_log_destroy(t->log);
    t->log = new_log;
    t->has_largest = false;
    t->largest_group = 0;
    t->largest_object = 0;
    t->cluster_group_floor_set = false;
    t->cluster_group_floor = 0;
    t->cluster_requester_hop = 0;
    t->cluster_requester_correlation = 0;
    t->linger_deadline_us = 0;
    t->gen = r_gen_renew(t->gen);   /* keep the slot, stale old readers/handles */
    r_bump_route_epoch(c);
}


uint32_t
track_free_slot_find(const moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        if (r_gen_openable(c->tracks[i].gen)) {
            return i;
        }
    }
    return UINT32_MAX;
}


/*
 * Demand-driven reclaim: make room for an admission that needs a track slot
 * (need_slot), trie nodes (need_nodes) and interned name bytes (need_bytes),
 * by freeing unreferenced trie nodes and then least-recently-used reclaimable
 * tracks. Every pool these draw on holds only cache or index once it is
 * reclaimable, so a relay never refuses work while cache could make room.
 *
 * Preflight first, with no state change: the room reclaim can supply now is
 * compared with the shortfall. If it suffices, reclaim runs and MOQR_OK is
 * returned. If it would suffice only once unpolled intents release the trie
 * bytes they borrow, MOQR_ERR_WOULD_BLOCK (nothing touched; drain and retry).
 * Otherwise the admission is refused with MOQR_ERR_CAPACITY, counted against
 * the first resource that cannot be met. *keep_node (R_NODE_NIL when unused)
 * is a trie node the caller needs; it is never freed and follows compaction.
 */
/* The preflight alone: whether room for (slot, nodes, bytes) exists now
 * (MOQR_OK), only once intent views drain (MOQR_ERR_WOULD_BLOCK), or not at
 * all (MOQR_ERR_CAPACITY, with *reason the first unmet resource). Pure: no
 * mutation, no refusal counted. */
moqr_result_t
r_room_check(const moqr_core_t *c, bool need_slot, uint32_t need_nodes,
             uint64_t need_bytes, uint32_t keep, moqr_refuse_reason_t *reason)
{
    bool slot_short = need_slot && track_free_slot_find(c) == UINT32_MAX;
    uint64_t node_short =
        (uint64_t)c->node_count + need_nodes > c->max_ns_nodes
            ? (uint64_t)c->node_count + need_nodes - c->max_ns_nodes
            : 0;
    uint64_t byte_short =
        (uint64_t)c->intern_used + need_bytes > c->intern_budget
            ? (uint64_t)c->intern_used + need_bytes - c->intern_budget
            : 0;
    if (!slot_short && node_short == 0 && byte_short == 0) {
        return MOQR_OK;
    }
    uint32_t tn_now = 0, tn_all = 0;
    uint64_t tb_now = 0, tb_all = 0;
    if (node_short != 0 || byte_short != 0) {
        trie_free_room(c, keep, &tn_now, &tb_now, &tn_all, &tb_all);
    }
    uint32_t tracks = 0;   /* reclaimable tracks whose slot reopens */
    uint64_t track_bytes = 0;
    if (slot_short || byte_short != 0) {
        for (uint32_t i = 0; i < c->max_tracks; i++) {
            if (track_reclaimable(c, i)) {
                tracks += r_gen_reopens(c->tracks[i].gen) ? 1u : 0u;
                track_bytes += c->tracks[i].key.len;
            }
        }
    }
    bool slot_ok = !slot_short || tracks != 0;
    if (!(slot_ok && node_short <= tn_now &&
          byte_short <= tb_now + track_bytes)) {
        if (slot_ok && node_short <= tn_all &&
            byte_short <= tb_all + track_bytes) {
            return MOQR_ERR_WOULD_BLOCK;   /* intent views hold the room */
        }
        *reason = node_short > tn_all ? MOQR_REFUSE_NS_NODES
                  : !slot_ok          ? MOQR_REFUSE_TRACKS
                                      : MOQR_REFUSE_NAME_BYTES;
        return MOQR_ERR_CAPACITY;
    }
    return MOQR_OK;
}


moqr_result_t
r_make_room(moqr_core_t *c, bool need_slot, uint32_t need_nodes,
            uint64_t need_bytes, uint32_t *keep_node)
{
    uint32_t keep = keep_node != NULL ? *keep_node : R_NODE_NIL;
    moqr_refuse_reason_t reason = MOQR_REFUSE_TRACKS;
    moqr_result_t rc =
        r_room_check(c, need_slot, need_nodes, need_bytes, keep, &reason);
    if (rc == MOQR_ERR_CAPACITY) {
        return r_refuse(c, reason);
    }
    if (rc != MOQR_OK) {
        return rc;
    }
    bool slot_short = need_slot && track_free_slot_find(c) == UINT32_MAX;
    bool node_short = (uint64_t)c->node_count + need_nodes > c->max_ns_nodes;
    bool byte_short =
        (uint64_t)c->intern_used + need_bytes > c->intern_budget;
    if (!slot_short && !node_short && !byte_short) {
        return MOQR_OK;
    }

    /* Index first: trie nodes of withdrawn namespaces are not cache. */
    if (node_short || byte_short) {
        (void)trie_reclaim(c, &keep);
    }
    for (;;) {
        bool slot_more = slot_short && track_free_slot_find(c) == UINT32_MAX;
        bool need_more =
            slot_more ||
            (uint64_t)c->intern_used + need_bytes > c->intern_budget;
        if (!need_more) {
            break;
        }
        uint32_t victim = track_lru_reclaimable(c, slot_more);
        if (victim == UINT32_MAX) {
            break;   /* unreachable: the preflight counted the supply */
        }
        track_reclaim(c, victim, R_RECLAIM_CAPACITY);
    }
    if (keep_node != NULL) {
        *keep_node = keep;
    }
    return MOQR_OK;
}


/* Create a track for (ns, name), whose absence the caller has just
 * established with track_lookup(): make room (slot + name bytes), intern the
 * key, open the log, and take the slot. Room comes first so the relay never
 * holds more than max_tracks logs or name_intern_bytes of names, even
 * transiently; an allocation failure after an eviction therefore loses the
 * evicted track's cache (cache only, which the relay may drop). */
void track_install(moqr_core_t *c, uint32_t slot, r_key_t key,
                   uint64_t hash, moqr_log_t *log);


moqr_result_t
track_create(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name, uint64_t hash,
             uint32_t *out_slot)
{
    uint32_t key_len = 0;
    if (!key_view_len(ns, name, &key_len)) {
        return MOQR_ERR_INVAL;
    }
    moqr_result_t rc = r_make_room(c, true, 0, key_len, NULL);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint32_t slot = track_free_slot_find(c);
    r_key_t key = { NULL, 0 };
    rc = key_build(c, ns, name, &key);
    if (rc != MOQR_OK) {
        return rc;
    }
    moqr_log_t *log = NULL;
    rc = track_make_log(c, slot, &log);
    if (rc != MOQR_OK) {
        key_release(c, &key);
        return rc;
    }
    track_install(c, slot, key, hash, log);
    *out_slot = slot;
    return MOQR_OK;
}


/* Take the openable slot `slot` for a new track that owns `key` and `log`. */
void
track_install(moqr_core_t *c, uint32_t slot, r_key_t key, uint64_t hash,
              moqr_log_t *log)
{
    r_track_t *t = &c->tracks[slot];
    uint32_t gen = r_gen_open(t->gen);
    memset(t, 0, sizeof(*t));
    t->gen = gen;
    t->subs_head = R_SUB_NIL;
    t->key = key;
    t->hash = hash;
    t->log = log;
    t->track_gen = 0;
    t->src_ann_node = UINT32_MAX;   /* 0 is the valid root node, so set explicitly */
    track_touch(c, slot);
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 1 /* track */, slot, hash, 0, 0);
}


moqr_track_t
track_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_track_t){ r_pack(c, MOQR_HANDLE_POOL_TRACK,
                                  c->tracks[slot].gen, slot) };
}


moqr_sub_t
sub_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_sub_t){ r_pack(c, MOQR_HANDLE_POOL_SUB, c->subs[slot].gen,
                                slot) };
}
