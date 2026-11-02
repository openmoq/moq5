#include "relay_internal.h"
#include "bindings_internal.h"
#include "auth_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

r_binding_t *
binding_resolve(moqr_core_t *c, moqr_binding_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_BINDING ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_bindings ||
        c->bindings[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->bindings[slot];
}

void binding_unpin(r_binding_t *b);


/* -- ops: bindings -------------------------------------------------------------------------- */

moqr_result_t
moqr_core_binding_open(moqr_core_t *c, uint64_t session_cookie,
                       moqr_binding_t *out)
{
    return moqr_core_binding_open_with_options(c, session_cookie, NULL, out);
}


void
moqr_binding_options_init_sized(moqr_binding_options_t *options, size_t size)
{
    if (options == NULL || size < sizeof(uint32_t)) return;
    size_t n = size < sizeof(*options) ? size : sizeof(*options);
    memset(options, 0, n);
    options->struct_size = (uint32_t)n;
}


moqr_result_t
moqr_core_binding_open_with_options(moqr_core_t *c, uint64_t session_cookie,
                                    const moqr_binding_options_t *options,
                                    moqr_binding_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Frozen version-0 prefix, independent of any future struct extension. */
    if (options != NULL &&
        (options->struct_size < 8u ||
         (options->flags & ~MOQR_BINDING_TRACK_METADATA_MANAGED) != 0)) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_BINDING_INVALID;
    for (uint32_t i = 0; i < c->max_bindings; i++) {
        if (r_gen_openable(c->bindings[i].gen)) {
            r_binding_t *b = &c->bindings[i];
            uint32_t gen = r_gen_open(b->gen);
            /* The slot owns reusable pin storage across binding lifetimes. */
            r_pin_chunk_t *pin_chunks = b->pin_chunks;
            uint32_t pin_chunks_cap = b->pin_chunks_cap;
            memset(b, 0, sizeof(*b));
            b->pin_chunks = pin_chunks;
            b->pin_chunks_cap = pin_chunks_cap;
            b->gen = gen;
            b->cookie = session_cookie;
            b->relay_cost = 1;
            b->metadata_managed = options != NULL &&
                (options->flags & MOQR_BINDING_TRACK_METADATA_MANAGED) != 0;
            b->subs_head = R_SUB_NIL;   /* empty (memset left it 0) */
            out->_opaque = r_pack(c, MOQR_HANDLE_POOL_BINDING, gen, i);
            r_trace(c, MOQR_TRACE_ROUTE_ADD, 0 /* binding */, i,
                    session_cookie, 0, 0);
            return MOQR_OK;
        }
    }
    return r_refuse(c, MOQR_REFUSE_BINDINGS);
}


void
binding_unpin(r_binding_t *b)
{
    moq_rcbuf_decref(b->pin_payload);
    moq_rcbuf_decref(b->pin_properties);
    b->pin_payload = NULL;
    b->pin_properties = NULL;
    for (uint32_t i = 0; i < b->pin_chunk_count; i++) {
        moq_rcbuf_decref(b->pin_chunks[i].buf);
        b->pin_chunks[i].buf = NULL;
    }
    b->pin_chunk_count = 0;
    b->out_chunked = false;
    b->out_abandoned = false;
    b->out_evict_reset = false;
    b->out_chunk_base = 0;
}


/* Drop a HELD outstanding delivery without advancing any cursor: releases the
 * binding's one delivery slot + its pins and clears the notice, leaving the
 * sub's positions untouched so a later pass re-derives the same work. Used
 * when the held delivery's own subscription has just paused (Forward=0) — the
 * binding must not stay monopolized by a delivery that can no longer be
 * written, and its siblings must keep flowing; on resume the release re-peeks
 * from the unadvanced cursor, so exactly-once is preserved. */
void
binding_drop_held(r_binding_t *b)
{
    b->out_active = false;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    binding_unpin(b);
}


/* Pin every chunk of a chunked record for the outstanding delivery: incref the
 * zero-copy slices into the binding's reusable pin array so the object survives
 * same-track eviction across a multi-pump delivery. The array grows lazily to
 * chunk_count (a record can hold no more chunks than the log's chunk-node pool,
 * so the growth is bounded — see moqr_core_capacity_describe) and is reused.
 * Returns MOQR_ERR_CAPACITY if the array cannot be grown (OOM: the caller fails
 * the delivery closed and retries, never delivering a truncatable object). */
moqr_result_t
binding_pin_chunks(moqr_core_t *c, r_binding_t *b, const moqr_log_t *log,
                   const moqr_record_view_t *v, uint32_t base)
{
    /* Pin only the undelivered batch [base, chunk_count): base is the sub's
     * emitted watermark, so already-scheduled chunks (0..base-1) are not
     * re-pinned. moqr_core_delivery_chunk maps an absolute index back through
     * out_chunk_base == base. */
    uint32_t n = v->chunk_count > base ? v->chunk_count - base : 0;
    if (n > b->pin_chunks_cap) {
        r_pin_chunk_t *na = r_alloc(c, (size_t)n * sizeof(r_pin_chunk_t));
        if (na == NULL) {
            return MOQR_ERR_CAPACITY;
        }
        r_free(c, b->pin_chunks,
               (size_t)b->pin_chunks_cap * sizeof(r_pin_chunk_t));
        b->pin_chunks = na;
        b->pin_chunks_cap = n;
    }
    for (uint32_t i = 0; i < n; i++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_log_view_chunk(log, v, base + i, &cb, &clen) != MOQR_OK) {
            for (uint32_t j = 0; j < i; j++) {
                moq_rcbuf_decref(b->pin_chunks[j].buf);
                b->pin_chunks[j].buf = NULL;
            }
            return MOQR_ERR_INTERNAL;   /* base+i < chunk_count: never expected */
        }
        b->pin_chunks[i].buf = moq_rcbuf_incref((moq_rcbuf_t *)(void *)cb);
        b->pin_chunks[i].len = clen;
    }
    b->pin_chunk_count = n;
    return MOQR_OK;
}


/* Per-binding subscription list: an intrusive doubly-linked list of
 * a binding's live subscriptions, threaded through r_sub_t.sub_next/sub_prev and
 * headed at r_binding_t.subs_head. Pushed when a subscription is fully created,
 * removed in sub_retire — so moqr_core_next_delivery walks a binding's own subs
 * directly instead of scanning all c->max_subs. */
void
sub_list_push(moqr_core_t *c, uint32_t bslot, uint32_t sslot)
{
    r_binding_t *b = &c->bindings[bslot];
    r_sub_t *s = &c->subs[sslot];
    s->sub_prev = R_SUB_NIL;
    s->sub_next = b->subs_head;
    if (b->subs_head != R_SUB_NIL) {
        c->subs[b->subs_head].sub_prev = sslot;
    }
    b->subs_head = sslot;
}


static void
sub_list_remove(moqr_core_t *c, uint32_t bslot, uint32_t sslot)
{
    r_binding_t *b = &c->bindings[bslot];
    r_sub_t *s = &c->subs[sslot];
    if (s->sub_prev != R_SUB_NIL) {
        c->subs[s->sub_prev].sub_next = s->sub_next;
    } else if (b->subs_head == sslot) {
        b->subs_head = s->sub_next;   /* this sub was the head */
    }
    if (s->sub_next != R_SUB_NIL) {
        c->subs[s->sub_next].sub_prev = s->sub_prev;
    }
    s->sub_next = R_SUB_NIL;
    s->sub_prev = R_SUB_NIL;
}


/* Per-track subscriber index: an intrusive doubly-linked list over the sub
 * pool holding exactly the subs track_sub_count's membership predicate
 * accepts (live, bound to this track slot, generation-matched). Linked when
 * a subscription is created against a live track, unlinked at retire while
 * the track generation still matches; track_free_slot clears the whole list
 * before the track's gen bump. Link fields of non-members are meaningless
 * and never read (every walk starts at a live track's subs_head). */
void
track_sub_link(moqr_core_t *c, uint32_t tslot, uint32_t sslot)
{
    r_track_t *t = &c->tracks[tslot];
    r_sub_t *s = &c->subs[sslot];
    s->track_prev = R_SUB_NIL;
    s->track_next = t->subs_head;
    if (t->subs_head != R_SUB_NIL) {
        c->subs[t->subs_head].track_prev = sslot;
    }
    t->subs_head = sslot;
    track_touch(c, tslot);
}


static void
track_sub_unlink(moqr_core_t *c, uint32_t tslot, uint32_t sslot)
{
    r_track_t *t = &c->tracks[tslot];
    r_sub_t *s = &c->subs[sslot];
    if (s->track_prev != R_SUB_NIL) {
        c->subs[s->track_prev].track_next = s->track_next;
    } else if (t->subs_head == sslot) {
        t->subs_head = s->track_next;   /* this sub was the head */
    }
    if (s->track_next != R_SUB_NIL) {
        c->subs[s->track_next].track_prev = s->track_prev;
    }
    s->track_next = R_SUB_NIL;
    s->track_prev = R_SUB_NIL;
    track_touch(c, tslot);
    r_room_changed(c);   /* the track may now be reclaimable */
}


/* Begun live-edge accounting: a (group,list) position transitions to begun
 * (emitted 0 -> >0) or unbegun (>0 -> 0). Maintained so the evict-reset sweep
 * can skip a binding with no begun objects. The per-binding begun_subs tracks
 * how many of the binding's subs currently have any begun position, so the
 * sweep is an O(1) check when nothing is begun. */
void
sub_begun_add(moqr_core_t *c, r_sub_t *s)
{
    if (s->begun_count++ == 0) {
        c->bindings[s->binding].begun_subs++;
    }
}


void
sub_begun_sub(moqr_core_t *c, r_sub_t *s)
{
    if (s->begun_count > 0 && --s->begun_count == 0) {
        c->bindings[s->binding].begun_subs--;
    }
}


void
sub_retire(moqr_core_t *c, uint32_t slot, uint64_t now_us)
{
    r_sub_t *s = &c->subs[slot];
    /* Drop any begun-live-edge contribution before unlinking: a retiring sub can
     * never need an evict-reset, and leaving begun_subs armed would keep the
     * sweep scanning a dead binding (and, after slot reuse, mis-gate). */
    if (s->begun_count > 0) {
        if (c->bindings[s->binding].begun_subs > 0) {
            c->bindings[s->binding].begun_subs--;
        }
        s->begun_count = 0;
    }
    sub_list_remove(c, s->binding, slot);   /* unlink from the binding's list */
    uint32_t track_slot = s->track;
    if ((c->tracks[track_slot].gen & 1u) != 0 &&
        c->tracks[track_slot].gen == s->track_gen_slot) {
        /* Last subscriber arms the linger deadline. */
        if (track_sub_count(c, track_slot) == 1 /* this one */ &&
            c->tracks[track_slot].state == R_TRACK_ACTIVE &&
            c->tracks[track_slot].source_kind == R_TRACK_SOURCE_PULL) {
            c->tracks[track_slot].linger_deadline_us =
                now_us + c->linger_us;
            if (c->tracks[track_slot].linger_deadline_us == 0) {
                c->tracks[track_slot].linger_deadline_us = 1; /* armed */
            }
        }
        track_sub_unlink(c, track_slot, slot);
    }
    /* Clear an outstanding delivery pinned to this sub. */
    r_binding_t *b = &c->bindings[s->binding];
    if ((b->gen & 1u) != 0 && b->gen == s->binding_gen && b->out_active &&
        b->out_sub == slot) {
        b->out_active = false;
        binding_unpin(b);
    }
    /* The retire changes what this binding delivers next (a pinned delivery
     * may have been released; a sibling sub's work surfaces). Spurious for
     * the last sub of a closing binding — the close clears the bit. */
    if ((b->gen & 1u) != 0 && b->gen == s->binding_gen) {
        ready_mark(c, s->binding);
    }
    gpos_free(c, s);
    s->gen = r_gen_close(s->gen);
}



/* Longest live announce for `ns` NOT owned by the excluded binding
 * generation (the one whose loss is being handled); UINT32_MAX if none. */
static uint32_t
trie_longest_announce_excluding(const moqr_core_t *c, moqr_ns_t ns,
                                uint32_t excl_bslot, uint32_t excl_bgen)
{
    uint32_t best = UINT32_MAX;
    uint32_t node = 0;
    if (c->nodes[0].has_announce &&
        !(c->nodes[0].ann_binding == excl_bslot &&
          c->nodes[0].ann_binding_gen == excl_bgen)) {
        best = 0;
    }
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            break;
        }
        if (c->nodes[node].has_announce &&
            !(c->nodes[node].ann_binding == excl_bslot &&
              c->nodes[node].ann_binding_gen == excl_bgen)) {
            best = node;
        }
    }
    return best;
}


/* The SOURCE of an ACTIVE track with live downstream subscribers just died
 * by transport terminal. Handle the standing demand VISIBLY and
 * deterministically: retarget the track to the longest live alternate
 * announce (one fresh upstream SUBSCRIBE; established subscribers stay
 * active and resolve silently — upstream acceptance touches only PARKED
 * subs — with retained delivery state untouched, and the generation bump
 * refusing any stale resolution from the dead source), or — with no
 * alternate — terminate every subscriber explicitly, exactly as a graceful
 * source end would. Silently leaving an established subscription alive
 * with no source is forbidden.
 *
 * Reserve-before-mutate: one intent for the retarget, or one per live sub
 * for the terminal fan-out; MOQR_ERR_WOULD_BLOCK leaves the track
 * untouched (the resumable binding-close re-finds it by its unchanged
 * upstream identity). *retargeted reports which arm ran. */
moqr_result_t
track_source_failover(moqr_core_t *c, uint32_t tslot, uint32_t excl_bslot,
                      uint32_t excl_bgen, uint64_t excluded_lifetime,
                      uint64_t now_us, bool *retargeted)
{
    r_track_t *t = &c->tracks[tslot];
    moq_bytes_t parts[32];
    moqr_ns_t ns = { .parts = parts, .count = 0 };
    uint32_t count = key_part_count(&t->key);

    *retargeted = false;
    for (uint32_t i = 0; i < count && i < 32; i++) {
        parts[i] = key_part(&t->key, i);
    }
    ns.count = count < 32 ? count : 32;

    uint32_t pub_node = c->cluster_enabled
        ? cluster_failover_node(c, ns, t, excl_bslot, excl_bgen, excluded_lifetime)
        : trie_longest_announce_excluding(c, ns, excl_bslot, excl_bgen);

    if (pub_node != UINT32_MAX) {
        if (!intent_space(c, 1)) {
            return MOQR_ERR_WOULD_BLOCK;
        }
        if (c->cluster_enabled) {
            t->cluster_group_floor_set = t->has_largest;
            t->cluster_group_floor = t->largest_group;
            moqr_reset_desc_t reset;
            if (moqr_reset_desc_local(MOQR_RESET_CANCELLED, &reset) == MOQR_OK) {
                /* Every retained group belongs to the released source. Its
                 * delayed chunks/terminals cannot touch the replacement, so
                 * finish all old partials and unsealed streams right here. */
                uint32_t groups = moqr_log_group_count(t->log);
                for (uint32_t gi = 0; gi < groups; gi++) {
                    uint64_t group = moqr_log_group_id_at(t->log, gi);
                    (void)moqr_log_abandon_group_open(t->log, group, reset, NULL);
                    uint32_t lists = moqr_log_group_list_count(t->log, group);
                    for (uint32_t li = 0; li < lists; li++) {
                        moqr_record_view_t rec;
                        uint64_t subgroup;
                        if (!moqr_log_subgroup_sealed(t->log, group, li, &subgroup) &&
                            moqr_log_read_rec(t->log, group, li, 0, &rec) == MOQR_OK)
                            (void)moqr_log_reset_subgroup(t->log, group, rec.subgroup_id, reset);
                    }
                }
                ready_mark_track(c, tslot);
            }
            for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
                if ((c->fetches[fi].gen & 1u) != 0 && c->fetches[fi].track == tslot &&
                    c->fetches[fi].track_gen_slot == t->gen) fetch_retire(c, &c->fetches[fi]);
            }
        }
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
        moqr_intent_t *up = intent_push(c, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        t->metadata_managed = t->metadata_managed ||
            c->bindings[t->up_binding].metadata_managed;
        up->binding_cookie = c->bindings[t->up_binding].cookie;
        up->track = track_handle(c, tslot);
        up->track_gen = t->track_gen;
        intent_set_ns_from_key(up, &t->key);
        r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 2 /* failover */, tslot,
                t->track_gen, t->up_binding, 0);
        *retargeted = true;
        return MOQR_OK;
    }

    uint32_t n = track_sub_count(c, tslot);
    if (n > 0 && !intent_space(c, n)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint32_t tgen = t->gen;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *sub = &c->subs[i];
        if ((sub->gen & 1u) == 0 || sub->track != tslot ||
            sub->track_gen_slot != tgen) {
            continue;
        }
        uint64_t bcookie = 0;
        if (sub->binding < c->max_bindings &&
            c->bindings[sub->binding].gen == sub->binding_gen) {
            bcookie = c->bindings[sub->binding].cookie;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
        it->binding_cookie = bcookie;
        it->cookie = sub->cookie;
        it->sub = sub_handle(c, i);
        it->error_code = R_DONE_TRACK_ENDED;
        it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
        sub_retire(c, i, now_us);
    }
    if (c->cluster_enabled) {
        for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
            if ((c->fetches[fi].gen & 1u) != 0 && c->fetches[fi].track == tslot &&
                c->fetches[fi].track_gen_slot == t->gen) fetch_retire(c, &c->fetches[fi]);
        }
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_binding_close(moqr_core_t *c, moqr_binding_t bh, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
#ifdef MOQR_CORE_TESTING
    if (c->dbg_close_hold > 0) {
        c->dbg_close_hold--;
        c->dbg_close_held++;
        c->dbg_close_held_cookie = b->cookie;
        return MOQR_ERR_WOULD_BLOCK;   /* before any effect */
    }
#endif

    /* Resumable close. The atomic worst case (parked-sub rejects +
     * per-announce NS_GONE fan-out over possibly many announced namespaces)
     * can exceed the intent ring, so close makes bounded progress each call
     * and returns WOULD_BLOCK with a partial result; the caller drains and
     * retries. Each unit reserved here fits an empty ring under the create
     * clamp (rejects: 1 each; one announce's fan-out: <= max_ns_subs).
     * Retries re-scan and skip already-processed items (retired subs,
     * cleared announces), so no per-binding cursor state is needed. */

    /* Phase 1: reject parked subs on tracks this binding upstreams — with
     * the upstream gone they can never resolve — then transition each track
     * once its parked set is clear. */
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0 || !t->has_upstream_binding ||
            t->up_binding != bslot || t->up_binding_gen != b->gen) {
            continue;
        }
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t s = 0; s < c->max_subs; s++) {
                r_sub_t *sub = &c->subs[s];
                if ((sub->gen & 1u) == 0 || sub->track != i ||
                    sub->track_gen_slot != t->gen ||
                    sub->state != R_SUB_PARKED) {
                    continue;
                }
                if (!intent_space(c, 1)) {
                    return MOQR_ERR_WOULD_BLOCK;   /* partial; retry */
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
                it->cookie = sub->cookie;
                it->binding_cookie = c->bindings[sub->binding].cookie;
                it->error_code = R_ERR_DOES_NOT_EXIST;
                sub_retire(c, s, now_us);
            }
        }
        if (t->state == R_TRACK_ACTIVE && track_sub_count(c, i) > 0) {
            /* standing established demand: retarget or terminate — the
             * one thing this close may NOT do is strand it silently */
            bool retargeted = false;
            moqr_result_t frc =
                track_source_failover(c, i, bslot, b->gen, 0, now_us,
                                      &retargeted);
            if (frc == MOQR_ERR_WOULD_BLOCK) {
                return MOQR_ERR_WOULD_BLOCK;   /* partial; retry */
            }
            if (retargeted) {
                continue;   /* the track now belongs to the alternate */
            }
        }
        t->has_upstream_binding = false;
        t->source_kind = R_TRACK_SOURCE_NONE;
        t->state = R_TRACK_WARM;
        track_touch(c, i);   /* source released: last use */
        r_room_changed(c);
        t->track_gen++;   /* stale any in-flight resolution */
        moqr_log_stats_t ls;
        moqr_log_get_stats(t->log, &ls);
        if (ls.record_count == 0 && track_sub_count(c, i) == 0 &&
            track_fetch_count(c, i) == 0) {
            track_free_slot(c, i);
        }
    }

    if (c->cluster_enabled) {
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            r_cluster_route_t *rr = &c->cluster_routes[ri];
            if (rr->used && rr->binding == bslot && rr->binding_gen == b->gen) {
                moqr_result_t crc = cluster_unannounce(c, bslot, b->gen, rr->node, rr->lifetime);
                if (crc != MOQR_OK) return crc;
            }
        }
    }

    /* Phase 2: announces owned by this binding — clear + NS_GONE fan-out,
     * one announce at a time (reserve its matching-watcher count first). */
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce || c->nodes[n].ann_binding != bslot ||
            c->nodes[n].ann_binding_gen != b->gen) {
            continue;
        }
        uint32_t matching = 0;
        for (uint32_t s = 0; s < c->max_ns_subs; s++) {
            if (c->ns_subs[s].used &&
                !(c->ns_subs[s].binding == bslot &&
                  c->ns_subs[s].binding_gen == b->gen) &&
                prefix_matches_node(c, &c->ns_subs[s], n)) {
                matching++;
            }
        }
        if (!intent_space(c, matching)) {
            return MOQR_ERR_WOULD_BLOCK;   /* this announce next retry */
        }
        c->nodes[n].has_announce = false;
        r_room_changed(c);
        r_bump_route_epoch(c);
        r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0 /* announce */, n, bslot, 0,
                0);
        for (uint32_t s = 0; s < c->max_ns_subs; s++) {
            if (c->ns_subs[s].used &&
                !(c->ns_subs[s].binding == bslot &&
                  c->ns_subs[s].binding_gen == b->gen) &&
                prefix_matches_node(c, &c->ns_subs[s], n)) {
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
                it->binding_cookie =
                    c->bindings[c->ns_subs[s].binding].cookie;
                it->cookie = c->ns_subs[s].cookie;
                intent_set_ns_from_node(c, it, n);
                it->value = key_rd32(c->ns_subs[s].prefix);
            }
        }
    }

    /* Phase 3: intent-free cleanup (only reached once phases 1-2 finish).
     * Subs owned by this binding retire; its namespace subscriptions free. */
    for (uint32_t s = 0; s < c->max_subs; s++) {
        if ((c->subs[s].gen & 1u) != 0 && c->subs[s].binding == bslot &&
            c->subs[s].binding_gen == b->gen) {
            sub_retire(c, s, now_us);
        }
    }
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used && c->ns_subs[s].binding == bslot &&
            c->ns_subs[s].binding_gen == b->gen) {
            uint32_t plen = c->ns_subs[s].prefix_len;
            c->intern_used -= plen;
            r_free(c, c->ns_subs[s].prefix, plen);
            r_room_changed(c);
            memset(&c->ns_subs[s], 0, sizeof(c->ns_subs[s]));
        }
        if (c->tsubs[s].used && c->tsubs[s].binding == bslot &&
            c->tsubs[s].binding_gen == b->gen) {
            tsub_free(c, s);
        }
    }
    for (uint32_t r = 0; r < c->max_rtoks; r++) {
        if (c->rtoks[r].used && c->rtoks[r].binding == bslot &&
            c->rtoks[r].binding_gen == b->gen) {
            r_free_material(c, &c->rtoks[r].mat, &c->intern_used);
            memset(&c->rtoks[r], 0, sizeof(c->rtoks[r]));
        }
    }
    /* Fetch cursors owned by this binding: unpin (release pin-byte budget) and
     * invalidate. Intent-free — the session/adapter tears down the fetch streams
     * on session close, so no wire action is queued here. */
    for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
        r_fetch_t *f = &c->fetches[fi];
        if ((f->gen & 1u) != 0 && f->binding == bslot &&
            f->binding_gen == b->gen) {
            fetch_retire(c, f);
        }
    }
    b->out_active = false;
    binding_unpin(b);
    /* The slot is reusable after the gen bump: a stale ready bit would
     * resolve to the REPLACEMENT binding's cookie on a later drain. */
    ready_clear(c, bslot);
    b->gen = r_gen_close(b->gen);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 2 /* binding */, bslot, 0, 0, 0);
    return MOQR_OK;
}

#ifdef MOQR_CORE_TESTING


/* Test-only: the next `attempts` calls of moqr_core_binding_close on this core
 * (any live binding) return MOQR_ERR_WOULD_BLOCK before any effect, as a close
 * that cannot yet make progress does; 0 releases the hold. The core's owner
 * thread only, like every other core call. Never compiled into a production
 * core. */
void
moqr_core_debug_hold_binding_close(moqr_core_t *c, uint32_t attempts)
{
    if (c != NULL) {
        c->dbg_close_hold = attempts;
    }
}


/* Test-only: how many closes the hold refused, and the cookie of the binding
 * the last one named. */
uint32_t
moqr_core_debug_binding_close_held(const moqr_core_t *c, uint64_t *out_cookie)
{
    if (c == NULL) {
        return 0;
    }
    if (out_cookie != NULL) {
        *out_cookie = c->dbg_close_held_cookie;
    }
    return c->dbg_close_held;
}

#endif /* MOQR_CORE_TESTING */
