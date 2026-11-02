#include "relay_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

/* -- intents ---------------------------------------------------------------------- */

bool
intent_space(const moqr_core_t *c, uint32_t need)
{
    return (uint64_t)c->intent_count + c->intent_reserved + need <= c->max_intents;
}


moqr_intent_t *
intent_push(moqr_core_t *c, moqr_intent_kind_t kind)
{
    moqr_intent_t *it =
        &c->intents[(c->intent_head + c->intent_count) % c->max_intents];
    c->intent_count++;
    if (c->intent_count > c->intent_hwm) {
        c->intent_hwm = c->intent_count;
    }
    memset(it, 0, sizeof(*it));
    it->kind = kind;
    return it;
}


/* Count an admission refusal against its resource and return the capacity
 * error. Centralizes the metric bump so every refusal path is observable. */
moqr_result_t
r_refuse(moqr_core_t *c, moqr_refuse_reason_t reason)
{
    if (reason < MOQR_REFUSE__COUNT) {
        c->refusals[reason]++;
    }
    return MOQR_ERR_CAPACITY;
}


void
intent_set_ns_from_key(moqr_intent_t *it, const r_key_t *key)
{
    uint32_t count = key_part_count(key);
    it->ns_count = count;
    for (uint32_t i = 0; i < count && i < 32; i++) {
        it->ns_parts[i] = key_part(key, i);
    }
    it->name = key_name(key);
}


size_t
moqr_core_poll_intents(moqr_core_t *c, moqr_intent_t *out, size_t cap)
{
    if (c == NULL || out == NULL || cap == 0) {
        return 0;
    }
    cluster_warm_flush(c);
    size_t n = c->intent_count < cap ? c->intent_count : cap;
    for (size_t i = 0; i < n; i++) {
        out[i] = c->intents[(c->intent_head + i) % c->max_intents];
    }
    c->intent_head = (uint32_t)((c->intent_head + n) % c->max_intents);
    c->intent_count -= (uint32_t)n;
    return n;
}


/* -- trie reclaim ---------------------------------------------------------------- *
 *
 * A trie node is only an index: it owns its part bytes, nothing else does. A
 * node is reclaimable when it is a leaf, carries no announce, and no unpolled
 * NS_FOUND / NS_GONE intent borrows its part bytes (intent views stay valid
 * until the consumer polls them). A track's src_ann_node does not pin: only
 * force-withdraw reads it, and only on a node that still carries an announce,
 * which is never reclaimed; a freed node clears the identity of any track that
 * named it. Namespace subscriptions hold their own prefix copy and never
 * reference nodes.
 *
 * Freed slots are compacted (the last node moves into the hole and every
 * index referring to it is rewritten), keeping the node array dense for every
 * node_count walk; moved nodes keep their part buffers, so borrowed views into
 * them stay valid. */

/* NS_FOUND / NS_GONE intents still in the ring (only these borrow trie
 * bytes). */
uint32_t
intent_ns_count(const moqr_core_t *c)
{
    uint32_t n = 0;
    for (uint32_t k = 0; k < c->intent_count; k++) {
        moqr_intent_kind_t kind =
            c->intents[(c->intent_head + k) % c->max_intents].kind;
        if (kind == MOQR_INTENT_NS_FOUND || kind == MOQR_INTENT_NS_GONE) {
            n++;
        }
    }
    return n;
}


bool
intent_borrows_part(const moqr_core_t *c, const uint8_t *part)
{
    if (part == NULL) {
        return false;   /* an empty part has no bytes to borrow */
    }
    for (uint32_t k = 0; k < c->intent_count; k++) {
        const moqr_intent_t *it =
            &c->intents[(c->intent_head + k) % c->max_intents];
        if (it->kind != MOQR_INTENT_NS_FOUND &&
            it->kind != MOQR_INTENT_NS_GONE) {
            continue;
        }
        for (uint32_t i = 0; i < it->ns_count && i < 32; i++) {
            if (it->ns_parts[i].data == part) {
                return true;
            }
        }
    }
    return false;
}


/* Fill intent ns views from a trie node's root path. */
void
intent_set_ns_from_node(const moqr_core_t *c, moqr_intent_t *it,
                        uint32_t node)
{
    uint32_t depth = node_depth(c, node);
    it->ns_count = depth;
    uint32_t n = node;
    for (uint32_t i = depth; i > 0; i--) {
        it->ns_parts[i - 1] = (moq_bytes_t){ .data = c->nodes[n].part,
                                             .len = c->nodes[n].part_len };
        n = c->nodes[n].parent;
    }
}


/* Drop the upstream work of a track that is being freed from the intent
 * ring. An unpolled UPSTREAM_SUBSCRIBE borrows the track's key bytes for its
 * namespace / name views, so it must not outlive the key; with the track gone
 * the upstream request has nothing to serve anyway. An UPSTREAM_UNSUBSCRIBE
 * for the same attempt is dropped with it: that upstream was never sent.
 * Order-preserving compaction of the ring. */
void
intent_scrub_track(moqr_core_t *c, uint32_t slot)
{
    uint64_t h = r_pack(c, MOQR_HANDLE_POOL_TRACK, c->tracks[slot].gen, slot);
    const moqr_intent_kind_t tomb = 0;   /* no intent kind is 0 */
    bool any = false;
    /* Mark: unsubscribes paired with a queued subscribe first (the lookup
     * needs the subscribes intact), then the subscribes. */
    for (uint32_t k = 0; k < c->intent_count; k++) {
        moqr_intent_t *it = &c->intents[(c->intent_head + k) % c->max_intents];
        if (it->kind != MOQR_INTENT_UPSTREAM_UNSUBSCRIBE ||
            it->track._opaque != h) {
            continue;
        }
        for (uint32_t j = 0; j < c->intent_count; j++) {
            const moqr_intent_t *sb =
                &c->intents[(c->intent_head + j) % c->max_intents];
            if (sb->kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
                sb->track._opaque == h && sb->track_gen == it->track_gen) {
                it->kind = tomb;
                any = true;
                break;
            }
        }
    }
    for (uint32_t k = 0; k < c->intent_count; k++) {
        moqr_intent_t *it = &c->intents[(c->intent_head + k) % c->max_intents];
        if (it->kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
            it->track._opaque == h) {
            it->kind = tomb;
            any = true;
        }
    }
    if (!any) {
        return;
    }
    uint32_t kept = 0;
    for (uint32_t k = 0; k < c->intent_count; k++) {
        const moqr_intent_t *it =
            &c->intents[(c->intent_head + k) % c->max_intents];
        if (it->kind == tomb) {
            continue;
        }
        if (kept != k) {
            c->intents[(c->intent_head + kept) % c->max_intents] = *it;
        }
        kept++;
    }
    c->intent_count = kept;
}
