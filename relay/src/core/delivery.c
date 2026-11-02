#include "relay_internal.h"
#include "delivery_internal.h"
#include "bindings_internal.h"
#include "intents_internal.h"
#include "subscribe_internal.h"
#include "tracks_internal.h"

uint64_t gpos_stride_for(uint64_t lists);

uint64_t gpos_hash_cap_for(uint64_t groups);


/* Whole 64-bit words for one ready bit per binding slot. Widened before the
 * +63 round-up: at max_bindings == UINT32_MAX a 32-bit add wraps and would
 * size the bitset at ZERO words while the capacity descriptor (64-bit
 * arithmetic) charges the true count. One helper feeds allocation,
 * destruction, draining, and the descriptor so they can never disagree. */
uint32_t
ready_word_count(uint32_t max_bindings)
{
    return (uint32_t)(((uint64_t)max_bindings + 63u) >> 6);
}

void gpos_free(moqr_core_t *c, r_sub_t *s);


uint32_t track_sub_count(const moqr_core_t *c, uint32_t track_slot);

uint32_t track_fetch_count(const moqr_core_t *c, uint32_t track_slot);


/* Seal-acknowledgement bit words per entry: one bit per position list. */
static uint32_t
gpos_acked_words(const r_sub_t *s)
{
    return (s->gpos_lists + 31u) / 32u;
}


/* Per-(sub, group) position-entry stride for `lists` position lists:
 * entry header + idx[] + emitted[] + seal-acked, gap and lifetime bit
 * words + the owed-cause record index per list, rounded up to
 * uint64_t alignment so entries beyond the first keep group_id naturally
 * aligned. Shared by the runtime and moqr_core_capacity_describe — the
 * advertised ceiling must never drift from the allocation. */
uint64_t
gpos_stride_for(uint64_t lists)
{
    /* Computed WIDE; callers reject a stride the runtime's 32-bit layout
     * cannot represent (create and describe share this single formula). */
    uint64_t raw = (uint64_t)sizeof(r_gpos_t) + 12u * lists +
                   3u * 4u * ((lists + 31u) / 32u);
    return (raw + 7u) & ~(uint64_t)7u;   /* 64-bit mask: ~7u would truncate */
}


/* The per-sub gpos hash index: a power of two >= 2x the group entries (load
 * factor <= 0.5), computed WIDE. Shared by gpos_alloc, create's gate, and
 * the capacity describe; callers reject a result above UINT32_MAX. */
uint64_t
gpos_hash_cap_for(uint64_t groups)
{
    uint64_t cap = 8u;
    while (cap < groups * 2u) {
        cap <<= 1u;
    }
    return cap;
}


static uint32_t
gpos_stride(const r_sub_t *s)
{
    /* Create rejected any layout whose wide stride exceeds UINT32_MAX. */
    return (uint32_t)gpos_stride_for(s->gpos_lists);
}


r_gpos_t *
gpos_at(const r_sub_t *s, uint32_t i)
{
    return (r_gpos_t *)(s->gpos + (size_t)i * gpos_stride(s));
}
uint32_t *
gpos_emit(const r_sub_t *s, r_gpos_t *e)
{
    return &e->idx[s->gpos_lists];
}


/* Seal-acknowledgement bit words follow emitted[] in the same block. A set
 * bit = this sub already acknowledged the (group, list) seal via its SEAL
 * notice (the only durable FIN path — records carry subgroup_end as
 * advisory metadata only), so it is never notified twice. */
static uint32_t *
gpos_acked(const r_sub_t *s, r_gpos_t *e)
{
    return &e->idx[2u * s->gpos_lists];
}


static bool
gpos_seal_acked(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    return ((gpos_acked(s, e)[list / 32u] >> (list % 32u)) & 1u) != 0;
}


static void
gpos_seal_ack(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    gpos_acked(s, e)[list / 32u] |= 1u << (list % 32u);
}


/* Zero an entry's per-list body — idx[], emitted[], and the seal-acked
 * words: a claimed or reused slot must never inherit stale positions or a
 * stale seal acknowledgement. */
/* Gap bit words follow the seal-acked words. A set bit = this sub skipped a
 * record of the list that it never began (an expired Object, draft-22 §10.3),
 * so the list's next record it delivers is not the next object of the stream
 * the skipped one would have continued (§11.3.2). Cleared when a record of
 * the list is delivered. */
static uint32_t *
gpos_gap(const r_sub_t *s, r_gpos_t *e)
{
    return &gpos_acked(s, e)[gpos_acked_words(s)];
}


static bool
gpos_gap_get(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    return ((gpos_gap(s, e)[list / 32u] >> (list % 32u)) & 1u) != 0;
}


static void
gpos_gap_set(const r_sub_t *s, r_gpos_t *e, uint32_t list, bool on)
{
    uint32_t bit = 1u << (list % 32u);
    if (on) {
        gpos_gap(s, e)[list / 32u] |= bit;
    } else {
        gpos_gap(s, e)[list / 32u] &= ~bit;
    }
}


/* Lifetime bit words follow the gap words. A set bit = this sub passed
 * over, as outside its window, the first record of a new upstream stream
 * lifetime of the list (a record moqr_log_internal_rec_flags marks
 * follows_gap): the list's next record it delivers opens a new downstream
 * stream although that record does not mark the boundary itself, and a seal
 * this sub acknowledged for the earlier lifetime does not cover it. Passing
 * over a record outside the window is not a missing object, so no gap bit is
 * set. Cleared when a record of the list is delivered. */
static uint32_t *
gpos_renew(const r_sub_t *s, r_gpos_t *e)
{
    return &gpos_gap(s, e)[gpos_acked_words(s)];
}


static bool
gpos_renew_get(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    return ((gpos_renew(s, e)[list / 32u] >> (list % 32u)) & 1u) != 0;
}


static void
gpos_renew_set(const r_sub_t *s, r_gpos_t *e, uint32_t list, bool on)
{
    uint32_t bit = 1u << (list % 32u);
    if (on) {
        gpos_renew(s, e)[list / 32u] |= bit;
    } else {
        gpos_renew(s, e)[list / 32u] &= ~bit;
    }
}


/* The owed-cause words follow the lifetime words, one per list: 1 + the
 * index of the first record marking a new upstream lifetime that this sub
 * passed over outside its window since it last delivered a record of the
 * list (0: none). That record carries how the lifetime of the downstream
 * stream this sub may still have open ended; a later boundary passed over
 * belongs to a lifetime never opened downstream. Cleared with the lifetime
 * bit when a record of the list is delivered. An index, not a pointer: the
 * record stays where it is for as long as its group's entry lives. */
static uint32_t *
gpos_owed(const r_sub_t *s, r_gpos_t *e)
{
    return &gpos_renew(s, e)[gpos_acked_words(s)];
}


/* This sub passes over the list's record at its cursor without beginning
 * it (outside its window, expired, or abandoned before it began): when the
 * record opens a new upstream lifetime and none has been passed over since
 * the sub last delivered a record of the list, it is the boundary that
 * says how the lifetime of the downstream stream the sub may still have open
 * ended. True when the record opens a new lifetime. */
static bool
gpos_pass_over(const r_sub_t *s, r_gpos_t *e, const moqr_log_t *log, uint64_t g,
               uint32_t list_ref, uint32_t pos_slot)
{
    bool sgp = false, boundary = false;
    moqr_reset_desc_t br = moqr_reset_desc_none();
    moqr_log_internal_rec_flags(log, g, list_ref, e->idx[pos_slot], &sgp, &boundary, &br);
    if (boundary && !gpos_renew_get(s, e, pos_slot)) {
        gpos_renew_set(s, e, pos_slot, true);
        gpos_owed(s, e)[pos_slot] = e->idx[pos_slot] + 1u;
    }
    return boundary;
}


/* The cause owed to the downstream stream of the list: false when no
 * boundary was passed over; otherwise true with the passed-over boundary's
 * reset in *out (none when that lifetime ended cleanly). */
static bool
gpos_owed_cause(const r_sub_t *s, r_gpos_t *e, const moqr_log_t *log, uint64_t g,
                uint32_t list_ref, uint32_t pos_slot, moqr_reset_desc_t *out)
{
    if (!gpos_renew_get(s, e, pos_slot) || gpos_owed(s, e)[pos_slot] == 0)
        return false;
    bool sgp = false, boundary = false;
    *out = moqr_reset_desc_none();
    moqr_log_internal_rec_flags(log, g, list_ref, gpos_owed(s, e)[pos_slot] - 1u,
                                &sgp, &boundary, out);
    return true;
}


static void
gpos_body_clear(const r_sub_t *s, r_gpos_t *e)
{
    memset(e->idx, 0, 12u * (size_t)s->gpos_lists +
                          3u * 4u * (size_t)gpos_acked_words(s));
}


/* -- Per-sub group_id -> gpos-slot index ---------------------------------------- *
 * The gpos entries stay at fixed array slots (pointer-stable — callers hold
 * r_gpos_t*), keyed by a stable group_id (log group ids are monotonic and never
 * reused). A small open-addressing index maps group_id -> slot for O(1) lookup,
 * replacing the former O(max_groups) linear scan in gpos_entry / delivery_done.
 * Eviction reclaim keeps the exact prior rule but is gated (gpos_gc) on the log's
 * monotonic evicted_groups_total, so it runs only when the log actually evicted;
 * a begun-evicted entry (emitted > 0) is KEPT until its evict-reset is confirmed
 * (the begun-evicted-reset guarantee). */

static uint32_t
gpos_hash0(uint64_t group_id, uint32_t cap)
{
    /* Fibonacci hash — the top bits of the product are well mixed. */
    return (uint32_t)((group_id * 0x9E3779B97F4A7C15ull) >> 48) & (cap - 1u);
}


/* Array slot for group_id, or R_GPOS_NIL. O(1) average. */
uint32_t
gpos_hash_find(const r_sub_t *s, uint64_t group_id)
{
    if (s->gpos_hash == NULL) {
        return R_GPOS_NIL;
    }
    uint32_t cap = s->gpos_hash_cap;
    uint32_t i = gpos_hash0(group_id, cap);
    for (uint32_t probe = 0; probe < cap; probe++) {
        uint32_t slot = s->gpos_hash[i];
        if (slot == R_GPOS_NIL) {
            return R_GPOS_NIL;   /* empty run: not present */
        }
        const r_gpos_t *e = gpos_at(s, slot);
        if (e->occupied && e->group_id == group_id) {
            return slot;
        }
        i = (i + 1u) & (cap - 1u);
    }
    return R_GPOS_NIL;
}


/* Insert group_id -> slot; the key must not already be present. */
static void
gpos_hash_insert(r_sub_t *s, uint64_t group_id, uint32_t slot)
{
    uint32_t cap = s->gpos_hash_cap;
    uint32_t i = gpos_hash0(group_id, cap);
    while (s->gpos_hash[i] != R_GPOS_NIL) {
        i = (i + 1u) & (cap - 1u);
    }
    s->gpos_hash[i] = slot;
}


/* Rebuild the index from the live array entries (drops mappings left stale by a
 * reclaim). O(gpos_groups); only on reclaim, never on the delivery path. */
static void
gpos_hash_rebuild(r_sub_t *s)
{
    if (s->gpos_hash == NULL) {
        return;
    }
    for (uint32_t i = 0; i < s->gpos_hash_cap; i++) {
        s->gpos_hash[i] = R_GPOS_NIL;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        if (gpos_at(s, i)->occupied) {
            gpos_hash_insert(s, gpos_at(s, i)->group_id, i);
        }
    }
}


/* -- Binding-ready set (produced here, drained by the binding layer) ----------
 *
 * ready_mark records that a binding MAY now have deliverable work; a set bit
 * re-marked is a no-op (dedup by construction) and the count tracks set bits
 * exactly. Pseudo-bindings (shard pump subs, cookie >= MOQR_SHARD_COOKIE_BASE)
 * are filtered at production: they never enter the set, never consume drain
 * capacity, and the shard data phase keeps its own polling. A spurious mark is
 * legal (it costs the consumer one empty probe); a MISSED readiness edge is
 * not — every core-owned transition that can make a binding deliverable must
 * mark. Nothing in the core consumes the set. */
void
ready_mark(moqr_core_t *c, uint32_t bslot)
{
    const r_binding_t *b = &c->bindings[bslot];
    if ((b->gen & 1u) == 0 || b->cookie >= MOQR_SHARD_COOKIE_BASE) {
        return;
    }
    uint64_t *w = &c->ready_words[bslot >> 6];
    uint64_t bit = 1ull << (bslot & 63u);
    if ((*w & bit) == 0) {
        *w |= bit;
        c->ready_count++;
    }
}


void
ready_clear(moqr_core_t *c, uint32_t bslot)
{
    uint64_t *w = &c->ready_words[bslot >> 6];
    uint64_t bit = 1ull << (bslot & 63u);
    if ((*w & bit) != 0) {
        *w &= ~bit;
        c->ready_count--;
    }
}


/* Track-wide readiness (ingest family, seal): mark every ACTIVE subscriber's
 * binding through the per-track index. PARKED subs are not deliverable and
 * are marked at activation instead. */
void
ready_mark_track(moqr_core_t *c, uint32_t tslot)
{
    if ((c->tracks[tslot].gen & 1u) == 0) {
        return;
    }
    for (uint32_t i = c->tracks[tslot].subs_head; i != R_SUB_NIL;
         i = c->subs[i].track_next) {
        if (c->subs[i].state == R_SUB_ACTIVE) {
            ready_mark(c, c->subs[i].binding);
        }
    }
}


/* Gated eviction reclaim (was inline in the former gpos_entry scan). Runs only
 * when the log evicted since this sub last swept. Reclaims entries whose group
 * is gone AND that the sub never began (emitted == 0), marking the skip; a
 * begun-evicted entry is kept so next_delivery's sweep can surface its reset
 * before reclaim. Rebuilds the index iff anything was reclaimed. */
static void
gpos_gc(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    if (s->gpos == NULL) {
        return;
    }
    uint64_t evicted = moqr_log_evicted_groups_total(log);
    if (evicted <= s->gpos_gc_evicted) {
        return;   /* nothing evicted since last sweep */
    }
    s->gpos_gc_evicted = evicted;
    bool reclaimed = false;
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (!e->occupied) {
            continue;
        }
        if (moqr_log_group_list_count(log, e->group_id) != 0) {
            continue;   /* still retained */
        }
        moqr_record_view_t probe;
        if (moqr_log_read_rec(log, e->group_id, MOQR_LOG_LIST_DATAGRAM, 0,
                              &probe) != MOQR_ERR_TOO_OLD) {
            continue;   /* present-but-empty, not evicted */
        }
        bool begun = false;
        for (uint32_t li = 0; li < s->gpos_lists; li++) {
            if (gpos_emit(s, e)[li] > 0) {
                begun = true;
                break;
            }
        }
        if (!begun) {
            e->occupied = false;   /* reclaim */
            s->pending_skip = true;
            ready_mark(c, s->binding);   /* the skip must be reported */
            reclaimed = true;
        }
        /* begun: keep the slot until its evict-reset is confirmed (belt-and-
         * suspenders — next_delivery's evict-reset sweep runs before this and
         * surfaces the reset first; preserved from the earlier reclaim-in-place
         * behavior). */
    }
    if (reclaimed) {
        gpos_hash_rebuild(s);
    }
}


moqr_result_t
gpos_alloc(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    s->gpos_groups = moqr_log_max_groups(log);
    s->gpos_lists = moqr_log_max_subgroups(log) + 1u;   /* + datagram list */
    size_t bytes = (size_t)s->gpos_groups * gpos_stride(s);
    s->gpos = r_alloc(c, bytes);
    if (s->gpos == NULL) {
        return MOQR_ERR_NOMEM;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        gpos_at(s, i)->occupied = false;
    }
    /* Wide shared resolver; create rejected any cap above UINT32_MAX. */
    uint32_t cap = (uint32_t)gpos_hash_cap_for(s->gpos_groups);
    s->gpos_hash_cap = cap;
    s->gpos_hash = r_alloc(c, (size_t)cap * sizeof(uint32_t));
    if (s->gpos_hash == NULL) {
        r_free(c, s->gpos, bytes);
        s->gpos = NULL;
        return MOQR_ERR_NOMEM;
    }
    for (uint32_t i = 0; i < cap; i++) {
        s->gpos_hash[i] = R_GPOS_NIL;
    }
    if (s->has_location) {
        s->excluded_cap = log->max_records;
        size_t n = s->excluded_cap / 8u + (s->excluded_cap % 8u != 0);
        s->excluded = r_alloc(c, n);
        if (s->excluded == NULL) {
            gpos_free(c, s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->excluded, 0, n);
    }
    /* Start even with the log's eviction watermark: this sub has no entries yet,
     * so there is nothing to reclaim for past evictions. */
    s->gpos_gc_evicted = moqr_log_evicted_groups_total(log);
    return MOQR_OK;
}


void
gpos_free(moqr_core_t *c, r_sub_t *s)
{
    if (s->excluded != NULL) {
        r_free(c, s->excluded,
               s->excluded_cap / 8u + (s->excluded_cap % 8u != 0));
        s->excluded = NULL;
    }
    if (s->gpos != NULL) {
        r_free(c, s->gpos, (size_t)s->gpos_groups * gpos_stride(s));
        s->gpos = NULL;
    }
    if (s->gpos_hash != NULL) {
        r_free(c, s->gpos_hash, (size_t)s->gpos_hash_cap * sizeof(uint32_t));
        s->gpos_hash = NULL;
    }
}


/* O(1) lookup of this sub's positions entry for a group, creating it on first
 * access. Eviction reclaim is handled by gpos_gc (run once per
 * sub_best_candidate), so this is pure find-or-allocate — no per-call scan. */
static r_gpos_t *
gpos_entry(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log,
           uint64_t group_id)
{
    (void)c;
    (void)log;
    uint32_t slot = gpos_hash_find(s, group_id);
    if (slot != R_GPOS_NIL) {
        return gpos_at(s, slot);
    }
    /* First access to this group: claim a free array slot. gpos_gc has already
     * reclaimed evicted (non-begun) entries, and retained groups never exceed the
     * gpos capacity, so a free slot exists. */
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (!e->occupied) {
            e->occupied = true;
            e->group_id = group_id;
            gpos_body_clear(s, e);
            gpos_hash_insert(s, group_id, i);
            return e;
        }
    }
    return NULL;   /* cannot happen: slots >= retained group cap */
}


/* Count live subs bound to a track slot. */
uint32_t
track_sub_count(const moqr_core_t *c, uint32_t track_slot)
{
    if ((c->tracks[track_slot].gen & 1u) == 0) {
        return 0;   /* dead track: no live sub can match an even gen */
    }
    uint32_t n = 0;
    for (uint32_t i = c->tracks[track_slot].subs_head; i != R_SUB_NIL;
         i = c->subs[i].track_next) {
        n++;
    }
    return n;
}


/* Live fetch cursors reading this track. A fetch is a track READER: while one is
 * open the track must not be age-evicted or freed (else its accepted, retained
 * range could vanish mid-fetch — a silent truncation). Cleanup resumes once the
 * fetch closes. */
uint32_t
track_fetch_count(const moqr_core_t *c, uint32_t track_slot)
{
    const r_track_t *t = &c->tracks[track_slot];
    return (t->gen & 1u) != 0 ? t->fetch_count : 0;
}


/* Resolve a sub's filter start/end from a largest location. */
void
sub_resolve_filter(r_sub_t *s, const moqr_filter_t *f, bool has_largest,
                   uint64_t lg, uint64_t lo)
{
    s->filter_type = f->type;
    s->has_end = false;
    s->end_object = UINT64_MAX;
    s->location_empty = false;
    switch (f->type) {
    case MOQR_FILTER_LARGEST_OBJECT:
        if (has_largest) {
            s->start_group = lg;
            s->start_object = lo;
            if (lo < UINT64_MAX) {
                s->start_object++;
            } else if (lg < UINT64_MAX) {
                /* No later object fits in this group; carry the lower bound. */
                s->start_group = lg + 1;
                s->start_object = 0;
            } else {
                s->location_empty = true;
            }
        } else {
            s->start_group = 0;
            s->start_object = 0;
        }
        break;
    case MOQR_FILTER_NEXT_GROUP_START:
        if (has_largest) {
            s->start_group = lg;
            s->start_object = 0;
            if (lg < UINT64_MAX) {
                s->start_group++;
            } else {
                s->location_empty = true;
            }
        } else {
            s->start_group = 0;
            s->start_object = 0;
        }
        break;
    case MOQR_FILTER_CURRENT_GROUP_START:
        if (has_largest) {
            s->start_group = lg;
            s->start_object = 0;
        } else {
            s->start_group = 0;
            s->start_object = 0;
        }
        break;
    case MOQR_FILTER_RELATIVE_START:
        /* {lg + 1 - N, 0}, clamped to [0, 2^64-1]; with no Largest the
         * Next Group is {0,0}, and so is every start before it. */
        s->start_object = 0;
        if (!has_largest) {
            s->start_group = 0;
        } else if (f->start_group == 0) {
            s->start_group = lg == UINT64_MAX ? UINT64_MAX : lg + 1;
        } else {
            s->start_group = f->start_group - 1 > lg ? 0 : lg - (f->start_group - 1);
        }
        break;
    case MOQR_FILTER_ABSOLUTE_RANGE:
        s->has_end = true;
        s->end_group = f->start_group + f->end_group_delta;
        /* fall through */
    case MOQR_FILTER_ABSOLUTE_START:
    default:
        s->start_group = f->start_group;
        s->start_object = f->start_object;
        break;
    }
}


bool
location_valid(const moq_location_filter_cfg_t *loc)
{
    if (loc == NULL || loc->struct_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE) {
        return false;
    }
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        return true;
    case MOQ_LOCATION_FILTER_GROUP_END:
    case MOQ_LOCATION_FILTER_OBJECT_END:
        return loc->end_group >= loc->start_group;
    default:
        return false;
    }
}


static bool
sub_location_passes(const r_sub_t *s, uint64_t group, uint64_t object)
{
    return !s->location_empty &&
           (group > s->start_group ||
            (group == s->start_group && object >= s->start_object)) &&
           (!s->has_end || group < s->end_group ||
            (group == s->end_group && object <= s->end_object));
}


void
sub_resolve_location(r_sub_t *s, bool has, uint64_t lg, uint64_t lo)
{
    const moq_location_filter_cfg_t *loc = &s->location;
    s->start_group = s->start_object = 0;
    s->end_group = s->end_object = UINT64_MAX;
    s->has_end = s->location_empty = false;
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
        break;
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        if (has) {
            s->start_group = lg;
            if (lo < UINT64_MAX) {
                s->start_object = lo + 1;
            } else if (lg < UINT64_MAX) {
                s->start_group = lg + 1;
            } else {
                s->location_empty = true;
            }
        }
        break;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        if (has) {
            uint64_t n = loc->relative_group_offset;
            s->start_group = n == 0 ? (lg == UINT64_MAX ? lg : lg + 1)
                                   : (n - 1 > lg ? 0 : lg - (n - 1));
        }
        break;
    default:
        s->start_group = loc->start_group;
        s->start_object = loc->start_object;
        if (loc->kind == MOQ_LOCATION_FILTER_GROUP_END ||
            loc->kind == MOQ_LOCATION_FILTER_OBJECT_END) {
            s->has_end = true;
            s->end_group = loc->end_group;
            if (loc->kind == MOQ_LOCATION_FILTER_OBJECT_END) {
                s->end_object = loc->end_object;
                s->location_empty = s->start_group == s->end_group &&
                                    s->start_object > s->end_object;
            }
        }
        break;
    }
}


static void
sub_exclude(r_sub_t *s, uint64_t seq, bool excluded)
{
    uint32_t slot = (uint32_t)(seq % s->excluded_cap);
    uint8_t mask = (uint8_t)(1u << (slot % 8u));
    if (excluded) s->excluded[slot / 8u] |= mask;
    else s->excluded[slot / 8u] &= (uint8_t)~mask;
}


bool
sub_excluded(const r_sub_t *s, uint64_t seq)
{
    if (s->excluded == NULL) return false;
    uint32_t slot = (uint32_t)(seq % s->excluded_cap);
    return (s->excluded[slot / 8u] & (1u << (slot % 8u))) != 0;
}


/* Record exclusions before replacing a window, including records behind a
 * blocked head. Bits survive further updates; new journal occupants overwrite
 * their bit at ingest, so eviction/reuse cannot inherit an old exclusion. */
void
sub_exclude_retained(r_sub_t *s, const moqr_log_t *log)
{
    if (s->excluded == NULL) return;
    for (uint32_t gi = 0; gi < log->group_count; gi++) {
        const moqr_group_t *g = &log->groups[gi];
        for (uint32_t li = 0; li <= g->sg_count; li++) {
            const moqr_rec_vec_t *v = li == g->sg_count ? &g->dg : &g->sgs[li].v;
            for (uint32_t i = 0; i < v->count; i++) {
                const moqr_rec_t *rec = &v->recs[i];
                if (!sub_location_passes(s, rec->group_id, rec->object_id)) {
                    sub_exclude(s, rec->arrival_seq, true);
                }
            }
        }
    }
}


void
track_location_ingest(moqr_core_t *c, r_track_t *t,
                      const moqr_log_append_desc_t *desc)
{
    uint64_t seq = t->log->next_seq - 1u;
    for (uint32_t i = t->subs_head; i != R_SUB_NIL; i = c->subs[i].track_next) {
        r_sub_t *s = &c->subs[i];
        if (s->excluded != NULL) {
            sub_exclude(s, seq, !sub_location_passes(s, desc->group_id,
                                                      desc->object_id));
        }
    }
}


/* -- delivery --------------------------------------------------------------------------------------------- */

bool
rec_passes(const r_sub_t *s, const moqr_record_view_t *v)
{
    /* rec_passes is the COMPLETE-record filter predicate: whole-object/status
     * records (chunk_count == 0, payload in the view) and COMPLETE-but-chunked
     * records (chunk_count > 0, payload NULL) pass; OPEN and ABANDONED do not.
     * OPEN live-edge delivery and ABANDONED-after-begin reset are handled by the
     * scheduling path AROUND this helper (sub_best_candidate selects an OPEN head
     * with new chunks and surfaces a begun ABANDONED head for reset, which
     * carries the terminal reset code forwarded downstream). */
    if (v->obj_state != MOQR_OBJ_COMPLETE || s->location_empty) {
        return false;
    }
    if (s->excluded != NULL) {
        return sub_location_passes(s, v->group_id, v->object_id) &&
               !sub_excluded(s, v->arrival_seq);
    }
    if (v->group_id < s->start_group ||
        (v->group_id == s->start_group && v->object_id < s->start_object)) {
        return false;
    }
    return true;
}


/* Group comparison honoring the sub's order preference (spec rule 3). */
static int
group_cmp(const r_sub_t *s, uint64_t a, uint64_t b)
{
    if (a == b) {
        return 0;
    }
    bool asc = s->group_order != MOQR_GROUP_ORDER_DESCENDING;
    return (a < b) == asc ? -1 : 1;
}


typedef struct r_cand {
    bool               have;
    uint32_t           list;      /* idx[] slot to advance on DELIVERED   */
    uint32_t           emitted;   /* this sub's live-edge watermark for the
                                   * list: the batch (and pin) base        */
    moqr_record_view_t view;
    bool               subgroup_properties;  /* upstream header PROPERTIES */
    moqr_reset_desc_t  gap_reset;            /* owed to the stream it does not
                                              * continue: an upstream RESET  */
    bool               follows_gap;          /* not the next object of the
                                              * stream its list continues  */
} r_cand_t;


/* Spec §7.1.1 within one subscription: publisher priority, then group
 * order, then lowest subgroup, then object/arrival. Datagram-preference
 * candidates rank after subgroup candidates at equal priority (cross-
 * preference order is implementation-dependent, -18 :2399-2401); within
 * the datagram list, delivery follows arrival order (the rule-4 lowest-
 * object tie-break is approximated; datagrams carry no ordering
 * obligation). */
static bool
cand_better(const r_sub_t *s, const r_cand_t *cand, const r_cand_t *best)
{
    if (!best->have) {
        return true;
    }
    const moqr_record_view_t *a = &cand->view, *b = &best->view;
    if (a->publisher_priority != b->publisher_priority) {
        return a->publisher_priority < b->publisher_priority;
    }
    int gc = group_cmp(s, a->group_id, b->group_id);
    if (gc != 0) {
        return gc < 0;
    }
    uint64_t sga = a->datagram_pref ? UINT64_MAX : a->subgroup_id;
    uint64_t sgb = b->datagram_pref ? UINT64_MAX : b->subgroup_id;
    if (sga != sgb) {
        return sga < sgb;
    }
    return a->arrival_seq < b->arrival_seq;
}


/* A pending SEAL notice discovered during the candidate scan: the lowest
 * (group, list) whose subgroup is sealed, fully consumed by this sub, and
 * not yet acknowledged. */
typedef struct r_seal_note {
    bool     have;
    uint64_t group;
    uint32_t list;
    uint64_t subgroup_id;
    bool     reset;        /* the terminal was a RESET: the notice's close
                            * is abnormal, carrying reset_code             */
    moqr_reset_desc_t reset_desc;
} r_seal_note_t;


/* Best deliverable record for one subscription: scan the heads of every
 * retained (group, list) position, consuming records below the filter
 * start in place. Returns false when nothing is deliverable. `out_seal`
 * (optional) collects the lowest pending SEAL notice seen along the way —
 * the scan already reads every list head, so late-FIN detection costs one
 * acked-bit test on drained lists and a sealed lookup only when that bit is
 * clear. */
/* The Object a record holds may no longer start downstream: the source's
 * MAX_CACHE_DURATION has passed since it arrived (draft-22 §10.3). */
bool
track_rec_expired(const r_track_t *t, const moqr_record_view_t *v,
                  uint64_t now_us)
{
    return t->has_max_cache && now_us >= v->arrival_us &&
           now_us - v->arrival_us >= t->max_cache_us;
}


static bool
sub_best_candidate(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log,
                   r_cand_t *out, bool *out_range_done,
                   r_seal_note_t *out_seal, uint64_t now_us,
                   moqr_core_delivery_eligible_fn eligible, void *ctx,
                   bool *capacity_blocked)
{
    uint32_t gn = moqr_log_group_count(log);
    *out_range_done = false;
    *capacity_blocked = false;
    r_cand_t best = { .have = false };
    bool any_in_range = false;
    bool blocked_open = false;   /* an in-range OPEN head is pending */

    /* Reclaim this sub's entries for groups evicted since last time (gated on the
     * log's eviction watermark), so gpos_entry below is pure O(1) find-or-add. */
    gpos_gc(c, s, log);

    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (s->excluded == NULL && !s->location_empty && g < s->start_group) {
            continue;
        }
        if (s->excluded == NULL && !s->location_empty && s->has_end &&
            g > s->end_group) {
            break;   /* ids ascend; nothing further can be in range */
        }
        any_in_range = true;
        r_gpos_t *e = gpos_entry(c, s, log, g);
        if (e == NULL) {
            continue;
        }
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t slot = 0; slot <= lists; slot++) {
            bool dg = slot == lists;   /* last position slot = datagrams */
            uint32_t pos_slot = dg ? s->gpos_lists - 1 : slot;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : slot;
            for (;;) {
                moqr_record_view_t v;
                moqr_result_t rc = moqr_log_read_rec(log, g, list_ref,
                                                     e->idx[pos_slot], &v);
                if (rc != MOQR_OK) {
                    /* MOQR_DONE: this sub has consumed every retained record
                     * of the list. A SEALED list reached this way owes the
                     * sub a late-FIN notification — the seal landed after
                     * the final record was delivered (or the cursor skipped
                     * past it), so no record delivery will ever carry
                     * subgroup_end. Groups and slots iterate ascending, so
                     * the first hit is the lowest (group, subgroup). */
                    if (rc == MOQR_DONE && !dg && out_seal != NULL &&
                        (!out_seal->have || out_seal->group == g) &&
                        !gpos_seal_acked(s, e, pos_slot)) {
                        /* Groups iterate ascending, so the first group with
                         * an eligible seal wins; WITHIN it, list slots are
                         * insertion-ordered, so take the lowest WIRE
                         * subgroup id across the group's eligible lists. */
                        uint64_t sgid = 0;
                        if (moqr_log_subgroup_sealed(log, g, slot, &sgid) &&
                            (!out_seal->have ||
                             sgid < out_seal->subgroup_id)) {
                            out_seal->have = true;
                            out_seal->group = g;
                            out_seal->list = pos_slot;
                            out_seal->subgroup_id = sgid;
                            /* Both terminal flavors share eligibility and
                             * acknowledgement; the flavor only decides how
                             * the bind closes downstream. */
                            out_seal->reset_desc = moqr_reset_desc_none();
                            /* After a lifetime boundary passed over, the
                             * stream this terminal closes is the one this
                             * sub last had open: it ends as that lifetime
                             * ended, not as the list's latest one did. */
                            moqr_reset_desc_t owed;
                            if (gpos_owed_cause(s, e, log, g, list_ref, pos_slot, &owed)) {
                                out_seal->reset_desc = owed;
                                out_seal->reset = owed.tag != MOQR_CODE_NONE;
                            } else {
                                out_seal->reset = moqr_log_subgroup_reset(
                                    log, g, slot, &out_seal->reset_desc);
                            }
                            /* This sub skipped an object it never began
                             * after the last one it delivered -- expired, or
                             * omitted for a paused subscription whose source
                             * finished: its stream misses that object, so
                             * whatever the upstream terminal, it is reset,
                             * never FINed (draft-22 §10.3, §11.3.2). The
                             * omission is the subscription ending early
                             * (CANCELLED); an expiry is this relay falling
                             * behind. */
                            if (!out_seal->reset &&
                                gpos_gap_get(s, e, pos_slot) &&
                                ((s->src_done && !s->forward)
                                     ? moqr_reset_desc_local(
                                           MOQR_RESET_CANCELLED,
                                           &out_seal->reset_desc)
                                     : moqr_reset_desc_internal(
                                           MOQR_RESET_CODE_EVICTED,
                                           &out_seal->reset_desc)) == MOQR_OK) {
                                out_seal->reset = true;
                            }
                        }
                    }
                    break;
                }
                uint32_t emit_raw = gpos_emit(s, e)[pos_slot];
                uint32_t emitted = emit_raw & GPOS_EMIT_MASK;
                bool begun = emit_raw != 0;
                /* A begun object must still finish/reset after a window change.
                 * Excluded unbegun records advance only their actual cursor. */
                bool excluded = s->excluded != NULL
                    ? sub_excluded(s, v.arrival_seq) ||
                      !sub_location_passes(s, v.group_id, v.object_id)
                    : s->location_empty || v.group_id < s->start_group ||
                      (v.group_id == s->start_group && v.object_id < s->start_object);
                if (excluded && !begun) {
                    if (!dg) gpos_pass_over(s, e, log, g, list_ref, pos_slot);
                    e->idx[pos_slot]++;
                    continue;
                }
                /* Live edge: an OPEN head (the last record in this
                 * list) is selectable only while it has grown past what this sub
                 * already scheduled downstream. Once drained, HOLD the cursor (no
                 * consume) until more chunks arrive or it flips terminal — that
                 * hold is what keeps a publisher-stalled object from blocking the
                 * list, and consuming it would skip the object forever. */
                if (v.obj_state == MOQR_OBJ_OPEN && v.chunk_count <= emitted) {
                    /* In-range OPEN head, drained for now: it is still pending
                     * (more chunks, then complete or abandon), so it blocks range
                     * completion — a bounded sub must not retire while an in-range
                     * object is unfinished (possibly begun downstream). */
                    blocked_open = true;
                    break;
                }
                /* An ABANDONED head this sub NEVER began downstream is skipped
                 * permanently; if it was begun (header shipped — even with zero
                 * chunks emitted) it is selected so the bind can RESET the
                 * downstream subgroup. `begun` is tracked separately from the
                 * chunk count precisely so a begin-with-zero-chunks still resets. */
                if (v.obj_state == MOQR_OBJ_ABANDONED && !begun) {
                    if (!dg) gpos_pass_over(s, e, log, g, list_ref, pos_slot);
                    e->idx[pos_slot]++;
                    continue;
                }
                /* An expired Object is not started (§10.3). Within a subgroup
                 * that leaves a hole: the sub's next record of the list must
                 * not continue the stream (§7.4, §11.3.2). */
                if (!begun && track_rec_expired(&c->tracks[s->track], &v, now_us)) {
                    if (!dg) {
                        /* The missing object is a hole in the lifetime it
                         * belongs to: in the one whose downstream stream this
                         * sub has open only while no boundary has been passed
                         * over, otherwise in a later lifetime it never opened
                         * downstream, whose terminal is not this stream's. */
                        bool later = gpos_renew_get(s, e, pos_slot);
                        if (gpos_pass_over(s, e, log, g, list_ref, pos_slot)) later = true;
                        if (!later) gpos_gap_set(s, e, pos_slot, true);
                    }
                    e->idx[pos_slot]++;
                    continue;
                }
                /* A paused subscription (Forward=0) whose source finished
                 * asked for nothing more: an Object it never began is never
                 * sent. Like an expired one it leaves a hole, so the stream
                 * the sub has open for the list ends with a reset, never a FIN
                 * (draft-18 section 11.4.3, draft-22 section 11.3.2: omitting
                 * a Subgroup Object for the Forward State), delivered as the
                 * list's seal before the subscription's terminal. */
                if (!begun && s->src_done && !s->forward) {
                    if (!dg) {
                        bool later = gpos_renew_get(s, e, pos_slot);
                        if (gpos_pass_over(s, e, log, g, list_ref, pos_slot)) later = true;
                        if (!later) gpos_gap_set(s, e, pos_slot, true);
                    }
                    e->idx[pos_slot]++;
                    continue;
                }
                /* Selectable: OPEN with new chunks, COMPLETE (deliver the
                 * remaining emitted..chunk_count then end), or ABANDONED-begun. */
                if (eligible != NULL && !eligible(ctx, s->cookie, &v)) {
                    *capacity_blocked = true;
                    break;   /* leave this list's head and ledgers untouched */
                }
                r_cand_t cand = { .have = true,
                                  .list = pos_slot,
                                  .emitted = emitted,
                                  .view = v };
                moqr_log_internal_rec_flags(log, g, list_ref, e->idx[pos_slot],
                                            &cand.subgroup_properties,
                                            &cand.follows_gap, &cand.gap_reset);
                if (!dg && (gpos_gap_get(s, e, pos_slot) ||
                            gpos_renew_get(s, e, pos_slot)))
                    cand.follows_gap = true;
                /* After a boundary passed over outside the window, the
                 * stream this record does not continue is the one this sub
                 * last had open, and it ends as its upstream lifetime ended:
                 * the cause the first such boundary carries, not that of any
                 * later lifetime it never opened. */
                if (!dg)
                    (void)gpos_owed_cause(s, e, log, g, list_ref, pos_slot,
                                          &cand.gap_reset);
                if (cand_better(s, &cand, &best)) {
                    best = cand;
                }
                break;
            }
        }
    }
    if (best.have) {
        *out = best;
        return true;
    }
    /* Range completion: the range end was passed by the live edge and no
     * in-range record remains deliverable — but NOT while an in-range OPEN head
     * is still pending (it must complete or be reset/abandoned first, else a
     * begun downstream object would be left without a terminal). */
    if (s->has_end && !blocked_open && !*capacity_blocked) {
        const r_track_t *t = &c->tracks[s->track];
        if (s->excluded != NULL) {
            *out_range_done = t->has_largest &&
                (t->largest_group > s->end_group ||
                 (t->largest_group == s->end_group &&
                  t->largest_object > s->end_object));
        } else if (gn > 0 && moqr_log_group_id_at(log, gn - 1) > s->end_group) {
            *out_range_done = true;
        }
    }
    (void)any_in_range;
    return false;
}


/* Find a group this sub BEGAN an OPEN object in (some emitted>0) that has since
 * been evicted from the log. The begun downstream object can never complete, so
 * it must be reset. Returns true + sets *out_group; the gpos slot is retained
 * (gpos_entry does not reclaim begun slots) until the reset is confirmed. */
static bool
sub_evicted_begun_group(r_sub_t *s, const moqr_log_t *log, uint64_t *out_group)
{
    if (s->gpos == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (!e->occupied) {
            continue;
        }
        bool begun = false;
        for (uint32_t li = 0; li < s->gpos_lists; li++) {
            if (gpos_emit(s, e)[li] > 0) {
                begun = true;
                break;
            }
        }
        if (!begun) {
            continue;
        }
        moqr_record_view_t probe;
        if (moqr_log_read_rec(log, e->group_id, MOQR_LOG_LIST_DATAGRAM, 0,
                              &probe) == MOQR_ERR_TOO_OLD) {
            *out_group = e->group_id;
            return true;
        }
    }
    return false;
}


/* True iff `s` is provably FINISHED on a track that itself ended: the log
 * carries a received END_OF_TRACK, and every in-range position from the
 * sub's effective start to the newest retained group is either delivered or
 * authoritatively closed. "Authoritatively closed" means below the log's
 * eviction floor (which an upstream watermark advances via
 * moqr_core_note_track_evicted_below) — an UNKNOWN gap (a missing group
 * above the floor, or an unsealed subgroup list) blocks retirement: unknown
 * is neither delivered nor nonexistent (draft-18 §2.1), so the subscription
 * must stay live. The caller has already established there is nothing
 * deliverable, no pending skip, and no pending seal notice for this sub.
 * Read-only: no cursor moves, no reclaim, no allocation. */
static bool
sub_eot_complete(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    (void)c;
    moqr_log_stats_t ls;
    moqr_log_get_stats(log, &ls);
    if (!ls.end_of_track || ls.group_count == 0) {
        return false;
    }
    uint64_t floor = moqr_log_evicted_floor(log);
    uint64_t expect = s->start_group > floor ? s->start_group : floor;
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < expect) {
            continue;   /* below the start filter or the floor: closed */
        }
        if (s->has_end && g > s->end_group) {
            break;      /* beyond the subscribed range: irrelevant */
        }
        if (g != expect) {
            return false;   /* a missing group above the floor: UNKNOWN */
        }
        expect = g + 1;
        uint32_t slot = gpos_hash_find(s, g);
        if (slot == R_GPOS_NIL) {
            return false;   /* untouched retained group: not delivered */
        }
        r_gpos_t *e = gpos_at(s, slot);
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t li = 0; li <= lists; li++) {
            bool dg = li == lists;
            uint32_t pos_slot = dg ? s->gpos_lists - 1u : li;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : li;
            moqr_record_view_t v;
            if (moqr_log_read_rec(log, g, list_ref, e->idx[pos_slot], &v) ==
                MOQR_OK) {
                return false;   /* undelivered record (defensive: the caller
                                 * already scanned and found nothing) */
            }
            if (!dg) {
                /* A subgroup list must be FINAL: sealed, or ended by its
                 * last record's end_of_group. Unsealed = could still have
                 * grown when the terminal raced the FIN = UNKNOWN. */
                uint64_t sgid = 0;
                if (!moqr_log_subgroup_sealed(log, g, li, &sgid) &&
                    !moqr_log_subgroup_is_final(
                        log, g, li,
                        e->idx[pos_slot] > 0 ? e->idx[pos_slot] - 1u : 0)) {
                    return false;
                }
            }
        }
    }
    return true;
}


static moqr_result_t
core_next_delivery(moqr_core_t *c, moqr_binding_t bh, uint64_t now_us,
                   moqr_delivery_t *out, moqr_core_delivery_eligible_fn eligible,
                   void *ctx)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (b->out_active) {
        /* If the held ordinary-RECORD delivery's own subscription has paused
         * (Forward=0), do NOT replay it — that write would draw
         * MOQ_ERR_WRONG_STATE and wrongly retire the sub, and holding it would
         * monopolize the binding against its siblings. Drop the hold (cursor
         * unadvanced) and fall through to select a sibling; resume re-derives
         * it. A held MAINTENANCE delivery (recordless notice or abandoned/evict
         * reset) is NOT dropped even while paused — stream cleanup must still
         * complete — so it re-peeks below. */
        if ((c->subs[b->out_sub].gen & 1u) != 0 &&
            !c->subs[b->out_sub].forward &&
            b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned) {
            binding_drop_held(b);
        } else {
            /* A chunked, abandoned-reset, or notice delivery is held across a
             * downstream WOULD_BLOCK: re-peek is idempotent so the binding
             * resumes the same object's chunk cursor, retries the reset, or
             * re-attempts the notice verbatim. Whole-object deliveries never
             * re-peek (they commit in the same pump), so their outstanding
             * state is still a caller error. */
            if (b->out_chunked || b->out_abandoned ||
                b->out_notice != MOQR_DELIVERY_NOTICE_NONE) {
                *out = b->out_view;
                return MOQR_OK;
            }
            return MOQR_ERR_WRONG_STATE;
        }
    }

    /* Evict-reset sweep (before normal scheduling): if any sub BEGAN an OPEN
     * object downstream whose group has since been evicted, the peer is left with
     * a begun-but-unfinishable object — surface a synthetic ABANDONED delivery so
     * the bind resets every begun subgroup of that group. Higher priority than
     * ordinary delivery so a hang is cleared promptly. */
    /* Walk only this binding's subs (the per-binding sub list), and pick the
     * lowest-slot evicted-begun sub — the same one the former slot-ordered scan
     * returned first. Gate: a begun-evicted group requires a sub with an
     * emitted>0 position, so skip the whole sweep when no sub on the binding has
     * begun anything (begun_subs==0), and skip individual subs that have not
     * (begun_count==0). Conservative: begun_subs>0 with no eviction still enters
     * and finds nothing (a false positive, harmless); it is never a false
     * negative — every begun position is counted, so a reset is never missed.
     * begun_subs==0 is the steady state for whole-object delivery. */
    if (b->begun_subs > 0) {
        c->evict_sweeps++;
        uint32_t evict_sub = R_SUB_NIL;
        uint64_t evict_group = 0;
        for (uint32_t i = b->subs_head; i != R_SUB_NIL;
             i = c->subs[i].sub_next) {
            r_sub_t *s = &c->subs[i];
            if ((s->gen & 1u) == 0 || s->binding != bslot ||
                s->binding_gen != b->gen || s->state != R_SUB_ACTIVE) {
                continue;
            }
            /* NB: a PAUSED (Forward=0) sub is NOT skipped here — a begun-evicted
             * object still needs its downstream reset (stream maintenance is not
             * object transmission), so the evict-reset sweep stays eligible while
             * paused. Only ordinary record scheduling is suppressed (below). */
            if (s->begun_count == 0) {
                continue;   /* no begun position => no begun-evicted group */
            }
            r_track_t *t = &c->tracks[s->track];
            if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot) {
                continue;
            }
            uint64_t eg = 0;
            if (!sub_evicted_begun_group(s, t->log, &eg)) {
                continue;
            }
            if (evict_sub == R_SUB_NIL || i < evict_sub) {
                evict_sub = i;
                evict_group = eg;
            }
        }
        if (evict_sub != R_SUB_NIL) {
            uint32_t i = evict_sub;
            r_sub_t *s = &c->subs[i];
            b->out_sub = i;
            b->out_group = evict_group;
            b->out_list = 0;           /* reset is by group, not by list */
            b->out_abandoned = true;
            b->out_evict_reset = true;
            b->out_chunked = false;
            b->out_chunk_base = 0;
            b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
            b->pin_payload = NULL;
            b->pin_properties = NULL;
            b->out_active = true;
            memset(out, 0, sizeof(*out));
            out->sub = sub_handle(c, i);
            out->sub_cookie = s->cookie;
            out->oldest_group = UINT64_MAX;
            out->rec.group_id = evict_group;
            out->rec.obj_state = MOQR_OBJ_ABANDONED;
            /* Eviction is this relay's own cause, never a draft's number:
             * build it as a local meaning before anything is staged. */
            if (moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED,
                                         &out->rec.reset) != MOQR_OK) {
                return MOQR_ERR_INTERNAL;
            }
            out->evicted_reset = true;
            b->out_view = *out;
            return MOQR_OK;
        }
    }

    bool have = false;
    uint32_t best_slot = 0;
    r_cand_t best;
    memset(&best, 0, sizeof(best));
    /* Pending recordless notice. A sub carries at most one notice per pass —
     * an unacknowledged eviction watermark (which also blocks that sub's
     * record work: its positions are not trustworthy until the downstream
     * closed what the jump orphaned), else its lowest pending seal — and the
     * notice IS that sub's candidate in the ordinary cross-subscription
     * schedule: subscriber priority first, then sub slot among notices. A
     * notice never blocks a SIBLING subscription's records; it only outranks
     * a record at equal subscriber priority (it is payload-free and unblocks
     * a downstream FIN/close). */
    uint32_t ntc_sub = R_SUB_NIL;
    uint8_t  ntc_kind = MOQR_DELIVERY_NOTICE_NONE;
    r_seal_note_t ntc_seal;
    memset(&ntc_seal, 0, sizeof(ntc_seal));
#define NTC_BETTER(c, i, ntc)                                              \
    ((ntc) == R_SUB_NIL ||                                                 \
     (c)->subs[(i)].subscriber_priority <                                  \
         (c)->subs[(ntc)].subscriber_priority ||                           \
     ((c)->subs[(i)].subscriber_priority ==                                \
          (c)->subs[(ntc)].subscriber_priority &&                          \
      (i) < (ntc)))

    for (uint32_t i = b->subs_head; i != R_SUB_NIL;) {
        uint32_t next = c->subs[i].sub_next;   /* save: sub_retire may unlink i */
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->binding != bslot ||
            s->binding_gen != b->gen || s->state != R_SUB_ACTIVE) {
            i = next;
            continue;
        }
        r_track_t *t = &c->tracks[s->track];
        if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot) {
            i = next;
            continue;
        }
        if (s->src_abort) {
            /* Its source failed: nothing more is delivered, the terminal goes
             * out as soon as the ring has room. */
            if (!intent_space(c, 1)) {
                ready_mark(c, bslot);   /* retire on a later scan */
            } else {
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = b->cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->pd = s->src_done_pd;
                sub_retire(c, i, now_us);
            }
            i = next;
            continue;
        }
        if (s->pending_skip) {
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
            }
            i = next;
            continue;   /* records blocked until the watermark is acked */
        }
        r_cand_t cand;
        bool range_done = false;
        bool capacity_blocked = false;
        r_seal_note_t seal;
        memset(&seal, 0, sizeof(seal));
        if (!sub_best_candidate(c, s, t->log, &cand, &range_done, &seal,
                                now_us, eligible, ctx, &capacity_blocked)) {
            if (s->pending_skip) {
                /* Fresh jump found by the scan's gpos_gc: EVICT_WATERMARK
                 * precedes any SEAL for the same sub. */
                if (NTC_BETTER(c, i, ntc_sub)) {
                    ntc_sub = i;
                    ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
                }
            } else if (seal.have) {
                /* An owed seal holds this sub open whether or not it wins
                 * this pass: its close (a FIN or the upstream reset) must
                 * reach the subscriber before any terminal retires it. */
                if (NTC_BETTER(c, i, ntc_sub)) {
                    ntc_sub = i;
                    ntc_kind = MOQR_DELIVERY_NOTICE_SEAL;
                    ntc_seal = seal;
                }
            } else if (capacity_blocked) {
                /* Still owes records; lack of a stream slot is not source
                 * completion, range completion or subscription retirement. */
            } else if (range_done && !s->range_end_holds && s->forward) {
                /* Range-completion retirement is a delivery outcome, not stream
                 * maintenance — hold it while paused (Forward=0); resume re-runs
                 * the scan and retires then. Like every terminal it waits
                 * behind the notices still owed above, so a stream of the
                 * range is closed as its upstream ended (a reset stays a
                 * reset) before the subscription ends. The wire status is
                 * PUBLISH_DONE SUBSCRIPTION_ENDED: the filter range is
                 * exhausted, whether or not the source has also finished. A
                 * sub whose range end holds is never retired here: it stays
                 * established with nothing left to schedule, and only the
                 * subscriber or the track's end (below) finishes it. */
                if (!intent_space(c, 1)) {
                    ready_mark(c, bslot);   /* retire on a later scan */
                } else {
                    moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                    it->binding_cookie = b->cookie;
                    it->cookie = s->cookie;
                    it->sub = sub_handle(c, i);
                    it->error_code = R_DONE_SUB_ENDED;
                    it->pd = core_local_done(MOQR_PD_SUBSCRIPTION_ENDED);
                    sub_retire(c, i, now_us);
                }
            } else if (s->src_done) {
                /* The source finished and everything admitted for this sub
                 * -- records, seals, skips -- has gone out: send the
                 * forwarded terminal, after the data it closes. */
                if (!intent_space(c, 1)) {
                    ready_mark(c, bslot);   /* retire on a later scan */
                } else {
                    moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                    it->binding_cookie = b->cookie;
                    it->cookie = s->cookie;
                    it->sub = sub_handle(c, i);
                    it->pd = s->src_done_pd;
                    sub_retire(c, i, now_us);
                }
            } else if (s->forward && sub_eot_complete(c, s, t->log) &&
                       intent_space(c, 1)) {
                /* The track ENDED and every in-range position is delivered
                 * or authoritatively closed: the subscription is finished.
                 * Same retirement as range completion — the existing
                 * SUB_DONE intent (wire PUBLISH_DONE), nothing new. Ordered
                 * after the skip/seal branches so every owed notice reaches
                 * the peer first. The wire status is PUBLISH_DONE
                 * TRACK_ENDED: the track itself is done, not the filter. */
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = b->cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, i, now_us);
            }
            i = next;
            continue;
        }
        if (s->pending_skip) {
            /* The scan's own gpos_gc just detected the jump: the candidate
             * it returned is post-jump and must wait behind the watermark. */
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
            }
            i = next;
            continue;
        }
        if (seal.have) {
            /* Notice precedes record work for THIS sub: contribute the seal
             * as its candidate and hold its record for a later pass. */
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_SEAL;
                ntc_seal = seal;
            }
            i = next;
            continue;
        }
        if (!s->forward && cand.view.obj_state != MOQR_OBJ_ABANDONED) {
            /* Forward=0: ordinary object transmission is suppressed for THIS
             * sub — its record is not selected and range-completion retirement
             * is held (above). Its maintenance notices (SEAL / EVICT_WATERMARK)
             * and the evict-reset sweep already ran unconditionally, so a
             * required downstream close/reset is never deferred to resume. An
             * ABANDONED candidate is a begun-downstream reset (sub_best_candidate
             * already dropped never-begun abandons), which is stream
             * maintenance too — it stays eligible while paused. Siblings are
             * unaffected; resume re-arms and re-derives the record from the
             * unadvanced cursor. Once its source has finished, a paused sub
             * asked for nothing more: with its maintenance notices done (they
             * were taken above), it retires instead of holding the drain. */
            if (s->src_done) {
                if (!intent_space(c, 1)) {
                    ready_mark(c, bslot);   /* retire on a later scan */
                } else {
                    moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                    it->binding_cookie = b->cookie;
                    it->cookie = s->cookie;
                    it->sub = sub_handle(c, i);
                    it->pd = s->src_done_pd;
                    sub_retire(c, i, now_us);
                }
            }
            i = next;
            continue;
        }
        /* Cross-subscription ranking: subscriber priority first (spec
         * rule 1), then the same record ordering with ascending groups
         * (cross-request order past the priorities is implementation-
         * dependent; ties resolve by sub slot for determinism, so the
         * selected best is independent of iteration order). */
        bool better;
        if (!have) {
            better = true;
        } else if (s->subscriber_priority !=
                   c->subs[best_slot].subscriber_priority) {
            better = s->subscriber_priority <
                     c->subs[best_slot].subscriber_priority;
        } else if (cand.view.publisher_priority !=
                   best.view.publisher_priority) {
            better = cand.view.publisher_priority <
                     best.view.publisher_priority;
        } else if (cand.view.group_id != best.view.group_id) {
            better = cand.view.group_id < best.view.group_id;
        } else if (cand.view.arrival_seq != best.view.arrival_seq) {
            better = cand.view.arrival_seq < best.view.arrival_seq;
        } else {
            better = i < best_slot;
        }
        if (better) {
            have = true;
            best_slot = i;
            best = cand;
        }
        i = next;
    }
    /* Cross-subscription pick between the best notice and the best record:
     * subscriber priority first; a notice outranks a record at EQUAL
     * priority (payload-free, unblocks a FIN/close) but never preempts a
     * higher-priority sibling's record. Deterministic throughout: the notice
     * winner was chosen by (priority, sub slot) and a SEAL already names its
     * lowest (group, subgroup). */
    if (ntc_sub != R_SUB_NIL && have &&
        c->subs[best_slot].subscriber_priority <
            c->subs[ntc_sub].subscriber_priority) {
        ntc_sub = R_SUB_NIL;   /* the record wins; the notice waits its turn */
    }
    if (ntc_sub != R_SUB_NIL) {
        r_sub_t *ns = &c->subs[ntc_sub];
        b->out_sub = ntc_sub;
        b->out_group =
            (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) ? ntc_seal.group : 0;
        b->out_list =
            (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) ? ntc_seal.list : 0;
        b->out_chunked = false;
        b->out_abandoned = false;
        b->out_evict_reset = false;
        b->out_chunk_base = 0;
        b->out_notice = ntc_kind;
        b->pin_payload = NULL;
        b->pin_properties = NULL;
        b->out_active = true;
        memset(out, 0, sizeof(*out));
        out->sub = sub_handle(c, ntc_sub);
        out->sub_cookie = ns->cookie;
        out->notice = ntc_kind;
        out->oldest_group = UINT64_MAX;
        if (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) {
            out->rec.group_id = ntc_seal.group;
            out->rec.subgroup_id = ntc_seal.subgroup_id;
            /* subgroup_end is the CLEAN-finish advisory; a reset-flavored
             * terminal is abnormal and must never read as one. */
            out->subgroup_end = !ntc_seal.reset;
            out->seal_reset = ntc_seal.reset;
            out->seal_reset_desc = ntc_seal.reset_desc;
        } else {
            moqr_log_stats_t ls;
            moqr_log_get_stats(c->tracks[ns->track].log, &ls);
            out->oldest_group = ls.oldest_group_id;
            /* The eviction jump surfaces in the trace ring at notice time
             * (once per notice — a held re-peek replays out_view without
             * re-entering this path). Layout: detail=0 (gpos eviction skip),
             * e0=binding slot, e1=sub slot, e2=oldest retained group. */
            r_trace(c, MOQR_TRACE_CURSOR_SKIP, 0, bslot, ntc_sub,
                    ls.oldest_group_id, 0);
        }
        b->out_view = *out;
        return MOQR_OK;
    }
    if (!have) {
        return MOQR_DONE;
    }

    r_sub_t *ws = &c->subs[best_slot];
    b->out_sub = best_slot;
    b->out_group = best.view.group_id;
    b->out_list = best.list;
    b->out_chunked = false;
    b->out_abandoned = false;
    b->out_chunk_base = 0;
    /* Pin the outstanding delivery so its view stays valid across same-track
     * ingest/eviction until delivery_done (log views alone survive only until
     * the next mutating log call). Three shapes: an ABANDONED-begun head (no
     * chunks — the bind resets downstream); a chunked record (pin the
     * undelivered batch [emitted, chunk_count)); or a whole-object record (pin
     * the single payload). Properties are pinned for chunked/whole. */
    if (best.view.obj_state == MOQR_OBJ_ABANDONED) {
        b->out_abandoned = true;
        b->pin_payload = NULL;
        b->pin_properties = NULL;
    } else if (best.view.chunk_count > 0) {
        b->out_chunked = true;
        b->out_chunk_base = best.emitted;
        if (binding_pin_chunks(c, b, c->tracks[ws->track].log, &best.view,
                               best.emitted) != MOQR_OK) {
            /* Cannot pin (OOM growing the reusable array): fail this delivery
             * closed and retry next pump rather than hand out a truncatable
             * object. Nothing was mutated; the record stays the head. */
            b->out_chunked = false;
            return MOQR_ERR_CAPACITY;
        }
        b->pin_payload = NULL;
        b->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                                 best.view.properties);
    } else {
        b->pin_payload = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                              best.view.payload);
        b->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                                 best.view.properties);
    }
    b->out_active = true;
    b->out_subgroup_properties = best.subgroup_properties;
    b->out_follows_gap = best.follows_gap;
    b->out_gap_reset = best.gap_reset;
    out->sub = sub_handle(c, best_slot);
    out->sub_cookie = ws->cookie;
    out->rec = best.view;
    out->evicted_reset = false;   /* only the evict-reset sweep sets this */
    out->skipped = false;         /* always: superseded by the notice     */
    out->notice = MOQR_DELIVERY_NOTICE_NONE;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    /* Eviction jumps are no longer reported on record deliveries: an
     * unacknowledged watermark surfaces as an EVICT_WATERMARK notice and
     * BLOCKS this sub's record selection until acked, so a record reaching
     * here has no pending jump by construction. */
    out->oldest_group = UINT64_MAX;
    /* Flag the last record of a SEALED subgroup (datagrams have no subgroup:
     * best.list is a datagram pos-slot there, not a subgroup slot, so guard
     * on it). ADVISORY on records: the downstream close rides the SEAL
     * notice that becomes eligible once this record's DELIVERED ack advances
     * the cursor past it — one durable, retryable close path, so a close
     * that would WOULD_BLOCK after a delivered record can never be swallowed
     * with the delivery's acknowledgement. */
    out->subgroup_end =
        !best.view.datagram_pref &&
        moqr_log_subgroup_is_final(c->tracks[ws->track].log,
                                   best.view.group_id, best.list,
                                   best.view.object_id);
    /* Held batches replay this view; all record shapes retain their scalar
     * identity here for intentional-discard tracing at confirmation. */
    b->out_view = *out;
    return MOQR_OK;
}


moqr_result_t
moqr_core_next_delivery(moqr_core_t *c, moqr_binding_t bh, uint64_t now_us,
                        moqr_delivery_t *out)
{
    return core_next_delivery(c, bh, now_us, out, NULL, NULL);
}


moqr_result_t
moqr_core_internal_next_eligible(moqr_core_t *c, moqr_binding_t bh,
                                 uint64_t now_us, moqr_delivery_t *out,
                                 moqr_core_delivery_eligible_fn eligible,
                                 void *ctx)
{
    if (eligible == NULL) return MOQR_ERR_INVAL;
    return core_next_delivery(c, bh, now_us, out, eligible, ctx);
}


moqr_result_t
moqr_core_internal_delivery_flags(const moqr_core_t *c, moqr_binding_t bh,
                                  bool *subgroup_properties, bool *follows_gap,
                                  moqr_reset_desc_t *gap_reset, uint32_t *list)
{
    if (c == NULL || subgroup_properties == NULL || follows_gap == NULL ||
        gap_reset == NULL || list == NULL) {
        return MOQR_ERR_INVAL;
    }
    *subgroup_properties = false;
    *follows_gap = false;
    *gap_reset = moqr_reset_desc_none();
    *list = 0;
    r_binding_t *b = binding_resolve((moqr_core_t *)(uintptr_t)c, bh, NULL);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->out_active || b->out_notice != MOQR_DELIVERY_NOTICE_NONE) {
        return MOQR_ERR_WRONG_STATE;
    }
    *subgroup_properties = b->out_subgroup_properties;
    *follows_gap = b->out_follows_gap;
    *gap_reset = b->out_gap_reset;
    *list = b->out_list;
    return MOQR_OK;
}


moqr_result_t
moqr_core_delivery_done(moqr_core_t *c, moqr_binding_t bh,
                        moqr_delivery_outcome_t outcome, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->out_active) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (outcome != MOQR_DELIVERY_DELIVERED &&
        outcome != MOQR_DELIVERY_WOULD_BLOCK &&
        outcome != MOQR_DELIVERY_STREAM_ERROR &&
        outcome != MOQR_DELIVERY_STALLED &&
        outcome != MOQR_DELIVERY_ABANDONED &&
        outcome != MOQR_DELIVERY_REFUSED_UNBEGUN &&
        outcome != MOQR_DELIVERY_DISCARDED) {
        return MOQR_ERR_INVAL;
    }
    if (b->out_notice != MOQR_DELIVERY_NOTICE_NONE &&
        (outcome == MOQR_DELIVERY_STALLED ||
         outcome == MOQR_DELIVERY_ABANDONED ||
         outcome == MOQR_DELIVERY_REFUSED_UNBEGUN ||
         outcome == MOQR_DELIVERY_DISCARDED)) {
        return MOQR_ERR_INVAL;   /* a notice has no stall/abandon/pre-begin
                                  * refusal shape */
    }
    if (outcome == MOQR_DELIVERY_DISCARDED && b->out_evict_reset) {
        return MOQR_ERR_INVAL;   /* names a group, not an exact record */
    }
    if (outcome == MOQR_DELIVERY_REFUSED_UNBEGUN && b->out_abandoned) {
        return MOQR_ERR_INVAL;   /* a reset has no pre-begin refusal site */
    }
    if (outcome == MOQR_DELIVERY_REFUSED_UNBEGUN && b->out_chunked &&
        b->out_chunk_base != 0) {
        /* The pinned batch resumes at a nonzero chunk index: the object
         * provably BEGAN downstream in an earlier batch, so the caller's
         * pre-begin claim is false — releasing would re-derive from the
         * emitted watermark and could duplicate. Fail closed. */
        return MOQR_ERR_INVAL;
    }
    /* Backpressure HOLDS the outstanding delivery (a chunked batch, an
     * abandoned-reset, or a recordless notice): keep out_active + pins so the
     * next next_delivery replays it verbatim and the bind resumes its chunk
     * cursor / retries the reset / re-attempts the notice. Whole-object
     * WOULD_BLOCK drops and re-derives (no sub-object position to preserve) —
     * and so does a REFUSED_UNBEGUN chunked record: nothing downstream began,
     * so there is no cursor to preserve, and holding it would starve the SEAL
     * notices whose closes free the slot it needs (the release lets the next
     * selection pick those notices first). Only while the sub is live; a
     * retired sub falls to cleanup. */
    if (outcome == MOQR_DELIVERY_WOULD_BLOCK &&
        (b->out_chunked || b->out_abandoned ||
         b->out_notice != MOQR_DELIVERY_NOTICE_NONE) &&
        (c->subs[b->out_sub].gen & 1u) != 0) {
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        return MOQR_OK;
    }
    uint8_t notice = b->out_notice;
    b->out_active = false;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    r_sub_t *s = &c->subs[b->out_sub];
    if ((s->gen & 1u) == 0) {
        binding_unpin(b);
        return MOQR_OK;   /* sub retired while outstanding */
    }
    /* This sub's positions entry for the delivered group (NULL if the group was
     * evicted meanwhile — already reclaimed; the skip reports on next delivery). */
    r_gpos_t *pe = NULL;
    {
        r_track_t *t = &c->tracks[s->track];
        if ((t->gen & 1u) != 0 && t->gen == s->track_gen_slot &&
            s->gpos != NULL) {
            uint32_t slot = gpos_hash_find(s, b->out_group);   /* O(1) */
            if (slot != R_GPOS_NIL) {
                pe = gpos_at(s, slot);
            }
        }
    }
    if (notice != MOQR_DELIVERY_NOTICE_NONE) {
        /* Recordless notice: WOULD_BLOCK was held above (live sub) and a
         * dead sub returned earlier, so only the acknowledgement and the
         * terminal reach here. The ack is what clears the pending core
         * state — never the peek. */
        if (outcome == MOQR_DELIVERY_DELIVERED) {
            if (notice == MOQR_DELIVERY_NOTICE_SEAL) {
                if (pe != NULL) {
                    gpos_seal_ack(s, pe, b->out_list);
                    /* The owed lifetime's terminal is delivered. */
                    gpos_renew_set(s, pe, b->out_list, false);
                    gpos_owed(s, pe)[b->out_list] = 0;
                }
                r_trace(c, MOQR_TRACE_DELIVERY, 1, bslot, b->out_sub,
                        b->out_group, 0);
            } else {
                s->pending_skip = false;
                r_trace(c, MOQR_TRACE_DELIVERY, 2, bslot, b->out_sub, 0, 0);
            }
        } else {
            sub_retire(c, b->out_sub, now_us);   /* STREAM_ERROR */
        }
        binding_unpin(b);
        return MOQR_OK;
    }
    switch (outcome) {
    case MOQR_DELIVERY_DISCARDED:
    case MOQR_DELIVERY_DELIVERED:
        if (pe != NULL) {
            if (gpos_emit(s, pe)[b->out_list] > 0) {
                sub_begun_sub(c, s);               /* a begun head completed   */
            }
            pe->idx[b->out_list]++;                /* advance to next record  */
            gpos_emit(s, pe)[b->out_list] = 0;     /* reset live-edge cursor  */
            gpos_gap_set(s, pe, b->out_list, false);
            gpos_renew_set(s, pe, b->out_list, false);
            gpos_owed(s, pe)[b->out_list] = 0;
            /* A record that does not continue the earlier stream opens a new
             * downstream stream, which owes its own terminal: a seal this sub
             * acknowledged belonged to the stream before it. */
            if ((b->out_follows_gap || outcome == MOQR_DELIVERY_DISCARDED) &&
                b->out_list < s->gpos_lists - 1) {
                gpos_acked(s, pe)[b->out_list / 32u] &= ~(1u << (b->out_list % 32u));
            }
        }
        if (outcome == MOQR_DELIVERY_DELIVERED) {
            c->delivered_total++;
            r_trace(c, MOQR_TRACE_DELIVERY, 0, bslot, b->out_sub, b->out_group, 0);
        } else {
            r_trace(c, MOQR_TRACE_CURSOR_SKIP, 2, bslot, b->out_sub,
                    b->out_group, b->out_view.rec.object_id);
        }
        break;
    case MOQR_DELIVERY_STALLED:
        /* Live edge drained but not COMPLETE: record how many chunks are now
         * scheduled (so the head is not re-selected until it grows past this),
         * but do NOT advance the record and do NOT count a delivery. STALLED
         * implies begin_object shipped the header, so mark this position begun
         * even when zero chunks are exposed yet (its object still needs a RESET
         * if abandoned/evicted). */
        if (pe != NULL) {
            if (gpos_emit(s, pe)[b->out_list] == 0) {
                sub_begun_add(c, s);   /* this position is now begun downstream */
            }
            gpos_emit(s, pe)[b->out_list] =
                GPOS_BEGUN_BIT | (uint32_t)b->out_view.rec.chunk_count;
        }
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_ABANDONED:
        /* Bind reset the downstream subgroup(s); advance past the abandoned state
         * without counting a delivery. An evict-reset covered the WHOLE evicted
         * group, so reclaim the entire position slot (a plain skip); a normal
         * abandon advanced just the one list past its tombstone. */
        if (pe != NULL) {
            if (b->out_evict_reset) {
                for (uint32_t li = 0; li < s->gpos_lists; li++) {
                    if (gpos_emit(s, pe)[li] > 0) {
                        sub_begun_sub(c, s);   /* clearing this begun position */
                    }
                }
                pe->occupied = false;
                gpos_body_clear(s, pe);
                s->pending_skip = true;
                ready_mark(c, s->binding);   /* the skip must be reported */
                gpos_hash_rebuild(s);   /* drop the reclaimed entry from the index */
            } else {
                if (gpos_emit(s, pe)[b->out_list] > 0) {
                    sub_begun_sub(c, s);   /* a begun head was abandoned */
                }
                pe->idx[b->out_list]++;
                gpos_emit(s, pe)[b->out_list] = 0;
            }
        }
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_WOULD_BLOCK:
    case MOQR_DELIVERY_REFUSED_UNBEGUN:
        /* Released without advancing: re-derived by a later selection (for
         * REFUSED_UNBEGUN nothing downstream began, so re-derivation cannot
         * duplicate). */
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_STREAM_ERROR:
    default:
        sub_retire(c, b->out_sub, now_us);
        break;
    }
    binding_unpin(b);
    return MOQR_OK;
}


moqr_result_t
moqr_core_internal_discard_subgroup(moqr_core_t *c, moqr_binding_t bh,
                                    moqr_sub_t sh, uint64_t group,
                                    uint64_t subgroup, uint32_t list,
                                    uint32_t budget, uint64_t now_us,
                                    bool *retired, bool *more)
{
    if (c == NULL || retired == NULL || more == NULL || budget == 0) {
        return MOQR_ERR_INVAL;
    }
    *retired = false;
    *more = false;
    uint32_t bslot = 0, sslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    r_sub_t *s = sub_resolve(c, sh, &sslot);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    if (s == NULL) {
        *retired = true;   /* this exact subscription owner is gone */
        return MOQR_ERR_STALE_HANDLE;
    }
    if (s->binding != bslot || s->binding_gen != b->gen || s->gpos_lists == 0 ||
        list >= s->gpos_lists - 1u) return MOQR_ERR_INVAL;
    r_track_t *t = &c->tracks[s->track];
    if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot) {
        return MOQR_ERR_STALE_HANDLE;
    }
    moqr_log_t *log = t->log;
    moqr_record_view_t probe;
    *retired = moqr_log_read_rec(log, group, MOQR_LOG_LIST_DATAGRAM, 0,
                                 &probe) == MOQR_ERR_TOO_OLD;
    /* Validate the captured position while the group is retained. Group
     * eviction never reuses its ID, so its old list index remains sufficient
     * to clear just this position's ledger after the log releases the list. */
    bool retained = false;
    for (uint32_t gi = 0; gi < log->group_count; gi++) {
        const moqr_group_t *g = &log->groups[gi];
        if (g->group_id != group) continue;
        if (list >= g->sg_count || g->sgs[list].subgroup_id != subgroup) {
            return MOQR_ERR_INVAL;
        }
        retained = true;
        break;
    }
    if (!retained && !*retired) return MOQR_ERR_WRONG_STATE;
    uint32_t used = 0;
    bool changed = false;
    if (b->out_active && b->out_sub == sslot &&
        b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_evict_reset &&
        b->out_group == group && b->out_list == list) {
        moqr_result_t rc = moqr_core_delivery_done(c, bh,
                                                   MOQR_DELIVERY_DISCARDED,
                                                   now_us);
        if (rc != MOQR_OK) return rc;
        used++;
        changed = true;
    }
    uint32_t pos = gpos_hash_find(s, group);
    r_gpos_t *pe = pos == R_GPOS_NIL ? NULL : gpos_at(s, pos);
    if (pe == NULL && !*retired) pe = gpos_entry(c, s, log, group);
    if (pe == NULL && !*retired) return MOQR_ERR_CAPACITY;
    if (pe != NULL) {
        if (gpos_emit(s, pe)[list] != 0) {
            sub_begun_sub(c, s);
            changed = true;
        }
        gpos_emit(s, pe)[list] = 0;
        gpos_gap_set(s, pe, list, false);
        gpos_renew_set(s, pe, list, false);
        gpos_owed(s, pe)[list] = 0;
        if (!*retired) {
            while (used < budget &&
                   moqr_log_read_rec(log, group, list, pe->idx[list],
                                      &probe) == MOQR_OK) {
                pe->idx[list]++;
                gpos_acked(s, pe)[list / 32u] &= ~(1u << (list % 32u));
                r_trace(c, MOQR_TRACE_CURSOR_SKIP, 2, bslot, sslot,
                        group, probe.object_id);
                used++;
                changed = true;
            }
            *more = moqr_log_read_rec(log, group, list, pe->idx[list],
                                       &probe) == MOQR_OK;
        }
        /* A held synthetic reset can lose its last begun position here.
         * Preserve it when any sibling of the evicted group still needs it. */
        if (b->out_active && b->out_evict_reset && b->out_sub == sslot &&
            b->out_group == group) {
            bool begun = false;
            for (uint32_t li = 0; li < s->gpos_lists; li++) {
                begun = begun || gpos_emit(s, pe)[li] != 0;
            }
            if (!begun) binding_drop_held(b);
        }
        if (*retired) {
            bool begun = false;
            for (uint32_t li = 0; li < s->gpos_lists; li++) {
                begun = begun || gpos_emit(s, pe)[li] != 0;
            }
            if (!begun) {
                pe->occupied = false;
                gpos_body_clear(s, pe);
                gpos_hash_rebuild(s);
                s->pending_skip = true;
            }
        }
    }
    if (changed || *retired) {
        ready_mark(c, bslot);   /* newly exposed notices still need ack */
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_delivery_chunk(moqr_core_t *c, moqr_binding_t bh, uint32_t idx,
                         const moq_rcbuf_t **out_buf, uint64_t *out_len)
{
    if (c == NULL || out_buf == NULL || out_len == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->out_active || !b->out_chunked) {
        return MOQR_ERR_WRONG_STATE;   /* no chunked delivery outstanding */
    }
    /* idx is ABSOLUTE (the bind's chunk cursor); the pinned array holds only the
     * current batch [out_chunk_base, out_chunk_base + pin_chunk_count). */
    if (idx < b->out_chunk_base ||
        idx - b->out_chunk_base >= b->pin_chunk_count) {
        return MOQR_DONE;              /* outside the pinned batch */
    }
    uint32_t rel = idx - b->out_chunk_base;
    *out_buf = b->pin_chunks[rel].buf;
    *out_len = b->pin_chunks[rel].len;
    return MOQR_OK;
}

#ifdef MOQR_CORE_TESTING


/* Test-only: read BOTH halves of the begun ledger for one live subscription —
 * its own begun_count and the begun_subs tally on its owning binding. They are
 * two records of the same fact, and a leak in either is invisible from outside:
 * no public gauge reports them, and by the time an eviction watermark passes a
 * stale position its subscription's cursor is long past it, so no delivery
 * differs. Pure observation: no scheduling, lifetime or accounting effect.
 *
 * Fail-closed — an unknown core, a stale subscription generation or a dead
 * owning binding returns false with both outputs untouched. Never compiled
 * into a production core. */
bool
moqr_core_debug_begun_ledger(const moqr_core_t *c, moqr_sub_t sh,
                             uint32_t *out_sub_begun,
                             uint32_t *out_binding_begun_subs)
{
    if (c == NULL || out_sub_begun == NULL ||
        out_binding_begun_subs == NULL) {
        return false;
    }
    uint32_t slot = 0;
    const r_sub_t *s = sub_resolve((moqr_core_t *)(void *)c, sh, &slot);

    if (s == NULL || s->binding >= c->max_bindings) {
        return false;
    }
    const r_binding_t *b = &c->bindings[s->binding];

    if (b->gen != s->binding_gen) {
        return false;   /* the owning binding is gone or reused */
    }
    *out_sub_begun = s->begun_count;
    *out_binding_begun_subs = b->begun_subs;
    return true;
}

#endif /* MOQR_CORE_TESTING */


uint32_t
moqr_core_drain_ready(moqr_core_t *c, uint64_t *cookies, uint32_t cap)
{
    if (c == NULL || cookies == NULL) {
        return 0;
    }
    uint32_t words = ready_word_count(c->max_bindings);
    uint32_t out = 0;
    for (uint32_t wi = 0; wi < words && out < cap && c->ready_count > 0;
         wi++) {
        while (c->ready_words[wi] != 0 && out < cap) {
            /* Lowest set bit first: ascending slot order within and across
             * words. */
            uint64_t w = c->ready_words[wi];
            uint64_t low = w & (~w + 1u);
            uint32_t bi = 0;
            for (uint32_t sh = 32; sh != 0; sh >>= 1) {
                if ((low >> sh) != 0) {
                    bi += sh;
                    low >>= sh;
                }
            }
            cookies[out++] = c->bindings[((uint64_t)wi << 6) + bi].cookie;
            c->ready_words[wi] &= ~(1ull << bi);
            c->ready_count--;
        }
    }
    return out;
}
