#include "relay_internal.h"
#include "fetch_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "subscribe_internal.h"
#include "tracks_internal.h"

void fetch_unpin(moqr_core_t *c, r_fetch_t *f);


/* -- Retained-hit FETCH (relay.h) --------------------------------------------------- */

static moqr_fetch_t
fetch_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_fetch_t){ r_pack(c, MOQR_HANDLE_POOL_FETCH,
                                  c->fetches[slot].gen, slot) };
}


static r_fetch_t *
fetch_resolve(moqr_core_t *c, moqr_fetch_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_FETCH ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_fetches ||
        c->fetches[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->fetches[slot];
}


bool
moqr_fetch_is_valid(moqr_fetch_t h)
{
    return moq_handle_pool_tag(h._opaque) == MOQR_HANDLE_POOL_FETCH &&
           moq_handle_session_tag(h._opaque) != 0 &&
           (moq_handle_generation(h._opaque) & 1u) != 0;
}


bool
moqr_fetch_eq(moqr_fetch_t a, moqr_fetch_t b)
{
    return a._opaque == b._opaque;
}


/* The sizeof of the layout through `cookie`: what a caller built before
 * report_gaps was appended allocates and passes. */
#define MOQR_FETCH_REQ_SIZE_THROUGH_COOKIE                                    \
    ((offsetof(moqr_fetch_req_t, cookie) +                                    \
      sizeof(((moqr_fetch_req_t *)0)->cookie) +                               \
      (_Alignof(moqr_fetch_req_t) - 1)) &                                     \
     ~(_Alignof(moqr_fetch_req_t) - 1))


/* Parenthesized: defines the symbol, not the header's sized macro. */
void
(moqr_fetch_req_init)(moqr_fetch_req_t *req)
{
    if (req == NULL) {
        return;
    }
    memset(req, 0, MOQR_FETCH_REQ_SIZE_THROUGH_COOKIE);
    req->struct_size = (uint32_t)MOQR_FETCH_REQ_SIZE_THROUGH_COOKIE;
}


void
moqr_fetch_req_init_sized(moqr_fetch_req_t *req, size_t req_size)
{
    if (req == NULL || req_size < sizeof(req->struct_size)) {
        return;   /* too small to even stamp */
    }
    size_t n = req_size < sizeof(*req) ? req_size : sizeof(*req);
    memset(req, 0, n);
    req->struct_size = (uint32_t)n;
}


uint32_t
fetch_slot_find(moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_fetches; i++) {
        if (r_gen_openable(c->fetches[i].gen)) {
            return i;
        }
    }
    return UINT32_MAX;
}


void
fetch_unpin(moqr_core_t *c, r_fetch_t *f)
{
    if (!f->out_active) {
        return;
    }
    moq_rcbuf_decref(f->pin_payload);
    moq_rcbuf_decref(f->pin_properties);
    f->pin_payload = NULL;
    f->pin_properties = NULL;
    if (c->fetch_pin_used >= f->pin_bytes) {
        c->fetch_pin_used -= f->pin_bytes;
    } else {
        c->fetch_pin_used = 0;   /* defensive; accounting is symmetric */
    }
    f->pin_bytes = 0;
    f->out_active = false;
}


/* Find the smallest NORMAL object at Location >= (cur_group, cur_object) within
 * the fetch's range. Fills *out with a borrowed view and returns MOQR_OK;
 * MOQR_DONE when the range is exhausted. Read-only — every list of the current
 * group is scanned (subgroup lists are object-id ascending; the datagram list
 * carries no ordering, so it is scanned in full), yielding a k-way merge by
 * object_id; the first group (ascending) with an in-range NORMAL object wins. */
static moqr_result_t
fetch_scan_next(const moqr_log_t *log, const r_fetch_t *f,
                const r_track_t *t, uint64_t now_us,
                moqr_record_view_t *out)
{
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < f->cur_group) {
            continue;
        }
        if (g > f->end_group) {
            break;   /* groups ascend; nothing further is in range */
        }
        uint64_t floor_obj = (g == f->cur_group) ? f->cur_object : 0;
        uint64_t ceil_obj =
            (g == f->end_group && !f->end_whole) ? f->end_object : UINT64_MAX;
        if (floor_obj > ceil_obj) {
            continue;   /* empty object window in this group */
        }
        bool have = false;
        moqr_record_view_t best;
        memset(&best, 0, sizeof(best));
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t slot = 0; slot <= lists; slot++) {
            uint32_t list_ref =
                (slot == lists) ? MOQR_LOG_LIST_DATAGRAM : slot;
            for (uint32_t idx = 0;; idx++) {
                moqr_record_view_t v;
                moqr_result_t rc =
                    moqr_log_read_rec(log, g, list_ref, idx, &v);
                if (rc != MOQR_OK) {
                    break;   /* list end (DONE) or evicted (TOO_OLD) */
                }
                if (v.status != MOQR_OBJ_NORMAL) {
                    continue;   /* status objects never become fetch items */
                }
                if (v.obj_state != MOQR_OBJ_COMPLETE) {
                    continue;   /* OPEN / ABANDONED are not fetchable (deferred).
                                 * COMPLETE records pass — whole-object (payload in
                                 * rec.payload) and chunked (chunk_count > 0, payload
                                 * coalesced in fetch_peek) alike. */
                }
                if (v.object_id < floor_obj || v.object_id > ceil_obj) {
                    continue;
                }
                if (track_rec_expired(t, &v, now_us)) {
                    continue;   /* expired from cache: its state is unknown */
                }
                if (!have || v.object_id < best.object_id) {
                    best = v;
                    have = true;
                }
            }
        }
        if (have) {
            *out = best;
            return MOQR_OK;
        }
        /* group had no in-range NORMAL object (holes / status-only): next group */
    }
    return MOQR_DONE;
}


/* Whether `bslot` at generation `gen` is a live shard pseudo-binding: the
 * stand-in for another shard of the same relay. */
static bool
binding_is_shard(const moqr_core_t *c, uint32_t bslot, uint32_t gen)
{
    return bslot < c->max_bindings && c->bindings[bslot].gen == gen &&
           (gen & 1u) != 0 &&
           c->bindings[bslot].cookie >= MOQR_SHARD_COOKIE_BASE;
}


/* Whether the namespace's longest announced prefix was announced here by
 * another shard (a mirror): its tracks are that shard's. */
static bool
ns_held_by_shard(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t n = trie_longest_announce(c, ns);
    return n != UINT32_MAX &&
           binding_is_shard(c, c->nodes[n].ann_binding,
                            c->nodes[n].ann_binding_gen);
}


/* Whether this core's Largest Object for the track is the relay's. It is when
 * the track's source is a connection of this core's own -- the publisher,
 * an upstream subscription, or none left and the log its own -- and when
 * another shard owns the track and feeds it here: from that shard's
 * acceptance, which carries its Largest, every later Object follows, so the
 * Largest here keeps up with the owner's. A copy that shard is not feeding
 * -- not yet accepted, or no longer fed -- can lag it. */
static bool
track_largest_is_relays(const moqr_core_t *c, const r_track_t *t)
{
    if (t->has_upstream_binding &&
        binding_is_shard(c, t->up_binding, t->up_binding_gen)) {
        return t->state == R_TRACK_ACTIVE;
    }
    if (!t->has_upstream_binding && t->src_ann_node != UINT32_MAX &&
        binding_is_shard(c, t->src_ann_binding, t->src_ann_binding_gen)) {
        return false;
    }
    return true;
}


/* The smallest Location at or after the cursor and within the fetch's range
 * holding an Object still arriving: a NORMAL record whose first bytes are in
 * the log and that is not yet complete. Read-only, the same walk as
 * fetch_scan_next. */
static bool
fetch_scan_pending(const moqr_log_t *log, const r_fetch_t *f, uint64_t *pg,
                   uint64_t *po)
{
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < f->cur_group) {
            continue;
        }
        if (g > f->end_group) {
            break;
        }
        uint64_t floor_obj = (g == f->cur_group) ? f->cur_object : 0;
        uint64_t ceil_obj =
            (g == f->end_group && !f->end_whole) ? f->end_object : UINT64_MAX;
        if (floor_obj > ceil_obj) {
            continue;
        }
        bool have = false;
        uint64_t best = 0;
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t slot = 0; slot <= lists; slot++) {
            uint32_t list_ref =
                (slot == lists) ? MOQR_LOG_LIST_DATAGRAM : slot;
            for (uint32_t idx = 0;; idx++) {
                moqr_record_view_t v;
                if (moqr_log_read_rec(log, g, list_ref, idx, &v) != MOQR_OK) {
                    break;
                }
                if (v.status != MOQR_OBJ_NORMAL ||
                    v.obj_state != MOQR_OBJ_OPEN ||
                    v.object_id < floor_obj || v.object_id > ceil_obj) {
                    continue;
                }
                if (!have || v.object_id < best) {
                    best = v.object_id;
                    have = true;
                }
            }
        }
        if (have) {
            *pg = g;
            *po = best;
            return true;
        }
    }
    return false;
}


static int loc_cmp(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo);


/* The shard binding a track or namespace this core only mirrors routes to:
 * its cookie, or 0 when this core is the track's own. */
static uint64_t
track_remote_cookie(const moqr_core_t *c, const r_track_t *t)
{
    if (t->has_upstream_binding &&
        binding_is_shard(c, t->up_binding, t->up_binding_gen)) {
        return c->bindings[t->up_binding].cookie;
    }
    if (!t->has_upstream_binding && t->src_ann_node != UINT32_MAX &&
        binding_is_shard(c, t->src_ann_binding, t->src_ann_binding_gen)) {
        return c->bindings[t->src_ann_binding].cookie;
    }
    return 0;
}


static uint64_t
ns_remote_cookie(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t n = trie_longest_announce(c, ns);
    if (n == UINT32_MAX ||
        !binding_is_shard(c, c->nodes[n].ann_binding, c->nodes[n].ann_binding_gen)) {
        return 0;
    }
    return c->bindings[c->nodes[n].ann_binding].cookie;
}


/* Plan a standalone FETCH and, on ACCEPT, open its cursor slot. With
 * `retrieval` set this admits a retrieval owner instead (see
 * retrieve_internal.h): descending order is allowed, a range the log does not
 * hold is accepted for upstream retrieval, and a track another shard owns is
 * that shard's. */
static moqr_result_t
core_fetch_admit(moqr_core_t *c, moqr_binding_t bh, const moqr_fetch_req_t *req,
                 moqr_fetch_t *out, moqr_fetch_plan_t *out_plan,
                 moqr_core_retrieve_plan_t *retrieval)
{
    /* Prefix-size (ABI) contract: report_gaps is a trailing field, so a caller
     * whose struct ends before it is valid and reports no gaps. */
    if (c == NULL || out == NULL || out_plan == NULL || req == NULL ||
        req->struct_size < offsetof(moqr_fetch_req_t, report_gaps) ||
        !ftn_view_ok(req->ns, req->name)) {
        return MOQR_ERR_INVAL;
    }
    bool req_gaps = false;
    if (req->struct_size >=
        offsetof(moqr_fetch_req_t, report_gaps) + sizeof(req->report_gaps)) {
        req_gaps = req->report_gaps;
    }
    bool req_unknown = false;
    if (req_gaps &&
        req->struct_size >= offsetof(moqr_fetch_req_t, report_unknown) +
                                sizeof(req->report_unknown)) {
        req_unknown = req->report_unknown;
    }
#define FETCH_REQ_HAS(f) \
    (req->struct_size >= offsetof(moqr_fetch_req_t, f) + sizeof(req->f))
    const uint32_t form = FETCH_REQ_HAS(range_form) ? req->range_form : 0u;
    const uint64_t rel_start =
        FETCH_REQ_HAS(relative_start) ? req->relative_start : 0u;
    const bool req_pending =
        FETCH_REQ_HAS(report_pending) ? req->report_pending : false;
#undef FETCH_REQ_HAS
    if ((form & ~(MOQR_FETCH_START_RELATIVE | MOQR_FETCH_START_NEXT_OBJECT |
                  MOQR_FETCH_END_AT_LARGEST)) != 0 ||
        ((form & MOQR_FETCH_START_RELATIVE) != 0 &&
         (form & MOQR_FETCH_START_NEXT_OBJECT) != 0)) {
        return MOQR_ERR_INVAL;   /* unknown bits, or two starts */
    }
    *out = MOQR_FETCH_INVALID;
    memset(out_plan, 0, sizeof(*out_plan));
    if (retrieval != NULL) {
        memset(retrieval, 0, sizeof(*retrieval));
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Standalone + ascending only (DEFAULT == ascending). The binding
     * rejects joining/descending before this call; the core guards anyway.
     * A retrieval owner's view is ascending within one group at a time, so
     * it admits descending group order as well. */
    if (retrieval == NULL && req->group_order == MOQR_GROUP_ORDER_DESCENDING) {
        return MOQR_ERR_UNSUPPORTED;
    }
    if (retrieval != NULL && req->group_order != MOQR_GROUP_ORDER_DEFAULT &&
        req->group_order != MOQR_GROUP_ORDER_ASCENDING &&
        req->group_order != MOQR_GROUP_ORDER_DESCENDING) {
        return MOQR_ERR_UNSUPPORTED;
    }

    uint32_t tslot = (c->cluster_enabled ? cluster_track_lookup(c, req->ns, req->name, bslot, b->gen, NULL) : track_lookup(c, req->ns, req->name, NULL));
    uint32_t cluster_node = UINT32_MAX;
    if (c->cluster_enabled && tslot == UINT32_MAX) {
        cluster_node = cluster_select_node(c, req->ns, bslot, b->gen);
        if (cluster_node == UINT32_MAX) {
            out_plan->admit = MOQR_FETCH_REJECT;
            out_plan->error_code = R_ERR_DOES_NOT_EXIST;
            return MOQR_OK;
        }
    }

    if (tslot == UINT32_MAX) {
        out_plan->admit = MOQR_FETCH_REJECT;
        /* fetch-on-miss seam. A namespace another shard announced here names
         * a track that shard may hold: not this core's to deny. */
        out_plan->error_code = ns_held_by_shard(c, req->ns)
                                   ? R_ERR_INTERNAL
                                   : R_ERR_DOES_NOT_EXIST;
        if (retrieval != NULL) {
            retrieval->remote_cookie = ns_remote_cookie(c, req->ns);
        }
        return MOQR_OK;
    }
    r_track_t *t = &c->tracks[tslot];
    if (!cluster_track_eligible(c, t, bslot, b->gen)) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }

    if (retrieval != NULL && track_remote_cookie(c, t) != 0) {
        /* A copy another shard feeds here: that shard holds the track, its
         * Largest Object and its upstream. */
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INTERNAL;
        retrieval->remote_cookie = track_remote_cookie(c, t);
        return MOQR_OK;
    }
    if (!track_largest_is_relays(c, t)) {
        /* A copy another shard feeds, while it is not being fed: its Largest
         * can lag the owner's, so no range is resolved or bounded by it. */
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INTERNAL;
        return MOQR_OK;
    }
    if (!t->has_largest) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;   /* empty track */
        return MOQR_OK;
    }
    /* The parts of the range stated relative to the Largest Object resolve
     * against this core's Largest, here, so the range is planned against the
     * same Largest it was resolved with. A start that would lie past any
     * representable Location is past the Largest Object. */
    uint64_t req_sg = req->start_group, req_so = req->start_object;
    uint64_t req_eg = req->end_group, req_eo = req->end_object;
    {
        const uint64_t lg = t->largest_group, lo = t->largest_object;
        bool start_past = false;
        if ((form & MOQR_FETCH_START_RELATIVE) != 0) {
            req_so = 0;
            if (rel_start == 0) {
                start_past = lg == UINT64_MAX;
                req_sg = start_past ? lg : lg + 1u;
            } else {
                req_sg = rel_start - 1u > lg ? 0 : lg - (rel_start - 1u);
            }
        } else if ((form & MOQR_FETCH_START_NEXT_OBJECT) != 0) {
            start_past = lo == UINT64_MAX;
            req_sg = lg;
            req_so = start_past ? lo : lo + 1u;
        }
        if ((form & MOQR_FETCH_END_AT_LARGEST) != 0) {
            req_eg = lg;
            req_eo = lo == UINT64_MAX ? 0 : lo + 1u;   /* raw wire: last + 1 */
        }
        if (start_past) {
            out_plan->admit = MOQR_FETCH_REJECT;
            out_plan->error_code = R_ERR_INVALID_RANGE;
            return MOQR_OK;
        }
    }
    /* Start past the largest object -> INVALID_RANGE (draft §FETCH). */
    if (req_sg > t->largest_group ||
        (req_sg == t->largest_group &&
         req_so > t->largest_object)) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    /* Fully-evicted log: no oldest retained group to anchor a marker to, so a
     * Start below the horizon has no servable suffix -> reject. A retrieval
     * owner retrieves what the log does not hold instead, and its views
     * report those Locations as unknown rather than skip them. */
    if (ls.group_count == 0 && retrieval == NULL) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    /* Evicted prefix: Start is below the retention horizon. The fetch serves the
     * retained suffix led by one UNKNOWN marker -- but only if the range still
     * reaches retained data (checked once the effective End is known, below).
     * Start >= oldest is the normal fully-retained path. */
    bool evicted_prefix = retrieval == NULL && req_sg < ls.oldest_group_id;

    /* Decode the raw-wire End Location to an inclusive last object. */
    bool req_whole = (req_eo == 0);
    uint64_t req_g = req_eg;
    uint64_t req_o = req_whole ? 0 : req_eo - 1;
    /* Effective inclusive last = min(requested, track largest). */
    bool req_le_largest;
    if (req_g != t->largest_group) {
        req_le_largest = req_g < t->largest_group;
    } else if (req_whole) {
        req_le_largest = false;   /* whole of largest_group covers largest_o */
    } else {
        req_le_largest = req_o <= t->largest_object;
    }
    bool eff_whole = req_le_largest ? req_whole : false;
    uint64_t eff_g = req_le_largest ? req_g : t->largest_group;
    uint64_t eff_o = req_le_largest ? req_o : t->largest_object;
    /* End MUST be >= Start (draft): reject an empty/inverted range. */
    bool eff_lt_start;
    if (eff_g != req_sg) {
        eff_lt_start = eff_g < req_sg;
    } else if (eff_whole) {
        eff_lt_start = false;
    } else {
        eff_lt_start = eff_o < req_so;
    }
    if (eff_lt_start) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    /* An evicted-prefix fetch must reach retained data: if the effective End is
     * still below the oldest retained group, the whole range is evicted -> no
     * suffix to serve, so reject rather than emit a marker with no objects. */
    if (evicted_prefix && eff_g < ls.oldest_group_id) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }

    uint32_t fslot = fetch_slot_find(c);
    if (fslot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_FETCHES);
    }
    r_fetch_t *f = &c->fetches[fslot];
    uint32_t gen = r_gen_open(f->gen);
    memset(f, 0, sizeof(*f));
    f->gen = gen;
    f->binding = bslot;
    f->binding_gen = b->gen;
    f->cluster_requester_hop = b->peer_hop_id;
    f->cluster_requester_correlation = b->correlation_id;
    f->track = tslot;
    f->track_gen_slot = t->gen;
    t->fetch_count++;
    track_touch(c, tslot);
    f->cookie = req->cookie;
    f->start_group = req_sg;
    f->start_object = req_so;
    f->end_group = eff_g;
    f->end_object = eff_o;
    f->end_whole = eff_whole;
    f->group_order = req->group_order;
    f->subscriber_priority = req->subscriber_priority;
    f->report_gaps = req_gaps;
    f->report_unknown = req_unknown;
    f->report_pending = req_pending;
    if (retrieval != NULL) {
        f->retrieval_owner = true;
        f->report_gaps = true;
        f->report_unknown = true;
        f->report_pending = true;
        f->adm_end_group = eff_g;
        f->adm_end_object = eff_whole ? UINT64_MAX : eff_o;
        f->retrieval_track_gen = t->track_gen;
    }
    if (evicted_prefix) {
        /* Skip the evicted prefix: serve from the oldest retained group, led by
         * one UNKNOWN marker emitted immediately before it. */
        f->lead_marker = true;
        f->lead_marker_group = ls.oldest_group_id;
        f->cur_group = ls.oldest_group_id;
        f->cur_object = 0;
    } else {
        f->cur_group = req_sg;
        f->cur_object = req_so;
    }

    out_plan->admit = MOQR_FETCH_ACCEPT;
    /* FETCH_OK End Location, re-encoded raw-wire ("last + 1"; 0 = whole group). */
    out_plan->end_group = eff_g;
    out_plan->end_object = eff_whole ? 0 : eff_o + 1;
    /* End Of Track only when the track has ended AND the served range reaches
     * the track's final (largest) object — a before-tail fetch reports 0. */
    out_plan->end_of_track =
        ls.end_of_track && eff_g == t->largest_group &&
        (eff_whole || eff_o == t->largest_object);

    if (retrieval != NULL) {
        retrieval->track = track_handle(c, tslot);
        retrieval->track_gen = t->track_gen;
        retrieval->start_group = req_sg;
        retrieval->start_object = req_so;
        retrieval->end_group = eff_g;
        retrieval->end_object = eff_whole ? UINT64_MAX : eff_o;
    }
    *out = fetch_handle(c, fslot);
    return MOQR_OK;
}


moqr_result_t
moqr_core_fetch_open(moqr_core_t *c, moqr_binding_t bh,
                     const moqr_fetch_req_t *req, uint64_t now_us,
                     moqr_fetch_t *out, moqr_fetch_plan_t *out_plan)
{
    (void)now_us;
    return core_fetch_admit(c, bh, req, out, out_plan, NULL);
}


moqr_result_t
moqr_core_internal_retrieve_open(moqr_core_t *c, moqr_binding_t bh,
                                 const moqr_fetch_req_t *req, uint64_t now_us,
                                 moqr_fetch_t *owner,
                                 moqr_core_retrieve_plan_t *plan)
{
    (void)now_us;
    if (plan == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_fetch_plan_t wire;
    moqr_result_t rc = core_fetch_admit(c, bh, req, owner, &wire, plan);
    if (rc == MOQR_OK) {
        plan->wire = wire;
    }
    return rc;
}


moqr_result_t
moqr_core_internal_retrieve_open_cluster(moqr_core_t *c, moqr_binding_t bh,
    const moqr_fetch_req_t *req, moqr_cluster_advertisement_id_t source,
    uint64_t requester_hop_id,
    uint64_t requester_correlation_id, uint64_t now_us,
    moqr_fetch_t *owner, moqr_core_retrieve_plan_t *plan)
{
    if (c == NULL || !c->cluster_enabled || source.serial == 0 || req == NULL ||
        req->struct_size < offsetof(moqr_fetch_req_t, report_gaps) || !ftn_view_ok(req->ns, req->name))
        return MOQR_ERR_INVAL;
    if (cluster_owner_node(c, source, req->ns) == UINT32_MAX)
        return MOQR_ERR_STALE_HANDLE;
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) return MOQR_ERR_STALE_HANDLE;
    uint64_t hop = b->peer_hop_id, corr = b->correlation_id;
    b->peer_hop_id = requester_hop_id;
    b->correlation_id = requester_correlation_id;
    c->cluster_forced = source;
    moqr_result_t rc = moqr_core_internal_retrieve_open(c, bh, req, now_us, owner, plan);
    memset(&c->cluster_forced, 0, sizeof(c->cluster_forced));
    b->peer_hop_id = hop;
    b->correlation_id = corr;
    return rc;
}


moqr_result_t
moqr_core_internal_retrieve_view(moqr_core_t *c, moqr_fetch_t owner,
                                 uint64_t start_group, uint64_t start_object,
                                 uint64_t end_group, uint64_t end_object)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, owner, NULL);
    if (f == NULL || !f->retrieval_owner) {
        return MOQR_ERR_STALE_HANDLE;
    }
    const r_track_t *t = &c->tracks[f->track];
    if ((t->gen & 1u) == 0 || t->gen != f->track_gen_slot ||
        t->track_gen != f->retrieval_track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (loc_cmp(start_group, start_object, end_group, end_object) > 0 ||
        loc_cmp(start_group, start_object, f->start_group, f->start_object) < 0 ||
        loc_cmp(end_group, end_object, f->adm_end_group, f->adm_end_object) > 0) {
        return MOQR_ERR_INVAL;
    }
    fetch_unpin(c, f);
    f->cur_group = start_group;
    f->cur_object = start_object;
    f->end_group = end_group;
    f->end_whole = end_object == UINT64_MAX;
    f->end_object = f->end_whole ? 0 : end_object;
    f->lead_marker = false;
    f->gap_active = false;
    f->exhausted = false;
    return MOQR_OK;
}


/* Bounded coalesce of a chunked COMPLETE FETCH payload. Retained standalone FETCH has no
 * zero-copy chunk-write session seam yet (the future begin_fetch_object /
 * write_fetch_object_data / end_fetch_object API). A
 * chunked COMPLETE record (produced by production streaming ingest) is
 * therefore served by coalescing its retained slices, in log order, into ONE
 * OWNING rcbuf so the existing single-payload write_fetch_object path serves it
 * unchanged. Bounded: charged as the fetch pin (fetch_pin_bytes, floored to the
 * per-track log budget), so the closed-form memory model holds. The subscribe
 * delivery path stays zero-copy and untouched.
 *
 * Lifetime (important): the payload is an OWNING rcbuf built with
 * moq_rcbuf_create — it copies the bytes into storage it allocates and frees them
 * via the allocator VTABLE it copied (whose ctx is the caller's allocator, which
 * outlives the core). It is deliberately NOT a moq_rcbuf_wrap over a core-owned
 * buffer with a core-pointer release callback: the session layer increfs this
 * payload into a send action and may hold the final reference PAST core teardown
 * (after moqr_core_fetch_commit unpins the core's ref), so the free must not
 * dereference the core. The price is a second copy on this cold retained-FETCH
 * path (chunks -> temp -> owning rcbuf) — accepted for a core-independent
 * lifetime. Failure at any step is a TERMINAL error (never a partial buffer),
 * with the temp released. */
static moqr_result_t
fetch_coalesce_chunks(moqr_core_t *c, const moqr_log_t *log,
                      const moqr_record_view_t *v, moq_rcbuf_t **out)
{
    *out = NULL;
    uint64_t total = v->declared_len;
    uint8_t *tmp = NULL;
    if (total > 0) {
        tmp = r_alloc(c, (size_t)total);
        if (tmp == NULL) {
            return MOQR_ERR_NOMEM;   /* buffer alloc: fail closed */
        }
    }
    uint64_t off = 0;
    for (uint32_t i = 0; i < v->chunk_count; i++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_log_view_chunk(log, v, i, &cb, &clen) != MOQR_OK ||
            off + clen > total) {
            r_free(c, tmp, (size_t)total);
            return MOQR_ERR_INTERNAL;   /* corrupt chunk list: fail closed */
        }
        if (clen > 0) {
            memcpy(tmp + off, moq_rcbuf_data(cb), (size_t)clen);
        }
        off += clen;
    }
    if (off != total) {
        r_free(c, tmp, (size_t)total);   /* chunk bytes != declared: fail closed */
        return MOQR_ERR_INTERNAL;
    }
    /* Copy the coalesced bytes into an owning rcbuf, then release the temp
     * (freed whether create succeeds or fails). */
    moq_rcbuf_t *rc_out = NULL;
    moq_result_t crc = moq_rcbuf_create(&c->alloc, tmp, (size_t)total, &rc_out);
    r_free(c, tmp, (size_t)total);
    if (crc != MOQ_OK) {
        return MOQR_ERR_NOMEM;
    }
    *out = rc_out;
    return MOQR_OK;
}


/* Location order: negative, zero, positive. */
static int
loc_cmp(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo)
{
    if (ag != bg) {
        return ag < bg ? -1 : 1;
    }
    if (ao != bo) {
        return ao < bo ? -1 : 1;
    }
    return 0;
}


/* What lies between the cursor and the next retained object `x` (or, with no
 * object left in range, the range end). */
typedef enum fetch_run {
    FETCH_RUN_NONE = 0,   /* nothing: the cursor is at x, or past the range  */
    FETCH_RUN_HOLE,       /* Locations not held that the log has evidence for */
    FETCH_RUN_UNKNOWN,    /* Locations the log has no evidence about          */
} fetch_run_t;


static void fetch_range_end(const r_fetch_t *f, uint64_t *eg, uint64_t *eo);


/* An UNKNOWN run starting at (g, o): it covers the rest of g and every group
 * the log does not hold after it, so it ends just before the next retained
 * group, or at the range end when that comes first. Bounded by the retained
 * group count. */
static fetch_run_t
fetch_unknown_run(const moqr_log_t *log, const r_fetch_t *f, uint64_t g,
                  uint64_t *eg, uint64_t *eo)
{
    uint64_t rg, ro;
    fetch_range_end(f, &rg, &ro);
    uint64_t ng = UINT64_MAX;
    bool have_next = false;
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t id = moqr_log_group_id_at(log, gi);
        if (id > g) {
            ng = id;
            have_next = true;
            break;   /* ids ascend */
        }
    }
    uint64_t end_g = rg, end_o = ro;
    if (have_next && loc_cmp(ng - 1u, UINT64_MAX, rg, ro) < 0) {
        end_g = ng - 1u;
        end_o = UINT64_MAX;
    }
    *eg = end_g;
    *eo = end_o;
    return FETCH_RUN_UNKNOWN;
}


/* Classify what precedes `x` from the cursor and write a run's inclusive end.
 *
 * A hole is a Location the log can expect: one inside a retained group's
 * extent (at or below the greatest id any record of that group carries) that
 * no retained object accounts for. A Location holding a status object is a
 * known non-object and is stepped over; an abandoned record counts as not
 * held. The caller bounds `x` at the first Object still arriving, so none is
 * met here.
 *
 * Past a group's extent the answer depends on what the log knows of the
 * group's end. Past a known final (moqr_log_group_final) no object exists,
 * so nothing there is a run. Otherwise -- and everywhere in a group the log
 * never saw -- a gap in observed ids conveys nothing about the skipped
 * Locations unless their non-existence is signalled (2.1.2); the log does
 * not read the Prior Group/Object ID Gap properties, so a known final is the
 * only such signal it has. They are a run of unknown state when the fetch
 * reports those (report_unknown), and passed over otherwise. Bounded by the
 * retained records stepped over. */
/* Whether the Object at (g, o) has expired from the cache (§10.3): its
 * record is retained but it may no longer be served, so its state is
 * unknown. */
static bool
fetch_loc_expired(const moqr_log_t *log, const r_track_t *t, uint64_t g,
                  uint64_t o, uint64_t now_us)
{
    moqr_record_view_t v;
    memset(&v, 0, sizeof(v));
    return t->has_max_cache &&
           moqr_log_internal_object_arrival(log, g, o, &v.arrival_us) &&
           track_rec_expired(t, &v, now_us);
}


static fetch_run_t
fetch_run_before(const moqr_log_t *log, const r_fetch_t *f, const r_track_t *t,
                 uint64_t now_us, bool have_x, const moqr_record_view_t *x,
                 uint64_t *eg, uint64_t *eo)
{
    uint64_t rg, ro;
    fetch_range_end(f, &rg, &ro);
    uint64_t g = f->cur_group, o = f->cur_object;
    uint64_t last = 0;
    for (;;) {
        if (have_x ? loc_cmp(g, o, x->group_id, x->object_id) >= 0
                   : loc_cmp(g, o, rg, ro) > 0) {
            return FETCH_RUN_NONE;
        }
        if (!moqr_log_group_last(log, g, &last)) {
            /* Nothing retained in this group. */
            if (f->report_unknown) {
                return fetch_unknown_run(log, f, g, eg, eo);
            }
            if (have_x && x->group_id > g) {
                g = x->group_id;   /* the next object's group may open with
                                    * a hole */
                o = 0;
                continue;
            }
            return FETCH_RUN_NONE;
        }
        if (o > last) {
            uint64_t fin = 0;
            if (f->report_unknown && !moqr_log_group_final(log, g, &fin)) {
                return fetch_unknown_run(log, f, g, eg, eo);
            }
            if (g == UINT64_MAX) {
                return FETCH_RUN_NONE;
            }
            g++;
            o = 0;
            continue;
        }
        moqr_obj_status_t st;
        moqr_obj_state_t state;
        if (!moqr_log_object_status(log, g, o, &st, &state)) {
            break;   /* a hole inside the group's extent */
        }
        if (st == MOQR_OBJ_NORMAL) {
            if (state != MOQR_OBJ_COMPLETE) {
                break;   /* in progress or abandoned: not held */
            }
            if (!fetch_loc_expired(log, t, g, o, now_us)) {
                return FETCH_RUN_NONE;   /* the next retained object itself */
            }
            /* Expired from the cache: neither served nor known not to
             * exist. Reported as unknown, through the expired Objects
             * that follow it, or passed over as other unknown Locations
             * are when those are not reported. */
            if (f->report_unknown) {
                uint64_t e = o;
                while (e < last && !(g == rg && e >= ro) &&
                       !(have_x && x->group_id == g && e + 1u >= x->object_id) &&
                       moqr_log_object_status(log, g, e + 1u, &st, &state) &&
                       st == MOQR_OBJ_NORMAL && state == MOQR_OBJ_COMPLETE &&
                       fetch_loc_expired(log, t, g, e + 1u, now_us)) {
                    e++;
                }
                *eg = g;
                *eo = e;
                return FETCH_RUN_UNKNOWN;
            }
        }
        /* A status record is not an object or a hole. Carry at the last
         * Object ID rather than revisiting this group's first location. */
        if (o == UINT64_MAX) {
            if (g == UINT64_MAX) {
                return FETCH_RUN_NONE;
            }
            g++;
            o = 0;
        } else {
            o++;
        }
    }
    /* The hole starts at (g, o) and ends before the next object, or at the
     * range end, never past the group's extent; a status object at the end
     * is not part of it. */
    uint64_t end_o;
    if (have_x) {
        end_o = (x->group_id == g && x->object_id > 0) ? x->object_id - 1 : last;
    } else {
        end_o = (rg == g && ro < last) ? ro : last;
    }
    for (;;) {
        moqr_obj_status_t st;
        moqr_obj_state_t state;
        if (end_o <= o || !moqr_log_object_status(log, g, end_o, &st, &state) ||
            st == MOQR_OBJ_NORMAL) {
            break;
        }
        end_o--;
    }
    *eg = g;
    *eo = end_o;
    return FETCH_RUN_HOLE;
}


/* The range's inclusive last Location; a whole-group end runs to the group's
 * last possible id. */
static void
fetch_range_end(const r_fetch_t *f, uint64_t *eg, uint64_t *eo)
{
    *eg = f->end_group;
    *eo = f->end_whole ? UINT64_MAX : f->end_object;
}


moqr_result_t
moqr_core_fetch_peek(moqr_core_t *c, moqr_fetch_t fh, uint64_t now_us,
                     moqr_fetch_item_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    memset(out, 0, sizeof(*out));
    f->gap_active = false;
    if (f->exhausted) {
        out->kind = MOQR_FETCH_ITEM_END;
        return MOQR_OK;
    }

    /* Idempotent re-peek: reconstruct the held OBJECT from stored scalars +
     * pins, so eviction of the underlying record between pumps is harmless. */
    if (f->out_active) {
        out->kind = MOQR_FETCH_ITEM_OBJECT;
        out->rec.group_id = f->out_group;
        out->rec.subgroup_id = f->out_subgroup;
        out->rec.object_id = f->out_object;
        out->rec.publisher_priority = f->out_priority;
        out->rec.datagram_pref = f->out_datagram;
        out->rec.status = MOQR_OBJ_NORMAL;
        out->rec.payload = f->pin_payload;
        out->rec.properties = f->pin_properties;
        return MOQR_OK;
    }

    /* Track gone under a live fetch: the accepted retained range was lost (an
     * open fetch pins the track against age/free, so this is the defensive net —
     * e.g. capacity eviction on an ACTIVE track that also freed it). It is a
     * TERMINAL (TOO_OLD), never a clean END — a clean END would let a caller FIN
     * a truncated range as if it completed. */
    if ((c->tracks[f->track].gen & 1u) == 0 ||
        c->tracks[f->track].gen != f->track_gen_slot) {
        return MOQR_ERR_TOO_OLD;
    }
    const moqr_log_t *log = c->tracks[f->track].log;

    /* Mid-fetch eviction: our resume group fell below the retention horizon, so
     * the remaining range can no longer be served fully — terminate (the binding
     * resets the fetch stream) rather than fabricate a silent gap. */
    moqr_log_stats_t ls;
    moqr_log_get_stats(log, &ls);
    if (!f->retrieval_owner && ls.group_count != 0 &&
        f->cur_group < ls.oldest_group_id) {
        return MOQR_ERR_TOO_OLD;
    }

    /* Evicted-prefix lead marker precedes all objects. It carries no payload
     * (no pin) and is idempotent while lead_marker holds -- cleared only on its
     * commit -- so a re-peek after a held/WOULD_BLOCK marker re-emits the same
     * marker. Placed after the eviction guards: if the anchored oldest group has
     * itself been evicted since open, that guard terminates (TOO_OLD) first
     * rather than emit a marker whose retained objects are already gone. */
    if (f->lead_marker) {
        out->kind = MOQR_FETCH_ITEM_MARKER;
        out->marker_group = f->lead_marker_group;
        return MOQR_OK;
    }

    moqr_record_view_t v;
    moqr_result_t rc = fetch_scan_next(log, f, &c->tracks[f->track], now_us, &v);
    if (rc != MOQR_OK && rc != MOQR_DONE) {
        return rc;   /* defensive terminal */
    }
    /* An Object still arriving before the next whole one is the next thing
     * the range serves: it is included in full once it completes (draft-22
     * 3.2), so nothing past it is served or reported before it. */
    uint64_t pg = 0, po = 0;
    const bool pending =
        fetch_scan_pending(log, f, &pg, &po) &&
        (rc == MOQR_DONE || loc_cmp(pg, po, v.group_id, v.object_id) < 0);
    moqr_record_view_t bound = v;
    if (pending) {
        memset(&bound, 0, sizeof(bound));
        bound.group_id = pg;
        bound.object_id = po;
    }
    if (f->report_gaps) {
        /* A run the log does not hold before the next object (or the range
         * end) is reported first; the object is served once nothing the log
         * can expect, or (report_unknown) has no evidence about, precedes
         * it. */
        uint64_t eg = 0, eo = 0;
        fetch_run_t run = fetch_run_before(log, f, &c->tracks[f->track], now_us,
                                           rc == MOQR_OK || pending, &bound,
                                           &eg, &eo);
        if (run != FETCH_RUN_NONE) {
            f->gap_active = true;
            f->gap_group = eg;
            f->gap_object = eo;
            out->kind = run == FETCH_RUN_HOLE ? MOQR_FETCH_ITEM_GAP
                                              : MOQR_FETCH_ITEM_UNKNOWN;
            out->rec.group_id = eg;
            out->rec.object_id = eo;
            return MOQR_OK;
        }
    }
    if (pending) {
        if (!f->report_pending) {
            return MOQR_ERR_WOULD_BLOCK;   /* retry once it is whole */
        }
        /* Committing it gives it up: the cursor moves past its Location. */
        f->gap_active = true;
        f->gap_group = pg;
        f->gap_object = po;
        out->kind = MOQR_FETCH_ITEM_PENDING;
        out->rec.group_id = pg;
        out->rec.object_id = po;
        return MOQR_OK;
    }
    if (rc == MOQR_DONE) {
        out->kind = MOQR_FETCH_ITEM_END;
        return MOQR_OK;
    }

    /* Pin budget: a single object is guaranteed to fit an idle budget (the
     * budget is floored to the log's max_bytes and a record can never exceed
     * that), so an object bigger than the whole budget is impossible — kept as a
     * terminal (CAPACITY) for defense. A momentarily-full budget (other fetches
     * hold pins) is WOULD_BLOCK: retry after they commit. Never an infinite wait. */
    uint64_t payload_len = v.chunk_count > 0
                               ? v.declared_len
                               : (uint64_t)moq_rcbuf_len(v.payload);
    uint64_t bytes = payload_len + (uint64_t)moq_rcbuf_len(v.properties);
    if (bytes > c->fetch_pin_cap) {
        return MOQR_ERR_CAPACITY;
    }
    if (c->fetch_pin_used + bytes > c->fetch_pin_cap) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    /* Acquire the payload pin AFTER the budget checks (so a rejected object never
     * allocates). Whole-object records pin the retained rcbuf directly (zero
     * copy); chunked COMPLETE records coalesce their slices into one owned
     * rcbuf. A coalesce failure is a terminal — the peer already has FETCH_OK
     * so the bind resets the fetch stream rather than send a short/empty object. */
    moq_rcbuf_t *payload_pin;
    if (v.chunk_count > 0) {
        moqr_result_t crc = fetch_coalesce_chunks(c, log, &v, &payload_pin);
        if (crc != MOQR_OK) {
            return crc;
        }
    } else {
        payload_pin = moq_rcbuf_incref((moq_rcbuf_t *)(void *)v.payload);
    }
    f->pin_payload = payload_pin;
    f->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)v.properties);
    f->pin_bytes = bytes;
    c->fetch_pin_used += bytes;
    f->out_active = true;
    f->out_group = v.group_id;
    f->out_subgroup = v.subgroup_id;
    f->out_object = v.object_id;
    f->out_priority = v.publisher_priority;
    f->out_datagram = v.datagram_pref;
    out->kind = MOQR_FETCH_ITEM_OBJECT;
    out->rec = v;
    /* Present the item as whole-object to the bind: payload is the pinned rcbuf
     * (coalesced for a chunked record), and chunk_count is cleared so the fetch
     * writer takes the single-payload path — matching the re-peek reconstruction
     * above. Whole-object records are unchanged (payload_pin == pinned v.payload,
     * chunk_count already 0). */
    out->rec.payload = f->pin_payload;
    out->rec.chunk_count = 0;
    return MOQR_OK;
}


moqr_result_t
moqr_core_fetch_commit(moqr_core_t *c, moqr_fetch_t fh)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Committing the lead marker: clear it (the marker's only state advance) and
     * return. There is no pin to release, and the cursor already starts at the
     * first retained group, so object scanning resumes there next peek. */
    if (f->lead_marker) {
        f->lead_marker = false;
        return MOQR_OK;
    }
    if (f->gap_active) {
        /* Skip past the reported run. A run ending at the range end exhausts
         * the fetch; one ending at a group's last id resumes at the next
         * group. */
        f->gap_active = false;
        uint64_t eg, eo;
        fetch_range_end(f, &eg, &eo);
        if (loc_cmp(f->gap_group, f->gap_object, eg, eo) >= 0) {
            f->exhausted = true;
            return MOQR_OK;
        }
        if (f->gap_object == UINT64_MAX) {
            f->cur_group = f->gap_group + 1;
            f->cur_object = 0;
        } else {
            f->cur_group = f->gap_group;
            f->cur_object = f->gap_object + 1;
        }
        return MOQR_OK;
    }
    if (!f->out_active) {
        return MOQR_ERR_WRONG_STATE;   /* commit without a peeked object */
    }
    /* Advance strictly past a full-width Object ID. The range check also
     * prevents carrying beyond the last representable Group ID. */
    uint64_t eg, eo;
    fetch_range_end(f, &eg, &eo);
    if (loc_cmp(f->out_group, f->out_object, eg, eo) >= 0) {
        f->exhausted = true;
    } else if (f->out_object == UINT64_MAX) {
        f->cur_group = f->out_group + 1;
        f->cur_object = 0;
    } else {
        f->cur_group = f->out_group;
        f->cur_object = f->out_object + 1;
    }
    fetch_unpin(c, f);
    return MOQR_OK;
}


moqr_result_t
moqr_core_internal_fetch_track(const moqr_core_t *c, moqr_fetch_t fh,
                               moqr_track_t *track, uint64_t *track_gen)
{
    if (c == NULL || track == NULL || track_gen == NULL) {
        return MOQR_ERR_INVAL;
    }
    const r_fetch_t *f = fetch_resolve((moqr_core_t *)(void *)c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    const r_track_t *t = &c->tracks[f->track];
    if ((t->gen & 1u) == 0 || t->gen != f->track_gen_slot) {
        return MOQR_ERR_STALE_HANDLE;
    }
    *track = track_handle(c, f->track);
    *track_gen = t->track_gen;
    return MOQR_OK;
}


moqr_result_t
moqr_core_fetch_close(moqr_core_t *c, moqr_fetch_t fh)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    bool same_track = f->track < c->max_tracks &&
                      c->tracks[f->track].gen == f->track_gen_slot;
    fetch_retire(c, f);   /* invalidate the handle */
    if (same_track) {
        track_touch(c, f->track);   /* the reader leaves: last use */
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_limits_resolve(const moqr_core_relay_cfg_t *cfg,
                         moqr_core_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
#define L_RESOLVE(dst, field, def)     out->dst = RELAY_CFG_HAS(cfg, field) && cfg->field != 0 ? cfg->field : (def)
    L_RESOLVE(max_bindings, max_bindings, R_DEF_BINDINGS);
    L_RESOLVE(max_tracks, max_tracks, R_DEF_TRACKS);
    L_RESOLVE(max_subs, max_subs, R_DEF_SUBS);
    L_RESOLVE(max_ns_nodes, max_ns_nodes, R_DEF_NS_NODES);
    L_RESOLVE(max_ns_subs, max_ns_subs, R_DEF_NS_SUBS);
    L_RESOLVE(max_intents, max_intents, R_DEF_INTENTS);
    L_RESOLVE(name_intern_bytes, name_intern_bytes, R_DEF_INTERN);
    L_RESOLVE(max_parked, max_parked, R_DEF_PARKED);
    L_RESOLVE(parked_bytes, parked_bytes, R_DEF_PARKED_BYTES);
    L_RESOLVE(max_grants, max_grants, R_DEF_GRANTS);
    L_RESOLVE(grant_bytes, grant_bytes, R_DEF_GRANT_BYTES);
    L_RESOLVE(max_fetches, max_fetches, R_DEF_FETCHES);
    L_RESOLVE(max_cancels, max_cancels, out->max_bindings);
    L_RESOLVE(fetch_pin_bytes, fetch_pin_bytes, R_DEF_FETCH_PIN_BYTES);
#undef L_RESOLVE
    /* Atomicity invariant: every single *atomic* reservation must fit an
     * empty intent ring, so a binding that fully drains can always retry to
     * success. The reservations, by call:
     *   subscribe               2 (ACCEPT + UPSTREAM_SUBSCRIBE)
     *   upstream_ok / _error    <= max_subs    (parked-sub fan-out)
     *   announce / unannounce   <= max_ns_subs (one namespace's watchers)
     *   ns_subscribe            <= max_ns_nodes (matching announce nodes)
     *   binding_close           chunked: 1 per reject, <= max_ns_subs per
     *                           announce (resumable). The ring need only
     *                           hold the largest single unit. */
    {
        uint64_t need = 2;
        if (out->max_subs > need) {
            need = out->max_subs;
        }
        if (out->max_ns_subs > need) {
            need = out->max_ns_subs;
        }
        if (out->max_ns_nodes > need) {
            need = out->max_ns_nodes;
        }
        /* force_withdraw purges one track per unit: that track's
         * subscriber terminals plus its one upstream release. */
        if ((uint64_t)out->max_subs + 1u > need) {
            need = (uint64_t)out->max_subs + 1u;
        }
        if (need > out->max_intents) {
            out->max_intents =
                need > UINT32_MAX ? UINT32_MAX : (uint32_t)need;
        }
    }
    return MOQR_OK;
}


void
moqr_core_get_limits(const moqr_core_t *c, moqr_core_limits_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (c == NULL) {
        return;
    }
    out->max_bindings = c->max_bindings;
    out->max_tracks = c->max_tracks;
    out->max_subs = c->max_subs;
    out->max_ns_nodes = c->max_ns_nodes;
    out->max_ns_subs = c->max_ns_subs;
    out->max_intents = c->max_intents;
    out->max_parked = c->max_parked;
    out->max_grants = c->max_grants;
    out->max_cancels = c->max_cancels;
    out->name_intern_bytes = (uint32_t)c->intern_budget;
    out->parked_bytes = (uint32_t)c->parked_bytes_cap;
    out->grant_bytes = (uint32_t)c->grant_bytes_cap;
    out->fetch_pin_bytes = (uint32_t)c->fetch_pin_cap;
    out->max_fetches = c->max_fetches;
}


moqr_result_t
moqr_core_upstream_lost(moqr_core_t *c, moqr_track_t th, uint64_t track_gen)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
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


/* The track's SOURCE ended gracefully — a pull upstream that sent SUBSCRIBE_DONE
 * or a push publisher that finished (PUBLISH_FINISHED). Terminate every
 * downstream subscriber on the track with a wire SUBSCRIBE_DONE carrying
 * `status`, then release the source — mirroring moqr_core_upstream_lost (WARM if
 * retained content remains for a rejoin, else freed) — so no downstream hangs
 * behind a now-sourceless ACTIVE track. Generation-guarded like the other
 * upstream resolutions; a stale or non-ACTIVE track is refused. Reserve-before-
 * mutate: needs one intent per downstream sub, so a short ring returns
 * MOQR_ERR_WOULD_BLOCK with NO state change and the binding retries after a
 * drain. */
moqr_result_t
moqr_core_source_done(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                      moqr_pd_desc_t status, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt terminal before retiring anything: every downstream subscription must stay live. */
    if (!moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* One SUB_DONE per downstream sub, reserved up front: either every
     * subscriber is told or none is (no partial termination on a short ring). */
    uint32_t n = track_sub_count(c, tslot);
    if (n > 0 && !intent_space(c, n)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint32_t tgen = c->tracks[tslot].gen; /* pool gen; stable across the loop */
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != tgen) {
            continue;
        }
        uint64_t bcookie = 0;
        if (s->binding < c->max_bindings &&
            c->bindings[s->binding].gen == s->binding_gen) {
            bcookie = c->bindings[s->binding].cookie;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
        it->binding_cookie = bcookie;
        it->cookie = s->cookie;
        it->sub = sub_handle(c, i);
        it->pd = status; /* the forwarded terminal, with its own origin */
        sub_retire(c, i, now_us);
    }
    /* Release the upstream, mirroring moqr_core_upstream_lost: bump the gen so a
     * late upstream resolution is refused; keep retained content WARM for a
     * rejoin, or free the track when nothing remains. */
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
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


/* End the track's source with every terminal produced by the delivery
 * scans; `abort` selects the immediate ending (below) over the graceful one.
 *
 * Graceful: the track's SOURCE finished and no further objects will be
 * admitted for it. Every forwarding downstream subscriber keeps what was
 * already admitted -- its pending records, notices and FINs are delivered in
 * their usual order -- and retires with a SUB_DONE carrying `status` from
 * moqr_core_next_delivery once nothing is left to deliver for it, so the
 * terminal follows the data (draft-18 section 10.11, draft-22 section 9.9:
 * the publisher sends PUBLISH_DONE after closing its streams, and a relay
 * forwarding that subscription is that publisher for its subscribers). A
 * paused subscriber (Forward=0) has asked for nothing further and retires on
 * its binding's next delivery scan. Every terminal is produced by that scan,
 * so finishing queues no intent and cannot be refused for intent room: the
 * caller never has to keep a finished source alive to retry. The source is
 * released exactly as by moqr_core_source_done (in either ending). */
static moqr_result_t
source_end_by_scan(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                   moqr_pd_desc_t status, bool abort)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (!moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    uint32_t tgen = c->tracks[tslot].gen;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != tgen) {
            continue;
        }
        s->src_done = true;
        s->src_abort = abort;
        s->src_done_pd = status;
        /* Its terminal is now a delivery outcome: the binding must scan it
         * even when nothing is left to send. */
        if (s->binding < c->max_bindings &&
            c->bindings[s->binding].gen == s->binding_gen) {
            ready_mark(c, s->binding);
        }
    }
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
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
moqr_core_source_finish(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                        moqr_pd_desc_t status, uint64_t now_us)
{
    (void)now_us;   /* nothing retires here; the scans own the timing */
    return source_end_by_scan(c, th, track_gen, status, false);
}


/* The immediate end of a failed source when moqr_core_source_done cannot
 * queue its terminals: the subscriptions are marked so that each binding's
 * next delivery scan retires them before delivering anything else (or keeps
 * the binding marked until the ring has room), and the source is released
 * now, so no ACTIVE source is left without an owner to retry it. Queues no
 * intent; never WOULD_BLOCK. */
/* One subscription cannot be served on its binding's draft (Object
 * Properties its destination cannot represent): it alone ends, with
 * `status`, by its binding's next delivery scan before anything else is
 * delivered for it -- the same per-subscription retirement a failed source
 * gives -- and a full intent ring keeps it marked for a later scan. The
 * track and every other subscription are untouched. */
moqr_result_t
moqr_core_internal_sub_fail(moqr_core_t *c, moqr_sub_t sh, moqr_pd_desc_t status)
{
    if (c == NULL || !moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (s->state != R_SUB_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    s->src_done = true;
    s->src_abort = true;
    s->src_done_pd = status;
    if (s->binding < c->max_bindings &&
        c->bindings[s->binding].gen == s->binding_gen) {
        ready_mark(c, s->binding);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_internal_source_abort(moqr_core_t *c, moqr_track_t th,
                                uint64_t track_gen, moqr_pd_desc_t status)
{
    return source_end_by_scan(c, th, track_gen, status, true);
}
