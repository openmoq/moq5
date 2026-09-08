#include "log_internal.h"
#include <moq/relay/capacity.h>

#include <stddef.h>
#include <string.h>

/* -- trace helpers ---------------------------------------------------- */

static void
tr(moqr_log_t *log, moqr_trace_kind_t kind, uint32_t detail,
   uint64_t e0, uint64_t e1, uint64_t e2, uint64_t e3)
{
    if (log->trace == NULL) {
        return;
    }
    moqr_trace_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = kind;
    rec.detail = detail;
    rec.e0 = e0;
    rec.e1 = e1;
    rec.e2 = e2;
    rec.e3 = e3;
    moqr_trace_emit(log->trace, rec);
}

/* -- config ------------------------------------------------------------ */

#define LOG_CFG_FIELD_FITS(field, n) \
    (offsetof(moqr_log_cfg_t, field) + \
         sizeof(((moqr_log_cfg_t *)0)->field) <= (n))

void
moqr_log_cfg_init_sized(moqr_log_cfg_t *cfg, size_t cfg_size,
                        const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_log_cfg_t) ? cfg_size
                                                 : sizeof(moqr_log_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (LOG_CFG_FIELD_FITS(alloc, n)) {
        cfg->alloc = alloc;
    }
}

/* Field-containment gate against the CALLER's declared struct_size —
 * the consumer-side twin of the initializer's prefix safety. */
#define LOG_CFG_HAS(cfg, field)                       \
    (offsetof(moqr_log_cfg_t, field) +                \
         sizeof(((moqr_log_cfg_t *)0)->field) <= (cfg)->struct_size)

moqr_result_t
moqr_log_cfg_resolve(const moqr_log_cfg_t *cfg, moqr_log_cfg_resolved_t *out)
{
    memset(out, 0, sizeof(*out));
    out->alloc = LOG_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;

    moqr_log_budget_t b = { 0, 0, 0 };
    if (LOG_CFG_HAS(cfg, budget)) {
        b = cfg->budget;
    }
    out->budget.max_groups = b.max_groups != 0 ? b.max_groups
                                               : MOQR_LOG_DEF_MAX_GROUPS;
    out->budget.max_bytes = b.max_bytes != 0 ? b.max_bytes
                                             : MOQR_LOG_DEF_MAX_BYTES;
    out->budget.max_age_us = b.max_age_us;

    out->max_subgroups =
        LOG_CFG_HAS(cfg, max_subgroups_per_group) &&
                cfg->max_subgroups_per_group != 0
            ? cfg->max_subgroups_per_group
            : MOQR_LOG_DEF_MAX_SUBGROUPS;
    out->max_objects_per_group =
        LOG_CFG_HAS(cfg, max_objects_per_group) &&
                cfg->max_objects_per_group != 0
            ? cfg->max_objects_per_group
            : MOQR_LOG_DEF_MAX_OBJECTS;
    out->max_records = MOQR_LOG_DEF_MAX_RECORDS;
    out->max_cursors = LOG_CFG_HAS(cfg, max_cursors) && cfg->max_cursors != 0
                           ? cfg->max_cursors
                           : MOQR_LOG_DEF_MAX_CURSORS;
    out->max_chunk_nodes =
        LOG_CFG_HAS(cfg, max_chunk_nodes) && cfg->max_chunk_nodes != 0
            ? cfg->max_chunk_nodes
            : MOQR_LOG_DEF_MAX_CHUNK_NODES;
    /* Derived counts stay representable in the runtime's 32-bit index
     * types, computed WIDE first: a config that cannot is rejected here —
     * never wrapped into a small pool. */
    {
        uint64_t want = 2ull * out->max_objects_per_group;
        uint64_t cap = 16;   /* historical floor preserved */
        while (cap < want) {
            cap <<= 1;
        }
        if (cap > UINT32_MAX ||
            out->max_subgroups >= UINT32_MAX) {   /* +1 datagram list must
                                                   * not wrap gpos lists */
            memset(out, 0, sizeof(*out));
            return MOQR_ERR_INVAL;
        }
        out->id_set_cap = (uint32_t)cap;
    }
    out->trace = LOG_CFG_HAS(cfg, trace) ? cfg->trace : NULL;
    out->trace_id = LOG_CFG_HAS(cfg, trace_id) ? cfg->trace_id : 0;
    return MOQR_OK;
}

uint64_t
moqr_log_structure_ceiling(const moqr_log_cfg_resolved_t *r)
{
    uint64_t live_cap = moqr_cap_mul(r->budget.max_groups,
                                     r->max_objects_per_group);
    if (live_cap > r->max_records) {
        live_cap = r->max_records;
    }
    /* Checked composition: a wrapped ceiling poisons to UINT64_MAX and the
     * describe layer refuses it (never an understated number). */
    uint64_t s = sizeof(moqr_log_t);
    s = moqr_cap_add(s, moqr_cap_mul(r->budget.max_groups,
                                     sizeof(moqr_group_t)));
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(r->budget.max_groups,
                                                  r->max_subgroups),
                                     sizeof(moqr_subgroup_t)));
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(r->budget.max_groups,
                                                  r->id_set_cap),
                                     sizeof(uint64_t)));
    s = moqr_cap_add(s, moqr_cap_mul(r->max_records, sizeof(moqr_jslot_t)));
    s = moqr_cap_add(s,
                     moqr_cap_mul(r->max_cursors, sizeof(moqr_cursor_slot_t)));
    s = moqr_cap_add(s, moqr_cap_mul(r->max_chunk_nodes,
                                     sizeof(moqr_chunk_node_t)));
    /* Record vectors: live records with doubling slack (<= 2x) plus one
     * initial vector per possible list. A ceiling, not an estimate. */
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(2u, live_cap),
                                     sizeof(moqr_rec_t)));
    s = moqr_cap_add(s,
                     moqr_cap_mul(moqr_cap_mul(r->budget.max_groups,
                                               (uint64_t)r->max_subgroups + 1u),
                                  (uint64_t)MOQR_REC_VEC_INITIAL *
                                      sizeof(moqr_rec_t)));
    return s;
}

void
moqr_log_append_desc_init(moqr_log_append_desc_t *d)
{
    if (d == NULL) {
        return;
    }
    memset(d, 0, sizeof(*d));
    d->struct_size = sizeof(*d);
}

/* -- lifecycle ---------------------------------------------------------- */

moqr_result_t
moqr_log_create(const moqr_log_cfg_t *cfg, moqr_log_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    moqr_log_cfg_resolved_t r;
    if (moqr_log_cfg_resolve(cfg, &r) != MOQR_OK) {
        return MOQR_ERR_INVAL;   /* derived counts unrepresentable */
    }
    if (r.alloc == NULL || r.alloc->alloc == NULL || r.alloc->free == NULL) {
        return MOQR_ERR_INVAL;   /* cfg too small to carry alloc, or none */
    }

    const moq_alloc_t *a = r.alloc;
    moqr_log_t *log = a->alloc(sizeof(*log), a->ctx);
    if (log == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(log, 0, sizeof(*log));
    log->alloc = *a;
    log->trace = r.trace;
    log->trace_id = r.trace_id;
    log->shard_tag = 1;

    log->max_groups = r.budget.max_groups;
    log->max_bytes = r.budget.max_bytes;
    log->max_age_us = r.budget.max_age_us;
    log->max_subgroups = r.max_subgroups;
    log->max_objects_per_group = r.max_objects_per_group;
    log->max_records = r.max_records;
    log->max_cursors = r.max_cursors;
    log->max_chunk_nodes = r.max_chunk_nodes;
    log->id_set_cap = r.id_set_cap;
    log->final_group_id = MOQR_FINAL_UNKNOWN;
    log->final_track_object_id = MOQR_FINAL_UNKNOWN;

    log->groups =
        log->alloc.alloc((size_t)log->max_groups * sizeof(*log->groups),
                         log->alloc.ctx);
    log->journal =
        log->alloc.alloc((size_t)log->max_records * sizeof(*log->journal),
                         log->alloc.ctx);
    log->cursors =
        log->alloc.alloc((size_t)log->max_cursors * sizeof(*log->cursors),
                         log->alloc.ctx);
    log->chunk_pool =
        log->alloc.alloc((size_t)log->max_chunk_nodes * sizeof(*log->chunk_pool),
                         log->alloc.ctx);
    if (log->groups == NULL || log->journal == NULL || log->cursors == NULL ||
        log->chunk_pool == NULL) {
        moqr_log_destroy(log);
        return MOQR_ERR_NOMEM;
    }
    memset(log->groups, 0, (size_t)log->max_groups * sizeof(*log->groups));
    memset(log->journal, 0,
           (size_t)log->max_records * sizeof(*log->journal));
    memset(log->cursors, 0,
           (size_t)log->max_cursors * sizeof(*log->cursors));
    /* Thread the chunk-node free list: [0..cap-1], last -> NIL. */
    for (uint32_t i = 0; i < log->max_chunk_nodes; i++) {
        log->chunk_pool[i].buf = NULL;
        log->chunk_pool[i].len = 0;
        log->chunk_pool[i].next =
            (i + 1u < log->max_chunk_nodes) ? (i + 1u) : MOQR_CHUNK_NIL;
    }
    log->chunk_free_head = (log->max_chunk_nodes > 0) ? 0u : MOQR_CHUNK_NIL;
    log->chunk_used = 0;

    *out = log;
    return MOQR_OK;
}

/* -- chunk-node pool ------------------------------------------------------ */

/* Pop a free chunk node index; MOQR_CHUNK_NIL when the pool is exhausted. */
static uint32_t
chunk_alloc(moqr_log_t *log)
{
    uint32_t idx = log->chunk_free_head;
    if (idx == MOQR_CHUNK_NIL) {
        return MOQR_CHUNK_NIL;
    }
    log->chunk_free_head = log->chunk_pool[idx].next;
    log->chunk_used++;
    return idx;
}

/* Return a record's chunk list to the pool, decref'ing each retained slice
 * exactly once, and reset the record's list to empty. Byte accounting is the
 * caller's job (eviction subtracts g->bytes wholesale; abandon adjusts
 * explicitly) so this never touches retained_bytes/g->bytes. */
static void
chunk_list_release(moqr_log_t *log, moqr_rec_t *rec)
{
    uint32_t idx = rec->chunk_head;
    while (idx != MOQR_CHUNK_NIL) {
        moqr_chunk_node_t *n = &log->chunk_pool[idx];
        uint32_t next = n->next;
        moq_rcbuf_decref(n->buf);
        n->buf = NULL;
        n->len = 0;
        n->next = log->chunk_free_head;
        log->chunk_free_head = idx;
        log->chunk_used--;
        idx = next;
    }
    rec->chunk_head = MOQR_CHUNK_NIL;
    rec->chunk_tail = MOQR_CHUNK_NIL;
    rec->chunk_count = 0;
}

static void
rec_release(moqr_log_t *log, moqr_rec_t *rec)
{
    moq_rcbuf_decref(rec->payload);
    moq_rcbuf_decref(rec->properties);
    rec->payload = NULL;
    rec->properties = NULL;
    if (rec->chunk_count > 0) {
        chunk_list_release(log, rec);
    }
}

static void
vec_free(moqr_log_t *log, moqr_rec_vec_t *v)
{
    if (v->recs != NULL) {
        log->alloc.free(v->recs, (size_t)v->cap * sizeof(*v->recs),
                        log->alloc.ctx);
    }
    v->recs = NULL;
    v->count = 0;
    v->cap = 0;
}

static void
group_release(moqr_log_t *log, moqr_group_t *g)
{
    for (uint32_t s = 0; s < g->sg_count; s++) {
        moqr_rec_vec_t *v = &g->sgs[s].v;
        for (uint32_t i = 0; i < v->count; i++) {
            rec_release(log, &v->recs[i]);
        }
        vec_free(log, v);
    }
    for (uint32_t i = 0; i < g->dg.count; i++) {
        rec_release(log, &g->dg.recs[i]);
    }
    vec_free(log, &g->dg);
    if (g->sgs != NULL) {
        log->alloc.free(g->sgs,
                        (size_t)log->max_subgroups * sizeof(*g->sgs),
                        log->alloc.ctx);
        g->sgs = NULL;
    }
    if (g->id_set != NULL) {
        log->alloc.free(g->id_set,
                        (size_t)log->id_set_cap * sizeof(*g->id_set),
                        log->alloc.ctx);
        g->id_set = NULL;
    }
}

/* -- per-group object-identity set ---------------------------------------- */

static bool
id_set_contains(const moqr_log_t *log, const moqr_group_t *g, uint64_t id)
{
    uint32_t mask = log->id_set_cap - 1;
    uint32_t i = (uint32_t)((id * UINT64_C(0x9E3779B97F4A7C15)) >> 32) & mask;
    while (g->id_set[i] != UINT64_MAX) {
        if (g->id_set[i] == id) {
            return true;
        }
        i = (i + 1) & mask;
    }
    return false;
}

static void
id_set_insert(const moqr_log_t *log, moqr_group_t *g, uint64_t id)
{
    uint32_t mask = log->id_set_cap - 1;
    uint32_t i = (uint32_t)((id * UINT64_C(0x9E3779B97F4A7C15)) >> 32) & mask;
    while (g->id_set[i] != UINT64_MAX) {
        i = (i + 1) & mask;
    }
    g->id_set[i] = id;
}

void
moqr_log_destroy(moqr_log_t *log)
{
    if (log == NULL) {
        return;
    }
    moq_alloc_t a = log->alloc;
    if (log->groups != NULL) {
        for (uint32_t i = 0; i < log->group_count; i++) {
            group_release(log, &log->groups[i]);
        }
        a.free(log->groups, (size_t)log->max_groups * sizeof(*log->groups),
               a.ctx);
    }
    if (log->journal != NULL) {
        a.free(log->journal,
               (size_t)log->max_records * sizeof(*log->journal), a.ctx);
    }
    if (log->cursors != NULL) {
        a.free(log->cursors,
               (size_t)log->max_cursors * sizeof(*log->cursors), a.ctx);
    }
    if (log->chunk_pool != NULL) {
        a.free(log->chunk_pool,
               (size_t)log->max_chunk_nodes * sizeof(*log->chunk_pool), a.ctx);
    }
    a.free(log, sizeof(*log), a.ctx);
}

/* -- group lookup (sorted array) ----------------------------------------- */

static moqr_group_t *
group_find(const moqr_log_t *log, uint64_t group_id, uint32_t *out_idx)
{
    uint32_t lo = 0, hi = log->group_count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (log->groups[mid].group_id < group_id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (out_idx != NULL) {
        *out_idx = lo;
    }
    if (lo < log->group_count && log->groups[lo].group_id == group_id) {
        return (moqr_group_t *)&log->groups[lo];
    }
    return NULL;
}

/* -- eviction ------------------------------------------------------------ */

static void
journal_compact_head(moqr_log_t *log)
{
    /* Advance begin past slots whose group no longer exists. */
    while (log->begin_seq < log->next_seq) {
        moqr_jslot_t *s = &log->journal[log->begin_seq % log->max_records];
        if (group_find(log, s->group_id, NULL) != NULL) {
            break;
        }
        log->begin_seq++;
    }
}

static void
evict_group_at(moqr_log_t *log, uint32_t idx, uint32_t reason)
{
    moqr_group_t *g = &log->groups[idx];
    uint64_t gid = g->group_id;
    uint64_t records = g->record_count;
    uint64_t bytes = g->bytes;

    group_release(log, g);
    log->retained_bytes -= bytes;
    log->live_records -= records;
    log->evicted_groups_total++;
    log->evicted_records_total += records;
    if (gid > log->max_evicted_group_id) {
        log->max_evicted_group_id = gid;
    }
    log->any_evicted = true;

    memmove(&log->groups[idx], &log->groups[idx + 1],
            (size_t)(log->group_count - idx - 1) * sizeof(*log->groups));
    log->group_count--;
    memset(&log->groups[log->group_count], 0, sizeof(*log->groups));

    journal_compact_head(log);
    tr(log, MOQR_TRACE_EVICT, reason, gid, records, bytes, log->trace_id);
}

size_t
moqr_log_evict_groups(moqr_log_t *log, size_t max_groups)
{
    if (log == NULL) {
        return 0;
    }
    size_t evicted = 0;
    while (evicted < max_groups && log->group_count > 0) {
        evict_group_at(log, 0, /*reason=*/1 /* explicit */);
        evicted++;
    }
    return evicted;
}

size_t
moqr_log_note_evicted_below(moqr_log_t *log, uint64_t oldest_retained)
{
    if (log == NULL || oldest_retained == 0) {
        return 0;
    }
    /* Locally retained groups below the watermark stay SERVABLE — the
     * upstream eviction only proves nothing MORE will arrive for them, so
     * their unsealed subgroup lists are sealed through the ordinary seal
     * path (late-FIN notices, downstream closes, retirement coverage all
     * follow from that), never destroyed. A seal the preflight rejects
     * (contradictory header-EOG evidence) is left unsealed: the range stays
     * UNKNOWN and fail-closed rather than force-closed. */
    size_t sealed = 0;
    for (uint32_t gi = 0; gi < log->group_count; gi++) {
        moqr_group_t *g = &log->groups[gi];
        if (g->group_id >= oldest_retained) {
            break;   /* ids ascend */
        }
        for (uint32_t si = 0; si < g->sg_count; si++) {
            if (g->sgs[si].sealed) {
                continue;
            }
            /* A list whose tail is ABANDONED was already terminalized
             * downstream by its reset — sealing it would close the same
             * subgroup twice. Leave it to the reset's own accounting. */
            const moqr_rec_vec_t *v = &g->sgs[si].v;
            if (v->count > 0 &&
                v->recs[v->count - 1].obj_state == MOQR_OBJ_ABANDONED) {
                continue;
            }
            if (moqr_log_seal_subgroup(log, g->group_id,
                                       g->sgs[si].subgroup_id) == MOQR_OK) {
                sealed++;
            }
        }
    }
    /* Advance eviction visibility over the ABSENT range: never-received ids
     * below the watermark must probe MOQR_ERR_TOO_OLD ("never will") instead
     * of MOQR_DONE ("not yet"), and can never be admitted late. Retained
     * groups are unaffected: every lookup finds them before the floor test. */
    if (oldest_retained - 1 > log->max_evicted_group_id ||
        !log->any_evicted) {
        if (oldest_retained - 1 > log->max_evicted_group_id) {
            log->max_evicted_group_id = oldest_retained - 1;
        }
        log->any_evicted = true;
    }
    return sealed;
}

uint64_t
moqr_log_evicted_floor(const moqr_log_t *log)
{
    if (log == NULL || !log->any_evicted) {
        return 0;
    }
    return log->max_evicted_group_id + 1;
}

moqr_result_t
moqr_log_abandon_group_open(moqr_log_t *log, uint64_t group_id,
                            moqr_reset_desc_t reset, uint32_t *out_abandoned)
{
    if (out_abandoned != NULL) {
        *out_abandoned = 0;
    }
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL) {
        return MOQR_DONE;   /* not retained: nothing to abandon */
    }
    for (uint32_t si = 0; si < g->sg_count; si++) {
        const moqr_rec_vec_t *v = &g->sgs[si].v;
        if (v->count > 0 &&
            v->recs[v->count - 1].obj_state == MOQR_OBJ_OPEN) {
            if (moqr_log_abandon_record(log, group_id,
                                        g->sgs[si].subgroup_id,
                                        v->recs[v->count - 1].object_id,
                                        reset) == MOQR_OK &&
                out_abandoned != NULL) {
                (*out_abandoned)++;
            }
        }
    }
    return MOQR_OK;
}

size_t
moqr_log_tick(moqr_log_t *log, uint64_t now_us)
{
    if (log == NULL || log->max_age_us == 0) {
        return 0;
    }
    size_t evicted = 0;
    while (log->group_count > 0) {
        moqr_group_t *oldest = &log->groups[0];
        if (now_us - oldest->first_arrival_us < log->max_age_us) {
            break;
        }
        evict_group_at(log, 0, /*reason=*/2 /* age */);
        evicted++;
    }
    return evicted;
}

/* -- append --------------------------------------------------------------- */

static moqr_result_t
vec_push(moqr_log_t *log, moqr_rec_vec_t *v, const moqr_rec_t *rec,
         uint32_t *out_index)
{
    if (v->count == v->cap) {
        uint32_t newcap = v->cap == 0 ? MOQR_REC_VEC_INITIAL : v->cap * 2;
        moqr_rec_t *n =
            log->alloc.alloc((size_t)newcap * sizeof(*n), log->alloc.ctx);
        if (n == NULL) {
            return MOQR_ERR_NOMEM;
        }
        if (v->count != 0) {
            memcpy(n, v->recs, (size_t)v->count * sizeof(*n));
        }
        if (v->recs != NULL) {
            log->alloc.free(v->recs, (size_t)v->cap * sizeof(*v->recs),
                            log->alloc.ctx);
        }
        /* Grow in place: count survives; only storage and cap change.
         * (Regression: routing this through vec_free() zeroed count,
         * truncating the group and leaking pre-growth payload refs.) */
        v->recs = n;
        v->cap = newcap;
    }
    *out_index = v->count;
    v->recs[v->count++] = *rec;
    return MOQR_OK;
}

static moqr_group_t *
group_activate(moqr_log_t *log, uint64_t group_id, uint64_t now_us,
               moqr_result_t *rc)
{
    uint32_t idx = 0;
    moqr_group_t *g = group_find(log, group_id, &idx);
    if (g != NULL) {
        return g;
    }
    if (log->any_evicted && group_id <= log->max_evicted_group_id) {
        *rc = MOQR_ERR_TOO_OLD;
        return NULL;
    }
    if (log->group_count == log->max_groups) {
        /* Evict the oldest to admit — unless the newcomer IS the oldest,
         * in which case admitting it would be self-defeating. */
        if (group_id < log->groups[0].group_id) {
            *rc = MOQR_ERR_CAPACITY;
            return NULL;
        }
        evict_group_at(log, 0, /*reason=*/0 /* budget */);
        (void)group_find(log, group_id, &idx);   /* recompute insert idx */
    }

    memmove(&log->groups[idx + 1], &log->groups[idx],
            (size_t)(log->group_count - idx) * sizeof(*log->groups));
    g = &log->groups[idx];
    memset(g, 0, sizeof(*g));
    g->group_id = group_id;
    g->final_object_id = MOQR_FINAL_UNKNOWN;
    g->first_arrival_us = now_us;
    g->last_arrival_us = now_us;
    g->sgs = log->alloc.alloc(
        (size_t)log->max_subgroups * sizeof(*g->sgs), log->alloc.ctx);
    g->id_set = log->alloc.alloc(
        (size_t)log->id_set_cap * sizeof(*g->id_set), log->alloc.ctx);
    if (g->sgs == NULL || g->id_set == NULL) {
        if (g->sgs != NULL) {
            log->alloc.free(g->sgs,
                            (size_t)log->max_subgroups * sizeof(*g->sgs),
                            log->alloc.ctx);
        }
        if (g->id_set != NULL) {
            log->alloc.free(g->id_set,
                            (size_t)log->id_set_cap * sizeof(*g->id_set),
                            log->alloc.ctx);
        }
        memmove(&log->groups[idx], &log->groups[idx + 1],
                (size_t)(log->group_count - idx) * sizeof(*log->groups));
        *rc = MOQR_ERR_NOMEM;
        return NULL;
    }
    memset(g->sgs, 0, (size_t)log->max_subgroups * sizeof(*g->sgs));
    memset(g->id_set, 0xFF, (size_t)log->id_set_cap * sizeof(*g->id_set));
    log->group_count++;
    return g;
}

/* Final-evidence preflight, shared by EVERY final source (END_OF_GROUP
 * status, EOG datagram, END_OF_TRACK, and the header-EOG seal): true when a
 * retained record of the group sits BEYOND a proposed group-final location.
 * Committing such a final would leave cached content past the declared end
 * of the group — malformed evidence the caller rejects atomically. Object
 * IDs ascend within a list, so each list's last_object_id is its maximum. */
static bool
group_retains_beyond(const moqr_group_t *g, uint64_t final_q)
{
    for (uint32_t s = 0; s < g->sg_count; s++) {
        if (g->sgs[s].has_records && g->sgs[s].last_object_id > final_q) {
            return true;
        }
    }
    for (uint32_t d = 0; d < g->dg.count; d++) {
        if (g->dg.recs[d].object_id > final_q) {
            return true;
        }
    }
    return false;
}

moqr_result_t
moqr_log_append(moqr_log_t *log, const moqr_log_append_desc_t *desc)
{
    if (log == NULL || desc == NULL ||
        desc->struct_size < sizeof(moqr_log_append_desc_t)) {
        return MOQR_ERR_INVAL;
    }
    /* Status objects carry no payload (mirrors the session's
     * write_status_object shape). */
    if (desc->status != MOQR_OBJ_NORMAL && desc->payload != NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Completeness: COMPLETE is the whole-object default; OPEN begins a
     * chunk-filled record (no payload yet, subgroup only). ABANDONED is never
     * appended directly (reached via moqr_log_abandon_record). */
    if (desc->obj_state != MOQR_OBJ_COMPLETE &&
        desc->obj_state != MOQR_OBJ_OPEN) {
        return MOQR_ERR_INVAL;
    }
    if (desc->obj_state == MOQR_OBJ_OPEN &&
        (desc->payload != NULL || desc->datagram_pref ||
         desc->status != MOQR_OBJ_NORMAL)) {
        /* OPEN records: NORMAL subgroup objects only, no payload up front.
         * The header EOG bit IS allowed — it is subgroup metadata (latched on
         * the subgroup, retained on the record for downstream propagation),
         * never a per-record terminal, so it cannot fire while OPEN. */
        return MOQR_ERR_INVAL;
    }
    /* Wire object IDs are varints (< 2^62); UINT64_MAX is the id-set
     * sentinel and can never be a real object. */
    if (desc->object_id == UINT64_MAX) {
        return MOQR_ERR_INVAL;
    }

    /* End-of-track: appends beyond the final location are ordering
     * violations the binding may escalate (spec: malformed-track
     * condition 5, -18 :1069-1072). */
    if (log->end_of_track) {
        if (desc->group_id > log->final_group_id ||
            (desc->group_id == log->final_group_id &&
             desc->object_id > log->final_track_object_id)) {
            return MOQR_ERR_INVAL;
        }
    }

    /* Accounted bytes now = payload + properties (payload is NULL/0 for an
     * OPEN record; its chunks are accounted later, as accepted). The ceiling
     * check uses the eventual size: declared_len + properties for OPEN. */
    uint64_t bytes = (uint64_t)moq_rcbuf_len(desc->payload) +
                     (uint64_t)moq_rcbuf_len(desc->properties);
    uint64_t ceiling = bytes;
    if (desc->obj_state == MOQR_OBJ_OPEN) {
        ceiling = desc->declared_len +
                  (uint64_t)moq_rcbuf_len(desc->properties);
    }
    if (ceiling > log->max_bytes) {
        return MOQR_ERR_CAPACITY;   /* can never fit, even alone */
    }

    /* Final-evidence preflight — BEFORE group activation and any capacity
     * eviction, on a NON-MUTATING lookup, so malformed evidence rejects with
     * zero mutation (an invalid final must never cost unrelated retained
     * data its slot). Per-record final sources: END_OF_GROUP status, the EOG
     * bit on a datagram, and END_OF_TRACK — which must validate its target
     * group regardless of an already-known group final (both the retained
     * content and the declared final may sit beyond the proposed track
     * final) and the log's retained groups against the track final. The
     * header-EOG bit is subgroup metadata; its final gets the same preflight
     * at seal. */
    bool eog_evidence = desc->status == MOQR_OBJ_END_OF_GROUP ||
                        (desc->end_of_group && desc->datagram_pref);
    bool eot_evidence = desc->status == MOQR_OBJ_END_OF_TRACK;
    if (eog_evidence || eot_evidence) {
        const moqr_group_t *pg = group_find(log, desc->group_id, NULL);
        if (pg != NULL) {
            bool lowers = pg->final_object_id == MOQR_FINAL_UNKNOWN ||
                          desc->object_id < pg->final_object_id;
            if ((eot_evidence || lowers) &&
                group_retains_beyond(pg, desc->object_id)) {
                return MOQR_ERR_INVAL;
            }
            if (eot_evidence &&
                pg->final_object_id != MOQR_FINAL_UNKNOWN &&
                pg->final_object_id > desc->object_id) {
                return MOQR_ERR_INVAL;   /* declared group final beyond EOT */
            }
        }
        if (eot_evidence && log->group_count > 0 &&
            log->groups[log->group_count - 1u].group_id > desc->group_id) {
            return MOQR_ERR_INVAL;   /* retained GROUP beyond the track final */
        }
    }

    moqr_result_t rc = MOQR_OK;
    moqr_group_t *g = group_activate(log, desc->group_id, desc->now_us, &rc);
    if (g == NULL) {
        return rc;
    }

    /* Group-final enforcement (EOG evidence seen earlier). */
    if (g->final_object_id != MOQR_FINAL_UNKNOWN &&
        desc->object_id > g->final_object_id) {
        return MOQR_ERR_INVAL;
    }
    if (g->record_count >= log->max_objects_per_group) {
        return MOQR_ERR_CAPACITY;
    }
    /* An object is uniquely (track, group, object); the same identity
     * re-appearing — other subgroup, datagram, any shape — is malformed
     * and must not enter the cache (-18 :795-798, :1069-1077). */
    if (id_set_contains(log, g, desc->object_id)) {
        return MOQR_ERR_INVAL;
    }

    /* Locate the destination list and enforce ordering. */
    moqr_subgroup_t *sg = NULL;
    uint32_t jslot_index = MOQR_JSLOT_DATAGRAM;
    if (desc->datagram_pref) {
        /* Datagrams are unordered on the wire; duplicate identity is
         * already refused group-wide by the id set above. */
    } else {
        for (uint32_t s = 0; s < g->sg_count; s++) {
            if (g->sgs[s].subgroup_id == desc->subgroup_id) {
                sg = &g->sgs[s];
                break;
            }
        }
        if (sg == NULL) {
            if (g->sg_count == log->max_subgroups) {
                return MOQR_ERR_CAPACITY;
            }
            sg = &g->sgs[g->sg_count];
            sg->subgroup_id = desc->subgroup_id;
            sg->has_records = false;
            sg->sealed = false;
            sg->sealed_reset = false;
            sg->reset = moqr_reset_desc_none();
            sg->end_of_group = false;
            jslot_index = g->sg_count;
        } else {
            jslot_index = (uint32_t)(sg - g->sgs);
        }
        /* Within a subgroup, Object IDs ascend strictly
         * (-18 :848; -16 :641). */
        if (sg->has_records && desc->object_id <= sg->last_object_id) {
            return MOQR_ERR_INVAL;
        }
        /* One open object per subgroup: a subgroup stream delivers its objects
         * sequentially, so no later object may be appended while the last
         * record is still OPEN (it would overtake the unfinished object).
         * Complete or abandon it first. */
        if (sg->has_records && sg->v.count > 0 &&
            sg->v.recs[sg->v.count - 1].obj_state == MOQR_OBJ_OPEN) {
            return MOQR_ERR_WRONG_STATE;
        }
    }

    /* Byte budget: evict oldest groups (never the target) until it fits. */
    while (log->retained_bytes + bytes > log->max_bytes) {
        if (&log->groups[0] == g || log->group_count == 0) {
            return MOQR_ERR_CAPACITY;
        }
        evict_group_at(log, 0, /*reason=*/0);
        /* g may have moved during memmove — re-find it. */
        g = group_find(log, desc->group_id, NULL);
        if (g == NULL) {
            return MOQR_ERR_INTERNAL;   /* target must survive eviction */
        }
        if (!desc->datagram_pref) {
            sg = NULL;
            for (uint32_t s = 0; s < g->sg_count; s++) {
                if (g->sgs[s].subgroup_id == desc->subgroup_id) {
                    sg = &g->sgs[s];
                    jslot_index = s;
                    break;
                }
            }
            if (sg == NULL) {
                sg = &g->sgs[g->sg_count];
                sg->subgroup_id = desc->subgroup_id;
                sg->has_records = false;
                sg->sealed = false;
                sg->sealed_reset = false;
                sg->reset = moqr_reset_desc_none();
                sg->end_of_group = false;
                jslot_index = g->sg_count;
            }
        }
    }

    /* Journal window: compact, then evict if still full. */
    while (log->next_seq - log->begin_seq == log->max_records) {
        journal_compact_head(log);
        if (log->next_seq - log->begin_seq < log->max_records) {
            break;
        }
        if (&log->groups[0] == g || log->group_count == 0) {
            return MOQR_ERR_CAPACITY;
        }
        evict_group_at(log, 0, /*reason=*/0);
        g = group_find(log, desc->group_id, NULL);
        if (g == NULL) {
            return MOQR_ERR_INTERNAL;
        }
        if (!desc->datagram_pref) {
            sg = NULL;
            for (uint32_t s = 0; s < g->sg_count; s++) {
                if (g->sgs[s].subgroup_id == desc->subgroup_id) {
                    sg = &g->sgs[s];
                    jslot_index = s;
                    break;
                }
            }
            if (sg == NULL) {
                sg = &g->sgs[g->sg_count];
                sg->subgroup_id = desc->subgroup_id;
                sg->has_records = false;
                sg->sealed = false;
                sg->sealed_reset = false;
                sg->reset = moqr_reset_desc_none();
                sg->end_of_group = false;
                jslot_index = g->sg_count;
            }
        }
    }

    /* Commit point: build the record, push, take ownership. Everything
     * after this must not fail except the vector growth, which happens
     * first. */
    moqr_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.arrival_seq = log->next_seq;
    rec.arrival_us = desc->now_us;
    rec.group_id = desc->group_id;
    rec.subgroup_id = desc->datagram_pref ? 0 : desc->subgroup_id;
    rec.object_id = desc->object_id;
    rec.bytes = bytes;
    rec.declared_len = (desc->obj_state == MOQR_OBJ_OPEN) ? desc->declared_len
                                                          : 0;
    rec.payload = desc->payload;
    rec.properties = desc->properties;
    rec.chunk_head = MOQR_CHUNK_NIL;   /* 0 is a valid pool index; NIL != 0 */
    rec.chunk_tail = MOQR_CHUNK_NIL;
    rec.chunk_count = 0;
    rec.publisher_priority = desc->publisher_priority;
    rec.status = desc->status;
    rec.obj_state = desc->obj_state;   /* COMPLETE (0) or OPEN */
    rec.datagram_pref = desc->datagram_pref;
    rec.end_of_group = desc->end_of_group;

    uint32_t rec_index = 0;
    moqr_rec_vec_t *dst = desc->datagram_pref ? &g->dg : &sg->v;
    moqr_result_t prc = vec_push(log, dst, &rec, &rec_index);
    if (prc != MOQR_OK) {
        return prc;   /* caller keeps its refs */
    }

    if (!desc->datagram_pref) {
        if (jslot_index == g->sg_count) {
            g->sg_count++;   /* the subgroup slot is now committed */
        }
        sg->last_object_id = desc->object_id;
        sg->has_records = true;
        /* desc->end_of_group is subgroup-header METADATA (this subgroup ends
         * its group), NOT a per-record terminal — more objects may follow on
         * the same subgroup. Sealing happens only at the actual graceful
         * stream terminal (moqr_log_seal_subgroup, driven by the session's
         * SUBGROUP_FINISHED). */
    }

    log->journal[log->next_seq % log->max_records] = (moqr_jslot_t){
        .group_id = desc->group_id,
        .sg_index = desc->datagram_pref ? MOQR_JSLOT_DATAGRAM : jslot_index,
        .rec_index = rec_index,
    };
    log->next_seq++;
    log->live_records++;
    log->retained_bytes += bytes;
    log->appended_total++;
    id_set_insert(log, g, desc->object_id);
    g->bytes += bytes;
    g->record_count++;
    g->last_arrival_us = desc->now_us;

    tr(log, MOQR_TRACE_APPEND, desc->datagram_pref ? 1u : 0u,
       desc->group_id,
       desc->datagram_pref ? UINT64_MAX : desc->subgroup_id,
       desc->object_id, bytes);

    /* End-of-group / end-of-track evidence (-18 :5145, :5289, :1054,
     * :1071). PER-RECORD final evidence is the END_OF_GROUP status object and
     * the EOG bit on a datagram (the datagram itself carries it). The
     * subgroup-header EOG bit is SUBGROUP metadata — the group ends at this
     * subgroup's LAST object, knowable only at the graceful terminal — so it
     * is recorded on the subgroup and resolved in moqr_log_seal_subgroup,
     * never treated as a terminal for the record that happens to carry it. */
    bool group_closed = false;
    if (desc->status == MOQR_OBJ_END_OF_GROUP ||
        (desc->end_of_group && desc->datagram_pref)) {
        if (g->final_object_id == MOQR_FINAL_UNKNOWN ||
            desc->object_id < g->final_object_id) {
            g->final_object_id = desc->object_id;
            group_closed = true;
        }
    } else if (desc->end_of_group && !desc->datagram_pref) {
        sg->end_of_group = true;
    }
    if (desc->status == MOQR_OBJ_END_OF_TRACK) {
        log->end_of_track = true;
        log->final_group_id = desc->group_id;
        log->final_track_object_id = desc->object_id;
        if (g->final_object_id == MOQR_FINAL_UNKNOWN) {
            g->final_object_id = desc->object_id;
        }
        group_closed = true;
    }
    if (group_closed) {
        tr(log, MOQR_TRACE_GROUP_CLOSE,
           desc->status == MOQR_OBJ_END_OF_TRACK ? 1u : 0u,
           g->group_id, g->final_object_id, g->record_count, log->trace_id);
    }

    return MOQR_OK;
}

/* -- chunk-through: OPEN records ------------------------------------------- */

moqr_result_t
moqr_log_open_record(moqr_log_t *log, const moqr_log_append_desc_t *desc)
{
    if (log == NULL || desc == NULL ||
        desc->struct_size < sizeof(moqr_log_append_desc_t)) {
        return MOQR_ERR_INVAL;
    }
    if (desc->obj_state != MOQR_OBJ_OPEN) {
        return MOQR_ERR_INVAL;   /* use moqr_log_append for whole objects */
    }
    /* append() admits the OPEN record (payload NULL, subgroup only, no EOG);
     * chunks are added by moqr_log_append_chunk. */
    return moqr_log_append(log, desc);
}

/* Resolve the OPEN record at (group, subgroup, object). Objects stream one at a
 * time (ascending), so the open record is the LAST in its subgroup. */
static moqr_result_t
resolve_open_rec(moqr_log_t *log, uint64_t group_id, uint64_t subgroup_id,
                 uint64_t object_id, moqr_group_t **out_g,
                 moqr_subgroup_t **out_sg, moqr_rec_t **out_rec)
{
    moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL) {
        /* Below the eviction horizon -> the group was evicted (TOO_OLD);
         * otherwise the caller never opened here (WRONG_STATE). */
        return (log->any_evicted && group_id <= log->max_evicted_group_id)
                   ? MOQR_ERR_TOO_OLD
                   : MOQR_ERR_WRONG_STATE;
    }
    moqr_subgroup_t *sg = NULL;
    for (uint32_t s = 0; s < g->sg_count; s++) {
        if (g->sgs[s].subgroup_id == subgroup_id) {
            sg = &g->sgs[s];
            break;
        }
    }
    if (sg == NULL || sg->v.count == 0) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_rec_t *rec = &sg->v.recs[sg->v.count - 1];
    if (rec->object_id != object_id || rec->obj_state != MOQR_OBJ_OPEN) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (out_g != NULL) *out_g = g;
    if (out_sg != NULL) *out_sg = sg;
    if (out_rec != NULL) *out_rec = rec;
    return MOQR_OK;
}

moqr_result_t
moqr_log_append_chunk(moqr_log_t *log, uint64_t group_id, uint64_t subgroup_id,
                      uint64_t object_id, moq_rcbuf_t *chunk)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_group_t *g = NULL;
    moqr_subgroup_t *sg = NULL;
    moqr_rec_t *rec = NULL;
    moqr_result_t rc =
        resolve_open_rec(log, group_id, subgroup_id, object_id, &g, &sg, &rec);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint64_t len = (uint64_t)moq_rcbuf_len(chunk);
    uint64_t props = (uint64_t)moq_rcbuf_len(rec->properties);
    uint64_t payload_so_far = rec->bytes - props;   /* rec->bytes = props+chunks */
    if (payload_so_far + len > rec->declared_len) {
        return MOQR_ERR_INVAL;   /* would exceed the declared payload length */
    }
    if (len == 0) {
        return MOQR_OK;   /* zero-length chunk: no node, no bytes, no state */
    }
    /* Byte budget: evict oldest groups (never this open record's group) until
     * the chunk fits. The record fits alone (open checked declared+props <=
     * max_bytes), so this only sheds older data; if this record's group is
     * itself the oldest and blocking, fail closed (the caller abandons). */
    while (log->retained_bytes + len > log->max_bytes) {
        if (log->group_count == 0 || &log->groups[0] == g) {
            return MOQR_ERR_CAPACITY;
        }
        evict_group_at(log, 0, /*reason=*/0);
        rc = resolve_open_rec(log, group_id, subgroup_id, object_id,
                              &g, &sg, &rec);
        if (rc != MOQR_OK) {
            return rc;   /* our own group was evicted -> TOO_OLD */
        }
        props = (uint64_t)moq_rcbuf_len(rec->properties);
    }
    uint32_t node = chunk_alloc(log);
    if (node == MOQR_CHUNK_NIL) {
        return MOQR_ERR_CAPACITY;   /* chunk-node pool exhausted */
    }
    moqr_chunk_node_t *n = &log->chunk_pool[node];
    n->buf = moq_rcbuf_incref(chunk);
    n->len = len;
    n->next = MOQR_CHUNK_NIL;
    if (rec->chunk_head == MOQR_CHUNK_NIL) {
        rec->chunk_head = node;
    } else {
        log->chunk_pool[rec->chunk_tail].next = node;
    }
    rec->chunk_tail = node;
    rec->chunk_count++;
    rec->bytes += len;
    log->retained_bytes += len;
    g->bytes += len;
    tr(log, MOQR_TRACE_APPEND, 2u /* chunk */, group_id, subgroup_id,
       object_id, len);
    return MOQR_OK;
}

moqr_result_t
moqr_log_complete_record(moqr_log_t *log, uint64_t group_id,
                         uint64_t subgroup_id, uint64_t object_id)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_rec_t *rec = NULL;
    moqr_result_t rc =
        resolve_open_rec(log, group_id, subgroup_id, object_id, NULL, NULL, &rec);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint64_t props = (uint64_t)moq_rcbuf_len(rec->properties);
    if (rec->bytes - props != rec->declared_len) {
        return MOQR_ERR_INVAL;   /* not fully received; stays OPEN */
    }
    rec->obj_state = MOQR_OBJ_COMPLETE;
    return MOQR_OK;
}

moqr_result_t
moqr_log_abandon_record(moqr_log_t *log, uint64_t group_id,
                        uint64_t subgroup_id, uint64_t object_id,
                        moqr_reset_desc_t reset)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_group_t *g = NULL;
    moqr_rec_t *rec = NULL;
    moqr_result_t rc =
        resolve_open_rec(log, group_id, subgroup_id, object_id, &g, NULL, &rec);
    if (rc != MOQR_OK) {
        return rc;
    }
    /* Release the partial payload chunks now (they will never be delivered),
     * dropping their bytes; keep the record as an ABANDONED tombstone so its
     * ascending object-id slot and identity persist for readers. */
    uint64_t props = (uint64_t)moq_rcbuf_len(rec->properties);
    uint64_t chunk_bytes = rec->bytes - props;
    chunk_list_release(log, rec);   /* decref slices, return nodes */
    log->retained_bytes -= chunk_bytes;
    g->bytes -= chunk_bytes;
    rec->bytes = props;
    rec->obj_state = MOQR_OBJ_ABANDONED;
    rec->reset = reset;
    return MOQR_OK;
}

/* -- cursors --------------------------------------------------------------- */

static moqr_cursor_slot_t *
cursor_resolve(moqr_log_t *log, moqr_cursor_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_CURSOR ||
        moq_handle_session_tag(h._opaque) != log->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= log->max_cursors) {
        return NULL;
    }
    moqr_cursor_slot_t *c = &log->cursors[slot];
    if (c->generation != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return c;
}

moqr_result_t
moqr_log_cursor_open(moqr_log_t *log, moqr_cursor_origin_t origin,
                     moqr_cursor_t *out)
{
    if (log == NULL || out == NULL ||
        (origin != MOQR_CURSOR_FROM_OLDEST &&
         origin != MOQR_CURSOR_FROM_LIVE)) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_CURSOR_INVALID;

    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < log->max_cursors; i++) {
        if ((log->cursors[i].generation & 1u) == 0) {
            slot = i;
            break;
        }
    }
    if (slot == UINT32_MAX) {
        return MOQR_ERR_CAPACITY;
    }

    moqr_cursor_slot_t *c = &log->cursors[slot];
    c->generation++;   /* even -> odd: live */
    c->skips = 0;
    c->peeked_seq = UINT64_MAX;
    c->seq = origin == MOQR_CURSOR_FROM_LIVE ? log->next_seq
                                             : log->begin_seq;

    uint64_t h = moq_handle_pack(MOQR_HANDLE_POOL_CURSOR, log->shard_tag,
                                 c->generation, (uint32_t)slot);
    if (h == 0) {
        c->generation++;   /* roll back to free (odd -> even) */
        return MOQR_ERR_INTERNAL;
    }
    out->_opaque = h;
    log->cursor_count++;
    tr(log, MOQR_TRACE_CURSOR_CREATE, (uint32_t)origin, slot, c->seq,
       log->trace_id, 0);
    return MOQR_OK;
}

moqr_result_t
moqr_log_cursor_close(moqr_log_t *log, moqr_cursor_t cursor)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    moqr_cursor_slot_t *c = cursor_resolve(log, cursor, &slot);
    if (c == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    tr(log, MOQR_TRACE_CURSOR_RETIRE, 0, slot, c->seq, c->skips,
       log->trace_id);
    c->generation++;   /* odd -> even: free */
    log->cursor_count--;
    return MOQR_OK;
}

/* Fill a borrowed view from a record (single source of truth for all readers,
 * incl. the chunk fields). Defined below; forward-declared for cursor_next. */
static void view_fill(moqr_record_view_t *out, const moqr_rec_t *rec);

moqr_result_t
moqr_log_cursor_next(moqr_log_t *log, moqr_cursor_t cursor,
                     moqr_record_view_t *out, bool *out_skipped)
{
    if (log == NULL || out == NULL || out_skipped == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out_skipped = false;

    uint32_t slot = 0;
    moqr_cursor_slot_t *c = cursor_resolve(log, cursor, &slot);
    if (c == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    c->peeked_seq = UINT64_MAX;   /* next() supersedes any outstanding peek */

    bool skipped = false;
    if (c->seq < log->begin_seq) {
        skipped = true;
        c->seq = log->begin_seq;
    }

    while (c->seq < log->next_seq) {
        const moqr_jslot_t *js = &log->journal[c->seq % log->max_records];
        const moqr_group_t *g = group_find(log, js->group_id, NULL);
        if (g == NULL) {
            /* Whole group evicted underneath us — group-boundary skip. */
            skipped = true;
            c->seq++;
            continue;
        }
        const moqr_rec_t *rec =
            js->sg_index == MOQR_JSLOT_DATAGRAM
                ? &g->dg.recs[js->rec_index]
                : &g->sgs[js->sg_index].v.recs[js->rec_index];

        view_fill(out, rec);

        c->seq = rec->arrival_seq + 1;
        if (skipped) {
            c->skips++;
            *out_skipped = true;
            tr(log, MOQR_TRACE_CURSOR_SKIP, 0, slot, rec->arrival_seq,
               c->skips, log->trace_id);
        }
        tr(log, MOQR_TRACE_CURSOR_ADVANCE, 0, slot, rec->group_id,
           rec->object_id, rec->arrival_seq);
        return MOQR_OK;
    }

    if (skipped) {
        c->skips++;
        *out_skipped = true;
        tr(log, MOQR_TRACE_CURSOR_SKIP, 1 /* to live edge */, slot, c->seq,
           c->skips, log->trace_id);
    }
    return MOQR_DONE;
}

static const moqr_rec_t *
jslot_resolve(const moqr_log_t *log, uint64_t seq)
{
    const moqr_jslot_t *js = &log->journal[seq % log->max_records];
    const moqr_group_t *g = group_find(log, js->group_id, NULL);
    if (g == NULL) {
        return NULL;
    }
    return js->sg_index == MOQR_JSLOT_DATAGRAM
               ? &g->dg.recs[js->rec_index]
               : &g->sgs[js->sg_index].v.recs[js->rec_index];
}

static void
view_fill(moqr_record_view_t *out, const moqr_rec_t *rec)
{
    out->arrival_seq = rec->arrival_seq;
    out->arrival_us = rec->arrival_us;
    out->group_id = rec->group_id;
    out->subgroup_id = rec->subgroup_id;
    out->object_id = rec->object_id;
    out->publisher_priority = rec->publisher_priority;
    out->status = rec->status;
    out->obj_state = rec->obj_state;
    out->datagram_pref = rec->datagram_pref;
    out->end_of_group = rec->end_of_group;
    out->payload = rec->payload;
    out->properties = rec->properties;
    out->chunk_count = rec->chunk_count;
    out->declared_len = rec->declared_len;
    out->_chunk_head = rec->chunk_head;
    out->reset = rec->reset;
}

moqr_result_t
moqr_log_view_chunk(const moqr_log_t *log, const moqr_record_view_t *view,
                    uint32_t idx, const moq_rcbuf_t **out_buf, uint64_t *out_len)
{
    if (log == NULL || view == NULL || out_buf == NULL || out_len == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (idx >= view->chunk_count) {
        return MOQR_DONE;
    }
    uint32_t node = view->_chunk_head;
    for (uint32_t i = 0; i < idx && node != MOQR_CHUNK_NIL; i++) {
        node = log->chunk_pool[node].next;
    }
    if (node == MOQR_CHUNK_NIL) {
        return MOQR_ERR_INVAL;   /* list shorter than chunk_count: corruption */
    }
    *out_buf = log->chunk_pool[node].buf;
    *out_len = log->chunk_pool[node].len;
    return MOQR_OK;
}

moqr_result_t
moqr_log_cursor_peek(moqr_log_t *log, moqr_cursor_t cursor,
                     moqr_record_view_t *out, bool *out_skipped)
{
    if (log == NULL || out == NULL || out_skipped == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out_skipped = false;

    uint32_t slot = 0;
    moqr_cursor_slot_t *c = cursor_resolve(log, cursor, &slot);
    if (c == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }

    /* Idempotent re-peek while the peeked record survives. */
    if (c->peeked_seq != UINT64_MAX && c->peeked_seq >= log->begin_seq) {
        const moqr_rec_t *rec = jslot_resolve(log, c->peeked_seq);
        if (rec != NULL) {
            view_fill(out, rec);
            return MOQR_OK;
        }
    }
    c->peeked_seq = UINT64_MAX;

    bool skipped = false;
    if (c->seq < log->begin_seq) {
        skipped = true;
        c->seq = log->begin_seq;
    }
    while (c->seq < log->next_seq) {
        const moqr_rec_t *rec = jslot_resolve(log, c->seq);
        if (rec == NULL) {
            skipped = true;
            c->seq++;
            continue;
        }
        c->peeked_seq = c->seq;
        view_fill(out, rec);
        if (skipped) {
            c->skips++;
            *out_skipped = true;
            tr(log, MOQR_TRACE_CURSOR_SKIP, 0, slot, rec->arrival_seq,
               c->skips, log->trace_id);
        }
        return MOQR_OK;
    }
    if (skipped) {
        c->skips++;
        *out_skipped = true;
        tr(log, MOQR_TRACE_CURSOR_SKIP, 1, slot, c->seq, c->skips,
           log->trace_id);
    }
    return MOQR_DONE;
}

moqr_result_t
moqr_log_cursor_commit(moqr_log_t *log, moqr_cursor_t cursor)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    moqr_cursor_slot_t *c = cursor_resolve(log, cursor, &slot);
    if (c == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (c->peeked_seq == UINT64_MAX) {
        return MOQR_ERR_WRONG_STATE;
    }
    tr(log, MOQR_TRACE_CURSOR_ADVANCE, 1 /* via commit */, slot,
       c->peeked_seq, c->skips, log->trace_id);
    c->seq = c->peeked_seq + 1;
    c->peeked_seq = UINT64_MAX;
    return MOQR_OK;
}

/* -- group-indexed reads ------------------------------------------------------ */

uint32_t
moqr_log_max_groups(const moqr_log_t *log)
{
    return log != NULL ? log->max_groups : 0;
}

uint32_t
moqr_log_max_subgroups(const moqr_log_t *log)
{
    return log != NULL ? log->max_subgroups : 0;
}

size_t
moqr_log_group_ids(const moqr_log_t *log, uint64_t *out, size_t cap)
{
    if (log == NULL || out == NULL) {
        return 0;
    }
    size_t n = log->group_count < cap ? log->group_count : cap;
    for (size_t i = 0; i < n; i++) {
        out[i] = log->groups[i].group_id;
    }
    return n;
}

uint32_t
moqr_log_group_count(const moqr_log_t *log)
{
    return log != NULL ? log->group_count : 0;
}

uint64_t
moqr_log_evicted_groups_total(const moqr_log_t *log)
{
    return log != NULL ? log->evicted_groups_total : 0;
}

uint64_t
moqr_log_group_id_at(const moqr_log_t *log, uint32_t idx)
{
    if (log == NULL || idx >= log->group_count) {
        return UINT64_MAX;
    }
    return log->groups[idx].group_id;
}

uint32_t
moqr_log_group_list_count(const moqr_log_t *log, uint64_t group_id)
{
    if (log == NULL) {
        return 0;
    }
    const moqr_group_t *g = group_find(log, group_id, NULL);
    return g != NULL ? g->sg_count : 0;
}

moqr_result_t
moqr_log_read_rec(const moqr_log_t *log, uint64_t group_id,
                  uint32_t list_slot, uint32_t rec_idx,
                  moqr_record_view_t *out)
{
    if (log == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    const moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL) {
        return log->any_evicted && group_id <= log->max_evicted_group_id
                   ? MOQR_ERR_TOO_OLD
                   : MOQR_DONE;
    }
    const moqr_rec_vec_t *v;
    if (list_slot == MOQR_LOG_LIST_DATAGRAM) {
        v = &g->dg;
    } else if (list_slot < g->sg_count) {
        v = &g->sgs[list_slot].v;
    } else {
        return MOQR_DONE;
    }
    if (rec_idx >= v->count) {
        return MOQR_DONE;
    }
    view_fill(out, &v->recs[rec_idx]);
    return MOQR_OK;
}

moqr_result_t
moqr_log_seal_subgroup(moqr_log_t *log, uint64_t group_id, uint64_t subgroup_id)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL) {
        /* Group already evicted / never admitted: the subgroup is gone, so its
         * downstream stream is closed by the eviction path, not by a seal. */
        return MOQR_DONE;
    }
    for (uint32_t s = 0; s < g->sg_count; s++) {
        if (g->sgs[s].subgroup_id == subgroup_id) {
            if (g->sgs[s].end_of_group && g->sgs[s].has_records &&
                (g->final_object_id == MOQR_FINAL_UNKNOWN ||
                 g->sgs[s].last_object_id < g->final_object_id)) {
                /* Header-EOG resolves here: the group's final object is this
                 * subgroup's last, known only now that the stream FIN'd.
                 * PREFLIGHT before committing: the final was unknown until
                 * this instant, so other lists may have admitted higher ids
                 * meanwhile — a cache must never retain an object beyond its
                 * declared final. A contradiction rejects the seal atomically
                 * (no sealed flag, no final, no trace); the caller treats it
                 * as malformed evidence and fails closed. */
                uint64_t final_q = g->sgs[s].last_object_id;
                if (group_retains_beyond(g, final_q)) {
                    return MOQR_ERR_INVAL;
                }
                g->sgs[s].sealed = true;
                g->final_object_id = final_q;
                tr(log, MOQR_TRACE_GROUP_CLOSE, 0u, g->group_id,
                   g->final_object_id, g->record_count, log->trace_id);
                return MOQR_OK;
            }
            g->sgs[s].sealed = true;
            return MOQR_OK;
        }
    }
    return MOQR_DONE;   /* no such subgroup list (e.g. empty-header FIN) */
}

moqr_result_t
moqr_log_reset_subgroup(moqr_log_t *log, uint64_t group_id,
                        uint64_t subgroup_id, moqr_reset_desc_t reset)
{
    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL) {
        /* Group already evicted / never admitted: the subgroup is gone and
         * the eviction path owes any downstream terminal, not this. */
        return MOQR_DONE;
    }
    for (uint32_t s = 0; s < g->sg_count; s++) {
        if (g->sgs[s].subgroup_id == subgroup_id) {
            /* A reset terminal never resolves header-EOG finality: a
             * truncated stream cannot testify to the group's final object,
             * so final_object_id stays as it was. First terminal wins —
             * a list already sealed (either flavor) keeps its flavor and
             * code, so replays cannot rewrite the recorded cause. */
            if (!g->sgs[s].sealed) {
                g->sgs[s].sealed = true;
                g->sgs[s].sealed_reset = true;
                g->sgs[s].reset = reset;
            }
            return MOQR_OK;
        }
    }
    return MOQR_DONE;   /* no such subgroup list (e.g. reset before data) */
}

bool
moqr_log_subgroup_is_final(const moqr_log_t *log, uint64_t group_id,
                           uint32_t list_slot, uint64_t object_id)
{
    if (log == NULL || list_slot == MOQR_LOG_LIST_DATAGRAM) {
        return false;   /* datagrams have no subgroup stream to close */
    }
    const moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL || list_slot >= g->sg_count) {
        return false;
    }
    const moqr_subgroup_t *sg = &g->sgs[list_slot];
    /* Final iff the subgroup is sealed (upstream FIN, no more objects) and this
     * is its last RETAINED record. Object IDs ascend strictly within a subgroup
     * and eviction only trims the front, so the last retained record carries
     * last_object_id. */
    /* A reset-terminal list never reports final: the clean-FIN advisory on
     * record delivery must not fire for an abnormal end — its downstream
     * terminal is the reset-flavored seal notice, nothing else. */
    return sg->sealed && !sg->sealed_reset && sg->v.count > 0 &&
           object_id == sg->last_object_id;
}

bool
moqr_log_subgroup_sealed(const moqr_log_t *log, uint64_t group_id,
                         uint32_t list_slot, uint64_t *subgroup_id)
{
    if (log == NULL || list_slot == MOQR_LOG_LIST_DATAGRAM) {
        return false;
    }
    const moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL || list_slot >= g->sg_count || !g->sgs[list_slot].sealed) {
        return false;
    }
    if (subgroup_id != NULL) {
        *subgroup_id = g->sgs[list_slot].subgroup_id;
    }
    return true;
}

bool
moqr_log_subgroup_reset(const moqr_log_t *log, uint64_t group_id,
                        uint32_t list_slot, moqr_reset_desc_t *out_reset)
{
    if (log == NULL || list_slot == MOQR_LOG_LIST_DATAGRAM) {
        return false;
    }
    const moqr_group_t *g = group_find(log, group_id, NULL);
    if (g == NULL || list_slot >= g->sg_count ||
        !g->sgs[list_slot].sealed_reset) {
        return false;
    }
    if (out_reset != NULL) {
        *out_reset = g->sgs[list_slot].reset;
    }
    return true;
}

/* -- stats ------------------------------------------------------------------ */

void
moqr_log_get_stats(const moqr_log_t *log, moqr_log_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (log == NULL) {
        return;
    }
    out->retained_bytes = log->retained_bytes;
    out->group_count = log->group_count;
    out->record_count = log->live_records;
    out->cursor_count = log->cursor_count;
    out->appended_total = log->appended_total;
    out->evicted_groups_total = log->evicted_groups_total;
    out->evicted_records_total = log->evicted_records_total;
    out->oldest_group_id = log->group_count > 0 ? log->groups[0].group_id
                                                : UINT64_MAX;
    out->newest_group_id =
        log->group_count > 0 ? log->groups[log->group_count - 1].group_id
                             : UINT64_MAX;
    out->end_of_track = log->end_of_track;
}
