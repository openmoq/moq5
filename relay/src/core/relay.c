#include "relay_internal.h"
#include "auth_internal.h"
#include "bindings_internal.h"
#include "delivery_internal.h"
#include "fetch_internal.h"
#include "intents_internal.h"
#include "namespace_internal.h"
#include "tracks_internal.h"

/* Default WARM-track idle retention. Longer than common QUIC idle timeouts
 * (msquic 30 s), so a subscriber whose connection died silently and rejoins
 * after detecting it still finds the cached objects; short enough that a
 * relay whose sessions come and go never holds dead content (each WARM track
 * retains up to the per-track log byte budget) for long. */
#define R_DEF_WARM_IDLE_US (60u * 1000u * 1000u)

#define R_RECLAIM_IDLE     2u


/* A terminal this relay chose is a meaning, not a draft's number: the
 * subscriber that reads it may speak either draft. */
moqr_pd_desc_t
core_local_done(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}
#define R_ERR_UNAUTHORIZED   0x1u    /* mirrors MOQ_REQUEST_ERROR_UNAUTHORIZED  */

#define R_ERR_TIMEOUT        0x2u    /* mirrors MOQ_REQUEST_ERROR_TIMEOUT */


/* The single-shard handle tag: a core with shard_index 0 packs this, keeping
 * the default build bit-identical. Per-core, the tag is shard_index + 1 (stored
 * on moqr_core_t) and the resolvers require the core's own tag, so a handle
 * repacked with a foreign shard tag is structurally refused. */
#define R_SHARD_TAG          1u



uint32_t
key_rd32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}


void
key_wr32(uint8_t *p, uint32_t v)
{
    memcpy(p, &v, sizeof(v));
}


uint32_t
key_part_count(const r_key_t *k)
{
    return key_rd32(k->buf);
}


moq_bytes_t
key_part(const r_key_t *k, uint32_t i)
{
    uint32_t count = key_rd32(k->buf);
    uint32_t off = 4u * (2u + count);
    for (uint32_t p = 0; p < i; p++) {
        off += key_rd32(k->buf + 4u * (1u + p));
    }
    return (moq_bytes_t){ .data = k->buf + off,
                          .len = key_rd32(k->buf + 4u * (1u + i)) };
}


moq_bytes_t
key_name(const r_key_t *k)
{
    uint32_t count = key_rd32(k->buf);
    uint32_t off = 4u * (2u + count);
    for (uint32_t p = 0; p < count; p++) {
        off += key_rd32(k->buf + 4u * (1u + p));
    }
    return (moq_bytes_t){ .data = k->buf + off,
                          .len = key_rd32(k->buf + 4u * (1u + count)) };
}

/* A free slot that can still take a live generation. */
bool
r_gen_openable(uint32_t gen)
{
    return (gen & 1u) == 0 && gen < R_GEN_LAST_LIVE;
}


/* Free -> live. Precondition: r_gen_openable(gen). */
uint32_t
r_gen_open(uint32_t gen)
{
    return gen + 1u;
}


/* Live -> free; the last live generation becomes R_GEN_RETIRED. */
uint32_t
r_gen_close(uint32_t gen)
{
    return gen + 1u;
}


/* Whether a live slot can move straight to a fresh live generation (keeping
 * the slot, staling every handle to the old one). */
bool
r_gen_renewable(uint32_t gen)
{
    return (gen & 1u) != 0 && gen <= R_GEN_LAST_LIVE - 2u;
}


/* Live -> the next live generation. Precondition: r_gen_renewable(gen). */
uint32_t
r_gen_renew(uint32_t gen)
{
    return gen + 2u;
}


/* Whether releasing a live slot leaves it openable (not retired). */
bool
r_gen_reopens(uint32_t gen)
{
    return (gen & 1u) != 0 && r_gen_openable(r_gen_close(gen));
}


void *
r_alloc(moqr_core_t *c, size_t n)
{
    return c->alloc.alloc(n, c->alloc.ctx);
}


void
r_free(moqr_core_t *c, void *p, size_t n)
{
    if (p != NULL) {
        c->alloc.free(p, n, c->alloc.ctx);
    }
}


void
r_trace(moqr_core_t *c, moqr_trace_kind_t kind, uint32_t detail,
        uint64_t e0, uint64_t e1, uint64_t e2, uint64_t e3)
{
    if (c->trace == NULL) {
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
    moqr_trace_emit(c->trace, rec);
}


/* Push the current epoch triple onto the trace ring so subsequent records name
 * their generation. node/shard epochs are preserved from the core (control-plane
 * generations); route_epoch is the core's per-mutation counter. */
static void
r_publish_epochs(moqr_core_t *c)
{
    if (c->trace == NULL) {
        return;
    }
    moqr_epochs_t e = MOQR_EPOCHS_INIT;
    e.node_epoch = c->node_epoch;
    e.shard_epoch = c->shard_epoch;
    e.route_epoch = c->route_epoch;
    moqr_trace_set_epochs(c->trace, e);
}


void
r_bump_route_epoch(moqr_core_t *c)
{
    c->route_epoch++;
    r_publish_epochs(c);
}


/* Admission room may have appeared: a pool released capacity (a track slot,
 * a trie node, interned name bytes) or capacity became reclaimable (an
 * announce withdrawn, so its trie path may be freed; a track left with no
 * source, subscription or fetch). Never moved by a change that only consumes
 * room, so a caller waiting for room (moqr_core_room_epoch) retries only
 * when it could succeed. */
void
r_room_changed(moqr_core_t *c)
{
    c->room_epoch++;
}


void
moqr_core_set_epochs(moqr_core_t *c, uint64_t node_epoch, uint64_t shard_epoch)
{
    if (c == NULL) {
        return;
    }
    c->node_epoch = node_epoch;
    c->shard_epoch = shard_epoch;
    r_publish_epochs(c);
}


/* handle helpers */

uint64_t
r_pack(const moqr_core_t *c, uint32_t pool, uint32_t gen, uint32_t slot)
{
    return moq_handle_pack(pool, c->shard_tag, gen, slot);
}

void
moqr_core_relay_cfg_init_sized(moqr_core_relay_cfg_t *cfg, size_t cfg_size,
                               const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_core_relay_cfg_t)
                   ? cfg_size
                   : sizeof(moqr_core_relay_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_core_relay_cfg_t, alloc) +
            sizeof(((moqr_core_relay_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
}


/* Retained request-token slots: one per namespace subscription, track
 * subscription and announced namespace. */
static uint32_t
r_rtok_slots(uint32_t max_ns_subs, uint32_t max_ns_nodes)
{
    uint64_t n = 2u * (uint64_t)max_ns_subs + max_ns_nodes;
    return n > UINT32_MAX ? UINT32_MAX : (uint32_t)n;
}


static moqr_result_t core_create_cluster(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_t **out);


moqr_result_t
moqr_core_create(const moqr_core_relay_cfg_t *cfg, moqr_core_t **out)
{
    return core_create_cluster(cfg, NULL, out);
}


void
moqr_cluster_cfg_init_sized(moqr_cluster_cfg_t *cfg, size_t size)
{
    if (cfg == NULL || size < sizeof(uint32_t)) return;
    moqr_cluster_cfg_t value = {0};
    moqr_cluster_write(cfg, size, &value, sizeof(value));
}


moq_result_t
moqr_cluster_cfg_resolve(const moqr_cluster_cfg_t *cfg, moqr_cluster_info_t *out,
                          size_t out_size)
{
    if (out == NULL || out_size < MOQR_CLUSTER_INFO_V0_SIZE) return MOQ_ERR_INVAL;
    moqr_cluster_cfg_t input = {0};
    if (cfg != NULL && !moqr_cluster_read(cfg, MOQR_CLUSTER_CFG_V0_SIZE,
                                         &input, sizeof(input))) return MOQ_ERR_INVAL;
    moqr_cluster_info_t value = {0};
    value.enabled = input.enabled;
    if (value.enabled) {
        value.local_hop_id = input.local_hop_id;
        value.max_routes = input.max_routes ? input.max_routes : 256u;
        value.max_hops = input.max_hops ? input.max_hops : 32u;
        if ((uint64_t)value.max_routes * value.max_hops > SIZE_MAX / sizeof(uint64_t))
            return MOQ_ERR_INVAL;
#if SIZE_MAX < UINT64_MAX
        if ((uint64_t)value.max_routes > SIZE_MAX / sizeof(r_cluster_route_t))
            return MOQ_ERR_INVAL;
#endif
    }
    moqr_cluster_write(out, out_size, &value, sizeof(value));
    return MOQ_OK;
}


moq_result_t
moqr_core_create_cluster(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_t **out)
{
    if (out == NULL) return MOQ_ERR_INVAL;
    moqr_core_t *created = NULL;
    moqr_result_t rc = core_create_cluster(cfg, cluster, &created);
    if (rc == MOQR_OK) *out = created;
    return moqr_result_public(rc);
}


static moqr_result_t
core_create_cluster(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    moqr_cluster_info_t cluster_resolved;
    if (moqr_cluster_cfg_resolve(cluster, &cluster_resolved, sizeof(cluster_resolved)) != MOQ_OK)
        return MOQR_ERR_INVAL;
    const moq_alloc_t *a = RELAY_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL) {
        return MOQR_ERR_INVAL;
    }

    /* Shard identity (single-shard default: index 0 / count 1). Validate before
     * allocating anything: index must be in range and the tag (index + 1)
     * representable in the 16-bit handle tag field. */
    uint32_t si = RELAY_CFG_HAS(cfg, shard_index) ? cfg->shard_index : 0;
    uint32_t sc = (RELAY_CFG_HAS(cfg, shard_count) && cfg->shard_count != 0)
                      ? cfg->shard_count
                      : 1;
    if (si >= sc || si + 1u > 0xFFFFu) {
        return MOQR_ERR_INVAL;
    }

    /* The track-discovery offer matrix multiplies two limits; one whose
     * element or byte count size_t cannot represent is refused before
     * anything is allocated (on 32-bit targets ordinary limits reach it). */
    {
        moqr_core_limits_t lim0;
        (void)moqr_core_limits_resolve(cfg, &lim0);
        uint64_t offers = (uint64_t)lim0.max_ns_subs * lim0.max_tracks;
        if (offers > SIZE_MAX / sizeof(r_offer_t)) {
            return MOQR_ERR_INVAL;
        }
    }

    moqr_core_t *c = a->alloc(sizeof(*c), a->ctx);
    if (c == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(c, 0, sizeof(*c));
    c->alloc = *a;
    c->trace = RELAY_CFG_HAS(cfg, trace) ? cfg->trace : NULL;
    c->shard_index = (uint16_t)si;
    c->shard_count = (uint16_t)sc;
    c->shard_tag = (uint16_t)(si + 1u);
    /* node/shard epochs stay 0 until a control input sets them (memset above). */

    /* Single-source resolution: create consumes the SAME pure resolver the
     * capacity describes use, so create and describe can never disagree. */
    moqr_core_limits_t lim;
    (void)moqr_core_limits_resolve(cfg, &lim);
    c->max_bindings = lim.max_bindings;
    c->max_tracks = lim.max_tracks;
    c->max_subs = lim.max_subs;
    c->max_ns_nodes = lim.max_ns_nodes;
    c->max_ns_subs = lim.max_ns_subs;
    c->max_intents = lim.max_intents;   /* atomicity clamp already folded */
    c->intern_budget = lim.name_intern_bytes;
    c->max_parked = lim.max_parked;
    c->parked_bytes_cap = lim.parked_bytes;
    c->max_grants = lim.max_grants;
    c->grant_bytes_cap = lim.grant_bytes;
    c->max_fetches = lim.max_fetches;
    c->max_cancels = lim.max_cancels;
    c->fetch_pin_cap = lim.fetch_pin_bytes;
    c->cluster_enabled = cluster_resolved.enabled;
    c->cluster_hop_id = cluster_resolved.local_hop_id;
    c->max_cluster_routes = cluster_resolved.max_routes;
    c->max_cluster_hops = cluster_resolved.max_hops;
    if (c->cluster_enabled) {
        uint64_t atomic = (uint64_t)c->max_subs + 2u * (uint64_t)c->max_tracks + c->max_ns_subs;
        if (atomic > UINT32_MAX) { moqr_core_destroy(c); return MOQR_ERR_INVAL; }
        if (c->max_intents < atomic) c->max_intents = (uint32_t)atomic;
    }
    /* The intent-ring atomicity clamp is folded into
     * moqr_core_limits_resolve (single source with the capacity describes);
     * lim.max_intents above already carries it. */
    if (RELAY_CFG_HAS(cfg, log_budget)) {
        c->log_budget = cfg->log_budget;
    }
    c->log_max_subgroups =
        RELAY_CFG_HAS(cfg, log_max_subgroups) ? cfg->log_max_subgroups : 0;
    c->log_max_objects = RELAY_CFG_HAS(cfg, log_max_objects_per_group)
                             ? cfg->log_max_objects_per_group
                             : 0;
    c->log_max_cursors =
        RELAY_CFG_HAS(cfg, log_max_cursors) ? cfg->log_max_cursors : 0;
    c->log_max_chunk_nodes =
        RELAY_CFG_HAS(cfg, log_max_chunk_nodes) ? cfg->log_max_chunk_nodes : 0;
    /* gpos layout representability: lists = subgroups + the datagram list,
     * and the per-(sub, group) stride, must both fit the runtime's 32-bit
     * fields. Same formula the capacity describe checks. */
    {
        uint64_t lists =
            (uint64_t)(c->log_max_subgroups != 0 ? c->log_max_subgroups
                                                 : 16u) +
            1u;
        uint64_t groups = (uint64_t)(c->log_budget.max_groups != 0
                                         ? c->log_budget.max_groups
                                         : 8u);
        if (lists > UINT32_MAX || gpos_stride_for(lists) > UINT32_MAX ||
            gpos_hash_cap_for(groups) > UINT32_MAX) {
            moqr_core_destroy(c);
            return MOQR_ERR_INVAL;
        }
    }
    c->linger_us = RELAY_CFG_HAS(cfg, linger_us) ? cfg->linger_us : 0;
    c->warm_idle_us = RELAY_CFG_HAS(cfg, warm_idle_us) && cfg->warm_idle_us != 0
                          ? cfg->warm_idle_us
                          : R_DEF_WARM_IDLE_US;
    c->authorize = RELAY_CFG_HAS(cfg, authorize) ? cfg->authorize : NULL;
    c->authorize_ctx =
        RELAY_CFG_HAS(cfg, authorize_ctx) ? cfg->authorize_ctx : NULL;

    /* Floor the fetch pin budget to the effective per-track log payload budget
     * (the largest a single retained record can be, log.c rejects bigger), so a
     * lone object always fits an idle budget: forward progress is guaranteed and
     * a fetch can never wait forever on the pin budget. */
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), &c->alloc);
        lc.budget = c->log_budget;
        lc.max_subgroups_per_group = c->log_max_subgroups;
        lc.max_objects_per_group = c->log_max_objects;
        lc.max_cursors = c->log_max_cursors;
        lc.max_chunk_nodes = c->log_max_chunk_nodes;
        moqr_log_capacity_t logc;
        if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
            /* Create/describe parity: a config whose per-track log model
             * wraps is refused HERE too — never a zero floor. */
            moqr_core_destroy(c);
            return MOQR_ERR_INVAL;
        }
        if (c->fetch_pin_cap < logc.payload_bytes) {
            c->fetch_pin_cap = logc.payload_bytes;
        }
    }

    /* Each pool is zeroed the moment it exists: a partial-failure destroy
     * must never walk uninitialized entries (pins, generations). */
#define R_POOL_ALLOC(field, count)                                        \
    do {                                                                  \
        uint64_t n_ = (uint64_t)(count);                                  \
        c->field = n_ <= SIZE_MAX / sizeof(*c->field)                     \
            ? r_alloc(c, (size_t)n_ * sizeof(*c->field)) : NULL;          \
        if (c->field != NULL) {                                           \
            memset(c->field, 0, (size_t)n_ * sizeof(*c->field));          \
        }                                                                 \
    } while (0)
    if (c->cluster_enabled) {
        R_POOL_ALLOC(cluster_routes, c->max_cluster_routes);
        R_POOL_ALLOC(cluster_hops, (uint64_t)c->max_cluster_routes * c->max_cluster_hops);
    }
    R_POOL_ALLOC(bindings, c->max_bindings);
    R_POOL_ALLOC(tracks, c->max_tracks);
    R_POOL_ALLOC(subs, c->max_subs);
    R_POOL_ALLOC(nodes, c->max_ns_nodes);
    R_POOL_ALLOC(ns_subs, c->max_ns_subs);
    R_POOL_ALLOC(tsubs, c->max_ns_subs);
    R_POOL_ALLOC(offers, (uint64_t)c->max_ns_subs * c->max_tracks);
    c->max_rtoks = r_rtok_slots(c->max_ns_subs, c->max_ns_nodes);
    R_POOL_ALLOC(rtoks, c->max_rtoks);
    R_POOL_ALLOC(intents, c->max_intents);
    R_POOL_ALLOC(parked, c->max_parked);
    R_POOL_ALLOC(grants, c->max_grants);
    R_POOL_ALLOC(fetches, c->max_fetches);
    R_POOL_ALLOC(pending_cancels, c->max_cancels);
#undef R_POOL_ALLOC
    /* Binding-ready bitset: ceil(max_bindings/64) words, zeroed (empty). */
    {
        size_t rw = (size_t)ready_word_count(c->max_bindings) *
                    sizeof(uint64_t);
        c->ready_words = r_alloc(c, rw);
        if (c->ready_words != NULL) {
            memset(c->ready_words, 0, rw);
        }
    }
    if ((c->cluster_enabled && (c->cluster_routes == NULL || c->cluster_hops == NULL)) ||
        c->bindings == NULL || c->tracks == NULL || c->subs == NULL ||
        c->ready_words == NULL ||
        c->nodes == NULL || c->ns_subs == NULL || c->intents == NULL ||
        c->tsubs == NULL || c->offers == NULL || c->rtoks == NULL ||
        (c->max_parked != 0 && c->parked == NULL) ||
        (c->max_grants != 0 && c->grants == NULL) ||
        (c->max_cancels != 0 && c->pending_cancels == NULL) ||
        (c->max_fetches != 0 && c->fetches == NULL)) {
        moqr_core_destroy(c);
        return MOQR_ERR_NOMEM;
    }

    /* Root trie node. */
    c->nodes[0].parent = UINT32_MAX;
    c->nodes[0].first_child = R_NODE_NIL;
    c->nodes[0].last_child = R_NODE_NIL;
    c->nodes[0].next_sib = R_NODE_NIL;
    c->nodes[0].prev_sib = R_NODE_NIL;
    c->node_count = 1;

    *out = c;
    return MOQR_OK;
}


void
moqr_core_destroy(moqr_core_t *c)
{
    if (c == NULL) {
        return;
    }
    moq_alloc_t a = c->alloc;
    r_free(c, c->cluster_routes, (size_t)c->max_cluster_routes * sizeof(*c->cluster_routes));
    r_free(c, c->cluster_hops, (size_t)c->max_cluster_routes * c->max_cluster_hops * sizeof(uint64_t));
    c->cluster_routes = NULL;
    c->cluster_hops = NULL;
    if (c->tracks != NULL) {
        for (uint32_t i = 0; i < c->max_tracks; i++) {
            if ((c->tracks[i].gen & 1u) != 0) {
                track_free_slot(c, i);
            }
        }
        a.free(c->tracks, (size_t)c->max_tracks * sizeof(*c->tracks), a.ctx);
    }
    if (c->ready_words != NULL) {
        a.free(c->ready_words,
               (size_t)ready_word_count(c->max_bindings) * sizeof(uint64_t),
               a.ctx);
    }
    if (c->nodes != NULL) {
        for (uint32_t i = 0; i < c->node_count; i++) {
            r_free(c, c->nodes[i].part, c->nodes[i].part_len);
        }
        a.free(c->nodes, (size_t)c->max_ns_nodes * sizeof(*c->nodes), a.ctx);
    }
    if (c->ns_subs != NULL) {
        for (uint32_t i = 0; i < c->max_ns_subs; i++) {
            r_free(c, c->ns_subs[i].prefix, c->ns_subs[i].prefix_len);
        }
        a.free(c->ns_subs, (size_t)c->max_ns_subs * sizeof(*c->ns_subs),
               a.ctx);
    }
    if (c->tsubs != NULL) {
        for (uint32_t i = 0; i < c->max_ns_subs; i++) {
            r_free(c, c->tsubs[i].prefix, c->tsubs[i].prefix_len);
        }
        a.free(c->tsubs, (size_t)c->max_ns_subs * sizeof(*c->tsubs), a.ctx);
    }
    if (c->offers != NULL) {
        a.free(c->offers, (size_t)c->max_ns_subs * c->max_tracks *
                              sizeof(*c->offers), a.ctx);
    }
    if (c->rtoks != NULL) {
        for (uint32_t i = 0; i < c->max_rtoks; i++) {
            if (c->rtoks[i].used) {
                r_free_material(c, &c->rtoks[i].mat, &c->intern_used);
            }
        }
        a.free(c->rtoks, (size_t)c->max_rtoks * sizeof(*c->rtoks), a.ctx);
    }
    if (c->bindings != NULL) {
        for (uint32_t i = 0; i < c->max_bindings; i++) {
            binding_unpin(&c->bindings[i]);
            r_free(c, c->bindings[i].pin_chunks,
                   (size_t)c->bindings[i].pin_chunks_cap *
                       sizeof(r_pin_chunk_t));
        }
        a.free(c->bindings, (size_t)c->max_bindings * sizeof(*c->bindings),
               a.ctx);
    }
    if (c->subs != NULL) {
        for (uint32_t i = 0; i < c->max_subs; i++) {
            if ((c->subs[i].gen & 1u) != 0) {
                gpos_free(c, &c->subs[i]);
            }
        }
        a.free(c->subs, (size_t)c->max_subs * sizeof(*c->subs), a.ctx);
    }
    if (c->intents != NULL) {
        a.free(c->intents, (size_t)c->max_intents * sizeof(*c->intents),
               a.ctx);
    }
    if (c->parked != NULL) {
        for (uint32_t i = 0; i < c->max_parked; i++) {
            if ((c->parked[i].gen & 1u) != 0) {
                parked_free_slot(c, i);
            }
        }
        a.free(c->parked, (size_t)c->max_parked * sizeof(*c->parked), a.ctx);
    }
    if (c->grants != NULL) {
        for (uint32_t i = 0; i < c->max_grants; i++) {
            if ((c->grants[i].gen & 1u) != 0) {
                grant_free_slot(c, i);
            }
        }
        a.free(c->grants, (size_t)c->max_grants * sizeof(*c->grants), a.ctx);
    }
    if (c->fetches != NULL) {
        for (uint32_t i = 0; i < c->max_fetches; i++) {
            if ((c->fetches[i].gen & 1u) != 0) {
                fetch_unpin(c, &c->fetches[i]);
            }
        }
        a.free(c->fetches, (size_t)c->max_fetches * sizeof(*c->fetches), a.ctx);
    }
    if (c->pending_cancels != NULL) {
        a.free(c->pending_cancels,
               (size_t)c->max_cancels * sizeof(*c->pending_cancels), a.ctx);
    }
    a.free(c, sizeof(*c), a.ctx);
}


/* A PUBLISH supplies the source held rendezvous subscriptions wait for
 * (§7.6, §9.20.6): the track becomes an ACTIVE pushed track, the PUBLISH is
 * accepted, and each held SUBSCRIBE proceeds normally -- accepted from the
 * new source -- while that subscriber's track subscriptions are not also
 * offered this PUBLISH. One intent per held subscription plus the accept,
 * reserved first. */
bool rendezvous_timeout_owed(const moqr_core_t *c, uint32_t tslot);


/* -- grant revalidation (driven off tick) --------------------------------------------------------- */

/* Re-check request built from a grant's COPIED material (the original session
 * event is long gone), for the revalidation hook. */
static moqr_auth_request_t
grant_auth_req(const r_grant *g, uint64_t now_us)
{
    moqr_auth_request_t req;
    memset(&req, 0, sizeof(req));
    req.struct_size = (uint32_t)sizeof(req);
    req.action = g->action;
    req.ns.parts = g->mat.ns_parts;
    req.ns.count = g->mat.ns_count;
    req.name.data = g->mat.name;
    req.name.len = g->mat.name_len;
    req.tokens = g->mat.tokens;
    req.token_count = g->mat.token_count;
    req.binding_cookie = g->binding_cookie;
    req.now_us = now_us;
    return req;
}


/* Execute (or continue) the teardown of a revoked grant. Subscribe revocation
 * is fully core-driven (revoke_sub -> SUB_DONE intent); announce revocation
 * is a FULL withdrawal — a namespace whose authorization was revoked must not
 * keep sourcing tracks, so the teardown runs moqr_core_force_withdraw: every
 * sourced track is purged (subscriber terminals carry the revoke status,
 * which is what terminates a remote demand's pump-sub too), the announce
 * clears with NS_GONE fan-out, and the binding then sends the publisher-side
 * cancel_namespace (peek/ack_revoked_grant). The grant itself is the cancel
 * vehicle: it is marked unannounced BEFORE the withdrawal so force_withdraw's
 * pending-cancel dedupe leaves the cancel to this grant's own peek/ack.
 * Retryable: WOULD_BLOCK (intent ring / cancel queue) leaves the grant marked
 * for the next tick. The withdrawal itself is resumable, so some of the
 * grant's tracks may already be purged when that happens; re-arming the cancel
 * vehicle is safe because the retry resumes from the tracks that remain and
 * performs the final namespace effects exactly once, at the end. */
static void
grant_do_teardown(moqr_core_t *c, uint32_t slot, uint64_t now_us)
{
    r_grant *g = &c->grants[slot];
    if (g->action == MOQR_AUTH_SUBSCRIBE) {
        moqr_result_t rc = moqr_core_revoke_sub(c, (moqr_sub_t){ g->sub_raw },
                                                g->revoke_pd, now_us);
        if (rc != MOQR_ERR_WOULD_BLOCK) {
            grant_free_slot(c, slot); /* SUB_DONE queued, or the sub was gone */
        }
        return; /* WOULD_BLOCK: g->revoked stays set, retry next tick */
    }
    /* announce */
    if (!g->unannounced) {
        moqr_ns_t ns = { g->mat.ns_parts, g->mat.ns_count };
        /* Ownership guard: this grant may be STALE — its announce already
         * withdrawn and the key re-announced by a DIFFERENT publisher.
         * Only the exact announce this grant authorized ({binding_cookie,
         * session_cookie}) may be withdrawn; anything else retires the
         * stale grant and touches NOTHING — never a replacement
         * publisher's route, tracks, or cancellation state. */
        uint32_t node = trie_walk(c, ns);
        if (c->cluster_enabled && node != UINT32_MAX) {
            for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
                const r_cluster_route_t *r = &c->cluster_routes[ri];
                if (r->used && r->node == node &&
                    c->bindings[r->binding].cookie == g->binding_cookie &&
                    r->session_cookie == g->session_cookie) {
                    cluster_project(c, node, ri);
                    break;
                }
            }
        }
        if (node == UINT32_MAX || !c->nodes[node].has_announce ||
            c->bindings[c->nodes[node].ann_binding].cookie !=
                g->binding_cookie ||
            c->nodes[node].session_cookie != g->session_cookie) {
            grant_free_slot(c, slot);
            return;
        }
        g->unannounced = true;   /* the cancel vehicle: dedupes the
                                  * withdrawal's own pending-cancel slot */
        moqr_result_t rc = moqr_core_force_withdraw(c, ns, g->revoke_request_error,
                                                    now_us);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            g->unannounced = false;   /* re-arm the vehicle; retry next tick */
            return;
        }
    }
    /* Now waiting for the binding's cancel_namespace via peek/ack_revoked_grant. */
}


static void
grant_tick(moqr_core_t *c, uint64_t now_us)
{
    for (uint32_t i = 0; i < c->max_grants; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) == 0 || !g->committed) {
            continue; /* free, or reserved-but-not-committed */
        }
        if (g->revoked) {
            grant_do_teardown(c, i, now_us); /* retry pending teardown */
            continue;
        }
        if (g->next_recheck_us > now_us) {
            continue; /* not due */
        }
        moqr_auth_request_t req = grant_auth_req(g, now_us);
        moqr_auth_verdict_t v;
        moqr_core_authorize(c, &req, &v); /* counts + traces the reval decision */
        if (v.decision == MOQR_AUTH_ALLOW && v.revalidate_after_us > 0) {
            g->lease_us = v.revalidate_after_us; /* renew the lease */
            g->next_recheck_us = now_us > UINT64_MAX - g->lease_us
                                     ? UINT64_MAX
                                     : now_us + g->lease_us;
            continue;
        }
        if (v.decision == MOQR_AUTH_ALLOW) {
            grant_free_slot(c, i); /* lease 0: stop revalidating */
            continue;
        }
        /* DENY / DEFER / garbage -> revoke, fail closed. moqr_core_authorize
         * canonicalizes decision/reason but NOT error_code, so a DENY/DEFER
         * (or garbage) verdict may carry error_code == 0. A zero code would
         * ride out as a normal "range complete" / clean cancel on the wire, so
         * default it to UNAUTHORIZED while preserving any custom DENY code. */
        /* Build the terminal before anything is marked revoked: a denial we
         * cannot state truthfully must not leave a half-torn-down grant.
         *
         * The terminal comes from the verdict's TAGGED field, never from
         * `error_code`: that scalar is REQUEST_ERROR-domain, and reading it as
         * a PUBLISH_DONE status would be exactly the numeric coincidence the
         * tagged descriptors exist to prevent. NONE or an invalid descriptor
         * degrades to the truthful UNAUTHORIZED meaning. */
        moqr_pd_desc_t rpd = v.revoke_terminal;

        if (!moqr_pd_desc_valid(rpd) &&
            moqr_pd_desc_local(MOQR_PD_UNAUTHORIZED, &rpd) != MOQR_OK) {
            continue;   /* cannot state a terminal: leave the grant alone */
        }
        g->revoked = true;
        g->revoke_request_error =
            v.error_code != 0 ? v.error_code : R_ERR_UNAUTHORIZED;
        g->revoke_pd = rpd;
        grant_do_teardown(c, i, now_us);
    }
}


/* -- tick ------------------------------------------------------------------------------------------------ */

/* Every held rendezvous whose deadline has passed at `now_us` ends with
 * TIMEOUT (§9.20.6) -- before any source that arrives at that time could
 * resume it. A TIMEOUT the intent ring cannot take now stays owed
 * (rendezvous_expired) and is retried here. */
static void
rendezvous_expire(moqr_core_t *c, uint64_t now_us)
{
    for (uint32_t k = 0; k < c->max_subs; k++) {
        r_sub_t *sub = &c->subs[k];
        if ((sub->gen & 1u) == 0 || !sub->rendezvous ||
            (now_us < sub->rendezvous_deadline_us && !sub->rendezvous_expired)) {
            continue;
        }
        if (!intent_space(c, 1)) {
            sub->rendezvous_expired = true;
            continue;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
        it->binding_cookie = c->bindings[sub->binding].cookie;
        it->cookie = sub->cookie;
        it->error_code = R_ERR_TIMEOUT;
        sub_retire(c, k, now_us);
    }
}


/* A held subscription of `tslot` whose TIMEOUT is still owed: the track
 * waits for it before a source may resume the others. */
bool
rendezvous_timeout_owed(const moqr_core_t *c, uint32_t tslot)
{
    const r_track_t *t = &c->tracks[tslot];
    for (uint32_t k = t->subs_head; k != R_SUB_NIL; k = c->subs[k].track_next) {
        if ((c->subs[k].gen & 1u) != 0 && c->subs[k].rendezvous_expired) {
            return true;
        }
    }
    return false;
}


moqr_result_t
moqr_core_internal_rendezvous_expire(moqr_core_t *c, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    rendezvous_expire(c, now_us);
    return MOQR_OK;
}


/* Held rendezvous subscriptions (§9.20.6), one track at a time, each unit
 * reserving its own intents so a short ring only delays the rest:
 *  - a track whose namespace now has an announced publisher resumes ordinary
 *    admission (one UPSTREAM_SUBSCRIBE; the subscriptions stay parked and are
 *    answered by the upstream's outcome, never early);
 *  - otherwise each subscription whose deadline has passed is refused with
 *    TIMEOUT and retired;
 *  - a track left with no subscription is freed. */
static void
rendezvous_sweep(moqr_core_t *c, uint64_t now_us)
{
    rendezvous_expire(c, now_us);
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0 || t->state != R_TRACK_RENDEZVOUS) {
            continue;
        }
        if (track_sub_count(c, i) == 0) {
            track_free_slot(c, i);
            continue;
        }
        if (rendezvous_timeout_owed(c, i)) {
            continue;   /* its TIMEOUTs go first */
        }
        moq_bytes_t parts[32];
        uint32_t count = key_part_count(&t->key);
        for (uint32_t p = 0; p < count && p < 32; p++) {
            parts[p] = key_part(&t->key, p);
        }
        moqr_ns_t ns = { .parts = parts, .count = count < 32 ? count : 32 };
        uint32_t pub_node = c->cluster_enabled && t->subs_head != R_SUB_NIL
            ? cluster_select_node(c, ns, c->subs[t->subs_head].binding,
                                  c->subs[t->subs_head].binding_gen)
            : trie_longest_announce(c, ns);
        if (pub_node != UINT32_MAX) {
            if (!intent_space(c, 1)) {
                continue;   /* next tick; the deadline does not move */
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
            if (c->cluster_enabled && t->subs_head != R_SUB_NIL) {
                const r_binding_t *requester = &c->bindings[c->subs[t->subs_head].binding];
                t->cluster_requester_hop = requester->peer_hop_id;
                t->cluster_requester_correlation = requester->correlation_id;
            }
            t->metadata_managed = c->bindings[t->up_binding].metadata_managed;
            for (uint32_t k = t->subs_head; k != R_SUB_NIL;
                 k = c->subs[k].track_next) {
                c->subs[k].rendezvous = false;
            }
            moqr_intent_t *up = intent_push(c, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
            up->binding_cookie = c->bindings[t->up_binding].cookie;
            up->track = track_handle(c, i);
            up->track_gen = t->track_gen;
            intent_set_ns_from_key(up, &t->key);
            r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 3 /* rendezvous */, i,
                    t->track_gen, t->up_binding, 0);
            continue;
        }
        for (uint32_t k = t->subs_head; k != R_SUB_NIL;) {
            uint32_t next = c->subs[k].track_next;
            r_sub_t *sub = &c->subs[k];
            if ((sub->gen & 1u) != 0 && sub->rendezvous &&
                now_us >= sub->rendezvous_deadline_us && intent_space(c, 1)) {
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
                it->binding_cookie = c->bindings[sub->binding].cookie;
                it->cookie = sub->cookie;
                it->error_code = R_ERR_TIMEOUT;
                sub_retire(c, k, now_us);
            }
            k = next;
        }
        if (track_sub_count(c, i) == 0) {
            track_free_slot(c, i);
        }
    }
}


moqr_result_t
moqr_core_tick(moqr_core_t *c, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    grant_tick(c, now_us);
    cluster_reparent_sweep(c);
    rendezvous_sweep(c, now_us);
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0) {
            continue;
        }
        /* An open fetch pins the track's retained range against AGE eviction:
         * a fetch accepted a fully-retained range and must be able to drain it.
         * (Capacity eviction from new ingest is ingest-driven, not here, and
         * still surfaces as the cur_group < oldest TOO_OLD terminal in peek.) */
        if (track_fetch_count(c, i) == 0) {
            (void)moqr_log_tick(t->log, now_us);
        }
        if (t->state == R_TRACK_ACTIVE &&
            t->source_kind == R_TRACK_SOURCE_PULL &&
            t->linger_deadline_us != 0 &&
            now_us >= t->linger_deadline_us &&
            track_sub_count(c, i) == 0) {
            if (!intent_space(c, 1)) {
                continue;   /* retry next tick; deterministic given inputs */
            }
            moqr_intent_t *it = intent_push(c,
                                            MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
            it->track = track_handle(c, i);
            it->track_gen = t->track_gen;
            it->cookie = t->upstream_cookie;
            if (t->has_upstream_binding &&
                (c->bindings[t->up_binding].gen & 1u) != 0) {
                it->binding_cookie = c->bindings[t->up_binding].cookie;
            }
            t->linger_deadline_us = 0;
            t->has_upstream_binding = false;
            t->source_kind = R_TRACK_SOURCE_NONE;
            t->track_gen++;
            t->state = R_TRACK_WARM;
            track_touch(c, i);   /* source released: last use */
            r_room_changed(c);
        }
        if (t->state == R_TRACK_WARM && track_sub_count(c, i) == 0 &&
            track_fetch_count(c, i) == 0) {
            moqr_log_stats_t ls;
            moqr_log_get_stats(t->log, &ls);
            if (ls.record_count == 0) {
                track_free_slot(c, i);
                continue;
            }
        }
        /* Idle retention: the first tick that finds a track reclaimable arms
         * its clock; a later tick at least warm_idle_us after that frees it.
         * Arming at a tick, not at the interest change, keeps the bound
         * conservative (never early) across calls that carry no clock. */
        if (!track_reclaimable(c, i)) {
            t->idle_since_us = 0;
        } else if (c->warm_idle_us != UINT64_MAX) {
            if (t->idle_since_us == 0) {
                t->idle_since_us = now_us != 0 ? now_us : 1;   /* armed */
            } else if (now_us >= t->idle_since_us &&
                       now_us - t->idle_since_us >= c->warm_idle_us) {
                track_reclaim(c, i, R_RECLAIM_IDLE);
            }
        }
    }
    return MOQR_OK;
}


uint64_t
moqr_core_room_epoch(const moqr_core_t *c)
{
    return c != NULL ? c->room_epoch : 0;
}


uint64_t
moqr_core_next_deadline_us(const moqr_core_t *c)
{
    if (c == NULL) {
        return UINT64_MAX;
    }
    uint64_t dl = UINT64_MAX;
    /* A held rendezvous subscription is answered TIMEOUT at its deadline. */
    for (uint32_t i = 0; i < c->max_subs; i++) {
        const r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) != 0 && s->rendezvous &&
            s->rendezvous_deadline_us < dl) {
            dl = s->rendezvous_deadline_us;
        }
    }
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        const r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0) {
            continue;
        }
        if (t->state == R_TRACK_ACTIVE &&
            t->source_kind == R_TRACK_SOURCE_PULL &&
            t->linger_deadline_us != 0 && t->linger_deadline_us < dl &&
            track_sub_count(c, i) == 0) {
            dl = t->linger_deadline_us;
        }
        if (c->warm_idle_us != UINT64_MAX && track_reclaimable(c, i)) {
            if (t->idle_since_us == 0) {
                return 0;   /* the next tick arms its idle clock */
            }
            uint64_t due = t->idle_since_us > UINT64_MAX - c->warm_idle_us
                               ? UINT64_MAX
                               : t->idle_since_us + c->warm_idle_us;
            if (due < dl) {
                dl = due;
            }
        }
    }
    return dl;
}


static moqr_result_t core_cluster_capacity(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_capacity_t *out);


moq_result_t
moqr_core_capacity_describe(const moqr_core_relay_cfg_t *cfg,
    moqr_core_capacity_t *out, size_t out_size)
{
    return moqr_core_cluster_capacity_describe(cfg, NULL, out, out_size);
}


moq_result_t
moqr_core_cluster_capacity_describe(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_capacity_t *out, size_t out_size)
{
    if (out == NULL || out_size < MOQR_CORE_CAPACITY_V0_SIZE) return MOQ_ERR_INVAL;
    moqr_core_capacity_t value;
    moqr_result_t rc = core_cluster_capacity(cfg, cluster, &value);
    if (rc == MOQR_OK) moqr_cluster_write(out, out_size, &value, sizeof(value));
    return moqr_result_public(rc);
}


static moqr_result_t
core_cluster_capacity(const moqr_core_relay_cfg_t *cfg,
    const moqr_cluster_cfg_t *cluster, moqr_core_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    moqr_cluster_info_t cluster_resolved;
    if (moqr_cluster_cfg_resolve(cluster, &cluster_resolved, sizeof(cluster_resolved)) != MOQ_OK)
        return MOQR_ERR_INVAL;
    /* The SAME pure resolution create() consumes — defaults, the
     * cancels/bindings coupling, and the intent-ring atomicity clamp all
     * come from one place. */
    moqr_core_limits_t lim;
    if (moqr_core_limits_resolve(cfg, &lim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    /* Refused by create too: an unrepresentable offer matrix. */
    if ((uint64_t)lim.max_ns_subs * lim.max_tracks > SIZE_MAX / sizeof(r_offer_t)) {
        return MOQR_ERR_INVAL;
    }
    uint64_t bindings = lim.max_bindings;
    uint64_t tracks = lim.max_tracks;
    uint64_t subs = lim.max_subs;
    uint64_t nodes = lim.max_ns_nodes;
    uint64_t ns_subs = lim.max_ns_subs;
    uint64_t intents = lim.max_intents;
    if (cluster_resolved.enabled) {
        uint64_t atomic = subs + 2u * tracks + ns_subs;
        if (atomic > UINT32_MAX) return MOQR_ERR_INVAL;
        if (intents < atomic) intents = atomic;
    }
    uint64_t intern = lim.name_intern_bytes;
    uint64_t parked = lim.max_parked;
    uint64_t parked_bytes = lim.parked_bytes;
    uint64_t grants = lim.max_grants;
    uint64_t grant_bytes = lim.grant_bytes;
    uint64_t cancels = lim.max_cancels;
    uint64_t fetches = lim.max_fetches;
    uint64_t fetch_pin = lim.fetch_pin_bytes;

    uint64_t s = sizeof(moqr_core_t);
    s = moqr_cap_add(s, moqr_cap_mul(bindings, sizeof(r_binding_t)));
    if (cluster_resolved.enabled) {
        uint64_t routes = cluster_resolved.max_routes;
        uint64_t hops = cluster_resolved.max_hops;
        if (routes * hops > SIZE_MAX / sizeof(uint64_t)) return MOQR_ERR_INVAL;
        s = moqr_cap_add(s, moqr_cap_mul(routes, sizeof(r_cluster_route_t)));
        s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(routes, hops), sizeof(uint64_t)));
    }
    /* Binding-ready bitset: one bit per binding slot, whole words. */
    s = moqr_cap_add(s, moqr_cap_mul(ready_word_count(lim.max_bindings),
                                     sizeof(uint64_t)));
    s = moqr_cap_add(s, moqr_cap_mul(tracks, sizeof(r_track_t)));
    s = moqr_cap_add(s, moqr_cap_mul(subs, sizeof(r_sub_t)));
    s = moqr_cap_add(s, moqr_cap_mul(nodes, sizeof(r_trie_node_t)));
    s = moqr_cap_add(s, moqr_cap_mul(ns_subs, sizeof(r_ns_sub_t)));
    /* Track discovery: the subscriptions, their per-track offer rows, and the
     * retained request-token slots (their bytes are in the name budget). */
    s = moqr_cap_add(s, moqr_cap_mul(ns_subs, sizeof(r_tsub_t)));
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(ns_subs, tracks),
                                     sizeof(r_offer_t)));
    s = moqr_cap_add(s, moqr_cap_mul(r_rtok_slots(lim.max_ns_subs,
                                                  lim.max_ns_nodes),
                                     sizeof(r_rtok_t)));
    s = moqr_cap_add(s, moqr_cap_mul(intents, sizeof(moqr_intent_t)));
    s = moqr_cap_add(s, moqr_cap_mul(parked, sizeof(r_parked)));
    s = moqr_cap_add(s, moqr_cap_mul(grants, sizeof(r_grant)));
    s = moqr_cap_add(s, moqr_cap_mul(cancels, sizeof(r_pending_cancel)));
    s = moqr_cap_add(s, moqr_cap_mul(fetches, sizeof(r_fetch_t)));
    s = moqr_cap_add(s, intern);   /* interned names + trie parts + ns-sub prefixes */
    /* Per-sub scheduling positions at full occupancy: max_groups bounded
     * entries, including presence, positions, emitted counts and seal acks. */
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), cfg->alloc);
    if (RELAY_CFG_HAS(cfg, log_budget)) {
        lc.budget = cfg->log_budget;
    }
    lc.max_subgroups_per_group =
        RELAY_CFG_HAS(cfg, log_max_subgroups) ? cfg->log_max_subgroups : 0;
    lc.max_objects_per_group = RELAY_CFG_HAS(cfg, log_max_objects_per_group)
                                   ? cfg->log_max_objects_per_group
                                   : 0;
    lc.max_cursors =
        RELAY_CFG_HAS(cfg, log_max_cursors) ? cfg->log_max_cursors : 0;
    lc.max_chunk_nodes =
        RELAY_CFG_HAS(cfg, log_max_chunk_nodes) ? cfg->log_max_chunk_nodes : 0;
    moqr_log_capacity_t logc;
    if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
        return MOQR_ERR_INVAL;   /* the per-track log term wrapped */
    }
    uint64_t groups = lc.budget.max_groups != 0 ? lc.budget.max_groups : 8u;
    uint64_t lists =
        (lc.max_subgroups_per_group != 0 ? lc.max_subgroups_per_group
                                         : 16u) +
        1u;
    /* The per-(sub, group) position-entry stride — shared with the runtime
     * (gpos_stride) so the advertised ceiling can never drift from the
     * allocation. */
    uint64_t stride = gpos_stride_for(lists);
    if (lists > UINT32_MAX || stride > UINT32_MAX) {
        return MOQR_ERR_INVAL;   /* gpos layout unrepresentable */
    }
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(subs, groups), stride));
    /* Per-sub group_id -> gpos-slot index (shared wide resolver). */
    uint64_t gpos_hash_cap = gpos_hash_cap_for(groups);
    if (gpos_hash_cap > UINT32_MAX) {
        return MOQR_ERR_INVAL;   /* the runtime's u32 index cannot hold it */
    }
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(subs, gpos_hash_cap), sizeof(uint32_t)));
    /* Semantic filter history: one exclusion bit per bounded journal slot. */
    s = moqr_cap_add(s, moqr_cap_mul(subs, MOQR_LOG_DEF_MAX_RECORDS / 8u +
                                         (MOQR_LOG_DEF_MAX_RECORDS % 8u != 0)));

    /* Worst-case per-binding chunk-pin storage: a chunked delivery pins every
     * chunk of the in-flight record (ref + length) so eviction cannot truncate
     * a multi-pump delivery. A record can hold no more chunks than the log's
     * chunk-node pool, so each binding's reusable array is bounded by
     * max_chunk_nodes; the core-level log_max_chunk_nodes knob feeds the same
     * per-track log capacity path as live log creation. */
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(bindings, logc.max_chunk_nodes), sizeof(r_pin_chunk_t)));

    /* The fetch pin budget is floored to the per-track payload budget (create
     * applies the identical floor), and adds to the payload ceiling: a
     * backpressured fetch can hold that many payload bytes pinned past eviction,
     * on top of the logs' own retained payload. */
    if (fetch_pin < logc.payload_bytes) {
        fetch_pin = logc.payload_bytes;
    }
    /* rcbuf header overhead: the per-log retained-content headers (payload +
     * properties per record, one per chunk node — logc.header_bytes) times
     * tracks, plus one independently allocated coalesced buffer per live
     * fetch (possible at zero pinned payload bytes). H is the pinned
     * additive moq_rcbuf_allocation_size(0). */
    size_t rc_hdr = 0;
    (void)moq_rcbuf_allocation_size(0, &rc_hdr);
    out->structure_bytes = moqr_cap_add(
        s, moqr_cap_add(
               moqr_cap_mul(tracks, logc.structure_bytes),
               moqr_cap_add(moqr_cap_mul(tracks, logc.header_bytes),
                            moqr_cap_mul(fetches, rc_hdr))));
    out->payload_bytes = moqr_cap_add(
        moqr_cap_mul(tracks, logc.payload_bytes),
        moqr_cap_add(parked_bytes, moqr_cap_add(grant_bytes, fetch_pin)));
    out->total_bytes =
        moqr_cap_add(out->structure_bytes, out->payload_bytes);
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}
