#include "bind_internal.h"
#include "authorization_internal.h"
#include "delivery_internal.h"
#include "discovery_internal.h"
#include "events_internal.h"
#include "fetch_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "subscription_update_internal.h"
#include "testing_internal.h"
#include "upstream_internal.h"

#define BIND_DEF_CONNS 64u

#define BIND_DEF_DSUBS 32u

#define BIND_DEF_SGS   MOQR_BIND_DEF_OPEN_SUBGROUPS

#define BIND_DEF_USUBS 8u

#define BIND_DEF_PUBS  8u

/* d16 MAX_REQUEST_ID grant step (request-id units). Each grant raises the
 * ceiling by this; the peer keeps ~half a window of headroom. */
#define BIND_DEF_GRANT_WINDOW 1024u

#define B_DRAFT_18 1u



#define BIND_CFG_HAS(cfg, field)                     \
    (offsetof(moqr_bind_cfg_t, field) +              \
         sizeof(((moqr_bind_cfg_t *)0)->field) <= (cfg)->struct_size)


void
moqr_bind_cfg_init_sized(moqr_bind_cfg_t *cfg, size_t cfg_size,
                         const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_bind_cfg_t) ? cfg_size
                                                  : sizeof(moqr_bind_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_bind_cfg_t, alloc) +
            sizeof(((moqr_bind_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
}


/* The aggregate forwarding byte pool: the appended cfg field when the caller's
 * struct reaches it and it is nonzero, else the named default. Values the
 * accounting cannot carry on a 32-bit host are refused, never truncated. */
static moqr_result_t
bind_fwd_bytes_resolve(const moqr_bind_cfg_t *cfg, uint64_t *out)
{
    uint64_t v = BIND_CFG_HAS(cfg, fetch_forward_bytes)
                     ? cfg->fetch_forward_bytes : 0;
    if (v == 0) {
        v = MOQR_BIND_DEF_FETCH_FORWARD_BYTES;
    }
    if (v > UINT32_MAX) {
        *out = 0;
        return MOQR_ERR_INVAL;
    }
    *out = v;
    return MOQR_OK;
}


/* The fill wait pair, in microseconds: each value when nonzero, else its
 * named default; a default above the maximum is refused. Both are bounded so
 * the deadline arithmetic (processing time + wait) cannot wrap. */
#define BIND_FILL_WAIT_MAX_MS (UINT64_MAX / 1000000u)

static moqr_result_t
bind_fill_wait_resolve(uint64_t d, uint64_t m, uint64_t *def_us,
                       uint64_t *max_us)
{
    if (d == 0) {
        d = MOQR_BIND_DEF_FILL_TIMEOUT_MS;
    }
    if (m == 0) {
        m = MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS;
    }
    if (d > m || m > BIND_FILL_WAIT_MAX_MS) {
        *def_us = 0;
        *max_us = 0;
        return MOQR_ERR_INVAL;
    }
    *def_us = d * 1000u;
    *max_us = m * 1000u;
    return MOQR_OK;
}


moqr_result_t
moqr_bind_cfg_resolve(const moqr_bind_cfg_t *cfg,
                      const moqr_core_limits_t *lim, moqr_bind_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t) || lim == NULL) {
        return MOQR_ERR_INVAL;
    }
#define BL_RESOLVE(dst, field, def) \
    out->dst = BIND_CFG_HAS(cfg, field) && cfg->field != 0 ? cfg->field : (def)
    BL_RESOLVE(max_conns, max_conns, lim->max_bindings);
    BL_RESOLVE(n_dsubs, max_downstream_subs, lim->max_subs);
    BL_RESOLVE(n_sgs, max_open_subgroups, BIND_DEF_SGS);
    BL_RESOLVE(n_usubs, max_upstream_subs, lim->max_tracks);
    BL_RESOLVE(n_pubs, max_publishes, lim->max_tracks);
    BL_RESOLVE(n_anns, max_announces, lim->max_ns_nodes);
    BL_RESOLVE(request_grant_window, request_grant_window,
               BIND_DEF_GRANT_WINDOW);
#undef BL_RESOLVE
    out->n_fetches = lim->max_fetches;
    /* Held-terminal entries per connection (b_refusal_t): one poll of
     * headroom and one per parkable authorization. */
    uint64_t refusals = (uint64_t)BIND_PUMP_EVENT_POLL + lim->max_parked;
    if (refusals > UINT32_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    out->n_refusals = (uint32_t)refusals;
    {
        uint64_t fwd_bytes;
        if (bind_fwd_bytes_resolve(cfg, &fwd_bytes) != MOQR_OK) {
            memset(out, 0, sizeof(*out));
            return MOQR_ERR_INVAL;
        }
    }
    /* Derived count, computed WIDE: a deferral ring the 32-bit field cannot
     * represent rejects the config — never a wrapped small ring. */
    uint64_t pend = 4ull * ((uint64_t)lim->max_subs + lim->max_tracks) +
                    4ull * lim->max_intents + 64u;
    if (pend > UINT32_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    out->pending_cap = (uint32_t)pend;
    /* The namespace payload sidecar is 1:1 with the deferral ring, so it needs
     * no bound of its own. This is a finite POLICY bound, not a mathematical
     * ceiling on simultaneously owed updates: prefix-to-namespace matching is a
     * fan-out and blocked updates accumulate across pumps, so exhaustion is
     * possible by construction and is handled fail-closed at the push site. */
    out->nsu_cap = out->pending_cap;
    return MOQR_OK;
}

static moqr_result_t bind_cluster_capacity(const moqr_bind_cfg_t *cfg,
    const moqr_core_limits_t *lim, const moqr_cluster_info_t *cluster_info,
    moqr_bind_capacity_t *out);


moq_result_t
moqr_bind_capacity_describe(const moqr_bind_cfg_t *cfg,
    const moqr_core_limits_t *lim, moqr_bind_capacity_t *out, size_t out_size)
{
    moqr_cluster_info_t cluster = {0};
    cluster.struct_size = sizeof(cluster);
    if (cfg != NULL && BIND_CFG_HAS(cfg, core) && cfg->core != NULL)
        (void)moqr_core_cluster_info(cfg->core, &cluster, sizeof(cluster));
    return moqr_bind_cluster_capacity_describe(cfg, lim, &cluster, out, out_size);
}


moq_result_t
moqr_bind_cluster_capacity_describe(const moqr_bind_cfg_t *cfg,
    const moqr_core_limits_t *lim, const moqr_cluster_info_t *cluster_info,
    moqr_bind_capacity_t *out, size_t out_size)
{
    if (out == NULL || out_size < MOQR_BIND_CAPACITY_V0_SIZE) return MOQ_ERR_INVAL;
    moqr_cluster_info_t cluster = {0};
    if (cluster_info != NULL && !moqr_cluster_read(cluster_info, MOQR_CLUSTER_INFO_V0_SIZE,
                                                  &cluster, sizeof(cluster))) return MOQ_ERR_INVAL;
    moqr_bind_capacity_t value = {0};
    moqr_result_t rc = bind_cluster_capacity(cfg, lim, &cluster, &value);
    if (rc == MOQR_OK) moqr_cluster_write(out, out_size, &value, sizeof(value));
    return moqr_result_public(rc);
}


static moqr_result_t
bind_cluster_capacity(const moqr_bind_cfg_t *cfg,
    const moqr_core_limits_t *lim, const moqr_cluster_info_t *cluster_info,
    moqr_bind_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    moqr_bind_limits_t bl;
    moqr_result_t rc = moqr_bind_cfg_resolve(cfg, lim, &bl);
    if (rc != MOQR_OK) {
        return rc;
    }
    /* Mirrors moqr_bind_create's allocations exactly: the bind struct, the
     * deferral ring, the conn array, and every per-conn table (eager).
     * Checked composition: a wrap refuses INVAL, never under-reports. */
    uint64_t per_conn = moqr_cap_add(
        moqr_cap_mul(bl.n_dsubs, sizeof(b_dsub_t)),
        moqr_cap_add(
            moqr_cap_mul(bl.n_sgs, sizeof(b_sg_t)),
            moqr_cap_add(
                moqr_cap_mul(bl.n_usubs, sizeof(b_usub_t)),
                moqr_cap_add(
                    moqr_cap_mul(bl.n_pubs, sizeof(b_pub_t)),
                    moqr_cap_add(
                        moqr_cap_mul(bl.n_anns, sizeof(b_ann_t)),
                        moqr_cap_add(
                            moqr_cap_mul(bl.n_fetches, sizeof(b_fetch_t)),
                            moqr_cap_mul(bl.n_refusals,
                                         sizeof(b_refusal_t))))))));
    out->structure_bytes = moqr_cap_add(
        sizeof(moqr_bind_t),
        moqr_cap_add(moqr_cap_mul(bl.pending_cap, sizeof(moqr_intent_t)),
                     moqr_cap_mul(bl.max_conns,
                                  moqr_cap_add(sizeof(b_conn_t),
                                               per_conn))));
    const moqr_cluster_info_t cluster = cluster_info != NULL
        ? *cluster_info : (moqr_cluster_info_t){0};
    if (cluster.enabled && cluster.max_hops == 0) return MOQR_ERR_INVAL;
    if (cluster.enabled) {
        out->structure_bytes = moqr_cap_add(out->structure_bytes,
            moqr_cap_add(moqr_cap_mul((uint64_t)cluster.max_hops + 1u, sizeof(uint64_t)),
                moqr_cap_mul(moqr_cap_mul(bl.max_conns, bl.n_anns),
                    moqr_cap_add(sizeof(b_cadv_t), moqr_cap_mul(cluster.max_hops, sizeof(uint64_t))))));
    }
    /* Delivery scheduling bitsets: ready + parked, one bit per conn slot. */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes,
        moqr_cap_mul(2u * (uint64_t)bind_dl_word_count(bl.max_conns),
                     sizeof(uint64_t)));
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes,
        moqr_cap_add(moqr_cap_mul(BIND_PUMP_EVENT_BATCH,
                                  sizeof(moq_event_t)),
                     moqr_cap_add(
                         moqr_cap_mul(BIND_PUMP_REVOKED_BATCH,
                                      sizeof(moqr_revoked_grant_t)),
                         moqr_cap_mul(BIND_PUMP_INTENT_BATCH,
                                      sizeof(moqr_intent_t)))));
    uint64_t ann_slots = moqr_cap_mul(bl.max_conns, bl.n_anns);
    if (ann_slots > lim->max_ns_nodes) {
        ann_slots = lim->max_ns_nodes;
    }
    out->announce_bytes = moqr_cap_mul(ann_slots, BIND_ANN_BYTES_MAX);
    if (cluster.enabled) {
        const uint64_t slots = moqr_cap_mul(bl.max_conns, bl.n_anns);
        const uint64_t incoming = slots; /* includes not-yet-admitted candidates */
        out->announce_bytes = moqr_cap_mul(moqr_cap_add(incoming, slots), BIND_ANN_BYTES_MAX);
    }
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes, moqr_cap_mul(bl.nsu_cap, sizeof(b_nsu_t)));
    out->announce_bytes = moqr_cap_add(
        out->announce_bytes, moqr_cap_mul(bl.nsu_cap, BIND_ANN_BYTES_MAX));
    /* Forwarded-fetch transactions: the bind-wide pool of fixed headers (one
     * per core fetch slot) is an eager table like the rest; the owned track
     * keys every upstream-subscription slot may hold and the aggregate
     * fetch_forward_bytes pool are variable ceilings that this frozen output
     * carries in total_bytes only (moqr_bind.h documents the accounting). */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes, moqr_cap_mul(bl.n_fetches,
            moqr_cap_add(sizeof(b_fwd_t), sizeof(b_retrieve_hold_t))));
    uint64_t fwd_bytes = 0;
    if (bind_fwd_bytes_resolve(cfg, &fwd_bytes) != MOQR_OK) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    uint64_t forward_bytes = moqr_cap_add(
        fwd_bytes,
        moqr_cap_mul(moqr_cap_mul(bl.max_conns, bl.n_usubs), BF_FWD_KEY_MAX));
    uint32_t meta_pubs;
    if (!bind_meta_publish_count(lim, &bl, &meta_pubs)) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    /* Held namespace-update answers: one per request that can be updated. */
    out->structure_bytes = moqr_cap_add(out->structure_bytes,
        moqr_cap_mul(bind_nsans_cap(lim), sizeof(b_nsans_t)));
    out->structure_bytes = moqr_cap_add(out->structure_bytes,
        moqr_cap_add(moqr_cap_mul(lim->max_tracks, sizeof(b_meta_entry_t)),
            moqr_cap_add(moqr_cap_mul(bl.pending_cap, sizeof(moqr_bind_meta_ref_t)),
                         moqr_cap_mul(meta_pubs, sizeof(b_meta_publish_t)))));
    out->total_bytes = moqr_cap_add(
        moqr_cap_add(out->structure_bytes, out->announce_bytes),
        moqr_cap_add(forward_bytes, MOQR_BIND_META_BYTES_MAX));
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}


moqr_result_t
moqr_bind_create(const moqr_bind_cfg_t *cfg, moqr_bind_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = BIND_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    moqr_core_t *core = BIND_CFG_HAS(cfg, core) ? cfg->core : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL || core == NULL) {
        return MOQR_ERR_INVAL;
    }

    moqr_bind_t *b = a->alloc(sizeof(*b), a->ctx);
    if (b == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(b, 0, sizeof(*b));
    b->alloc = *a;
    b->core = core;
    moqr_core_cluster_info(core, &b->cluster_info, sizeof(b->cluster_info));
    /* Default map sizes FOLLOW the core's admission limits, so the
     * binding can never be under-provisioned relative to what the core
     * will admit. Explicit cfg values still win (tests use tiny maps to
     * prove the refusal paths). */
    moqr_core_limits_t lim;
    moqr_core_get_limits(core, &lim);
    /* Same pure resolution the capacity describe uses; the live core's
     * limits equal moqr_core_limits_resolve(cfg) by construction. */
    moqr_bind_limits_t bl;
    if (moqr_bind_cfg_resolve(cfg, &lim, &bl) != MOQR_OK ||
        !bind_meta_publish_count(&lim, &bl, &b->metadata_publish_cap)) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_INVAL;
    }
    b->max_conns = bl.max_conns;
    b->n_dsubs = bl.n_dsubs;
    b->n_sgs = bl.n_sgs;
    b->n_usubs = bl.n_usubs;
    b->n_pubs = bl.n_pubs;
    b->n_anns = bl.n_anns;
    b->request_grant_window = bl.request_grant_window;
    b->n_fetches = bl.n_fetches;
    b->n_refusals = bl.n_refusals;
    if (bind_fwd_bytes_resolve(cfg, &b->fwd_bytes_cap) != MOQR_OK ||
        bind_fill_wait_resolve(0, 0, &b->fill_wait_default_us,
                               &b->fill_wait_max_us) != MOQR_OK) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_INVAL;
    }

    /* Intent router (multi-shard runtime; absent by default). */
    b->router_cookie_base =
        BIND_CFG_HAS(cfg, router_cookie_base) ? cfg->router_cookie_base : 0;
    b->router = BIND_CFG_HAS(cfg, router) ? cfg->router : NULL;
    b->router_ctx = BIND_CFG_HAS(cfg, router_ctx) ? cfg->router_ctx : NULL;
    /* A router with base 0 would route real connection cookies (1..max_conns)
     * too, starving every session of its intents. Treat 0 as "use the standard
     * reserved range" (the cfg's 0-means-default idiom) so the common case —
     * install a router, get MOQR_SHARD_COOKIE_BASE — is not a footgun. */
    if (b->router != NULL && b->router_cookie_base == 0) {
        b->router_cookie_base = MOQR_SHARD_COOKIE_BASE;
    }

    /* Deferred-output ring: large enough to hold every scalar terminal that can
     * be in flight at once (each live sub can owe an ACCEPT and a SUB_DONE, each
     * live pub an ACCEPT and a REJECT, plus upstream-failure REJECT_SUB fan-outs
     * and in-flight track-status), with generous headroom over the core intent
     * ring. Overflow is counted, never dropped silently; the lifecycle sweep
     * asserts the high-water stays well under this. */
    b->pending_cap = bl.pending_cap;
    b->pending = b->alloc.alloc((size_t)b->pending_cap * sizeof(*b->pending),
                                b->alloc.ctx);
    if (b->pending == NULL) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->pending, 0, (size_t)b->pending_cap * sizeof(*b->pending));

    /* Retained namespace updates (draft-18 Section 6.2 owes each one durably).
     * Eagerly sized like the deferral ring so a blocked control path never has
     * to allocate the ring itself under pressure. */
    b->nsu_cap = bl.nsu_cap;
    b->nsu = b->alloc.alloc((size_t)b->nsu_cap * sizeof(*b->nsu),
                            b->alloc.ctx);
    if (b->nsu == NULL) {
        b->alloc.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending),
                      b->alloc.ctx);
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->nsu, 0, (size_t)b->nsu_cap * sizeof(*b->nsu));

    b->conns = b->alloc.alloc((size_t)b->max_conns * sizeof(*b->conns),
                              b->alloc.ctx);
    if (b->conns == NULL) {
        b->alloc.free(b->nsu, (size_t)b->nsu_cap * sizeof(*b->nsu),
                      b->alloc.ctx);
        b->alloc.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending),
                      b->alloc.ctx);
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->conns, 0, (size_t)b->max_conns * sizeof(*b->conns));
    b->metadata_cap = lim.max_tracks;
    b->metadata = b->alloc.alloc((size_t)b->metadata_cap * sizeof(*b->metadata),
                                 b->alloc.ctx);
    if (b->metadata == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->metadata, 0, (size_t)b->metadata_cap * sizeof(*b->metadata));
    b->pending_metadata = b->alloc.alloc(
        (size_t)b->pending_cap * sizeof(*b->pending_metadata), b->alloc.ctx);
    if (b->pending_metadata == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->pending_metadata, 0,
           (size_t)b->pending_cap * sizeof(*b->pending_metadata));
    b->metadata_publishes = b->alloc.alloc(
        (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes), b->alloc.ctx);
    if (b->metadata_publishes == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->metadata_publishes, 0,
           (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes));
    b->nsans_cap = bind_nsans_cap(&lim);
    if (b->nsans_cap > 0) {
        b->nsans = b->alloc.alloc((size_t)b->nsans_cap * sizeof(*b->nsans),
                                  b->alloc.ctx);
        if (b->nsans == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->nsans, 0, (size_t)b->nsans_cap * sizeof(*b->nsans));
    }
    /* Forwarded-fetch transaction pool: from here on every failure routes
     * through moqr_bind_destroy, which frees whatever was allocated. */
    b->n_fwd = bl.n_fetches;
    if (b->n_fwd > 0) {
        b->fwd = b->alloc.alloc((size_t)b->n_fwd * sizeof(*b->fwd),
                                b->alloc.ctx);
        if (b->fwd == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->fwd, 0, (size_t)b->n_fwd * sizeof(*b->fwd));
        if (sizeof(*b->cached_holds) > SIZE_MAX / b->n_fwd) {
            moqr_bind_destroy(b);
            return MOQR_ERR_INVAL;
        }
        b->cached_holds = b->alloc.alloc(
            (size_t)b->n_fwd * sizeof(*b->cached_holds), b->alloc.ctx);
        if (b->cached_holds == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->cached_holds, 0,
               (size_t)b->n_fwd * sizeof(*b->cached_holds));
    }
    {
        size_t dw = (size_t)bind_dl_word_count(b->max_conns) *
                    sizeof(uint64_t);
        b->dl_ready = b->alloc.alloc(dw, b->alloc.ctx);
        b->dl_parked = b->alloc.alloc(dw, b->alloc.ctx);
        if (b->dl_ready == NULL || b->dl_parked == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->dl_ready, 0, dw);
        memset(b->dl_parked, 0, dw);
    }
    b->pump_events =
        b->alloc.alloc((size_t)BIND_PUMP_EVENT_BATCH *
                           sizeof(*b->pump_events),
                       b->alloc.ctx);
    if (b->pump_events == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    b->pump_revoked =
        b->alloc.alloc((size_t)BIND_PUMP_REVOKED_BATCH *
                           sizeof(*b->pump_revoked),
                       b->alloc.ctx);
    if (b->pump_revoked == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    b->pump_intents =
        b->alloc.alloc((size_t)BIND_PUMP_INTENT_BATCH *
                           sizeof(*b->pump_intents),
                       b->alloc.ctx);
    if (b->pump_intents == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    if (b->cluster_info.enabled) {
        b->cluster_hops = b->alloc.alloc(
            ((size_t)b->cluster_info.max_hops + 1u) * sizeof(uint64_t), b->alloc.ctx);
        if (b->cluster_hops == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        /* Allocate each per-connection table and zero it IMMEDIATELY, before the
         * next allocation can fail. A partial-create moqr_bind_destroy scans the
         * `used` flags of every non-NULL table, so an allocated-but-uninitialized
         * table would be read as garbage; zeroing per table keeps the failed and
         * not-yet-reached tables NULL (skipped by destroy) and every allocated
         * table initialized. */
#define BIND_ALLOC_TABLE(field, count)                                        \
        do {                                                                  \
            cn->field =                                                       \
                b->alloc.alloc((size_t)(count) * sizeof(*cn->field),          \
                               b->alloc.ctx);                                 \
            if (cn->field == NULL) {                                          \
                moqr_bind_destroy(b);                                         \
                return MOQR_ERR_NOMEM;                                        \
            }                                                                 \
            memset(cn->field, 0, (size_t)(count) * sizeof(*cn->field));       \
        } while (0)
        BIND_ALLOC_TABLE(dsubs, b->n_dsubs);
        BIND_ALLOC_TABLE(sgs, b->n_sgs);
        BIND_ALLOC_TABLE(usubs, b->n_usubs);
        BIND_ALLOC_TABLE(pubs, b->n_pubs);
        BIND_ALLOC_TABLE(anns, b->n_anns);
        if (b->cluster_info.enabled) {
            BIND_ALLOC_TABLE(cluster_adverts, b->n_anns);
            if ((size_t)b->n_anns > SIZE_MAX / sizeof(uint64_t) / b->cluster_info.max_hops) {
                moqr_bind_destroy(b);
                return MOQR_ERR_INVAL;
            }
            BIND_ALLOC_TABLE(cluster_paths, (size_t)b->n_anns * b->cluster_info.max_hops);
        }
        BIND_ALLOC_TABLE(fetches, b->n_fetches);
        BIND_ALLOC_TABLE(refusals, b->n_refusals);
#undef BIND_ALLOC_TABLE
    }
    *out = b;
    return MOQR_OK;
}

b_conn_t *conn_by_cookie(moqr_bind_t *b, uint64_t cookie);

void conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);


/* An upstream terminal code is that connection's draft, not ours. Capture the
 * origin with the code at ingest: the record may be replayed to a subscriber
 * on the other draft long after this connection is gone, and by then nothing
 * else can say which registry the number was written in. */
moqr_reset_desc_t
bind_upstream_reset(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(moqr_bind_conn_version(b, cn->session), wire,
                             &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}


/* Encode a retained reset cause for the connection about to read it. This is
 * the single translation point: the descriptor carries its own origin, so a
 * draft-16 terminal replayed to a draft-18 subscriber is renumbered here and
 * nowhere else. A descriptor with nothing truthful to say fails the delivery
 * closed rather than emitting a plausible number. */
bool
bind_reset_wire(const moqr_bind_t *b, const b_conn_t *cn,
                moqr_reset_desc_t desc, uint64_t *out)
{
    return moqr_reset_desc_emit(desc, moqr_bind_conn_version(b, cn->session),
                                out) == MOQR_OK;
}


/* An upstream PUBLISH_DONE status is that connection's draft. Capture the
 * origin with it: downstream subscribers may read the other draft, and the
 * terminal outlives this connection. */
moqr_pd_desc_t
bind_upstream_pd(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_wire(moqr_bind_conn_version(b, cn->session), wire, &d) !=
        MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}


/* A terminal this relay chose is a meaning, not a draft's number. */
moqr_pd_desc_t
bind_local_pd(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}


void
conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    if (!cn->used || cn->closed) {
        return;
    }
    cn->closed = true;   /* no new work routes to this conn */
    bind_dup_owners_release(b, cn);
    /* A fill reading through the fill source ends its claim now: the source
     * holds resources on its behalf until it is told, and this table is
     * cleared without another look when the slot is reused. */
    for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
        b_fetch_t *bf = &cn->fetches[fi];
        if (bf->used && bf->fill_src_open) {
            b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
            bf->fill_src_open = false;
        }
        if (bf->used && bf->sa_remote) {
            b->sa_src.close(b->sa_src.ctx, bf->sa_handle);
            bf->sa_remote = false;
        }
        if (bf->used && bf->retrieval.live) {
            moqr_bind_retrieve_close(b, &bf->retrieval);
        }
    }
    fwd_conn_gone(b, cn);
    bind_refusal_conn_gone(b, cn);
    bind_meta_pub_purge(b, cn);
    bind_nsans_conn_gone(b, cn);
    bind_dl_clear_conn(b, cn);
    /* Retire this binding's parked (DEFERred) requests and revalidation grants
     * before the core binding close, freeing their deep-copied bytes: a later
     * scheduled recheck can no longer touch a torn-down session. Drop any
     * parked output intents for this conn too — its session is going away, so
     * they are moot, and the cookie (= slot + 1) may be reused. */
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    bind_pending_purge_conn(b, bcookie);
    moqr_core_retire_parked(b->core, bcookie);
    moqr_core_retire_grants(b->core, bcookie);   /* held reservations too */
    bind_dgrant_conn_gone(b, cn);
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_binding_close(b->core, cn->binding,
                                                now_us));
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        cn->detach_pending = true;   /* retried after each pump's drain */
#ifdef MOQR_BIND_TESTING
        if (bind_dbg_detach_pending_fn != NULL) {
            bind_dbg_detach_pending_fn(b, cn->session,
                                       bind_dbg_detach_pending_ctx);
        }
#endif
        return;
    }
    if (rc != MOQR_OK) {
        b->session_errors++;
    }
    ann_clear_all(b, cn);
    bind_cluster_adverts_clear(b, cn, 0);
    usub_release_all(b, cn);
    cn->used = false;
    cn->detach_pending = false;
    cn->session = NULL;
}


void
moqr_bind_destroy(moqr_bind_t *b)
{
    if (b == NULL) {
        return;
    }
    moq_alloc_t a = b->alloc;
    /* Strictly memory-only: destroy calls NO session and NO core APIs.
     * It runs at process shutdown AFTER the transport (and its sessions)
     * are gone, so touching a session here would be use-after-free, and
     * executing intents would deliver to dead sessions. Per-connection
     * session teardown happens live on the network thread via conn_detach
     * (SESSION_CLOSED / moqr_bind_conn_close); the owning core is destroyed
     * separately by the caller, which frees any core bindings still open. */
    if (b->conns != NULL) {
        for (uint32_t i = 0; i < b->max_conns; i++) {
            b_conn_t *cn = &b->conns[i];
            if (cn->dsubs != NULL) {
                a.free(cn->dsubs, (size_t)b->n_dsubs * sizeof(*cn->dsubs),
                       a.ctx);
            }
            if (cn->sgs != NULL) {
                a.free(cn->sgs, (size_t)b->n_sgs * sizeof(*cn->sgs), a.ctx);
            }
            if (cn->usubs != NULL) {
                usub_release_all(b, cn);
                a.free(cn->usubs, (size_t)b->n_usubs * sizeof(*cn->usubs),
                       a.ctx);
            }
            if (cn->pubs != NULL) {
                a.free(cn->pubs, (size_t)b->n_pubs * sizeof(*cn->pubs),
                       a.ctx);
            }
            if (cn->anns != NULL) {
                ann_clear_all(b, cn);
                a.free(cn->anns, (size_t)b->n_anns * sizeof(*cn->anns),
                       a.ctx);
            }
            if (cn->cluster_paths != NULL)
                a.free(cn->cluster_paths, (size_t)b->n_anns * b->cluster_info.max_hops * sizeof(uint64_t), a.ctx);
            if (cn->cluster_adverts != NULL) {
                bind_cluster_adverts_clear(b, cn, 0);
                a.free(cn->cluster_adverts, (size_t)b->n_anns * sizeof(*cn->cluster_adverts), a.ctx);
            }
            if (cn->fetches != NULL) {
                a.free(cn->fetches, (size_t)b->n_fetches * sizeof(*cn->fetches),
                       a.ctx);
            }
            if (cn->refusals != NULL) {
                a.free(cn->refusals,
                       (size_t)b->n_refusals * sizeof(*cn->refusals), a.ctx);
            }
        }
        a.free(b->conns, (size_t)b->max_conns * sizeof(*b->conns), a.ctx);
    }
    if (b->cluster_hops != NULL)
        a.free(b->cluster_hops, ((size_t)b->cluster_info.max_hops + 1u) * sizeof(uint64_t), a.ctx);
    if (b->fwd != NULL) {
        /* Memory-only: owned references the transactions still hold are
         * released without any session call. */
        for (uint32_t i = 0; i < b->n_fwd; i++) {
            b_fwd_t *t = &b->fwd[i];
            if (!t->used) {
                continue;
            }
            for (uint32_t k = t->next_write; k < t->n_items; k++) {
                if (t->items[k].payload != NULL) {
                    moq_rcbuf_decref(t->items[k].payload);
                }
                if (t->items[k].properties != NULL) {
                    moq_rcbuf_decref(t->items[k].properties);
                }
            }
            if (t->props != NULL) a.free(t->props, t->props_len, a.ctx);
            if (t->reason != NULL) a.free(t->reason, t->reason_len, a.ctx);
            if (t->key != NULL) a.free(t->key, t->key_len, a.ctx);
        }
        a.free(b->fwd, (size_t)b->n_fwd * sizeof(*b->fwd), a.ctx);
    }
    if (b->cached_holds != NULL) {
        for (uint32_t i = 0; i < b->n_fwd; i++) {
            moq_rcbuf_decref(b->cached_holds[i].payload);
            moq_rcbuf_decref(b->cached_holds[i].properties);
        }
        a.free(b->cached_holds,
               (size_t)b->n_fwd * sizeof(*b->cached_holds), a.ctx);
    }
    {
        size_t dw = (size_t)bind_dl_word_count(b->max_conns) *
                    sizeof(uint64_t);
        if (b->dl_ready != NULL) {
            a.free(b->dl_ready, dw, a.ctx);
        }
        if (b->dl_parked != NULL) {
            a.free(b->dl_parked, dw, a.ctx);
        }
    }
    if (b->pump_intents != NULL) {
        a.free(b->pump_intents,
               (size_t)BIND_PUMP_INTENT_BATCH * sizeof(*b->pump_intents),
               a.ctx);
    }
    if (b->pump_revoked != NULL) {
        a.free(b->pump_revoked,
               (size_t)BIND_PUMP_REVOKED_BATCH * sizeof(*b->pump_revoked),
               a.ctx);
    }
    if (b->pump_events != NULL) {
        a.free(b->pump_events,
               (size_t)BIND_PUMP_EVENT_BATCH * sizeof(*b->pump_events),
               a.ctx);
    }
    if (b->nsu != NULL) {
        for (uint32_t i = 0; i < b->nsu_cap; i++) {
            if (b->nsu[i].used && b->nsu[i].bytes != NULL) {
                a.free(b->nsu[i].bytes, b->nsu[i].bytes_len, a.ctx);
            }
        }
        a.free(b->nsu, (size_t)b->nsu_cap * sizeof(*b->nsu), a.ctx);
    }
    if (b->nsans != NULL) {
        a.free(b->nsans, (size_t)b->nsans_cap * sizeof(*b->nsans), a.ctx);
    }
    if (b->pending != NULL) {
        a.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending), a.ctx);
    }
    if (b->pending_metadata != NULL) {
        for (uint32_t i = 0; i < b->pending_cap; i++) {
            moqr_bind_meta_release(b, &b->pending_metadata[i]);
        }
        a.free(b->pending_metadata,
               (size_t)b->pending_cap * sizeof(*b->pending_metadata), a.ctx);
    }
    if (b->metadata_publishes != NULL) {
        for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
            bind_meta_pub_release(b, &b->metadata_publishes[i]);
        }
        a.free(b->metadata_publishes,
               (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes),
               a.ctx);
    }
    if (b->metadata != NULL) {
        for (uint32_t i = 0; i < b->metadata_cap; i++) {
            moqr_bind_meta_release(b, &b->metadata[i].latest);
        }
        a.free(b->metadata, (size_t)b->metadata_cap * sizeof(*b->metadata), a.ctx);
    }
    a.free(b, sizeof(*b), a.ctx);
}


b_conn_t *
conn_by_session(moqr_bind_t *b, const moq_session_t *s)
{
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used && b->conns[i].session == s) {
            return &b->conns[i];
        }
    }
    return NULL;
}


b_conn_t *
conn_by_cookie(moqr_bind_t *b, uint64_t cookie)
{
    if (cookie < 1 || cookie > b->max_conns) {
        return NULL;
    }
    b_conn_t *cn = &b->conns[cookie - 1];
    return cn->used ? cn : NULL;
}


static moqr_result_t
bind_conn_open_cluster(moqr_bind_t *b, moq_session_t *session,
                    moq_version_t version, const moqr_bind_cluster_peer_cfg_t *cluster)
{
    if (b == NULL || session == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Refuse an unrecognized draft before anything is taken. Every terminal
     * this binding emits is encoded for the draft the peer reads, so a
     * connection whose draft is unknown has no safe encoding — and defaulting
     * to a plausible one is the silent wrong-numbering this parameter exists
     * to prevent. Rejecting here also keeps a refused attach free of cost: no
     * slot, no core binding. */
    uint8_t draft;
    switch (version) {
    case MOQ_VERSION_DRAFT_16: draft = B_DRAFT_16; break;
    case MOQ_VERSION_DRAFT_18: draft = B_DRAFT_18; break;
    case MOQ_VERSION_DRAFT_22: draft = B_DRAFT_22; break;
    default:                   return MOQR_ERR_INVAL;
    }
    moq_cluster_info_t ci;
    if (moq_session_cluster_info(session, &ci, sizeof(ci)) == MOQ_OK && ci.enabled &&
        (!b->cluster_info.enabled || ci.local_hop_id != b->cluster_info.local_hop_id))
        return MOQR_ERR_INVAL;
    if (conn_by_session(b, session) != NULL) {
        return MOQR_ERR_INVAL;   /* duplicate attach */
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used) {
            moqr_binding_t binding;
            moqr_binding_options_t options;
            moqr_binding_options_init_sized(&options, sizeof(options));
            options.flags = MOQR_BINDING_TRACK_METADATA_MANAGED;
            moqr_result_t rc = moqr_core_binding_open_with_options(
                b->core, (uint64_t)i + 1, &options, &binding);
            if (rc != MOQR_OK) {
                return rc;
            }
            cn->used = true;
            cn->closed = false;
            cn->session = session;
            cn->wire_draft = draft;
            cn->binding = binding;
            cn->cluster_peer_ready = false;
            cn->cluster_discovery_closed = false;
            cn->cluster_discovery_retiring = false;
            cn->cluster_discovery_cancel_owed = false;
            cn->cluster_negotiated = false;
            cn->cluster_discovery = (moq_ns_sub_handle_t){0};
            cn->cluster_cost_override = cluster != NULL && cluster->has_relay_cost;
            cn->cluster_cost = cluster != NULL ? cluster->relay_cost : 1;
            cn->cluster_correlation = cluster != NULL ? cluster->correlation_id : 0;
            bind_dl_clear(b->dl_ready, i);
            bind_dl_clear(b->dl_parked, i);
            cn->park_reason = B_PARK_NONE;
            cn->park_cap = 0;
#ifdef MOQR_BIND_TESTING
            memset(cn->dbg_blocked, 0, sizeof(cn->dbg_blocked));
            cn->dbg_bind_sg_track = 0;
            cn->dbg_bind_sg_track_gen = 0;
            cn->dbg_bind_sg_track_set = false;
            cn->dbg_bind_sg_ambiguous = false;
#endif
            memset(cn->dsubs, 0, (size_t)b->n_dsubs * sizeof(*cn->dsubs));
            bind_dgrant_conn_gone(b, cn);
            memset(cn->sgs, 0, (size_t)b->n_sgs * sizeof(*cn->sgs));
            usub_release_all(b, cn);   /* owned keys, then the clean table */
            memset(cn->pubs, 0, (size_t)b->n_pubs * sizeof(*cn->pubs));
            memset(cn->fetches, 0, (size_t)b->n_fetches * sizeof(*cn->fetches));
            cn->fetch_hw = 0;
            memset(cn->refusals, 0, (size_t)b->n_refusals * sizeof(*cn->refusals));
            cn->refusals_reserved = 0;
            cn->retry_pending = 0;
            /* The relay answers every downstream subscription update itself
             * (bind_update_answer), so nothing is acknowledged before the
             * relay has decided. */
            (void)moq_session_set_defer_subscription_updates(session, true);
            /* A FETCH range stated relative to the Largest Object is the
             * relay's to resolve, against its own log (bind_fetch_range). */
            (void)moq_session_set_defer_fetch_ranges(session, true);
            /* Draft-22 namespace updates are the relay's to authorize and
             * answer (bind_nsu_authorize); older drafts have none. */
            if (draft == B_DRAFT_22) {
                (void)moq_session_set_defer_namespace_updates(session, true);
            }
            return MOQR_OK;
        }
    }
    return MOQR_ERR_CAPACITY;
}


moqr_result_t
moqr_bind_conn_open(moqr_bind_t *b, moq_session_t *session, moq_version_t version)
{
    return bind_conn_open_cluster(b, session, version, NULL);
}


void
moqr_bind_cluster_peer_cfg_init_sized(moqr_bind_cluster_peer_cfg_t *cfg, size_t size)
{
    if (cfg == NULL || size < sizeof(uint32_t)) return;
    const moqr_bind_cluster_peer_cfg_t value = {0};
    moqr_cluster_write(cfg, size, &value, sizeof(value));
}


moq_result_t
moqr_bind_conn_open_cluster(moqr_bind_t *b, moq_session_t *session,
    moq_version_t version, const moqr_bind_cluster_peer_cfg_t *cfg)
{
    moqr_bind_cluster_peer_cfg_t value = {0};
    if (cfg != NULL && !moqr_cluster_read(cfg, MOQR_BIND_CLUSTER_PEER_CFG_V0_SIZE,
                                         &value, sizeof(value))) return MOQ_ERR_INVAL;
    return moqr_result_public(bind_conn_open_cluster(b, session, version, &value));
}


/* The integer codec of the Object and Track Properties a connection reads
 * and writes: its negotiated draft's (draft-16 (i); draft-18 and draft-22
 * vi64). */
uint8_t
bind_props_codec(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_16 ? MOQR_PROPS_CODEC_QUIC_VARINT
                                        : MOQR_PROPS_CODEC_VI64;
}


moq_version_t
moqr_bind_conn_version(const moqr_bind_t *b, const moq_session_t *session)
{
    const b_conn_t *cn = (b == NULL || session == NULL)
                             ? NULL
                             : conn_by_session((moqr_bind_t *)(void *)b,
                                               session);

    if (cn == NULL) {
        return (moq_version_t)0;   /* unattached names no draft */
    }
    switch (cn->wire_draft) {
    case B_DRAFT_22: return MOQ_VERSION_DRAFT_22;
    case B_DRAFT_18: return MOQ_VERSION_DRAFT_18;
    default:         return MOQ_VERSION_DRAFT_16;
    }
}


bool
moqr_bind_conn_is_open(const moqr_bind_t *b, const moq_session_t *session)
{
    return b != NULL && session != NULL &&
           conn_by_session((moqr_bind_t *)(void *)b, session) != NULL;
}


moqr_result_t
moqr_bind_conn_close(moqr_bind_t *b, moq_session_t *session)
{
    if (b == NULL || session == NULL) {
        return MOQR_ERR_INVAL;
    }
    b_conn_t *cn = conn_by_session(b, session);
    if (cn == NULL) {
        return MOQR_OK;   /* idempotent */
    }
    conn_detach(b, cn, 0);
    /* binding_close is resumable. A connection retained for the next pump
     * still owns its session pointer, so returning OK would falsely authorize
     * the caller to destroy storage the binding can still reference. */
    return conn_by_session(b, session) != NULL ? MOQR_ERR_WOULD_BLOCK
                                                : MOQR_OK;
}


/* -- request-capacity auto-grant (draft-16 MAX_REQUEST_ID) ---------------------- */

/* A peer-originated request that consumes one request id (draft-16 §9.5:
 * SUBSCRIBE, PUBLISH_NAMESPACE, FETCH, SUBSCRIBE_NAMESPACE, REQUEST_UPDATE,
 * TRACK_STATUS). PUBLISH / SUBSCRIBE_TRACKS are counted too — a superset only
 * over-estimates consumption, which grants EARLY (never late), so the peer can
 * never stall from an undercount. */
static bool
bind_event_is_request(uint32_t kind)
{
    switch (kind) {
    case MOQ_EVENT_SUBSCRIBE_REQUEST:
    case MOQ_EVENT_NAMESPACE_PUBLISHED:
    case MOQ_EVENT_NS_SUB_REQUEST:
    case MOQ_EVENT_TRACK_STATUS_REQUEST:
    case MOQ_EVENT_FETCH_REQUEST:
    case MOQ_EVENT_PUBLISH_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_UPDATED:
    case MOQ_EVENT_PUBLISH_UPDATED:
    case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST:
    case MOQ_EVENT_NS_SUB_UPDATE_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST:
    case MOQ_EVENT_NAMESPACE_UPDATE_RESULT:
    case MOQ_EVENT_NS_SUB_UPDATE_RESULT:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT:
        return true;
    default:
        return false;
    }
}


/* Keep the peer's request-id credit ahead of its consumption. Draft-18 has no
 * MAX_REQUEST_ID (local_request_capacity() == 0), so this is inert there. */
static void
bind_maybe_grant_capacity(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    uint64_t ceiling = moq_session_local_request_capacity(cn->session);
    if (ceiling == 0 || b->request_grant_window == 0) {
        return;   /* draft-18, or no local grant advertised */
    }
    /* Peer (client) request ids start at 0 and advance by 2 per request (the
     * relay owns the odd ids), so its position is 2 * requests_seen. Grant when
     * the remaining credit drops below half a window. */
    uint64_t consumed = cn->requests_seen * 2u;
    uint64_t headroom = ceiling > consumed ? ceiling - consumed : 0;
    if (headroom >= (uint64_t)b->request_grant_window / 2u) {
        return;
    }
    /* New ceiling is a full window above the peer's position: absolute and
     * strictly greater than the current ceiling (headroom < window/2 < window).
     * A WOULD_BLOCK does not advance the session's ceiling (grant contract), and
     * this runs every pump, so a backpressured grant is retried until it ships —
     * never lost, and the peer never waits forever. */
    uint64_t new_ceiling = consumed + b->request_grant_window;
    (void)moq_session_grant_request_capacity(cn->session, new_ceiling, now_us);
}


/* -- pump ----------------------------------------------------------------------- */

moqr_result_t
moqr_bind_pump(moqr_bind_t *b, uint64_t now_us)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump || b->pump_events == NULL || b->pump_revoked == NULL ||
        b->pump_intents == NULL) {
        return MOQR_ERR_INVAL;
    }
    b->in_pump = true;
    /* A bounded delivery/discard pass, or refusal recovery with events still
     * undrained, may owe another pump without an external edge. The runtime
     * turns that debt into one coalesced self-wake. */
    b->budget_pending = false;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used && !b->conns[i].closed) {
            b->conns[i].events_pending = true;
        }
    }
    /* Held rendezvous SUBSCRIBEs past their deadline end before any event
     * of this pump -- a source among them included -- is consumed. */
    (void)moqr_core_internal_rendezvous_expire(b->core, now_us);
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        size_t n;
        while (cn->used && !cn->closed) {
            if (!bind_refusal_can_poll(b, cn)) {
                /* Held refusals leave no room to own another request: this
                 * connection's events wait in its session until one commits. */
                FWD_DBG(b, polls_paused);
                break;
            }
            n = moq_session_poll_events(cn->session, b->pump_events,
                                        BIND_PUMP_EVENT_POLL);
            if (n == 0) {
                cn->events_pending = false;
                break;
            }
            for (size_t e = 0; e < n; e++) {
                moq_event_t *ev = &b->pump_events[e];
                if (bind_event_is_request(ev->kind)) {
                    cn->requests_seen++;
                }
                /* A fail-close must stop translation immediately — a DEFERRED
                 * detach (binding close held on intent space) leaves used=true
                 * with closed=true, and no further event of this conn may be
                 * ingested. */
                if (cn->used && !cn->closed) {
                    bind_on_event(b, cn, ev, now_us);
                }
                moq_event_cleanup(ev);
            }
        }
        /* Re-check the grant every pump (not only when events arrive): a peer
         * blocked at its ceiling sends nothing until the grant reaches it. */
        if (cn->used && !cn->closed && !cn->events_pending && cn->session != NULL) {
            if (!cn->cluster_discovery_closed && b->cluster_info.enabled) {
                for (uint32_t a = 0; a < b->n_anns && cn->used && !cn->closed; a++)
                    bind_cluster_apply_ann(b, cn, a, now_us);
            }
            if (!cn->used || cn->closed) continue;
            if (cn->cluster_discovery_cancel_owed)
                bind_cluster_discovery_cancel(b, cn, cn->cluster_discovery, now_us);
            else if (cn->cluster_discovery_retiring)
                bind_cluster_discovery_end(b, cn, cn->cluster_discovery._opaque, now_us);
            bind_cluster_sync(b, cn, now_us, true);
            if (!cn->used || cn->closed) continue;
            bind_maybe_grant_capacity(b, cn, now_us);
            bind_retry_owed_cancels(b, cn, now_us);
        }
    }
    /* All source connections have ingested their events now. A subscriber
     * earlier in the connection table must also reconcile bytes that arrived
     * on a later source, even while paused with no ordinary delivery. */
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && !cn->closed && !cn->events_pending) {
            (void)bind_discard_stopped(b, cn, now_us);
        }
    }
    (void)moqr_core_tick(b->core, now_us);
    bind_meta_sweep(b);
    /* Announce grants the tick revoked on revalidation: the core already
     * cleared its routing state and fanned NS_GONE to watchers, but the
     * publisher-side namespace withdrawal is a session write only the binding
     * can make. Peek them and send moq_session_cancel_namespace carrying the
     * denial code (custom DENY code or the UNAUTHORIZED default). Subscribe
     * revocations are fully core-driven (SUB_DONE via intents) and never appear
     * here. Ack (free the grant) once the cancel is on the wire; a WOULD_BLOCK
     * leaves the grant peekable and is retried next pump, after the transport
     * drains the session action queue — the core grant is the durable record,
     * so a backpressured cancel is never lost. A revoked grant on a gone/closed
     * conn is acked with no wire cancel (never act on a torn-down session).
     * Peek is non-draining, so this is a single bounded pass, not a while-loop. */
    size_t rn = moqr_core_peek_revoked_grants(b->core, b->pump_revoked,
                                              BIND_PUMP_REVOKED_BATCH);
    for (size_t k = 0; k < rn; k++) {
        moqr_revoked_grant_t *rev = &b->pump_revoked[k];
        if (rev->binding_cookie == 0 ||
            rev->binding_cookie - 1u >= b->max_conns) {
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
            continue;
        }
        b_conn_t *rcn = &b->conns[rev->binding_cookie - 1u];
        if (!rcn->used || rcn->closed || rcn->session == NULL) {
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
            continue;
        }
        if (rcn->events_pending) continue;
        moq_cancel_namespace_cfg_t ccfg;
        moq_cancel_namespace_cfg_init(&ccfg);
        ccfg.error_code = rev->error_code;
        int source_ann = ann_find(b, rcn, rev->session_cookie);
        bool discovered = source_ann >= 0 && rcn->anns[source_ann].discovered;
        moq_ns_sub_handle_t discovery = discovered
            ? rcn->anns[source_ann].discovery : (moq_ns_sub_handle_t){0};
        moq_result_t crc = discovered
            ? moq_session_cancel_namespace_sub(rcn->session, discovery, now_us)
            : moq_session_cancel_namespace(rcn->session,
                (moq_announcement_t){ ._opaque = rev->session_cookie }, &ccfg, now_us);
        if (discovered && crc != MOQ_ERR_WOULD_BLOCK)
            bind_cluster_discovery_end(b, rcn, discovery._opaque, now_us);
        if (crc == MOQ_OK) {
            int ann_slot = ann_find(b, rcn, rev->session_cookie);
            if (ann_slot >= 0) {
                ann_clear_slot(b, rcn, (uint32_t)ann_slot);
            }
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
        } else if (crc != MOQ_ERR_WOULD_BLOCK) {
            /* A hard error (e.g. wrong state) can never succeed on retry: drop
             * it rather than spin forever, retire the binding mirror whose
             * core route is already gone, and record the anomaly. */
            int ann_slot = ann_find(b, rcn, rev->session_cookie);
            if (ann_slot >= 0) {
                ann_clear_slot(b, rcn, (uint32_t)ann_slot);
            }
            b->session_errors++;
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
        }
        /* WOULD_BLOCK: leave the grant peekable; retried on the next pump. */
    }
    (void)bind_execute_intents(b, now_us);
    /* Retry closes that blocked on intent space (the ring is drained). */
    bool detach_retried = false;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && cn->detach_pending) {
            cn->closed = false;
            cn->detach_pending = false;
            conn_detach(b, cn, now_us);
            detach_retried = true;
        }
    }
    if (detach_retried) {
        (void)bind_execute_intents(b, now_us);
    }
    /* Forwarded Joining FETCH transactions: upstream requests/cancels and
     * downstream responses, each step bounded and retry-safe. */
    bind_fwd_pump(b, now_us);
    bind_refusal_pump(b, now_us);
    bind_update_answer_pump(b, now_us);
    bind_nsu_pump(b, now_us);
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && !cn->closed && !cn->events_pending &&
            cn->wire_draft == B_DRAFT_22) {
            bind_discovery_pump(b, cn, now_us);
        }
    }
    /* Hold delivery while an output intent is still parked: an object must
     * never overtake the ACCEPT/REJECT/DONE queued ahead of it. The backlog
     * drains first (next pump, after the transport frees queue room), then
     * objects flow. */
    if (b->pending_count == 0) {
        /* One complete bounded drain of the core-ready set into the bind
         * set (pseudo cookies are filtered at production; a real cookie is
         * slot + 1). Looping until a short batch means a per-call cap can
         * never defer a ready binding to a later pump. */
        uint64_t rdy[64];
        uint32_t got;
        do {
            got = moqr_core_drain_ready(b->core, rdy, 64);
            for (uint32_t k = 0; k < got; k++) {
                if (rdy[k] != 0 && rdy[k] - 1u < b->max_conns) {
                    bind_dl_set(b->dl_ready, (uint32_t)(rdy[k] - 1u));
                }
            }
        } while (got == 64);
        for (uint32_t i = 0; i < b->max_conns; i++) {
            b_conn_t *cn = &b->conns[i];
            if (cn->used && !cn->closed && !cn->events_pending) {
                bool ready = bind_dl_test(b->dl_ready, i);
                bool parked = bind_dl_test(b->dl_parked, i);
                bool attempt;
                if (parked && cn->park_reason == B_PARK_ACTION_CAP) {
                    /* One O(1) capacity read per pump, zero probes below
                     * the floor. A ready mark that lands meanwhile stays
                     * SET (never cleared unattempted), so the post-recovery
                     * pass sees it — no lost wakeup, no early probe. */
                    attempt = moq_session_action_capacity(cn->session) >
                              cn->park_cap;
                } else if (parked) {
                    /* SESSION_SG: exactly one bounded re-attempt per pump —
                     * each attempt's own open prologue performs the reap. */
                    attempt = true;
#ifdef MOQR_BIND_TESTING
                    atomic_fetch_add(&bind_dbg_sg_attempts, 1);
#endif
                } else {
                    attempt = ready;
                }
                if (attempt) {
                    /* Clear BEFORE the pass: a mark landing during it (a
                     * delivery-triggered state change) re-sets the bit and
                     * the next pump serves it — same thread, in order. */
                    bind_dl_clear(b->dl_ready, i);
                    switch (bind_deliver(b, cn, now_us)) {
                    case B_DELIVER_DRAINED:
                        bind_dl_clear(b->dl_parked, i);
                        cn->park_reason = B_PARK_NONE;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = false;   /* no longer pool-blocked */
                        break;
                    case B_DELIVER_BLOCKED_ACTION_CAP:
                        /* Park on the OBSERVED capacity floor: a property-
                         * bearing write needs TWO slots, so it can refuse
                         * with capacity 1 — the retry gate is capacity
                         * EXCEEDING what the refused call saw, not merely
                         * nonzero. (The subgroup-open case observed zero.) */
                        bind_dl_set(b->dl_parked, i);
                        cn->park_reason = B_PARK_ACTION_CAP;
                        cn->park_cap =
                            moq_session_action_capacity(cn->session);
                        cn->bind_sg_waiting = false;   /* blocked elsewhere */
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[0]);
#endif
                        break;
                    case B_DELIVER_BLOCKED_SESSION_SG:
                        bind_dl_set(b->dl_parked, i);
                        cn->park_reason = B_PARK_SESSION_SG;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = false;   /* blocked elsewhere */
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[1]);
#endif
                        break;
                    case B_DELIVER_BLOCKED_BIND_SG:
                        /* Never parked and never polled: latch that a delivery
                         * is waiting on this conn's subgroup-slot pool, so ONLY
                         * a later slot release for THIS conn re-arms the ready
                         * bit (bind_dl_rearm_on_slot_release). Ordinary closes
                         * with nothing waiting re-arm nothing. */
                        bind_dl_clear(b->dl_parked, i);
                        cn->park_reason = B_PARK_NONE;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = true;
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[2]);
#endif
                        break;
                    case B_DELIVER_BUDGET:
                        /* The guard expired mid-batch: the batch may be the
                         * only traffic there is, so surface it for the
                         * runtime's one further self-wake. RETRY below is
                         * deliberately NOT surfaced — its leftovers arise
                         * mid-anomaly during active traffic and ride the
                         * next pump any event earns (the ambient cadence). */
                        b->budget_pending = true;
                        /* fallthrough */
                    case B_DELIVER_RETRY:
                        /* Work remains that no future mark re-signals:
                         * self-rearm; parked state unchanged. Every other
                         * ready binding still got its own budget this
                         * pump, so the starvation bound is exactly the
                         * old one (guard x ready bindings). Not a pool block:
                         * the self-rearm above already schedules the retry. */
                        bind_dl_set(b->dl_ready, i);
                        break;
                    case B_DELIVER_CLOSED:
                        /* Every CLOSED path detached the connection, which
                         * cleared its scheduling state; the guarded clear
                         * below is pure defense against a future
                         * non-detaching CLOSED path. */
                        if (cn->used && !cn->closed) {
                            bind_dl_clear_conn(b, cn);
                        }
                        break;
                    }
                }
            }
            /* The fetch half is untouched: per open connection, every
             * pump, exactly as before. */
            if (cn->used && !cn->closed && !cn->events_pending) {
                bind_fetch_pump(b, cn, now_us);
            }
        }
        /* A delivery pass can end subscriptions: a sub whose source finished
         * or whose track ended retires once nothing is left to send, queueing
         * its DONE behind the data it closes. Execute those terminals in this
         * pump; nothing else is bound to bring a later one, so they would
         * otherwise wait for an unrelated event. */
        (void)bind_execute_intents(b, now_us);
    }
    /* Refusal recovery permits another event drain; it does not prove that
     * the deferred terminal event was seen. Preserve ready/park state and
     * arrange the next pump before any delivery can use an old handle. */
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && !cn->closed && cn->events_pending &&
            bind_refusal_can_poll(b, cn)) b->budget_pending = true;
    }
    b->in_pump = false;
    return MOQR_OK;
}


moqr_result_t
moqr_bind_set_fill_source(moqr_bind_t *b,
                          const moqr_bind_fill_source_t *source)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    const size_t need = offsetof(moqr_bind_fill_source_t, close) +
                        sizeof(source->close);
    if (source != NULL &&
        (source->struct_size < need || source->open == NULL ||
         source->peek == NULL || source->commit == NULL ||
         source->close == NULL)) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump) {
        return MOQR_ERR_WRONG_STATE;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t fi = 0; cn->used && fi < b->n_fetches; fi++) {
            if (cn->fetches[fi].used && cn->fetches[fi].fill_src_open) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    memset(&b->fill_src, 0, sizeof(b->fill_src));
    b->retrieve_source_peek = NULL;
    b->has_fill_src = source != NULL;
    if (source != NULL) {
        size_t n = source->struct_size < sizeof(b->fill_src)
                       ? source->struct_size
                       : sizeof(b->fill_src);
        memcpy(&b->fill_src, source, n);
        b->fill_src.struct_size = (uint32_t)sizeof(b->fill_src);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_bind_retrieve_source_set(moqr_bind_t *b, moqr_retrieve_source_peek_fn peek)
{
    if (b == NULL || (peek != NULL && !b->has_fill_src)) return MOQR_ERR_INVAL;
    if (b->in_pump) return MOQR_ERR_WRONG_STATE;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t j = 0; cn->used && j < b->n_fetches; j++) {
            if (cn->fetches[j].used && cn->fetches[j].fill) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    b->retrieve_source_peek = peek;
    return MOQR_OK;
}


moqr_result_t
moqr_bind_retrieve_standalone_set(moqr_bind_t *b,
                                  const moqr_retrieve_standalone_ops_t *ops)
{
    if (b == NULL || (ops != NULL &&
                      (ops->open == NULL || ops->plan == NULL ||
                       ops->peek == NULL || ops->commit == NULL ||
                       ops->close == NULL))) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump) return MOQR_ERR_WRONG_STATE;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t j = 0; cn->used && j < b->n_fetches; j++) {
            if (cn->fetches[j].used && cn->fetches[j].sa_remote) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    if (ops == NULL) {
        memset(&b->sa_src, 0, sizeof(b->sa_src));
        b->has_sa_src = false;
    } else {
        b->sa_src = *ops;
        b->has_sa_src = true;
    }
    return MOQR_OK;
}


moqr_result_t
moqr_bind_set_fill_timeouts(moqr_bind_t *b, uint64_t default_ms,
                            uint64_t max_ms)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint64_t def_us = 0;
    uint64_t max_us = 0;
    if (bind_fill_wait_resolve(default_ms, max_ms, &def_us, &max_us) !=
        MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    b->fill_wait_default_us = def_us;
    b->fill_wait_max_us = max_us;
    return MOQR_OK;
}


uint64_t
moqr_bind_next_deadline_us(const moqr_bind_t *b)
{
    uint64_t next = UINT64_MAX;
    if (b == NULL) {
        return next;
    }
    /* Includes owner-shard collectors even while their FIFO has no credit.
     * Cancellation is independent of whether the consumer is being peeked. */
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        const b_fwd_t *t = &b->fwd[i];
        if (t->used && t->consumer == FW_CONSUMER_FILL &&
            t->end == FW_END_NONE && t->retrieval.deadline_us < next) {
            next = t->retrieval.deadline_us;
        }
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        for (uint32_t fi = 0; fi < cn->fetch_hw; fi++) {
            const b_fetch_t *bf = &cn->fetches[fi];
            /* A fill reading through a fill source that reports its own
             * deadlines leaves its deadline to the source, which knows
             * whether the part it serves still waits on time. */
            bool by_source = bf->fill_src_open &&
                             b->fill_src.next_deadline_us != NULL;
            uint64_t fill_deadline = bf->retrieval.live
                ? moqr_bind_retrieve_next_deadline(&bf->retrieval)
                : bf->fill_deadline_us;
            if (bf->used && bf->fill && bf->state == BF_STREAMING &&
                !by_source && fill_deadline < next) {
                next = fill_deadline;
            }
            /* A standalone FETCH waiting for an origin to retrieve from
             * stops waiting at its deadline (an admitted upstream FETCH's
             * timer is its collector's, above). */
            if (bf->used && bf->standalone && bf->state == BF_STREAMING &&
                bf->retrieval.live) {
                uint64_t d = moqr_bind_retrieve_next_deadline(&bf->retrieval);
                if (d < next) next = d;
            }
        }
    }
    if (b->has_fill_src && b->fill_src.next_deadline_us != NULL) {
        uint64_t sd = b->fill_src.next_deadline_us(b->fill_src.ctx);
        if (sd < next) {
            next = sd;
        }
    }
    /* A SUBSCRIBE held waiting for a publisher ends at its deadline. */
    uint64_t cd = moqr_core_next_deadline_us(b->core);
    if (cd < next) {
        next = cd;
    }
    return next;
}


bool
moqr_bind_pump_budget_pending(const moqr_bind_t *b)
{
    return b != NULL && b->budget_pending;
}


bool
moqr_bind_pump_continuation_pending(const moqr_bind_t *b)
{
    /* Work left by the last pump that a self-continuation must carry forward,
     * REASON-AWARE (a self-wake only helps if a future pump would make progress
     * without any external edge):
     *   - any connection still READY (a mark that landed during a pass, which
     *     nothing else re-signals on this thread); or
     *   - a connection parked on SESSION_SG, whose bounded one-per-pump
     *     re-attempt (its open prologue performs the reap) needs a pump to
     *     happen and has no external edge to earn one.
     * A connection parked on ACTION_CAP is DELIBERATELY excluded: its retry
     * gate waits for downstream transport capacity to rise above the observed
     * floor — an external edge the managed adapter surfaces by arming the lane
     * pump from a transport-event batch (write completions etc.), a cause
     * distinct from a shard push/credit wake. An arbitrary self-continuation
     * would only spin while that capacity is unchanged. A fully-drained pass
     * leaves nothing here, so an applied batch that delivered everything earns
     * no self-wake. */
    if (b == NULL || b->dl_ready == NULL) {
        return false;
    }
    /* Scan only LIVE connections: a core-ready cookie for a slot with no live
     * bind connection (e.g. a direct-to-core binding the bind never opened a
     * connection for) aliases an unused dl_ready bit that the pump's delivery
     * loop never services and never clears. A raw word scan would treat that
     * sticky bit as residual and manufacture a self-continuation on every
     * inbound pop. Liveness is the guard. */
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        if (cn->events_pending) {
            if (bind_refusal_can_poll(b, cn)) return true;
            continue;   /* retained readiness cannot outrun the event drain */
        }
        if (bind_dl_test(b->dl_ready, i)) {
            return true;   /* a live connection has residual ready work */
        }
        if (b->dl_parked != NULL && bind_dl_test(b->dl_parked, i) &&
            cn->park_reason == B_PARK_SESSION_SG) {
            return true;   /* SESSION_SG re-attempt needs a pump */
        }
    }
    return false;
}


moqr_result_t
moqr_bind_auth_resolve(moqr_bind_t *b, uint64_t ticket,
                       const moqr_auth_verdict_t *result, uint64_t now_us)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_auth_verdict_t verdict;
    moqr_park_req_t     view;
    moqr_result_t rc = moqr_core_auth_begin_resolve_sized(
        b->core, ticket, result, now_us, &verdict, &view, sizeof(view));
    if (rc != MOQR_OK) {
        return rc; /* unknown / retired / stale -> no-op, nothing pinned */
    }
    /* Single cleanup path: every exit below finishes the resolve. Revalidate
     * the owning connection before touching any session — a closed/gone
     * binding means no-op (never act on a stale session). */
    uint64_t  slot = view.binding_cookie - 1u; /* cookie == slot + 1 */
    b_conn_t *cn = (view.binding_cookie != 0 && slot < b->max_conns)
                       ? &b->conns[slot]
                       : NULL;
    if (cn != NULL && cn->used && !cn->closed && cn->session != NULL) {
        /* The entry reserved at park time now backs whatever refusal the
         * resolution owes, so a blocked rejection is held, not lost. */
        bool reserved = view.action != MOQR_AUTH_PUBLISH &&
                        view.sub_filter_type != BIND_PARK_DISCOVERY &&
                        view.sub_filter_type != BIND_PARK_NS_UPDATE;
        if (reserved && cn->refusals_reserved != 0) {
            cn->refusals_reserved--;
        }
        b_meta_publish_t *p = view.action == MOQR_AUTH_PUBLISH
                                 ? bind_meta_pub_find(b, cn, view.session_cookie)
                                 : NULL;
        bool current = view.action != MOQR_AUTH_PUBLISH ||
                       (p != NULL && p->state == B_META_PUB_AUTH && p->ticket == ticket);
        /* A cancelled PUBLISH has no bind-owned snapshot to resume. */
        if (current && view.action == MOQR_AUTH_SUBSCRIBE &&
            view.sub_filter_type == BIND_PARK_DISCOVERY) {
            bind_discovery_resolved(b, cn, &view, &verdict, now_us);
        } else if (current && view.sub_filter_type == BIND_PARK_NS_UPDATE &&
                   bind_nsu_family(view.session_cookie) != 0) {
            bind_nsu_resolved(b, cn, &view, &verdict, now_us);
        } else if (current) {
            if (verdict.decision == MOQR_AUTH_ALLOW) {
                bind_resume(b, cn, &view, verdict.revalidate_after_us, now_us);
            } else {
                uint64_t err = verdict.error_code != 0
                                   ? verdict.error_code
                                   : MOQ_REQUEST_ERROR_UNAUTHORIZED;
                bind_reject(b, cn, view.action, view.session_cookie, err, now_us);
            }
        }
    }
    moqr_core_auth_finish_resolve(b->core, ticket);
    return MOQR_OK;
}
