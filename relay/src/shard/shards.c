#include "shards_internal.h"
#include "data_internal.h"
#include "demand_internal.h"
#include "fill_internal.h"
#include "journal_internal.h"
#include "mailbox_internal.h"
#include "placement_internal.h"

#define R_SHARDS_DEF_TRACE   512u

#define R_SHARDS_DEF_MAILBOX 256u



/* Checked capacity arithmetic: overflow poisons to UINT64_MAX, and the
 * describe rejects a poisoned total with INVAL — never a silent wrap. */
uint64_t
cap_add(uint64_t a, uint64_t b)
{
    return (UINT64_MAX - a < b) ? UINT64_MAX : a + b;
}


static uint64_t
cap_mul(uint64_t a, uint64_t b)
{
    if (a == 0 || b == 0) {
        return 0;
    }
    return (a > UINT64_MAX / b) ? UINT64_MAX : a * b;
}
/* PUBLISH_DONE status registry (distinct from REQUEST_ERROR): the track is
 * no longer being published by the departing owner. */
#define R_DONE_TRACK_ENDED   0x2u

/* Data-stream reset registry (distinct again): the publisher ended the
 * subscription; PUBLISH_DONE carries the detailed status. */
#define R_RESET_CANCELLED    0x1u


#define SHARDS_CFG_HAS(cfg, field)                       \
    (offsetof(moqr_shards_cfg_t, field) +                \
         sizeof(((moqr_shards_cfg_t *)0)->field) <= (cfg)->struct_size)


/* -- small allocator helpers ------------------------------------------------ */

void *
sh_alloc(const moq_alloc_t *a, size_t n)
{
    return a->alloc(n, a->ctx);
}


void
sh_free(const moq_alloc_t *a, void *p, size_t n)
{
    if (p != NULL) {
        a->free(p, n, a->ctx);
    }
}


/* -- create / destroy of the per-shard manager ------------------------------ */

static void
mgr_free(r_mgr *m)
{
    if (m == NULL) {
        return;
    }
    const moq_alloc_t *a = &m->alloc;
    if (m->cluster != NULL) {
        for (uint32_t i = 0; i < m->s->cluster_routes; ++i) {
            sh_free(a, m->cluster[i].canon, m->cluster[i].canon_len);
            sh_free(a, m->cluster[i].hops,
                    m->cluster[i].record.route.metadata.hop_count * sizeof(uint64_t));
        }
        sh_free(a, m->cluster,
                (size_t)m->s->cluster_routes * sizeof(*m->cluster));
    }
    if (m->jrn != NULL) {
        for (uint32_t i = 0; i < m->jrn_len; i++) {
            sh_free(a, m->jrn[i].canon, m->jrn[i].canon_len);
        }
        sh_free(a, m->jrn, (size_t)m->jrn_cap * sizeof(m->jrn[0]));
    }
    if (m->pend != NULL) {
        for (uint32_t i = 0; i < m->pend_len; i++) {
            sh_free(a, m->pend[i].canon, m->pend[i].canon_len);
        }
        sh_free(a, m->pend, (size_t)m->pend_cap * sizeof(m->pend[0]));
    }
    sh_free(a, m->psub, (size_t)m->psub_cap * sizeof(m->psub[0]));
    sh_free(a, m->psg,
            (size_t)m->psub_cap * m->s->sg_slots * sizeof(r_psg_t));
    sh_free(a, m->popen,
            (size_t)m->pend_cap * m->s->sg_slots * sizeof(r_psg_t));
    sh_free(a, m->popen_row_used, m->pend_cap);
    if (m->fret != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            for (uint32_t k = 0; k < R_FILL_WINDOW; k++) {
                moq_rcbuf_decref(m->fret[i].win[k].payload);
                moq_rcbuf_decref(m->fret[i].win[k].props);
            }
            moq_rcbuf_decref(m->fret[i].meta_props);
            sh_free(a, m->fret[i].canon, m->fret[i].canon_len);
        }
        sh_free(a, m->fret, (size_t)m->fill_cap * sizeof(m->fret[0]));
    }
    if (m->oret != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            /* bind_destroy already released collector/cached copies. */
            moqr_bind_retrieve_forget(&m->oret[i].cursor);
            moq_rcbuf_decref(m->oret[i].meta_props);
        }
        sh_free(a, m->oret, (size_t)m->fill_cap * sizeof(m->oret[0]));
    }
    if (m->pb != NULL) {
        sh_free(a, m->pb, (size_t)m->s->shard_count * sizeof(m->pb[0]));
    }
    sh_free(a, m, sizeof(*m));
}


static r_mgr *
mgr_create(moqr_shards_t *s, uint16_t shard, moqr_core_t *core)
{
    const moq_alloc_t *a = &s->alloc;
    r_mgr *m = sh_alloc(a, sizeof(*m));
    if (m == NULL) {
        return NULL;
    }
    memset(m, 0, sizeof(*m));
    m->s = s;
    m->alloc = s->alloc;
    m->shard = shard;
    m->core = core;
    m->jrn_cap = s->jrn_cap;
    m->pend_cap = s->pend_cap;
    m->psub_cap = s->pend_cap;
    if (s->cluster_enabled) {
        m->cluster = sh_alloc(a, (size_t)s->cluster_routes * sizeof(*m->cluster));
        if (m->cluster == NULL) {
            mgr_free(m);
            return NULL;
        }
        memset(m->cluster, 0, (size_t)s->cluster_routes * sizeof(*m->cluster));
        for (uint32_t i = 0; i < s->cluster_routes; ++i)
            m->cluster[i].sent = UINT64_MAX;
    }
    m->jrn = sh_alloc(a, (size_t)m->jrn_cap * sizeof(m->jrn[0]));
    m->pend = sh_alloc(a, (size_t)m->pend_cap * sizeof(m->pend[0]));
    m->psub = sh_alloc(a, (size_t)m->psub_cap * sizeof(m->psub[0]));
    if (s->admit_remote) {
        /* Subgroup-progress storage exists only where admitted data can
         * flow; a refusal-only runtime stays structurally inert — nothing
         * reserved, nothing to validate. */
        m->psg = sh_alloc(a,
                          (size_t)m->psub_cap * s->sg_slots * sizeof(r_psg_t));
        m->popen =
            sh_alloc(a, (size_t)m->pend_cap * s->sg_slots * sizeof(r_psg_t));
        m->popen_row_used = sh_alloc(a, m->pend_cap);
        m->fill_cap = s->fill_cap;
        m->fret = sh_alloc(a, (size_t)m->fill_cap * sizeof(m->fret[0]));
        m->oret = sh_alloc(a, (size_t)m->fill_cap * sizeof(m->oret[0]));
        if (m->fret != NULL) {
            memset(m->fret, 0, (size_t)m->fill_cap * sizeof(m->fret[0]));
        }
        if (m->oret != NULL) {
            memset(m->oret, 0, (size_t)m->fill_cap * sizeof(m->oret[0]));
        }
    }
    m->pb = sh_alloc(a, (size_t)s->shard_count * sizeof(m->pb[0]));
    if (m->jrn == NULL || m->pend == NULL || m->psub == NULL ||
        (s->admit_remote &&
         (m->psg == NULL || m->popen == NULL ||
          m->popen_row_used == NULL ||
          (m->fill_cap > 0 && (m->fret == NULL || m->oret == NULL)))) ||
        m->pb == NULL) {
        mgr_free(m);
        return NULL;
    }
    memset(m->psub, 0, (size_t)m->psub_cap * sizeof(m->psub[0]));
    if (s->admit_remote) {
        memset(m->psg, 0,
               (size_t)m->psub_cap * s->sg_slots * sizeof(r_psg_t));
        memset(m->popen, 0,
               (size_t)m->pend_cap * s->sg_slots * sizeof(r_psg_t));
        memset(m->popen_row_used, 0, m->pend_cap);
    }
    memset(m->pb, 0, (size_t)s->shard_count * sizeof(m->pb[0]));
    return m;
}


/* Open the pseudo-bindings + the wildcard watcher on the manager's core. */
static moqr_result_t
mgr_open_bindings(r_mgr *m)
{
    uint16_t k = m->s->shard_count;
    moqr_binding_options_t options;
    moqr_binding_options_init_sized(&options, sizeof(options));
    options.flags = MOQR_BINDING_TRACK_METADATA_MANAGED;
    for (uint16_t j = 0; j < k; j++) {
        if (j == m->shard) {
            continue;
        }
        moqr_result_t rc = moqr_core_binding_open_with_options(
            m->core, MOQR_SHARD_COOKIE_BASE + j, &options, &m->pb[j]);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    moqr_result_t rc = moqr_core_binding_open_with_options(
        m->core, MOQR_SHARD_COOKIE_BASE + k, &options, &m->mgr_bind);
    if (rc != MOQR_OK) {
        return rc;
    }
    if (m->s->cluster_enabled) {
        return MOQR_OK; /* snapshots enumerate all sources directly */
    }
    /* Zero-count wildcard prefix: matches every announce; opened on an empty
     * trie at create, so there is no replay backlog to overflow the intent
     * ring. */
    moqr_ns_t wildcard = { NULL, 0 };
    return moqr_core_ns_subscribe(m->core, m->mgr_bind, wildcard,
                                  MOQR_SHARD_COOKIE_BASE + k);
}


static void
shard_step_order(const moqr_shards_t *s, uint16_t *order)
{
    uint16_t n = s->shard_count;
    for (uint16_t i = 0; i < n; i++) {
        order[i] = i;
    }
    if (s->permute_seed == 0 || n <= 1) {
        return;
    }
    uint64_t r = r_splitmix64(s->permute_seed ^ s->round);
    for (uint16_t i = n; i > 1; i--) {
        r = r_splitmix64(r);
        uint16_t j = (uint16_t)(r % i);
        uint16_t tmp = order[i - 1];
        order[i - 1] = order[j];
        order[j] = tmp;
    }
}


/* The per-shard step seam: run the six phases for exactly ONE shard. Touches
 * only this shard's {core, bind, mgr} plus its mailbox and channel endpoints
 * (inbound consumes src→shard rings; outbound pushes shard→dst rings).
 * Returns the bind pump's result verbatim; fail-stop aggregation (router
 * oom) and the round barrier stay with the runner. `pushed_dst_mask`
 * (optional) reports the step's WAKE SET: every destination that received an
 * outbound push (control mailbox, demand-channel control, or demand-channel
 * data) plus — merged by the step_shard wrapper — every producer whose
 * channel toward this shard regained capacity through a durable pop (the
 * producer credit). The seam a runtime uses to wake exactly the shards with
 * new inbound state or newly freed outbound room. */
static moqr_result_t
shards_step_one(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                uint64_t *pushed_dst_mask)
{
    shard_inbound_batch(s, shard);                              /* phase 1 */
    shard_demand_inbound(s, shard, now_us, pushed_dst_mask);    /*   ..    */
    moqr_result_t rc = moqr_bind_pump(s->shards[shard].bind, now_us); /* 2 */
    r_mgr *m = s->shards[shard].mgr;
    if (rc == MOQR_OK && m != NULL && m->cluster_inbound_retry) {
        m->cluster_inbound_retry = false;
        shard_inbound_batch(s, shard);
        /* A successful import marks its newly queued fan-out for a local
         * drain. A still-refused import owns its slot without self-spinning. */
    }
    shard_reconcile(s, shard, now_us);                          /* phase 3 */
    shard_outbound(s, shard, pushed_dst_mask);                  /* phase 4 */
    shard_demand_phase(s, shard, now_us, pushed_dst_mask);      /* phase 5 */
    shard_data_phase(s, shard, now_us, pushed_dst_mask);        /* phase 6 */
    return rc;
}


/* Advance the round barrier exactly once, after EVERY shard has stepped:
 * pushes stamped this round become visible next round, which is what makes a
 * round's result invariant under the intra-round shard order. */
void
shards_round_advance(moqr_shards_t *s)
{
    s->round++;
}


/* Population count of a wake mask (portable, branch-per-set-bit). */
static uint32_t
mask_popcount(uint64_t m)
{
    uint32_t n = 0;
    while (m != 0) {
        m &= m - 1u;
        n++;
    }
    return n;
}


/* One ACCOUNTED shard step — the single helper BOTH runners use, so the
 * wake-cause counters mean the same thing everywhere. The push and credit
 * masks stay separate until the accounting: each cause adds popcount(its
 * mask) once per step — several messages toward one destination in one
 * step are ONE wake request — and only then do they merge into the
 * caller's wake set (the deterministic runner discards the merged mask but
 * still records both causes). */
static moqr_result_t
shards_step_accounted(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                      uint64_t *pushed_dst_mask)
{
    s->shards[shard].pop_wake = 0;
    r_mgr *wm = s->shards[shard].mgr;
    if (wm != NULL) {
        wm->inbound_data_applied = false;
        wm->reconcile_owed_local_drain = false;
        wm->cluster_inbound_retry = false;
        wm->fill_wake_owed = false;
    }
    uint64_t push_mask = 0;
    moqr_result_t rc = shards_step_one(s, shard, now_us, &push_mask);
    uint64_t credit_mask = s->shards[shard].pop_wake;
    /* The LOCAL continuation, from four exact causes and nothing broader:
     * (a) this step applied inbound data into its own core AND its bind pump
     * (one pass, already run) left work a self-wake can carry forward —
     * REASON-AWARE per moqr_bind_pump_continuation_pending: a ready mark that
     * landed during the pass, or a SESSION_SG-parked conn (whose one-per-pump
     * re-attempt has no external edge). An ACTION_CAP-parked conn is NOT it: it
     * awaits a downstream transport-capacity edge (the managed adapter arms the
     * lane pump from a transport-event batch — a cause distinct from a shard
     * push/credit wake). (b) that pump pass ended ON THE DELIVERY-BUDGET GUARD with work
     * remaining. Both are the leftover-work outcome no future event re-signals,
     * which is how a batch bigger than one pass drains across a FINITE chain
     * (one self-wake per unfinished pass) instead of stranding one hop later. (c)
     * this step's reconcile left core intents owed a drain, its namespace
     * fan-out having landed in the intent ring AFTER the one pump pass that
     * could have taken it. (d) a fill-retrieval slot freed after a fill was
     * refused one: that fill retries its claim on this shard's next pump,
     * which nothing else is bound to bring. All four are the leftover-work
     * outcome no future event re-signals. A
     * distinct cause, never folded into push (no message crossed toward anyone)
     * or credit (no producer regained room). Applying inbound data whose pass
     * DRAINS EVERYTHING (or leaves only ACTION_CAP backpressure) requests
     * nothing, and a reconcile that converges without queueing anything
     * requests nothing, so steady traffic that keeps up earns no spurious
     * self-wakes. */
    bool continue_local =
        (wm != NULL && wm->inbound_data_applied &&
         moqr_bind_pump_continuation_pending(s->shards[shard].bind)) ||
        moqr_bind_pump_budget_pending(s->shards[shard].bind) ||
        (wm != NULL && wm->reconcile_owed_local_drain) ||
        (wm != NULL && wm->fill_wake_owed);
    uint64_t local_mask = continue_local ? (1ull << shard) : 0;
    if (wm != NULL) {
        wm->wake_req_push += mask_popcount(push_mask);
        wm->wake_req_credit += mask_popcount(credit_mask);
        wm->wake_req_local += mask_popcount(local_mask);
    }
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask = push_mask | credit_mask | local_mask;
    }
    return rc;
}


moqr_result_t
moqr_shards_step(moqr_shards_t *s, uint64_t now_us)
{
    if (s == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint16_t order[MOQR_SHARDS_MAX];
    shard_step_order(s, order);
    moqr_result_t result = MOQR_OK;
    for (uint16_t p = 0; p < s->shard_count; p++) {
        moqr_result_t rc = shards_step_accounted(s, order[p], now_us, NULL);
        if (rc != MOQR_OK && result == MOQR_OK) {
            result = rc;
        }
    }
    /* A local NS_FOUND/NS_GONE the router could not record (its canonical key
     * would not allocate, or the journal is full) is a BORROWED view that cannot
     * be parked or replayed — it is gone. Surface it so the runtime never reports
     * convergence over a candidate set that silently lost an observation. */
    for (uint16_t i = 0; i < s->shard_count && result == MOQR_OK; i++) {
        r_mgr *m = s->shards[i].mgr;
        if (m != NULL && m->oom) result = MOQR_ERR_NOMEM;
        else if (m != NULL && m->cluster_error != MOQR_OK) result = m->cluster_error;
    }
    shards_round_advance(s);
    return result;
}


/* -- lifecycle -------------------------------------------------------------- */

void
moqr_shards_cfg_init_sized(moqr_shards_cfg_t *cfg, size_t cfg_size,
                           const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_shards_cfg_t) ? cfg_size
                                                    : sizeof(moqr_shards_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_shards_cfg_t, alloc) +
            sizeof(((moqr_shards_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
    if (offsetof(moqr_shards_cfg_t, core_cfg) +
            sizeof(((moqr_shards_cfg_t *)0)->core_cfg) <= n) {
        moqr_core_relay_cfg_init_sized(&cfg->core_cfg, sizeof(cfg->core_cfg),
                                       alloc);
    }
    if (offsetof(moqr_shards_cfg_t, bind_cfg) +
            sizeof(((moqr_shards_cfg_t *)0)->bind_cfg) <= n) {
        moqr_bind_cfg_init_sized(&cfg->bind_cfg, sizeof(cfg->bind_cfg), alloc);
    }
}


/* Per-shard core/bind templates: the caller's nested config only when the
 * outer prefix reaches it AND it is fully initialized; else a library
 * default. Shared by create, the resolver, and the capacity describe. */
/* Fill-retrieval slots per shard and role: the resolved core fetch-cursor
 * pool, which bounds the fills a shard can serve at once and the cursors an
 * owner can lend to retrievals. Zero without admission. */
static uint32_t
shards_fill_slots(const moqr_shards_limits_t *lim,
                  const moqr_core_limits_t *clim)
{
    return lim->admit ? clim->max_fetches : 0u;
}


static void
shards_templates(const moqr_shards_cfg_t *cfg, const moq_alloc_t *a,
                 moqr_core_relay_cfg_t *core_tmpl, moqr_bind_cfg_t *bind_tmpl)
{
    if (SHARDS_CFG_HAS(cfg, core_cfg) &&
        cfg->core_cfg.struct_size == sizeof(cfg->core_cfg)) {
        *core_tmpl = cfg->core_cfg;
    } else {
        moqr_core_relay_cfg_init_sized(core_tmpl, sizeof(*core_tmpl), a);
    }
    if (SHARDS_CFG_HAS(cfg, bind_cfg) &&
        cfg->bind_cfg.struct_size == sizeof(cfg->bind_cfg)) {
        *bind_tmpl = cfg->bind_cfg;
    } else {
        moqr_bind_cfg_init_sized(bind_tmpl, sizeof(*bind_tmpl), a);
    }
}


static moqr_cluster_cfg_t
shards_cluster_cfg(const moqr_shards_cfg_t *cfg)
{
    moqr_cluster_cfg_t out;
    moqr_cluster_cfg_init_sized(&out, sizeof(out));
    out.enabled = SHARDS_CFG_HAS(cfg, cluster_enabled) && cfg->cluster_enabled;
    out.local_hop_id = SHARDS_CFG_HAS(cfg, cluster_hop_id) ? cfg->cluster_hop_id : 0;
    out.max_routes = SHARDS_CFG_HAS(cfg, max_cluster_routes) ? cfg->max_cluster_routes : 0;
    out.max_hops = SHARDS_CFG_HAS(cfg, max_cluster_hops) ? cfg->max_cluster_hops : 0;
    return out;
}


moqr_result_t
moqr_shards_cfg_resolve(const moqr_shards_cfg_t *cfg,
                        moqr_shards_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a =
        (SHARDS_CFG_HAS(cfg, alloc) && cfg->alloc != NULL) ? cfg->alloc : NULL;
    uint32_t k = (SHARDS_CFG_HAS(cfg, shards) && cfg->shards != 0)
                     ? cfg->shards
                     : 1u;
    if (k > MOQR_SHARDS_MAX) {
        return MOQR_ERR_INVAL;
    }
    out->shards = (uint16_t)k;
    /* Admission is effective only at K>1: a single lane builds no manager, so
     * the progress tables are never reserved and a forced admit_remote_demand
     * is structurally inert. Normalize here so the resolved view, the capacity
     * descriptor, and s->admit_remote all agree on effective admission. */
    out->admit = k > 1u && SHARDS_CFG_HAS(cfg, admit_remote_demand) &&
                 cfg->admit_remote_demand;
    out->mbox_cap =
        (SHARDS_CFG_HAS(cfg, mailbox_entries) && cfg->mailbox_entries != 0)
            ? cfg->mailbox_entries
            : R_SHARDS_DEF_MAILBOX;
    out->trace_ring =
        (SHARDS_CFG_HAS(cfg, trace_ring_records) &&
         cfg->trace_ring_records != 0)
            ? cfg->trace_ring_records
            : R_SHARDS_DEF_TRACE;

    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);

    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    if (cluster_cfg.enabled) {
        /* The reserved withdrawal contributes both allocation bytes and one
         * pending count. Refuse before either arithmetic can wrap. */
        uint64_t slots = (uint64_t)out->mbox_cap + 1u;
        if (k > 1u && (slots > UINT32_MAX || slots > SIZE_MAX / sizeof(r_ctrl_msg)))
            return MOQR_ERR_INVAL;
        moqr_core_capacity_t capacity;
        if (moqr_core_cluster_capacity_describe(&core_tmpl, &cluster_cfg, &capacity, sizeof(capacity)) != MOQ_OK)
            return MOQR_ERR_INVAL;
    }

    out->jrn_cap =
        (SHARDS_CFG_HAS(cfg, journal_entries) && cfg->journal_entries != 0)
            ? cfg->journal_entries
            : ((core_tmpl.max_ns_nodes != 0) ? core_tmpl.max_ns_nodes : 256u);
    out->pend_cap =
        (SHARDS_CFG_HAS(cfg, pending_demand_entries) &&
         cfg->pending_demand_entries != 0)
            ? cfg->pending_demand_entries
            : ((core_tmpl.max_tracks != 0) ? core_tmpl.max_tracks : 64u);
    out->dch_cap =
        (SHARDS_CFG_HAS(cfg, demand_channel_entries) &&
         cfg->demand_channel_entries != 0)
            ? cfg->demand_channel_entries
            : out->pend_cap;
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), a);
        lc.budget = core_tmpl.log_budget;
        moqr_log_capacity_t logc;
        if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
            return MOQR_ERR_INVAL;   /* a wrapped log model must never feed
                                      * a zero channel byte cap */
        }
        uint64_t resolved = logc.payload_bytes;
        uint64_t want = SHARDS_CFG_HAS(cfg, demand_channel_bytes)
                            ? cfg->demand_channel_bytes
                            : 0;
        if (want != 0 && want < resolved) {
            return MOQR_ERR_INVAL;   /* a legal record must always fit */
        }
        out->dch_byte_cap = want != 0 ? want : resolved;
    }
    out->pump_turn_msgs =
        (SHARDS_CFG_HAS(cfg, pump_turn_messages) &&
         cfg->pump_turn_messages != 0)
            ? cfg->pump_turn_messages
            : 64u;
    out->pump_turn_bytes =
        (SHARDS_CFG_HAS(cfg, pump_turn_bytes) && cfg->pump_turn_bytes != 0)
            ? cfg->pump_turn_bytes
            : out->dch_byte_cap / 4u;
    out->sg_slots =
        (SHARDS_CFG_HAS(cfg, pump_subgroup_slots) &&
         cfg->pump_subgroup_slots != 0)
            ? cfg->pump_subgroup_slots
            : (bind_tmpl.max_open_subgroups != 0
                   ? bind_tmpl.max_open_subgroups
                   : MOQR_BIND_DEF_OPEN_SUBGROUPS);
    if (k > 1u && out->admit) {
        uint64_t rows = (uint64_t)out->pend_cap * out->sg_slots;
        if (rows > UINT32_MAX || rows > SIZE_MAX / sizeof(r_psg_t)) {
            return MOQR_ERR_INVAL;
        }
    }
    /* Binding budget: at K>1 every shard's manager consumes K core binding
     * slots (K-1 origin pseudo-bindings + the manager binding), so the core
     * pool must leave at least one external slot. K=1 has no manager and
     * reserves nothing. usable = what a transport may actually admit. */
    moqr_core_limits_t clim;
    if (moqr_core_limits_resolve(&core_tmpl, &clim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    if (k > 1u && clim.max_bindings <= k) {
        return MOQR_ERR_INVAL;
    }
    moqr_bind_limits_t blim;
    if (moqr_bind_cfg_resolve(&bind_tmpl, &clim, &blim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    uint32_t external =
        (k > 1u) ? clim.max_bindings - k : clim.max_bindings;
    out->usable_bindings =
        blim.max_conns < external ? blim.max_conns : external;
    return MOQR_OK;
}


moqr_result_t
moqr_shards_capacity_describe(const moqr_shards_cfg_t *cfg,
                              moqr_shards_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    moqr_shards_limits_t lim;
    moqr_result_t rc = moqr_shards_cfg_resolve(cfg, &lim);
    if (rc != MOQR_OK) {
        return rc;
    }
    const moq_alloc_t *a =
        (SHARDS_CFG_HAS(cfg, alloc) && cfg->alloc != NULL) ? cfg->alloc : NULL;
    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);
    moqr_core_limits_t clim;
    (void)moqr_core_limits_resolve(&core_tmpl, &clim);

    uint64_t K = lim.shards;
    uint64_t K2 = cap_mul(K, K);

    moqr_core_capacity_t cc;
    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    if (moqr_core_cluster_capacity_describe(&core_tmpl, &cluster_cfg, &cc, sizeof(cc)) != MOQ_OK) {
        return MOQR_ERR_INVAL;
    }
    moqr_bind_capacity_t bc;
    moqr_cluster_info_t cluster_info;
    if (moqr_cluster_cfg_resolve(&cluster_cfg, &cluster_info, sizeof(cluster_info)) != MOQ_OK)
        return MOQR_ERR_INVAL;
    if (moqr_bind_cluster_capacity_describe(&bind_tmpl, &clim, &cluster_info, &bc, sizeof(bc)) != MOQ_OK) {
        return MOQR_ERR_INVAL;
    }
    out->core_structure_bytes = cc.structure_bytes;
    out->core_payload_bytes = cc.payload_bytes;
    out->bind_structure_bytes = bc.total_bytes;
    out->trace_bytes = moqr_trace_bytes(lim.trace_ring);
    out->per_shard_bytes =
        cap_add(cap_add(cc.total_bytes, bc.total_bytes), out->trace_bytes);

    /* Runtime container + the K>1 pools (§1 terms 1,2,6..18). */
    uint64_t st = cap_add(sizeof(moqr_shards_t),
                          cap_mul(K, sizeof(r_shard_t)));
    if (K > 1u) {
        st = cap_add(st, cap_mul(K2, cap_add(sizeof(r_mailbox),
                                             cap_mul((uint64_t)lim.mbox_cap + (cluster_cfg.enabled ? 1u : 0u),
                                                     sizeof(r_ctrl_msg)))));
        st = cap_add(st,
                     cap_mul(K2, cap_add(sizeof(r_demand_channel_t),
                                         cap_mul(lim.dch_cap,
                                                 sizeof(r_demand_msg_t)))));
        st = cap_add(st, K2);   /* the arbiter byte per directed pair */
        uint64_t mgr = cap_add(
            sizeof(r_mgr),
            cap_add(cap_mul(lim.jrn_cap, sizeof(r_jentry)),
                    cap_mul(lim.pend_cap,
                            sizeof(r_pdemand) + sizeof(r_psub))));
        st = cap_add(st, cap_mul(K, mgr));
        if (cluster_cfg.enabled) {
            uint64_t routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
            st = cap_add(st, cap_mul(K, cap_mul(routes, sizeof(r_cluster_snapshot))));
        }
        st = cap_add(st, cap_mul(K2, sizeof(moqr_binding_t)));
        if (lim.admit) {
            uint64_t rows = cap_mul(2u * (uint64_t)lim.pend_cap,
                                    lim.sg_slots);
            st = cap_add(st, cap_mul(K, cap_add(cap_mul(rows,
                                                        sizeof(r_psg_t)),
                                                lim.pend_cap)));
            /* Fill-retrieval tables. The window items' payload is charged
             * to the channel that carried it, inside channel_byte_ceiling. */
            st = cap_add(st, cap_mul(K, cap_mul(shards_fill_slots(&lim,
                                                                  &clim),
                                                sizeof(r_fret_t) +
                                                    sizeof(r_oret_t))));
        }
    }
    out->shards_structure_bytes = st;

    size_t hdr = 0;
    (void)moq_rcbuf_allocation_size(0, &hdr);
    if (K > 1u) {
        /* Channel ceiling: the byte gauge gates DATA only — control kinds
         * are counted but exempt, so every slot may additionally hold a
         * full-track canon key or a P-sized Track Properties clone. The
         * existing two clone headers per slot also cover metadata. */
        uint64_t per_ch = cap_add(
            lim.dch_byte_cap,
            cap_mul(lim.dch_cap,
                    cap_add(cap_add(R_CANON_FT_MAX, MOQR_BIND_META_SET_MAX),
                            cap_mul(2u, hdr))));
        out->channel_byte_ceiling = cap_mul(K2, per_ch);
        out->canon_byte_ceiling = cap_add(
            cap_mul(K2, cap_mul(lim.mbox_cap, R_CANON_NS_MAX)),
            cap_add(cap_mul(K, cap_mul(lim.jrn_cap, R_CANON_NS_MAX)),
                    cap_mul(K, cap_mul(lim.pend_cap, R_CANON_FT_MAX))));
        /* A requester retrieval slot routing a standalone FETCH to the
         * owning shard owns that request's full-track key until its REQ
         * carries it (admission only). */
        out->canon_byte_ceiling = cap_add(
            out->canon_byte_ceiling,
            cap_mul(K, cap_mul(shards_fill_slots(&lim, &clim),
                               R_CANON_FT_MAX)));
        /* Each standalone retrieval slot, owner and requester side, holds
         * at most one Track Properties copy of at most a full set: the
         * owner's from admission until OPEN carries it, the requester's from
         * OPEN until the request closes. In between it is the channel's,
         * inside channel_byte_ceiling, and never in two places at once. */
        if (lim.admit) {
            out->canon_byte_ceiling = cap_add(
                out->canon_byte_ceiling,
                cap_mul(K, cap_mul(cap_mul(2u, shards_fill_slots(&lim, &clim)),
                                   cap_add(MOQR_BIND_META_SET_MAX, hdr))));
        }
        /* Allocate-before-push staging: one message's owned material per
         * concurrently stepping source shard (a channel-bounded record/reason +
         * two clone headers, a full-track DEMAND key, or P-sized metadata
         * plus its clone header). Owner pins/requester directory copies
         * remain inside each binding's separately charged metadata budget. */
        {
            moqr_log_cfg_t lc;
            moqr_log_cfg_init_sized(&lc, sizeof(lc), a);
            lc.budget = core_tmpl.log_budget;
            moqr_log_capacity_t logc;
            if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
                memset(out, 0, sizeof(*out));
                return MOQR_ERR_INVAL;
            }
            out->staging_byte_ceiling = cap_mul(
                K, cap_add(lim.dch_byte_cap,
                           cap_add(cap_add(cap_mul(2u, hdr), R_CANON_FT_MAX),
                                   cap_add(MOQR_BIND_META_SET_MAX, hdr))));
        }
    }

    if (K > 1u && cluster_cfg.enabled) {
        uint64_t routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
        uint64_t hops = cluster_cfg.max_hops != 0 ? cluster_cfg.max_hops : 32u;
        uint64_t hop_bytes = cap_mul(hops, sizeof(uint64_t));
        out->canon_byte_ceiling = cap_add(out->canon_byte_ceiling,
            cap_add(cap_mul(K, cap_mul(routes, cap_add(R_CANON_NS_MAX, hop_bytes))),
                    cap_mul(K2, cap_mul(lim.mbox_cap, hop_bytes))));
        /* Snapshot replacement and mailbox push allocate before releasing
         * their old bytes; at most one such operation per source shard. */
        out->staging_byte_ceiling = cap_add(out->staging_byte_ceiling,
            cap_mul(K, cap_add(R_CANON_NS_MAX, hop_bytes)));
    }

    uint64_t total = cap_mul(K, out->per_shard_bytes);
    total = cap_add(total, out->shards_structure_bytes);
    total = cap_add(total, out->channel_byte_ceiling);
    total = cap_add(total, out->canon_byte_ceiling);
    total = cap_add(total, out->staging_byte_ceiling);
    if (total == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* poisoned by overflow: refuse */
    }
    out->relay_alloc_ceiling = total;
    out->usable_bindings_per_shard = lim.usable_bindings;
    out->shards = lim.shards;
    out->admission = lim.admit;
    return MOQR_OK;
}


moqr_result_t
moqr_shards_create(const moqr_shards_cfg_t *cfg, moqr_shards_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = SHARDS_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* One pure resolution shared with moqr_shards_capacity_describe —
     * every default, coupling, and INVAL rule (byte cap, progress-table
     * product, K>1 binding budget) fires HERE, before any allocation. */
    moqr_shards_limits_t lim;
    {
        moqr_result_t rrc = moqr_shards_cfg_resolve(cfg, &lim);
        if (rrc != MOQR_OK) {
            return rrc;
        }
    }
    uint32_t k = lim.shards;

    moqr_shards_t *s = a->alloc(sizeof(*s), a->ctx);
    if (s == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(s, 0, sizeof(*s));
    s->alloc = *a;
    s->shard_count = (uint16_t)k;
    s->placement = (SHARDS_CFG_HAS(cfg, placement) && cfg->placement != NULL)
                       ? cfg->placement
                       : r_rendezvous_place;
    s->permute_seed = SHARDS_CFG_HAS(cfg, permute_seed) ? cfg->permute_seed : 0;
    s->mbox_cap = lim.mbox_cap;
    s->place_state.struct_size = (uint32_t)sizeof(s->place_state);
    /* epochs stay generation 0 (memset); a control input bumps them later. */
    s->place_state.shard_count = (uint16_t)k;
    s->place_state.node_count = 1;

    s->shards = a->alloc((size_t)k * sizeof(*s->shards), a->ctx);
    if (s->shards == NULL) {
        a->free(s, sizeof(*s), a->ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(s->shards, 0, (size_t)k * sizeof(*s->shards));

    uint32_t trace_ring = lim.trace_ring;

    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);

    /* Journal bound (per shard): one entry per tracked namespace. Explicit knob if
     * given, else sized off the core's namespace-node budget. Self-echo suppression
     * is per-entry, so it needs no separate pool. */
    s->jrn_cap = lim.jrn_cap;
    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    s->cluster_enabled = cluster_cfg.enabled;
    s->cluster_routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
    s->cluster_hops = cluster_cfg.max_hops != 0 ? cluster_cfg.max_hops : 32u;

    /* Pending remote-owner demand bound (per shard): at most one upstream attempt
     * per track, so the core's max_tracks is the natural default; explicit knob
     * lets a test exercise the fail-stop. */
    s->pend_cap = lim.pend_cap;

    /* Inbound visibility policy, fixed at create (before any stepper runs): a
     * concurrent per-shard runtime sets it true so a push is visible as soon
     * as the mailbox mutex publishes it (no shared round to advance); the
     * deterministic runner leaves it false and relies on the round barrier. */
    s->live_visibility =
        SHARDS_CFG_HAS(cfg, live_visibility) && cfg->live_visibility;

    /* Demand-channel ENTRY bound (per directed pair). Control keeps at most
     * one in-flight message per demand per direction, so the pending-demand
     * cap is the natural default; queued data is additionally governed by
     * the byte cap below. The explicit knob lets tests exercise channel
     * backpressure. */
    s->dch_cap = lim.dch_cap;

    /* Owner-side demand admission, fixed at create and effective only at K>1.
     * A bare config defaults OFF — the owner refuses every forwarded demand
     * (NOT_SUPPORTED); the production CLI turns it on for every multi-lane
     * serve, where a subscriber on one lane must reach a publisher on another.
     * A single lane builds no manager, so a forced flag is inert. */
    s->admit_remote = lim.admit;

    /* Channel byte cap: default = the RESOLVED per-track log byte budget
     * (moqr_log_capacity_describe applies the log's own defaulting), so any
     * single legal record — which the log bounds to that budget — always
     * eventually fits an empty channel. An explicit smaller cap could strand
     * a legal record forever: refuse it at create rather than wedge at
     * runtime. This is a per-channel bound, deliberately NOT a process-wide
     * ceiling — the capacity command reports the closed-form process ceiling
     * separately, at every lane count. */
    s->dch_byte_cap = lim.dch_byte_cap;
    s->pump_turn_msgs = lim.pump_turn_msgs;
    s->pump_turn_bytes = lim.pump_turn_bytes;
    /* Per-demand subgroup-progress capacity. Default follows the session
     * binding's resolution of max_open_subgroups: the template value when
     * set, else the shared MOQR_BIND_DEF_OPEN_SUBGROUPS default. */
    s->sg_slots = lim.sg_slots;
    {
        moqr_core_limits_t clim;
        if (moqr_core_limits_resolve(&core_tmpl, &clim) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        s->fill_cap = shards_fill_slots(&lim, &clim);
    }

    /* Both progress tables are pend_cap x sg_slots rows per shard; reject
     * products the allocation and index arithmetic cannot represent BEFORE
     * any dependent allocation — an undersized table is never an option.
     * Only where the tables can exist: a K=1 or refusal-only runtime never
     * allocates them, so an impossible product is dead config there, not a
     * startup failure. */
    /* The progress-table product and K>1 binding-budget guards fired in
     * moqr_shards_cfg_resolve above, before anything was allocated. */

    /* Control mailboxes (K>1 only). */
    if (k > 1u) {
        size_t nring = (size_t)k * k;
        s->mbox = a->alloc(nring * sizeof(*s->mbox), a->ctx);
        if (s->mbox == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->mbox, 0, nring * sizeof(*s->mbox));
        /* Init every mailbox mutex first (cannot fail with default attrs on
         * the supported platforms; checked anyway), so a slot-allocation
         * failure below hands destroy() uniformly-initialized mutexes. */
        for (size_t r = 0; r < nring; r++) {
            if (pthread_mutex_init(&s->mbox[r].mu, NULL) != 0) {
                while (r > 0) {
                    r--;
                    (void)pthread_mutex_destroy(&s->mbox[r].mu);
                }
                s->alloc.free(s->mbox, nring * sizeof(*s->mbox), s->alloc.ctx);
                s->mbox = NULL;
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
        }
        for (size_t r = 0; r < nring; r++) {
            r_mailbox *mb = &s->mbox[r];
            mb->cap = s->mbox_cap;
            size_t slots = (size_t)s->mbox_cap + (s->cluster_enabled ? 1u : 0u);
            mb->slots = a->alloc(slots * sizeof(r_ctrl_msg), a->ctx);
            if (mb->slots == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            memset(mb->slots, 0, slots * sizeof(r_ctrl_msg));
            if (s->cluster_enabled) mb->withdrawal = &mb->slots[mb->cap];
        }
        /* Demand channels: same layout and the same mutex-first discipline. */
        s->dch = a->alloc(nring * sizeof(*s->dch), a->ctx);
        if (s->dch == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->dch, 0, nring * sizeof(*s->dch));
        for (size_t r = 0; r < nring; r++) {
            if (pthread_mutex_init(&s->dch[r].mu, NULL) != 0) {
                while (r > 0) {
                    r--;
                    (void)pthread_mutex_destroy(&s->dch[r].mu);
                }
                s->alloc.free(s->dch, nring * sizeof(*s->dch), s->alloc.ctx);
                s->dch = NULL;
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
        }
        for (size_t r = 0; r < nring; r++) {
            r_demand_channel_t *ch = &s->dch[r];
            ch->cap = s->dch_cap;
            ch->slots =
                a->alloc((size_t)s->dch_cap * sizeof(r_demand_msg_t), a->ctx);
            if (ch->slots == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            memset(ch->slots, 0, (size_t)s->dch_cap * sizeof(r_demand_msg_t));
        }
        /* Sticky arbitration tokens, one per directed channel; zero =
         * R_ARB_CTRL, the ruled initial preference. */
        s->arb = a->alloc(nring, a->ctx);
        if (s->arb == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->arb, 0, nring);
    }

    for (uint16_t i = 0; i < (uint16_t)k; i++) {
        r_shard_t *sh = &s->shards[i];
        if (moqr_trace_create(&s->alloc, trace_ring, &sh->trace) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        moqr_core_relay_cfg_t ccfg = core_tmpl;
        ccfg.trace = sh->trace;
        ccfg.shard_index = i;
        ccfg.shard_count = (uint16_t)k;
        moq_result_t create_rc = moqr_core_create_cluster(&ccfg, &cluster_cfg, &sh->core);
        if (create_rc != MOQ_OK) {
            moqr_shards_destroy(s);
            return moqr_result_legacy(create_rc);
        }
        /* Manager (K>1 only): allocated before the bind so the intent router can
         * point at it. K==1 is inert — no manager, no router. */
        moqr_bind_cfg_t bcfg = bind_tmpl;
        bcfg.core = sh->core;
        if (k > 1u) {
            sh->mgr = mgr_create(s, i, sh->core);
            if (sh->mgr == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            bcfg.router = shards_router;
            bcfg.router_ctx = sh->mgr;
            bcfg.router_cookie_base = MOQR_SHARD_COOKIE_BASE;
        }
        if (moqr_bind_create(&bcfg, &sh->bind) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        if (sh->mgr != NULL &&
            moqr_bind_meta_set_observer(sh->bind, shards_metadata_changed,
                                        sh->mgr) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        /* The fill wait: carried by this config's own appended fields, never
         * by the embedded bind template, whose size is fixed. */
        if (moqr_bind_set_fill_timeouts(
                sh->bind,
                SHARDS_CFG_HAS(cfg, fill_timeout_default_ms)
                    ? cfg->fill_timeout_default_ms : 0,
                SHARDS_CFG_HAS(cfg, fill_timeout_max_ms)
                    ? cfg->fill_timeout_max_ms : 0) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        if (sh->mgr != NULL && s->fill_cap > 0) {
            /* Fills over a range that precedes this shard's feed of a
             * remote-owned track are retrieved from the owner. */
            moqr_bind_fill_source_t fsrc;
            memset(&fsrc, 0, sizeof(fsrc));
            fsrc.struct_size = (uint32_t)sizeof(fsrc);
            fsrc.ctx = sh->mgr;
            fsrc.open = shards_fill_open;
            fsrc.peek = shards_fill_peek;
            fsrc.commit = shards_fill_commit;
            fsrc.close = shards_fill_close;
            fsrc.next_deadline_us = shards_fill_next_deadline;
            if (moqr_bind_set_fill_source(sh->bind, &fsrc) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            if (moqr_bind_retrieve_source_set(sh->bind,
                    shards_fill_retrieve_peek) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_INVAL;
            }
            /* A standalone FETCH for a track another shard owns is answered
             * by that shard through the same retrieval answers. */
            moqr_retrieve_standalone_ops_t sops = {
                .ctx = sh->mgr, .open = shards_sa_open, .plan = shards_sa_plan,
                .peek = shards_fill_retrieve_peek, .commit = shards_fill_commit,
                .close = shards_fill_close,
            };
            if (moqr_bind_retrieve_standalone_set(sh->bind, &sops) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_INVAL;
            }
        }
        if (sh->mgr != NULL && mgr_open_bindings(sh->mgr) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
    }

    *out = s;
    return MOQR_OK;
}


void
moqr_shards_destroy(moqr_shards_t *s)
{
    if (s == NULL) {
        return;
    }
    if (s->shards != NULL) {
        for (uint16_t i = 0; i < s->shard_count; i++) {
            r_shard_t *sh = &s->shards[i];
            if (sh->bind != NULL) {
                /* Memory-only, while the bind still owns its accounting and
                 * allocator. Partial creation without a bind has no pins. */
                if (sh->mgr != NULL && sh->mgr->psub != NULL) {
                    for (uint32_t j = 0; j < sh->mgr->psub_cap; j++) {
                        psub_metadata_release(sh->mgr, &sh->mgr->psub[j]);
                    }
                }
                moqr_bind_destroy(sh->bind);
            }
            mgr_free(sh->mgr);
            if (sh->core != NULL) {
                moqr_core_destroy(sh->core);
            }
            if (sh->trace != NULL) {
                moqr_trace_destroy(sh->trace);   /* after the core borrowing it */
            }
        }
        s->alloc.free(s->shards, (size_t)s->shard_count * sizeof(*s->shards),
                      s->alloc.ctx);
    }
    if (s->arb != NULL) {
        s->alloc.free(s->arb, (size_t)s->shard_count * s->shard_count,
                      s->alloc.ctx);
    }
    if (s->dch != NULL) {
        size_t nring = (size_t)s->shard_count * s->shard_count;
        for (size_t r = 0; r < nring; r++) {
            r_demand_channel_t *ch = &s->dch[r];
            for (uint32_t i = 0; ch->slots != NULL && i < ch->count; i++) {
                r_demand_msg_t *msg = &ch->slots[(ch->head + i) % ch->cap];
                dmsg_release(&s->alloc, msg);
            }
            sh_free(&s->alloc, ch->slots,
                    (size_t)ch->cap * sizeof(r_demand_msg_t));
            (void)pthread_mutex_destroy(&ch->mu);
        }
        s->alloc.free(s->dch, nring * sizeof(*s->dch), s->alloc.ctx);
    }
    if (s->mbox != NULL) {
        size_t nring = (size_t)s->shard_count * s->shard_count;
        for (size_t r = 0; r < nring; r++) {
            r_mailbox *mb = &s->mbox[r];
            for (uint32_t i = 0; mb->slots != NULL && i < mb->cap; i++) {
                if (mb->slots[i].used) {
                    sh_free(&s->alloc, mb->slots[i].canon, mb->slots[i].canon_len);
                    sh_free(&s->alloc, mb->slots[i].hops,
                            mb->slots[i].record.route.metadata.hop_count * sizeof(uint64_t));
                }
            }
            sh_free(&s->alloc, mb->slots,
                    ((size_t)mb->cap + (s->cluster_enabled ? 1u : 0u)) * sizeof(r_ctrl_msg));
            (void)pthread_mutex_destroy(&mb->mu);
        }
        s->alloc.free(s->mbox, nring * sizeof(*s->mbox), s->alloc.ctx);
    }
    s->alloc.free(s, sizeof(*s), s->alloc.ctx);
}


/* -- accessors -------------------------------------------------------------- */

uint16_t
moqr_shards_count(const moqr_shards_t *s)
{
    return s != NULL ? s->shard_count : 0;
}


moqr_core_t *
moqr_shards_core(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].core : NULL;
}


moqr_bind_t *
moqr_shards_bind(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].bind : NULL;
}


moqr_trace_t *
moqr_shards_trace(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].trace : NULL;
}


moqr_owner_t
moqr_shards_place(const moqr_shards_t *s, const moqr_place_key_t *key)
{
    moqr_owner_t none = { 0, 0 };
    if (s == NULL || key == NULL) {
        return none;
    }
    return s->placement(key, &s->place_state);
}



moqr_result_t
moqr_shards_step_shard(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                       uint64_t *pushed_dst_mask)
{
    if (s == NULL || shard >= s->shard_count) {
        return MOQR_ERR_INVAL;
    }
    /* The accounted step: wake causes counted at the mask level, and the
     * returned wake set is push destinations MERGED with producer credits
     * (lanes whose src->this channel regained capacity through a durable
     * pop are woken alongside the push destinations). */
    moqr_result_t rc =
        shards_step_accounted(s, shard, now_us, pushed_dst_mask);
    if (rc != MOQR_OK) {
        return rc;   /* the bind pump's result takes precedence */
    }
    /* Surface this shard's manager fail-stop: a borrowed observation the
     * router/refuse path could not record latches mgr->oom (sticky, never
     * cleared), so once set every step of this shard reports it. Mirrors the
     * aggregate runner's per-shard oom scan (moqr_shards_step), scoped to the
     * one shard a threaded caller drives. */
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL && m->oom) return MOQR_ERR_NOMEM;
    if (m != NULL && m->cluster_error != MOQR_OK) return m->cluster_error;
    return MOQR_OK;
}

#ifdef MOQR_BIND_TESTING


void moqr_shards_debug_fail_stop(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count) {
        return;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL) {
        m->oom = true;
    }
}

#endif
