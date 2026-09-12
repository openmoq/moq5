#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif

#include <moq/relay/capacity.h>

#include "moqr_bind_auth.h"

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Fixed forward-latency bucket bounds (microseconds), 100us..1s. Shared by
 * the observer here and the exporter (via moqr_latency_bound_us). */
static const uint64_t moqr_lat_bounds[MOQR_LATENCY_BUCKETS] = {
    100u,   250u,   500u,    1000u,   2500u,   5000u,
    10000u, 25000u, 50000u,  100000u, 250000u, 1000000u,
};

uint64_t
moqr_latency_bound_us(uint32_t i)
{
    return i < MOQR_LATENCY_BUCKETS ? moqr_lat_bounds[i] : 0u;
}

void
moqr_latency_observe(moqr_latency_hist_t *h, uint64_t latency_us)
{
    uint32_t i = 0;
    while (i < MOQR_LATENCY_BUCKETS && latency_us > moqr_lat_bounds[i]) {
        i++;
    }
    h->bucket[i]++;   /* i == MOQR_LATENCY_BUCKETS => +Inf overflow */
    h->sum_us += latency_us;
    h->count++;
}

#define BIND_DEF_CONNS 64u
#define BIND_DEF_DSUBS 32u
#define BIND_DEF_SGS   MOQR_BIND_DEF_OPEN_SUBGROUPS
#define BIND_DEF_USUBS 8u
#define BIND_DEF_PUBS  8u
/* d16 MAX_REQUEST_ID grant step (request-id units). Each grant raises the
 * ceiling by this; the peer keeps ~half a window of headroom. */
#define BIND_DEF_GRANT_WINDOW 1024u
/* Fixed pump work batches. Storage is bind-owned, not automatic, so embedded
 * callers do not inherit a hidden multi-KiB moqr_bind_pump() frame. */
#define BIND_PUMP_EVENT_BATCH 16u
#define BIND_PUMP_REVOKED_BATCH 16u
#define BIND_PUMP_INTENT_BATCH 32u

typedef struct b_dsub {
    bool       used;
    uint64_t   sub_raw;      /* downstream session subscription handle    */
    moqr_sub_t rsub;
} b_dsub_t;

typedef struct b_sg {
    bool                  used;
    uint64_t              sub_raw;
    uint64_t              group, subgroup;
    moq_subgroup_handle_t h;
    /* Chunked-delivery write progress on this downstream subgroup:
     * whether begin_object has shipped for the in-flight object, its id, and the
     * next chunk index to write. Held across a downstream WOULD_BLOCK so
     * write_object_data resumes without duplicating or skipping a chunk. */
    bool                  obj_begun;
    uint64_t              obj_id;
    uint32_t              next_chunk;
} b_sg_t;

/* A downstream FETCH being served from a core fetch cursor. State machine:
 * NEEDS_OK -> accept_fetch (FETCH_OK + data stream) -> STREAMING -> peek/
 * write_fetch_object/commit (holds on WOULD_BLOCK) -> NEEDS_FIN -> end_fetch. */
typedef enum b_fetch_state {
    BF_NEEDS_OK = 0,
    BF_STREAMING,
    BF_NEEDS_FIN,
} b_fetch_state_t;

typedef struct b_fetch {
    bool            used;
    b_fetch_state_t state;
    moq_fetch_t     sfetch;      /* downstream session fetch handle          */
    moqr_fetch_t    cfetch;      /* core fetch cursor                        */
    bool            eot;         /* plan.end_of_track -> accept_fetch cfg     */
    uint64_t        end_group;   /* plan End Location (raw wire) -> FETCH_OK  */
    uint64_t        end_object;
} b_fetch_t;

typedef struct b_usub {
    bool               used;
    moqr_track_t       track;
    uint64_t           track_gen;
    moq_subscription_t ssub;    /* the relay's upstream subscription      */
} b_usub_t;

typedef struct b_pub {
    bool         used;
    uint64_t     pub_raw;       /* peer-initiated publish handle          */
    moqr_track_t track;
    uint64_t     track_gen;
} b_pub_t;

typedef struct b_ann {
    bool         used;
    uint64_t     ann_raw;       /* peer PUBLISH_NAMESPACE handle          */
    moqr_ns_t    ns;
    moq_bytes_t  parts[32];
    uint8_t     *bytes;
    size_t       bytes_len;
} b_ann_t;

/* A namespace update held for retry. draft-18 Section 6.2 makes the NAMESPACE
 * owed to a matching prefix subscriber a MUST, so a blocked control path may
 * delay it but must never discard it. The intent's ns_parts borrow
 * core-interned storage (relay.h:102), so the suffix is deep-copied here — the
 * same ownership shape ann_store() uses for an accepted announce. */
/* Payload sidecar for one pending-ring slot. Indexed 1:1 by ring slot, so the
 * ring stays the single ordering authority: a namespace update parks in the
 * SAME FIFO as every scalar terminal and cannot overtake or be overtaken.
 * Only slots whose parked intent is NS_FOUND / NS_GONE carry bytes. */
typedef struct b_nsu {
    bool     used;        /* occupancy is explicit: a valid suffix can carry
                           * zero bytes because it has NO fields at all --
                           * a present field always has at least one byte --
                           * so a NULL bytes pointer does NOT mean "free"  */
    uint8_t *bytes;       /* the parked suffix's bytes; parts ride in the
                           * intent's own inline ns_parts array            */
    size_t   bytes_len;
} b_nsu_t;

/* Delivery-park reasons (D3): why this connection's last delivery pass
 * blocked, deciding its recovery gate. BIND_SG never parks — its recovery is
 * the explicit re-arm at every subgroup-slot release site. */
#define B_PARK_NONE       0u
#define B_PARK_ACTION_CAP 1u   /* session action queue full: capacity gate  */
#define B_PARK_SESSION_SG 2u   /* session subgroup pool: one try per pump   */

#define B_DRAFT_16 0u
#define B_DRAFT_18 1u

typedef struct b_conn {
    bool           used;
    bool           closed;
    bool           detach_pending;   /* close blocked on intent space   */
    bool           bind_sg_waiting;  /* a delivery blocked on this conn's
                                      * subgroup-slot pool (B_DELIVER_BLOCKED_
                                      * BIND_SG): the ONLY state that makes a
                                      * later slot release re-arm the ready
                                      * bit. Cleared when the conn next drains
                                      * or on lifecycle reset, so ordinary
                                      * seal-close traffic (no delivery
                                      * waiting) re-arms nothing. Rides the
                                      * bool padding — no size cost.     */
    uint8_t        park_reason;      /* B_PARK_*                        */
    /* This connection's negotiated draft, held as a compact tag: the status
     * and reset registries disagree on several codepoints, so every terminal
     * is encoded for the peer that reads it. Rides the bool padding — no size
     * cost. */
    uint8_t        wire_draft;       /* B_DRAFT_*                       */
    size_t         park_cap;         /* ACTION_CAP: capacity floor when
                                      * parked; retry only above it     */
#ifdef MOQR_BIND_TESTING
    /* Test-only blockage attribution, PER CONNECTION (this conn's lane
     * thread is the only writer): how many delivery passes for THIS conn
     * ended in each park/refusal class (0 action-cap, 1 session-sg,
     * 2 bind-sg). Snapshot only from the owning lane or after the lane
     * threads are joined. */
    uint64_t       dbg_blocked[3];
    /* Test-only BIND_SG attribution: the TRACK whose delivery hit this
     * conn's subgroup-pool refusal. The track — not the delivery's
     * sub_cookie, which is the DOWNSTREAM subscription handle, a different
     * id space from the demand — is the join key the shard layer resolves
     * to a demand id (the demand table is keyed by track). The FIRST
     * refusal latches it; a later refusal for a DIFFERENT track (hence a
     * different demand — demands are per track) flips ambiguous, and an
     * unresolvable sub handle also flips ambiguous, so the SGDIAG→SEALLOG
     * join fails closed rather than attributing to a guess. Cleared on
     * slot claim/reuse alongside dbg_blocked. */
    uint64_t       dbg_bind_sg_track;      /* moqr_track_t raw            */
    uint64_t       dbg_bind_sg_track_gen;
    bool           dbg_bind_sg_track_set;
    bool           dbg_bind_sg_ambiguous;
#endif
    moq_session_t *session;
    moqr_binding_t binding;
    b_dsub_t      *dsubs;
    b_sg_t        *sgs;
    b_usub_t      *usubs;
    b_pub_t       *pubs;
    b_ann_t       *anns;
    b_fetch_t     *fetches;
    uint64_t       requests_seen;    /* inbound peer requests observed (d16
                                      * request-id consumption proxy)     */
} b_conn_t;

struct moqr_bind {
    moq_alloc_t  alloc;
    moqr_core_t *core;
    uint32_t     max_conns, n_dsubs, n_sgs, n_usubs, n_pubs, n_anns, n_fetches;
    uint32_t     request_grant_window;   /* d16 MAX_REQUEST_ID grant step   */
    /* Intent router for shard-manager pseudo-binding cookies (NULL = none). */
    uint64_t                    router_cookie_base;
    moqr_bind_intent_router_fn  router;
    void                       *router_ctx;
    /* Set during a pump when any connection's delivery pass expired the
     * 256-delivery guard with work remaining; cleared at the next pump's
     * start. Read through moqr_bind_pump_budget_pending. */
    bool         budget_pending;
    bool         in_pump;   /* non-reentrant: one owner thread pumps a bind */
    bool         intent_drain_active;
#ifdef MOQR_BIND_TESTING
    uint64_t     dbg_intent_retry_suppressed;
#endif
    b_conn_t    *conns;
    /* Phase F delivery scheduling (bind is the only writer of both):
     * dl_ready is filled from moqr_core_drain_ready and self-rearms
     * (BUDGET/RETRY); dl_parked holds connections whose last delivery pass
     * blocked downstream, gated per park_reason. One bit per conn slot,
     * ceil(max_conns/64) words each. */
    uint64_t    *dl_ready;
    uint64_t    *dl_parked;
    moq_event_t *pump_events;
    moqr_revoked_grant_t *pump_revoked;
    moqr_intent_t *pump_intents;
    uint64_t     events_translated;
    uint64_t     deliveries_written;
    uint64_t     ingest_refusals;
    uint64_t     session_errors;
    moqr_latency_hist_t forward_latency;
    /* Deferred-output ring: output intents whose session write hit WOULD_BLOCK,
     * awaiting retry. FIFO order is preserved so an ACCEPT can never be
     * overtaken by the SUB_DONE or object delivery that follows it. Two kinds
     * of entry live here, and nothing else:
     *   - scalar-safe kinds (bind_intent_deferrable), stored as polled; and
     *   - NS_FOUND / NS_GONE, stored only AFTER their borrowed suffix has been
     *     deep-copied into this slot's payload sidecar, which is what makes
     *     them safe to hold across pumps.
     * Every other borrowed-view kind is consume-in-pump: an intent whose
     * ns_parts/name views BORROW core-interned storage (relay.h:135) and is not
     * deep-copied cannot survive to the next pump, so UPSTREAM_SUBSCRIBE is
     * executed inline the moment it is polled and never parked.
     * Sized from the core's admission limits (bind_create) to comfortably hold
     * every scalar terminal that can be simultaneously in flight; a would-be
     * overflow is counted, never a silent drop. */
    moqr_intent_t *pending;
    uint32_t       pending_cap;
    uint32_t       pending_head;
    uint32_t       pending_count;
    uint32_t       pending_high_water;      /* max depth seen (telemetry)     */
    uint32_t       pending_nonscalar_blocked; /* borrowed kind hit defer: BUG */
    b_nsu_t       *nsu;        /* payload sidecar, 1:1 with pending[]        */
    uint32_t       nsu_cap;    /* == pending_cap, by construction            */
    uint32_t       nsu_high_water;
    uint64_t       nsu_retained;
    uint64_t       nsu_failed_closed;
    uint64_t       ordered_failed_closed;
    uint32_t       nsu_live;   /* OCCUPIED sidecar slots; a valid zero-byte
                                * suffix is occupied while owning no bytes  */
    /* Bumped every time a purge REMOVES ordered-ring entries. A retry of the
     * ring head can detach its own connection (hard send failure), and detach
     * purges + compacts the ring underneath the caller -- possibly moving a
     * DIFFERENT connection's entry into the head slot. The caller compares
     * this stamp across the retry so it never pops a slot it did not consume.
     * A stamp, not a count: purge can remove several entries at once. */
    uint32_t       pending_purge_epoch;
};

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

/* Whole 64-bit words for one scheduling bit per conn slot — widened before
 * the +63 round-up so the uint32_t maximum cannot wrap (mirrors the core's
 * ready_word_count; the two layers size independent pools). */
static uint32_t
bind_dl_word_count(uint32_t max_conns)
{
    return (uint32_t)(((uint64_t)max_conns + 63u) >> 6);
}

static void
bind_dl_set(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] |= 1ull << (slot & 63u);
}

static void
bind_dl_clear(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] &= ~(1ull << (slot & 63u));
}

static bool
bind_dl_test(const uint64_t *words, uint32_t slot)
{
    return (words[slot >> 6] & (1ull << (slot & 63u))) != 0;
}

/* Re-arm a connection's delivery readiness from a bind-local recovery edge
 * (a subgroup-slot release on a still-used, still-open connection). Spurious
 * re-arms cost one empty probe and dedup in the bitset; teardown paths CLEAR
 * scheduling state instead — a dead or reused slot must never re-arm itself. */
static void
bind_dl_rearm(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->used && !cn->closed) {
        bind_dl_set(b->dl_ready, (uint32_t)(cn - b->conns));
    }
}

/* Re-arm on a freed subgroup-pool slot ONLY when a delivery is actually waiting
 * on that pool (bind_sg_waiting, set by the last pass's B_DELIVER_BLOCKED_BIND_SG
 * and cleared by any other outcome). Ordinary seal-close / reset traffic, where
 * nothing is pool-blocked, re-arms nothing — so a fully-draining pass leaves the
 * ready set empty and earns no spurious self-wake. */
static void
bind_dl_rearm_on_slot_release(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->bind_sg_waiting) {
        bind_dl_rearm(b, cn);
    }
}

static void
bind_dl_clear_conn(moqr_bind_t *b, b_conn_t *cn)
{
    uint32_t slot = (uint32_t)(cn - b->conns);
    bind_dl_clear(b->dl_ready, slot);
    bind_dl_clear(b->dl_parked, slot);
    cn->park_reason = B_PARK_NONE;
    cn->park_cap = 0;
    cn->bind_sg_waiting = false;
}

#ifdef MOQR_BIND_TESTING
#include <stdatomic.h>
/* Test-only probe gauge (compiled solely into the test-internals build):
 * one count per moqr_core_next_delivery call issued by the delivery half.
 * ATOMIC: multi-lane fixtures increment from every lane thread. */
static _Atomic uint64_t bind_dbg_probes;
uint64_t
moqr_bind_debug_delivery_probes(void)
{
    return atomic_load(&bind_dbg_probes);
}

/* Test-only fault injection: fail the Nth delivery confirmation from now
 * (1 = the very next one) as a consistency error. */
static _Atomic int bind_dbg_fail_confirms;
void
moqr_bind_debug_fail_confirm(int nth)
{
    atomic_store(&bind_dbg_fail_confirms, nth);
}

/* Test-only fault injection: fail the Nth SUB_DONE begun-subgroup reset from
 * now as a hard (non-WOULD_BLOCK) error, so the production close -> detach ->
 * purge path runs from inside an ordered-ring retry. */
static _Atomic int bind_dbg_fail_sg_resets;
static _Atomic int bind_dbg_fail_nsu_stores;
static _Atomic int bind_dbg_ring_full_n;
static _Atomic int bind_dbg_ring_full_nsu;
static moqr_bind_debug_detach_pending_fn bind_dbg_detach_pending_fn;
static void *bind_dbg_detach_pending_ctx;
void
moqr_bind_debug_on_detach_pending(moqr_bind_debug_detach_pending_fn fn,
                                  void *ctx)
{
    bind_dbg_detach_pending_fn = fn;
    bind_dbg_detach_pending_ctx = ctx;
}
void
moqr_bind_debug_ring_full(int nth, bool is_nsu)
{
    atomic_store(&bind_dbg_ring_full_nsu, is_nsu ? 1 : 0);
    atomic_store(&bind_dbg_ring_full_n, nth);
}
void
moqr_bind_debug_fail_nsu_store(int nth)
{
    atomic_store(&bind_dbg_fail_nsu_stores, nth);
}
void
moqr_bind_debug_fail_sg_reset(int nth)
{
    atomic_store(&bind_dbg_fail_sg_resets, nth);
}

/* Test-only fault injection: make the next N downstream SUB_DONE terminal
 * writes observe WOULD_BLOCK without calling the session, so the production
 * defer-and-retry arm runs against a terminal that was never sent. */
static _Atomic int bind_dbg_block_sub_dones;
void
moqr_bind_debug_block_sub_done(int n)
{
    atomic_store(&bind_dbg_block_sub_dones, n);
}
int
moqr_bind_debug_sub_done_blocks_left(void)
{
    return atomic_load(&bind_dbg_block_sub_dones);
}

/* Test-only probe-result injection: force the Nth next_delivery probe from
 * now to report `result` without calling the core — pins the CAPACITY-only
 * RETRY classification (every other error must fail closed). */
static _Atomic int bind_dbg_fail_probes;
static moqr_result_t bind_dbg_probe_result;
void
moqr_bind_debug_fail_probe(int nth, moqr_result_t result)
{
    bind_dbg_probe_result = result;   /* set before arming the countdown */
    atomic_store(&bind_dbg_fail_probes, nth);
}

/* Test-only reason-specific gauges: SESSION_SG re-attempts made by the
 * pump, and the current SESSION_SG-parked population (the exceptional
 * set the plan bounds). */
static _Atomic uint64_t bind_dbg_sg_attempts;
uint64_t
moqr_bind_debug_sg_attempts(void)
{
    return atomic_load(&bind_dbg_sg_attempts);
}

/* Saturating increment for the per-conn blockage counters: a counter that
 * has reached UINT64_MAX stays there rather than wrapping to 0, so the
 * LIVE aggregate below can never see a large history collapse to a small
 * value (the aggregate's checked-add would still catch a genuine overflow). */
static inline void
bind_dbg_blocked_inc(uint64_t *v)
{
    if (*v != UINT64_MAX) {
        (*v)++;
    }
}

/* Test-only seeding of one LIVE conn slot's blockage counters — drives the
 * aggregate overflow path and the lifecycle assertions without needing to
 * pump millions of refusals. Returns false for a NULL bind or a slot that is
 * out of range or not currently live (used && !closed). */
bool
moqr_bind_debug_seed_blocked(moqr_bind_t *b, uint32_t slot,
                             const uint64_t counts[3])
{
    if (b == NULL || counts == NULL || slot >= b->max_conns) {
        return false;
    }
    b_conn_t *cn = &b->conns[slot];
    if (!cn->used || cn->closed) {
        return false;
    }
    cn->dbg_blocked[0] = counts[0];
    cn->dbg_blocked[1] = counts[1];
    cn->dbg_blocked[2] = counts[2];
    return true;
}

/* Test-only blockage attribution for ONE conn slot of ONE bind. The
 * caller owns the synchronization: read from the owning lane thread, or
 * after the lane threads have been joined (post managed-stop). */
void
moqr_bind_debug_conn_blocked_counts(const moqr_bind_t *b, uint32_t slot,
                                    uint64_t out[3])
{
    out[0] = out[1] = out[2] = 0;
    if (slot < b->max_conns) {
        out[0] = b->conns[slot].dbg_blocked[0];
        out[1] = b->conns[slot].dbg_blocked[1];
        out[2] = b->conns[slot].dbg_blocked[2];
    }
}

/* Test-only BIND_SG track attribution for ONE conn slot: which TRACK's
 * delivery hit the subgroup-pool refusal (first latches; a different track
 * later — or an unresolvable handle — marks ambiguity). The track is the
 * join key: demands are keyed by track, so the shard layer resolves it to
 * the demand id (moqr_shards_debug_track_demand). Returns false (zeroed
 * outs) for NULL/out-of-range/never-refused; *ambiguous is meaningful even
 * then (an early unresolvable handle latches ambiguity without a track).
 * Same synchronization contract as moqr_bind_debug_conn_blocked_counts. */
bool
moqr_bind_debug_conn_bind_sg_track(const moqr_bind_t *b, uint32_t slot,
                                   uint64_t *track_raw, uint64_t *track_gen,
                                   bool *ambiguous)
{
    if (track_raw != NULL) {
        *track_raw = 0;
    }
    if (track_gen != NULL) {
        *track_gen = 0;
    }
    if (ambiguous != NULL) {
        *ambiguous = false;
    }
    if (b == NULL || slot >= b->max_conns) {
        return false;
    }
    const b_conn_t *cn = &b->conns[slot];
    if (ambiguous != NULL) {
        *ambiguous = cn->dbg_bind_sg_ambiguous;
    }
    if (!cn->dbg_bind_sg_track_set) {
        return false;
    }
    if (track_raw != NULL) {
        *track_raw = cn->dbg_bind_sg_track;
    }
    if (track_gen != NULL) {
        *track_gen = cn->dbg_bind_sg_track_gen;
    }
    return true;
}

uint32_t
moqr_bind_debug_sg_park_count(const moqr_bind_t *b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (bind_dl_test(b->dl_parked, i) &&
            b->conns[i].park_reason == B_PARK_SESSION_SG) {
            n++;
        }
    }
    return n;
}

/* Checked u64 add: MOQR_OK and *acc += v, or MOQR_ERR_INTERNAL on overflow
 * (leaving *acc unchanged). Load-bearing for the aggregate's three totals so
 * a counter sum that would wrap makes the lane emit a refusal row. */
static moqr_result_t
bind_ckadd_u64(uint64_t *acc, uint64_t v)
{
    if (*acc > UINT64_MAX - v) {
        return MOQR_ERR_INTERNAL;
    }
    *acc += v;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_debug_blocked_aggregate(const moqr_bind_t *b,
                                  moqr_bind_blocked_agg_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));   /* zero output on every failure path too */
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;   /* LIVE only — a detached slot keeps stale counters */
        }
        out->live_conns++;
        if (cn->dbg_blocked[0] > 0) {
            out->conns_action_cap++;
        }
        if (cn->dbg_blocked[1] > 0) {
            out->conns_session_sg++;
        }
        if (cn->dbg_blocked[2] > 0) {
            out->conns_bind_sg++;
        }
        if (bind_ckadd_u64(&out->action_cap_total, cn->dbg_blocked[0]) !=
                MOQR_OK ||
            bind_ckadd_u64(&out->session_sg_total, cn->dbg_blocked[1]) !=
                MOQR_OK ||
            bind_ckadd_u64(&out->bind_sg_total, cn->dbg_blocked[2]) !=
                MOQR_OK) {
            memset(out, 0, sizeof(*out));   /* fail closed: no partial totals */
            return MOQR_ERR_INTERNAL;
        }
        if (bind_dl_test(b->dl_parked, i)) {
            if (cn->park_reason == B_PARK_ACTION_CAP) {
                out->parked_action_cap++;
            } else if (cn->park_reason == B_PARK_SESSION_SG) {
                out->parked_session_sg++;
            }
        }
    }
    return MOQR_OK;
}

/* Test-only downstream subgroup-pool occupancy for one conn slot. */
uint32_t
moqr_bind_debug_conn_open_sgs(const moqr_bind_t *b, uint32_t slot)
{
    uint32_t n = 0;
    if (slot < b->max_conns) {
        const b_conn_t *cn = &b->conns[slot];
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (cn->sgs[i].used) {
                n++;
            }
        }
    }
    return n;
}

/* Test-only resolved conn-slot capacity. */
uint32_t
moqr_bind_debug_max_conns(const moqr_bind_t *b)
{
    return b != NULL ? b->max_conns : 0u;
}

uint64_t
moqr_bind_debug_intent_retry_suppressed(const moqr_bind_t *b)
{
    return b != NULL ? b->dbg_intent_retry_suppressed : 0u;
}

/* Test-only scheduling-state dump for one conn slot. Out-of-range slots
 * report a zeroed state (the SGDIAG sweep probes a fixed slot range). */
void
moqr_bind_debug_dl_state(const moqr_bind_t *b, uint32_t slot, bool *ready,
                         bool *parked, uint8_t *reason)
{
    if (slot >= b->max_conns) {
        *ready = false;
        *parked = false;
        *reason = 0;
        return;
    }
    *ready = bind_dl_test(b->dl_ready, slot);
    *parked = bind_dl_test(b->dl_parked, slot);
    *reason = b->conns[slot].park_reason;
}
#endif

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

/* One announce's copied namespace bytes can never exceed the core's shared
 * full-track-name cap; a bind slot exists only for a core-ACCEPTED announce,
 * so the live-slot count is bounded by the core ns-node pool as well as the
 * bind tables. */
#define BIND_ANN_BYTES_MAX 4096u

moqr_result_t
moqr_bind_capacity_describe(const moqr_bind_cfg_t *cfg,
                            const moqr_core_limits_t *lim,
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
                        moqr_cap_mul(bl.n_fetches, sizeof(b_fetch_t)))))));
    out->structure_bytes = moqr_cap_add(
        sizeof(moqr_bind_t),
        moqr_cap_add(moqr_cap_mul(bl.pending_cap, sizeof(moqr_intent_t)),
                     moqr_cap_mul(bl.max_conns,
                                  moqr_cap_add(sizeof(b_conn_t),
                                               per_conn))));
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
    /* The namespace payload sidecar: one entry per deferral-ring slot, plus
     * the suffix bytes every slot could own at once. One held suffix is
     * bounded by the same full-track-name cap an announce is. */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes, moqr_cap_mul(bl.nsu_cap, sizeof(b_nsu_t)));
    out->announce_bytes = moqr_cap_add(
        out->announce_bytes, moqr_cap_mul(bl.nsu_cap, BIND_ANN_BYTES_MAX));
    out->total_bytes =
        moqr_cap_add(out->structure_bytes, out->announce_bytes);
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
    /* Default map sizes FOLLOW the core's admission limits, so the
     * binding can never be under-provisioned relative to what the core
     * will admit. Explicit cfg values still win (tests use tiny maps to
     * prove the refusal paths). */
    moqr_core_limits_t lim;
    moqr_core_get_limits(core, &lim);
    /* Same pure resolution the capacity describe uses; the live core's
     * limits equal moqr_core_limits_resolve(cfg) by construction. */
    moqr_bind_limits_t bl;
    if (moqr_bind_cfg_resolve(cfg, &lim, &bl) != MOQR_OK) {
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
        BIND_ALLOC_TABLE(fetches, b->n_fetches);
#undef BIND_ALLOC_TABLE
    }
    *out = b;
    return MOQR_OK;
}

/* Scalar-safe intent kinds: payload is fully self-contained, so a copy can be
 * parked in the pending ring and retried across pumps. Kinds that read the
 * borrowed ns_parts/name views (UPSTREAM_SUBSCRIBE / NS_FOUND / NS_GONE) are
 * deliberately absent — those byte spans borrow core-interned storage and are
 * valid only until the next mutating core call (relay.h:102), so they can
 * never be parked by a shallow copy. NS_FOUND / NS_GONE reach the same ring
 * through bind_defer_push_nsu, which deep-copies the suffix into the slot's
 * payload sidecar first; UPSTREAM_SUBSCRIBE remains consume-in-pump. */
static bool
bind_intent_deferrable(moqr_intent_kind_t kind)
{
    switch (kind) {
    case MOQR_INTENT_ACCEPT_SUB:
    case MOQR_INTENT_REJECT_SUB:
    case MOQR_INTENT_SUB_DONE:
    case MOQR_INTENT_ACCEPT_PUBLISH:
    case MOQR_INTENT_REJECT_PUBLISH:
    case MOQR_INTENT_TRACK_STATUS_OK:
    case MOQR_INTENT_TRACK_STATUS_ERROR:
        return true;
    default:
        return false;
    }
}

/* Execute one output intent's session write(s).
 *   true  => consumed: delivered, moot (conn gone/closed), hard-failed, or
 *            (NS_FOUND/NS_GONE only) newly parked in the ring with its suffix
 *            deep-copied — *parked_out reports that case so the caller can set
 *            `blocked` before the next intent is attempted.
 *   false => the write hit WOULD_BLOCK and the intent must be retried from the
 *            ring next pump; *defer holds it (a copy of *it, or a synthesized
 *            refusal when downstream tracking was exhausted). Deferral leaves
 *            all binding/core state untouched so the retry re-runs cleanly.
 * Which kinds can return false: scalar-safe kinds (bind_intent_deferrable), and
 * a retained NS_FOUND/NS_GONE re-attempted as BIND_TRY_FROM_RING — its payload
 * is already bind-owned, so it stays in place rather than being re-parked. A
 * FRESH namespace intent instead parks and returns true. Consume-in-pump
 * borrowed kinds always return true and are never parked. */
/* How the executor is entering bind_try_intent for this intent. */
#define BIND_TRY_FRESH      0u   /* newly polled, nothing blocked          */
#define BIND_TRY_FROM_RING  1u   /* a retry of an already-parked slot      */
#define BIND_TRY_FORCE_PARK 2u   /* something is blocked: queue, never send */

static bool bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it,
                            uint64_t now_us, uint8_t mode,
                            bool *parked_out, moqr_intent_t *defer);

/* Deep-copy one namespace update's suffix into the retained ring. The intent's
 * ns_parts borrow core-interned storage that dies at the next mutating core
 * call, so nothing borrowed may escape this call.
 * Returns true if the update is now owned by the bind and will be retried.
 * false means held state is exhausted — the caller must fail closed rather
 * than drop a MUST-required update. */
static bool
bind_nsu_store(moqr_bind_t *b, uint32_t slot, moqr_intent_t *parked)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_nsu_stores) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_nsu_stores, 1) == 1) {
        return false;   /* injected: byte allocation / slot exhaustion */
    }
#endif
    /* The sidecar owns the SUFFIX the subscriber is owed -- the same span the
     * healthy send passes (ns_parts + value). The prefix bytes are never sent
     * and must not be copied or left semantically live in the parked intent,
     * but the prefix/full CARDINALITY is preserved (see below). */
    if (parked->value > parked->ns_count) {
        return false;
    }
    uint32_t off = parked->value;
    uint32_t suffix_count = parked->ns_count - off;
    if (suffix_count > 32) {
        return false;
    }
    size_t total = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        const moq_bytes_t *src = &parked->ns_parts[off + i];
        if (src->len > 0 && src->data == NULL) {
            return false;
        }
        if (SIZE_MAX - total < src->len) {
            return false;
        }
        total += src->len;
    }
    /* Defense in depth: never allocate more than the per-slot bound the
     * capacity description charges for. */
    if (total > BIND_ANN_BYTES_MAX) {
        return false;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return false;
        }
    }
    b_nsu_t *u = &b->nsu[slot];
    if (u->used) {
        /* A live payload here means the ring handed out an occupied slot.
         * Overwriting it would silently drop an update already owed, so the
         * store is refused and the caller fails closed. The existing slot is
         * left exactly as it was. */
        if (bytes != NULL) {
            b->alloc.free(bytes, total, b->alloc.ctx);
        }
        return false;
    }
    u->used = true;   /* occupancy is explicit: a valid suffix may own 0 bytes */
    u->bytes = bytes;
    u->bytes_len = total;
    /* Only the SUFFIX is bind-owned, but ns_count and value keep their meaning:
     * ns_count is the FULL announced Track Namespace cardinality and value is
     * the subscribed prefix cardinality, exactly as the fresh intent carried
     * them. That distinction is load-bearing -- a true root announcement is
     * (ns_count 0, value 0), while a non-root namespace equal to its prefix is
     * (ns_count N, value N) and merely has an empty wire suffix -- so it must
     * survive retention or a replayed exact-prefix update would be
     * indistinguishable from a root one.
     *
     * The owned suffix descriptors therefore stay at their ORIGINAL indices
     * [value, ns_count); the prefix descriptors [0, value) are cleared so no
     * borrowed pointer survives the pump, and their bytes are never copied. */
    size_t o = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        moq_bytes_t src = parked->ns_parts[off + i];
        parked->ns_parts[off + i].len = src.len;
        if (src.len == 0) {
            parked->ns_parts[off + i].data = NULL;   /* empty, not aliased */
            continue;
        }
        memcpy(bytes + o, src.data, src.len);
        parked->ns_parts[off + i].data = bytes + o;
        o += src.len;
    }
    for (uint32_t i = 0; i < off; i++) {
        parked->ns_parts[i].data = NULL;   /* no borrowed prefix left live */
        parked->ns_parts[i].len = 0;
    }
    b->nsu_live++;
    b->nsu_retained++;
    if (b->nsu_live > b->nsu_high_water) {
        b->nsu_high_water = b->nsu_live;
    }
    return true;
}

/* Release one sidecar slot's payload (delivered, purged, or torn down). */
static void
bind_nsu_release(moqr_bind_t *b, uint32_t slot)
{
    b_nsu_t *u = &b->nsu[slot];
    if (!u->used) {
        return;
    }
    if (u->bytes != NULL) {
        b->alloc.free(u->bytes, u->bytes_len, b->alloc.ctx);
    }
    b->nsu_live--;
    memset(u, 0, sizeof(*u));
}

/* THE ordered-ring room predicate. Both insertion kinds go through it, so an
 * injected "full" and a genuinely full ring take the identical branch. */
static bool
bind_ordered_room(moqr_bind_t *b, bool is_nsu)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_ring_full_n) > 0 &&
        (atomic_load(&bind_dbg_ring_full_nsu) != 0) == is_nsu &&
        atomic_fetch_sub(&bind_dbg_ring_full_n, 1) == 1) {
        return false;
    }
#endif
    (void)is_nsu;
    return b->pending_count < b->pending_cap;
}

/* Append to the ordered ring. PRECONDITION: room was already admitted for this
 * insertion by exactly one bind_ordered_room() call. Admission is separate from
 * the append so that one insertion answers to one decision: a namespace
 * admitted as a namespace must never also be judged as a scalar. */
static void
bind_pending_append(moqr_bind_t *b, const moqr_intent_t *it)
{
    b->pending[(b->pending_head + b->pending_count) % b->pending_cap] = *it;
    b->pending_count++;
    if (b->pending_count > b->pending_high_water) {
        b->pending_high_water = b->pending_count;
    }
}

/* Scalar admission + append. Namespace insertions do NOT come through here:
 * they admit themselves as namespaces in bind_defer_push_nsu. */
static bool
bind_pending_push(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, false)) {
        return false;
    }
    bind_pending_append(b, it);
    return true;
}

/* false => the intent could NOT join the ordered FIFO. Namespace updates now
 * share this ring, so "a scalar terminal always fits" is no longer a safe
 * assumption: a caller that ignores the failure would consume mandatory
 * ordered output while its target connection stays live. */
static bool
bind_defer_push(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_intent_deferrable(it->kind)) {
        b->pending_nonscalar_blocked++;
        return true;   /* contract violation, counted; not a ring overflow */
    }
    if (!bind_pending_push(b, it)) {
        b->session_errors++;
        return false;
    }
    return true;
}

#define BIND_SESSION_INTERNAL_ERROR 0x1u   /* MoQ session INTERNAL_ERROR      */

/* Wire registries are distinct namespaces; the bind never lets one value
 * serve two of them.
 * - Data-stream reset CANCELLED: both drafts assign 0x1 — the stream was
 *   cancelled because the publisher ended the subscription, and PUBLISH_DONE
 *   carries the more detailed status.
 * - PUBLISH_DONE GOING_AWAY: both drafts assign 0x4 — the publisher issued
 *   a GOAWAY.
 * - REQUEST_ERROR INTERNAL_ERROR: both drafts assign 0x0 — the relay's own
 *   upstream request was terminated by a signal it cannot act on. */
#define BIND_RESET_CANCELLED   0x1u
#define BIND_DONE_GOING_AWAY   0x4u
#define BIND_REQERR_INTERNAL   0x0u

static b_conn_t *conn_by_cookie(moqr_bind_t *b, uint64_t cookie);
static void conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);

/* The ordered ring is full for this intent: the connection it targets can no
 * longer be given output in order, so close it rather than continue a live
 * session that has lost mandatory ordered output. Other connections' retained
 * entries are untouched. */
static void
bind_ordered_overflow_close(moqr_bind_t *b, const moqr_intent_t *it,
                            uint64_t now_us)
{
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return;
    }
    b->ordered_failed_closed++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "ordered output ring exhausted", now_us);
    conn_detach(b, cn, now_us);
}

/* Park a namespace update in the SAME ordered ring as every scalar terminal,
 * with its borrowed suffix deep-copied into the slot's payload sidecar. One
 * queue is what makes ordering exact: an update can neither overtake nor be
 * overtaken by control output queued around it.
 * false => the ring or the copy is exhausted; the caller must fail closed. */
static bool
bind_defer_push_nsu(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, true)) {
        return false;
    }
    uint32_t slot = (b->pending_head + b->pending_count) % b->pending_cap;
    moqr_intent_t parked = *it;
    if (!bind_nsu_store(b, slot, &parked)) {
        return false;
    }
    bind_pending_append(b, &parked);   /* admitted above, as a namespace */
    return true;
}

/* Flush the core's outward intents to their sessions, durably. The ENTIRE core
 * ring is drained every pump: borrowed-payload views cannot survive to the next
 * pump (relay.h:102), so UPSTREAM_SUBSCRIBE is executed inline the moment it is
 * polled. A blocked NS_FOUND / NS_GONE instead parks in the SAME ordered ring
 * with its suffix deep-copied, so draft-18 Section 6.2's MUST survives without
 * letting it overtake or be overtaken. Once anything is blocked, every
 * subsequent parkable output queues behind it (preserving order). Returns true
 * if nothing is parked (caller may deliver), false otherwise (caller holds
 * delivery so no object overtakes a parked terminal). */
static bool
bind_execute_intents(moqr_bind_t *b, uint64_t now_us)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    b->intent_drain_active = true;

    /* 1. Retry parked scalars first, in FIFO order. Stop retrying at the first
     *    that still blocks (nothing behind it may overtake it), but fall
     *    through to step 2 so borrowed intents newly in the ring are still
     *    consumed this pump. */
    bool blocked = false;
    while (b->pending_count > 0) {
        moqr_intent_t defer;
        uint32_t epoch = b->pending_purge_epoch;
        if (!bind_try_intent(b, &b->pending[b->pending_head], now_us,
                             BIND_TRY_FROM_RING, NULL, &defer)) {
            b->pending[b->pending_head] = defer;
            blocked = true;
            break;
        }
        if (b->pending_purge_epoch != epoch) {
            /* The retry detached its own connection: purge already released
             * this slot and compacted the ring, so the head is now somebody
             * else's entry. Popping here would silently discard it. */
            continue;
        }
        bind_nsu_release(b, b->pending_head);   /* no-op unless it held bytes */
        b->pending_head = (b->pending_head + 1u) % b->pending_cap;
        b->pending_count--;
    }
    blocked = blocked || b->pending_count > 0;
    /* 2. Drain the whole ring. Consume-in-pump borrowed intents
     *    (UPSTREAM_SUBSCRIBE) execute inline and are never parked; NS_FOUND /
     *    NS_GONE are borrowed too but DO park, after their suffix is
     *    deep-copied into the slot's sidecar. Both those and scalar intents
     *    park once anything is blocked (order), else execute and park only if
     *    they themselves block. */
    size_t n;
    while ((n = moqr_core_poll_intents(b->core, b->pump_intents,
                                       BIND_PUMP_INTENT_BATCH)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moqr_intent_t *it = &b->pump_intents[i];
            bool is_nsu = it->kind == MOQR_INTENT_NS_FOUND ||
                          it->kind == MOQR_INTENT_NS_GONE;
            if (!bind_intent_deferrable(it->kind) && !is_nsu) {
                /* Router-only / consume-in-pump kinds (UPSTREAM_SUBSCRIBE):
                 * they own no per-session output order to preserve. */
                moqr_intent_t tw;
                (void)bind_try_intent(b, it, now_us, BIND_TRY_FRESH, NULL,
                                      &tw);
                continue;
            }
            if (blocked) {
                /* Anything ordered joins the one FIFO with NO inline send:
                 * an attempt here could reach the wire ahead of what is
                 * already parked. */
                if (is_nsu) {
                    moqr_intent_t tw;
                    (void)bind_try_intent(b, it, now_us,
                                          BIND_TRY_FORCE_PARK, NULL, &tw);
                } else if (!bind_defer_push(b, it)) {
                    bind_ordered_overflow_close(b, it, now_us);
                }
                continue;
            }
            moqr_intent_t defer;
            bool parked = false;
            if (!bind_try_intent(b, it, now_us, BIND_TRY_FRESH, &parked,
                                 &defer)) {
                if (!bind_defer_push(b, &defer)) {
                    bind_ordered_overflow_close(b, &defer, now_us);
                }
                blocked = true;
            } else if (parked) {
                /* A namespace update just joined the FIFO: everything polled
                 * after it must queue behind it, not race it. */
                blocked = true;
            }
        }
    }
    b->intent_drain_active = false;
    return !blocked;
}

/* Drop any parked outputs targeting a connection that is tearing down: its
 * session is about to go away, so the writes are moot. Called at close, before
 * the slot (and its binding cookie) can be reused by a new connection. */
static void
bind_pending_purge_conn(moqr_bind_t *b, uint64_t binding_cookie)
{
    uint32_t kept = 0;
    uint32_t scanned = b->pending_count;
    for (uint32_t i = 0; i < scanned; i++) {
        uint32_t from = (b->pending_head + i) % b->pending_cap;
        moqr_intent_t *src = &b->pending[from];
        if (src->binding_cookie == binding_cookie) {
            bind_nsu_release(b, from);   /* frees any retained namespace bytes */
            continue;
        }
        uint32_t to = (b->pending_head + kept) % b->pending_cap;
        if (to != from) {
            /* The payload sidecar is keyed by slot, so it moves with the
             * entry and the parked view is re-pointed at its new home. */
            /* The sidecar bookkeeping moves with the entry; the byte
             * allocation itself does not, so the parked part pointers stay
             * valid without fixup. */
            bind_nsu_release(b, to);
            b->nsu[to] = b->nsu[from];
            memset(&b->nsu[from], 0, sizeof(b->nsu[from]));
            b->pending[to] = *src;
        }
        kept++;
    }
    if (kept != scanned) {
        b->pending_purge_epoch++;   /* the ring was rewritten under any caller */
    }
    b->pending_count = kept;
}

static bool
bind_retry_may_drain_intents(moqr_bind_t *b)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    return true;
}

/* A core call that queues intents may return WOULD_BLOCK when the ring is
 * momentarily full (leftover intents from earlier in the same pump). The
 * core's atomicity invariant guarantees any single fan-out fits an EMPTY
 * ring, so executing the pending intents (a full drain) and retrying makes
 * progress outside an already-active drain. During an intent drain, preserve
 * WOULD_BLOCK for the caller-specific defer path instead of recursively
 * reusing b->pump_intents and overwriting the outer batch. Evaluate the
 * expression, draining+retrying up to a small bound; assign the final result
 * to `dst`. */
#define BIND_CALL_RETRY(dst, expr)                                        \
    do {                                                                  \
        (dst) = (expr);                                                   \
        for (int _r = 0; (dst) == MOQR_ERR_WOULD_BLOCK && _r < 8; _r++) { \
            if (!bind_retry_may_drain_intents(b)) {                       \
                break;                                                    \
            }                                                             \
            (void)bind_execute_intents(b, now_us);                        \
            (dst) = (expr);                                               \
        }                                                                 \
    } while (0)

static void
ann_clear_slot(moqr_bind_t *b, b_conn_t *cn, uint32_t slot)
{
    if (slot >= b->n_anns || cn->anns[slot].used == false) {
        return;
    }
    if (cn->anns[slot].bytes != NULL) {
        b->alloc.free(cn->anns[slot].bytes, cn->anns[slot].bytes_len,
                      b->alloc.ctx);
    }
    memset(&cn->anns[slot], 0, sizeof(cn->anns[slot]));
}

static void
ann_clear_all(moqr_bind_t *b, b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        ann_clear_slot(b, cn, i);
    }
}

static int
ann_find(const moqr_bind_t *b, const b_conn_t *cn, uint64_t raw)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (cn->anns[i].used && cn->anns[i].ann_raw == raw) {
            return (int)i;
        }
    }
    return -1;
}

static int
ann_store(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, moqr_ns_t ns)
{
    /* draft-18 Section 2.4.1 admits 0..32 fields, so a ROOT namespace (count 0)
     * is a legitimate announce and must be storable; draft-16's 1..32 floor is
     * enforced by the session profile before the event reaches here. Only a
     * NON-EMPTY namespace needs a parts array. */
    if (raw == 0 || ns.count > 32 || (ns.count > 0 && ns.parts == NULL) ||
        ann_find(b, cn, raw) >= 0) {
        return -1;
    }
    int slot = -1;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (!cn->anns[i].used) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return -1;
    }
    size_t total = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Every PRESENT Track Namespace Field must carry at least one byte
         * (draft-18 and draft-16 Section 2.4.1). Zero fields is a legal ROOT
         * namespace; a zero-LENGTH field is not, whether or not its pointer is
         * non-NULL. */
        if (ns.parts[i].len == 0 || ns.parts[i].data == NULL) {
            return -1;
        }
        if (SIZE_MAX - total < ns.parts[i].len) {
            return -1;
        }
        total += ns.parts[i].len;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return -1;
        }
    }
    b_ann_t *a = &cn->anns[slot];
    memset(a, 0, sizeof(*a));
    a->used = true;
    a->ann_raw = raw;
    a->bytes = bytes;
    a->bytes_len = total;
    a->ns.parts = a->parts;
    a->ns.count = ns.count;
    size_t off = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Validated above: every present field has len > 0 and non-NULL data. */
        a->parts[i].len = ns.parts[i].len;
        memcpy(bytes + off, ns.parts[i].data, ns.parts[i].len);
        a->parts[i].data = bytes + off;
        off += ns.parts[i].len;
    }
    return slot;
}

static bool
ns_equal(moqr_ns_t a, moqr_ns_t b)
{
    if (a.count != b.count) {
        return false;
    }
    for (size_t i = 0; i < a.count; i++) {
        if (a.parts[i].len != b.parts[i].len ||
            memcmp(a.parts[i].data, b.parts[i].data, a.parts[i].len) != 0) {
            return false;
        }
    }
    return true;
}

static bool
conn_has_other_announce(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t raw, moqr_ns_t ns)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        const b_ann_t *ann = &cn->anns[i];
        if (ann->used && ann->ann_raw != raw && ns_equal(ann->ns, ns)) {
            return true;
        }
    }
    return false;
}

/* The core has one source slot for an exact namespace. Drafts 16 Section 8.3
 * and 18 Section 9.3 permit establishment time to select a publisher when an
 * implementation cannot retain every source. This binding's deterministic
 * policy is newest-wins across connections: retire the incumbent generation,
 * including every track it sourced, then install the new one. A duplicate on
 * the same connection remains a request error and never displaces itself. */
static moqr_result_t
bind_announce(moqr_bind_t *b, b_conn_t *cn, moqr_ns_t ns, uint64_t raw,
              uint64_t now_us)
{
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    if (rc != MOQR_ERR_WRONG_STATE ||
        conn_has_other_announce(b, cn, raw, ns)) {
        return rc;
    }

    BIND_CALL_RETRY(rc, moqr_core_force_withdraw(
                            b->core, ns, MOQ_REQUEST_ERROR_UNINTERESTED,
                            now_us));
    if (rc != MOQR_OK) {
        return rc;
    }
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    return rc;
}

#ifdef MOQR_BIND_TESTING
/* Drive the PRODUCTION ann_store validator on one live conn slot.
 *
 * A malformed present Track Namespace Field cannot arrive through a real
 * session -- the parser rejects it first -- so this is the only way to exercise
 * bind's defense-in-depth arm without copying the predicate into the test.
 * Returns true when ann_store ACCEPTED the namespace; an accepted slot is
 * released again so the probe leaves no announcement state behind. */
bool
moqr_bind_debug_ann_store_probe(moqr_bind_t *b, uint32_t slot, uint64_t raw,
                                moqr_ns_t ns)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return false;
    }
    int r = ann_store(b, &b->conns[slot], raw, ns);
    if (r < 0) {
        return false;
    }
    ann_clear_slot(b, &b->conns[slot], (uint32_t)r);
    return true;
}
#endif

/* An upstream terminal code is that connection's draft, not ours. Capture the
 * origin with the code at ingest: the record may be replayed to a subscriber
 * on the other draft long after this connection is gone, and by then nothing
 * else can say which registry the number was written in. */
static moqr_reset_desc_t
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
static bool
bind_reset_wire(const moqr_bind_t *b, const b_conn_t *cn,
                moqr_reset_desc_t desc, uint64_t *out)
{
    return moqr_reset_desc_emit(desc, moqr_bind_conn_version(b, cn->session),
                                out) == MOQR_OK;
}

/* An upstream PUBLISH_DONE status is that connection's draft. Capture the
 * origin with it: downstream subscribers may read the other draft, and the
 * terminal outlives this connection. */
static moqr_pd_desc_t
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
static moqr_pd_desc_t
bind_local_pd(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static void
conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    if (!cn->used || cn->closed) {
        return;
    }
    cn->closed = true;   /* no new work routes to this conn */
    bind_dl_clear_conn(b, cn);
    /* Retire this binding's parked (DEFERred) requests and revalidation grants
     * before the core binding close, freeing their deep-copied bytes: a later
     * scheduled recheck can no longer touch a torn-down session. Drop any
     * parked output intents for this conn too — its session is going away, so
     * they are moot, and the cookie (= slot + 1) may be reused. */
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    bind_pending_purge_conn(b, bcookie);
    moqr_core_retire_parked(b->core, bcookie);
    moqr_core_retire_grants(b->core, bcookie);
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
            if (cn->fetches != NULL) {
                a.free(cn->fetches, (size_t)b->n_fetches * sizeof(*cn->fetches),
                       a.ctx);
            }
        }
        a.free(b->conns, (size_t)b->max_conns * sizeof(*b->conns), a.ctx);
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
    if (b->pending != NULL) {
        a.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending), a.ctx);
    }
    a.free(b, sizeof(*b), a.ctx);
}

static b_conn_t *
conn_by_session(moqr_bind_t *b, const moq_session_t *s)
{
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used && b->conns[i].session == s) {
            return &b->conns[i];
        }
    }
    return NULL;
}

static b_conn_t *
conn_by_cookie(moqr_bind_t *b, uint64_t cookie)
{
    if (cookie < 1 || cookie > b->max_conns) {
        return NULL;
    }
    b_conn_t *cn = &b->conns[cookie - 1];
    return cn->used ? cn : NULL;
}

moqr_result_t
moqr_bind_conn_open(moqr_bind_t *b, moq_session_t *session,
                    moq_version_t version)
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
    default:                   return MOQR_ERR_INVAL;
    }
    if (conn_by_session(b, session) != NULL) {
        return MOQR_ERR_INVAL;   /* duplicate attach */
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used) {
            moqr_binding_t binding;
            moqr_result_t rc = moqr_core_binding_open(b->core,
                                                      (uint64_t)i + 1,
                                                      &binding);
            if (rc != MOQR_OK) {
                return rc;
            }
            cn->used = true;
            cn->closed = false;
            cn->session = session;
            cn->wire_draft = draft;
            cn->binding = binding;
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
            memset(cn->sgs, 0, (size_t)b->n_sgs * sizeof(*cn->sgs));
            memset(cn->usubs, 0, (size_t)b->n_usubs * sizeof(*cn->usubs));
            memset(cn->pubs, 0, (size_t)b->n_pubs * sizeof(*cn->pubs));
            memset(cn->fetches, 0, (size_t)b->n_fetches * sizeof(*cn->fetches));
            return MOQR_OK;
        }
    }
    return MOQR_ERR_CAPACITY;
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
    return cn->wire_draft == B_DRAFT_18 ? MOQ_VERSION_DRAFT_18
                                        : MOQ_VERSION_DRAFT_16;
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

/* -- event translation (the parity-proven table) --------------------------- */

static moqr_ns_t
ns_from_event(const moq_namespace_t *ns, moq_bytes_t *storage)
{
    size_t count = ns->count < 32 ? ns->count : 32;
    for (size_t i = 0; i < count; i++) {
        storage[i] = ns->parts[i];
    }
    return (moqr_ns_t){ .parts = storage, .count = count };
}

/* Session-level error code for an unauthorized connection (setup DENY). This
 * is the SESSION close code (0x2), distinct from the request-level
 * MOQ_REQUEST_ERROR_UNAUTHORIZED (0x1) used to reject individual requests. */
#define BIND_SESSION_UNAUTHORIZED 0x2u

/* The outcome of authorizing one control request. */
typedef enum {
    BIND_AUTH_ALLOW = 0,   /* proceed with the op                          */
    BIND_AUTH_DENY,        /* reject with *err                             */
    BIND_AUTH_DEFER,       /* park under *ticket; resolve later            */
    BIND_AUTH_FAIL_CLOSED, /* token mirror failed / DEFER without a ticket */
} bind_auth_result_t;

/* Authorize a control request against the core's auth hook via the shared,
 * unit-tested seam. On DENY/FAIL_CLOSED, *err gets the wire error code; on
 * DEFER, *ticket gets the hook's (nonzero) correlation ticket; on ALLOW, *lease
 * (when non-NULL) gets the verdict's revalidate_after_us — the caller passes a
 * pointer only for the grantable actions (SUBSCRIBE / PUBLISH_NAMESPACE) that
 * create a CAT revalidation grant, and NULL everywhere else. */
static bind_auth_result_t
bind_authorize(moqr_bind_t *b, const b_conn_t *cn, moqr_auth_action_t action,
               moqr_ns_t ns, moq_bytes_t name,
               const moq_resolved_token_t *tokens, size_t token_count,
               uint64_t now_us, uint64_t *err, uint64_t *ticket, uint64_t *lease)
{
    moqr_auth_verdict_t v;
    if (!moqr_bind_auth_eval(b->core, action, ns, name, tokens, token_count,
                             (uint64_t)(cn - b->conns) + 1u, now_us, &v)) {
        if (err != NULL) {
            *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_FAIL_CLOSED; /* token set couldn't be mirrored */
    }
    switch (v.decision) {
    case MOQR_AUTH_ALLOW:
        if (lease != NULL) {
            *lease = v.revalidate_after_us;
        }
        return BIND_AUTH_ALLOW;
    case MOQR_AUTH_DEFER:
        if (v.ticket == 0) {
            if (err != NULL) {
                *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            }
            return BIND_AUTH_FAIL_CLOSED; /* DEFER needs a nonzero ticket */
        }
        if (ticket != NULL) {
            *ticket = v.ticket;
        }
        return BIND_AUTH_DEFER;
    default: /* canonicalized DENY */
        if (err != NULL) {
            *err = v.error_code != 0 ? v.error_code
                                     : MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_DENY;
    }
}

/* Fill the common (every-action) fields of a parked request. SUBSCRIBE callers
 * additionally set the pr->sub_* scalars before parking. */
static void
bind_park_init(moqr_park_req_t *pr, const b_conn_t *cn, const moqr_bind_t *b,
               moqr_auth_action_t action, moqr_ns_t ns, moq_bytes_t name,
               uint64_t session_cookie)
{
    memset(pr, 0, sizeof(*pr));
    pr->action = action;
    pr->binding_cookie = (uint64_t)(cn - b->conns) + 1u;
    pr->session_cookie = session_cookie;
    pr->ns = ns;
    pr->name = name;
}

/* Mirror the event's borrowed tokens (fail closed if the set can't be
 * faithfully mirrored) and hand the request to core parked storage, which
 * deep-copies it. */
static moqr_result_t
bind_park(moqr_bind_t *b, moqr_park_req_t *pr,
          const moq_resolved_token_t *tokens, size_t token_count,
          uint64_t ticket, uint64_t now_us)
{
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_park(b->core, pr, ticket, now_us);
}

/* Reserve a CAT revalidation grant for an ALLOWed SUBSCRIBE / PUBLISH_NAMESPACE
 * that carries a lease, BEFORE the core op mutates state. `pr` is already
 * populated by bind_park_init (action / cookies / ns / name). A lease of 0 means
 * no grant is wanted: *res is zeroed and MOQR_OK returned. Otherwise the event's
 * borrowed tokens are mirrored (fail closed if the set can't be represented) and
 * the reservation is taken; the core deep-copies the material. Any non-OK return
 * means the caller MUST fail the op closed with no wire accept. On success *res
 * identifies the reservation for a later commit (op OK) or abort (op failed). */
static moqr_result_t
bind_grant_reserve(moqr_bind_t *b, moqr_park_req_t *pr,
                   const moq_resolved_token_t *tokens, size_t token_count,
                   uint64_t lease_us, uint64_t now_us, moqr_grant_res_t *res)
{
    memset(res, 0, sizeof(*res));
    if (lease_us == 0) {
        return MOQR_OK; /* no revalidation grant for this ALLOW */
    }
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_grant_reserve(b->core, pr, lease_us, now_us, res);
}

/* Fixed diagnostic text only: never expose peer tokens or policy inputs. */
static moq_bytes_t
bind_subscribe_error_reason(moq_request_error_t code)
{
    switch (code) {
    case MOQ_REQUEST_ERROR_DOES_NOT_EXIST:
        return MOQ_BYTES_LITERAL("track does not exist");
    case MOQ_REQUEST_ERROR_UNAUTHORIZED:
        return MOQ_BYTES_LITERAL("unauthorized");
    case MOQ_REQUEST_ERROR_TIMEOUT:
        return MOQ_BYTES_LITERAL("timeout");
    case MOQ_REQUEST_ERROR_NOT_SUPPORTED:
        return MOQ_BYTES_LITERAL("not supported");
    case MOQ_REQUEST_ERROR_INVALID_RANGE:
        return MOQ_BYTES_LITERAL("invalid range");
    case MOQ_REQUEST_ERROR_MALFORMED_TRACK:
        return MOQ_BYTES_LITERAL("malformed track");
    case MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION:
        return MOQ_BYTES_LITERAL("duplicate subscription");
    case MOQ_REQUEST_ERROR_UNINTERESTED:
        return MOQ_BYTES_LITERAL("uninterested");
    case MOQ_REQUEST_ERROR_EXCESSIVE_LOAD:
        return MOQ_BYTES_LITERAL("excessive load");
    case MOQ_REQUEST_ERROR_INTERNAL_ERROR:
        return MOQ_BYTES_LITERAL("internal error");
    default:
        return (moq_bytes_t){ NULL, 0 };
    }
}

static void
bind_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg,
                               moq_request_error_t code)
{
    moq_reject_subscribe_cfg_init(cfg);
    cfg->error_code = code;
    cfg->reason = bind_subscribe_error_reason(code);
}

/* Reject the pending session request identified by (action, session_cookie)
 * with the wire error code. Shared by the inline refuse path and deferred
 * resolution, reconstructing the handle from the stored cookie. */
static void
bind_reject(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
            uint64_t session_cookie, uint64_t err, uint64_t now_us)
{
    (void)b;
    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_reject_namespace_cfg_t r;
        moq_reject_namespace_cfg_init(&r);
        r.error_code = err;
        (void)moq_session_reject_namespace(
            cn->session, (moq_announcement_t){ ._opaque = session_cookie }, &r,
            now_us);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_reject_subscribe_cfg_t r;
        bind_reject_subscribe_cfg_init(&r, err);
        (void)moq_session_reject_subscribe(
            cn->session, (moq_subscription_t){ ._opaque = session_cookie }, &r,
            now_us);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_reject_ns_sub_cfg_t r;
        moq_reject_ns_sub_cfg_init(&r);
        r.error_code = err;
        (void)moq_session_reject_ns_sub(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = session_cookie }, &r,
            now_us);
        break;
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_reject_track_status_cfg_t r;
        moq_reject_track_status_cfg_init(&r);
        r.error_code = err;
        (void)moq_session_reject_track_status(
            cn->session, (moq_track_status_handle_t){ ._opaque = session_cookie },
            &r, now_us);
        break;
    }
    case MOQR_AUTH_PUBLISH: {
        moq_reject_publish_cfg_t r;
        moq_reject_publish_cfg_init(&r);
        r.error_code = err;
        (void)moq_session_reject_publish(
            cn->session, (moq_publication_t){ ._opaque = session_cookie }, &r,
            now_us);
        break;
    }
    default:
        break;
    }
}

/* Commit a reserved subscribe grant after moqr_core_subscribe succeeded (no-op
 * when there was no grant). A commit failure is an invariant violation — the
 * reservation was just taken — so revoke the just-created sub: no untracked,
 * unrevalidatable subscription may stay live on the wire. */
static void
bind_grant_commit_sub(moqr_bind_t *b, moqr_grant_res_t gres, moqr_sub_t rsub,
                      uint64_t now_us)
{
    if (gres.gen == 0) {
        return; /* no grant (lease 0) */
    }
    if (moqr_core_grant_commit(b->core, gres, rsub._opaque) != MOQR_OK) {
        moqr_result_t vrc;
        /* The grant could not be committed: the truthful terminal is the
         * PUBLISH_DONE registry's INTERNAL_ERROR, not the same-numbered
         * REQUEST_ERROR constant this once borrowed. */
        BIND_CALL_RETRY(vrc, moqr_core_revoke_sub(
                                 b->core, rsub,
                                 bind_local_pd(MOQR_PD_INTERNAL_ERROR),
                                 now_us));
        (void)vrc;
        b->session_errors++;
    }
}

/* Commit a reserved announce grant after moqr_core_announce succeeded. Returns
 * true when the caller should send the wire accept. On the invariant commit
 * failure it undoes the announce and rejects (no untracked namespace stays
 * live) and returns false. A no-grant reservation (lease 0) returns true. */
static bool
bind_grant_commit_ann(moqr_bind_t *b, b_conn_t *cn, moqr_grant_res_t gres,
                      moqr_ns_t ns, uint64_t session_cookie, uint64_t now_us)
{
    if (gres.gen == 0) {
        return true; /* no grant (lease 0): proceed to accept */
    }
    if (moqr_core_grant_commit(b->core, gres, 0) != MOQR_OK) {
        moqr_result_t urc;
        BIND_CALL_RETRY(urc, moqr_core_unannounce(b->core, cn->binding, ns));
        (void)urc;
        bind_reject(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, session_cookie,
                    MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        b->session_errors++;
        return false;
    }
    return true;
}

/* Resume the original ALLOWed op from a parked view: re-run the same core call
 * + session accept the inline path would have. An ALLOW that carries a CAT
 * lease (`lease` > 0) reserves + commits a revalidation grant around the op,
 * exactly like the synchronous request path — an async ALLOW must be revalidat-
 * able too. On core failure (WOULD_BLOCK exhausted or error), fail closed by
 * rejecting the pending request. */
static void
bind_resume(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
            uint64_t lease, uint64_t now_us)
{
    moqr_result_t rc = MOQR_OK;
    switch (v->action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        /* The pinned view already holds deep-copied moqr_auth_token_t bytes, so
         * reserve straight from it (no session-token mirror). Reserve fails the
         * request closed with no announce; op failure aborts the reservation. */
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        int ann_slot = ann_store(b, cn, v->session_cookie, v->ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            return;
        }
        rc = bind_announce(b, cn, v->ns, v->session_cookie, now_us);
        if (rc != MOQR_OK) {
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            break;
        }
        if (bind_grant_commit_ann(b, cn, gres, v->ns, v->session_cookie,
                                  now_us)) {
            moq_accept_namespace_cfg_t a;
            moq_accept_namespace_cfg_init(&a);
            if (moq_session_accept_namespace(
                    cn->session,
                    (moq_announcement_t){ ._opaque = v->session_cookie }, &a,
                    now_us) != MOQ_OK) {
                b->session_errors++;
            }
        } else {
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        return;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = v->ns;
        rq.name = v->name;
        rq.filter.type = (moqr_filter_type_t)v->sub_filter_type;
        rq.filter.start_group = v->sub_start_group;
        rq.filter.start_object = v->sub_start_object;
        rq.filter.end_group_delta = v->sub_end_group_delta;
        rq.subscriber_priority = v->sub_priority;
        rq.group_order = (moqr_group_order_t)v->sub_group_order;
        rq.cookie = v->session_cookie;
        moqr_sub_t rs;
        BIND_CALL_RETRY(rc, moqr_core_subscribe(b->core, cn->binding, &rq, &rs));
        if (rc != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            break;
        }
        bind_grant_commit_sub(b, gres, rs, now_us);
        return; /* ACCEPT_SUB / REJECT_SUB flows through intents */
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
        BIND_CALL_RETRY(rc, moqr_core_ns_subscribe(b->core, cn->binding, v->ns,
                                                   v->session_cookie));
        if (rc == MOQR_OK) {
            moq_accept_ns_sub_cfg_t a;
            moq_accept_ns_sub_cfg_init(&a);
            if (moq_session_accept_ns_sub(
                    cn->session,
                    (moq_ns_sub_handle_t){ ._opaque = v->session_cookie }, &a,
                    now_us) != MOQ_OK) {
                b->session_errors++;
            }
            return;
        }
        break;
    case MOQR_AUTH_TRACK_STATUS:
        BIND_CALL_RETRY(rc, moqr_core_track_status(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie));
        if (rc == MOQR_OK) {
            return;
        }
        break;
    case MOQR_AUTH_PUBLISH: {
        moqr_track_t tr;
        BIND_CALL_RETRY(rc, moqr_core_publish_open(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie,
                                                   &tr));
        if (rc == MOQR_OK) {
            return;
        }
        break;
    }
    default:
        return;
    }
    /* Resume failed: reject the pending request rather than leave it hanging. */
    bind_reject(b, cn, v->action, v->session_cookie,
                MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
}

/* Handle a non-ALLOW result at a request site: DEFER parks (fail closed if the
 * park itself fails), and DENY/FAIL_CLOSED/park-failure reject. */
static void
bind_refuse_or_park(moqr_bind_t *b, b_conn_t *cn, bind_auth_result_t ar,
                    moqr_park_req_t *pr, const moq_resolved_token_t *tokens,
                    size_t token_count, uint64_t ticket, uint64_t err,
                    uint64_t now_us)
{
    if (ar == BIND_AUTH_DEFER &&
        bind_park(b, pr, tokens, token_count, ticket, now_us) == MOQR_OK) {
        return; /* parked: leave the session request pending */
    }
    /* DENY / FAIL_CLOSED / park failure -> reject fail-closed. A valid DEFER
     * leaves err == 0 (bind_authorize only fills it for DENY/FAIL_CLOSED), so a
     * park failure (duplicate ticket / capacity / byte budget) must still carry
     * a definite wire code, never 0. */
    if (err == 0) {
        err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    }
    bind_reject(b, cn, pr->action, pr->session_cookie, err, now_us);
}

/* Which signal ended the relay's OWN upstream request. draft-18 gives these
 * two different meanings and they must not collapse into one cause:
 *
 *   GOAWAY on a request stream (Section 10.4) — the upstream is going away and
 *   the request should be re-issued elsewhere;
 *   REQUEST_ERROR REDIRECT (Section 10.6.1) — nobody is going away; this
 *   endpoint cannot serve the request and names where it could be served.
 *
 * draft-16 has neither: its GOAWAY (Section 9.4) is session-scoped only, and
 * its REQUEST_ERROR registry (Section 13.4.2) assigns no REDIRECT. */
typedef enum bind_upstream_end {
    BIND_UP_END_GOAWAY = 0,
    BIND_UP_END_REDIRECT,
} bind_upstream_end_t;

/* The PUBLISH_DONE status an ACTIVE upstream subscription ends with. The relay
 * neither re-issues a request nor follows a Redirect, so what it owes its
 * downstream subscribers is a truthful terminal for its OWN state:
 *
 *   GOAWAY   -> GOING_AWAY: the upstream really is departing (0x4 in both
 *               registries, d16 Section 13.4.3 / d18 Section 15.10.3);
 *   REDIRECT -> TRACK_ENDED: nobody is departing. This relay will not serve
 *               the track from the named location, so the track has ended as
 *               far as these subscribers are concerned (0x2 in both). Saying
 *               GOING_AWAY here would assert a departure that is not
 *               happening. */
static moqr_pd_status_t
bind_upstream_end_pd(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
        return MOQR_PD_GOING_AWAY;
    case BIND_UP_END_REDIRECT:
        return MOQR_PD_TRACK_ENDED;
    }
    return MOQR_PD_INTERNAL_ERROR;
}

/* The REQUEST_ERROR a PENDING upstream's parked subscribers are rejected with.
 *
 * TIMEOUT (0x2) is not available to either cause: no timeout elapsed, and
 * draft-18 Section 10.6 defines TIMEOUT as an implementation timeout the relay
 * did not hit. REDIRECT (0x34) is not available either — it is meaningful only
 * with the Redirect structure of Section 10.6.1, which this relay does not
 * carry, and draft-16 does not assign it at all. draft-18's GOING_AWAY (0x6)
 * would be precise for the GOAWAY cause, but draft-16 assigns no GOING_AWAY in
 * this registry, and REQUEST_ERROR codes travel to downstream subscribers as a
 * plain number with no per-target translation — so a d18-only code here would
 * reach a draft-16 subscriber as an unassigned number.
 *
 * The generic INTERNAL_ERROR (0x0) is assigned identically in both registries
 * and states nothing false. */
static uint32_t
bind_upstream_end_reqerr(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
    case BIND_UP_END_REDIRECT:
        return BIND_REQERR_INTERNAL;
    }
    return BIND_REQERR_INTERNAL;
}

/* A relay-originated request was terminated by the upstream. The relay does NOT
 * follow redirect URIs or migrate requests to a new session (a later routing
 * feature), so it treats the request as terminal and notifies downstreams
 * rather than hanging. The only request the relay originates is the upstream
 * SUBSCRIBE, so only that family maps to tracked state: an ACTIVE upstream
 * terminates its downstream subscribers with a wire SUBSCRIBE_DONE
 * (source_done); a PENDING upstream (terminated before its OK) rejects its
 * parked downstream subscribers (upstream_error). Any other family, or a
 * handle that matches no live upstream subscription, is a no-op — these are
 * terminal responses to the relay's OWN requests, not peer requests owed a
 * wire reply.
 *
 * The relay implements no reissue or retry schedule, so no answer it produces
 * here is retryable, whatever retry interval the upstream offered. */
static void
bind_upstream_terminated(moqr_bind_t *b, b_conn_t *cn,
                         moq_request_family_t family, uint64_t handle_raw,
                         bind_upstream_end_t how, uint64_t now_us)
{
    if (family != MOQ_REQUEST_FAMILY_SUBSCRIBE) {
        return; /* the relay originates no other upstream request family */
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != handle_raw) {
            continue;
        }
        moqr_result_t rc;
        BIND_CALL_RETRY(rc, moqr_core_source_done(
            b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
            bind_local_pd(bind_upstream_end_pd(how)), now_us));
        if (rc == MOQR_ERR_WRONG_STATE) {
            /* PENDING upstream (terminal before OK): reject the parked
             * downstream subscribers instead of terminating active ones. */
            BIND_CALL_RETRY(rc, moqr_core_upstream_error(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_end_reqerr(how), now_us));
        }
        if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE) {
            b->session_errors++;
        }
        cn->usubs[i].used = false;
    }
}

#ifdef MOQR_BIND_TESTING
/* Drive the production upstream-termination dispatch for one connection.
 * draft-18 makes two of the four cause/state combinations unreachable over the
 * wire — a REQUEST_ERROR cannot answer a request that already had its OK, and
 * a per-request GOAWAY is eligible only on an ESTABLISHED request — so those
 * two rows are pinned here, against the same function the event handler calls,
 * rather than left unpinned. */
/* The first live upstream subscription this bind holds, as {conn slot, the
 * handle on that connection's session} — the two coordinates the termination
 * dispatch matches on. */
bool
moqr_bind_debug_first_usub(const moqr_bind_t *b, uint32_t *slot,
                           uint64_t *handle_raw)
{
    if (b == NULL || slot == NULL || handle_raw == NULL) {
        return false;
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        if (!b->conns[ci].used) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (b->conns[ci].usubs[i].used) {
                *slot = ci;
                *handle_raw = b->conns[ci].usubs[i].ssub._opaque;
                return true;
            }
        }
    }
    return false;
}

void
moqr_bind_debug_upstream_terminated(moqr_bind_t *b, uint32_t slot,
                                    uint32_t family, uint64_t handle_raw,
                                    int redirect, uint64_t now_us)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return;
    }
    bind_upstream_terminated(b, &b->conns[slot], (moq_request_family_t)family,
                             handle_raw,
                             redirect ? BIND_UP_END_REDIRECT
                                      : BIND_UP_END_GOAWAY,
                             now_us);
}
#endif

/* Map an inbound data event's sub/pub handle to the relay track it feeds (the
 * relay's upstream subscription or a peer-initiated publish). MOQR_TRACK_INVALID
 * when it targets neither — i.e. not relay ingest. */
static moqr_track_t
bind_ingest_track(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                  moq_publication_t pub)
{
    if (moq_subscription_is_valid(sub)) {
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == sub._opaque) {
                return cn->usubs[i].track;
            }
        }
    } else if (moq_publication_is_valid(pub)) {
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].pub_raw == pub._opaque) {
                return cn->pubs[i].track;
            }
        }
    }
    return MOQR_TRACK_INVALID;
}

static void
bind_on_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
              uint64_t now_us)
{
    moq_bytes_t nsbuf[32];
    b->events_translated++;
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        const moq_namespace_published_event_t *np =
            &ev->u.namespace_published;
        moqr_ns_t ns = ns_from_event(&np->track_namespace, nsbuf);
        uint64_t aerr = 0;
        uint64_t atkt = 0;
        uint64_t alease = 0;
        bind_auth_result_t aar =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->tokens,
                           np->token_count, now_us, &aerr, &atkt, &alease);
        if (aar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
            bind_refuse_or_park(b, cn, aar, &pr, np->tokens, np->token_count,
                                atkt, aerr, now_us);
            break;
        }
        /* ALLOW: if it carries a lease, reserve a revalidation grant BEFORE the
         * announce mutates routing state. A reserve failure fails the request
         * closed with no wire accept (CAT: a recipient that cannot track a
         * revalidatable grant rejects the request). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                       (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, np->tokens, np->token_count, alease,
                               now_us, &gres) != MOQR_OK) {
            moq_reject_namespace_cfg_t rcfg;
            moq_reject_namespace_cfg_init(&rcfg);
            rcfg.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            (void)moq_session_reject_namespace(cn->session, np->ann, &rcfg,
                                               now_us);
            break;
        }
        int ann_slot = ann_store(b, cn, np->ann._opaque, ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            moq_reject_namespace_cfg_t rcfg;
            moq_reject_namespace_cfg_init(&rcfg);
            rcfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_namespace(cn->session, np->ann, &rcfg,
                                               now_us);
            break;
        }
        moqr_result_t arc;
        arc = bind_announce(b, cn, ns, np->ann._opaque, now_us);
        if (arc != MOQR_OK) {
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            moq_reject_namespace_cfg_t rcfg;
            moq_reject_namespace_cfg_init(&rcfg);
            rcfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_namespace(cn->session, np->ann, &rcfg,
                                               now_us);
            break;
        }
        /* Announce succeeded. Commit the reservation (if any), undoing the
         * announce on the invariant commit failure. */
        if (bind_grant_commit_ann(b, cn, gres, ns, np->ann._opaque, now_us)) {
            moq_accept_namespace_cfg_t acfg;
            moq_accept_namespace_cfg_init(&acfg);
            if (moq_session_accept_namespace(cn->session, np->ann, &acfg,
                                             now_us) != MOQ_OK) {
                b->session_errors++;
            }
        } else {
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        break;
    }
    case MOQ_EVENT_NAMESPACE_DONE: {
        uint64_t raw = ev->u.namespace_done.ann._opaque;
        int ann_slot = ann_find(b, cn, raw);
        if (ann_slot < 0) {
            break;
        }
        moqr_result_t urc;
        BIND_CALL_RETRY(urc, moqr_core_unannounce(
            b->core, cn->binding, cn->anns[ann_slot].ns));
        if (urc == MOQR_OK || urc == MOQR_ERR_STALE_HANDLE ||
            urc == MOQR_ERR_WRONG_STATE) {
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        } else {
            b->session_errors++;
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_REQUEST: {
        const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
        moqr_ns_t sns = ns_from_event(&sq->track_namespace, nsbuf);
        uint64_t serr = 0;
        uint64_t stkt = 0;
        uint64_t slease = 0;
        bind_auth_result_t sar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                           sq->tokens, sq->token_count, now_us, &serr, &stkt,
                           &slease);
        if (sar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE, sns,
                           sq->track_name, sq->sub._opaque);
            pr.sub_filter_type = sq->filter;
            pr.sub_start_group = sq->start_group;
            pr.sub_start_object = sq->start_object;
            pr.sub_end_group_delta =
                sq->filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE
                    ? sq->end_group - sq->start_group
                    : 0;
            pr.sub_priority = sq->subscriber_priority;
            pr.sub_group_order = sq->group_order;
            bind_refuse_or_park(b, cn, sar, &pr, sq->tokens, sq->token_count,
                                stkt, serr, now_us);
            break;
        }
        /* ALLOW: reserve a revalidation grant BEFORE subscribing when the
         * verdict carries a lease. A reserve failure fails the request closed
         * with no wire OK (CAT: cannot track a revalidatable grant -> reject). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                       sq->sub._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, sq->tokens, sq->token_count, slease,
                               now_us, &gres) != MOQR_OK) {
            moq_reject_subscribe_cfg_t rj;
            bind_reject_subscribe_cfg_init(&rj,
                                           MOQ_REQUEST_ERROR_UNAUTHORIZED);
            (void)moq_session_reject_subscribe(cn->session, sq->sub, &rj,
                                               now_us);
            break;
        }
        moqr_subscribe_req_t req;
        moqr_subscribe_req_init(&req);
        req.ns = sns;
        req.name = sq->track_name;
        if (sq->filter != MOQ_SUBSCRIBE_FILTER_NONE) {
            req.filter.type = (moqr_filter_type_t)sq->filter;
            req.filter.start_group = sq->start_group;
            req.filter.start_object = sq->start_object;
            if (sq->filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) {
                req.filter.end_group_delta = sq->end_group - sq->start_group;
            }
        }
        req.subscriber_priority = sq->subscriber_priority;
        req.group_order = (moqr_group_order_t)sq->group_order;
        req.cookie = sq->sub._opaque;
        req.forward = sq->forward;   /* carry initial Forward (a paused
                                      * subscription must not be scheduled) */
        moqr_sub_t rsub;
        moqr_result_t src;
        BIND_CALL_RETRY(src, moqr_core_subscribe(b->core, cn->binding,
                                                 &req, &rsub));
        if (src != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            moq_reject_subscribe_cfg_t rj;
            bind_reject_subscribe_cfg_init(&rj,
                                           MOQ_REQUEST_ERROR_INTERNAL_ERROR);
            (void)moq_session_reject_subscribe(cn->session, sq->sub, &rj,
                                               now_us);
            break;
        }
        /* Subscribe succeeded; the ACCEPT_SUB flows through intents. Commit the
         * reservation with the new sub handle (revoking it on the invariant
         * commit failure). */
        bind_grant_commit_sub(b, gres, rsub, now_us);
        break;
    }
    case MOQ_EVENT_UNSUBSCRIBED: {
        uint64_t raw = ev->u.unsubscribed.sub._opaque;
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                (void)moqr_core_unsubscribe(b->core, cn->dsubs[i].rsub,
                                            now_us);
                cn->dsubs[i].used = false;
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_OK: {
        uint64_t raw = ev->u.subscribe_ok.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_ok(
                    b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                    raw, ev->u.subscribe_ok.has_largest,
                    ev->u.subscribe_ok.largest_group,
                    ev->u.subscribe_ok.largest_object));
                if (urc != MOQR_OK) {
                    b->session_errors++;   /* invariant makes this unreachable */
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_ERROR: {
        uint64_t raw = ev->u.subscribe_error.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_error(
                    b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                    ev->u.subscribe_error.error_code, now_us));
                /* Only release the tracking slot once the core actually
                 * consumed the resolution (it retires the parked subs). */
                if (urc == MOQR_OK) {
                    cn->usubs[i].used = false;
                } else {
                    b->session_errors++;
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_UPDATED: {
        /* A downstream SUBSCRIBE_UPDATE. Request-credit accounting already
         * counted it (bind_event_is_request). Apply a Forward change to relay
         * scheduling: resolve the downstream session handle to its core
         * subscription via b_dsub, then set Forward — Forward=0 pauses object
         * scheduling only (control stays live), Forward=1 resumes. An update
         * omitting FORWARD (has_forward=0) leaves state unchanged. Priority /
         * delivery-timeout / new-group changes on this event are NOT applied
         * here (separate audit item, out of scope for this slice). */
        const moq_subscribe_updated_event_t *up = &ev->u.subscribe_updated;
        if (up->has_forward) {
            uint64_t raw = up->sub._opaque;
            bool matched = false;
            for (uint32_t i = 0; i < b->n_dsubs; i++) {
                if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                    moqr_result_t frc = moqr_core_sub_set_forward(
                        b->core, cn->dsubs[i].rsub, up->forward, now_us);
                    /* STALE = core already retired it (a terminal crossed):
                     * moot. Any other non-OK is a broken invariant — fail
                     * closed via the counter, never touch another slot. */
                    if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                        b->session_errors++;
                    }
                    matched = true;
                    break;
                }
            }
            /* An update for a subscription the binding does not track is an
             * invariant break: fail closed (count it), mutate nothing. */
            if (!matched) {
                b->session_errors++;
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_DONE: {
        /* The origin finished the relay's upstream subscription: terminate the
         * track's downstream subscribers with a wire SUBSCRIBE_DONE and release
         * the upstream, so nothing hangs behind a sourceless ACTIVE track. The
         * upstream subscription is over on the wire, so the tracking slot is
         * always released (STALE/WRONG_STATE mean the core already moved on). */
        uint64_t raw = ev->u.subscribe_done.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw) {
                continue;
            }
            moqr_result_t drc;
            BIND_CALL_RETRY(drc, moqr_core_source_done(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_pd(b, cn,
                                 ev->u.subscribe_done.status_code),
                now_us));
            if (drc != MOQR_OK && drc != MOQR_ERR_STALE_HANDLE &&
                drc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            cn->usubs[i].used = false;
        }
        break;
    }
    case MOQ_EVENT_REQUEST_REDIRECT: {
        /* The relay does not follow redirect URIs (a later routing feature);
         * the redirected upstream request is terminal. The Redirect's retry
         * interval is deliberately not read: there is no reissue schedule to
         * feed it to. */
        bind_upstream_terminated(b, cn, ev->u.request_redirect.family,
                                 ev->u.request_redirect.handle.raw,
                                 BIND_UP_END_REDIRECT, now_us);
        break;
    }
    case MOQ_EVENT_REQUEST_GOAWAY: {
        /* The relay does not migrate a request to a new session; treat the
         * per-request GOAWAY as terminal for the upstream subscription. */
        bind_upstream_terminated(b, cn, ev->u.request_goaway.family,
                                 ev->u.request_goaway.handle.raw,
                                 BIND_UP_END_GOAWAY, now_us);
        break;
    }
    case MOQ_EVENT_OBJECT_RECEIVED: {
        moq_object_received_event_t *ob = &ev->u.object_received;
        moqr_track_t track = bind_ingest_track(b, cn, ob->sub, ob->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = ob->group_id;
        d.subgroup_id = ob->subgroup_id;
        d.object_id = ob->object_id;
        d.publisher_priority = ob->publisher_priority;
        d.status = (moqr_obj_status_t)ob->status;
        d.datagram_pref = ob->datagram;
        d.end_of_group = ob->end_of_group;
        d.payload = ob->payload;        /* ownership transfers on OK */
        d.properties = ob->properties;
        d.now_us = now_us;
        if (moqr_core_ingest(b->core, track, &d) == MOQR_OK) {
            ob->payload = NULL;
            ob->properties = NULL;
        } else {
            b->ingest_refusals++;   /* event cleanup releases the refs */
        }
        break;
    }
    case MOQ_EVENT_OBJECT_CHUNK: {
        /* Streaming ingest: map a streamed NORMAL object onto the
         * OPEN-record machinery -- begin opens, payload slices append, end
         * completes (NORMAL) or abandons (RESET/STOP). Every chunk carries
         * (group, subgroup, object), so continuation chunks route correctly
         * even when concurrent subgroups interleave. Chunked COMPLETE records
         * deliver on the subscribe path, OPEN records at the live edge;
         * an abandon stores the terminal reset code (upstream RESET →
         * the publisher's code, else a relay-defined code) which the subscribe
         * path forwards downstream via reset_subgroup. Retained FETCH
         * still skips chunked records. */
        moq_object_chunk_event_t *oc = &ev->u.object_chunk;
        moqr_track_t track = bind_ingest_track(b, cn, oc->sub, oc->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        /* Status objects (no payload) arrive as one begin+end chunk and are
         * whole-object records -- ingest via the COMPLETE path, which handles
         * EOG/EOT. Only NORMAL objects use the OPEN chunk machinery. */
        if (oc->begin && oc->status != MOQ_OBJECT_NORMAL) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = (moqr_obj_status_t)oc->status;
            d.end_of_group = oc->end_of_group;
            d.payload = NULL;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            if (moqr_core_ingest(b->core, track, &d) == MOQR_OK) {
                oc->properties = NULL;
            } else {
                b->ingest_refusals++;
            }
            break;
        }
        /* NORMAL object: begin opens the OPEN record, carrying the subgroup
         * header's EOG bit — subgroup metadata the log latches for the
         * deferred group-final at seal, and the record retains for the
         * downstream subgroup open. The subgroup itself still seals only via
         * SUBGROUP_FINISHED. */
        if (oc->begin) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = MOQR_OBJ_NORMAL;
            d.end_of_group = oc->end_of_group;
            d.obj_state = MOQR_OBJ_OPEN;
            d.declared_len = oc->payload_length;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            if (moqr_core_ingest(b->core, track, &d) == MOQR_OK) {
                oc->properties = NULL;
            } else {
                b->ingest_refusals++;
                break;   /* open failed: no record to abandon */
            }
        }
        /* Payload slice (begin/continuation/end may each carry one). The log
         * increfs the slice; the event keeps its ref (freed by event cleanup).
         * Fail-closed: an append failure abandons the record, no half-state. */
        if (oc->chunk != NULL) {
            if (moqr_core_append_chunk(b->core, track, oc->group_id,
                                       oc->subgroup_id, oc->object_id,
                                       oc->chunk) != MOQR_OK) {
                moqr_reset_desc_t stopd;
                if (moqr_reset_desc_internal(MOQR_RESET_CODE_STOPPED,
                                             &stopd) == MOQR_OK) {
                    (void)moqr_core_abandon_record(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id,
                                                   oc->object_id, stopd);
                }
                b->ingest_refusals++;
                break;
            }
        }
        /* End: complete (NORMAL) or abandon (RESET/STOP). A failed complete
         * (e.g. bytes != declared) also abandons rather than leave OPEN. The
         * abandon stores the terminal cause code: a genuine upstream RESET
         * forwards the publisher's oc->error_code; STOP / a relay-initiated
         * abandon uses a relay-defined code. Downstream delivery
         * propagates it. */
        if (oc->end) {
            moqr_result_t erc = MOQR_OK;
            if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                erc = moqr_core_complete_record(b->core, track, oc->group_id,
                                                oc->subgroup_id, oc->object_id);
            }
            if (oc->terminal != MOQ_OBJECT_TERMINAL_NORMAL || erc != MOQR_OK) {
                moqr_reset_desc_t rdesc;
                bool have_rdesc;
                if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    rdesc = bind_upstream_reset(b, cn, oc->error_code);
                    have_rdesc = rdesc.tag != MOQR_CODE_NONE;
                } else {
                    have_rdesc = moqr_reset_desc_internal(
                                     MOQR_RESET_CODE_STOPPED, &rdesc) ==
                                 MOQR_OK;
                }
                if (have_rdesc) {
                    (void)moqr_core_abandon_record(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id,
                                                   oc->object_id, rdesc);
                }
                if (erc != MOQR_OK) {
                    b->ingest_refusals++;
                }
            }
        }
        break;
    }
#ifdef MOQ_EVENT_SUBGROUP_RESET
    case MOQ_EVENT_SUBGROUP_RESET: {
        /* The upstream subgroup stream RESET in whole-object receive mode:
         * the session dropped the partial object (no OBJECT_RECEIVED for
         * it), so this event is the only ingest-side evidence the stream
         * ended. Terminate the (group, subgroup) ledger abnormally with the
         * publisher's code — retained COMPLETE records stay servable and
         * each subscription's downstream subgroup is reset (never FIN'd)
         * once it has consumed them. Streaming receive mode surfaces resets
         * as terminal OBJECT_CHUNKs and never reaches here. */
        moq_subgroup_reset_event_t *sr = &ev->u.subgroup_reset;
        moqr_track_t track = bind_ingest_track(b, cn, sr->sub, sr->pub);
        if (moqr_track_is_valid(track)) {
            moqr_reset_desc_t srd = bind_upstream_reset(b, cn,
                                                        sr->error_code);
            if (srd.tag != MOQR_CODE_NONE) {
                (void)moqr_core_reset_subgroup(b->core, track, sr->group_id,
                                               sr->subgroup_id, srd);
            }
        }
        break;
    }
#endif
    case MOQ_EVENT_SUBGROUP_FINISHED: {
        /* The upstream subgroup stream FIN'd (plain FIN or EOG). Seal the log's
         * (group, subgroup): each subscription is then owed one acknowledged
         * SEAL notice (records only carry subgroup_end as advisory metadata),
         * and the notice's close — the ONLY durable FIN path — retires the
         * downstream stream and reclaims the slot. Not emitted for RESET/STOP,
         * so a reset is never mistaken for a graceful finish. Ordering is safe:
         * every OBJECT_RECEIVED for this subgroup was polled and ingested before
         * this event, so the sealed list will not grow. */
        moq_subgroup_finished_event_t *sf = &ev->u.subgroup_finished;
        moqr_track_t track = bind_ingest_track(b, cn, sf->sub, sf->pub);
        if (moqr_track_is_valid(track)) {
            moqr_result_t src = moqr_core_seal_subgroup(
                b->core, track, sf->group_id, sf->subgroup_id);
            if (src == MOQR_ERR_INVAL) {
                /* Malformed final evidence: this header-EOG subgroup's last
                 * object contradicts an already-retained higher object in
                 * the group. The cache must never declare a final below
                 * retained content — fail the publisher closed rather than
                 * discard the contradiction. */
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "eog final conflict", now_us);
                conn_detach(b, cn, now_us);
                break;
            }
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_FINISHED: {
        /* A push publisher finished: same "source ended" contract as an upstream
         * SUBSCRIBE_DONE — terminate the track's downstream subscribers with a
         * wire SUBSCRIBE_DONE (forwarding the finish status) and release the
         * source, so no open-ended subscriber hangs behind a WARM track. */
        uint64_t raw = ev->u.publish_finished.pub._opaque;
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (!cn->pubs[i].used || cn->pubs[i].pub_raw != raw) {
                continue;
            }
            moqr_result_t lrc = moqr_core_source_done(
                b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                bind_upstream_pd(b, cn,
                                 ev->u.publish_finished.status_code),
                now_us);
            if (lrc != MOQR_OK && lrc != MOQR_ERR_STALE_HANDLE &&
                lrc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            cn->pubs[i].used = false;
        }
        break;
    }
    case MOQ_EVENT_NS_SUB_REQUEST: {
        const moq_ns_sub_request_event_t *nq = &ev->u.ns_sub_request;
        moqr_ns_t prefix =
            ns_from_event(&nq->track_namespace_prefix, nsbuf);
        uint64_t nerr = 0;
        uint64_t ntkt = 0;
        bind_auth_result_t nar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->tokens,
                           nq->token_count, now_us, &nerr, &ntkt, NULL);
        if (nar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->handle._opaque);
            bind_refuse_or_park(b, cn, nar, &pr, nq->tokens, nq->token_count,
                                ntkt, nerr, now_us);
            break;
        }
        moqr_result_t nrc;
        BIND_CALL_RETRY(nrc, moqr_core_ns_subscribe(
            b->core, cn->binding, prefix, nq->handle._opaque));
        if (nrc == MOQR_OK) {
            moq_accept_ns_sub_cfg_t acfg;
            moq_accept_ns_sub_cfg_init(&acfg);
            if (moq_session_accept_ns_sub(cn->session, nq->handle, &acfg,
                                          now_us) != MOQ_OK) {
                b->session_errors++;
            }
        } else {
            moq_reject_ns_sub_cfg_t rcfg;
            moq_reject_ns_sub_cfg_init(&rcfg);
            rcfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_ns_sub(cn->session, nq->handle, &rcfg,
                                            now_us);
        }
        break;
    }
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        const moq_track_status_request_event_t *tq =
            &ev->u.track_status_request;
        moqr_ns_t ns = ns_from_event(&tq->track_namespace, nsbuf);
        uint64_t terr = 0;
        uint64_t ttkt = 0;
        bind_auth_result_t tar =
            bind_authorize(b, cn, MOQR_AUTH_TRACK_STATUS, ns, tq->track_name,
                           tq->tokens, tq->token_count, now_us, &terr, &ttkt,
                           NULL);
        if (tar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_TRACK_STATUS, ns,
                           tq->track_name, tq->handle._opaque);
            bind_refuse_or_park(b, cn, tar, &pr, tq->tokens, tq->token_count,
                                ttkt, terr, now_us);
            break;
        }
        moqr_result_t trc;
        BIND_CALL_RETRY(trc, moqr_core_track_status(
            b->core, cn->binding, ns, tq->track_name, tq->handle._opaque));
        if (trc != MOQR_OK) {
            /* A ring-full status request would otherwise leave the peer
             * waiting forever; the invariant + drain makes trc==OK, but
             * if it ever weren't, reject on the wire rather than drop. */
            moq_reject_track_status_cfg_t rj;
            moq_reject_track_status_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_track_status(cn->session, tq->handle,
                                                  &rj, now_us);
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_REQUEST: {
        const moq_publish_request_event_t *pq = &ev->u.publish_request;
        moqr_ns_t ns = ns_from_event(&pq->track_namespace, nsbuf);
        uint64_t perr = 0;
        uint64_t ptkt = 0;
        bind_auth_result_t par =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->tokens, pq->token_count, now_us, &perr, &ptkt,
                           NULL);
        if (par != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->pub._opaque);
            bind_refuse_or_park(b, cn, par, &pr, pq->tokens, pq->token_count,
                                ptkt, perr, now_us);
            break;
        }
        moqr_track_t track;
        moqr_result_t prc;
        BIND_CALL_RETRY(prc, moqr_core_publish_open(
            b->core, cn->binding, ns, pq->track_name, pq->pub._opaque,
            &track));
        if (prc != MOQR_OK) {
            moq_reject_publish_cfg_t rj;
            moq_reject_publish_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_publish(cn->session, pq->pub, &rj,
                                             now_us);
        }
        break;
    }
    case MOQ_EVENT_FETCH_REQUEST: {
        const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
        moqr_ns_t fns = ns_from_event(&fq->track_namespace, nsbuf);
        /* Authorize FIRST so unsupported fetch shapes are never a pre-auth
         * support oracle. A DEFER also fails closed (no parked fetch yet). */
        uint64_t ferr = 0;
        uint64_t ftkt = 0;
        bind_auth_result_t far =
            bind_authorize(b, cn, MOQR_AUTH_FETCH, fns, fq->track_name,
                           fq->tokens, fq->token_count, now_us, &ferr, &ftkt,
                           NULL);
        moq_reject_fetch_cfg_t rj;
        if (far != BIND_AUTH_ALLOW) {
            moq_reject_fetch_cfg_init(&rj);
            rj.error_code = ferr != 0 ? ferr : MOQ_REQUEST_ERROR_UNAUTHORIZED;
            (void)moq_session_reject_fetch(cn->session, fq->fetch, &rj, now_us);
            break;
        }
        /* Then reject unsupported shapes (joining + descending). */
        if (fq->joining_sub._opaque != 0 ||
            fq->group_order == MOQ_GROUP_ORDER_DESCENDING) {
            moq_reject_fetch_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
            (void)moq_session_reject_fetch(cn->session, fq->fetch, &rj, now_us);
            break;
        }
        /* Plan against the core retained cursor. */
        moqr_fetch_req_t freq;
        moqr_fetch_req_init(&freq);
        freq.ns = fns;
        freq.name = fq->track_name;
        freq.start_group = fq->start_group;
        freq.start_object = fq->start_object;
        freq.end_group = fq->end_group;
        freq.end_object = fq->end_object;
        freq.group_order = (moqr_group_order_t)fq->group_order;
        freq.subscriber_priority = fq->subscriber_priority;
        freq.cookie = fq->fetch._opaque;
        moqr_fetch_t cfetch;
        moqr_fetch_plan_t plan;
        if (moqr_core_fetch_open(b->core, cn->binding, &freq, now_us, &cfetch,
                                 &plan) != MOQR_OK) {
            moq_reject_fetch_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;   /* pool full */
            (void)moq_session_reject_fetch(cn->session, fq->fetch, &rj, now_us);
            break;
        }
        if (plan.admit == MOQR_FETCH_REJECT) {
            moq_reject_fetch_cfg_init(&rj);
            rj.error_code = plan.error_code;   /* DOES_NOT_EXIST / INVALID_RANGE */
            (void)moq_session_reject_fetch(cn->session, fq->fetch, &rj, now_us);
            break;
        }
        /* ACCEPT: park it in the conn fetch table; bind_fetch_pump drives
         * accept_fetch -> stream objects -> end_fetch on the network thread. */
        int fi = -1;
        for (uint32_t i = 0; i < b->n_fetches; i++) {
            if (!cn->fetches[i].used) {
                fi = (int)i;
                break;
            }
        }
        if (fi < 0) {   /* impossible: the bind table matches the core pool */
            (void)moqr_core_fetch_close(b->core, cfetch);
            moq_reject_fetch_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            (void)moq_session_reject_fetch(cn->session, fq->fetch, &rj, now_us);
            break;
        }
        b_fetch_t *bf = &cn->fetches[fi];
        bf->used = true;
        bf->state = BF_NEEDS_OK;
        bf->sfetch = fq->fetch;
        bf->cfetch = cfetch;
        bf->eot = plan.end_of_track;
        bf->end_group = plan.end_group;
        bf->end_object = plan.end_object;
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        const moq_subscribe_tracks_request_event_t *sq =
            &ev->u.subscribe_tracks_request;
        moq_reject_subscribe_tracks_cfg_t cfg;
        moq_reject_subscribe_tracks_cfg_init(&cfg);
        cfg.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        (void)moq_session_reject_subscribe_tracks(cn->session, sq->handle,
                                                  &cfg, now_us);
        break;
    }
    case MOQ_EVENT_SETUP_COMPLETE: {
        const moq_setup_complete_event_t *su = &ev->u.setup_complete;
        if (bind_authorize(b, cn, MOQR_AUTH_CLIENT_SETUP,
                           (moqr_ns_t){ NULL, 0 }, (moq_bytes_t){ NULL, 0 },
                           su->tokens, su->token_count, now_us, NULL,
                           NULL, NULL) != BIND_AUTH_ALLOW) {
            /* Setup DENY (and DEFER, which fails closed) hard-closes the
             * connection with the session-level UNAUTHORIZED code.
             * moq_session_close queues CLOSE_SESSION — the adapter drains it
             * to the peer — plus SESSION_CLOSED; the session stays
             * adapter-owned, so detaching the binding here is safe and the
             * close still reaches the wire. The reason is a static literal so
             * it outlives the queued action/event. */
            (void)moq_session_close(cn->session, BIND_SESSION_UNAUTHORIZED,
                                    "unauthorized", now_us);
            conn_detach(b, cn, now_us);
        }
        break;
    }
    case MOQ_EVENT_SESSION_CLOSED:
        conn_detach(b, cn, now_us);
        break;
    default:
        break;
    }
}

/* -- intent execution --------------------------------------------------------- */

static int
dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (!cn->dsubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static int
usub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static int
pub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_pubs; i++) {
        if (!cn->pubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static bool
bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked_out, moqr_intent_t *defer)
{
    /* Reserved-cookie intents belong to the shard manager's pseudo-bindings,
     * not a real session: hand them to the router. Without a router installed
     * (the single-shard default) this is skipped and the reserved cookie is
     * moot exactly as an unknown cookie is below. */
    if (b->router != NULL && it->binding_cookie >= b->router_cookie_base) {
        if (b->router(b->router_ctx, it, now_us)) {
            return true; /* the router consumed it */
        }
        if (bind_intent_deferrable(it->kind)) {
            *defer = *it; /* retry next pump, like a WOULD_BLOCKed write */
            return false;
        }
        return true; /* declined borrowed-view kind: moot, as an unrouted cookie */
    }
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return true; /* moot: the target session is gone or tearing down */
    }
    switch (it->kind) {
    case MOQR_INTENT_ACCEPT_SUB: {
        moq_subscription_t ssub = { it->cookie };
        /* Reserve the tracking slot BEFORE accepting on the wire: a sub
         * the binding cannot track must be refused, never orphaned. */
        int slot = dsub_free_slot(b, cn);
        if (slot < 0) {
            /* Untrackable: drop the core sub and refuse on the wire. The
             * refusal is itself durable — a blocked reject parks as a
             * REJECT_SUB (the core sub is already gone; the retry only
             * re-sends the refusal). */
            (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            moq_reject_subscribe_cfg_t rj;
            bind_reject_subscribe_cfg_init(&rj,
                                           MOQ_REQUEST_ERROR_INTERNAL_ERROR);
            if (moq_session_reject_subscribe(cn->session, ssub, &rj, now_us) ==
                MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                defer->kind = MOQR_INTENT_REJECT_SUB;
                defer->error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                return false;
            }
            return true;
        }
        moq_accept_subscribe_cfg_t cfg;
        moq_accept_subscribe_cfg_init(&cfg);
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        moq_result_t arc =
            moq_session_accept_subscribe(cn->session, ssub, &cfg, now_us);
        if (arc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it; /* slot NOT committed; the retry re-reserves */
            return false;
        }
        if (arc != MOQ_OK) {
            (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            b->session_errors++;
            return true;
        }
        cn->dsubs[slot].used = true;
        cn->dsubs[slot].sub_raw = it->cookie;
        cn->dsubs[slot].rsub = it->sub;
        return true;
    }
    case MOQR_INTENT_REJECT_SUB: {
        moq_subscription_t ssub = { it->cookie };
        moq_reject_subscribe_cfg_t cfg;
        bind_reject_subscribe_cfg_init(&cfg, it->error_code);
        if (moq_session_reject_subscribe(cn->session, ssub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_SUB_DONE: {
        /* Close any open subgroup streams for this sub, THEN send the
         * terminal. Defer the whole SUB_DONE if any write blocks: tracking is
         * left intact so the retry re-closes (already-closed slots are
         * cleared, so it is idempotent) and re-sends DONE. Downstream tracking
         * is torn down only once the terminal is actually on the wire. */
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (!cn->sgs[i].used || cn->sgs[i].sub_raw != it->cookie) {
                continue;
            }
            moq_result_t crc;
            if (cn->sgs[i].obj_begun) {
                /* A begun object cannot be FIN-closed: close_subgroup returns
                 * WRONG_STATE on a streaming subgroup, stranding the object with
                 * no terminal and blocking the subscription's own terminal.
                 * Reset it (the streaming-safe terminal) with the reset
                 * registry's CANCELLED — the SUB_DONE's status code lives in
                 * the PUBLISH_DONE registry and never doubles as a reset
                 * code. */
#ifdef MOQR_BIND_TESTING
                if (atomic_load(&bind_dbg_fail_sg_resets) > 0 &&
                    atomic_fetch_sub(&bind_dbg_fail_sg_resets, 1) == 1) {
                    /* Injected hard failure: the session is NOT called, so
                     * the production fail-closed path runs unchanged. */
                    crc = MOQ_ERR_WRONG_STATE;
                } else
#endif
                crc = moq_session_reset_subgroup(cn->session, cn->sgs[i].h,
                                                 BIND_RESET_CANCELLED,
                                                 now_us);
            } else {
                crc = moq_session_close_subgroup(cn->session, cn->sgs[i].h,
                                                 now_us);
            }
            if (crc == MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                return false;
            }
            if (cn->sgs[i].obj_begun && crc != MOQ_OK) {
                /* A hard reset failure leaves the begun object unterminated:
                 * fail closed rather than strand it. */
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "sub_done reset", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
            cn->sgs[i].used = false;
            cn->sgs[i].obj_begun = false;
            bind_dl_rearm_on_slot_release(b, cn);   /* freed slot: unblock a waiting delivery */
        }
        moq_subscription_t ssub = { it->cookie };
        moq_done_subscribe_cfg_t cfg;
        moq_done_subscribe_cfg_init(&cfg);
        /* The retained terminal is encoded for the draft this subscriber
         * reads — the single translation point for the PUBLISH_DONE domain. */
        uint64_t pd_wire = 0;
        if (moqr_pd_desc_emit(it->pd, moqr_bind_conn_version(b, cn->session),
                              &pd_wire) != MOQR_OK) {
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "untranslatable terminal", now_us);
            conn_detach(b, cn, now_us);
            return false;
        }
        cfg.status_code = pd_wire;   /* 64-bit: an extension may exceed 32 bits */
        moq_result_t drc;
#ifdef MOQR_BIND_TESTING
        if (atomic_load(&bind_dbg_block_sub_dones) > 0) {
            /* Injected block: the session is NOT called, so the terminal is
             * genuinely unsent and the retry must produce it. */
            (void)atomic_fetch_sub(&bind_dbg_block_sub_dones, 1);
            drc = MOQ_ERR_WOULD_BLOCK;
        } else
#endif
        drc = moq_session_done_subscribe(cn->session, ssub, &cfg, now_us);
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == it->cookie) {
                cn->dsubs[i].used = false;
            }
        }
        return true;
    }
    case MOQR_INTENT_UPSTREAM_SUBSCRIBE: {
        /* Borrowed-payload (ns_parts/name); NOT deferrable — a blocked
         * subscribe fails closed to upstream_error, whose downstream REJECT_SUB
         * IS durable, so the requester still gets a terminal. Making the
         * upstream subscribe itself retryable is a later refinement. */
        int slot = usub_free_slot(b, cn);
        if (slot < 0) {
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            return true;
        }
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = it->ns_parts,
                                                 .count = it->ns_count };
        cfg.track_name = it->name;
        cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t ssub;
        if (moq_session_subscribe(cn->session, &cfg, now_us, &ssub) ==
            MOQ_OK) {
            cn->usubs[slot].used = true;
            cn->usubs[slot].track = it->track;
            cn->usubs[slot].track_gen = it->track_gen;
            cn->usubs[slot].ssub = ssub;
        } else {
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        }
        return true;
    }
    case MOQR_INTENT_UPSTREAM_UNSUBSCRIBE:
        /* NOT deferrable: moq_session_unsubscribe WOULD_BLOCKs on UNSUBSCRIBE
         * tombstone-capacity exhaustion (session_subscribe.c), which persists
         * until late responses reclaim tombstones — not a transient full
         * queue. Parking it would deadlock the retry head. It is upstream
         * cleanup, not a downstream terminal: a dropped unsubscribe leaks an
         * unwanted upstream subscription (bounded, per-track), never hangs a
         * peer. Best-effort, as before. */
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used &&
                moqr_track_eq(cn->usubs[i].track, it->track)) {
                (void)moq_session_unsubscribe(cn->session, cn->usubs[i].ssub,
                                              now_us);
                cn->usubs[i].used = false;
            }
        }
        return true;
    case MOQR_INTENT_ACCEPT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        /* Reserve tracking BEFORE accepting: an untracked publish would
         * silently drop every object it pushes. On refusal, unwind the
         * core's track (upstream lost) and reject on the wire (durable, as
         * with ACCEPT_SUB's no-slot path). */
        int slot = pub_free_slot(b, cn);
        if (slot < 0) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            moq_reject_publish_cfg_t rj;
            moq_reject_publish_cfg_init(&rj);
            rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            if (moq_session_reject_publish(cn->session, pub, &rj, now_us) ==
                MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                defer->kind = MOQR_INTENT_REJECT_PUBLISH;
                defer->error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                return false;
            }
            return true;
        }
        moq_accept_publish_cfg_t cfg;
        moq_accept_publish_cfg_init(&cfg);
        moq_result_t prc =
            moq_session_accept_publish(cn->session, pub, &cfg, now_us);
        if (prc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (prc != MOQ_OK) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            b->session_errors++;
            return true;
        }
        cn->pubs[slot].used = true;
        cn->pubs[slot].pub_raw = it->cookie;
        cn->pubs[slot].track = it->track;
        cn->pubs[slot].track_gen = it->track_gen;
        return true;
    }
    case MOQR_INTENT_REJECT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        moq_reject_publish_cfg_t cfg;
        moq_reject_publish_cfg_init(&cfg);
        cfg.error_code = it->error_code;
        if (moq_session_reject_publish(cn->session, pub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_NS_FOUND:
    case MOQR_INTENT_NS_GONE: {
        /* Borrowed-payload (ns_parts), so the intent itself can never be
         * parked. draft-18 Section 6.2 owes this NAMESPACE to the matching
         * prefix subscriber as a MUST, so a blocked write is retained by
         * deep copy (bind_nsu_retain) and replayed, never dropped. */
        moq_ns_sub_handle_t h = { it->cookie };
        bool is_gone = it->kind == MOQR_INTENT_NS_GONE;
        moqr_ns_t suffix_ns = {
            .parts = it->ns_parts + it->value,
            .count = it->ns_count - (size_t)it->value,
        };
        /* The ROOT namespace -- zero ANNOUNCED fields -- has no representation
         * on a peer whose negotiated draft requires at least one, so sending it
         * would put an invalid namespace on that wire. The test is the announced
         * cardinality, not an empty suffix: a non-root namespace equal to its
         * prefix also yields a zero-length suffix and is unaffected. Ask the
         * CAPABILITY, never a draft number, and drop the update before the send
         * and before any retain/owed accounting, so it is never queued,
         * retained or counted as owed. FOUND and GONE are suppressed
         * symmetrically, so the peer is never left holding half a pair; its own
         * namespace subscription, including a legal empty prefix, is
         * untouched. */
        if (it->ns_count == 0 &&
            !moq_session_supports_zero_field_track_namespace(cn->session)) {
            return true;   /* consumed: nothing sent, nothing owed */
        }
        if (mode != BIND_TRY_FORCE_PARK) {
            moq_namespace_t suffix = { .parts = suffix_ns.parts,
                                       .count = suffix_ns.count };
            moq_result_t rc =
                is_gone
                    ? moq_session_send_namespace_done(cn->session, h, &suffix,
                                                      now_us)
                    : moq_session_send_namespace(cn->session, h, &suffix,
                                                 now_us);
            if (rc == MOQ_OK) {
                return true;   /* healthy: nothing allocated, nothing parked */
            }
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                /* A hard send failure loses an update the spec says MUST be
                 * delivered. Counting it is telemetry, not enforcement: the
                 * subscriber that can no longer be told the truth is closed
                 * on the same lifecycle-safe path capacity exhaustion uses. */
                b->session_errors++;
                b->nsu_failed_closed++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "namespace update failed", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
        }
        if (mode == BIND_TRY_FROM_RING) {
            /* Already parked, and its payload already bind-owned: keep the
             * slot exactly where it is so nothing overtakes it. */
            *defer = *it;
            return false;
        }
        if (!bind_defer_push_nsu(b, it)) {
            /* Held state exhausted. Failing closed on the subscriber that can
             * no longer be told the truth is the only honest option: silently
             * continuing would drop a MUST. Other connections' parked output
             * is untouched. */
            b->nsu_failed_closed++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "namespace update undeliverable", now_us);
            conn_detach(b, cn, now_us);
            return true;
        }
        /* Parked in the one FIFO: the executor must now treat the bind as
         * blocked so nothing polled after this can be attempted ahead of it. */
        if (parked_out != NULL) {
            *parked_out = true;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_OK: {
        moq_track_status_handle_t h = { it->cookie };
        moq_accept_track_status_cfg_t cfg;
        moq_accept_track_status_cfg_init(&cfg);
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        moq_result_t trc =
            moq_session_accept_track_status(cn->session, h, &cfg, now_us);
        if (trc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (trc != MOQ_OK) {
            b->session_errors++;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_ERROR: {
        moq_track_status_handle_t h = { it->cookie };
        moq_reject_track_status_cfg_t cfg;
        moq_reject_track_status_cfg_init(&cfg);
        cfg.error_code = it->error_code;
        if (moq_session_reject_track_status(cn->session, h, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    default:
        return true;
    }
}

/* -- delivery -------------------------------------------------------------------- */

/* An ACCEPTED fetch hit an unrecoverable session error (eviction TOO_OLD, or a
 * write/accept/FIN failure). The session has no per-fetch abort, and the peer
 * has been (or is about to be) admitted, so its liveness demands a terminal:
 * hard-close the session (a
 * visible transport close reaches the peer) and detach the conn. binding_close
 * inside conn_detach retires the conn's core fetch cursors. A per-fetch reset /
 * range markers is a later refinement. Returns after detaching — the caller must
 * stop touching this conn. */
static void
bind_fetch_terminate(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    b->session_errors++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "fetch error", now_us);
    conn_detach(b, cn, now_us);
}

static void
bind_fetch_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
        b_fetch_t *bf = &cn->fetches[fi];
        if (!bf->used) {
            continue;
        }
        /* Drive this fetch as far as the session queue allows; every step holds
         * (breaks, retried next pump) on WOULD_BLOCK with no state advance. */
        for (uint32_t guard = 0; guard < 512; guard++) {
            if (bf->state == BF_NEEDS_OK) {
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_of_track = bf->eot;
                ac.end_group = bf->end_group;
                ac.end_object = bf->end_object;
                moq_result_t rc = moq_session_accept_fetch(cn->session,
                                                           bf->sfetch, &ac,
                                                           now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    break;
                }
                if (rc != MOQ_OK) {
                    bind_fetch_terminate(b, cn, now_us);   /* FETCH_OK failed */
                    return;
                }
                bf->state = BF_STREAMING;
                continue;
            }
            if (bf->state == BF_STREAMING) {
                moqr_fetch_item_t it;
                moqr_result_t prc = moqr_core_fetch_peek(b->core, bf->cfetch,
                                                         now_us, &it);
                if (prc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* pin budget momentarily full; retry next pump */
                }
                if (prc != MOQR_OK) {
                    /* TOO_OLD / CAPACITY: the accepted range was lost mid-fetch
                     * (only reachable under ACTIVE-track capacity eviction; the
                     * lifetime guard pins WARM tracks). The peer already saw
                     * FETCH_OK, so a clean END would FIN a truncated range and a
                     * silent drop would hang it — give it a terminal via a hard
                     * close. */
                    bind_fetch_terminate(b, cn, now_us);
                    return;
                }
                if (it.kind == MOQR_FETCH_ITEM_END) {
                    bf->state = BF_NEEDS_FIN;
                    continue;
                }
                if (it.kind == MOQR_FETCH_ITEM_MARKER) {
                    /* Evicted-prefix UNKNOWN marker before the first retained
                     * group, via the profile-owned seam. Same retry model as
                     * objects: HOLD on WOULD_BLOCK (re-peek re-emits the marker),
                     * terminal on any hard error, and commit (the cursor advance)
                     * ONLY after the session write succeeds. */
                    moq_result_t mrc =
                        moq_session_write_fetch_range_before_group(
                            cn->session, bf->sfetch, MOQ_FETCH_RANGE_UNKNOWN,
                            it.marker_group, now_us);
                    if (mrc == MOQ_ERR_WOULD_BLOCK) {
                        break;   /* HOLD: do not commit */
                    }
                    if (mrc != MOQ_OK) {
                        bind_fetch_terminate(b, cn, now_us);
                        return;
                    }
                    (void)moqr_core_fetch_commit(b->core, bf->cfetch);
                    continue;
                }
                moq_fetch_object_cfg_t oc;
                moq_fetch_object_cfg_init(&oc);
                oc.group_id = it.rec.group_id;
                oc.subgroup_id = it.rec.subgroup_id;
                oc.object_id = it.rec.object_id;
                oc.publisher_priority = it.rec.publisher_priority;
                /* Datagram preference is forwarding metadata, not object
                 * identity. Draft-18 fetch headers can encode it; draft-16
                 * cannot, so for a draft-16 fetcher clear the bit and serve the
                 * object normally (same group/object/payload) rather than fail
                 * the write. The capability query never leaks the version. */
                oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                                  ? it.rec.datagram_pref
                                  : false;
                oc.payload = (moq_rcbuf_t *)(void *)it.rec.payload;
                oc.properties = (moq_rcbuf_t *)(void *)it.rec.properties;
                moq_result_t wrc = moq_session_write_fetch_object(
                    cn->session, bf->sfetch, &oc, now_us);
                if (wrc == MOQ_ERR_WOULD_BLOCK) {
                    break;   /* HOLD: do not commit; re-peek re-emits it */
                }
                if (wrc != MOQ_OK) {
                    /* The object cannot be written (defensive: the cursor only
                     * yields encodable NORMAL objects, and the datagram bit is
                     * already cleared for a draft-16 fetcher), so the fetch
                     * cannot complete: terminal. */
                    bind_fetch_terminate(b, cn, now_us);
                    return;
                }
                (void)moqr_core_fetch_commit(b->core, bf->cfetch);
                b->deliveries_written++;
                continue;
            }
            /* BF_NEEDS_FIN */
            moq_result_t rc = moq_session_end_fetch(cn->session, bf->sfetch,
                                                    now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                break;
            }
            if (rc != MOQ_OK) {
                bind_fetch_terminate(b, cn, now_us);   /* FIN failed: terminal */
                return;
            }
            (void)moqr_core_fetch_close(b->core, bf->cfetch);
            bf->used = false;
            break;
        }
    }
}

/* Stream one COMPLETE chunked record downstream: begin_object once, then
 * write_object_data for each retained chunk in order, then end_object. Progress
 * (obj_begun / obj_id / next_chunk) lives on the sg slot so a WOULD_BLOCK at any
 * step HOLDS and the next pump resumes the SAME chunk — no duplicate, no skip
 * (each write_object_data is retry-safe: capacity is gated before any incref).
 * Returns MOQ_OK only when the whole object shipped (begin + all chunks + end);
 * MOQ_ERR_WOULD_BLOCK to hold; another error to fail the delivery. Chunks are
 * read from the core's pinned array — never from the log directly. */
/* Map a single session write result to a delivery outcome (whole-object,
 * status, and datagram paths, which each emit exactly one action). */
static moqr_delivery_outcome_t
oc_from_result(moq_result_t wrc)
{
    return wrc == MOQ_OK              ? MOQR_DELIVERY_DELIVERED
           : wrc == MOQ_ERR_WOULD_BLOCK ? MOQR_DELIVERY_WOULD_BLOCK
                                        : MOQR_DELIVERY_STREAM_ERROR;
}

static moqr_delivery_outcome_t
bind_write_chunked(moqr_bind_t *b, b_conn_t *cn, const moqr_delivery_t *d,
                   b_sg_t *sg, uint64_t now_us)
{
    if (!sg->obj_begun || sg->obj_id != d->rec.object_id) {
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = d->rec.object_id;
        bc.payload_length = d->rec.declared_len;   /* known at open (OPEN too) */
        bc.properties = (moq_rcbuf_t *)(void *)d->rec.properties;
        moq_result_t brc =
            moq_session_begin_object_ex(cn->session, sg->h, &bc, now_us);
        if (brc == MOQ_ERR_WOULD_BLOCK) {
            return MOQR_DELIVERY_WOULD_BLOCK;   /* subgroup stays OPEN, retry */
        }
        if (brc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->obj_begun = true;
        sg->obj_id = d->rec.object_id;
        sg->next_chunk = 0;
    }
    while (sg->next_chunk < d->rec.chunk_count) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_core_delivery_chunk(b->core, cn->binding, sg->next_chunk, &cb,
                                     &clen) != MOQR_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;   /* pinned chunk missing */
        }
        moq_result_t wrc = moq_session_write_object_data(
            cn->session, sg->h, (moq_rcbuf_t *)(void *)cb, now_us);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {
            /* Hold: checkpoint that the object is begun downstream and how many
             * chunks have shipped (sg->next_chunk, which is <= chunk_count and
             * may be 0 when begin_object succeeded but this first chunk blocked).
             * This keeps the core's begun watermark accurate so that, if a
             * Forward pause releases this partial and the group is then
             * abandoned/evicted, the required RESET still surfaces. SAME chunk
             * retries next pump. next_chunk is in range, so INVAL here is a
             * consistency break, not a normal hold. */
            if (moqr_core_delivery_note_emitted(b->core, cn->binding,
                                                sg->next_chunk, now_us) !=
                MOQR_OK) {
                return MOQR_DELIVERY_STREAM_ERROR;
            }
            return MOQR_DELIVERY_WOULD_BLOCK;
        }
        if (wrc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->next_chunk++;
    }
    if (d->rec.obj_state != MOQR_OBJ_COMPLETE) {
        /* OPEN live edge: every currently-exposed chunk shipped but the
         * object is not complete yet — HOLD here without ending it. The core
         * records the emitted watermark on STALLED and re-selects this head only
         * when it grows or flips terminal, so other subs are not HOL-blocked. */
        return MOQR_DELIVERY_STALLED;
    }
    moq_result_t erc = moq_session_end_object(cn->session, sg->h, now_us);
    if (erc != MOQ_OK) {
        return MOQR_DELIVERY_STREAM_ERROR;   /* end_object never WOULD_BLOCKs */
    }
    sg->obj_begun = false;   /* object complete; the slot can begin the next */
    return MOQR_DELIVERY_DELIVERED;
}

/* Confirm a delivery outcome to the core. delivery_done's error vocabulary
 * (INVAL / STALE_HANDLE / WRONG_STATE) is all consistency failures — never
 * transient — so any non-OK confirmation fails the session closed and
 * detaches; a binding whose confirmations misfire must not stay live with
 * an unacknowledged delivery. Returns false when the connection was
 * detached (the caller returns B_DELIVER_CLOSED). */
static bool
bind_confirm_delivery(moqr_bind_t *b, b_conn_t *cn,
                      moqr_delivery_outcome_t oc, uint64_t now_us)
{
    moqr_result_t crc;
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_confirms) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_confirms, 1) == 1) {
        /* Injected consistency failure: the core is NOT called, so the
         * outstanding delivery stays live — teardown must cope with it. */
        crc = MOQR_ERR_WRONG_STATE;
    } else
#endif
    {
        crc = moqr_core_delivery_done(b->core, cn->binding, oc, now_us);
    }
    if (crc == MOQR_OK) {
        return true;
    }
    b->session_errors++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "delivery confirm failed", now_us);
    conn_detach(b, cn, now_us);
    return false;
}

/* One delivery pass over a connection, ending in exactly one outcome that
 * tells Phase F how to reschedule it. STALLED and ABANDONED remain in-loop
 * continuations (sibling subscriptions keep being served in the same pass);
 * every delivery_done error is a consistency failure — deterministic contract
 * violations are never retried. */
typedef enum b_deliver_outcome {
    B_DELIVER_DRAINED,            /* nothing deliverable now                */
    B_DELIVER_BLOCKED_ACTION_CAP, /* action-queue WB: capacity-floor gate   */
    B_DELIVER_BLOCKED_SESSION_SG, /* subgroup-open WB with queue room       */
    B_DELIVER_BLOCKED_BIND_SG,    /* downstream WB: bind subgroup slots     */
    B_DELIVER_BUDGET,             /* 256-delivery guard expired, work left  */
    B_DELIVER_RETRY,              /* transient CAPACITY / stream error:
                                   * sibling work needs a self-rearm        */
    B_DELIVER_CLOSED,             /* conn detached or consistency failure   */
} b_deliver_outcome_t;

static b_deliver_outcome_t
bind_deliver(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t guard = 0; guard < 256; guard++) {
        moqr_delivery_t d;
        moqr_result_t drc;
#ifdef MOQR_BIND_TESTING
        atomic_fetch_add(&bind_dbg_probes, 1);
        if (atomic_load(&bind_dbg_fail_probes) > 0 &&
            atomic_fetch_sub(&bind_dbg_fail_probes, 1) == 1) {
            drc = bind_dbg_probe_result;   /* injected; d stays unused */
        } else
#endif
        {
            drc = moqr_core_next_delivery(b->core, cn->binding, now_us, &d);
        }
        if (drc != MOQR_OK) {
            if (drc == MOQR_DONE) {
                return B_DELIVER_DRAINED;   /* nothing NOW; a mark re-arms */
            }
            if (drc == MOQR_ERR_CAPACITY) {
                /* Transient scheduler capacity (pin array / table growth):
                 * nothing was consumed; self-rearm so sibling work is never
                 * stranded waiting for a mark that will not come. */
                return B_DELIVER_RETRY;
            }
            /* STALE_HANDLE (the core binding vanished under a live conn) or
             * any other error is a consistency failure: fail closed rather
             * than strand an unschedulable connection. */
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "delivery probe failed", now_us);
            conn_detach(b, cn, now_us);
            return B_DELIVER_CLOSED;
        }
        moq_subscription_t ssub = { d.sub_cookie };
        /* Recordless acknowledged notices precede record work. The close IS
         * the payload here, so the notice is acknowledged (DELIVERED) only
         * once every close it names is on the wire; a WOULD_BLOCK holds the
         * notice and the next pump retries exactly where it left off (closed
         * slots are already cleared). A hard close error fails closed — the
         * peer must not be left with a stream that will never FIN. */
        if (d.notice != MOQR_DELIVERY_NOTICE_NONE) {
            bool blocked = false, faulted = false;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie) {
                    continue;
                }
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL
                        ? (cn->sgs[i].group != d.rec.group_id ||
                           cn->sgs[i].subgroup != d.rec.subgroup_id)
                        /* EVICT_WATERMARK: any open subgroup of a group BELOW
                         * the track's oldest RETAINED group is gone (its
                         * records are evicted; the group id is never
                         * re-admitted) and can never receive more objects.
                         * The watermark is the oldest retained group, NOT a
                         * delivered record's group: the schedule is not
                         * strictly ascending, so a lower group can still be
                         * live only at/above it. */
                        : cn->sgs[i].group >= d.oldest_group) {
                    continue;
                }
                /* A reset-flavored seal closes abnormally, forwarding the
                 * upstream RESET code — the subgroup's content is truncated
                 * at the source, and a clean FIN would misrepresent it as
                 * complete. Same retry discipline either way. */
                uint64_t seal_wire = 0;
                bool seal_is_reset = d.notice == MOQR_DELIVERY_NOTICE_SEAL &&
                                     d.seal_reset;
                if (seal_is_reset &&
                    !bind_reset_wire(b, cn, d.seal_reset_desc, &seal_wire)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t crc =
                    seal_is_reset
                        ? moq_session_reset_subgroup(cn->session,
                                                     cn->sgs[i].h, seal_wire,
                                                     now_us)
                        : moq_session_close_subgroup(cn->session,
                                                     cn->sgs[i].h, now_us);
                if (crc == MOQ_ERR_WOULD_BLOCK) {
                    blocked = true;
                    break;
                }
                if (crc != MOQ_OK) {
                    faulted = true;
                    break;
                }
                cn->sgs[i].used = false;
                cn->sgs[i].obj_begun = false;
                bind_dl_rearm_on_slot_release(b, cn);
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
                    break;   /* a seal names exactly one subgroup */
                }
            }
            if (faulted) {
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "notice close failed", now_us);
                conn_detach(b, cn, now_us);
                return B_DELIVER_CLOSED;
            }
            if (!bind_confirm_delivery(b, cn,
                                       blocked ? MOQR_DELIVERY_WOULD_BLOCK
                                               : MOQR_DELIVERY_DELIVERED,
                                       now_us)) {
                return B_DELIVER_CLOSED;
            }
            if (blocked) {
                /* held: the close is an ordinary session write, so this is
                 * action-queue backpressure — resume above the floor */
                return B_DELIVER_BLOCKED_ACTION_CAP;
            }
            continue;
        }
        moqr_delivery_outcome_t oc;
        if (d.rec.datagram_pref) {
            moq_result_t wrc;
            if (d.rec.payload == NULL) {
                wrc = moq_session_send_status_datagram(
                    cn->session, ssub, d.rec.group_id, d.rec.object_id,
                    d.rec.publisher_priority,
                    (moq_object_status_t)d.rec.status, now_us);
            } else {
                const uint8_t *pb = NULL;
                size_t pl = 0;
                if (d.rec.properties != NULL) {
                    pb = moq_rcbuf_data(d.rec.properties);
                    pl = moq_rcbuf_len(d.rec.properties);
                }
                wrc = moq_session_send_object_datagram(
                    cn->session, ssub, d.rec.group_id, d.rec.object_id,
                    d.rec.publisher_priority, d.rec.end_of_group,
                    (moq_rcbuf_t *)(void *)d.rec.payload, pb, pl, now_us);
            }
            oc = oc_from_result(wrc);
        } else {
            int sgi = -1, freei = -1;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (cn->sgs[i].used && cn->sgs[i].sub_raw == d.sub_cookie &&
                    cn->sgs[i].group == d.rec.group_id &&
                    cn->sgs[i].subgroup == d.rec.subgroup_id) {
                    sgi = (int)i;
                    break;
                }
                if (!cn->sgs[i].used && freei < 0) {
                    freei = (int)i;
                }
            }
            if (d.rec.obj_state == MOQR_OBJ_ABANDONED && d.evicted_reset) {
                /* The whole group was evicted while begun downstream: reset EVERY
                 * begun subgroup of the group so no peer stream is left with a
                 * begun object and no terminal. Retry-safe — a reset subgroup is
                 * cleared (used=false), so a WOULD_BLOCK mid-way holds the
                 * delivery and the next pump resumes with the remaining ones. A
                 * hard reset error fails closed (terminate the connection). */
                bool blocked = false, faulted = false;
                for (uint32_t i = 0; i < b->n_sgs; i++) {
                    if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie ||
                        cn->sgs[i].group != d.rec.group_id ||
                        !cn->sgs[i].obj_begun) {
                        continue;
                    }
                    uint64_t rec_wire = 0;
                    if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire)) {
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "untranslatable reset", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    moq_result_t rrc = moq_session_reset_subgroup(
                        cn->session, cn->sgs[i].h, rec_wire, now_us);
                    if (rrc == MOQ_ERR_WOULD_BLOCK) {
                        blocked = true;
                        break;
                    }
                    if (rrc != MOQ_OK) {
                        faulted = true;
                        break;
                    }
                    cn->sgs[i].used = false;
                    cn->sgs[i].obj_begun = false;
                    bind_dl_rearm_on_slot_release(b, cn);
                }
                if (faulted) {
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "evict reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                if (blocked) {
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                oc = MOQR_DELIVERY_ABANDONED;
            } else if (d.rec.obj_state == MOQR_OBJ_ABANDONED) {
                /* Abandoned head, selected only because this sub already began
                 * the object downstream: RESET the existing subgroup so
                 * the peer sees an abnormal end, not a stuck object — never open
                 * a fresh one. The reset forwards the record's stored terminal
                 * code (d.rec.reset_code): the publisher's RESET code, or a
                 * relay-defined code for STOP / eviction. The core reports
                 * ABANDONED only after the reset is on the wire, so a missing
                 * begun subgroup or a failed reset FAILS CLOSED (terminate the
                 * connection) rather than silently advancing. */
                if (sgi < 0 || !cn->sgs[sgi].obj_begun ||
                    cn->sgs[sgi].obj_id != d.rec.object_id) {
                    /* Desync: core scheduled chunks (emitted>0) but the bind has
                     * no matching begun subgroup. Do not advance without a wire
                     * terminal. */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon desync", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                uint64_t rec_wire2 = 0;
                if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire2)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t rrc = moq_session_reset_subgroup(
                    cn->session, cn->sgs[sgi].h, rec_wire2, now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) {
                    /* Retry the reset next pump; the core holds the delivery. */
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                if (rrc != MOQ_OK) {
                    /* Reset could not be put on the wire: fail closed — the peer
                     * must see a terminal for the begun object, so terminate the
                     * connection (its close IS the terminal). */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                cn->sgs[sgi].used = false;   /* reset shipped: stream finished */
                cn->sgs[sgi].obj_begun = false;
                bind_dl_rearm_on_slot_release(b, cn);
                oc = MOQR_DELIVERY_ABANDONED;
            } else {
                if (sgi < 0) {
                    moq_subgroup_cfg_t sc;
                    moq_subgroup_cfg_init(&sc);
                    sc.group_id = d.rec.group_id;
                    sc.subgroup_id = d.rec.subgroup_id;
                    sc.publisher_priority = d.rec.publisher_priority;
                    sc.object_properties = d.rec.properties != NULL;
                    /* Header-EOG evidence rides the subgroup header on every
                     * record of the upstream subgroup, so the first record
                     * (which opens the downstream stream) preserves the bit. */
                    sc.end_of_group = d.rec.end_of_group;
                    moq_subgroup_handle_t h;
                    if (freei < 0) {
                        /* Bind subgroup slots exhausted — never reaches the
                         * session, and provably BEFORE any downstream write
                         * (a mid-object batch would have found its subgroup
                         * open, sgi >= 0). Confirmed as REFUSED_UNBEGUN so
                         * the core releases a chunked outstanding delivery
                         * instead of holding it — a held pre-begin record
                         * would starve the SEAL notices whose closes free
                         * the slot it needs. No parked state: every
                         * slot-release site on this still-open connection
                         * re-arms readiness. */
#ifdef MOQR_BIND_TESTING
                        /* Attribute the refusal to the OFFENDING track
                         * (first latches; a different track — a different
                         * demand — or an unresolvable handle marks the
                         * conn ambiguous: the join must fail closed, never
                         * pick between candidates). */
                        {
                            moqr_track_t dtr;
                            uint64_t dtg;
                            if (moqr_core_sub_track(b->core, d.sub, &dtr,
                                                    &dtg) != MOQR_OK) {
                                cn->dbg_bind_sg_ambiguous = true;
                            } else if (!cn->dbg_bind_sg_track_set) {
                                cn->dbg_bind_sg_track = dtr._opaque;
                                cn->dbg_bind_sg_track_gen = dtg;
                                cn->dbg_bind_sg_track_set = true;
                            } else if (cn->dbg_bind_sg_track !=
                                           dtr._opaque ||
                                       cn->dbg_bind_sg_track_gen != dtg) {
                                cn->dbg_bind_sg_ambiguous = true;
                            }
                        }
#endif
                        if (!bind_confirm_delivery(
                                b, cn, MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_BLOCKED_BIND_SG;
                    }
                    /* Snapshot BEFORE the call: open_subgroup returns one
                     * WOULD_BLOCK for two distinct causes. A full action
                     * queue (cap == 0) recovers on the capacity gate; a
                     * queue with room means the SUBGROUP POOL refused —
                     * its frees are observable only through the reap each
                     * open's own prologue performs, so that set gets one
                     * bounded re-attempt per pump. */
                    size_t precap =
                        moq_session_action_capacity(cn->session);
                    moq_result_t orc = moq_session_open_subgroup(
                        cn->session, ssub, &sc, now_us, &h);
                    if (orc == MOQ_ERR_WOULD_BLOCK) {
                        /* The open itself failed, so the downstream object
                         * provably never began. The SESSION-pool refusal
                         * (queue had room) confirms REFUSED_UNBEGUN so a
                         * chunked outstanding delivery RELEASES instead of
                         * starving the SEAL notices whose closes free the
                         * pool; the zero-capacity ACTION_CAP branch keeps
                         * plain WOULD_BLOCK (its capacity-gated park and
                         * recovery are validated as-is, and a whole-object
                         * release there is already the behavior). */
                        if (!bind_confirm_delivery(
                                b, cn,
                                precap == 0
                                    ? MOQR_DELIVERY_WOULD_BLOCK
                                    : MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return precap == 0 ? B_DELIVER_BLOCKED_ACTION_CAP
                                           : B_DELIVER_BLOCKED_SESSION_SG;
                    }
                    if (orc != MOQ_OK) {
                        /* A deterministic contract violation can never
                         * succeed on retry: fail closed. */
                        b->session_errors++;
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "subgroup open failed",
                                                now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    sgi = freei;
                    cn->sgs[sgi].used = true;
                    cn->sgs[sgi].sub_raw = d.sub_cookie;
                    cn->sgs[sgi].group = d.rec.group_id;
                    cn->sgs[sgi].subgroup = d.rec.subgroup_id;
                    cn->sgs[sgi].h = h;
                    cn->sgs[sgi].obj_begun = false;   /* fresh write progress  */
                    cn->sgs[sgi].obj_id = 0;
                    cn->sgs[sgi].next_chunk = 0;
                }
                if (d.rec.chunk_count > 0) {
                    /* Chunked record: stream the batch. Returns DELIVERED after
                     * end_object (COMPLETE), STALLED at the live edge (OPEN),
                     * WOULD_BLOCK to hold, or STREAM_ERROR. */
                    oc = bind_write_chunked(b, cn, &d, &cn->sgs[sgi], now_us);
                } else if (d.rec.payload == NULL) {
                    oc = oc_from_result(moq_session_write_status_object(
                        cn->session, cn->sgs[sgi].h, d.rec.object_id,
                        (moq_object_status_t)d.rec.status, now_us));
                } else {
                    moq_object_cfg_t ocfg;
                    moq_object_cfg_init(&ocfg);
                    ocfg.object_id = d.rec.object_id;
                    ocfg.payload = (moq_rcbuf_t *)(void *)d.rec.payload;
                    ocfg.properties = (moq_rcbuf_t *)(void *)d.rec.properties;
                    oc = oc_from_result(moq_session_write_object_ex(
                        cn->session, cn->sgs[sgi].h, &ocfg, now_us));
                }
                /* NO inline graceful close here — every graceful subgroup
                 * closure rides the acknowledged SEAL notice, whose close
                 * retries under WOULD_BLOCK. An inline close swallowed on
                 * WOULD_BLOCK after a delivered record would clear the slot
                 * and lose the FIN forever (the later notice would find
                 * nothing to close). The seal itself comes only from the
                 * upstream's actual graceful terminal (SUBGROUP_FINISHED,
                 * emitted after the FIN and every buffered object) — the
                 * header EOG bit is subgroup metadata preserved on the
                 * downstream open, never a per-record terminal. */
            }
        }
        if (!bind_confirm_delivery(b, cn, oc, now_us)) {
            return B_DELIVER_CLOSED;
        }
        if (oc == MOQR_DELIVERY_WOULD_BLOCK) {
            /* Write-path backpressure: ordinary writes document only the
             * action queue, and a property-bearing write needs TWO slots —
             * so the observed remaining capacity is the honest floor (the
             * write retries only once the queue drains PAST it). The held
             * (chunked/abandoned) delivery resumes on the recovery probe. */
            return B_DELIVER_BLOCKED_ACTION_CAP;
        }
        if (oc == MOQR_DELIVERY_STREAM_ERROR) {
            /* Terminal write error retired ONE subscription; sibling subs on
             * this binding may hold deliverable work no future mark
             * re-signals — self-rearm rather than strand them. */
            return B_DELIVER_RETRY;
        }
        if (oc != MOQR_DELIVERY_DELIVERED) {
            /* STALLED (live edge) or ABANDONED (reset done): nothing to observe,
             * and the head is no longer selectable — keep serving OTHER subs on
             * this binding so one publisher-stalled object never HOL-blocks them. */
            continue;
        }
        /* Every successful downstream write records exactly one latency
         * observation, so the histogram count tracks deliveries_written. A
         * regressed clock (now_us < arrival) saturates to 0us rather than
         * skipping — the observation is never dropped. */
        uint64_t latency_us =
            now_us >= d.rec.arrival_us ? now_us - d.rec.arrival_us : 0;
        moqr_latency_observe(&b->forward_latency, latency_us);
        b->deliveries_written++;
    }
    return B_DELIVER_BUDGET;   /* guard expired with work remaining */
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
    /* Per-pump flag, not sticky: a pass below reports it when the
     * 256-delivery guard expired with work remaining — the ONE outcome
     * whose leftover work no future event re-signals AND whose retry needs
     * nothing but another pump. The runtime turns it into one further
     * coalesced self-wake (see moqr_bind_pump_budget_pending). */
    b->budget_pending = false;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        size_t n;
        while (cn->used && !cn->closed &&
               (n = moq_session_poll_events(cn->session, b->pump_events,
                                            BIND_PUMP_EVENT_BATCH)) > 0) {
            for (size_t e = 0; e < n; e++) {
                moq_event_t *ev = &b->pump_events[e];
                if (bind_event_is_request(ev->kind)) {
                    cn->requests_seen++;
                }
                /* A fail-close mid-batch must stop translation immediately —
                 * a DEFERRED detach (binding close held on intent space)
                 * leaves used=true with closed=true, and no further event of
                 * this conn, including the remainder of THIS batch, may be
                 * ingested. */
                if (cn->used && !cn->closed) {
                    bind_on_event(b, cn, ev, now_us);
                }
                moq_event_cleanup(ev);
            }
        }
        /* Re-check the grant every pump (not only when events arrive): a peer
         * blocked at its ceiling sends nothing until the grant reaches it. */
        if (cn->used && !cn->closed && cn->session != NULL) {
            bind_maybe_grant_capacity(b, cn, now_us);
        }
    }
    (void)moqr_core_tick(b->core, now_us);
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
        moq_cancel_namespace_cfg_t ccfg;
        moq_cancel_namespace_cfg_init(&ccfg);
        ccfg.error_code = rev->error_code;
        moq_result_t crc = moq_session_cancel_namespace(
            rcn->session,
            (moq_announcement_t){ ._opaque = rev->session_cookie }, &ccfg,
            now_us);
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
            if (cn->used && !cn->closed) {
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
                        cn->bind_sg_waiting = false;
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
            if (cn->used && !cn->closed) {
                bind_fetch_pump(b, cn, now_us);
            }
        }
    }
    b->in_pump = false;
    return MOQR_OK;
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
    moqr_result_t rc = moqr_core_auth_begin_resolve(b->core, ticket, result,
                                                    now_us, &verdict, &view);
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
        if (verdict.decision == MOQR_AUTH_ALLOW) {
            bind_resume(b, cn, &view, verdict.revalidate_after_us, now_us);
        } else {
            uint64_t err = verdict.error_code != 0
                               ? verdict.error_code
                               : MOQ_REQUEST_ERROR_UNAUTHORIZED;
            bind_reject(b, cn, view.action, view.session_cookie, err, now_us);
        }
    }
    moqr_core_auth_finish_resolve(b->core, ticket);
    return MOQR_OK;
}

void
moqr_bind_get_stats(const moqr_bind_t *b, moqr_bind_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (b == NULL) {
        return;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used) {
            out->conns++;
        }
    }
    out->events_translated = b->events_translated;
    out->deliveries_written = b->deliveries_written;
    out->ingest_refusals = b->ingest_refusals;
    out->session_errors = b->session_errors;
    out->forward_latency = b->forward_latency;
    out->pending_high_water = b->pending_high_water;
    out->pending_nonscalar_blocked = b->pending_nonscalar_blocked;
    out->nsu_high_water = b->nsu_high_water;
    out->nsu_live = b->nsu_live;
    out->nsu_retained = b->nsu_retained;
    out->nsu_failed_closed = b->nsu_failed_closed;
    out->ordered_failed_closed = b->ordered_failed_closed;
}
