#include "bind_internal.h"
#include "testing_internal.h"
#include "delivery_internal.h"

#ifdef MOQR_BIND_TESTING
/* Test-only probe gauge (compiled solely into the test-internals build):
 * one count per moqr_core_next_delivery call issued by the delivery half.
 * ATOMIC: multi-lane fixtures increment from every lane thread. */
_Atomic uint64_t bind_dbg_probes;

uint64_t
moqr_bind_debug_delivery_probes(void)
{
    return atomic_load(&bind_dbg_probes);
}


/* Test-only fault injection: fail the Nth delivery confirmation from now
 * (1 = the very next one) as a consistency error. */
_Atomic int bind_dbg_fail_confirms;

void
moqr_bind_debug_fail_confirm(int nth)
{
    atomic_store(&bind_dbg_fail_confirms, nth);
}


/* Test-only fault injection: fail the Nth SUB_DONE subgroup reset from now as
 * a hard (non-WOULD_BLOCK) error without calling the session, so the
 * production close -> detach -> purge path runs from inside an ordered-ring
 * retry. */
_Atomic int bind_dbg_fail_sg_resets;

/* Test-only: the Location Filter kind the binding reads on the next
 * subscription update that carries a replacement filter, in place of the
 * session's. 0 = none pending. Stands in for a peer that sends a form this
 * library's own subscriber API cannot express on an update. */
_Atomic uint32_t bind_dbg_update_filter_kind;

/* Test-only: the next N downstream update answers observe WOULD_BLOCK
 * without calling the session, so the hold-and-retry path runs against an
 * answer that was never given. */
_Atomic int bind_dbg_block_update_answers;

_Atomic int bind_dbg_fail_nsu_stores;

_Atomic int bind_dbg_ring_full_n;

_Atomic int bind_dbg_ring_full_nsu;

moqr_bind_debug_detach_pending_fn bind_dbg_detach_pending_fn;

void *bind_dbg_detach_pending_ctx;

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

void
moqr_bind_debug_update_filter_kind(uint32_t kind)
{
    atomic_store(&bind_dbg_update_filter_kind, kind);
}

void
moqr_bind_debug_block_update_answers(int n)
{
    atomic_store(&bind_dbg_block_update_answers, n);
}


/* Test-only: the next N upstream cancellations a malformed Track owes
 * observe WOULD_BLOCK without calling the session, so the owed-cancel retry
 * runs against a cancellation that was never sent. */
_Atomic int bind_dbg_block_upstream_cancels;

void
moqr_bind_debug_block_upstream_cancels(int n)
{
    atomic_store(&bind_dbg_block_upstream_cancels, n);
}


/* Test-only fault injection: make the Nth next retry of a held acceptance
 * observe its request as already retired (without calling the session), so
 * the production undo arm runs. */
_Atomic int bind_dbg_accept_gone;

void
moqr_bind_debug_accept_gone(int nth)
{
    atomic_store(&bind_dbg_accept_gone, nth);
}


/* Test-only: the next N upstream cancellations a malformed Track owes fail
 * with MOQ_ERR_BUFFER (a message that can never fit) without calling the
 * session, so the hard-failure arm runs against a cancellation that can
 * never be made. */
_Atomic int bind_dbg_fail_upstream_cancels;

int
moqr_bind_debug_fail_upstream_cancels(int n)
{
    return atomic_exchange(&bind_dbg_fail_upstream_cancels, n);
}


/* Test-only fault injection: make the next N downstream SUB_DONE terminal
 * writes observe WOULD_BLOCK without calling the session, so the production
 * defer-and-retry arm runs against a terminal that was never sent. */
_Atomic int bind_dbg_block_sub_dones;

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


/* Test-only fault injection: refuse the Nth SUB_DONE terminal write from now
 * with WRONG_STATE without calling the session -- a refusal that queued
 * nothing -- restricted to subscriptions (kind 1), publications (kind 2) or
 * either (kind 0). Proves the binding's handling of a hard refusal; it says
 * nothing about which real states produce one. */
_Atomic int bind_dbg_fail_sub_dones;

_Atomic int bind_dbg_fail_sub_done_kind;

void
moqr_bind_debug_fail_sub_done(int nth, int kind)
{
    atomic_store(&bind_dbg_fail_sub_done_kind, kind);
    atomic_store(&bind_dbg_fail_sub_dones, nth);
}


/* Test-only fault injection: make the next N subgroup resets a SUB_DONE owes
 * observe WOULD_BLOCK without calling the session, so the whole terminal is
 * deferred with that stream's obligation still recorded and the retry must
 * reset it before the terminal goes out. */
_Atomic int bind_dbg_block_sg_resets;

void
moqr_bind_debug_block_sg_reset(int n)
{
    atomic_store(&bind_dbg_block_sg_resets, n);
}

int
moqr_bind_debug_sg_reset_blocks_left(void)
{
    return atomic_load(&bind_dbg_block_sg_resets);
}


/* Test-only probe-result injection: force the Nth next_delivery probe from
 * now to report `result` without calling the core — pins the CAPACITY-only
 * RETRY classification (every other error must fail closed). */
_Atomic int bind_dbg_fail_probes;

moqr_result_t bind_dbg_probe_result;

void
moqr_bind_debug_fail_probe(int nth, moqr_result_t result)
{
    bind_dbg_probe_result = result;   /* set before arming the countdown */
    atomic_store(&bind_dbg_fail_probes, nth);
}


/* Test-only write-failure injection for a forwarded fetch after its
 * downstream FETCH_OK: the Nth next forwarded object write from now is
 * reported MOQ_ERR_INVAL without reaching the session, so the request-local
 * abort path (never a connection close) is observable. */
_Atomic int bind_dbg_fail_fwd_writes;

void
moqr_bind_debug_fail_fwd_write(int nth)
{
    atomic_store(&bind_dbg_fail_fwd_writes, nth);
}


/* Test-only reason-specific gauges: SESSION_SG re-attempts made by the
 * pump, and the current SESSION_SG-parked population (the exceptional
 * set the plan bounds). */
_Atomic uint64_t bind_dbg_sg_attempts;

uint64_t
moqr_bind_debug_sg_attempts(void)
{
    return atomic_load(&bind_dbg_sg_attempts);
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


bool
moqr_bind_debug_scan_bounds_ok(const moqr_bind_t *b)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        const b_conn_t *cn = &b->conns[ci];
        if (!cn->used) {
            continue;
        }
        for (uint32_t fi = cn->fetch_hw; fi < b->n_fetches; fi++) {
            if (cn->fetches[fi].used) {
                return false;
            }
        }
        for (uint32_t i = 0; !(cn->retry_pending & B_RETRY_REFUSAL) &&
                             i < b->n_refusals; i++) {
            if (cn->refusals[i].used) {
                return false;
            }
        }
        for (uint32_t i = 0; !(cn->retry_pending & B_RETRY_CANCEL) &&
                             i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].cancel_owed) {
                return false;
            }
        }
        for (uint32_t i = 0; !(cn->retry_pending & B_RETRY_CANCEL) &&
                             i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].cancel_owed) {
                return false;
            }
        }
    }
    return true;
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
