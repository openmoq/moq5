#ifndef MOQR_BIND_TESTING_INTERNAL_H
#define MOQR_BIND_TESTING_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

#ifdef MOQR_BIND_TESTING


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

extern _Atomic uint64_t bind_dbg_probes;

extern _Atomic int bind_dbg_fail_confirms;

extern _Atomic int bind_dbg_fail_sg_resets;

extern _Atomic uint32_t bind_dbg_update_filter_kind;

extern _Atomic int bind_dbg_block_update_answers;

extern _Atomic int bind_dbg_fail_nsu_stores;

extern _Atomic int bind_dbg_ring_full_n;

extern _Atomic int bind_dbg_ring_full_nsu;

extern moqr_bind_debug_detach_pending_fn bind_dbg_detach_pending_fn;

extern void *bind_dbg_detach_pending_ctx;

extern _Atomic int bind_dbg_block_upstream_cancels;

extern _Atomic int bind_dbg_accept_gone;

extern _Atomic int bind_dbg_fail_upstream_cancels;

extern _Atomic int bind_dbg_block_sub_dones;

extern _Atomic int bind_dbg_fail_sub_dones;

extern _Atomic int bind_dbg_fail_sub_done_kind;

extern _Atomic int bind_dbg_block_sg_resets;

extern _Atomic int bind_dbg_fail_probes;

extern moqr_result_t bind_dbg_probe_result;

extern _Atomic int bind_dbg_fail_fwd_writes;

extern _Atomic uint64_t bind_dbg_sg_attempts;

#endif
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_TESTING_INTERNAL_H */
