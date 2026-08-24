#ifndef MOQ_TEST_SWEEP_ARM_H
#define MOQ_TEST_SWEEP_ARM_H

/*
 * White-box helpers that place runnable work in the deferred-completion sweep.
 *
 * Each helper LINKS its slot into the pool's occupancy list, because that list
 * is what the sweeps walk: an owner armed without it would be invisible to the
 * very scan the helper exists to feed. Arming here must mirror what production
 * allocation does, or the fixture is arming a state no session can hold.
 *
 * A budgeted advance suspends only when the sweep has work it cannot afford, so
 * every suspension proof needs an owner the sweep will classify as runnable.
 * Shared by the session-level sweep tests and the bridge-level preservation
 * tests, which arm the same state and differ only in how they drive it.
 *
 * NOT installed. Requires the session internals; link moq-core-test-internals.
 */

#include "../../core/src/session/session_internal.h"

/*
 * A publication owner that is already EXPIRED, so the sweep skips the due-mark
 * and goes straight to STOP_STREAMS and then FINALIZE.
 */
static inline void sweep_arm_expired_pub(moq_session_t *s, size_t slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    pub_occ_link(s, slot);
    pe->done_pending = true;
    pe->done_expired = true;
    pe->done_stream_count = MOQ_QUIC_VARINT_MAX;
    pe->processed_stream_count = 0;
    pe->done_deadline_us = 0;
    /* Handles are pool-tagged: a raw integer is not a valid publication, so
     * build a real one (odd generation, per moq_handle_pack). */
    pe->handle = (moq_publication_t){
        moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, 1u, 1u, (uint32_t)slot) };
}

/* Bind an active rx stream to that publication so STOP_STREAMS has work. */
static inline void sweep_bind_rx(moq_session_t *s, size_t rx_slot,
                                 moq_publication_t pub)
{
    moq_rx_stream_t *rx = &s->rx_streams[rx_slot];
    rx_occ_link(s, rx_slot);
    rx->active = true;
    rx->pub_handle = pub;
    rx->sub = MOQ_SUBSCRIPTION_INVALID;
}

/*
 * A subscription owner that is already EXPIRED. The SUB stage runs last, so
 * suspending there leaves the PUB pool already walked -- which is what makes a
 * continuation's own fresh sweep observable.
 */
static inline void sweep_arm_expired_sub(moq_session_t *s, size_t slot)
{
    moq_sub_entry_t *se = &s->subs[slot];
    sub_occ_link(s, slot);
    se->done_pending = true;
    se->done_expired = true;
    se->done_stream_count = MOQ_QUIC_VARINT_MAX;
    se->processed_stream_count = 0;
    se->done_deadline_us = 0;
    se->handle = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, 1u, 1u, (uint32_t)slot) };
}

/*
 * A subgroup the REAP_SUBGROUPS stage will reap, one unit apiece.
 *
 * The deadline is pushed out of reach deliberately. A zero delivery deadline
 * reads as expired at any now_us (reported_subgroup_deadline), so the tick
 * stage would also come due on the same unconsumed unit -- and a suspension
 * missed at an earlier boundary would then be re-reported there, leaving the
 * pass looking correct. Arming sweep work must arm nothing else; a test that
 * wants a due deadline sets one and asserts that precondition itself.
 */
static inline void sweep_arm_closing_subgroup(moq_session_t *s, size_t slot)
{
    sg_occ_link(s, slot);
    s->subgroups[slot].state = MOQ_SG_CLOSING;
    s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
}

#endif /* MOQ_TEST_SWEEP_ARM_H */
