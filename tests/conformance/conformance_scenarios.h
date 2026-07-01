#ifndef MOQ_CONFORMANCE_SCENARIOS_H
#define MOQ_CONFORMANCE_SCENARIOS_H

/*
 * Adapter conformance scenarios.
 *
 * Each scenario is a self-contained C function that drives a
 * moq_adapter_pair_t through a protocol sequence and asserts
 * expected behavior. Returns 0 on pass, failure count otherwise.
 *
 * Scenarios are transport-agnostic: they work identically across
 * mvfst BridgePair, picoquic fake pair, and future adapter bindings.
 */

#include "moq_adapter_pair.h"

#ifdef __cplusplus
extern "C" {
#endif

int conformance_setup_handshake(moq_adapter_pair_t *pair);
int conformance_subscribe_and_object(moq_adapter_pair_t *pair);
int conformance_datagram_object(moq_adapter_pair_t *pair);
int conformance_single_control_stream(moq_adapter_pair_t *pair);
int conformance_close_not_fatal(moq_adapter_pair_t *pair);
int conformance_session_timer_goaway(moq_adapter_pair_t *pair);
int conformance_bidi_halfclose_accept(moq_adapter_pair_t *pair);
int conformance_bidi_halfclose_reject_late_fin(moq_adapter_pair_t *pair);
int conformance_crossed_cancel(moq_adapter_pair_t *pair);
int conformance_reset_propagation(moq_adapter_pair_t *pair);
int conformance_would_block_ordering(moq_adapter_pair_t *pair);
int conformance_dropped_datagram_unblocks(moq_adapter_pair_t *pair);
int conformance_blocked_ns_reject_tombstone(moq_adapter_pair_t *pair);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CONFORMANCE_SCENARIOS_H */
