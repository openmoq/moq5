#ifndef MOQ_BRIDGE_CONFORMANCE_PAIR_H
#define MOQ_BRIDGE_CONFORMANCE_PAIR_H

/*
 * Bridge conformance pair: two moq_transport_bridge_t instances
 * connected via fake endpoints for deterministic testing.
 *
 * Implements moq_adapter_pair_ops_t so the same conformance
 * scenarios that run against picoquic and mvfst adapters also
 * run against the shared transport bridge.
 *
 * NOT installed. Test infrastructure only.
 */

#include "../conformance/moq_adapter_pair.h"

#ifdef __cplusplus
extern "C" {
#endif

moq_adapter_pair_t bridge_conformance_create(void);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_BRIDGE_CONFORMANCE_PAIR_H */
