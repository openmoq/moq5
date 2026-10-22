#ifndef MOQ_CLUSTER_H
#define MOQ_CLUSTER_H

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Cluster draft-01 namespace metadata. Hop IDs are ordered from the original
 * publisher to the sender. Arrays supplied to an API are borrowed for that
 * call; arrays in events have the session's output-scratch borrow lifetime.
 * hop_count == 0 means HOP_PATH is absent, permitted only in a partial update
 * or on a session that did not negotiate the extension. Zero IDs are valid;
 * duplicate nonzero IDs are not. Presence of cost is independent of zero.
 * This view's layout is frozen: it is embedded in size-aware public records. */
typedef struct moq_cluster_metadata {
    const uint64_t *hop_ids;
    size_t hop_count;
    bool has_route_cost;
    uint64_t route_cost;
} moq_cluster_metadata_t;

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CLUSTER_H */
