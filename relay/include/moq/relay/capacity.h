#ifndef MOQRELAY_CAPACITY_H
#define MOQRELAY_CAPACITY_H

/*
 * Closed-form capacity model.
 *
 * Every structure the relay retains is bounded by configuration, so the
 * memory ceiling is computable before anything runs. The describe function
 * is the formula; the check function is the enforcement — debug/test
 * callers assert it after every mutating step, which is what turns
 * "cannot OOM" from a slogan into a CI property.
 */

#include "log.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Ceiling for one log under a given config (the per-track term of the
 * shard formula). structure_bytes counts every allocation the log itself
 * can make at full occupancy (pools, journal, group slots, record vectors
 * at cap); payload_bytes is budget.max_bytes; total = both. */
/* Checked capacity arithmetic, shared by every describe in the relay:
 * overflow poisons to UINT64_MAX and the describe returns INVAL rather than
 * publish an understated ceiling. */
static inline uint64_t
moqr_cap_add(uint64_t a, uint64_t b)
{
    return (UINT64_MAX - a < b) ? UINT64_MAX : a + b;
}

static inline uint64_t
moqr_cap_mul(uint64_t a, uint64_t b)
{
    if (a == 0 || b == 0) {
        return 0;
    }
    return (a > UINT64_MAX / b) ? UINT64_MAX : a * b;
}

typedef struct moqr_log_capacity {
    uint64_t structure_bytes;
    uint64_t payload_bytes;
    uint64_t total_bytes;
    uint64_t max_records;      /* journal window = groups x objects cap  */
    uint32_t max_chunk_nodes;  /* resolved OPEN-record chunk-node pool size */
    /* rcbuf header overhead behind retained content: one payload + one
     * properties buffer per possible record, one slice per chunk node —
     * headers a logical-byte budget alone cannot see (H is the pinned
     * additive moq_rcbuf_allocation_size(0)). Included in total_bytes. */
    uint64_t header_bytes;
} moqr_log_capacity_t;

/* Pure function of the config (resolved defaults applied). */
MOQR_CORE_API moqr_result_t moqr_log_capacity_describe(const moqr_log_cfg_t *cfg,
                                          moqr_log_capacity_t *out);

/*
 * Verify a live log's invariants against its config-derived bounds:
 * retained payload bytes <= budget, group count <= cap, journal window
 * <= max_records, live record count consistent, per-subgroup Object IDs
 * strictly ascending, and actual allocator-visible structure bytes
 * (recomputed from current pools and vector capacities) <= the ceiling
 * moqr_log_capacity_describe promises.
 *
 * Returns MOQR_OK when every invariant holds; MOQR_ERR_INTERNAL with a
 * human-readable reason in *out_reason (static storage) otherwise.
 * Observing; never mutates.
 */
MOQR_CORE_API moqr_result_t moqr_log_capacity_check(const moqr_log_t *log,
                                      const char **out_reason);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_CAPACITY_H */
