/*
 * The listener's allocator footprint.
 *
 * A separate translation unit so the capacity model can link one checked
 * descriptor without dragging socket and thread code into every config
 * consumer. It derives from the same private layout the constructor allocates,
 * so the reported ceiling and the real request cannot drift.
 */
#include "admin_listen.h"
#include "admin_listen_layout.h"

#include "../obs/moqr_obs.h"
#include "shards_doc.h"

#include <moqrelay/capacity.h>

#include <string.h>

moqr_result_t
moqr_admin_listen_footprint(uint32_t lanes,
                            moqr_admin_listen_footprint_t *out)
{
    uint64_t om = 0;
    uint64_t pr = 0;
    uint64_t sh;
    uint64_t per_bank;

    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    /* Sized for the WIDEST document the endpoint can serve -- bind and shard
     * families present -- because a bank sized for a narrower composition is
     * too small the moment a richer one is rendered. */
    if (moqr_metrics_bound(lanes, MOQR_OBS_FMT_OPENMETRICS_100, true, true,
                           &om) != MOQR_OK ||
        moqr_metrics_bound(lanes, MOQR_OBS_FMT_PROMETHEUS_004, true, true,
                           &pr) != MOQR_OK ||
        om == UINT64_MAX || pr == UINT64_MAX) {
        return MOQR_ERR_CAPACITY;
    }
    sh = moqr_cli_shards_bound(lanes);
    if (sh == UINT64_MAX) {
        return MOQR_ERR_CAPACITY;
    }
    /* The single heap object, counted exactly once. It contains the broker
     * pointer, the admin state machine with its fixed client slots and their
     * request/header/response bookkeeping, the descriptor tables, the refusal
     * slot with its fixed head buffer, the pthread and atomic fields, the
     * endpoint identity and the callback pointers -- all fields, not separate
     * terms. */
    out->object_bytes = (uint64_t)sizeof(struct moqr_admin_listen);
    /* Every preallocated body in both banks -- the two metrics formats and
     * the shards JSON document -- each with room for a terminating byte. */
    out->body_cap[MOQR_OBS_FMT_OPENMETRICS_100] = moqr_cap_add(om, 1u);
    out->body_cap[MOQR_OBS_FMT_PROMETHEUS_004] = moqr_cap_add(pr, 1u);
    out->body_cap[MOQR_ADMIN_BODY_SHARDS] = moqr_cap_add(sh, 1u);
    per_bank = moqr_cap_add(out->body_cap[MOQR_OBS_FMT_OPENMETRICS_100],
                            out->body_cap[MOQR_OBS_FMT_PROMETHEUS_004]);
    per_bank = moqr_cap_add(per_bank, out->body_cap[MOQR_ADMIN_BODY_SHARDS]);
    out->body_bytes = moqr_cap_mul(per_bank, (uint64_t)MOQR_ADMIN_BANKS);
    out->total_alloc_bytes = moqr_cap_add(out->object_bytes, out->body_bytes);
    if (out->total_alloc_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_CAPACITY;   /* refuse rather than under-report */
    }
    /* A reservation, not an allocator request: reported beside the total, never
     * inside it. Kernel socket buffers are excluded for the same reason. */
    out->thread_stack_bytes = MOQR_CLI_ADMIN_STACK_BYTES;
    return MOQR_OK;
}
