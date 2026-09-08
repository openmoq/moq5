#include <moq/relay/capacity.h>

#include "log_internal.h"

#include <moq/rcbuf.h>

#include <string.h>

moqr_result_t
moqr_log_capacity_describe(const moqr_log_cfg_t *cfg,
                           moqr_log_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    /* Same prefix-safe resolution create() uses — describe and create can
     * never disagree about what a config means. */
    moqr_log_cfg_resolved_t r;
    if (moqr_log_cfg_resolve(cfg, &r) != MOQR_OK) {
        return MOQR_ERR_INVAL;   /* derived counts unrepresentable */
    }

    uint64_t live_cap = moqr_cap_mul(r.budget.max_groups,
                                     r.max_objects_per_group);
    if (live_cap > r.max_records) {
        live_cap = r.max_records;
    }

    out->structure_bytes = moqr_log_structure_ceiling(&r);
    out->payload_bytes = r.budget.max_bytes;
    /* Retained-log rcbuf headers: every record may hold a payload and a
     * properties buffer, every chunk node a slice — each one allocator
     * request of H + payload, where only the payload is in max_bytes. */
    size_t hdr = 0;
    (void)moq_rcbuf_allocation_size(0, &hdr);
    out->header_bytes = moqr_cap_mul(
        hdr, moqr_cap_add(moqr_cap_mul(2u, live_cap), r.max_chunk_nodes));
    out->total_bytes = moqr_cap_add(
        moqr_cap_add(out->structure_bytes, out->payload_bytes),
        out->header_bytes);
    out->max_records = live_cap;
    out->max_chunk_nodes = r.max_chunk_nodes;
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}

#define CHECK_FAIL(why)          \
    do {                         \
        if (out_reason != NULL)  \
            *out_reason = (why); \
        return MOQR_ERR_INTERNAL; \
    } while (0)

moqr_result_t
moqr_log_capacity_check(const moqr_log_t *log, const char **out_reason)
{
    if (out_reason != NULL) {
        *out_reason = "ok";
    }
    if (log == NULL) {
        CHECK_FAIL("log is NULL");
    }
    if (log->retained_bytes > log->max_bytes) {
        CHECK_FAIL("retained bytes exceed budget");
    }
    if (log->group_count > log->max_groups) {
        CHECK_FAIL("group count exceeds cap");
    }
    if (log->next_seq - log->begin_seq > log->max_records) {
        CHECK_FAIL("journal window exceeds max records");
    }
    if (log->live_records > log->next_seq - log->begin_seq) {
        CHECK_FAIL("live records exceed journal window");
    }

    uint64_t bytes = 0, recs = 0;
    uint64_t prev_group = 0;
    bool first = true;
    for (uint32_t i = 0; i < log->group_count; i++) {
        const moqr_group_t *g = &log->groups[i];
        if (!first && g->group_id <= prev_group) {
            CHECK_FAIL("group array not strictly ascending");
        }
        first = false;
        prev_group = g->group_id;

        if (g->record_count > log->max_objects_per_group) {
            CHECK_FAIL("group exceeds per-group object cap");
        }
        if (g->sg_count > log->max_subgroups) {
            CHECK_FAIL("group exceeds subgroup cap");
        }
        uint64_t gbytes = 0, grecs = 0;
        for (uint32_t sgi = 0; sgi < g->sg_count; sgi++) {
            const moqr_subgroup_t *sg = &g->sgs[sgi];
            uint64_t prev_obj = 0;
            for (uint32_t r = 0; r < sg->v.count; r++) {
                const moqr_rec_t *rec = &sg->v.recs[r];
                if (r != 0 && rec->object_id <= prev_obj) {
                    CHECK_FAIL("subgroup object ids not ascending");
                }
                prev_obj = rec->object_id;
                if (g->final_object_id != MOQR_FINAL_UNKNOWN &&
                    rec->object_id > g->final_object_id) {
                    CHECK_FAIL("record beyond group final");
                }
                gbytes += rec->bytes;
                grecs++;
            }
        }
        for (uint32_t r = 0; r < g->dg.count; r++) {
            gbytes += g->dg.recs[r].bytes;
            grecs++;
        }
        if (gbytes != g->bytes) {
            CHECK_FAIL("group byte aggregate mismatch");
        }
        if (grecs != g->record_count) {
            CHECK_FAIL("group record aggregate mismatch");
        }
        bytes += gbytes;
        recs += grecs;
    }
    if (bytes != log->retained_bytes) {
        CHECK_FAIL("log byte aggregate mismatch");
    }
    if (recs != log->live_records) {
        CHECK_FAIL("log record aggregate mismatch");
    }

    /* Allocator-visible structure bytes: recompute the log's actual
     * footprint from its current shape and require it under the ceiling
     * the formula promises. */
    {
        moqr_log_cfg_resolved_t r;
        memset(&r, 0, sizeof(r));
        r.budget.max_groups = log->max_groups;
        r.budget.max_bytes = log->max_bytes;
        r.max_subgroups = log->max_subgroups;
        r.max_objects_per_group = log->max_objects_per_group;
        r.max_records = log->max_records;
        r.max_cursors = log->max_cursors;
        r.max_chunk_nodes = log->max_chunk_nodes;
        r.id_set_cap = log->id_set_cap;
        uint64_t ceiling = moqr_log_structure_ceiling(&r);

        uint64_t actual = sizeof(moqr_log_t);
        actual += (uint64_t)log->max_groups * sizeof(moqr_group_t);
        actual += (uint64_t)log->max_records * sizeof(moqr_jslot_t);
        actual += (uint64_t)log->max_cursors * sizeof(moqr_cursor_slot_t);
        actual += (uint64_t)log->max_chunk_nodes * sizeof(moqr_chunk_node_t);
        for (uint32_t i = 0; i < log->group_count; i++) {
            const moqr_group_t *g = &log->groups[i];
            actual += (uint64_t)log->max_subgroups * sizeof(moqr_subgroup_t);
            actual += (uint64_t)log->id_set_cap * sizeof(uint64_t);
            for (uint32_t sgi = 0; sgi < g->sg_count; sgi++) {
                actual += (uint64_t)g->sgs[sgi].v.cap * sizeof(moqr_rec_t);
            }
            actual += (uint64_t)g->dg.cap * sizeof(moqr_rec_t);
        }
        if (actual > ceiling) {
            CHECK_FAIL("structure bytes exceed described ceiling");
        }
    }

    uint32_t live_cursors = 0;
    for (uint32_t i = 0; i < log->max_cursors; i++) {
        if ((log->cursors[i].generation & 1u) != 0) {
            live_cursors++;
        }
    }
    if (live_cursors != log->cursor_count) {
        CHECK_FAIL("cursor count mismatch");
    }

    return MOQR_OK;
}
