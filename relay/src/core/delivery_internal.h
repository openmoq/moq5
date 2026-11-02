#ifndef MOQR_RELAY_DELIVERY_INTERNAL_H
#define MOQR_RELAY_DELIVERY_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint32_t
ready_word_count(uint32_t max_bindings);

uint64_t
gpos_stride_for(uint64_t lists);

uint64_t
gpos_hash_cap_for(uint64_t groups);

r_gpos_t *
gpos_at(const r_sub_t *s, uint32_t i);

uint32_t *
gpos_emit(const r_sub_t *s, r_gpos_t *e);

uint32_t
gpos_hash_find(const r_sub_t *s, uint64_t group_id);

void
ready_mark(moqr_core_t *c, uint32_t bslot);

void
ready_clear(moqr_core_t *c, uint32_t bslot);

void
ready_mark_track(moqr_core_t *c, uint32_t tslot);

moqr_result_t
gpos_alloc(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log);

void
gpos_free(moqr_core_t *c, r_sub_t *s);

uint32_t
track_sub_count(const moqr_core_t *c, uint32_t track_slot);

uint32_t
track_fetch_count(const moqr_core_t *c, uint32_t track_slot);

void
sub_resolve_filter(r_sub_t *s, const moqr_filter_t *f, bool has_largest,
                   uint64_t lg, uint64_t lo);

bool
location_valid(const moq_location_filter_cfg_t *loc);

void
sub_resolve_location(r_sub_t *s, bool has, uint64_t lg, uint64_t lo);

bool
sub_excluded(const r_sub_t *s, uint64_t seq);

void
sub_exclude_retained(r_sub_t *s, const moqr_log_t *log);

void
track_location_ingest(moqr_core_t *c, r_track_t *t,
                      const moqr_log_append_desc_t *desc);

bool
rec_passes(const r_sub_t *s, const moqr_record_view_t *v);

bool
track_rec_expired(const r_track_t *t, const moqr_record_view_t *v,
                  uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_DELIVERY_INTERNAL_H */
