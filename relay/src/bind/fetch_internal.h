#ifndef MOQR_BIND_FETCH_INTERNAL_H
#define MOQR_BIND_FETCH_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void
bind_fetch_mark_used(b_conn_t *cn, b_fetch_t *bf);

int
retrieve_loc_cmp(moqr_retrieve_location_t a, moqr_retrieve_location_t z);

moqr_retrieve_location_t
retrieve_loc(uint64_t group, uint64_t object);

moqr_retrieve_location_t
retrieve_inclusive_end(uint64_t group, uint64_t raw_object);

bool
retrieve_fwd_current(moqr_bind_t *b, const b_fwd_t *t);

void
retrieve_fwd_expire(b_fwd_t *t, uint64_t now_us);

moqr_result_t
retrieve_copy_object(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it,
                     const moq_fetch_object_event_t *o);

void
bind_standalone_abort(moqr_bind_t *b, const b_conn_t *cn, b_fetch_t *bf,
                      moqr_reset_cause_t cause);

void
bind_standalone_request(moqr_bind_t *b, b_conn_t *cn,
                        const moq_fetch_request_event_t *fq,
                        const moqr_fetch_req_t *freq,
                        const moq_fetch_request_options_t *fo,
                        const moqr_track_t *join_track, uint64_t now_us);

bool
bind_standalone_cancelled(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch);

const moq_fetch_request_options_t *
bind_fetch_options(const moq_event_t *ev);

void
bind_fetch_range(const moq_fetch_request_options_t *fo, moqr_fetch_req_t *freq);

void
bind_fetch_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_FETCH_INTERNAL_H */
