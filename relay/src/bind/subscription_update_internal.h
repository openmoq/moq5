#ifndef MOQR_BIND_SUBSCRIPTION_UPDATE_INTERNAL_H
#define MOQR_BIND_SUBSCRIPTION_UPDATE_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moqr_result_t
bind_read_sub_filter(b_conn_t *cn, moq_subscription_t sub, moqr_filter_t *out);

moqr_result_t
bind_read_sub_location(b_conn_t *cn, moq_subscription_t sub,
                       moq_location_filter_cfg_t *out);

bool
bind_range_end_holds(const b_conn_t *cn);

bool
bind_update_filter_applies(const b_conn_t *cn);

const moq_subscription_update_options_t *
bind_update_options(const moq_event_t *ev);

moqr_result_t
bind_update_resolved_location(const moq_subscription_update_options_t *options,
                              uint64_t id, moq_location_filter_cfg_t *out);

moq_fetch_range_kind_t
bind_fill_unserved_kind(bool budget_spent);

void
bind_dsub_release(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds);

void
bind_dsub_retire(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds, uint64_t now_us);

void
bind_note_relay_largest(moqr_bind_t *b, moqr_track_t track);

void
bind_update_answer_for(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t id,
                       bool has_forward, bool forward, bool resolve_filter,
                       uint64_t now_us);

void
bind_update_answer(moqr_bind_t *b, b_conn_t *cn,
                   const moq_subscribe_updated_event_t *up, bool resolve_filter,
                   uint64_t now_us);

void
bind_update_answer_pump(moqr_bind_t *b, uint64_t now_us);

b_upd_give_t
bind_update_answer_settle(moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw,
                          uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_SUBSCRIPTION_UPDATE_INTERNAL_H */
