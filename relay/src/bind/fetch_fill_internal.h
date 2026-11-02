#ifndef MOQR_BIND_FETCH_FILL_INTERNAL_H
#define MOQR_BIND_FETCH_FILL_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moqr_reset_cause_t
bind_retrieve_error_cause(const moqr_retrieve_item_t *it);

uint64_t
bind_retrieve_reset_code(const moqr_bind_t *b, const b_conn_t *cn,
                         moqr_reset_cause_t cause);

void
bind_fill_release(moqr_bind_t *b, b_fetch_t *bf);

uint64_t
bind_wait_deadline(const moqr_bind_t *b, uint64_t processed_at_us,
                   bool has_fill_timeout, uint64_t fill_timeout_ms);

void
bind_fill_request(moqr_bind_t *b, b_conn_t *cn,
                  const moq_fill_request_event_t *fr, uint64_t now_us);

void
bind_fill_terminal(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill);

bool
bind_fill_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_FETCH_FILL_INTERNAL_H */
