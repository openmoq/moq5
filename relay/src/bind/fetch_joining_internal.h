#ifndef MOQR_BIND_FETCH_JOINING_INTERNAL_H
#define MOQR_BIND_FETCH_JOINING_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t
conn_cookie(const moqr_bind_t *b, const b_conn_t *cn);

b_conn_t *
fwd_conn(moqr_bind_t *b, uint64_t cookie, moqr_binding_t binding);

bool
fwd_bytes_charge(moqr_bind_t *b, uint64_t *charged, uint64_t n);

bool
fwd_charge(moqr_bind_t *b, b_fwd_t *t, uint64_t n);

void
fwd_item_release(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it);

void
fwd_release(moqr_bind_t *b, b_fwd_t *t);

void
fwd_end_up(b_fwd_t *t, uint8_t end);

void
fwd_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

void
fwd_admit(moqr_bind_t *b, b_conn_t *cn, const moq_fetch_request_event_t *fq,
          const moq_fetch_request_options_t *fo, uint64_t now_us);

bool
fwd_on_up_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev, uint64_t now_us);

bool
fwd_on_down_cancelled(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);

bool
fwd_result_is_gone(moq_result_t rc);

void
bind_fwd_pump(moqr_bind_t *b, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_FETCH_JOINING_INTERNAL_H */
