#ifndef MOQR_BIND_REQUEST_ANSWERS_INTERNAL_H
#define MOQR_BIND_REQUEST_ANSWERS_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moq_result_t
bind_refusal_submit(b_conn_t *cn, const b_refusal_t *r, uint64_t now_us);

void
bind_refuse(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t error_code, bool can_retry,
            uint64_t retry_ms, uint64_t now_us);

void
bind_terminal_hold(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *want);

bool
bind_kind_is_accept(moqr_auth_action_t kind);

void
bind_accept(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t now_us);

bool
bind_ns_sub_accept_held(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t handle);

void
bind_fetch_refuse(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch,
                  uint64_t error_code, uint64_t now_us);

uint32_t
bind_refusal_free(const moqr_bind_t *b, const b_conn_t *cn);

bool
bind_refusal_can_poll(const moqr_bind_t *b, const b_conn_t *cn);

void
bind_refusal_pump(moqr_bind_t *b, uint64_t now_us);

bool
bind_refusal_drop(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);

void
bind_refusal_conn_gone(moqr_bind_t *b, b_conn_t *cn);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_REQUEST_ANSWERS_INTERNAL_H */
