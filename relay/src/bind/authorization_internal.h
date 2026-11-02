#ifndef MOQR_BIND_AUTHORIZATION_INTERNAL_H
#define MOQR_BIND_AUTHORIZATION_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

bind_auth_result_t
bind_authorize(moqr_bind_t *b, const b_conn_t *cn, moqr_auth_action_t action,
               moqr_ns_t ns, moq_bytes_t name,
               const moq_resolved_token_t *tokens, size_t token_count,
               uint64_t now_us, uint64_t *err, uint64_t *ticket, uint64_t *lease);

void
bind_park_init(moqr_park_req_t *pr, const b_conn_t *cn, const moqr_bind_t *b,
               moqr_auth_action_t action, moqr_ns_t ns, moq_bytes_t name,
               uint64_t session_cookie);

moqr_result_t
bind_park(moqr_bind_t *b, moqr_park_req_t *pr,
          const moq_resolved_token_t *tokens, size_t token_count,
          uint64_t ticket, uint64_t now_us);

moqr_result_t
bind_grant_reserve(moqr_bind_t *b, moqr_park_req_t *pr,
                   const moq_resolved_token_t *tokens, size_t token_count,
                   uint64_t lease_us, uint64_t now_us, moqr_grant_res_t *res);

moq_bytes_t
bind_subscribe_error_reason(moq_request_error_t code);

void
bind_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg,
                               moq_request_error_t code);

uint64_t
bind_capacity_retry_ms(uint64_t request_cookie);

moq_request_error_t
bind_capacity_error(const b_conn_t *cn);

void
bind_reject_load(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                 uint64_t now_us);

void
bind_reject_filter(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                   uint64_t now_us);

void
bind_reject(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
            uint64_t session_cookie, uint64_t err, uint64_t now_us);

void
bind_reject_admission(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
                      uint64_t session_cookie, moqr_result_t rc,
                      uint64_t now_us);

void
bind_grant_commit_sub(moqr_bind_t *b, moqr_grant_res_t gres, moqr_sub_t rsub,
                      uint64_t now_us);

bool
bind_grant_commit_ann(moqr_bind_t *b, b_conn_t *cn, moqr_grant_res_t gres,
                      moqr_ns_t ns, uint64_t session_cookie, uint64_t now_us);

void
bind_resume(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
            uint64_t lease, uint64_t now_us);

void
bind_refuse_or_park(moqr_bind_t *b, b_conn_t *cn, bind_auth_result_t ar,
                    moqr_park_req_t *pr, const moq_resolved_token_t *tokens,
                    size_t token_count, uint64_t ticket, uint64_t err,
                    uint64_t now_us);

bool
bind_sub_rendezvous(const b_conn_t *cn, const moq_event_t *ev, uint64_t now_us,
                    uint64_t *deadline_us);

void
bind_retain_session_tokens(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                           const moq_resolved_token_t *tokens, size_t count);

const moq_subscribe_tracks_options_t *
bind_tracks_options(const moq_event_t *ev);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_AUTHORIZATION_INTERNAL_H */
