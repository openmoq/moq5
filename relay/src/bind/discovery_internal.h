#ifndef MOQR_BIND_DISCOVERY_INTERNAL_H
#define MOQR_BIND_DISCOVERY_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint32_t
bind_nsans_cap(const moqr_core_limits_t *lim);

void
ann_clear_slot(moqr_bind_t *b, b_conn_t *cn, uint32_t slot);

void
ann_clear_all(moqr_bind_t *b, b_conn_t *cn);

int
ann_find(const moqr_bind_t *b, const b_conn_t *cn, uint64_t raw);

moqr_result_t
bind_ann_withdraw(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t now_us);

int
ann_store(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, moqr_ns_t ns);

bool
ns_equal(moqr_ns_t a, moqr_ns_t b);

void
bind_cluster_adverts_clear(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);

moqr_result_t
bind_announce(moqr_bind_t *b, b_conn_t *cn, moqr_ns_t ns, uint64_t raw,
              uint64_t now_us);

uint8_t
bind_nsu_family(uint64_t handle);

bool
bind_cookie_is_publication(uint64_t cookie);

bool
bind_cookie_is_track_sub(uint64_t cookie);

void
bind_tokens_to_relay(const moq_resolved_token_t *in, size_t n,
                     moqr_auth_token_t *out);

void
bind_nsu_retire(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t now_us);

b_nsans_t *
bind_nsans_lookup(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle,
                  bool initial);

b_nsans_t *
bind_nsans_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle);

void
bind_nsans_forget(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle);

void
bind_nsans_drop(moqr_bind_t *b, b_nsans_t *e);

void
bind_nsans_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

b_nsans_t *
bind_nsans_hold(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t now_us);

void
bind_tracks_hold_answer(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                        bool reject, uint64_t err, uint64_t now_us);

void
bind_nsu_authorize(moqr_bind_t *b, b_conn_t *cn, uint8_t family,
                   uint64_t handle, uint64_t id, moqr_auth_action_t stage,
                   uint64_t now_us);

void
bind_nsu_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                  const moqr_auth_verdict_t *verdict, uint64_t now_us);

void
bind_nsu_pump(moqr_bind_t *b, uint64_t now_us);

void
bind_tracks_reject(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, uint64_t err,
                   uint64_t now_us);

void
bind_tracks_admit(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, moqr_ns_t prefix,
                  bool forward, bool include_properties, uint32_t group_order,
                  const moqr_auth_token_t *tokens, size_t token_count,
                  uint64_t now_us);

void
bind_dgrant_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

void
bind_discovery_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);

void
bind_discovery_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                        const moqr_auth_verdict_t *verdict, uint64_t now_us);

void
bind_publication_answered(moqr_bind_t *b, b_conn_t *cn, uint64_t pub_raw,
                          bool ok, bool forward, uint64_t now_us);

void bind_cluster_sync(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us, bool discover);

void bind_cluster_discovery_end(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                                uint64_t now_us);

void bind_cluster_discovery_cancel(moqr_bind_t *b, b_conn_t *cn, moq_ns_sub_handle_t handle,
                                   uint64_t now_us);

void bind_cluster_apply_ann(moqr_bind_t *b, b_conn_t *cn, uint32_t slot, uint64_t now_us);

void bind_cluster_found(moqr_bind_t *b, b_conn_t *cn,
                        const moq_namespace_found_event_t *found, uint64_t now_us);

moq_result_t bind_cluster_namespace(moqr_bind_t *b, b_conn_t *cn, const moqr_intent_t *it,
                                    uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_DISCOVERY_INTERNAL_H */
