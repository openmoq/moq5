#ifndef MOQR_BIND_INTENTS_INTERNAL_H
#define MOQR_BIND_INTENTS_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

bool
bind_execute_intents(moqr_bind_t *b, uint64_t now_us);

void
bind_pending_purge(moqr_bind_t *b, uint64_t binding_cookie, bool ns_sub_only,
                   uint64_t ns_cookie);

void
bind_pending_purge_conn(moqr_bind_t *b, uint64_t binding_cookie);

bool
bind_retry_may_drain_intents(moqr_bind_t *b);

b_dsub_t *
dsub_find(const moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw);

bool
bind_request_ended_by_session(const b_conn_t *cn, const b_dsub_t *ds);

uint64_t
bind_stream_count_next(uint64_t count, uint8_t wire_draft);

int
dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn);

bool
bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked_out, moqr_intent_t *defer);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_INTENTS_INTERNAL_H */
