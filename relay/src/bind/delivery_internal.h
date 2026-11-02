#ifndef MOQR_BIND_DELIVERY_INTERNAL_H
#define MOQR_BIND_DELIVERY_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint32_t
bind_dl_word_count(uint32_t max_conns);

void
bind_dl_set(uint64_t *words, uint32_t slot);

void
bind_dl_clear(uint64_t *words, uint32_t slot);

bool
bind_dl_test(const uint64_t *words, uint32_t slot);

void
bind_dl_rearm_on_slot_release(moqr_bind_t *b, const b_conn_t *cn);

void
bind_dl_clear_conn(moqr_bind_t *b, b_conn_t *cn);

moqr_delivery_outcome_t
bind_write_chunked(moqr_bind_t *b, b_conn_t *cn, const moqr_delivery_t *d,
                   b_sg_t *sg, uint64_t now_us);

bool
bind_confirm_delivery(moqr_bind_t *b, b_conn_t *cn,
                      moqr_delivery_outcome_t oc, uint64_t now_us);

bool
bind_discard_stopped(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);

bool
bind_delivery_eligible(void *ctx, uint64_t cookie, const moqr_record_view_t *rec);

b_deliver_outcome_t
bind_deliver(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_DELIVERY_INTERNAL_H */
