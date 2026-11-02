#ifndef MOQR_RELAY_BINDINGS_INTERNAL_H
#define MOQR_RELAY_BINDINGS_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

r_binding_t *
binding_resolve(moqr_core_t *c, moqr_binding_t h, uint32_t *out_slot);

void
binding_unpin(r_binding_t *b);

void
binding_drop_held(r_binding_t *b);

moqr_result_t
binding_pin_chunks(moqr_core_t *c, r_binding_t *b, const moqr_log_t *log,
                   const moqr_record_view_t *v, uint32_t base);

void
sub_list_push(moqr_core_t *c, uint32_t bslot, uint32_t sslot);

void
track_sub_link(moqr_core_t *c, uint32_t tslot, uint32_t sslot);

void
sub_begun_add(moqr_core_t *c, r_sub_t *s);

void
sub_begun_sub(moqr_core_t *c, r_sub_t *s);

void
sub_retire(moqr_core_t *c, uint32_t slot, uint64_t now_us);

moqr_result_t
track_source_failover(moqr_core_t *c, uint32_t tslot, uint32_t excl_bslot,
                      uint32_t excl_bgen, uint64_t excluded_lifetime,
                      uint64_t now_us, bool *retargeted);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_BINDINGS_INTERNAL_H */
