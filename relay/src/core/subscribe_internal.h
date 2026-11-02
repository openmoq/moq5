#ifndef MOQR_RELAY_SUBSCRIBE_INTERNAL_H
#define MOQR_RELAY_SUBSCRIBE_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

r_sub_t *
sub_resolve(moqr_core_t *c, moqr_sub_t h, uint32_t *out_slot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_SUBSCRIBE_INTERNAL_H */
