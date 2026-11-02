#ifndef MOQR_RELAY_FETCH_INTERNAL_H
#define MOQR_RELAY_FETCH_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint32_t
fetch_slot_find(moqr_core_t *c);

void
fetch_unpin(moqr_core_t *c, r_fetch_t *f);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_FETCH_INTERNAL_H */
