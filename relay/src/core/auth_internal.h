#ifndef MOQR_RELAY_AUTH_INTERNAL_H
#define MOQR_RELAY_AUTH_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moqr_result_t
r_copy_material(moqr_core_t *c, const moqr_park_req_t *req, uint32_t *used,
                uint32_t cap, r_material *m);

void
r_free_material(moqr_core_t *c, r_material *m, uint32_t *used);

void
parked_free_slot(moqr_core_t *c, uint32_t slot);

void
grant_free_slot(moqr_core_t *c, uint32_t slot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_AUTH_INTERNAL_H */
