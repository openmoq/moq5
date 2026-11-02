#ifndef MOQR_RELAY_INTENTS_INTERNAL_H
#define MOQR_RELAY_INTENTS_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

bool
intent_space(const moqr_core_t *c, uint32_t need);

moqr_intent_t *
intent_push(moqr_core_t *c, moqr_intent_kind_t kind);

moqr_result_t
r_refuse(moqr_core_t *c, moqr_refuse_reason_t reason);

void
intent_set_ns_from_key(moqr_intent_t *it, const r_key_t *key);

uint32_t
intent_ns_count(const moqr_core_t *c);

bool
intent_borrows_part(const moqr_core_t *c, const uint8_t *part);

void
intent_set_ns_from_node(const moqr_core_t *c, moqr_intent_t *it,
                        uint32_t node);

void
intent_scrub_track(moqr_core_t *c, uint32_t slot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_INTENTS_INTERNAL_H */
