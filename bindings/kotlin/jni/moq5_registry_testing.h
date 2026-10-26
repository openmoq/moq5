/* Test-only control over the registry's internals.
 *
 * Allocation failure, a near-wrap generation, slot retirement and lease
 * scheduling are not reachable from the public contract, and a component whose
 * failure paths cannot be driven has untested failure paths. These seams exist
 * so the oracle can drive them.
 *
 * They are compiled ONLY under MOQ5_REGISTRY_TESTING. The Android objects never
 * define it, and the gate asserts these symbols are absent from every shipped
 * image and from all DSO exports.
 */
#ifndef MOQ5_REGISTRY_TESTING_H
#define MOQ5_REGISTRY_TESTING_H

#include <stdint.h>

#include "moq5_registry.h"

#ifdef MOQ5_REGISTRY_TESTING

/* Make the next internal allocation fail, once. */
void moq5_reg_test_fail_next_alloc(int on);

/* Prime a FREE slot's next-issued generation, to reach the wrap boundary
 * without 2^32 real reuses.
 *
 * It refuses a live slot on purpose: rewriting a live entry's generation would
 * leave the tokens its descendants recorded pointing at a root identity that no
 * longer exists, which is an incoherent state no real execution can reach
 * (chats/libmoq/1232 F1). Returns non-zero when the slot was primed. */
int moq5_reg_test_prime_free_slot_generation(uint32_t slot, uint32_t generation);

/* Whether a slot has been permanently retired by the wrap policy. */
int moq5_reg_test_slot_retired(uint32_t slot);

/* The slot the next insertion would take, so a failed insertion can be shown to
 * consume nothing. */
uint32_t moq5_reg_test_next_free_slot(void);

/* The table's current capacity, so a test can fill it exactly and force the
 * growth path rather than hoping one is needed. */
uint32_t moq5_reg_test_capacity(void);

/* A slot's CURRENT generation, so a failed operation can be shown to have
 * consumed none. The next-free slot alone proves neither capacity nor
 * generation. */
uint32_t moq5_reg_test_slot_generation(uint32_t slot);

/* Internal allocations minus frees. */
long moq5_reg_test_alloc_balance(void);

/* A hook the retiring thread calls once it has invalidated the subtree and is
 * about to wait for pins. It is how the oracle releases a lease holder at a
 * deterministic point instead of sleeping. */
void moq5_reg_test_on_retire_wait(void *arg, void (*fn)(void *));

/* Whether the registry lock is currently FREE, observed with a trylock.
 *
 * A cleanup calls this to prove the lock is not held across it. A trylock says
 * so immediately and cannot hang, unlike a timed probe that only makes the
 * answer likely (chats/libmoq/1232 F6). */
int moq5_reg_test_lock_is_free(void);

/* Set an entry's pin count directly, to drive the representable limit without
 * taking four billion pins. Returns non-zero when it was applied. */
int moq5_reg_test_set_pins(moq5_handle_t handle, uint32_t pins);

/* Whether the last cleanup ran on the thread that called retire. */
int moq5_reg_test_last_cleanup_was_retiring_thread(void);

#endif /* MOQ5_REGISTRY_TESTING */
#endif /* MOQ5_REGISTRY_TESTING_H */
