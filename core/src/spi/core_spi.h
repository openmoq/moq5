#ifndef MOQ_CORE_SPI_H
#define MOQ_CORE_SPI_H

/*
 * Same-build interface between libmoq-core and the relay, media service and virtual transport
 * built with it. Each symbol here is exported from the core library so a
 * shared sibling library can call it, which makes it a private
 * same-build ABI: a binary dependency between components built together, not
 * a stable or public ABI. No installed header declares it, and it changes with
 * the core it ships beside. tests/cmake/check_core_spi_symbols.cmake pins the
 * exact exported set. Wire decoding stays in the core; callers receive
 * values, never wire forms.
 */

#include <moq/export.h>
#include <moq/publisher.h>
#include <moq/session.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The MAX_CACHE_DURATION Track Property (0x04: draft-22 §10.3, draft-18
 * §12.3, draft-16 §11.1.1) in a Track Property set encoded for `version`.
 * Draft-18 and draft-22 search Immutable Properties as well, as their text
 * requires. Draft-16 reads the top-level set only: its Immutable Extensions
 * (§11.2) hold Track Extensions a relay "MAY decode and view", so one found
 * only inside them is not applied. True with its milliseconds in *out_ms when
 * present; false when absent, for an unsupported version, or past a point
 * where the set does not parse. A relay "MUST NOT start forwarding any
 * individual Object received through this subscription or fetch after the
 * specified number of milliseconds has elapsed since the beginning of the
 * Object was received". */
MOQ_API bool moq_core_internal_track_max_cache_duration(moq_version_t version,
                                                        const uint8_t *props,
                                                        size_t len,
                                                        uint64_t *out_ms);

/* moq_track_properties_validate() with the track's group order as well,
 * checked as the publisher facade adds it (DEFAULT_PUBLISHER_GROUP_ORDER,
 * draft-22 10.5): a set that already carries another order, or an order
 * other than the three defined, is MOQ_ERR_INVAL. MOQ_GROUP_ORDER_DEFAULT
 * checks nothing more. */
MOQ_API moq_result_t moq_core_internal_track_properties_validate(
    moq_version_t version, const uint8_t *props, size_t len,
    bool dynamic_groups, moq_group_order_t group_order);

/* moq_pub_reset_group() with an observation of this call's actual effects.
 * out_did_reset is required and is initialized to false before validation.
 * True means at least one moq_session_reset_subgroup() returned MOQ_OK during
 * THIS call, including partial progress before WOULD_BLOCK or another error.
 * Stale handles and releasing only a local pending operation do not count.
 * A caller counting one completed abandonment must retain this fact across
 * retries and count only when the whole operation returns MOQ_OK. All other
 * behavior, ownership and serialization requirements match the public entry. */
MOQ_API moq_result_t moq_core_internal_pub_reset_group(
    moq_publisher_t *pub, moq_pub_track_t *track,
    uint64_t error_code, uint64_t now_us, bool *out_did_reset);

/* Shipping virtual transport observation. Not a second exclusive owner:
 * native bridge ownership overrides polling. Identity is never dereferenced.
 * Poll is one observing batch with the public selection/borrow contract;
 * unlike raw polling it retains first-open records until passive feedback.
 * The caller consumes/cleans every action before finish. No advancing call
 * on this sender is allowed while its batch borrows are outstanding. */
typedef struct moq_core_observed_open {
    uint64_t ref;
    uint64_t owner;
    uint8_t mode;
} moq_core_observed_open_t;

#define MOQ_CORE_VIRTUAL_OPENED 1u
#define MOQ_CORE_VIRTUAL_SUPPRESSED 2u

MOQ_API moq_result_t moq_core_internal_virtual_register(
    moq_session_t *s, const void *observer);
MOQ_API void moq_core_internal_virtual_unregister(
    moq_session_t *s, const void *observer);
/* cap==0 is a passive availability check, including native-owner priority. */
MOQ_API moq_result_t moq_core_internal_virtual_poll(
    moq_session_t *s, const void *observer, moq_action_t *actions,
    moq_core_observed_open_t *opens, size_t cap, size_t *out_count);
/* SUPPRESSED requires permanent cancellation/refusal of this stream ref:
 * the driver must never open it later, including for a FIN-only action.
 * A failed reservation with a still-live producer does not prove that fact;
 * leave it unresolved for EXTERNAL abort instead. */
MOQ_API bool moq_core_internal_virtual_resolve(
    moq_session_t *s, const void *observer,
    const moq_core_observed_open_t *open, uint8_t disposition);
/* abort_batch is only for hard driver failure after all transferred actions
 * were cleaned. Unresolved facts become EXTERNAL, never guessed suppressed.
 * A permanently cancelled, proven non-opening may be resolved SUPPRESSED
 * beforehand under the no-future-open requirement above. */
MOQ_API moq_result_t moq_core_internal_virtual_finish(
    moq_session_t *s, const void *observer, bool abort_batch);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CORE_SPI_H */
