#ifndef MOQ_WIRE_CONTROL_D16_INTERNAL_H
#define MOQ_WIRE_CONTROL_D16_INTERNAL_H

/*
 * Internal draft-16 control-codec helpers -- NOT part of the public wire codec
 * surface (no MOQ_API, so hidden from the shared-library dynamic symbol table).
 * Shared between the wire codec (control_d16.c) and the session profile
 * (profile_d16.c) without widening the public ABI.
 */

#include "moq/control.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Pure per-profile timeout scanner (§9.8, internal, draft-16): extracts the
 * DELIVERY TIMEOUT Track Extension (varint ms). Wire 0 is MOQ_ERR_PROTO in
 * every mode (§11.1). DUPLICATES: strict (local emission) refuses to emit
 * one (MOQ_ERR_PROTO); lenient (inbound) keeps the FIRST value and ignores
 * repeats -- repeatability is an unfinished d16 registry item, not a
 * documented violation. Structural failure: strict -> MOQ_ERR_PROTO,
 * lenient -> stop, nothing extracted. */
moq_result_t moq_d16_scan_delivery_timeout_ext(const uint8_t *ext, size_t len,
                                               bool strict,
                                               bool *out_has,
                                               uint64_t *out_ms);

/* DEFAULT_PUBLISHER_GROUP_ORDER Track Extension (draft-16 11.1.1.2). */
#define MOQ_D16_EXT_GROUP_ORDER 0x22u

/* *out_group_order: 0 if omitted, else 1 (ascending) / 2 (descending). A
 * value outside 1..2 is MOQ_ERR_PROTO in every mode; duplicates and bad
 * structure only in strict mode, as in the timeout scanner. */
moq_result_t moq_d16_scan_group_order_ext(const uint8_t *ext, size_t len,
                                          bool strict,
                                          uint8_t *out_group_order);

/* Copy `in` to `out` with the even `type` = `value` inserted in order. `type`
 * must not already be present. */
moq_result_t moq_d16_track_ext_put_varint(const uint8_t *in, size_t in_len,
                                          uint64_t type, uint64_t value,
                                          uint8_t *out, size_t out_cap,
                                          size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WIRE_CONTROL_D16_INTERNAL_H */
