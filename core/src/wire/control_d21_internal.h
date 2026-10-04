#ifndef MOQ_WIRE_CONTROL_D21_INTERNAL_H
#define MOQ_WIRE_CONTROL_D21_INTERNAL_H

/* TRANSITIONAL: see the banner in moq/control_d21.h. */

/*
 * Internal draft-18 control-codec helpers -- NOT part of the public wire codec
 * surface (no MOQ_API, so hidden from the shared-library dynamic symbol table).
 * Shared between the wire codec (control_d21.c) and the session profile
 * (profile_d18.c) without widening the public ABI.
 */

#include "moq/control_d21.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Pure per-profile timeout scanner (§9.8, internal): extracts
 * OBJECT/SUBGROUP delivery-timeout Track Properties (raw wire ms),
 * searching the mutable list and IMMUTABLE_PROPERTIES contents; duplicates
 * and nested immutable blocks are MOQ_ERR_PROTO; unknown properties pass
 * through. */
moq_result_t moq_d21_scan_delivery_timeouts(const uint8_t *props, size_t len,
                                            bool *out_has_object,
                                            uint64_t *out_object_ms,
                                            bool *out_has_subgroup,
                                            uint64_t *out_subgroup_ms);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WIRE_CONTROL_D21_INTERNAL_H */
