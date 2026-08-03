#ifndef MOQ_WIRE_CONTROL_D18_INTERNAL_H
#define MOQ_WIRE_CONTROL_D18_INTERNAL_H

/*
 * Internal draft-18 control-codec helpers -- NOT part of the public wire codec
 * surface (no MOQ_API, so hidden from the shared-library dynamic symbol table).
 * Shared between the wire codec (control_d18.c) and the session profile
 * (profile_d18.c) without widening the public ABI.
 */

#include "moq/control_d18.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Pure per-profile timeout scanner (§9.8, internal): extracts
 * OBJECT/SUBGROUP delivery-timeout Track Properties (raw wire ms),
 * searching the mutable list and IMMUTABLE_PROPERTIES contents; duplicates
 * and nested immutable blocks are MOQ_ERR_PROTO; unknown properties pass
 * through. */
moq_result_t moq_d18_scan_delivery_timeouts(const uint8_t *props, size_t len,
                                            bool *out_has_object,
                                            uint64_t *out_object_ms,
                                            bool *out_has_subgroup,
                                            uint64_t *out_subgroup_ms);

/*
 * REQUEST_UPDATE_OK codec (§10.5): the REQUEST_OK form that carries
 * LARGEST_OBJECT / EXPIRES response parameters with empty Track Properties.
 * The zero-parameter moq_d18_{encode,decode}_request_ok in the public header
 * stay for the other REQUEST_OK responses.
 */
moq_result_t moq_d18_encode_request_update_ok(moq_buf_writer_t *w,
                                              const moq_d18_msg_params_t *p);
moq_result_t moq_d18_decode_request_update_ok(const uint8_t *payload,
                                              size_t payload_len,
                                              moq_d18_msg_params_t *out);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WIRE_CONTROL_D18_INTERNAL_H */
