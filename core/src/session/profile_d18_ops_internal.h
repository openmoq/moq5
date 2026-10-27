#ifndef MOQ_SESSION_PROFILE_D18_OPS_INTERNAL_H
#define MOQ_SESSION_PROFILE_D18_OPS_INTERNAL_H

#include "session_internal.h"

/* The draft-18 profile operations that draft-22 shares unchanged: the data
 * plane (identical framing on both drafts), stream classification, and the
 * request-capacity and unsubscribe stubs of the request-stream model. They
 * are named here so another profile's operation table can be a constant
 * initializer that refers to them directly. Internal to the core. */

moq_uni_class_t moq_d18_op_classify_uni_stream(const uint8_t *data, size_t len);
uint32_t moq_d18_op_classify_data_stream(const uint8_t *data, size_t len);
moq_result_t moq_d18_op_classify_bidi_stream(moq_session_t *s,
                                             moq_stream_ref_t ref,
                                             const uint8_t *data,
                                             size_t len);
moq_result_t moq_d18_op_scan_delivery_timeouts(const uint8_t *data,
                                               size_t len,
                                               bool strict_local,
                                               moq_dt_scan_t *out);
bool moq_d18_op_track_properties_dynamic_groups(const uint8_t *data,
                                                size_t len);
moq_result_t moq_d18_op_track_properties_add_dynamic_groups(const uint8_t *in,
                                                            size_t len,
                                                            uint8_t *out,
                                                            size_t cap,
                                                            size_t *out_len);
uint8_t moq_d18_op_track_properties_group_order(const uint8_t *data,
                                                size_t len);
moq_result_t moq_d18_op_track_properties_add_object_timeout(
    const uint8_t *in, size_t len, uint64_t ms, uint8_t *out, size_t cap,
    size_t *out_len);
moq_result_t moq_d18_op_track_properties_add_subgroup_timeout(
    const uint8_t *in, size_t len, uint64_t ms, uint8_t *out, size_t cap,
    size_t *out_len);
moq_result_t moq_d18_op_track_properties_add_group_order(const uint8_t *in,
                                                         size_t len,
                                                         uint8_t order,
                                                         uint8_t *out,
                                                         size_t cap,
                                                         size_t *out_len);
bool moq_d18_op_track_properties_max_cache(const uint8_t *data, size_t len,
                                           uint64_t *out_ms);
moq_result_t moq_d18_op_decode_subgroup_header(
    moq_session_t *s, moq_buf_reader_t *r, moq_decoded_subgroup_header_t *out);
moq_result_t moq_d18_op_encode_subgroup_header(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_subgroup_header_encode_args_t *args);
moq_result_t moq_d18_op_encode_object_header(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_object_header_encode_args_t *args);
moq_result_t moq_d18_op_encode_object_payload_prefix(moq_session_t *s,
                                                     moq_buf_writer_t *w,
                                                     uint64_t payload_len,
                                                     bool with_status);
moq_result_t moq_d18_op_scan_object_gaps(const uint8_t *props, size_t len,
                                         moq_obj_gaps_t *out);
moq_result_t moq_d18_op_add_object_gap(const uint8_t *in, size_t len,
                                       bool object_gap, uint64_t value,
                                       uint8_t *out, size_t cap, size_t *out_len);
moq_result_t moq_d18_op_validate_object_properties(moq_session_t *s,
                                                   const uint8_t *props,
                                                   size_t len);
moq_result_t moq_d18_op_decode_object_header(
    moq_session_t *s, moq_buf_reader_t *r, bool has_extensions,
    uint64_t prev_object_id, bool has_prev_object,
    moq_decoded_object_header_t *out);
moq_result_t moq_d18_op_encode_object_datagram(
    moq_session_t *s, moq_buf_writer_t *w,
    const struct moq_datagram_encode_args *args);
moq_result_t moq_d18_op_decode_object_datagram(
    moq_session_t *s, const uint8_t *data, size_t len,
    struct moq_decoded_object_datagram *out);
moq_result_t moq_d18_op_decode_fetch_header(
    moq_session_t *s, moq_buf_reader_t *r,
    moq_decoded_fetch_stream_header_t *out);
moq_result_t moq_d18_op_encode_fetch_header(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id);
moq_result_t moq_d18_op_encode_fetch_object(
    moq_session_t *s, moq_buf_writer_t *w,
    moq_group_order_t effective_order,
    const moq_fetch_object_encode_args_t *args,
    const moq_fetch_prior_object_t *prev);
moq_result_t moq_d18_op_encode_unsubscribe(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id);
moq_result_t moq_d18_op_grant_capacity(moq_session_t *s,
                                       uint64_t new_capacity,
                                       uint64_t now_us);
uint64_t moq_d18_op_request_capacity(const moq_session_t *s);

#endif
