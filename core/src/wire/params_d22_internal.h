#ifndef MOQ_WIRE_PARAMS_D22_INTERNAL_H
#define MOQ_WIRE_PARAMS_D22_INTERNAL_H

#include "control_d22_internal.h"

/* Private error domain: a request refusal, not a session protocol error. */
#define MOQ_D22_ERR_INVALID_FILTER (-201)
#define MOQ_D22_HOP_PATH UINT64_C(0x40B57)
#define MOQ_D22_ROUTE_COST UINT64_C(0x40B58)
#define MOQ_D22_PARAM_PRESENT(type) (UINT64_C(1) << (type))

typedef enum moq_d22_param_scope {
    MOQ_D22_SCOPE_SUBSCRIBE,
    MOQ_D22_SCOPE_PUBLISH,
    MOQ_D22_SCOPE_FETCH,
    MOQ_D22_SCOPE_TRACK_STATUS,
    MOQ_D22_SCOPE_PUBLISH_NAMESPACE,
    MOQ_D22_SCOPE_SUBSCRIBE_NAMESPACE,
    MOQ_D22_SCOPE_SUBSCRIBE_TRACKS,
    MOQ_D22_SCOPE_SUBSCRIBE_OK,
    MOQ_D22_SCOPE_PUBLISH_OK,
    MOQ_D22_SCOPE_UPDATE_OK,
    MOQ_D22_SCOPE_TRACK_STATUS_OK,
    MOQ_D22_SCOPE_NAMESPACE_OK,
    MOQ_D22_SCOPE_FETCH_OK,
    MOQ_D22_SCOPE_NOTIFY,
    MOQ_D22_SCOPE_UPDATE_SUBSCRIBER,
    MOQ_D22_SCOPE_UPDATE_PUBLISHER,
    MOQ_D22_SCOPE_UPDATE_FETCH,
    MOQ_D22_SCOPE_UPDATE_PUBLISH_NAMESPACE,
    MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_NAMESPACE,
    MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_TRACKS,
    MOQ_D22_SCOPE_FILL,
    MOQ_D22_SCOPE_NAMESPACE
} moq_d22_param_scope_t;

typedef struct moq_d22_location {
    uint64_t group;
    uint64_t object;
} moq_d22_location_t;

/* encoded includes the field count; fields excludes it. Both borrow input. */
typedef struct moq_d22_namespace {
    moq_bytes_t encoded;
    moq_bytes_t fields;
    size_t count;
    size_t value_bytes;
} moq_d22_namespace_t;

typedef struct moq_d22_token {
    uint64_t alias_type;
    uint64_t alias;
    uint64_t type;
    moq_bytes_t value;
} moq_d22_token_t;

typedef struct moq_d22_range {
    uint64_t start;
    uint64_t end;
    bool open_end;
} moq_d22_range_t;

typedef struct moq_d22_range_filter {
    bool clear;
    uint8_t set_id;
    uint64_t property_type; /* active for 0x28/0x29 only */
    moq_bytes_t ranges; /* delta-encoded pairs; use range_next */
    uint64_t count;
} moq_d22_range_filter_t;

typedef struct moq_d22_param {
    uint64_t type;
    const uint64_t *hop_ids; /* encode only: semantic path, otherwise encoded span */
    size_t hop_count;
    union {
        uint64_t integer; /* also uint8 parameters; checked on encode */
        moq_d22_location_t location;
        moq_d22_location_filter_t filter;
        moq_d22_token_t token;
        moq_d22_range_filter_t ranges;
        moq_d22_namespace_t ns;
        moq_bytes_t hop_path; /* validated, encoded vi64 sequence */
        moq_bytes_t fill; /* the length-prefixed value: count, then parameters */
    } value;
} moq_d22_param_t;

/* All values, including repeated auth/range filters, remain accessible via
 * param_next. Presence is independent of zero/None/empty fill. No allocation
 * or caller-sized arrays: neither tokens nor ranges can silently truncate. */
typedef struct moq_d22_params {
    moq_bytes_t encoded;
    uint64_t count;
    uint64_t present;
    uint64_t range_count;
    bool has_hop_path;
    bool has_route_cost;
} moq_d22_params_t;

moq_result_t moq_d22_validate_hop_path(moq_bytes_t path);

/* Streaming functions conserve cursor, previous value and output on failure.
 * A truncated streaming prefix is BUFFER. Bounded token/range/fill interiors
 * are complete values: malformed interiors are PROTO (tokens KVP_FORMAT).
 * Param iteration validates one value; decode_params additionally checks
 * scope/duplicates/limits across the sequence. INVALID_FILTER is deferred
 * until the whole requested sequence passes syntax/scope checks, but does not
 * validate bytes beyond that sequence. Token cache effects and update's
 * retained range budget belong to the session. max_filter_ranges applies
 * independently to a fill scope. */
moq_result_t moq_d22_decode_namespace(moq_buf_reader_t *r,
                                    moq_d22_namespace_t *out);
moq_result_t moq_d22_encode_namespace(moq_buf_writer_t *w,
                                    const moq_namespace_t *ns);
moq_result_t moq_d22_range_next(moq_buf_reader_t *r, uint64_t *previous_end,
                              moq_d22_range_t *out);
moq_result_t moq_d22_param_next(moq_buf_reader_t *r, uint64_t *previous_type,
                              moq_d22_param_t *out);
/* Only after scan_params has validated the complete sequence. Allows an
 * already-classified INVALID_FILTER while projecting non-range parameters. */
moq_result_t moq_d22_param_next_scanned(moq_buf_reader_t *r, uint64_t *previous_type,
                                      moq_d22_param_t *out);
moq_result_t moq_d22_decode_params(moq_buf_reader_t *r, uint64_t count,
                                 moq_d22_param_scope_t scope,
                                 uint64_t max_filter_ranges,
                                 moq_d22_params_t *out);
/* Private composition primitive for complete-body validation. On OK publishes
 * the fully parsed sequence and whether it contains request-invalid filters;
 * on fatal/incomplete input ALL outputs remain unchanged. The enclosing
 * decoder must finish its body/property checks before returning INVALID_FILTER.
 * This is not an authorization/cache admission decision. */
moq_result_t moq_d22_scan_params(moq_buf_reader_t *r, uint64_t count,
                               moq_d22_param_scope_t scope,
                               uint64_t max_filter_ranges,
                               moq_d22_params_t *out, bool *invalid_filter);
/* COMPLETE fill value: Number of Parameters (vi64) then exactly that many
 * Parameters filling the value (Section 9.20.15 "as if they were Parameters
 * for a separate message"); a fresh scope and delta base; recursive fills
 * are forbidden. out->encoded spans the parameters after the count. */
moq_result_t moq_d22_decode_fill(moq_bytes_t body, uint64_t max_filter_ranges,
                               moq_d22_params_t *out);
/* Writes the parameter sequence only, without a count or byte-length prefix.
 * Input is sorted by type. Fills/ranges/namespaces borrow validated encodings;
 * scalar values and tokens are semantic. Borrowed input must not overlap the
 * output buffer. Encoder commits cursor only on OK. */
moq_result_t moq_d22_encode_params(moq_buf_writer_t *w,
                                 const moq_d22_param_t *params, size_t count,
                                 moq_d22_param_scope_t scope,
                                 uint64_t max_filter_ranges);

#endif
