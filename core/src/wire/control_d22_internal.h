#ifndef MOQ_WIRE_CONTROL_D22_INTERNAL_H
#define MOQ_WIRE_CONTROL_D22_INTERNAL_H

/* Draft-22 wire building blocks: envelope, location filter value, Setup
 * Option KVPs, SETUP and GOAWAY bodies. Internal; nothing here is installed
 * or exported. */
#include "moq/buf.h"
#include "moq/control.h"

#define MOQ_D22_SETUP UINT64_C(0x2f00)
#define MOQ_D22_GOAWAY UINT64_C(0x10)
#define MOQ_D22_MAX_URI 8192u
#define MOQ_D22_MAX_KVP 65535u
#define MOQ_D22_ERR_KVP_FORMAT (-200)

typedef enum moq_d22_filter_type {
    MOQ_D22_FILTER_NONE = 0,
    MOQ_D22_FILTER_RELATIVE = 1,
    MOQ_D22_FILTER_ABSOLUTE = 2,
    MOQ_D22_FILTER_GROUP_END = 3,
    MOQ_D22_FILTER_RANGE = 4,
    MOQ_D22_FILTER_NEXT_OBJECT = 5
} moq_d22_filter_type_t;

typedef struct moq_d22_location_filter {
    moq_d22_filter_type_t type;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group_delta;
    uint64_t end_object; /* inclusive; active only for RANGE */
} moq_d22_location_filter_t;

typedef struct moq_d22_kvp {
    uint64_t type;
    uint64_t integer; /* even type */
    moq_bytes_t bytes; /* odd type, borrows input */
} moq_d22_kvp_t;

typedef struct moq_d22_setup_options {
    bool has_hop_id;
    bool has_relay_cost;
    uint64_t hop_id;
    uint64_t relay_cost;
    bool has_path;
    bool has_authority;
    bool has_implementation;
    bool has_max_auth_token_cache_size;
    bool has_max_filter_ranges;
    bool has_max_request_updates;
    moq_bytes_t path;
    moq_bytes_t authority;
    moq_bytes_t implementation;
    uint64_t max_auth_token_cache_size;
    uint64_t max_filter_ranges;
    uint64_t max_request_updates; /* absent/zero means unlimited */
    size_t auth_token_count;
    /* Original, validated option list. Walk it with decode_kvp to consume
     * repeated tokens; no fixed token-array bound or hidden allocation. */
    moq_bytes_t encoded;
} moq_d22_setup_options_t;

typedef struct moq_d22_goaway {
    moq_bytes_t uri;
    uint64_t timeout_ms;
} moq_d22_goaway_t;

/* Cursor and output commit only on success. BUFFER means a streaming prefix
 * is incomplete; an unknown filter or malformed KVP is PROTO. Filter functions
 * encode/decode the VALUE (starting with its discriminator), not parameter 0x21.
 * Fields not selected by the discriminator are ignored on encode, zero on
 * decode. No reader or result owns its borrowed storage. */
moq_result_t moq_d22_decode_envelope(moq_buf_reader_t *r,
                                    moq_control_envelope_t *out);
moq_result_t moq_d22_decode_location_filter(moq_buf_reader_t *r,
                                    moq_d22_location_filter_t *out);
moq_result_t moq_d22_encode_location_filter(moq_buf_writer_t *w,
                                    const moq_d22_location_filter_t *filter);
uint64_t moq_d22_relative_start_group(uint64_t largest_group, uint64_t offset);
moq_result_t moq_d22_decode_kvp(moq_buf_reader_t *r, uint64_t *previous_type,
                               moq_d22_kvp_t *out);

/* SETUP takes a COMPLETE body (not its envelope); truncated fields are PROTO.
 * Malformed authorization Token values return ERR_KVP_FORMAT. SETUP role/URI
 * validation and alias cache side effects belong to the session, not codec.
 * Encode takes already type-sorted options, including any repeated tokens. */
moq_result_t moq_d22_decode_setup_options(const uint8_t *body, size_t len,
                                         moq_d22_setup_options_t *out);
moq_result_t moq_d22_encode_setup(moq_buf_writer_t *w,
                                 const moq_d22_kvp_t *options, size_t count);
moq_result_t moq_d22_decode_goaway(const uint8_t *body, size_t len,
                                 moq_d22_goaway_t *out);
moq_result_t moq_d22_encode_goaway(moq_buf_writer_t *w,
                                 const moq_d22_goaway_t *goaway);

#endif
