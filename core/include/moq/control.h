#ifndef MOQ_CONTROL_H
#define MOQ_CONTROL_H

/*
 * Stability: draft-specific wire/tooling API. This is NOT the
 * application-level session API; applications should normally include
 * <moq/moq.h> or <moq/session.h>. Wire/profile details may change across
 * drafts. (For the wire-codec gateway and audience, see <moq/codec.h>.)
 *
 * Draft-16 control message envelope and SETUP codec.
 *
 * Wire format (draft-ietf-moq-transport-16 section 9):
 *   MOQT Control Message {
 *     Message Type (i),        -- QUIC varint
 *     Message Length (16),     -- fixed uint16 big-endian
 *     Message Payload (..)
 *   }
 *
 * Note: the future session engine MUST close the session on unknown
 * control message types (spec line 2267). At the codec level, unknown
 * types decode successfully for tooling/diagnostics use.
 */

#include "export.h"
#include "types.h"
#include "buf.h"
#include "kvp.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -- Draft-16 control message type codes --------------------------- */
/* See spec Table 1 (line 2196-2265). */

#define MOQ_D16_CLIENT_SETUP         0x20u
#define MOQ_D16_SERVER_SETUP         0x21u
#define MOQ_D16_GOAWAY               0x10u
#define MOQ_D16_MAX_REQUEST_ID       0x15u
#define MOQ_D16_REQUESTS_BLOCKED     0x1Au
#define MOQ_D16_REQUEST_OK           0x07u
#define MOQ_D16_REQUEST_ERROR        0x05u
#define MOQ_D16_SUBSCRIBE            0x03u
#define MOQ_D16_SUBSCRIBE_OK         0x04u
#define MOQ_D16_REQUEST_UPDATE       0x02u
#define MOQ_D16_UNSUBSCRIBE          0x0Au
#define MOQ_D16_PUBLISH              0x1Du
#define MOQ_D16_PUBLISH_OK           0x1Eu
#define MOQ_D16_PUBLISH_DONE         0x0Bu
#define MOQ_D16_FETCH                0x16u
#define MOQ_D16_FETCH_OK             0x18u
#define MOQ_D16_FETCH_CANCEL         0x17u
#define MOQ_D16_TRACK_STATUS         0x0Du
#define MOQ_D16_PUBLISH_NAMESPACE    0x06u
#define MOQ_D16_NAMESPACE            0x08u
#define MOQ_D16_PUBLISH_NAMESPACE_DONE   0x09u
#define MOQ_D16_NAMESPACE_DONE       0x0Eu
#define MOQ_D16_PUBLISH_NAMESPACE_CANCEL 0x0Cu
#define MOQ_D16_SUBSCRIBE_NAMESPACE  0x11u

/* -- Setup parameter type codes ------------------------------------ */
/*
 * Draft-16 setup parameter codes (spec section 9.3.1). These are codec/
 * tooling constants, intentionally not included in the stable moq.h
 * umbrella except through <moq/codec.h>.
 */

#define MOQ_SETUP_PARAM_PATH                       0x01u
#define MOQ_SETUP_PARAM_MAX_REQUEST_ID             0x02u
#define MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN        0x03u
#define MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE  0x04u
#define MOQ_SETUP_PARAM_AUTHORITY                  0x05u
#define MOQ_SETUP_PARAM_MOQT_IMPLEMENTATION        0x07u

/* -- Control envelope ---------------------------------------------- */

typedef struct moq_control_envelope {
    uint64_t       msg_type;     /* QUIC varint decoded */
    uint16_t       payload_len;  /* from the uint16 length field */
    const uint8_t *payload;      /* borrowed from input buffer */
} moq_control_envelope_t;

/*
 * Decode a control message envelope from the reader.
 * On success, the reader is advanced past the full envelope.
 * payload points into the reader's underlying buffer (borrowed).
 *
 * Returns MOQ_ERR_BUFFER if truncated (does not advance), including
 * when payload_len exceeds the currently available bytes after the
 * length field. The session layer maps a complete malformed message
 * length to a PROTOCOL_VIOLATION close.
 */
MOQ_API moq_result_t moq_control_decode_envelope(moq_buf_reader_t *r,
                                                  moq_control_envelope_t *out);

/*
 * Encode a control message envelope. Writes the type varint and
 * the uint16 length, then copies payload_len bytes from payload.
 */
MOQ_API moq_result_t moq_control_encode_envelope(moq_buf_writer_t *w,
                                                  uint64_t msg_type,
                                                  const uint8_t *payload,
                                                  uint16_t payload_len);

/*
 * Write a control envelope header (type + length placeholder).
 * Returns the offset of the length field for later patching.
 * After writing the payload, call moq_buf_patch_uint16 with the
 * actual payload length.
 */
MOQ_API moq_result_t moq_control_write_header(moq_buf_writer_t *w,
                                               uint64_t msg_type,
                                               size_t *len_offset);

/* -- Draft-16 SETUP messages --------------------------------------- */
/*
 * Draft-16 CLIENT_SETUP (0x20) and SERVER_SETUP (0x21).
 *
 * Wire format (spec line 2749-2756, Figure 5):
 *   CLIENT_SETUP { Type=0x20, Length(16), NumParams(i), Params.. }
 *   SERVER_SETUP { Type=0x21, Length(16), NumParams(i), Params.. }
 *
 * Draft-16 does NOT carry version lists in SETUP messages.
 * Version negotiation is done via ALPN at the transport layer.
 *
 * Setup parameters use delta-encoded KVP (same as all draft-16 KVP).
 * Unknown setup parameters MUST be ignored (preserved in the decoded
 * params array for relay forwarding).
 */

typedef struct moq_d16_setup {
    moq_kvp_entry_t *params;      /* caller-provided array, filled on decode */
    size_t           params_count; /* number of decoded entries */
    size_t           params_cap;   /* capacity of the params array */
} moq_d16_setup_t;

/*
 * Decode a CLIENT_SETUP payload (after envelope has been stripped).
 * The payload bytes are the envelope's payload field.
 *
 * params array is caller-provided; params_cap is its capacity.
 * On success, params_count is set to the number of decoded entries.
 * Decoded entries borrow from the payload buffer.
 *
 * Returns MOQ_ERR_BUFFER if params_cap is insufficient.
 * Returns MOQ_ERR_PROTO on malformed payload, including trailing bytes
 * after the declared number of parameters.
 */
MOQ_API moq_result_t moq_d16_decode_client_setup(const uint8_t *payload,
                                                   size_t payload_len,
                                                   moq_d16_setup_t *out);

/*
 * Decode a SERVER_SETUP payload. Same structure as CLIENT_SETUP
 * in draft-16 (no selected version field).
 */
MOQ_API moq_result_t moq_d16_decode_server_setup(const uint8_t *payload,
                                                   size_t payload_len,
                                                   moq_d16_setup_t *out);

/*
 * Encode a CLIENT_SETUP as a complete control message (envelope + payload).
 */
MOQ_API moq_result_t moq_d16_encode_client_setup(moq_buf_writer_t *w,
                                                   const moq_kvp_entry_t *params,
                                                   size_t params_count);

/*
 * Encode a SERVER_SETUP as a complete control message.
 */
MOQ_API moq_result_t moq_d16_encode_server_setup(moq_buf_writer_t *w,
                                                   const moq_kvp_entry_t *params,
                                                   size_t params_count);

/* -- Simple scalar control messages -------------------------------- */

/*
 * Messages with a single varint field (Request ID or Max Request ID).
 * Payload: Request_ID (i)  — no other fields.
 *
 * Used by: UNSUBSCRIBE, FETCH_CANCEL, PUBLISH_NAMESPACE_DONE,
 *          MAX_REQUEST_ID, REQUESTS_BLOCKED.
 *
 * The encoder rejects msg_type values outside that set.
 */
MOQ_API moq_result_t moq_d16_decode_varint_msg(const uint8_t *payload,
                                                size_t payload_len,
                                                uint64_t *out_value);

MOQ_API moq_result_t moq_d16_encode_varint_msg(moq_buf_writer_t *w,
                                                uint64_t msg_type,
                                                uint64_t value);

/* -- GOAWAY -------------------------------------------------------- */
/*
 * Payload: New Session URI Length (i) + New Session URI (..)
 * URI max 8192 bytes. Zero-length URI means reuse current URI.
 */

typedef struct moq_d16_goaway {
    const uint8_t *uri;     /* borrowed from payload; NULL if len == 0 */
    size_t         uri_len;
} moq_d16_goaway_t;

MOQ_API moq_result_t moq_d16_decode_goaway(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_d16_goaway_t *out);

MOQ_API moq_result_t moq_d16_encode_goaway(moq_buf_writer_t *w,
                                            const uint8_t *uri,
                                            size_t uri_len);

/* -- PUBLISH_NAMESPACE_CANCEL -------------------------------------- */
/*
 * Payload: Request ID (i) + Error Code (i) + Reason Phrase
 * Reason Phrase: Length (i) + UTF-8 bytes (max 1024 bytes).
 */

typedef struct moq_d16_publish_namespace_cancel {
    uint64_t       request_id;
    uint64_t       error_code;
    const uint8_t *reason;      /* borrowed; may be NULL if len == 0 */
    size_t         reason_len;
} moq_d16_publish_namespace_cancel_t;

MOQ_API moq_result_t moq_d16_decode_publish_namespace_cancel(
    const uint8_t *payload, size_t payload_len,
    moq_d16_publish_namespace_cancel_t *out);

MOQ_API moq_result_t moq_d16_encode_publish_namespace_cancel(
    moq_buf_writer_t *w, uint64_t request_id,
    uint64_t error_code, const uint8_t *reason, size_t reason_len);

/* -- REQUEST_OK ---------------------------------------------------- */
/*
 * Payload: Request ID (i) + Number of Parameters (i) + Parameters..
 * Parameters use delta-encoded KVP. Unknown params are preserved
 * (the session layer later decides policy).
 */

typedef struct moq_d16_request_ok {
    uint64_t         request_id;
    moq_kvp_entry_t *params;        /* caller-provided array */
    size_t           params_count;
    size_t           params_cap;
} moq_d16_request_ok_t;

MOQ_API moq_result_t moq_d16_decode_request_ok(const uint8_t *payload,
                                                size_t payload_len,
                                                moq_d16_request_ok_t *out);

MOQ_API moq_result_t moq_d16_encode_request_ok(moq_buf_writer_t *w,
                                                uint64_t request_id,
                                                const moq_kvp_entry_t *params,
                                                size_t params_count);

/* -- REQUEST_ERROR ------------------------------------------------- */
/*
 * Payload: Request ID (i) + Error Code (i) + Retry Interval (i)
 *          + Reason Phrase { Length (i) + Value (..) }
 * Reason max 1024 bytes.
 */

typedef struct moq_d16_request_error {
    uint64_t       request_id;
    uint64_t       error_code;
    uint64_t       retry_interval;
    const uint8_t *reason;          /* borrowed; NULL if len == 0 */
    size_t         reason_len;
} moq_d16_request_error_t;

MOQ_API moq_result_t moq_d16_decode_request_error(const uint8_t *payload,
                                                   size_t payload_len,
                                                   moq_d16_request_error_t *out);

MOQ_API moq_result_t moq_d16_encode_request_error(moq_buf_writer_t *w,
                                                   uint64_t request_id,
                                                   uint64_t error_code,
                                                   uint64_t retry_interval,
                                                   const uint8_t *reason,
                                                   size_t reason_len);

/* -- REQUEST_UPDATE ------------------------------------------------- */

typedef struct moq_d16_request_update {
    uint64_t         request_id;
    uint64_t         existing_request_id;
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
} moq_d16_request_update_t;

MOQ_API moq_result_t moq_d16_decode_request_update(
    const uint8_t *payload, size_t payload_len,
    moq_d16_request_update_t *out);

MOQ_API moq_result_t moq_d16_encode_request_update(
    moq_buf_writer_t *w,
    uint64_t request_id,
    uint64_t existing_request_id,
    const moq_kvp_entry_t *params,
    size_t params_count);

/* -- Draft-16 message parameter type codes ------------------------- */
/* These are version-specific (unlike setup params). §9.2.2.            */
/* Unknown message params MUST close with PROTOCOL_VIOLATION (§9.2).    */

#define MOQ_MSG_PARAM_AUTHORIZATION_TOKEN   0x03u
#define MOQ_MSG_PARAM_DELIVERY_TIMEOUT      0x02u
#define MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY   0x20u
#define MOQ_MSG_PARAM_GROUP_ORDER           0x22u
#define MOQ_MSG_PARAM_SUBSCRIPTION_FILTER   0x21u
#define MOQ_MSG_PARAM_FORWARD               0x10u
#define MOQ_MSG_PARAM_NEW_GROUP_REQUEST     0x32u
#define MOQ_MSG_PARAM_EXPIRES               0x08u
#define MOQ_MSG_PARAM_LARGEST_OBJECT        0x09u

/* -- Draft-16 SUBSCRIBE (0x03) ------------------------------------- */

typedef struct moq_d16_subscribe {
    uint64_t         request_id;
    moq_namespace_t  track_namespace;   /* parts borrowed from payload */
    moq_bytes_t      track_name;        /* borrowed from payload */
    moq_kvp_entry_t *params;            /* caller-provided array */
    size_t           params_count;
    size_t           params_cap;
} moq_d16_subscribe_t;

MOQ_API moq_result_t moq_d16_decode_subscribe(const uint8_t *payload,
                                               size_t payload_len,
                                               moq_bytes_t *ns_parts,
                                               size_t ns_parts_cap,
                                               moq_d16_subscribe_t *out);

MOQ_API moq_result_t moq_d16_encode_subscribe(moq_buf_writer_t *w,
                                               uint64_t request_id,
                                               const moq_namespace_t *ns,
                                               moq_bytes_t track_name,
                                               const moq_kvp_entry_t *params,
                                               size_t params_count);

/* -- Draft-16 PUBLISH_NAMESPACE (0x06) ----------------------------- */

typedef struct moq_d16_publish_namespace {
    uint64_t         request_id;
    moq_namespace_t  track_namespace;   /* parts borrowed from payload */
    moq_kvp_entry_t *params;            /* caller-provided array */
    size_t           params_count;
    size_t           params_cap;
} moq_d16_publish_namespace_t;

MOQ_API moq_result_t moq_d16_decode_publish_namespace(const uint8_t *payload,
                                                       size_t payload_len,
                                                       moq_bytes_t *ns_parts,
                                                       size_t ns_parts_cap,
                                                       moq_d16_publish_namespace_t *out);

MOQ_API moq_result_t moq_d16_encode_publish_namespace(moq_buf_writer_t *w,
                                                       uint64_t request_id,
                                                       const moq_namespace_t *ns,
                                                       const moq_kvp_entry_t *params,
                                                       size_t params_count);

/* -- Draft-16 SUBSCRIBE_OK (0x04) ---------------------------------- */

typedef struct moq_d16_subscribe_ok {
    uint64_t         request_id;
    uint64_t         track_alias;
    moq_kvp_entry_t *params;            /* caller-provided array */
    size_t           params_count;
    size_t           params_cap;
    const uint8_t   *track_extensions;  /* borrowed; raw bytes after params */
    size_t           track_extensions_len;
} moq_d16_subscribe_ok_t;

MOQ_API moq_result_t moq_d16_decode_subscribe_ok(const uint8_t *payload,
                                                  size_t payload_len,
                                                  moq_d16_subscribe_ok_t *out);

MOQ_API moq_result_t moq_d16_encode_subscribe_ok(moq_buf_writer_t *w,
                                                  uint64_t request_id,
                                                  uint64_t track_alias,
                                                  const moq_kvp_entry_t *params,
                                                  size_t params_count,
                                                  const uint8_t *track_extensions,
                                                  size_t track_extensions_len);

/* -- Message parameter helpers ------------------------------------- */
/*
 * Decode/encode helpers for typed message parameter values.
 * These operate on the raw value bytes from a moq_kvp_entry_t.
 */

/* Subscription Filter (§5.1.2, param 0x21). */
typedef struct moq_d16_subscription_filter {
    uint32_t filter_type;   /* 0x1-0x4 */
    uint64_t start_group;   /* AbsoluteStart / AbsoluteRange */
    uint64_t start_object;  /* AbsoluteStart / AbsoluteRange */
    uint64_t end_group;     /* AbsoluteRange only */
} moq_d16_subscription_filter_t;

MOQ_API moq_result_t moq_d16_decode_subscription_filter(
    const uint8_t *value, size_t value_len,
    moq_d16_subscription_filter_t *out);

MOQ_API moq_result_t moq_d16_encode_subscription_filter(
    uint8_t *buf, size_t cap, size_t *out_len,
    const moq_d16_subscription_filter_t *filter);

/* Largest Object / Location (§9.2.2.7, param 0x09). */
typedef struct moq_d16_location {
    uint64_t group;
    uint64_t object;
} moq_d16_location_t;

MOQ_API moq_result_t moq_d16_decode_location(
    const uint8_t *value, size_t value_len,
    moq_d16_location_t *out);

/* Simple varint parameter helpers with range validation. */
MOQ_API moq_result_t moq_d16_decode_param_forward(
    const uint8_t *value, size_t value_len, bool *out);

MOQ_API moq_result_t moq_d16_decode_param_subscriber_priority(
    const uint8_t *value, size_t value_len, uint8_t *out);

MOQ_API moq_result_t moq_d16_decode_param_group_order(
    const uint8_t *value, size_t value_len, uint8_t *out);

MOQ_API moq_result_t moq_d16_decode_param_expires(
    const uint8_t *value, size_t value_len, uint64_t *out);

/* -- Draft-16 Subgroup Header (§10.4.2) ---------------------------- */
/*
 * Type field: 0b00X1XXXX. Bit meanings:
 *   0x01  EXTENSIONS — Extensions length field present on all objects;
 *                     objects with no extension headers encode length 0
 *   0x06  SUBGROUP_ID_MODE (2 bits): 00=absent/0, 01=first-obj, 10=present
 *   0x08  END_OF_GROUP — FIN implies last object in group
 *   0x20  DEFAULT_PRIORITY — priority omitted
 *
 * Valid: 0x10..0x15, 0x18..0x1D, 0x30..0x35, 0x38..0x3D
 * Invalid (mode 0b11): 0x16,0x17,0x1E,0x1F,0x36,0x37,0x3E,0x3F
 */

#define MOQ_SUBGROUP_BIT_EXTENSIONS      0x01u
#define MOQ_SUBGROUP_MASK_ID_MODE        0x06u
#define MOQ_SUBGROUP_BIT_END_OF_GROUP    0x08u
#define MOQ_SUBGROUP_BIT_DEFAULT_PRIORITY 0x20u

#define MOQ_SUBGROUP_ID_MODE_ZERO        0u
#define MOQ_SUBGROUP_ID_MODE_FIRST_OBJ   1u
#define MOQ_SUBGROUP_ID_MODE_PRESENT     2u

typedef struct moq_d16_subgroup_header {
    uint8_t  type;                /* raw type byte */
    bool     has_extensions;
    uint8_t  subgroup_id_mode;    /* 0, 1, or 2 */
    bool     end_of_group;
    bool     default_priority;
    uint64_t track_alias;
    uint64_t group_id;
    uint64_t subgroup_id;         /* wire-present only when mode == PRESENT.
                                     FIRST_OBJ mode: resolved by session
                                     after decoding the first object ID. */
    uint8_t  publisher_priority;  /* valid when !default_priority */
} moq_d16_subgroup_header_t;

MOQ_API moq_result_t moq_d16_decode_subgroup_header(
    moq_buf_reader_t *r,
    moq_d16_subgroup_header_t *out);

MOQ_API moq_result_t moq_d16_encode_subgroup_header(
    moq_buf_writer_t *w,
    const moq_d16_subgroup_header_t *hdr);

MOQ_API bool moq_d16_subgroup_type_valid(uint8_t type);

/* -- Draft-16 Subgroup Object Fields (§10.4.2, Figure 29) ---------- */

#define MOQ_OBJECT_STATUS_NORMAL       0x0u
#define MOQ_OBJECT_STATUS_END_OF_GROUP 0x3u
#define MOQ_OBJECT_STATUS_END_OF_TRACK 0x4u

typedef struct moq_d16_object_fields {
    uint64_t       object_id_delta;
    uint64_t       payload_len;
    bool           has_status;      /* true when payload_len == 0 */
    uint8_t        status;          /* valid when has_status */
    const uint8_t *extensions;      /* borrowed; raw extension KVP bytes */
    size_t         extensions_len;
    const uint8_t *payload;         /* borrowed from reader; NULL if len==0 */
} moq_d16_object_fields_t;

/*
 * Decode object fields from a subgroup stream.
 * Reader must be positioned after the subgroup header.
 * When has_extensions is true, extension bytes are preserved as a
 * raw borrowed span (not interpreted). Payload borrows from reader.
 */
MOQ_API moq_result_t moq_d16_decode_object_fields(
    moq_buf_reader_t *r,
    bool has_extensions,
    moq_d16_object_fields_t *out);

/*
 * Encode object framing header only (delta + payload_len + status).
 * Does NOT write payload bytes. For SEND_DATA inline header[32].
 * Zero-length Normal: encodes explicit status 0x0.
 * No extensions in first slice.
 */
MOQ_API moq_result_t moq_d16_encode_object_header(
    moq_buf_writer_t *w,
    uint64_t object_id_delta,
    uint64_t payload_len);

/*
 * Encode complete object fields including payload (for tests/tools).
 * For zero-length Normal: encodes explicit status 0x0.
 * No extensions.
 */
MOQ_API moq_result_t moq_d16_encode_object_fields(
    moq_buf_writer_t *w,
    uint64_t object_id_delta,
    uint64_t payload_len,
    const uint8_t *payload);

/* -- Draft-16 Authorization Token structure (§9.2.2.1) ------------ */

#define MOQ_AUTH_TOKEN_DELETE    0u
#define MOQ_AUTH_TOKEN_REGISTER  1u
#define MOQ_AUTH_TOKEN_USE_ALIAS 2u
#define MOQ_AUTH_TOKEN_USE_VALUE 3u

typedef struct moq_d16_auth_token {
    uint32_t       alias_type;
    uint64_t       alias;           /* valid for DELETE, REGISTER, USE_ALIAS */
    uint64_t       token_type;      /* valid for REGISTER, USE_VALUE */
    const uint8_t *token_value;     /* borrowed; valid for REGISTER, USE_VALUE */
    size_t         token_value_len;
} moq_d16_auth_token_t;

/*
 * Decode one Token structure from the value bytes of an AUTH_TOKEN
 * KVP parameter entry.
 * Returns MOQ_ERR_PROTO on malformed structure (caller should map
 * to KEY_VALUE_FORMATTING_ERROR session close code 0x06).
 */
MOQ_API moq_result_t moq_d16_auth_token_decode(const uint8_t *value,
                                                size_t value_len,
                                                moq_d16_auth_token_t *out);

MOQ_API moq_result_t moq_d16_auth_token_encode(moq_buf_writer_t *w,
                                                const moq_d16_auth_token_t *tok);

/* -- Draft-16 FETCH (0x16) ----------------------------------------- */
/*
 * D16 FETCH type codes for the fetch_type field.
 */
#define MOQ_D16_FETCH_TYPE_STANDALONE     0x01u
#define MOQ_D16_FETCH_TYPE_RELATIVE_JOIN  0x02u
#define MOQ_D16_FETCH_TYPE_ABSOLUTE_JOIN  0x03u

/* Fetch object serialization flags. */
#define MOQ_D16_FETCH_FLAG_SUBGROUP_MASK  0x03u
#define MOQ_D16_FETCH_FLAG_OBJECT_ID     0x04u
#define MOQ_D16_FETCH_FLAG_GROUP_ID      0x08u
#define MOQ_D16_FETCH_FLAG_PRIORITY      0x10u
#define MOQ_D16_FETCH_FLAG_EXTENSIONS    0x20u
#define MOQ_D16_FETCH_FLAG_DATAGRAM      0x40u

/* End-of-range markers. */
#define MOQ_D16_FETCH_END_NON_EXISTENT   0x8Cu
#define MOQ_D16_FETCH_END_UNKNOWN        0x10Cu

/* FETCH_HEADER stream type (raw varint on uni stream). */
#define MOQ_D16_FETCH_HEADER_TYPE    0x05u

typedef struct moq_d16_fetch {
    uint64_t         request_id;
    uint32_t         fetch_type;    /* 1, 2, or 3 */
    /* Standalone (fetch_type == 1): */
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_name;
    uint64_t         start_group;
    uint64_t         start_object;
    uint64_t         end_group;
    uint64_t         end_object;
    /* Joining (fetch_type == 2 or 3): */
    uint64_t         joining_request_id;
    uint64_t         joining_start;
    /* Params: */
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
} moq_d16_fetch_t;

typedef struct moq_d16_fetch_ok {
    uint64_t         request_id;
    uint8_t          end_of_track;   /* 0 or 1 */
    uint64_t         end_group;
    uint64_t         end_object;
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
    const uint8_t   *track_extensions;
    size_t           track_extensions_len;
} moq_d16_fetch_ok_t;

typedef struct moq_d16_fetch_header {
    uint64_t request_id;
} moq_d16_fetch_header_t;

/*
 * End-of-range markers carry group/object from wire plus inherited
 * subgroup/priority from prev, so the struct is usable as prev for
 * the next object's delta decoding without separate bookkeeping.
 */
typedef struct moq_d16_fetch_object {
    bool     is_end_of_range;
    uint32_t range_kind;           /* MOQ_D16_FETCH_END_NON_EXISTENT or _UNKNOWN */
    uint64_t group_id;
    uint64_t subgroup_id;
    uint64_t object_id;
    uint8_t  publisher_priority;
    const uint8_t *extensions;     /* borrowed */
    size_t   extensions_len;
    uint64_t payload_len;
    const uint8_t *payload;        /* borrowed */
} moq_d16_fetch_object_t;

/* FETCH */
MOQ_API moq_result_t moq_d16_decode_fetch(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_bytes_t *ns_parts,
                                            size_t ns_parts_cap,
                                            moq_d16_fetch_t *out);

MOQ_API moq_result_t moq_d16_encode_fetch(moq_buf_writer_t *w,
                                            const moq_d16_fetch_t *fetch,
                                            const moq_kvp_entry_t *params,
                                            size_t params_count);

/* FETCH_OK */
MOQ_API moq_result_t moq_d16_decode_fetch_ok(const uint8_t *payload,
                                               size_t payload_len,
                                               moq_d16_fetch_ok_t *out);

MOQ_API moq_result_t moq_d16_encode_fetch_ok(moq_buf_writer_t *w,
                                               const moq_d16_fetch_ok_t *ok);

/* FETCH_CANCEL — covered by moq_d16_encode_varint_msg / decode_varint_msg */

/* FETCH_HEADER (raw varints on uni stream, not a control envelope) */
MOQ_API moq_result_t moq_d16_decode_fetch_header(moq_buf_reader_t *r,
                                                   moq_d16_fetch_header_t *out);

MOQ_API moq_result_t moq_d16_encode_fetch_header(moq_buf_writer_t *w,
                                                   uint64_t request_id);

/* Fetch Object Fields */
MOQ_API moq_result_t moq_d16_decode_fetch_object(moq_buf_reader_t *r,
                                                   const moq_d16_fetch_object_t *prev,
                                                   moq_d16_fetch_object_t *out);

MOQ_API moq_result_t moq_d16_encode_fetch_object(moq_buf_writer_t *w,
                                                   const moq_d16_fetch_object_t *obj,
                                                   const moq_d16_fetch_object_t *prev);

/* End-of-range markers */
MOQ_API moq_result_t moq_d16_encode_fetch_end_of_range(moq_buf_writer_t *w,
                                                         uint32_t range_kind,
                                                         uint64_t group_id,
                                                         uint64_t object_id);

/* -- Draft-16 PUBLISH (0x1D) --------------------------------------- */

typedef struct moq_d16_publish {
    uint64_t         request_id;
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_name;
    uint64_t         track_alias;
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
    const uint8_t   *track_extensions;
    size_t           track_extensions_len;
} moq_d16_publish_t;

typedef struct moq_d16_publish_ok {
    uint64_t         request_id;
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
} moq_d16_publish_ok_t;

typedef struct moq_d16_publish_done {
    uint64_t       request_id;
    uint64_t       status_code;
    uint64_t       stream_count;
    const uint8_t *reason;
    size_t         reason_len;
} moq_d16_publish_done_t;

MOQ_API moq_result_t moq_d16_decode_publish(const uint8_t *payload, size_t payload_len,
    moq_bytes_t *ns_parts, size_t ns_parts_cap, moq_d16_publish_t *out);
MOQ_API moq_result_t moq_d16_encode_publish(moq_buf_writer_t *w,
    const moq_d16_publish_t *pub);

MOQ_API moq_result_t moq_d16_decode_publish_ok(const uint8_t *payload, size_t payload_len,
    moq_d16_publish_ok_t *out);
MOQ_API moq_result_t moq_d16_encode_publish_ok(moq_buf_writer_t *w,
    uint64_t request_id, const moq_kvp_entry_t *params, size_t params_count);

MOQ_API moq_result_t moq_d16_decode_publish_done(const uint8_t *payload, size_t payload_len,
    moq_d16_publish_done_t *out);
MOQ_API moq_result_t moq_d16_encode_publish_done(moq_buf_writer_t *w,
    uint64_t request_id, uint64_t status_code, uint64_t stream_count,
    const uint8_t *reason, size_t reason_len);

/* -- Draft-16 SUBSCRIBE_NAMESPACE (0x11) --------------------------- */

typedef struct moq_d16_subscribe_namespace {
    uint64_t         request_id;
    moq_namespace_t  track_namespace_prefix; /* parts borrowed from payload */
    uint64_t         subscribe_options;      /* 0=PUBLISH, 1=NAMESPACE, 2=BOTH */
    moq_kvp_entry_t *params;
    size_t           params_count;
    size_t           params_cap;
} moq_d16_subscribe_namespace_t;

MOQ_API moq_result_t moq_d16_decode_subscribe_namespace(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *ns_parts, size_t ns_parts_cap,
    moq_d16_subscribe_namespace_t *out);

MOQ_API moq_result_t moq_d16_encode_subscribe_namespace(
    moq_buf_writer_t *w,
    uint64_t request_id,
    const moq_namespace_t *prefix,
    uint64_t subscribe_options,
    const moq_kvp_entry_t *params,
    size_t params_count);

/* -- Draft-16 TRACK_STATUS (0x0D) ---------------------------------- */
/*
 * TRACK_STATUS has the same wire format as SUBSCRIBE (Section 9.19):
 * Request ID, Track Namespace, Track Name, Parameters.
 * Subscriber delivery params (priority, filter) are not included.
 * The response is REQUEST_OK (with SUBSCRIBE_OK-style params) or
 * REQUEST_ERROR.
 */
typedef moq_d16_subscribe_t moq_d16_track_status_t;

MOQ_API moq_result_t moq_d16_decode_track_status(const uint8_t *payload,
                                                   size_t payload_len,
                                                   moq_bytes_t *ns_parts,
                                                   size_t ns_parts_cap,
                                                   moq_d16_track_status_t *out);

MOQ_API moq_result_t moq_d16_encode_track_status(moq_buf_writer_t *w,
                                                   uint64_t request_id,
                                                   const moq_namespace_t *ns,
                                                   moq_bytes_t track_name,
                                                   const moq_kvp_entry_t *params,
                                                   size_t params_count);

/* -- Draft-16 OBJECT_DATAGRAM (§10.3.1) ----------------------------- */

#define MOQ_D16_DGRAM_FLAG_EXTENSIONS     0x01u
#define MOQ_D16_DGRAM_FLAG_END_OF_GROUP   0x02u
#define MOQ_D16_DGRAM_FLAG_ZERO_OBJECT_ID 0x04u
#define MOQ_D16_DGRAM_FLAG_DEFAULT_PRIO   0x08u
#define MOQ_D16_DGRAM_FLAG_STATUS         0x20u

typedef struct moq_d16_object_datagram {
    uint64_t       track_alias;
    uint64_t       group_id;
    uint64_t       object_id;
    uint8_t        publisher_priority;
    bool           has_extensions;
    const uint8_t *extensions;      /* borrowed from input */
    size_t         extensions_len;
    bool           is_status;
    uint64_t       object_status;
    bool           end_of_group;
    bool           default_priority;
    const uint8_t *payload;         /* borrowed; remainder of datagram */
    size_t         payload_len;
} moq_d16_object_datagram_t;

MOQ_API moq_result_t moq_d16_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d16_object_datagram_t *out);

MOQ_API moq_result_t moq_d16_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d16_object_datagram_t *dg);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CONTROL_H */
