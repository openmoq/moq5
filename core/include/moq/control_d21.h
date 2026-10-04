#ifndef MOQ_CONTROL_D21_H
#define MOQ_CONTROL_D21_H

/*
 * TRANSITIONAL (draft-21 conversion in progress, see
 * docs/draft21-implementation-plan.md Task 4).
 *
 * This header began as a mechanical copy of control_d18.h. Message families
 * are converted to draft 21 one at a time, each with tests that assert the
 * draft-21 bytes and cite the draft-21 section. Until a family is converted its
 * declarations still describe DRAFT-18 behavior, and any comment that cites a
 * draft-18 section number is stale. The authoritative draft-21 layouts are in
 * docs/draft21-wire-reference.md.
 */

/*
 * Draft-18 control-message wire codec (tooling tier, not the application API).
 *
 * Draft-18 frames each control message as Type (vi64) + Length (16-bit) +
 * payload, and carries control on a unidirectional stream that begins with a
 * SETUP message. This header currently covers the pieces needed to bring up
 * the control-channel pair (SETUP); request/data messages are added as the
 * corresponding implementation is added.
 */

#include "export.h"
#include "types.h"
#include "buf.h"
#include "control.h"   /* moq_control_envelope_t (encoding-agnostic) */

#ifdef __cplusplus
extern "C" {
#endif

/* Unidirectional stream types (draft-18 §3.4). */
#define MOQ_D21_STREAM_SETUP        ((uint64_t)0x2F00u)
#define MOQ_D21_STREAM_PADDING      ((uint64_t)0x132B3E28u)
#define MOQ_D21_STREAM_FETCH_HEADER ((uint64_t)0x05u)

/*
 * Decode a draft-18 control-message envelope: Type (vi64) + Length (16-bit) +
 * payload. On success the reader is advanced past the full envelope and *out
 * borrows the payload from the reader's buffer. Returns MOQ_ERR_BUFFER when the
 * envelope (including the declared payload length) is not yet fully available.
 */
MOQ_API moq_result_t moq_d21_decode_envelope(moq_buf_reader_t *r,
                                              moq_control_envelope_t *out);

/*
 * Encode a draft-18 SETUP message with no Setup Options: Type (vi64) = 0x2F00,
 * Length (16) = 0. (This helper does not encode any Setup Options.)
 */
MOQ_API moq_result_t moq_d21_encode_setup(moq_buf_writer_t *w);

/* Setup Option types (draft-21 section 9.1). Options are vi64 Key-Value-Pairs
 * (section 8.3): delta-encoded type; odd types carry a vi64 Length + bytes
 * (at most 2^16-1), even types a single vi64 value. Unlike Message Parameters,
 * unknown Setup Options are structurally skippable and MUST be ignored (9.1),
 * and duplicates of unknown options are allowed. */
#define MOQ_D21_SETUP_OPT_PATH                      ((uint64_t)0x01u)  /* bytes, 9.1.2 */
#define MOQ_D21_SETUP_OPT_AUTHORIZATION_TOKEN       ((uint64_t)0x03u)  /* token; may repeat, 9.1.4 */
#define MOQ_D21_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE ((uint64_t)0x04u)  /* varint bytes, 9.1.3 */
#define MOQ_D21_SETUP_OPT_AUTHORITY                 ((uint64_t)0x05u)  /* bytes, 9.1.1 */
#define MOQ_D21_SETUP_OPT_MAX_FILTER_RANGES         ((uint64_t)0x06u)  /* varint, 9.1.6 */
#define MOQ_D21_SETUP_OPT_MOQT_IMPLEMENTATION       ((uint64_t)0x07u)  /* bytes, 9.1.5 */
#define MOQ_D21_SETUP_OPT_MAX_REQUEST_UPDATES       ((uint64_t)0x08u)  /* varint, 9.1.7 */

/* Reason Phrase maximum length (draft-18 §1.4.4). */
#define MOQ_D21_MAX_REASON 1024u

/* Maximum Full Track Name length: sum of Track Namespace Field lengths plus
 * the Track Name length (draft-18 §2.4.1). */
#define MOQ_D21_MAX_FULL_TRACK 4096u

/* Control-message type codes (draft-18 §10). */
#define MOQ_D21_SUBSCRIBE         ((uint64_t)0x03u)
#define MOQ_D21_REQUEST_UPDATE    ((uint64_t)0x02u)
#define MOQ_D21_SUBSCRIBE_OK      ((uint64_t)0x04u)
#define MOQ_D21_REQUEST_OK        ((uint64_t)0x07u)
#define MOQ_D21_REQUEST_ERROR     ((uint64_t)0x05u)
#define MOQ_D21_PUBLISH_NAMESPACE ((uint64_t)0x06u)
#define MOQ_D21_NAMESPACE         ((uint64_t)0x08u)
#define MOQ_D21_NAMESPACE_DONE    ((uint64_t)0x0Eu)
#define MOQ_D21_TRACK_STATUS      ((uint64_t)0x0Du)
#define MOQ_D21_SUBSCRIBE_NAMESPACE ((uint64_t)0x50u)
#define MOQ_D21_SUBSCRIBE_TRACKS  ((uint64_t)0x51u)
#define MOQ_D21_PUBLISH_SKIPPED   ((uint64_t)0x0Fu)
#define MOQ_D21_PUBLISH_STATE_NOTIFY ((uint64_t)0x22u)
#define MOQ_D21_PUBLISH           ((uint64_t)0x1Du)
#define MOQ_D21_GOAWAY            ((uint64_t)0x10u)

/* A draft-18 decode result distinct from the generic PROTOCOL_VIOLATION: an
 * AUTHORIZATION_TOKEN whose Token structure cannot be decoded must close the
 * session with KEY_VALUE_FORMATTING_ERROR (0x06, §10.2.2) rather than 0x03. It
 * is a negative moq_result_t-compatible value outside the shared error range, so
 * callers that only test `rc < 0` are unaffected; the D18 profile maps it to the
 * 0x06 close code. */
#define MOQ_D21_ERR_KVP_FORMAT (-200)

/* Message Parameter types (draft-21 section 9.20 and 16.7). */
#define MOQ_D21_PARAM_OBJECT_DELIVERY_TIMEOUT   ((uint64_t)0x02u)  /* varint ms, 9.20.5 */
#define MOQ_D21_PARAM_AUTHORIZATION_TOKEN       ((uint64_t)0x03u)  /* len-prefixed; may repeat, 9.20.3 */
#define MOQ_D21_PARAM_RENDEZVOUS_TIMEOUT        ((uint64_t)0x04u)  /* varint ms, 9.20.7 */
#define MOQ_D21_PARAM_SUBGROUP_DELIVERY_TIMEOUT ((uint64_t)0x06u)  /* varint ms, 9.20.4 */
#define MOQ_D21_PARAM_EXPIRES                   ((uint64_t)0x08u)  /* varint ms, 9.20.17 */
#define MOQ_D21_PARAM_LARGEST_OBJECT            ((uint64_t)0x09u)  /* Location, 9.20.18 */
#define MOQ_D21_PARAM_FILL_TIMEOUT              ((uint64_t)0x0Au)  /* varint ms, 9.20.6 */
#define MOQ_D21_PARAM_FORWARD                   ((uint64_t)0x10u)  /* uint8 0/1, 9.20.19 */
#define MOQ_D21_PARAM_SUBSCRIBER_PRIORITY       ((uint64_t)0x20u)  /* uint8, 9.20.8 */
#define MOQ_D21_PARAM_LOCATION_FILTER           ((uint64_t)0x21u)  /* len-prefixed, 9.20.10 */
#define MOQ_D21_PARAM_GROUP_ORDER               ((uint64_t)0x22u)  /* uint8 1/2, 9.20.9 */
#define MOQ_D21_PARAM_FILL_PARAMETERS           ((uint64_t)0x23u)  /* len-prefixed, nested, 9.20.16 */
#define MOQ_D21_PARAM_SUBGROUP_FILTER           ((uint64_t)0x25u)  /* Range Filter, 9.20.11 */
#define MOQ_D21_PARAM_OBJECTID_FILTER           ((uint64_t)0x26u)  /* Range Filter, 9.20.12 */
#define MOQ_D21_PARAM_PRIORITY_FILTER           ((uint64_t)0x27u)  /* Range Filter, 9.20.13 */
#define MOQ_D21_PARAM_OBJECT_PROPERTY_FILTER    ((uint64_t)0x28u)  /* Range Filter, 9.20.14 */
#define MOQ_D21_PARAM_TRACK_PROPERTY_FILTER     ((uint64_t)0x29u)  /* Range Filter, 9.20.15 */
#define MOQ_D21_PARAM_NEW_GROUP_REQUEST         ((uint64_t)0x32u)  /* varint, 9.20.20 */
#define MOQ_D21_PARAM_TRACK_NAMESPACE_PREFIX    ((uint64_t)0x34u)  /* namespace, 9.20.21 */
#define MOQ_D21_PARAM_INCLUDE_PROPERTIES        ((uint64_t)0x35u)  /* uint8 0/1, 9.20.22 */

/* One bit per parameter type, for the per-message legality masks below. A
 * defined parameter in a message that does not allow it is a PROTOCOL_VIOLATION
 * (9.20.1); an unknown type is too (9.20: parameters cannot be skipped). */
#define MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT   (1u << 0)
#define MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN       (1u << 1)
#define MOQ_D21_PARAM_BIT_RENDEZVOUS_TIMEOUT        (1u << 2)
#define MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT (1u << 3)
#define MOQ_D21_PARAM_BIT_EXPIRES                   (1u << 4)
#define MOQ_D21_PARAM_BIT_LARGEST_OBJECT            (1u << 5)
#define MOQ_D21_PARAM_BIT_FILL_TIMEOUT              (1u << 6)
#define MOQ_D21_PARAM_BIT_FORWARD                   (1u << 7)
#define MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY       (1u << 8)
#define MOQ_D21_PARAM_BIT_LOCATION_FILTER           (1u << 9)
#define MOQ_D21_PARAM_BIT_GROUP_ORDER               (1u << 10)
#define MOQ_D21_PARAM_BIT_FILL_PARAMETERS           (1u << 11)
#define MOQ_D21_PARAM_BIT_SUBGROUP_FILTER           (1u << 12)
#define MOQ_D21_PARAM_BIT_OBJECTID_FILTER           (1u << 13)
#define MOQ_D21_PARAM_BIT_PRIORITY_FILTER           (1u << 14)
#define MOQ_D21_PARAM_BIT_OBJECT_PROPERTY_FILTER    (1u << 15)
#define MOQ_D21_PARAM_BIT_TRACK_PROPERTY_FILTER     (1u << 16)
#define MOQ_D21_PARAM_BIT_NEW_GROUP_REQUEST         (1u << 17)
#define MOQ_D21_PARAM_BIT_TRACK_NAMESPACE_PREFIX    (1u << 18)
#define MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES        (1u << 19)

/* The four Range Filters that FETCH, SUBSCRIBE and REQUEST_UPDATE accept. */
#define MOQ_D21_PARAM_BITS_RANGE_FILTERS \
    (MOQ_D21_PARAM_BIT_SUBGROUP_FILTER | MOQ_D21_PARAM_BIT_OBJECTID_FILTER | \
     MOQ_D21_PARAM_BIT_PRIORITY_FILTER | MOQ_D21_PARAM_BIT_OBJECT_PROPERTY_FILTER)

/*
 * Which parameters each message permits, built from each parameter's own "MAY
 * appear in" sentence in 9.20.3 to 9.20.22: the draft's table in 9.20.2 is empty
 * in the checked-in text (docs/draft21-wire-reference.md, open ambiguity A3).
 *
 * REQUEST_OK is generic on the wire, so the caller picks the mask for the
 * request it answers. TRACK_STATUS_OK admits EXPIRES as well as LARGEST_OBJECT:
 * 9.13 says it carries the parameters a SUBSCRIBE_OK would, while 9.20.17 does
 * not list it (open ambiguity A4); the permissive reading cannot close a
 * session on a conforming peer.
 */
#define MOQ_D21_MASK_SUBSCRIBE \
    (MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN | \
     MOQ_D21_PARAM_BIT_RENDEZVOUS_TIMEOUT | MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | \
     MOQ_D21_PARAM_BIT_FORWARD | MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY | \
     MOQ_D21_PARAM_BIT_LOCATION_FILTER | MOQ_D21_PARAM_BIT_GROUP_ORDER | \
     MOQ_D21_PARAM_BIT_FILL_PARAMETERS | MOQ_D21_PARAM_BITS_RANGE_FILTERS | \
     MOQ_D21_PARAM_BIT_NEW_GROUP_REQUEST | MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES)
/* 9.18.1: any parameter valid on a subscription is valid on SUBSCRIBE_TRACKS,
 * which also takes the Track Property filter (9.20.15). */
#define MOQ_D21_MASK_SUBSCRIBE_TRACKS \
    (MOQ_D21_MASK_SUBSCRIBE | MOQ_D21_PARAM_BIT_TRACK_PROPERTY_FILTER)
/* The union over every request type an update can target; the session checks
 * the parameter against the actual target. GROUP_ORDER and INCLUDE_PROPERTIES
 * are not allowed (9.20.9, 9.20.22). */
#define MOQ_D21_MASK_REQUEST_UPDATE \
    (MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN | \
     MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | MOQ_D21_PARAM_BIT_FORWARD | \
     MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY | MOQ_D21_PARAM_BIT_LOCATION_FILTER | \
     MOQ_D21_PARAM_BIT_FILL_PARAMETERS | MOQ_D21_PARAM_BITS_RANGE_FILTERS | \
     MOQ_D21_PARAM_BIT_TRACK_PROPERTY_FILTER | MOQ_D21_PARAM_BIT_NEW_GROUP_REQUEST | \
     MOQ_D21_PARAM_BIT_TRACK_NAMESPACE_PREFIX)
#define MOQ_D21_MASK_PUBLISH \
    (MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN | \
     MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | MOQ_D21_PARAM_BIT_EXPIRES | \
     MOQ_D21_PARAM_BIT_LARGEST_OBJECT | MOQ_D21_PARAM_BIT_FORWARD | \
     MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY | MOQ_D21_PARAM_BIT_LOCATION_FILTER | \
     MOQ_D21_PARAM_BIT_GROUP_ORDER)
#define MOQ_D21_MASK_FETCH \
    (MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN | MOQ_D21_PARAM_BIT_FILL_TIMEOUT | \
     MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY | MOQ_D21_PARAM_BIT_LOCATION_FILTER | \
     MOQ_D21_PARAM_BIT_GROUP_ORDER | MOQ_D21_PARAM_BITS_RANGE_FILTERS | \
     MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES)
#define MOQ_D21_MASK_TRACK_STATUS \
    (MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN | MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES)
#define MOQ_D21_MASK_NAMESPACE_REQUEST   MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN
#define MOQ_D21_MASK_SUBSCRIBE_OK \
    (MOQ_D21_PARAM_BIT_EXPIRES | MOQ_D21_PARAM_BIT_LARGEST_OBJECT)
#define MOQ_D21_MASK_FETCH_OK            0u   /* no parameter is defined for FETCH_OK */
#define MOQ_D21_MASK_PUBLISH_STATE_NOTIFY \
    (MOQ_D21_PARAM_BIT_LOCATION_FILTER | MOQ_D21_PARAM_BIT_FORWARD | \
     MOQ_D21_PARAM_BIT_LARGEST_OBJECT)
/* REQUEST_OK forms (9.3): the shorthand names the request it answers. */
#define MOQ_D21_MASK_PUBLISH_OK          MOQ_D21_PARAM_BIT_EXPIRES
#define MOQ_D21_MASK_REQUEST_UPDATE_OK \
    (MOQ_D21_PARAM_BIT_EXPIRES | MOQ_D21_PARAM_BIT_LARGEST_OBJECT)
#define MOQ_D21_MASK_TRACK_STATUS_OK \
    (MOQ_D21_PARAM_BIT_EXPIRES | MOQ_D21_PARAM_BIT_LARGEST_OBJECT)
#define MOQ_D21_MASK_NAMESPACE_OK        MOQ_D21_PARAM_BIT_EXPIRES
/* What FILL_PARAMETERS may contain (9.20.16); anything else, including a nested
 * FILL_PARAMETERS, is a PROTOCOL_VIOLATION. Track Property filters are excluded. */
#define MOQ_D21_MASK_FILL_NESTED \
    (MOQ_D21_PARAM_BIT_FILL_TIMEOUT | MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY | \
     MOQ_D21_PARAM_BIT_LOCATION_FILTER | MOQ_D21_PARAM_BIT_GROUP_ORDER | \
     MOQ_D21_PARAM_BITS_RANGE_FILTERS)

/* Maximum AUTHORIZATION_TOKEN parameters carried per message. Matches the
 * session-core decoded-token cap (MOQ_DECODED_MAX_TOKENS); the profile
 * static-asserts the relation. */
#define MOQ_D21_MAX_AUTH_TOKENS 16

/* One AUTHORIZATION_TOKEN Token structure (§10.2.2), decoded from / encoded to
 * the parameter's length-prefixed value. Integers are vi64; token_value borrows
 * from the decode buffer (REGISTER / USE_VALUE only). */
typedef struct moq_d21_auth_token {
    uint32_t    alias_type;   /* 0 DELETE, 1 REGISTER, 2 USE_ALIAS, 3 USE_VALUE */
    uint64_t    alias;        /* DELETE / REGISTER / USE_ALIAS */
    uint64_t    token_type;   /* REGISTER / USE_VALUE */
    moq_bytes_t token_value;  /* REGISTER / USE_VALUE; borrowed */
} moq_d21_auth_token_t;

/* Decoded SETUP Options (draft-21 9.1). PATH/AUTHORITY surface as presence
 * flags (mirroring the draft-16 setup-parameter handling); token values and the
 * implementation string borrow from the payload. Unknown options are skipped.
 *
 * Absent options leave their value 0, which is also the draft default for each:
 * MAX_AUTH_TOKEN_CACHE_SIZE 0 (no aliases, 9.1.3), MAX_FILTER_RANGES 0 (the peer
 * MUST NOT send Range Filters, 9.1.6) and MAX_REQUEST_UPDATES 0 (unlimited,
 * 9.1.7). The has_ flags only say whether the peer sent the option. */
typedef struct moq_d21_setup_opts {
    bool     has_path;
    bool     has_authority;
    bool     has_max_auth_token_cache_size;
    uint64_t max_auth_token_cache_size;
    bool     has_max_filter_ranges;
    uint64_t max_filter_ranges;
    bool     has_max_request_updates;
    uint64_t max_request_updates;
    bool        has_implementation;
    moq_bytes_t implementation;   /* MOQT_IMPLEMENTATION; borrowed, not NUL-terminated */
    size_t               auth_token_count;
    moq_d21_auth_token_t auth_tokens[MOQ_D21_MAX_AUTH_TOKENS];
} moq_d21_setup_opts_t;

/* Decode the SETUP payload (the bytes after the 16-bit Length) as a vi64 KVP
 * Setup Options walk: unknown options (and duplicates of unknown options) are
 * skipped; a duplicate known non-repeatable option, an over-cap value length
 * (> 2^16-1), a truncated value, or a Delta Type overflow is MOQ_ERR_PROTO; a
 * malformed AUTHORIZATION_TOKEN structure is MOQ_D21_ERR_KVP_FORMAT (close
 * 0x6, KEY_VALUE_FORMATTING_ERROR). */
MOQ_API moq_result_t moq_d21_decode_setup_opts(const uint8_t *payload, size_t len,
                                               moq_d21_setup_opts_t *out);

/* Encode a SETUP message carrying the given options (NULL opts == no options),
 * in ascending type order with delta types. Emitted: MAX_AUTH_TOKEN_CACHE_SIZE,
 * MAX_FILTER_RANGES, MOQT_IMPLEMENTATION and MAX_REQUEST_UPDATES.
 * PATH/AUTHORITY/token emission are rejected with MOQ_ERR_INVAL until a public
 * surface supplies them, as is an implementation string over 2^16-1 bytes; on
 * any failure the writer is left exactly as it was. */
MOQ_API moq_result_t moq_d21_encode_setup_opts(moq_buf_writer_t *w,
                                               const moq_d21_setup_opts_t *opts);

/*
 * A Location Filter (9.20.10 and 3.3.1). The wire value is a vi64 byte Length
 * followed by 0 to 4 optional vi64 fields in a fixed order; field_count says how
 * many were present, so the session can apply the draft's rules for a relative
 * start (one field), the Next Object (two zero fields), or an absolute range.
 * field_count 0 is a zero-length filter: "no filter", which removes the filter
 * in a REQUEST_UPDATE and, inside FILL_PARAMETERS, means the whole track up to
 * Largest Object (3.4). end_group_delta is the raw delta from start_group; the
 * decoder rejects a sum above 2^64-1 (PROTOCOL_VIOLATION).
 */
typedef struct moq_d21_location_filter {
    uint8_t  field_count;       /* 0..4 */
    uint64_t start_group;       /* field_count >= 1 */
    uint64_t start_object;      /* field_count >= 2 */
    uint64_t end_group_delta;   /* field_count >= 3 */
    uint64_t end_object;        /* field_count == 4 */
} moq_d21_location_filter_t;

/* The parameters FILL_PARAMETERS may carry (9.20.16): the fill range, ordering
 * and priority overrides for a fill fetch stream. A parameter left unset takes
 * its value from the enclosing subscription. */
typedef struct moq_d21_fill_params {
    bool     has_fill_timeout;
    uint64_t fill_timeout_ms;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_group_order;
    uint8_t  group_order;        /* 1 ascending, 2 descending */
    bool     has_location_filter;
    moq_d21_location_filter_t location_filter;
    /* Range Filters are parsed for structure but never acted on; see the
     * range_filter_* fields of moq_d21_msg_params_t. */
    uint32_t range_filter_params;
    uint64_t range_filter_ranges;
    bool     range_filter_invalid;
} moq_d21_fill_params_t;

/*
 * Decoded Message Parameters (draft-21 9.20). Only the representable subset is
 * carried; an unknown or not-permitted parameter type fails the decode
 * (PROTOCOL_VIOLATION). AUTHORIZATION_TOKEN may repeat (8.9), so it is carried as
 * an array.
 *
 * Range Filters (3.3.2, 9.20.11 to 9.20.15) are parsed far enough to find their
 * end and count their Ranges, then discarded: this implementation does not
 * advertise MAX_FILTER_RANGES, so the peer MUST NOT send any (9.1.6) and the
 * session answers REQUEST_ERROR INVALID_FILTER when range_filter_ranges exceeds
 * the limit it advertised. range_filter_invalid is set for the conditions the
 * draft also answers with INVALID_FILTER: a decoded value that overflows 2^64-1,
 * a Priority Filter value above 255, or an odd Property Type in the Object or
 * Track Property filter. Those are request errors, not session errors, so the
 * decode itself still succeeds.
 */
typedef struct moq_d21_msg_params {
    bool     has_forward;
    uint8_t  forward;                /* 0 or 1 */
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_group_order;
    uint8_t  group_order;            /* 1 ascending, 2 descending */
    bool     has_location_filter;
    moq_d21_location_filter_t location_filter;
    bool     has_expires;            /* response parameter */
    uint64_t expires_ms;
    bool     has_largest;            /* response parameter */
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_object_delivery_timeout;
    uint64_t object_delivery_timeout_ms;
    bool     has_subgroup_delivery_timeout;
    uint64_t subgroup_delivery_timeout_ms;
    bool     has_rendezvous_timeout;
    uint64_t rendezvous_timeout_ms;
    bool     has_fill_timeout;       /* FETCH only; FILL_PARAMETERS carries its own */
    uint64_t fill_timeout_ms;
    size_t               auth_token_count;
    moq_d21_auth_token_t auth_tokens[MOQ_D21_MAX_AUTH_TOKENS];
    bool     has_new_group_request;  /* the value 0 ("no group info") is meaningful */
    uint64_t new_group_request;
    bool     has_track_namespace_prefix;   /* validated, not surfaced (9.20.21) */
    bool     has_include_properties;
    uint8_t  include_properties;     /* 0 or 1; absent means 1 (9.20.22) */
    bool                  has_fill;  /* FILL_PARAMETERS present (9.20.16) */
    moq_d21_fill_params_t fill;
    uint32_t range_filter_params;    /* Range Filter parameters present */
    uint64_t range_filter_ranges;    /* total Ranges across them */
    bool     range_filter_invalid;   /* see above */
} moq_d21_msg_params_t;

/* Encode a parameter block: a vi64 count followed by the set parameters in
 * ascending Type-Delta order (0x02, 0x03 (repeatable), 0x04, 0x06, 0x08, 0x09,
 * 0x0A, 0x10, 0x20, 0x21, 0x22, 0x23, 0x32, 0x35). Refuses (MOQ_ERR_INVAL, writer
 * unchanged) a value the wire cannot carry or a field this codec never emits:
 * Range Filters and TRACK_NAMESPACE_PREFIX. */
MOQ_API moq_result_t moq_d21_encode_msg_params(moq_buf_writer_t *w,
                                               const moq_d21_msg_params_t *p);

/* Decode `count` parameters from the reader into `out`. Enforces ascending
 * Type-Delta order and rejects duplicates (only AUTHORIZATION_TOKEN and the Range
 * Filters may repeat), unknown types, any type not in `allowed_mask`, and
 * out-of-range values for the enumerated parameters (PROTO). A truncated value
 * is MOQ_ERR_BUFFER at this level and MOQ_ERR_PROTO inside a nested block. */
MOQ_API moq_result_t moq_d21_decode_msg_params(moq_buf_reader_t *r,
                                               uint64_t count,
                                               uint32_t allowed_mask,
                                               moq_d21_msg_params_t *out);

/*
 * SUBSCRIBE (draft-18 §10.7). Priority/forward/group-order/filter are carried as
 * Message Parameters; `params` supplies the ones to emit (and receives the
 * decoded ones). Unknown or non-SUBSCRIBE parameters fail the decode. Namespace
 * parts and track name borrow from the decoded buffer.
 */
typedef struct moq_d21_subscribe {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    moq_d21_msg_params_t params;
} moq_d21_subscribe_t;

MOQ_API moq_result_t moq_d21_encode_subscribe(moq_buf_writer_t *w,
                                              uint64_t request_id,
                                              const moq_namespace_t *ns,
                                              moq_bytes_t track_name,
                                              const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_subscribe(const uint8_t *payload,
                                              size_t payload_len,
                                              moq_bytes_t *parts,
                                              size_t max_parts,
                                              moq_d21_subscribe_t *out);

/*
 * PUBLISH_NAMESPACE (draft-18 §10.15): Request ID + Track Namespace (0..32
 * fields) + Message Parameters. Only AUTHORIZATION_TOKEN is permitted; the
 * response is REQUEST_OK / REQUEST_ERROR on the bidi (no dedicated OK message).
 * Namespace parts borrow from the decoded buffer.
 */
typedef struct moq_d21_publish_namespace {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_d21_msg_params_t params;
} moq_d21_publish_namespace_t;

MOQ_API moq_result_t moq_d21_encode_publish_namespace(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *ns,
    const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_publish_namespace(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d21_publish_namespace_t *out);

/*
 * SUBSCRIBE_NAMESPACE (draft-18 §10.18): Request ID + Track Namespace Prefix
 * (0..32 fields) + Message Parameters. It is namespace-only in draft-18 (the
 * old interest field split into SUBSCRIBE_TRACKS), and only AUTHORIZATION_TOKEN
 * is permitted. The response is REQUEST_OK / REQUEST_ERROR on the bidi, then a
 * stream of NAMESPACE / NAMESPACE_DONE messages. Namespace parts borrow from the
 * decoded buffer.
 */
typedef struct moq_d21_subscribe_namespace {
    uint64_t             request_id;
    moq_namespace_t      track_namespace_prefix;
    moq_d21_msg_params_t params;
} moq_d21_subscribe_namespace_t;

MOQ_API moq_result_t moq_d21_encode_subscribe_namespace(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *prefix,
    const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_subscribe_namespace(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d21_subscribe_namespace_t *out);

/*
 * SUBSCRIBE_TRACKS (draft-18 §10.19): Request ID + Track Namespace Prefix
 * (0..32 fields) + Message Parameters. Requests PUBLISH messages for all tracks
 * under the prefix (and future ones). FORWARD (§10.2.12) and AUTHORIZATION_TOKEN
 * (§10.2.2) are the only permitted parameters; the response is REQUEST_OK /
 * REQUEST_ERROR on the bidi, then a stream of PUBLISH_SKIPPED messages while the
 * subscription is established (the resulting PUBLISH messages travel on separate
 * bidi streams). The overlap space is independent of SUBSCRIBE_NAMESPACE.
 * Namespace parts borrow from the decoded buffer.
 */
typedef struct moq_d21_subscribe_tracks {
    uint64_t             request_id;
    moq_namespace_t      track_namespace_prefix;
    moq_d21_msg_params_t params;
} moq_d21_subscribe_tracks_t;

MOQ_API moq_result_t moq_d21_encode_subscribe_tracks(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *prefix,
    const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_subscribe_tracks(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d21_subscribe_tracks_t *out);

/*
 * PUBLISH_SKIPPED (draft-21 9.19, renamed from draft 18's PUBLISH_BLOCKED; the
 * bytes are unchanged): Track Namespace Suffix (0..32 fields) + Track Name. Sent by the publisher on a SUBSCRIBE_TRACKS response stream to
 * signal it cannot open a PUBLISH for a matching track; it correlates by that
 * stream, so it carries no Request ID. The suffix is relative to the request's
 * Track Namespace Prefix. Suffix parts and track name borrow from the buffer.
 */
typedef struct moq_d21_publish_skipped {
    moq_namespace_t track_namespace_suffix;
    moq_bytes_t     track_name;
} moq_d21_publish_skipped_t;

MOQ_API moq_result_t moq_d21_encode_publish_skipped(
    moq_buf_writer_t *w, const moq_namespace_t *suffix, moq_bytes_t track_name);
MOQ_API moq_result_t moq_d21_decode_publish_skipped(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d21_publish_skipped_t *out);

/*
 * NAMESPACE (0x8) / NAMESPACE_DONE (0xE) (draft-18 §10.16 / §10.17): a single
 * Track Namespace Suffix (0..32 fields), sent on a SUBSCRIBE_NAMESPACE response
 * stream. The prefix is implied by the request, so only the suffix is carried.
 */
MOQ_API moq_result_t moq_d21_encode_namespace_msg(moq_buf_writer_t *w,
                                                  const moq_namespace_t *suffix,
                                                  bool is_done);
MOQ_API moq_result_t moq_d21_decode_namespace_msg(const uint8_t *payload,
                                                  size_t payload_len,
                                                  moq_bytes_t *parts,
                                                  size_t max_parts,
                                                  moq_namespace_t *out_suffix);

/*
 * TRACK_STATUS (draft-18 §10.14): the SUBSCRIBE layout (Request ID + Track
 * Namespace + Track Name + Message Parameters) minus the Track-delivery
 * subscriber parameters. Only AUTHORIZATION_TOKEN is permitted. Sent as the
 * first and only message on a new bidi; the response is TRACK_STATUS_OK
 * (a REQUEST_OK with params + Track Properties) or REQUEST_ERROR, then FIN.
 * Namespace parts and track name borrow from the decoded buffer.
 */
typedef struct moq_d21_track_status {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    moq_d21_msg_params_t params;
} moq_d21_track_status_t;

MOQ_API moq_result_t moq_d21_encode_track_status(moq_buf_writer_t *w,
                                                 uint64_t request_id,
                                                 const moq_namespace_t *ns,
                                                 moq_bytes_t track_name,
                                                 const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_track_status(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_bytes_t *parts,
                                                 size_t max_parts,
                                                 moq_d21_track_status_t *out);

/*
 * SUBSCRIBE_OK (draft-18 §10.8): no Request ID (the bidi stream correlates).
 * Carries LARGEST_OBJECT / EXPIRES as Message Parameters, then a Track
 * Properties tail. Track Properties are preserved opaquely (`track_properties`
 * borrows from the payload); their KVP structure is validated on decode and the
 * mandatory-property range is rejected, but their contents are not interpreted.
 */
typedef struct moq_d21_subscribe_ok {
    uint64_t             track_alias;
    moq_d21_msg_params_t params;
    moq_bytes_t          track_properties;   /* borrowed from payload */
    bool                 track_properties_unsupported;  /* mandatory prop present */
    bool                 dynamic_groups;     /* Track Property 0x30 == 1 (§12.6) */
} moq_d21_subscribe_ok_t;

MOQ_API moq_result_t moq_d21_encode_subscribe_ok(moq_buf_writer_t *w,
                                                 uint64_t track_alias,
                                                 const moq_d21_msg_params_t *params,
                                                 moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d21_decode_subscribe_ok(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_d21_subscribe_ok_t *out);

/*
 * PUBLISH (draft-18 §10.10): a publisher-initiated subscription. Request ID +
 * Track Namespace + Track Name + Track Alias (publisher-chosen) + Message
 * Parameters + Track Properties. Only FORWARD (the publisher's initial forward
 * intent) and AUTHORIZATION_TOKEN are permitted; the Track Properties tail is
 * preserved opaquely (KVP structure validated). Sent as the first message on a
 * new bidi; the response is PUBLISH_OK / REQUEST_ERROR, then the subscription is
 * established. Namespace parts, track name, and properties borrow from the buffer.
 */
typedef struct moq_d21_publish {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    uint64_t             track_alias;
    moq_d21_msg_params_t params;
    moq_bytes_t          track_properties;   /* borrowed from payload */
    bool                 track_properties_unsupported;  /* mandatory prop present */
    bool                 dynamic_groups;     /* Track Property 0x30 == 1 (§12.6) */
} moq_d21_publish_t;

MOQ_API moq_result_t moq_d21_encode_publish(moq_buf_writer_t *w,
                                            const moq_d21_publish_t *p);
MOQ_API moq_result_t moq_d21_decode_publish(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_bytes_t *parts, size_t max_parts,
                                            moq_d21_publish_t *out);

/*
 * GOAWAY (draft-21 9.2): a New Session URI (length-prefixed) and a Timeout in
 * milliseconds. Draft 21 removed the Request ID that draft 18 carried on the
 * control-stream form, so the control-stream and request-stream forms are the same
 * bytes; which one it is follows from the stream it arrives on. A client MUST send
 * a zero-length URI and a server that receives a non-zero URI MUST close with
 * PROTOCOL_VIOLATION (session-level rules); the URI is at most 8192 bytes. The URI
 * borrows from the payload; a Timeout of 0 means "no specific timeout".
 */
typedef struct moq_d21_goaway {
    moq_bytes_t uri;          /* borrowed from payload; NULL/0 if none */
    uint64_t    timeout_ms;
} moq_d21_goaway_t;

MOQ_API moq_result_t moq_d21_encode_goaway(moq_buf_writer_t *w,
                                           const uint8_t *uri, size_t uri_len,
                                           uint64_t timeout_ms);
MOQ_API moq_result_t moq_d21_decode_goaway(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d21_goaway_t *out);

/* REQUEST_ERROR codes (draft-21 16.11.2, 12.3). Draft 21 removed
 * DUPLICATE_SUBSCRIPTION (0x19) and INVALID_JOINING_REQUEST_ID (0x32) and added
 * CONFLICTING_FILTERS and INVALID_FILTER. An unregistered code, GREASE included,
 * is treated as INTERNAL_ERROR and never closes the session (13). */
#define MOQ_D21_ERROR_INTERNAL_ERROR        ((uint64_t)0x00u)
#define MOQ_D21_ERROR_UNAUTHORIZED          ((uint64_t)0x01u)
#define MOQ_D21_ERROR_TIMEOUT               ((uint64_t)0x02u)
#define MOQ_D21_ERROR_NOT_SUPPORTED         ((uint64_t)0x03u)
#define MOQ_D21_ERROR_MALFORMED_AUTH_TOKEN  ((uint64_t)0x04u)
#define MOQ_D21_ERROR_EXPIRED_AUTH_TOKEN    ((uint64_t)0x05u)
#define MOQ_D21_ERROR_GOING_AWAY            ((uint64_t)0x06u)
#define MOQ_D21_ERROR_EXCESSIVE_LOAD        ((uint64_t)0x09u)
#define MOQ_D21_ERROR_DOES_NOT_EXIST        ((uint64_t)0x10u)
#define MOQ_D21_ERROR_INVALID_RANGE         ((uint64_t)0x11u)
#define MOQ_D21_ERROR_MALFORMED_TRACK       ((uint64_t)0x12u)
#define MOQ_D21_ERROR_UNINTERESTED          ((uint64_t)0x20u)
#define MOQ_D21_ERROR_PREFIX_OVERLAP        ((uint64_t)0x30u)
#define MOQ_D21_ERROR_NAMESPACE_TOO_LARGE   ((uint64_t)0x31u)
#define MOQ_D21_ERROR_UNSUPPORTED_EXTENSION ((uint64_t)0x33u)
/* The code that carries a trailing Redirect structure (9.4.1). */
#define MOQ_D21_ERROR_REDIRECT              ((uint64_t)0x34u)
#define MOQ_D21_ERROR_CONFLICTING_FILTERS   ((uint64_t)0x35u)
#define MOQ_D21_ERROR_INVALID_FILTER        ((uint64_t)0x36u)

/* PUBLISH_DONE status codes (16.11.3). SUBSCRIPTION_ENDED (0x3) is gone: a
 * subscription does not end because Largest Object passed the end of its Location
 * Filter (3.3.1). */
#define MOQ_D21_PUBLISH_DONE_INTERNAL_ERROR ((uint64_t)0x00u)
#define MOQ_D21_PUBLISH_DONE_UNAUTHORIZED   ((uint64_t)0x01u)
#define MOQ_D21_PUBLISH_DONE_TRACK_ENDED    ((uint64_t)0x02u)
#define MOQ_D21_PUBLISH_DONE_GOING_AWAY     ((uint64_t)0x04u)
#define MOQ_D21_PUBLISH_DONE_TOO_FAR_BEHIND ((uint64_t)0x05u)
#define MOQ_D21_PUBLISH_DONE_EXPIRED        ((uint64_t)0x06u)
#define MOQ_D21_PUBLISH_DONE_UPDATE_FAILED  ((uint64_t)0x08u)
#define MOQ_D21_PUBLISH_DONE_EXCESSIVE_LOAD ((uint64_t)0x09u)
#define MOQ_D21_PUBLISH_DONE_MALFORMED_TRACK ((uint64_t)0x12u)

/* Session termination codes (16.11.1). VERSION_NEGOTIATION_FAILED (0x15) is gone
 * and TOO_MANY_REQUEST_UPDATES (0x1B) is new. */
#define MOQ_D21_SESSION_ERR_NO_ERROR                  ((uint64_t)0x00u)
#define MOQ_D21_SESSION_ERR_INTERNAL_ERROR            ((uint64_t)0x01u)
#define MOQ_D21_SESSION_ERR_UNAUTHORIZED              ((uint64_t)0x02u)
#define MOQ_D21_SESSION_ERR_PROTOCOL_VIOLATION        ((uint64_t)0x03u)
#define MOQ_D21_SESSION_ERR_INVALID_REQUEST_ID        ((uint64_t)0x04u)
#define MOQ_D21_SESSION_ERR_DUPLICATE_TRACK_ALIAS     ((uint64_t)0x05u)
#define MOQ_D21_SESSION_ERR_KEY_VALUE_FORMATTING      ((uint64_t)0x06u)
#define MOQ_D21_SESSION_ERR_INVALID_PATH              ((uint64_t)0x08u)
#define MOQ_D21_SESSION_ERR_MALFORMED_PATH            ((uint64_t)0x09u)
#define MOQ_D21_SESSION_ERR_GOAWAY_TIMEOUT            ((uint64_t)0x10u)
#define MOQ_D21_SESSION_ERR_CONTROL_MESSAGE_TIMEOUT   ((uint64_t)0x11u)
#define MOQ_D21_SESSION_ERR_DATA_STREAM_TIMEOUT       ((uint64_t)0x12u)
#define MOQ_D21_SESSION_ERR_AUTH_TOKEN_CACHE_OVERFLOW ((uint64_t)0x13u)
#define MOQ_D21_SESSION_ERR_DUPLICATE_AUTH_TOKEN_ALIAS ((uint64_t)0x14u)
#define MOQ_D21_SESSION_ERR_MALFORMED_AUTH_TOKEN      ((uint64_t)0x16u)
#define MOQ_D21_SESSION_ERR_UNKNOWN_AUTH_TOKEN_ALIAS  ((uint64_t)0x17u)
#define MOQ_D21_SESSION_ERR_EXPIRED_AUTH_TOKEN        ((uint64_t)0x18u)
#define MOQ_D21_SESSION_ERR_INVALID_AUTHORITY         ((uint64_t)0x19u)
#define MOQ_D21_SESSION_ERR_MALFORMED_AUTHORITY       ((uint64_t)0x1Au)
#define MOQ_D21_SESSION_ERR_TOO_MANY_REQUEST_UPDATES  ((uint64_t)0x1Bu)

/* Whether a code is in the draft-21 registry for its context (GREASE values and
 * everything else are not). Receivers treat an unregistered value as
 * INTERNAL_ERROR for that context. */
MOQ_API bool moq_d21_request_error_registered(uint64_t code);
MOQ_API bool moq_d21_publish_done_registered(uint64_t code);
MOQ_API bool moq_d21_session_error_registered(uint64_t code);

/*
 * REQUEST_ERROR (draft-21 9.4.2): no Request ID. A trailing Redirect structure
 * is present exactly when the error code is REDIRECT. The plain encoder refuses
 * the REDIRECT code and the plain decoder does not accept it (a REDIRECT without
 * its Redirect is malformed); use the *_redirect variants, which take the
 * namespace parts externally like the other namespace decoders.
 */
typedef struct moq_d21_request_error {
    uint64_t    error_code;
    uint64_t    retry_interval;
    moq_bytes_t reason;
} moq_d21_request_error_t;

MOQ_API moq_result_t moq_d21_encode_request_error(moq_buf_writer_t *w,
                                                  uint64_t error_code,
                                                  uint64_t retry_interval,
                                                  moq_bytes_t reason);
MOQ_API moq_result_t moq_d21_decode_request_error(const uint8_t *payload,
                                                  size_t payload_len,
                                                  moq_d21_request_error_t *out);

/*
 * Redirect structure (draft-18 §10.6.1): a Connect URI (empty ⇒ reuse current
 * session URI) and an optional redirect Full Track Name (both empty ⇒ reuse the
 * original request's). All spans borrow from the payload; the namespace parts are
 * caller-supplied (see decode below).
 */
typedef struct moq_d21_redirect {
    moq_bytes_t     connect_uri;
    moq_namespace_t track_namespace;   /* parts point into caller-supplied buffer */
    moq_bytes_t     track_name;
} moq_d21_redirect_t;

MOQ_API moq_result_t moq_d21_encode_redirect(moq_buf_writer_t *w,
                                             const moq_d21_redirect_t *redirect);
MOQ_API moq_result_t moq_d21_decode_redirect(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_bytes_t *parts, size_t max_parts,
                                             moq_d21_redirect_t *out);

/* REQUEST_ERROR encode carrying a Redirect tail (for the REDIRECT error code). */
MOQ_API moq_result_t moq_d21_encode_request_error_redirect(
    moq_buf_writer_t *w, uint64_t error_code, uint64_t retry_interval,
    moq_bytes_t reason, const moq_d21_redirect_t *redirect);

/* REQUEST_ERROR decode that also decodes the trailing Redirect when the error
 * code is REDIRECT (0x34). out_redirect is populated only in that case; the
 * caller keys on out_err->error_code. Namespace parts are caller-supplied. */
MOQ_API moq_result_t moq_d21_decode_request_error_redirect(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *parts, size_t max_parts,
    moq_d21_request_error_t *out_err, moq_d21_redirect_t *out_redirect);

/*
 * REQUEST_UPDATE (draft-18 §10.9): Request ID (a fresh request id, sender
 * parity) followed by Message Parameters. Sent on the same bidi as the original
 * request; the receiver replies with exactly one REQUEST_OK or REQUEST_ERROR.
 */
typedef struct moq_d21_request_update {
    uint64_t             request_id;
    moq_d21_msg_params_t params;
} moq_d21_request_update_t;

MOQ_API moq_result_t moq_d21_encode_request_update(
    moq_buf_writer_t *w, uint64_t request_id, const moq_d21_msg_params_t *p);
MOQ_API moq_result_t moq_d21_decode_request_update(
    const uint8_t *payload, size_t payload_len, moq_d21_request_update_t *out);

/*
 * REQUEST_OK (draft-21 9.3): no Request ID (the bidi stream correlates). One
 * generic wire message answers six requests; the draft names each form by a
 * shorthand (PUBLISH_OK, REQUEST_UPDATE_OK, TRACK_STATUS_OK, ...) and there is
 * no separate PUBLISH_OK message in draft 21 (type 0x1E is reserved). The caller
 * says which request it answers, which fixes the parameters the message may
 * carry (MOQ_D21_MASK_*_OK) and whether Track Properties are allowed.
 *
 * Track Properties follow the parameters and run to the end of the message. They
 * are populated only for TRACK_STATUS_OK and MUST be empty in every other form; a
 * receiver that gets them closes with PROTOCOL_VIOLATION (9.3), and the encoder
 * refuses to send them. Track Properties borrow from the payload on decode.
 */
typedef enum moq_d21_request_ok_kind {
    MOQ_D21_REQUEST_OK_PUBLISH = 1,             /* PUBLISH_OK: EXPIRES */
    MOQ_D21_REQUEST_OK_REQUEST_UPDATE,          /* REQUEST_UPDATE_OK: EXPIRES, LARGEST_OBJECT */
    MOQ_D21_REQUEST_OK_TRACK_STATUS,            /* TRACK_STATUS_OK: + Track Properties */
    MOQ_D21_REQUEST_OK_SUBSCRIBE_NAMESPACE,     /* EXPIRES */
    MOQ_D21_REQUEST_OK_SUBSCRIBE_TRACKS,        /* EXPIRES */
    MOQ_D21_REQUEST_OK_PUBLISH_NAMESPACE        /* EXPIRES */
} moq_d21_request_ok_kind_t;

typedef struct moq_d21_request_ok {
    moq_d21_msg_params_t params;
    moq_bytes_t          track_properties;   /* TRACK_STATUS_OK only; borrowed */
} moq_d21_request_ok_t;

/* `params` may be NULL (none). `track_properties` must be empty unless `kind` is
 * TRACK_STATUS, and is structurally validated (a malformed block, or a Mandatory
 * Track Property, is MOQ_ERR_INVAL). On failure the writer is left unchanged. */
MOQ_API moq_result_t moq_d21_encode_request_ok(moq_buf_writer_t *w,
                                               moq_d21_request_ok_kind_t kind,
                                               const moq_d21_msg_params_t *params,
                                               moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d21_decode_request_ok(const uint8_t *payload,
                                               size_t payload_len,
                                               moq_d21_request_ok_kind_t kind,
                                               moq_d21_request_ok_t *out);

/*
 * PUBLISH_DONE (draft-21 9.9): a publisher's final message before closing
 * (FIN) a subscription's request bidi. Stream Count includes fill fetch streams;
 * 2^64-1 means the publisher could not count. No Request ID (the bidi correlates).
 */
#define MOQ_D21_PUBLISH_DONE   ((uint64_t)0x0Bu)

typedef struct moq_d21_publish_done {
    uint64_t    status_code;
    uint64_t    stream_count;
    moq_bytes_t reason;
} moq_d21_publish_done_t;

MOQ_API moq_result_t moq_d21_encode_publish_done(moq_buf_writer_t *w,
                                                 uint64_t status_code,
                                                 uint64_t stream_count,
                                                 moq_bytes_t reason);
MOQ_API moq_result_t moq_d21_decode_publish_done(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_d21_publish_done_t *out);

/*
 * PUBLISH_STATE_NOTIFY (draft-21 9.10): a publisher's unilateral notice that a
 * subscription's state changed for a reason other than a subscriber's own
 * REQUEST_UPDATE. No Request ID (the subscription's bidi stream correlates) and no
 * response. The body is a parameter block and nothing else (no Track Properties).
 * It may carry only LOCATION_FILTER, FORWARD and LARGEST_OBJECT; the draft requires
 * the publisher to include LARGEST_OBJECT when it is known.
 */
typedef struct moq_d21_publish_state_notify {
    moq_d21_msg_params_t params;
} moq_d21_publish_state_notify_t;

MOQ_API moq_result_t moq_d21_encode_publish_state_notify(
    moq_buf_writer_t *w, const moq_d21_msg_params_t *params);
MOQ_API moq_result_t moq_d21_decode_publish_state_notify(
    const uint8_t *payload, size_t payload_len,
    moq_d21_publish_state_notify_t *out);

/*
 * SUBGROUP_HEADER (draft-18 §11.4.2). The type byte has the form 0b0XX1XXXX
 * (bit 4 always set): PROPERTIES (0x01), SUBGROUP_ID_MODE (bits 1-2: 0=zero,
 * 1=first-object, 2=present, 3=reserved/invalid), END_OF_GROUP (0x08),
 * DEFAULT_PRIORITY (0x20), FIRST_OBJECT (0x40). Integer fields are vi64; the
 * priority is a raw byte present only when DEFAULT_PRIORITY is clear.
 */
#define MOQ_D21_SUBGROUP_BIT_PROPERTIES        0x01u
#define MOQ_D21_SUBGROUP_MASK_ID_MODE          0x06u
#define MOQ_D21_SUBGROUP_BIT_END_OF_GROUP      0x08u
#define MOQ_D21_SUBGROUP_BIT_DEFAULT_PRIORITY  0x20u
#define MOQ_D21_SUBGROUP_BIT_FIRST_OBJECT      0x40u

typedef struct moq_d21_subgroup_header {
    uint8_t  type;                /* raw type byte */
    bool     has_properties;
    uint8_t  subgroup_id_mode;    /* 0=zero, 1=first-object, 2=present */
    bool     end_of_group;
    bool     default_priority;
    bool     first_object;
    uint64_t track_alias;
    uint64_t group_id;
    uint64_t subgroup_id;         /* wire-present only when mode == present */
    uint8_t  publisher_priority;  /* valid when !default_priority */
} moq_d21_subgroup_header_t;

/* True if `type` is a structurally valid SUBGROUP_HEADER type (form 0b0XX1XXXX
 * with SUBGROUP_ID_MODE != 0b11). */
MOQ_API bool moq_d21_subgroup_type_valid(uint8_t type);

/*
 * FETCH family (draft-21 9.11, 9.12, 11.4.1). Control messages (FETCH, FETCH_OK)
 * are framed as vi64 Type + 16-bit Length + payload; FETCH_HEADER is a data-stream
 * lead (vi64 type + Request ID, no length). A Location is a (Group, Object) vi64
 * pair.
 *
 * Draft 21 removed the Fetch Type, the joining variants and the start and end
 * fields: a FETCH names a Full Track Name and carries its range in the
 * LOCATION_FILTER parameter (no filter means {0,0} up to Largest Object, 3.3.1).
 * A "joining" retrieval is now a SUBSCRIBE with FILL_PARAMETERS (3.4).
 */
#define MOQ_D21_FETCH                  ((uint64_t)0x16u)
#define MOQ_D21_FETCH_OK               ((uint64_t)0x18u)

typedef struct moq_d21_location {
    uint64_t group;
    uint64_t object;
} moq_d21_location_t;

typedef struct moq_d21_fetch {
    uint64_t        request_id;
    moq_namespace_t track_namespace;     /* parts in the caller's array */
    moq_bytes_t     track_name;
    /* AUTHORIZATION_TOKEN, FILL_TIMEOUT, SUBSCRIBER_PRIORITY, LOCATION_FILTER,
     * GROUP_ORDER, the Range Filters and INCLUDE_PROPERTIES (MOQ_D21_MASK_FETCH).
     * FETCH takes no FORWARD and no FILL_PARAMETERS. */
    moq_d21_msg_params_t params;
} moq_d21_fetch_t;

MOQ_API moq_result_t moq_d21_encode_fetch(moq_buf_writer_t *w,
                                          const moq_d21_fetch_t *f);
MOQ_API moq_result_t moq_d21_decode_fetch(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_bytes_t *parts, size_t max_parts,
                                          moq_d21_fetch_t *out);

/* FETCH_OK (9.12): End Of Track (8), End Location, a parameter block (no
 * parameter is defined for FETCH_OK, so it must be empty) and Track Properties. */
typedef struct moq_d21_fetch_ok {
    bool               end_of_track;
    moq_d21_location_t end;
    moq_bytes_t        track_properties;   /* borrowed from payload */
    bool               track_properties_unsupported;  /* mandatory prop present */
} moq_d21_fetch_ok_t;

MOQ_API moq_result_t moq_d21_encode_fetch_ok(moq_buf_writer_t *w,
                                             bool end_of_track,
                                             moq_d21_location_t end,
                                             moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d21_decode_fetch_ok(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_d21_fetch_ok_t *out);

/* FETCH_HEADER lead on a fetch data stream: vi64 type (0x05) + Request ID. */
MOQ_API moq_result_t moq_d21_encode_fetch_header(moq_buf_writer_t *w,
                                                 uint64_t request_id);
MOQ_API moq_result_t moq_d21_decode_fetch_header(moq_buf_reader_t *r,
                                                 uint64_t *out_request_id);

MOQ_API moq_result_t moq_d21_encode_subgroup_header(
    moq_buf_writer_t *w, const moq_d21_subgroup_header_t *hdr);
MOQ_API moq_result_t moq_d21_decode_subgroup_header(
    moq_buf_reader_t *r, moq_d21_subgroup_header_t *out);

/* Validate a draft-18 Object Property KVP block (§11.2.1.2): structurally
 * well-formed, and free of any Mandatory Track Property (0x4000-0x7FFF, §2.5.1)
 * including one hidden inside IMMUTABLE_PROPERTIES (§12.7) — a mandatory property
 * carried as an Object Property makes the object malformed. Returns MOQ_OK or
 * MOQ_ERR_PROTO. (Track Properties, where a mandatory property yields a
 * request-level UNSUPPORTED_EXTENSION rather than a fault, are scanned by the
 * control-message decoders.) */
MOQ_API moq_result_t moq_d21_validate_properties(const uint8_t *data, size_t len);

/* Lenient Track-Property scan reporting whether DYNAMIC_GROUPS (0x30) is
 * present with value 1 (SS12.6). Structural failure / value above 1 returns
 * MOQ_ERR_PROTO with *out false. */
MOQ_API moq_result_t moq_d21_scan_dynamic_groups(const uint8_t *data,
                                                 size_t len,
                                                 bool *out_dynamic_groups);

/*
 * OBJECT_DATAGRAM (§11.3.1): a single object in a datagram. The Type takes the
 * form 0b00X0XXXX (0x00-0x0F / 0x20-0x2F); the present fields are selected by
 * bits in the Type. Field order: Type, Track Alias, Group ID, [Object ID],
 * [Publisher Priority], [Properties: vi64 length + KVP], [Object Status vi64],
 * else the rest of the datagram is the payload (no length field). Properties are
 * the draft-18 vi64 KVP block (validated with moq_d21_validate_properties).
 */
#define MOQ_D21_DGRAM_BIT_PROPERTIES     0x01u
#define MOQ_D21_DGRAM_BIT_END_OF_GROUP   0x02u
#define MOQ_D21_DGRAM_BIT_ZERO_OBJECT_ID 0x04u
#define MOQ_D21_DGRAM_BIT_DEFAULT_PRIO   0x08u
#define MOQ_D21_DGRAM_BIT_STATUS         0x20u
/* Padding datagram (§11.5.2): type then all-zero bytes; the receiver discards it. */
#define MOQ_D21_PADDING_DATAGRAM         ((uint64_t)0x132B3E29u)

typedef struct moq_d21_object_datagram {
    uint64_t       track_alias;
    uint64_t       group_id;
    uint64_t       object_id;
    uint8_t        publisher_priority;
    bool           has_properties;
    const uint8_t *properties;      /* borrowed from input; vi64 KVP block */
    size_t         properties_len;
    bool           is_status;
    uint64_t       object_status;   /* wire value (0x0 / 0x3 / 0x4) */
    bool           end_of_group;
    bool           default_priority;
    const uint8_t *payload;         /* borrowed; remainder of datagram */
    size_t         payload_len;
} moq_d21_object_datagram_t;

/* Decode an OBJECT_DATAGRAM. Returns MOQ_OK for an object datagram, MOQ_DONE for
 * a padding datagram (caller discards), or MOQ_ERR_PROTO for a malformed/invalid
 * datagram (caller closes the session). */
MOQ_API moq_result_t moq_d21_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d21_object_datagram_t *out);

MOQ_API moq_result_t moq_d21_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d21_object_datagram_t *dg);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CONTROL_D21_H */
