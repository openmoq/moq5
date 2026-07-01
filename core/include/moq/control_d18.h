#ifndef MOQ_CONTROL_D18_H
#define MOQ_CONTROL_D18_H

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
#define MOQ_D18_STREAM_SETUP        ((uint64_t)0x2F00u)
#define MOQ_D18_STREAM_PADDING      ((uint64_t)0x132B3E28u)
#define MOQ_D18_STREAM_FETCH_HEADER ((uint64_t)0x05u)

/*
 * Decode a draft-18 control-message envelope: Type (vi64) + Length (16-bit) +
 * payload. On success the reader is advanced past the full envelope and *out
 * borrows the payload from the reader's buffer. Returns MOQ_ERR_BUFFER when the
 * envelope (including the declared payload length) is not yet fully available.
 */
MOQ_API moq_result_t moq_d18_decode_envelope(moq_buf_reader_t *r,
                                              moq_control_envelope_t *out);

/*
 * Encode a draft-18 SETUP message with no Setup Options: Type (vi64) = 0x2F00,
 * Length (16) = 0. (This helper does not encode any Setup Options.)
 */
MOQ_API moq_result_t moq_d18_encode_setup(moq_buf_writer_t *w);

/* Setup Option types (§10.3.1). Options are vi64 Key-Value-Pairs (§1.4.3):
 * delta-encoded type; odd types carry a vi64 Length + bytes, even types a
 * single vi64 value. Unlike Message Parameters, unknown Setup Options are
 * structurally skippable and MUST be ignored (§10.3). */
#define MOQ_D18_SETUP_OPT_PATH                      ((uint64_t)0x01u)  /* bytes */
#define MOQ_D18_SETUP_OPT_AUTHORIZATION_TOKEN       ((uint64_t)0x03u)  /* token; may repeat */
#define MOQ_D18_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE ((uint64_t)0x04u)  /* varint bytes */
#define MOQ_D18_SETUP_OPT_AUTHORITY                 ((uint64_t)0x05u)  /* bytes */

/* Reason Phrase maximum length (draft-18 §1.4.4). */
#define MOQ_D18_MAX_REASON 1024u

/* Maximum Full Track Name length: sum of Track Namespace Field lengths plus
 * the Track Name length (draft-18 §2.4.1). */
#define MOQ_D18_MAX_FULL_TRACK 4096u

/* Control-message type codes (draft-18 §10). */
#define MOQ_D18_SUBSCRIBE         ((uint64_t)0x03u)
#define MOQ_D18_REQUEST_UPDATE    ((uint64_t)0x02u)
#define MOQ_D18_SUBSCRIBE_OK      ((uint64_t)0x04u)
#define MOQ_D18_REQUEST_OK        ((uint64_t)0x07u)
#define MOQ_D18_REQUEST_ERROR     ((uint64_t)0x05u)
#define MOQ_D18_PUBLISH_NAMESPACE ((uint64_t)0x06u)
#define MOQ_D18_NAMESPACE         ((uint64_t)0x08u)
#define MOQ_D18_NAMESPACE_DONE    ((uint64_t)0x0Eu)
#define MOQ_D18_TRACK_STATUS      ((uint64_t)0x0Du)
#define MOQ_D18_SUBSCRIBE_NAMESPACE ((uint64_t)0x50u)
#define MOQ_D18_SUBSCRIBE_TRACKS  ((uint64_t)0x51u)
#define MOQ_D18_PUBLISH_BLOCKED   ((uint64_t)0x0Fu)
#define MOQ_D18_PUBLISH           ((uint64_t)0x1Du)
#define MOQ_D18_GOAWAY            ((uint64_t)0x10u)

/* A draft-18 decode result distinct from the generic PROTOCOL_VIOLATION: an
 * AUTHORIZATION_TOKEN whose Token structure cannot be decoded must close the
 * session with KEY_VALUE_FORMATTING_ERROR (0x06, §10.2.2) rather than 0x03. It
 * is a negative moq_result_t-compatible value outside the shared error range, so
 * callers that only test `rc < 0` are unaffected; the D18 profile maps it to the
 * 0x06 close code. */
#define MOQ_D18_ERR_KVP_FORMAT (-200)

/* Message Parameter types (draft-18 §10.2). */
#define MOQ_D18_PARAM_AUTHORIZATION_TOKEN       ((uint64_t)0x03u)  /* len-prefixed */
#define MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT   ((uint64_t)0x02u)  /* varint ms */
#define MOQ_D18_PARAM_SUBGROUP_DELIVERY_TIMEOUT ((uint64_t)0x06u)  /* varint ms */
#define MOQ_D18_PARAM_EXPIRES             ((uint64_t)0x08u)  /* varint (ms) */
#define MOQ_D18_PARAM_LARGEST_OBJECT      ((uint64_t)0x09u)  /* Location */
#define MOQ_D18_PARAM_FORWARD             ((uint64_t)0x10u)  /* uint8 0/1 */
#define MOQ_D18_PARAM_SUBSCRIBER_PRIORITY ((uint64_t)0x20u)  /* uint8 */
#define MOQ_D18_PARAM_SUBSCRIPTION_FILTER ((uint64_t)0x21u)  /* len-prefixed */
#define MOQ_D18_PARAM_GROUP_ORDER         ((uint64_t)0x22u)  /* uint8 1/2 */
/* Defined draft-18 parameters that are accepted (decoded + form-validated) but
 * not surfaced: their semantics are not modeled yet, and a defined parameter
 * must not be treated as unknown (§10.2 closes the session for unknown only). */
#define MOQ_D18_PARAM_RENDEZVOUS_TIMEOUT  ((uint64_t)0x04u)  /* varint ms (§10.2.6) */
#define MOQ_D18_PARAM_FILL_TIMEOUT        ((uint64_t)0x0Au)  /* varint ms (§10.2.5) */
#define MOQ_D18_PARAM_NEW_GROUP_REQUEST   ((uint64_t)0x32u)  /* varint (§10.2.13) */
#define MOQ_D18_PARAM_TRACK_NAMESPACE_PREFIX ((uint64_t)0x34u) /* namespace (§10.2.14) */

/* Which parameter types a given message permits. The decoder rejects any
 * parameter outside the allowed set (as well as any unknown type) with PROTO,
 * since draft-18 parameters are not skippable. */
#define MOQ_D18_PARAM_BIT_FORWARD                 (1u << 0)
#define MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY     (1u << 1)
#define MOQ_D18_PARAM_BIT_SUBSCRIPTION_FILTER     (1u << 2)
#define MOQ_D18_PARAM_BIT_GROUP_ORDER             (1u << 3)
#define MOQ_D18_PARAM_BIT_EXPIRES                 (1u << 4)
#define MOQ_D18_PARAM_BIT_LARGEST_OBJECT          (1u << 5)
#define MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT (1u << 6)
#define MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT (1u << 7)
#define MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN     (1u << 8)
#define MOQ_D18_PARAM_BIT_RENDEZVOUS_TIMEOUT      (1u << 9)
#define MOQ_D18_PARAM_BIT_FILL_TIMEOUT            (1u << 10)
#define MOQ_D18_PARAM_BIT_NEW_GROUP_REQUEST       (1u << 11)
#define MOQ_D18_PARAM_BIT_TRACK_NAMESPACE_PREFIX  (1u << 12)

/* Maximum AUTHORIZATION_TOKEN parameters carried per message. Matches the
 * session-core decoded-token cap (MOQ_DECODED_MAX_TOKENS); the profile
 * static-asserts the relation. */
#define MOQ_D18_MAX_AUTH_TOKENS 16

/* One AUTHORIZATION_TOKEN Token structure (§10.2.2), decoded from / encoded to
 * the parameter's length-prefixed value. Integers are vi64; token_value borrows
 * from the decode buffer (REGISTER / USE_VALUE only). */
typedef struct moq_d18_auth_token {
    uint32_t    alias_type;   /* 0 DELETE, 1 REGISTER, 2 USE_ALIAS, 3 USE_VALUE */
    uint64_t    alias;        /* DELETE / REGISTER / USE_ALIAS */
    uint64_t    token_type;   /* REGISTER / USE_VALUE */
    moq_bytes_t token_value;  /* REGISTER / USE_VALUE; borrowed */
} moq_d18_auth_token_t;

/* Decoded SETUP Options (§10.3.1). PATH/AUTHORITY surface as presence flags
 * (mirroring the draft-16 setup-parameter handling); token values borrow from
 * the payload. Unknown options are skipped per §10.3. */
typedef struct moq_d18_setup_opts {
    bool     has_path;
    bool     has_authority;
    bool     has_max_auth_token_cache_size;
    uint64_t max_auth_token_cache_size;
    size_t               auth_token_count;
    moq_d18_auth_token_t auth_tokens[MOQ_D18_MAX_AUTH_TOKENS];
} moq_d18_setup_opts_t;

/* Decode the SETUP payload (the bytes after the 16-bit Length) as a vi64 KVP
 * Setup Options walk: unknown options (and duplicates of unknown options) are
 * skipped; a duplicate known non-repeatable option, an over-cap value length
 * (> 2^16-1), or a Delta Type overflow is MOQ_ERR_PROTO; a malformed
 * AUTHORIZATION_TOKEN structure is MOQ_D18_ERR_KVP_FORMAT (close 0x6). */
MOQ_API moq_result_t moq_d18_decode_setup_opts(const uint8_t *payload, size_t len,
                                               moq_d18_setup_opts_t *out);

/* Encode a SETUP message carrying the given options (NULL opts == no options).
 * Only MAX_AUTH_TOKEN_CACHE_SIZE emission is supported (the only option the
 * session currently sources); PATH/AUTHORITY/token emission are rejected with
 * MOQ_ERR_INVAL until a public surface supplies them. */
MOQ_API moq_result_t moq_d18_encode_setup_opts(moq_buf_writer_t *w,
                                               const moq_d18_setup_opts_t *opts);

/*
 * Decoded Message Parameters. Only the representable subset is carried; an
 * unknown or not-permitted parameter type fails the decode (draft-18 §10.2:
 * unknown parameters cannot be skipped and are a PROTOCOL_VIOLATION).
 * AUTHORIZATION_TOKEN may repeat (§10.2.2), so it is carried as an array.
 */
typedef struct moq_d18_msg_params {
    bool     has_forward;
    uint8_t  forward;                /* 0 or 1 */
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_group_order;
    uint8_t  group_order;            /* 1 ascending, 2 descending */
    bool     has_filter;
    uint32_t filter_type;            /* §5.1.2: 1 next-group, 2 largest, 3/4 absolute */
    uint64_t filter_start_group;     /* filter types 3/4 */
    uint64_t filter_start_object;    /* filter types 3/4 */
    uint64_t filter_end_group;       /* filter type 4 (absolute, decoded from delta) */
    bool     has_expires;            /* response parameter (SUBSCRIBE_OK) */
    uint64_t expires_ms;
    bool     has_largest;            /* response parameter (SUBSCRIBE_OK) */
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_object_delivery_timeout;
    uint64_t object_delivery_timeout_ms;
    bool     has_subgroup_delivery_timeout;
    uint64_t subgroup_delivery_timeout_ms;
    size_t               auth_token_count;
    moq_d18_auth_token_t auth_tokens[MOQ_D18_MAX_AUTH_TOKENS];
    bool     has_new_group_request;  /* §10.2.13: SUBSCRIBE / PUBLISH_OK /
                                      * REQUEST_UPDATE; the value 0 ("no group
                                      * info") is meaningful */
    uint64_t new_group_request;
} moq_d18_msg_params_t;

/* Encode a parameter block: a vi64 count followed by the set parameters in
 * ascending Type-Delta order (OBJECT_DELIVERY_TIMEOUT, AUTHORIZATION_TOKEN
 * (repeatable), SUBGROUP_DELIVERY_TIMEOUT, EXPIRES, LARGEST_OBJECT, FORWARD,
 * SUBSCRIBER_PRIORITY, SUBSCRIPTION_FILTER, GROUP_ORDER, NEW_GROUP_REQUEST). */
MOQ_API moq_result_t moq_d18_encode_msg_params(moq_buf_writer_t *w,
                                               const moq_d18_msg_params_t *p);

/* Decode `count` parameters from the reader into `out`. Enforces ascending
 * Type-Delta order, rejects duplicates, unknown types, and any type not in
 * `allowed_mask` (PROTO). */
MOQ_API moq_result_t moq_d18_decode_msg_params(moq_buf_reader_t *r,
                                               uint64_t count,
                                               uint32_t allowed_mask,
                                               moq_d18_msg_params_t *out);

/*
 * SUBSCRIBE (draft-18 §10.7). Priority/forward/group-order/filter are carried as
 * Message Parameters; `params` supplies the ones to emit (and receives the
 * decoded ones). Unknown or non-SUBSCRIBE parameters fail the decode. Namespace
 * parts and track name borrow from the decoded buffer.
 */
typedef struct moq_d18_subscribe {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    moq_d18_msg_params_t params;
} moq_d18_subscribe_t;

MOQ_API moq_result_t moq_d18_encode_subscribe(moq_buf_writer_t *w,
                                              uint64_t request_id,
                                              const moq_namespace_t *ns,
                                              moq_bytes_t track_name,
                                              const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_subscribe(const uint8_t *payload,
                                              size_t payload_len,
                                              moq_bytes_t *parts,
                                              size_t max_parts,
                                              moq_d18_subscribe_t *out);

/*
 * PUBLISH_NAMESPACE (draft-18 §10.15): Request ID + Track Namespace (0..32
 * fields) + Message Parameters. Only AUTHORIZATION_TOKEN is permitted; the
 * response is REQUEST_OK / REQUEST_ERROR on the bidi (no dedicated OK message).
 * Namespace parts borrow from the decoded buffer.
 */
typedef struct moq_d18_publish_namespace {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_d18_msg_params_t params;
} moq_d18_publish_namespace_t;

MOQ_API moq_result_t moq_d18_encode_publish_namespace(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *ns,
    const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_publish_namespace(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d18_publish_namespace_t *out);

/*
 * SUBSCRIBE_NAMESPACE (draft-18 §10.18): Request ID + Track Namespace Prefix
 * (0..32 fields) + Message Parameters. It is namespace-only in draft-18 (the
 * old interest field split into SUBSCRIBE_TRACKS), and only AUTHORIZATION_TOKEN
 * is permitted. The response is REQUEST_OK / REQUEST_ERROR on the bidi, then a
 * stream of NAMESPACE / NAMESPACE_DONE messages. Namespace parts borrow from the
 * decoded buffer.
 */
typedef struct moq_d18_subscribe_namespace {
    uint64_t             request_id;
    moq_namespace_t      track_namespace_prefix;
    moq_d18_msg_params_t params;
} moq_d18_subscribe_namespace_t;

MOQ_API moq_result_t moq_d18_encode_subscribe_namespace(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *prefix,
    const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_subscribe_namespace(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d18_subscribe_namespace_t *out);

/*
 * SUBSCRIBE_TRACKS (draft-18 §10.19): Request ID + Track Namespace Prefix
 * (0..32 fields) + Message Parameters. Requests PUBLISH messages for all tracks
 * under the prefix (and future ones). FORWARD (§10.2.12) and AUTHORIZATION_TOKEN
 * (§10.2.2) are the only permitted parameters; the response is REQUEST_OK /
 * REQUEST_ERROR on the bidi, then a stream of PUBLISH_BLOCKED messages while the
 * subscription is established (the resulting PUBLISH messages travel on separate
 * bidi streams). The overlap space is independent of SUBSCRIBE_NAMESPACE.
 * Namespace parts borrow from the decoded buffer.
 */
typedef struct moq_d18_subscribe_tracks {
    uint64_t             request_id;
    moq_namespace_t      track_namespace_prefix;
    moq_d18_msg_params_t params;
} moq_d18_subscribe_tracks_t;

MOQ_API moq_result_t moq_d18_encode_subscribe_tracks(
    moq_buf_writer_t *w, uint64_t request_id, const moq_namespace_t *prefix,
    const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_subscribe_tracks(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d18_subscribe_tracks_t *out);

/*
 * PUBLISH_BLOCKED (draft-18 §10.20): Track Namespace Suffix (0..32 fields) +
 * Track Name. Sent by the publisher on a SUBSCRIBE_TRACKS response stream to
 * signal it cannot open a PUBLISH for a matching track; it correlates by that
 * stream, so it carries no Request ID. The suffix is relative to the request's
 * Track Namespace Prefix. Suffix parts and track name borrow from the buffer.
 */
typedef struct moq_d18_publish_blocked {
    moq_namespace_t track_namespace_suffix;
    moq_bytes_t     track_name;
} moq_d18_publish_blocked_t;

MOQ_API moq_result_t moq_d18_encode_publish_blocked(
    moq_buf_writer_t *w, const moq_namespace_t *suffix, moq_bytes_t track_name);
MOQ_API moq_result_t moq_d18_decode_publish_blocked(
    const uint8_t *payload, size_t payload_len, moq_bytes_t *parts,
    size_t max_parts, moq_d18_publish_blocked_t *out);

/*
 * NAMESPACE (0x8) / NAMESPACE_DONE (0xE) (draft-18 §10.16 / §10.17): a single
 * Track Namespace Suffix (0..32 fields), sent on a SUBSCRIBE_NAMESPACE response
 * stream. The prefix is implied by the request, so only the suffix is carried.
 */
MOQ_API moq_result_t moq_d18_encode_namespace_msg(moq_buf_writer_t *w,
                                                  const moq_namespace_t *suffix,
                                                  bool is_done);
MOQ_API moq_result_t moq_d18_decode_namespace_msg(const uint8_t *payload,
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
typedef struct moq_d18_track_status {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    moq_d18_msg_params_t params;
} moq_d18_track_status_t;

MOQ_API moq_result_t moq_d18_encode_track_status(moq_buf_writer_t *w,
                                                 uint64_t request_id,
                                                 const moq_namespace_t *ns,
                                                 moq_bytes_t track_name,
                                                 const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_track_status(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_bytes_t *parts,
                                                 size_t max_parts,
                                                 moq_d18_track_status_t *out);

/*
 * TRACK_STATUS_OK (draft-18 §10.14 / §10.5): a REQUEST_OK (0x7) carrying the
 * same parameters (LARGEST_OBJECT / EXPIRES) and Track Properties a SUBSCRIBE_OK
 * would, but with no Track Alias. Track Properties borrow from the payload.
 */
typedef struct moq_d18_track_status_ok {
    moq_d18_msg_params_t params;
    moq_bytes_t          track_properties;   /* borrowed from payload */
} moq_d18_track_status_ok_t;

MOQ_API moq_result_t moq_d18_encode_track_status_ok(moq_buf_writer_t *w,
                                                    const moq_d18_msg_params_t *params,
                                                    moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d18_decode_track_status_ok(const uint8_t *payload,
                                                    size_t payload_len,
                                                    moq_d18_track_status_ok_t *out);

/*
 * SUBSCRIBE_OK (draft-18 §10.8): no Request ID (the bidi stream correlates).
 * Carries LARGEST_OBJECT / EXPIRES as Message Parameters, then a Track
 * Properties tail. Track Properties are preserved opaquely (`track_properties`
 * borrows from the payload); their KVP structure is validated on decode and the
 * mandatory-property range is rejected, but their contents are not interpreted.
 */
typedef struct moq_d18_subscribe_ok {
    uint64_t             track_alias;
    moq_d18_msg_params_t params;
    moq_bytes_t          track_properties;   /* borrowed from payload */
    bool                 track_properties_unsupported;  /* mandatory prop present */
    bool                 dynamic_groups;     /* Track Property 0x30 == 1 (§12.6) */
} moq_d18_subscribe_ok_t;

MOQ_API moq_result_t moq_d18_encode_subscribe_ok(moq_buf_writer_t *w,
                                                 uint64_t track_alias,
                                                 const moq_d18_msg_params_t *params,
                                                 moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d18_decode_subscribe_ok(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_d18_subscribe_ok_t *out);

/*
 * PUBLISH (draft-18 §10.10): a publisher-initiated subscription. Request ID +
 * Track Namespace + Track Name + Track Alias (publisher-chosen) + Message
 * Parameters + Track Properties. Only FORWARD (the publisher's initial forward
 * intent) and AUTHORIZATION_TOKEN are permitted; the Track Properties tail is
 * preserved opaquely (KVP structure validated). Sent as the first message on a
 * new bidi; the response is PUBLISH_OK / REQUEST_ERROR, then the subscription is
 * established. Namespace parts, track name, and properties borrow from the buffer.
 */
typedef struct moq_d18_publish {
    uint64_t             request_id;
    moq_namespace_t      track_namespace;
    moq_bytes_t          track_name;
    uint64_t             track_alias;
    moq_d18_msg_params_t params;
    moq_bytes_t          track_properties;   /* borrowed from payload */
    bool                 track_properties_unsupported;  /* mandatory prop present */
    bool                 dynamic_groups;     /* Track Property 0x30 == 1 (§12.6) */
} moq_d18_publish_t;

MOQ_API moq_result_t moq_d18_encode_publish(moq_buf_writer_t *w,
                                            const moq_d18_publish_t *p);
MOQ_API moq_result_t moq_d18_decode_publish(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_bytes_t *parts, size_t max_parts,
                                            moq_d18_publish_t *out);

/*
 * PUBLISH_OK (draft-18 §10.10 / §10.5): a REQUEST_OK carrying the subscriber's
 * delivery parameters (SUBSCRIBER_PRIORITY / FORWARD / GROUP_ORDER / delivery
 * timeouts) and an EMPTY Track Properties tail (a non-empty tail is a
 * PROTOCOL_VIOLATION, §10.5). No Request ID (the bidi stream correlates).
 */
typedef struct moq_d18_publish_ok {
    moq_d18_msg_params_t params;
} moq_d18_publish_ok_t;

MOQ_API moq_result_t moq_d18_encode_publish_ok(moq_buf_writer_t *w,
                                               const moq_d18_msg_params_t *params);
MOQ_API moq_result_t moq_d18_decode_publish_ok(const uint8_t *payload,
                                               size_t payload_len,
                                               moq_d18_publish_ok_t *out);

/*
 * GOAWAY (draft-18 §10.4), control-stream form: New Session URI (length-prefixed;
 * the client MUST send zero length), a Timeout (milliseconds; a drain hint), and
 * the Request ID (the smallest unprocessed peer Request ID). The request-stream
 * form (no Request ID; the stream identifies the request) uses the *_request
 * variants below. URI borrows from the payload.
 */
typedef struct moq_d18_goaway {
    moq_bytes_t uri;          /* borrowed from payload; NULL/0 if none */
    uint64_t    timeout_ms;
    uint64_t    request_id;   /* control-stream form only; 0 for request-stream */
} moq_d18_goaway_t;

MOQ_API moq_result_t moq_d18_encode_goaway(moq_buf_writer_t *w,
                                           const uint8_t *uri, size_t uri_len,
                                           uint64_t timeout_ms,
                                           uint64_t request_id);
MOQ_API moq_result_t moq_d18_decode_goaway(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d18_goaway_t *out);

/* Request-stream GOAWAY (§10.4): New Session URI + Timeout, no Request ID. */
MOQ_API moq_result_t moq_d18_encode_goaway_request(moq_buf_writer_t *w,
                                                   const uint8_t *uri,
                                                   size_t uri_len,
                                                   uint64_t timeout_ms);
MOQ_API moq_result_t moq_d18_decode_goaway_request(const uint8_t *payload,
                                                   size_t payload_len,
                                                   moq_d18_goaway_t *out);

/* REQUEST_ERROR error code carrying a trailing Redirect structure (§10.6). */
#define MOQ_D18_ERROR_REDIRECT ((uint64_t)0x34u)

/*
 * REQUEST_ERROR (draft-18 §10.6.2): no Request ID. A trailing Redirect structure
 * is present only when the error code is REDIRECT; the base decoder below does
 * not decode it (it strict-rejects any tail). Use the redirect-aware decoder for
 * REDIRECT (it takes the namespace parts externally, like the other D18 namespace
 * decoders).
 */
typedef struct moq_d18_request_error {
    uint64_t    error_code;
    uint64_t    retry_interval;
    moq_bytes_t reason;
} moq_d18_request_error_t;

MOQ_API moq_result_t moq_d18_encode_request_error(moq_buf_writer_t *w,
                                                  uint64_t error_code,
                                                  uint64_t retry_interval,
                                                  moq_bytes_t reason);
MOQ_API moq_result_t moq_d18_decode_request_error(const uint8_t *payload,
                                                  size_t payload_len,
                                                  moq_d18_request_error_t *out);

/*
 * Redirect structure (draft-18 §10.6.1): a Connect URI (empty ⇒ reuse current
 * session URI) and an optional redirect Full Track Name (both empty ⇒ reuse the
 * original request's). All spans borrow from the payload; the namespace parts are
 * caller-supplied (see decode below).
 */
typedef struct moq_d18_redirect {
    moq_bytes_t     connect_uri;
    moq_namespace_t track_namespace;   /* parts point into caller-supplied buffer */
    moq_bytes_t     track_name;
} moq_d18_redirect_t;

MOQ_API moq_result_t moq_d18_encode_redirect(moq_buf_writer_t *w,
                                             const moq_d18_redirect_t *redirect);
MOQ_API moq_result_t moq_d18_decode_redirect(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_bytes_t *parts, size_t max_parts,
                                             moq_d18_redirect_t *out);

/* REQUEST_ERROR encode carrying a Redirect tail (for the REDIRECT error code). */
MOQ_API moq_result_t moq_d18_encode_request_error_redirect(
    moq_buf_writer_t *w, uint64_t error_code, uint64_t retry_interval,
    moq_bytes_t reason, const moq_d18_redirect_t *redirect);

/* REQUEST_ERROR decode that also decodes the trailing Redirect when the error
 * code is REDIRECT (0x34). out_redirect is populated only in that case; the
 * caller keys on out_err->error_code. Namespace parts are caller-supplied. */
MOQ_API moq_result_t moq_d18_decode_request_error_redirect(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *parts, size_t max_parts,
    moq_d18_request_error_t *out_err, moq_d18_redirect_t *out_redirect);

/*
 * REQUEST_UPDATE (draft-18 §10.9): Request ID (a fresh request id, sender
 * parity) followed by Message Parameters. Sent on the same bidi as the original
 * request; the receiver replies with exactly one REQUEST_OK or REQUEST_ERROR.
 */
typedef struct moq_d18_request_update {
    uint64_t             request_id;
    moq_d18_msg_params_t params;
} moq_d18_request_update_t;

MOQ_API moq_result_t moq_d18_encode_request_update(
    moq_buf_writer_t *w, uint64_t request_id, const moq_d18_msg_params_t *p);
MOQ_API moq_result_t moq_d18_decode_request_update(
    const uint8_t *payload, size_t payload_len, moq_d18_request_update_t *out);

/*
 * REQUEST_OK (draft-18 §10.5): no Request ID (the bidi stream correlates).
 * REQUEST_UPDATE_OK carries no parameters and empty Track Properties; the
 * decoder accepts only that form (non-empty parameters/properties are a
 * PROTOCOL_VIOLATION here until those are modelled).
 */
MOQ_API moq_result_t moq_d18_encode_request_ok(moq_buf_writer_t *w);
MOQ_API moq_result_t moq_d18_decode_request_ok(const uint8_t *payload,
                                               size_t payload_len);

/*
 * PUBLISH_DONE (draft-18 §10.11): a publisher's final message before closing
 * (FIN) a subscription's request bidi. No Request ID (the bidi correlates).
 */
#define MOQ_D18_PUBLISH_DONE   ((uint64_t)0x0Bu)

typedef struct moq_d18_publish_done {
    uint64_t    status_code;
    uint64_t    stream_count;
    moq_bytes_t reason;
} moq_d18_publish_done_t;

MOQ_API moq_result_t moq_d18_encode_publish_done(moq_buf_writer_t *w,
                                                 uint64_t status_code,
                                                 uint64_t stream_count,
                                                 moq_bytes_t reason);
MOQ_API moq_result_t moq_d18_decode_publish_done(const uint8_t *payload,
                                                 size_t payload_len,
                                                 moq_d18_publish_done_t *out);

/*
 * SUBGROUP_HEADER (draft-18 §11.4.2). The type byte has the form 0b0XX1XXXX
 * (bit 4 always set): PROPERTIES (0x01), SUBGROUP_ID_MODE (bits 1-2: 0=zero,
 * 1=first-object, 2=present, 3=reserved/invalid), END_OF_GROUP (0x08),
 * DEFAULT_PRIORITY (0x20), FIRST_OBJECT (0x40). Integer fields are vi64; the
 * priority is a raw byte present only when DEFAULT_PRIORITY is clear.
 */
#define MOQ_D18_SUBGROUP_BIT_PROPERTIES        0x01u
#define MOQ_D18_SUBGROUP_MASK_ID_MODE          0x06u
#define MOQ_D18_SUBGROUP_BIT_END_OF_GROUP      0x08u
#define MOQ_D18_SUBGROUP_BIT_DEFAULT_PRIORITY  0x20u
#define MOQ_D18_SUBGROUP_BIT_FIRST_OBJECT      0x40u

typedef struct moq_d18_subgroup_header {
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
} moq_d18_subgroup_header_t;

/* True if `type` is a structurally valid SUBGROUP_HEADER type (form 0b0XX1XXXX
 * with SUBGROUP_ID_MODE != 0b11). */
MOQ_API bool moq_d18_subgroup_type_valid(uint8_t type);

/*
 * FETCH family (draft-18 §10.12 / §10.13 / §11.4.4). Control messages (FETCH,
 * FETCH_OK) are framed as vi64 Type + 16-bit Length + payload; FETCH_HEADER is
 * a data-stream lead (vi64 type + Request ID, no length). A Location is a
 * (Group, Object) vi64 pair. Parameters and Track Properties are not
 * encoded/decoded yet (zero-parameter form only).
 */
#define MOQ_D18_FETCH                  ((uint64_t)0x16u)
#define MOQ_D18_FETCH_OK               ((uint64_t)0x18u)
#define MOQ_D18_FETCH_TYPE_STANDALONE  ((uint64_t)0x1u)
#define MOQ_D18_FETCH_TYPE_RELATIVE    ((uint64_t)0x2u)
#define MOQ_D18_FETCH_TYPE_ABSOLUTE    ((uint64_t)0x3u)

typedef struct moq_d18_location {
    uint64_t group;
    uint64_t object;
} moq_d18_location_t;

typedef struct moq_d18_fetch {
    uint64_t        request_id;
    uint64_t        fetch_type;          /* 1=standalone, 2=relative, 3=absolute */
    moq_namespace_t track_namespace;     /* standalone: parts in caller array */
    moq_bytes_t     track_name;          /* standalone */
    moq_d18_location_t start;            /* standalone */
    moq_d18_location_t end;              /* standalone */
    uint64_t        joining_request_id;  /* joining (type 2/3) */
    uint64_t        joining_start;       /* joining (type 2/3) */
    /* SUBSCRIBER_PRIORITY / GROUP_ORDER message parameters (FETCH carries no
     * FORWARD or SUBSCRIPTION_FILTER). */
    moq_d18_msg_params_t params;
} moq_d18_fetch_t;

MOQ_API moq_result_t moq_d18_encode_fetch(moq_buf_writer_t *w,
                                          const moq_d18_fetch_t *f);
MOQ_API moq_result_t moq_d18_decode_fetch(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_bytes_t *parts, size_t max_parts,
                                          moq_d18_fetch_t *out);

typedef struct moq_d18_fetch_ok {
    bool               end_of_track;
    moq_d18_location_t end;
    moq_bytes_t        track_properties;   /* borrowed from payload */
    bool               track_properties_unsupported;  /* mandatory prop present */
} moq_d18_fetch_ok_t;

MOQ_API moq_result_t moq_d18_encode_fetch_ok(moq_buf_writer_t *w,
                                             bool end_of_track,
                                             moq_d18_location_t end,
                                             moq_bytes_t track_properties);
MOQ_API moq_result_t moq_d18_decode_fetch_ok(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_d18_fetch_ok_t *out);

/* FETCH_HEADER lead on a fetch data stream: vi64 type (0x05) + Request ID. */
MOQ_API moq_result_t moq_d18_encode_fetch_header(moq_buf_writer_t *w,
                                                 uint64_t request_id);
MOQ_API moq_result_t moq_d18_decode_fetch_header(moq_buf_reader_t *r,
                                                 uint64_t *out_request_id);

MOQ_API moq_result_t moq_d18_encode_subgroup_header(
    moq_buf_writer_t *w, const moq_d18_subgroup_header_t *hdr);
MOQ_API moq_result_t moq_d18_decode_subgroup_header(
    moq_buf_reader_t *r, moq_d18_subgroup_header_t *out);

/* Validate a draft-18 Object Property KVP block (§11.2.1.2): structurally
 * well-formed, and free of any Mandatory Track Property (0x4000-0x7FFF, §2.5.1)
 * including one hidden inside IMMUTABLE_PROPERTIES (§12.7) — a mandatory property
 * carried as an Object Property makes the object malformed. Returns MOQ_OK or
 * MOQ_ERR_PROTO. (Track Properties, where a mandatory property yields a
 * request-level UNSUPPORTED_EXTENSION rather than a fault, are scanned by the
 * control-message decoders.) */
MOQ_API moq_result_t moq_d18_validate_properties(const uint8_t *data, size_t len);

/* Lenient Track-Property scan reporting whether DYNAMIC_GROUPS (0x30) is
 * present with value 1 (SS12.6). Structural failure / value above 1 returns
 * MOQ_ERR_PROTO with *out false. */
MOQ_API moq_result_t moq_d18_scan_dynamic_groups(const uint8_t *data,
                                                 size_t len,
                                                 bool *out_dynamic_groups);

/*
 * OBJECT_DATAGRAM (§11.3.1): a single object in a datagram. The Type takes the
 * form 0b00X0XXXX (0x00-0x0F / 0x20-0x2F); the present fields are selected by
 * bits in the Type. Field order: Type, Track Alias, Group ID, [Object ID],
 * [Publisher Priority], [Properties: vi64 length + KVP], [Object Status vi64],
 * else the rest of the datagram is the payload (no length field). Properties are
 * the draft-18 vi64 KVP block (validated with moq_d18_validate_properties).
 */
#define MOQ_D18_DGRAM_BIT_PROPERTIES     0x01u
#define MOQ_D18_DGRAM_BIT_END_OF_GROUP   0x02u
#define MOQ_D18_DGRAM_BIT_ZERO_OBJECT_ID 0x04u
#define MOQ_D18_DGRAM_BIT_DEFAULT_PRIO   0x08u
#define MOQ_D18_DGRAM_BIT_STATUS         0x20u
/* Padding datagram (§11.5.2): type then all-zero bytes; the receiver discards it. */
#define MOQ_D18_PADDING_DATAGRAM         ((uint64_t)0x132B3E29u)

typedef struct moq_d18_object_datagram {
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
} moq_d18_object_datagram_t;

/* Decode an OBJECT_DATAGRAM. Returns MOQ_OK for an object datagram, MOQ_DONE for
 * a padding datagram (caller discards), or MOQ_ERR_PROTO for a malformed/invalid
 * datagram (caller closes the session). */
MOQ_API moq_result_t moq_d18_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d18_object_datagram_t *out);

MOQ_API moq_result_t moq_d18_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d18_object_datagram_t *dg);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CONTROL_D18_H */
