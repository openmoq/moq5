#ifndef MOQ_WIRE_MESSAGES_D22_INTERNAL_H
#define MOQ_WIRE_MESSAGES_D22_INTERNAL_H

#include "params_d22_internal.h"

#define MOQ_D22_REQUEST_UPDATE UINT64_C(0x02)
#define MOQ_D22_SUBSCRIBE UINT64_C(0x03)
#define MOQ_D22_SUBSCRIBE_OK UINT64_C(0x04)
#define MOQ_D22_REQUEST_ERROR UINT64_C(0x05)
#define MOQ_D22_PUBLISH_NAMESPACE UINT64_C(0x06)
#define MOQ_D22_REQUEST_OK UINT64_C(0x07)
#define MOQ_D22_NAMESPACE UINT64_C(0x08)
#define MOQ_D22_PUBLISH_DONE UINT64_C(0x0b)
#define MOQ_D22_TRACK_STATUS UINT64_C(0x0d)
#define MOQ_D22_NAMESPACE_DONE UINT64_C(0x0e)
#define MOQ_D22_PUBLISH_SKIPPED UINT64_C(0x0f)
#define MOQ_D22_FETCH UINT64_C(0x16)
#define MOQ_D22_FETCH_OK UINT64_C(0x18)
#define MOQ_D22_PUBLISH UINT64_C(0x1d)
#define MOQ_D22_PUBLISH_STATE_NOTIFY UINT64_C(0x22)
#define MOQ_D22_SUBSCRIBE_NAMESPACE UINT64_C(0x50)
#define MOQ_D22_SUBSCRIBE_TRACKS UINT64_C(0x51)
#define MOQ_D22_ERROR_REDIRECT UINT64_C(0x34)
#define MOQ_D22_MAX_REASON 1024u

typedef struct moq_d22_message_context {
    /* For REQUEST_OK/ERROR: the request answered; for REQUEST_UPDATE: the
     * initial request. Other messages do not consult request_type. */
    uint64_t request_type;
    /* Needed for namespace redirects answering a REQUEST_UPDATE. */
    uint64_t updated_request_type;
    bool update_from_subscriber;
    uint64_t max_filter_ranges;
    bool cluster;
} moq_d22_message_context_t;

/* A semantic tagged message. Only fields selected by type are active.
 * All spans borrow the input. Request identity/parity, request lifetime,
 * redirect role/URI semantics remain session work. Known property value ranges
 * and immutable-container structure are validated by the codec. */
typedef struct moq_d22_message {
    uint64_t type;
    uint64_t request_id;
    moq_d22_namespace_t ns;
    moq_bytes_t track_name;
    uint64_t track_alias;
    moq_d22_params_t params;
    moq_bytes_t properties;
    uint64_t code;
    uint64_t retry_interval;
    uint64_t stream_count;
    moq_bytes_t reason;
    moq_bytes_t redirect_uri;
    uint8_t end_of_track;
    moq_d22_location_t end_location;
    /* Decode only: the Track Properties carry a Mandatory Track Property
     * (§3.7, 0x4000-0x7FFF), in the mutable list or inside
     * IMMUTABLE_PROPERTIES. No such type is understood, so the receiver of
     * PUBLISH, SUBSCRIBE_OK or FETCH_OK refuses or cancels the track. */
    bool mandatory_track_property;
} moq_d22_message_t;

/* Pure validation of borrowed original Track Properties, including known
 * priority/order/dynamic-groups ranges inside immutable containers. Unknown
 * odd properties remain opaque, and a Mandatory Track Property is not itself
 * malformed. No bytes are rewritten or retained. */
moq_result_t moq_d22_validate_track_properties(const uint8_t *data, size_t len);

/* COMPLETE body without envelope; truncated fields are PROTO. Output commits
 * only on success. REQUEST_OK/UPDATE legal parameters require context. SETUP
 * and GOAWAY are handled by control_d22.c. Unknown message types are PROTO.
 * INVALID_FILTER means the entire body passed syntax/scope/exact-end checks;
 * output still remains unchanged. Session integration must retain the body
 * and process validated REGISTER tokens even on this request refusal (8.9).
 * Do not use partial output or mutate a token cache while decoding. */
moq_result_t moq_d22_decode_message(uint64_t type, const uint8_t *body,
                                  size_t len,
                                  const moq_d22_message_context_t *context,
                                  moq_d22_message_t *out);
/* Session-facing scan: also commits the fully validated body on INVALID_FILTER,
 * retaining request identity and borrowed REGISTER tokens for a local refusal.
 * Every other failure leaves output unchanged, including malformed fields after
 * an invalid filter. Individual refused ranges must not be applied. */
moq_result_t moq_d22_scan_message(uint64_t type, const uint8_t *body, size_t len,
                                const moq_d22_message_context_t *context,
                                moq_d22_message_t *out);
/* Encode full envelope and body, with a semantic parameter array replacing
 * message.params. Namespace and property spans must be valid and borrowed
 * separately from the output buffer. Invalid input is INVAL; capacity refusal
 * is BUFFER. Committed cursor is unchanged on any failure. */
moq_result_t moq_d22_encode_message(moq_buf_writer_t *w,
                                  const moq_d22_message_t *message,
                                  const moq_d22_param_t *params, size_t count,
                                  const moq_d22_message_context_t *context);

#endif
