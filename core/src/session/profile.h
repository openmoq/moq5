#ifndef MOQ_PROFILE_H
#define MOQ_PROFILE_H

#include "moq/session.h"

struct moq_buf_writer;
struct moq_buf_reader;
struct moq_request_endpoint;
struct moq_subscribe_encode_args;
struct moq_subscribe_ok_encode_args;
struct moq_request_error_encode_args;
struct moq_request_update_encode_args;
struct moq_request_update_ok_encode_args;
struct moq_publish_ok_encode_args;
struct moq_publish_state_encode_args;
struct moq_decoded_subgroup_header;
struct moq_decoded_object_header;
struct moq_subgroup_header_encode_args;
struct moq_object_header_encode_args;
struct moq_goaway_encode_args;
struct moq_publish_namespace_encode_args;
struct moq_publish_namespace_cancel_encode_args;
struct moq_subscribe_namespace_encode_args;
struct moq_namespace_msg_encode_args;
struct moq_subscribe_tracks_encode_args;
struct moq_namespace_update_encode_args;
struct moq_decoded_ns_sub_request;
struct moq_decoded_ns_sub_response;
struct moq_fetch_encode_args;
struct moq_publish_encode_args;
struct moq_finish_publish_encode_args;
struct moq_accept_fetch_encode_args;
struct moq_decoded_fetch_stream_header;
struct moq_fetch_prior_object;
struct moq_decoded_fetch_object;
struct moq_fetch_object_encode_args;
struct moq_track_status_encode_args;
struct moq_track_status_ok_encode_args;
struct moq_datagram_encode_args;
struct moq_decoded_object_datagram;
struct moq_obj_gaps;

/* Initial PUBLISH arguments; legacy profiles ignore the D22-only tail. */
typedef struct moq_publish_encode_args {
    uint64_t request_id;
    moq_namespace_t track_namespace;
    moq_bytes_t track_name;
    uint64_t track_alias;
    bool has_forward;
    bool forward;
    const uint8_t *track_properties;
    size_t track_properties_len;
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
    /* History-derived, never caller-supplied. */
    bool has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    moq_publish_initial_parameters_t initial;
    bool has_expires;
    uint64_t expires_ms;
} moq_publish_encode_args_t;

/* D16/18 delivery choices; D22 permits only optional expiry. */
typedef struct moq_publish_ok_encode_args {
    uint64_t request_id;
    uint8_t subscriber_priority;
    uint8_t group_order;
    bool has_new_group_request;
    uint64_t new_group_request;
    bool has_filter;
    moq_subscribe_filter_t filter;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    bool has_forward;
    bool forward;
    bool has_expires;
    uint64_t expires_ms;
    /* The subscriber's delivery timeouts, checked by the session. */
    bool dt_has_object;
    uint64_t dt_object_ms;
    bool dt_has_subgroup;
    uint64_t dt_subgroup_ms;
} moq_publish_ok_encode_args_t;

/*
 * Classification of an inbound unidirectional stream, for profiles that use a
 * unidirectional control channel. Profiles that carry control on a single
 * bidirectional stream (e.g. draft-16) leave the classifier hook NULL; the
 * session core then treats every peer unidirectional stream as DATA, which
 * preserves that behavior. DATA is value 0 so a zeroed default reads as "data".
 */
typedef enum moq_uni_class {
    MOQ_UNI_CLASS_DATA      = 0,  /* object data stream (e.g. subgroup/fetch) */
    MOQ_UNI_CLASS_CONTROL   = 1,  /* the peer's unidirectional control channel */
    MOQ_UNI_CLASS_PADDING   = 2,  /* padding stream; bytes are discarded */
    MOQ_UNI_CLASS_NEED_MORE = 3,  /* not enough leading bytes to classify yet */
    MOQ_UNI_CLASS_UNKNOWN   = 4,  /* unrecognized; caller treats as a violation */
} moq_uni_class_t;

/*
 * Internal profile ops vtable.
 *
 * A profile translates between draft-specific wire encodings and the
 * semantic records consumed by the session core. Each session binds to
 * exactly one profile, selected by moq_version_t at creation time.
 *
 * Profile state is co-allocated inside the session's contiguous block.
 * init_in_place initializes pre-allocated memory; destroy cleans up
 * without freeing (the block is freed by session_destroy).
 */

/* §9.8: exact per-type delivery-timeout scan result (raw wire ms; has_
 * preserves d18 omission-vs-explicit-zero). */
typedef struct moq_dt_scan {
    bool     has_object;
    uint64_t object_ms;
    bool     has_subgroup;
    uint64_t subgroup_ms;
} moq_dt_scan_t;

typedef struct moq_profile_ops {
    /*
     * The wire profile this table implements. Invariant, asserted by the
     * version-registry test: moq_profile_lookup(v)->version == v for every
     * version this build supports. A profile that omits it leaves 0 under
     * the designated initializers below and fails that assertion, rather
     * than silently reporting the wrong draft. This is the single authority
     * for a session's version -- the session stores no separate copy.
     */
    moq_version_t version;

    size_t state_size;
    size_t state_align;

    void (*init_in_place)(void *profile_state,
                          const moq_session_cfg_t *cfg);
    void (*destroy)(void *profile_state);

    moq_result_t (*start)(moq_session_t *s);
    moq_result_t (*process_control_data)(moq_session_t *s,
                                          const uint8_t *data, size_t len,
                                          size_t *out_consumed);

    /* -- Request admission ops ---------------------------------------- */

    /* has_request_capacity: query-only, not yet wired into a public API
     * caller. Reserved for future use (e.g. pre-check before batched
     * request creation). prepare_request already checks capacity. */
    bool (*has_request_capacity)(const moq_session_t *s);
    moq_result_t (*prepare_request)(moq_session_t *s,
                                    struct moq_request_endpoint *out);
    void (*commit_request)(moq_session_t *s,
                           const struct moq_request_endpoint *ep);
    void (*abort_request)(moq_session_t *s,
                          const struct moq_request_endpoint *ep);
    moq_result_t (*validate_inbound_request)(moq_session_t *s,
                                             uint64_t wire_request_id,
                                             struct moq_request_endpoint *out);
    void (*commit_inbound_request)(moq_session_t *s,
                                   const struct moq_request_endpoint *ep);
    void (*release_request)(moq_session_t *s,
                            const struct moq_request_endpoint *ep);
    moq_result_t (*grant_capacity)(moq_session_t *s, uint64_t new_capacity,
                                   uint64_t now_us);
    uint64_t (*peer_request_capacity)(const moq_session_t *s);
    uint64_t (*local_request_capacity)(const moq_session_t *s);

    /* -- Track alias ops ---------------------------------------------- */

    uint64_t (*next_track_alias)(const moq_session_t *s);
    void (*advance_track_alias)(moq_session_t *s, uint64_t next_after);

    /* -- Outbound subscribe-family encode ops ------------------------- */

    moq_result_t (*encode_subscribe)(moq_session_t *s,
                                      struct moq_buf_writer *w,
                                      const struct moq_subscribe_encode_args *args);
    moq_result_t (*encode_subscribe_ok)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         const struct moq_subscribe_ok_encode_args *args);
    moq_result_t (*encode_request_ok)(moq_session_t *s,
                                       struct moq_buf_writer *w,
                                       uint64_t request_id);
    /* REQUEST_UPDATE_OK response: REQUEST_OK carrying the resolved LARGEST_OBJECT
     * (both drafts) and EXPIRES (draft-18 only), empty Track Properties. */
    moq_result_t (*encode_request_update_ok)(moq_session_t *s,
                                       struct moq_buf_writer *w,
                                       const struct moq_request_update_ok_encode_args *args);
    moq_result_t (*encode_request_error)(moq_session_t *s,
                                          struct moq_buf_writer *w,
                                          const struct moq_request_error_encode_args *args);
    /* Encode a per-request-stream GOAWAY (§10.4): New Session URI + Timeout, no
     * Request ID. NULL on profiles without per-request GOAWAY (draft-16) so the
     * outbound sender returns MOQ_ERR_INVAL. */
    moq_result_t (*encode_request_goaway)(moq_session_t *s,
                                           struct moq_buf_writer *w,
                                           const uint8_t *uri, size_t uri_len,
                                           uint64_t timeout_ms);
    moq_result_t (*encode_request_update)(moq_session_t *s,
                                           struct moq_buf_writer *w,
                                           const struct moq_request_update_encode_args *args);
    moq_result_t (*encode_unsubscribe)(moq_session_t *s,
                                        struct moq_buf_writer *w,
                                        uint64_t request_id);
    moq_result_t (*encode_goaway)(moq_session_t *s,
                                    struct moq_buf_writer *w,
                                    const struct moq_goaway_encode_args *args);
    moq_result_t (*encode_publish_namespace)(moq_session_t *s,
                                              struct moq_buf_writer *w,
                                              const struct moq_publish_namespace_encode_args *args);
    moq_result_t (*encode_publish_namespace_done)(moq_session_t *s,
                                                    struct moq_buf_writer *w,
                                                    uint64_t request_id);
    moq_result_t (*encode_publish_namespace_cancel)(moq_session_t *s,
                                                      struct moq_buf_writer *w,
                                                      const struct moq_publish_namespace_cancel_encode_args *args);

    /* -- Outbound publish encode ops ---------------------------------- */

    moq_result_t (*encode_publish)(moq_session_t *s,
                                    struct moq_buf_writer *w,
                                    const struct moq_publish_encode_args *args);
    /* Lenient extraction of dynamic-group support (Track Property /
     * Extension 0x30 == 1) from an OUTBOUND track-properties blob, so the
     * sender can latch what it advertised and gate inbound new-group
     * requests against it. Never errors: a malformed blob reads as "no
     * support" (outbound validation lives in the encoders). */
    bool (*track_properties_dynamic_groups)(const uint8_t *data, size_t len);
    /* `in` with DYNAMIC_GROUPS = 1 added in this draft's track-property
     * encoding, written to `out`. A set that already advertises it is copied
     * unchanged; one that carries DYNAMIC_GROUPS with another value, or does
     * not parse, is MOQ_ERR_INVAL; MOQ_ERR_BUFFER when `out` is too small. */
    moq_result_t (*track_properties_add_dynamic_groups)(const uint8_t *in, size_t len,
                                                        uint8_t *out, size_t cap,
                                                        size_t *out_len);
    /* The publisher's group order preference -- the
     * DEFAULT_PUBLISHER_GROUP_ORDER Track Property (0x22) -- in a track
     * properties blob: MOQ_GROUP_ORDER_ASCENDING / _DESCENDING, or
     * MOQ_GROUP_ORDER_DEFAULT when it is absent or unreadable. Never
     * errors (validation lives in the encoders and the inbound decoders). */
    uint8_t (*track_properties_group_order)(const uint8_t *data, size_t len);
    /* `in` with DEFAULT_PUBLISHER_GROUP_ORDER = `order` (ASCENDING or
     * DESCENDING) added, as track_properties_add_dynamic_groups adds its
     * property: a set that already carries `order` is copied unchanged, one
     * with another value is MOQ_ERR_INVAL. */
    moq_result_t (*track_properties_add_group_order)(const uint8_t *in, size_t len,
                                                     uint8_t order, uint8_t *out,
                                                     size_t cap, size_t *out_len);
    /* `in` with the publisher's OBJECT_ (or, with `subgroup`, SUBGROUP_)
     * DELIVERY_TIMEOUT = `ms` added the same way. Draft-16's single
     * DELIVERY_TIMEOUT stands for either. */
    moq_result_t (*track_properties_add_delivery_timeout)(
        const uint8_t *in, size_t len, bool subgroup, uint64_t ms,
        uint8_t *out, size_t cap, size_t *out_len);
    /* MAX_CACHE_DURATION (0x04) in a track properties blob, searching the
     * mutable list and Immutable Properties: true with its milliseconds when
     * present. A blob that does not parse states nothing past the failure. */
    bool (*track_properties_max_cache)(const uint8_t *data, size_t len,
                                       uint64_t *out_ms);
    /* Pure Track Property scan: property absence is 128; invalid values are
     * refused before owner commit. NULL preserves 128-only compatibility
     * without claiming inheritance support for that profile's format. */
    moq_result_t (*scan_default_publisher_priority)(const uint8_t *data,
                                                    size_t len, uint8_t *out);
    /* Pure original Track Property validation before local advance or omission.
     * NULL preserves the profile's existing local validation rules. */
    moq_result_t (*validate_track_properties)(const uint8_t *data, size_t len);

    moq_result_t (*encode_publish_ok)(moq_session_t *s,
                                       struct moq_buf_writer *w,
                                       const struct moq_publish_ok_encode_args *args);
    moq_result_t (*encode_publish_done)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         const struct moq_finish_publish_encode_args *args);
    /* The publisher's unilateral state notification on a subscription's
     * request stream. NULL on profiles without a wire form for it. */
    moq_result_t (*encode_publish_state_notify)(moq_session_t *s,
                                                 struct moq_buf_writer *w,
                                                 const struct moq_publish_state_encode_args *args);

    /* -- Outbound fetch encode ops ------------------------------------ */

    moq_result_t (*encode_fetch)(moq_session_t *s,
                                  struct moq_buf_writer *w,
                                  const struct moq_fetch_encode_args *args);
    moq_result_t (*encode_fetch_cancel)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         uint64_t request_id);
    moq_result_t (*encode_fetch_ok)(moq_session_t *s,
                                     struct moq_buf_writer *w,
                                     const struct moq_accept_fetch_encode_args *args);
    moq_result_t (*encode_fetch_header)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         uint64_t request_id);
    moq_result_t (*decode_fetch_header)(moq_session_t *s,
                                         struct moq_buf_reader *r,
                                         struct moq_decoded_fetch_stream_header *out);
    uint32_t (*classify_data_stream)(const uint8_t *data, size_t len);
    moq_result_t (*classify_bidi_stream)(moq_session_t *s,
                                          moq_stream_ref_t ref,
                                          const uint8_t *data, size_t len);

    /* -- Fetch data-plane decode/encode ops --------------------------- */

    moq_result_t (*decode_fetch_object)(moq_session_t *s,
                                         struct moq_buf_reader *r,
                                         moq_group_order_t effective_order,
                                         const struct moq_fetch_prior_object *prev,
                                         struct moq_decoded_fetch_object *out);
    moq_result_t (*encode_fetch_object)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         moq_group_order_t effective_order,
                                         const struct moq_fetch_object_encode_args *args,
                                         const struct moq_fetch_prior_object *prev);
    moq_result_t (*encode_fetch_range)(moq_session_t *s,
                                        struct moq_buf_writer *w,
                                        uint32_t range_kind,
                                        uint64_t group_id,
                                        uint64_t object_id);

    /* -- Outbound namespace-sub encode ops ---------------------------- */

    moq_result_t (*encode_subscribe_namespace)(moq_session_t *s,
                                                struct moq_buf_writer *w,
                                                const struct moq_subscribe_namespace_encode_args *args);
    moq_result_t (*encode_namespace_msg)(moq_session_t *s,
                                          struct moq_buf_writer *w,
                                          const struct moq_namespace_msg_encode_args *args);

    /* -- Outbound SUBSCRIBE_TRACKS encode ops (draft-18) -------------- */

    moq_result_t (*encode_subscribe_tracks)(moq_session_t *s,
                                            struct moq_buf_writer *w,
                                            const struct moq_subscribe_tracks_encode_args *args);
    moq_result_t (*encode_publish_blocked)(moq_session_t *s,
                                           struct moq_buf_writer *w,
                                           const moq_namespace_t *suffix,
                                           moq_bytes_t track_name);
    /* REQUEST_UPDATE of a PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE or
     * SUBSCRIBE_TRACKS, scoped by the family it updates. NULL on profiles
     * without namespace updates. */
    moq_result_t (*encode_namespace_update)(moq_session_t *s,
                                            struct moq_buf_writer *w,
                                            const struct moq_namespace_update_encode_args *args);

    /* -- Inbound namespace-sub decode ops ----------------------------- */

    moq_result_t (*decode_ns_sub_request)(moq_session_t *s,
                                           const uint8_t *data, size_t len,
                                           struct moq_decoded_ns_sub_request *out);

    moq_result_t (*decode_ns_sub_response)(moq_session_t *s,
                                            const uint8_t *data, size_t len,
                                            bool got_response,
                                            uint64_t expected_request_id,
                                            struct moq_decoded_ns_sub_response *out);

    /* -- Inbound data-plane decode ops -------------------------------- */

    moq_result_t (*decode_subgroup_header)(moq_session_t *s,
                                            struct moq_buf_reader *r,
                                            struct moq_decoded_subgroup_header *out);
    /* -- Outbound data-plane encode ops -------------------------------- */

    moq_result_t (*encode_subgroup_header)(moq_session_t *s,
                                            struct moq_buf_writer *w,
                                            const struct moq_subgroup_header_encode_args *args);
    moq_result_t (*encode_object_header)(moq_session_t *s,
                                          struct moq_buf_writer *w,
                                          const struct moq_object_header_encode_args *args);

    /* Encode the object payload-length prefix that follows an Object Properties
     * block in the two-action property write (the bytes after the property rcbuf):
     * the Object Payload Length, plus the Object Status when with_status is true
     * and the length is zero. Draft-specific varint width (vi64 vs QUIC varint),
     * so it must not be open-coded in the draft-neutral write paths. */
    moq_result_t (*encode_object_payload_prefix)(moq_session_t *s,
                                                  struct moq_buf_writer *w,
                                                  uint64_t payload_len,
                                                  bool with_status);

    /* Validate an outbound Object Properties block before it is queued: the draft
     * may forbid certain property types (draft-18 rejects a Mandatory Track
     * Property, 0x4000-0x7FFF, carried as an object property — symmetric with the
     * inbound malformed check). Returns MOQ_OK to allow, MOQ_ERR_INVAL to refuse.
     * NULL or a no-op permits everything (draft-16 has no such restriction). */
    moq_result_t (*validate_object_properties)(moq_session_t *s,
                                                const uint8_t *props, size_t len);

    /* -- Inbound data-plane decode ops ---------------------------------- */

    moq_result_t (*decode_object_header)(moq_session_t *s,
                                          struct moq_buf_reader *r,
                                          bool has_extensions,
                                          uint64_t prev_object_id,
                                          bool has_prev_object,
                                          struct moq_decoded_object_header *out);

    /* -- Outbound track-status encode ops ----------------------------- */

    moq_result_t (*encode_track_status)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         const struct moq_track_status_encode_args *args);

    /* TRACK_STATUS_OK response: REQUEST_OK with LARGEST_OBJECT / EXPIRES params
     * (draft-18 adds an opaque Track Properties tail). */
    moq_result_t (*encode_track_status_ok)(moq_session_t *s,
                                            struct moq_buf_writer *w,
                                            const struct moq_track_status_ok_encode_args *args);

    /* -- Datagram encode/decode ops ----------------------------------- */

    moq_result_t (*encode_object_datagram)(moq_session_t *s,
                                            struct moq_buf_writer *w,
                                            const struct moq_datagram_encode_args *args);
    moq_result_t (*decode_object_datagram)(moq_session_t *s,
                                            const uint8_t *data, size_t len,
                                            struct moq_decoded_object_datagram *out);

    /* -- Request-stream correlation capability + hooks ---------------- */

    /* Track Namespace cardinality for full-name request APIs. Draft-16
     * requires 1..32 fields; draft-18 permits the root namespace (0 fields).
     * Present fields are non-empty in both profiles. */
    size_t min_track_namespace_fields;

    /*
     * Capability: when true, this profile correlates requests by the identity
     * of a dedicated bidirectional stream opened per request (the request
     * travels on its own bidi stream and the response returns on it), rather
     * than by a wire request-id carried on a shared control channel. The
     * session core asks this capability instead of testing a draft version.
     * Draft-16 sets false (it correlates by request-id).
     */
    bool uses_request_streams;

    /*
     * Capability: the largest Object Payload Length this profile can encode.
     * Draft-16 uses a QUIC varint (2^62-1); draft-18 uses vi64 and therefore
     * accepts the full uint64_t range. Draft-neutral write paths must use this
     * instead of imposing the draft-16 ceiling on every profile.
     */
    uint64_t object_payload_len_max;

    /*
     * Capability: the largest value a Location field (group / object id) can
     * carry on the wire for this profile. Draft-16 uses the QUIC varint
     * (MOQ_QUIC_VARINT_MAX == 2^62-1); draft-18's vi64 spans the full 64-bit
     * range (MOQ_VI64_MAX == UINT64_MAX, §1.4.1). The session core asks this
     * capability instead of testing a draft version when bounding / advancing
     * Largest Object locations (design §4).
     */
    uint64_t location_varint_max;

    /*
     * Capability: true if this profile's FETCH object header can carry the
     * datagram-preference bit. Draft-18 sets true; draft-16 sets false (no
     * datagram bit in the fetch header). The session core asks this capability
     * (moq_session_supports_fetch_datagram) instead of testing a draft version,
     * so the query never leaks the version.
     */
    bool fetch_datagram_supported;

    /*
     * Capability: the largest REQUEST_ERROR code this profile can put on the
     * wire. Draft-16 encodes it as a QUIC varint (MOQ_QUIC_VARINT_MAX ==
     * 2^62-1, §9.8); draft-18 encodes it as a vi64, which spans the full
     * 64-bit range (MOQ_VI64_MAX == UINT64_MAX, §1.4.1 / §10.6.2). Public
     * reject/cancel APIs ask this capability instead of testing a draft
     * version, and refuse an unrepresentable value BEFORE any mutation
     * rather than truncating it.
     */
    uint64_t request_error_wire_max;

    /*
     * The semantic REQUEST_ERROR code this profile surfaces for a raw decoded
     * wire value. Draft-16 states no unknown-code rule, so it returns the raw
     * value unchanged; draft-18 §15 (:6411-6415) requires every unknown code,
     * GREASE included, to be treated as INTERNAL_ERROR, so it maps anything
     * outside its own registry (§15.10.2). Called with the FULL decoded
     * value, and only after any raw-value dispatch (REDIRECT recognition)
     * has already happened.
     */
    moq_request_error_t (*semantic_request_error)(uint64_t raw);

    /*
     * Whether this side may request a descending FETCH. Draft-16 uses
     * absolute Group IDs; draft-18 and draft-22 read the response's Group ID
     * deltas in the request's retained order.
     */
    bool fetch_descending_supported;

    /*
     * Capability: true if this profile carries control on a local/peer pair of
     * unidirectional control channels (uni-control-pair topology), so the
     * transport bridge runs in uni-control-pair mode. Draft-16 sets false.
     * Kept separate from classify_uni_stream so "has a uni stream classifier"
     * and "uses uni-control topology" remain independent capabilities.
     */
    bool uses_uni_control_channel;

    /*
     * Classify the leading bytes of an inbound unidirectional stream, for
     * profiles that use a unidirectional control channel. Inspects only the
     * leading stream-type bytes (no message-body parsing). NULL for profiles
     * that carry control on a bidirectional stream (draft-16); the core then
     * treats every peer unidirectional stream as MOQ_UNI_CLASS_DATA.
     */
    moq_uni_class_t (*classify_uni_stream)(const uint8_t *data, size_t len);

    /*
     * Validate and register an inbound request that arrived on its own
     * bidirectional stream, for stream-correlated profiles. Receives both the
     * bidi stream ref and the wire request-id parsed from the request message
     * (such profiles still carry a request-id for cross-references). Fills *out
     * with the bound endpoint (has_stream_ref set, and has_request_id when a
     * request-id is present). NULL for profiles that do not open a bidi stream
     * per request (draft-16).
     */
    moq_result_t (*validate_inbound_request_stream)(moq_session_t *s,
                                                    moq_stream_ref_t ref,
                                                    uint64_t msg_type,
                                                    uint64_t wire_request_id,
                                                    struct moq_request_endpoint *out);

    /*
     * Decode and dispatch a request message buffered on an inbound request
     * bidi stream (stream-correlated profiles). The session core owns the
     * reserved slot, buffering, and lifecycle; this op decodes the wire message
     * from buf[0..len) and dispatches it through the core request handlers
     * (e.g. session_core_on_subscribe) into the reserved slot.
     *
     * On a complete message: sets *out_consumed to the bytes consumed and
     * returns MOQ_OK. On an incomplete message: sets *out_consumed to 0 and
     * returns MOQ_OK (the core retains the buffer / handles FIN). On a
     * malformed message it closes the session and returns its result.
     */
    moq_result_t (*process_request_stream)(moq_session_t *s,
                                           moq_stream_ref_t stream_ref,
                                           int slot,
                                           const uint8_t *buf, size_t len,
                                           bool fin, size_t *out_consumed);
    /*
     * Decode and dispatch a response message buffered on an outbound request
     * bidi stream (stream-correlated profiles). The session core owns the slot,
     * buffering, and lifecycle; it passes the request `kind` (the core branches
     * on registry kind, not the wire type) so the op routes the decoded response
     * to the right handlers: a subscription expects SUBSCRIBE_OK / REQUEST_ERROR
     * (session_core_on_subscribe_ok / _error), a fetch expects FETCH_OK /
     * REQUEST_ERROR (session_core_on_fetch_ok / _error). The first response must
     * be valid for the kind, else the op closes the session. `kind` carries a
     * moq_request_kind_t value (passed as uint32_t to avoid an internal-header
     * dependency here).
     *
     * Same out_consumed / incomplete / malformed contract as
     * process_request_stream.
     */
    moq_result_t (*process_response_stream)(moq_session_t *s,
                                            moq_stream_ref_t stream_ref,
                                            int slot, uint32_t kind,
                                            const uint8_t *buf, size_t len,
                                            bool fin, size_t *out_consumed);

    /* §9.8 pure timeout-property scanner (bytes in, exact per-type raw-ms
     * out; no session access). strict_local selects the LOCAL wrapper
     * semantics (structural failure errors); inbound passes false (d16
     * stays lenient-opaque on structure; semantic violations -- wire zero,
     * duplicates, nested immutable -- error in every mode). */
    moq_result_t (*scan_delivery_timeouts)(const uint8_t *data, size_t len,
                                           bool strict_local,
                                           moq_dt_scan_t *out);
    /* Codec table for the shared request-stream engine (request_stream.c).
     * Set by profiles whose process_request_stream / process_response_stream
     * point at the engine; NULL for profiles that carry requests on the
     * control stream. */
    const struct moq_request_codec *request_codec;

    /* -- Behavioural capabilities ------------------------------------- *
     * The session core asks these instead of testing a draft number. Each is
     * documented by the behaviour it enables; a profile sets every one. */

    /* The profile's wire implementation is complete enough to be offered to
     * peers. The endpoint offers only wire-ready versions. */
    bool wire_ready;
    /* Preserve bounded original-sender lifecycle input while the application
     * answers the initial request. A complete UPDATE waits for that answer;
     * PUBLISH staging transfers its lifecycle suffix to the committed owner
     * (a head PUBLISH_DONE still dispatches immediately). False retains the
     * profile's previous pending-input guards and staging-tail refusal. */
    bool request_retains_pending_original_input;
    /* A FIN on the requester's half of an established request stream only
     * closes that direction; it is not a cancellation of the request. False
     * means a requester FIN before the response is a truncated request. */
    bool request_fin_is_not_cancel;
    /* A FIN that arrives before the required messages fails that request,
     * not the session. False means the session closes with a protocol
     * violation. */
    bool premature_fin_fails_request;
    /* A retired request-stream drain reference is released only by the
     * peer's FIN or RESET of the receive half; a STOP of the local send half
     * cannot release it. */
    bool request_drain_requires_receive_terminal;
    /* Several concurrent subscriptions to one track are legal, in either
     * role: a publisher may give them one shared alias (its own choice is
     * a distinct alias per subscription), an object matching several of
     * them is sent once per subscription, and a subscriber attributes an
     * object on a shared alias by re-applying each subscription's filter.
     * False rejects a second subscription to the same track. */
    bool allows_concurrent_subscriptions;
    /* The REQUEST_ERROR code that refuses a second subscription to a track
     * when concurrent subscriptions are not allowed: a code registered by
     * this profile's own error table. Not consulted when they are. */
    uint64_t duplicate_subscription_error;
    /* Initial PUBLISH carries typed delivery parameters and the subscriber
     * may supply an expiry on acceptance. The immutable received parameter
     * snapshot is available to the application; older profiles do not expose
     * this parameter set merely because they support PUBLISH. */
    bool publish_initial_parameters;
    /* SUBSCRIBE updates carry semantic location and independent timeout
     * options, including an empty refresh, with a lossless event snapshot. */
    bool subscription_update_options;
    /* The PUBLISH response carries the subscriber's delivery parameters.
     * False forbids acceptance overrides: a later change must be explicitly
     * requested, never synthesized as part of acceptance. */
    bool publish_ok_carries_params;
    /* FETCH has joining forms that reference a subscription. False refuses a
     * joining fetch request. */
    bool supports_joining_fetch;
    /* Commit the profile's subscription-update location effect using the
     * acknowledged snapshot, before the caller changes Forward. `slot` is a
     * live subscription entry in either role. Called only after response/event
     * queueing succeeds; cannot fail, allocate, queue output or retain pointers.
     * Draft-18 section 5.1 replaces the Joining Location on Forward 0 -> 1,
     * preserving absence as well as coordinates. NULL leaves it unchanged. */
    void (*commit_subscribe_update_location)(moq_session_t *s, int slot,
        bool has_forward, bool forward, bool has_largest,
        uint64_t largest_group, uint64_t largest_object);
    /* A subscription may request fill streams for past objects. False
     * refuses a fill request. */
    bool supports_fill;
    /* FETCH carries what moq_fetch_request_options_t describes: its range as
     * a Location Filter, which may start relative to the Largest Object and
     * may leave the end open, and a FILL_TIMEOUT answered with Timed-Out
     * gaps. The decoded request reports both (has_options). False: a FETCH
     * range is explicit, and a request asking for either is refused. */
    bool fetch_request_options;
    /* A fetch stream can mark a range End of Timed-Out. False refuses that
     * marker kind. */
    bool fetch_range_timed_out;
    /* The PUBLISH_DONE Stream Count value meaning the publisher could not
     * count its streams: the terminal is deferred to the wait deadline
     * instead of being satisfied by counting. */
    uint64_t stream_count_unknown;
    /* A subgroup's delivery timer starts at its FIN and honours the first
     * object's override. False starts it as the stream opens. */
    bool subgroup_timer_at_fin;
    /* An object's delivery clock starts when its header has been provided.
     * False starts it at the object's first payload byte. */
    bool object_timer_at_header;
    /* The first object of a subgroup may carry either delivery timeout as an
     * Object Property, overriding the track-level value for that subgroup.
     * False ignores such properties for timing. */
    bool first_object_timeout_override;
    /* PUBLISH_DONE has a status meaning the subscription ended because its
     * filter range was exhausted. False means passing the filter end does
     * not end a subscription and that status does not exist. */
    bool publish_done_subscription_ended;
    /* A REQUEST_ERROR with code REDIRECT may answer any request family,
     * PUBLISH and SUBSCRIBE_TRACKS included. False limits it to SUBSCRIBE,
     * FETCH, TRACK_STATUS, PUBLISH_NAMESPACE and SUBSCRIBE_NAMESPACE. */
    bool redirect_on_any_request;
    /* Transport facts a session must be told before it starts (bitmask;
     * zero means an unbound start is fine). */
    uint32_t binding_requirements;
    /* The widest Token Type an AUTHORIZATION TOKEN carries on this wire: the
     * ceiling of the integer encoding the profile writes it with. Outgoing
     * tokens above it are refused before any effect. */
    uint64_t auth_token_type_max;
    /* One REQUEST_ERROR may answer a coalesced run of REQUEST_UPDATEs that
     * failed. False answers every update in the run, each by its own id. */
    bool request_update_errors_coalesce;
    /* The bound this side advertises on unanswered REQUEST_UPDATEs per
     * request stream, and the session error for a peer that exceeds it.
     * Zero means none is advertised: unanswered updates are coalesced. */
    uint64_t request_updates_max;
    uint64_t request_updates_exceeded_error;
    /* The REQUEST_ERROR code that refuses a new request arriving after our
     * GOAWAY: GOING_AWAY where the draft registers it, INTERNAL_ERROR where
     * it does not. */
    uint64_t going_away_request_error;
    /* The REQUEST_ERROR code that refuses a request because a local pool or
     * capacity is exhausted, and whether that refusal invites an immediate
     * retry (Retry Interval 1): EXCESSIVE_LOAD, "The responder is overloaded
     * and cannot process the request at this time", where the draft
     * registers it; INTERNAL_ERROR, with no retry, where it does not. */
    uint64_t capacity_request_error;
    bool     capacity_request_error_retryable;
    /* Stream error used to cancel at a local resource limit. Draft-16
     * section 10.4.3 defines CANCELLED for subscriber cancellation; draft-18
     * section 3.3.3 and draft-22 section 12.5 define EXCESSIVE_LOAD for an
     * overloaded endpoint. This is a stream code, not a REQUEST_ERROR. */
    uint64_t resource_limit_stream_error;
    /* The draft reserves the "." and ".session" first namespace fields: a
     * request naming one is refused with DOES_NOT_EXIST before the
     * application sees it, and the application may not publish under one. */
    bool reserved_session_namespaces;
    bool cluster_metadata; /* profile supports negotiated cluster advertisements */
    /* Relative group starts clamp at the location ceiling. False preserves
     * legacy NEXT_GROUP's empty window when no successor is representable. */
    bool relative_group_clamps;
    /* Initial SUBSCRIBE carries semantic admission options and independently
     * selectable object/subgroup delivery timeouts in milliseconds. */
    bool subscription_admission_options;
    /* One DELIVERY_TIMEOUT stands for both timeout types, and it is never 0
     * (draft-16 9.2.2.2 and 11.1). */
    bool single_delivery_timeout;
    /* SUBSCRIBE_TRACKS carries GROUP_ORDER and INCLUDE_PROPERTIES, observed
     * through the request event's options record. */
    bool subscribe_tracks_options;
    /* SUBSCRIBE, FETCH and TRACK_STATUS can carry INCLUDE_PROPERTIES, asking
     * for an answer with or without Track Properties. False refuses a request
     * that selects it. */
    bool include_properties_request;
    /* A received Track that breaks the Malformed Track conditions (draft-22
     * §12.1; draft-16 and draft-18 §2.4.2) fails the subscriptions and
     * fetches carrying it, not the session: Object Property violations that
     * the draft makes Track-level are reported by the data-plane decoders
     * instead of refused, and the session tracks the per-Track ordering and
     * end conditions. False keeps every such violation a session-level
     * PROTOCOL_VIOLATION and tracks nothing. */
    bool malformed_track_local;
    /* The Prior Group ID Gap and Prior Object ID Gap Object Properties, where
     * the draft defines them (NULL where it does not). scan reports the gaps
     * an Object Property block states, neither when the block is malformed;
     * add copies `in` to `out` (distinct buffers) with one gap, the Object
     * gap when `object_gap` is set and the Group gap otherwise, added in type
     * order (MOQ_ERR_INVAL when `in` already states it with another value,
     * MOQ_ERR_BUFFER when `out` is too small). */
    moq_result_t (*scan_object_gaps)(const uint8_t *props, size_t len,
                                     struct moq_obj_gaps *out);
    moq_result_t (*add_object_gap)(const uint8_t *in, size_t len,
                                   bool object_gap, uint64_t value,
                                   uint8_t *out, size_t cap, size_t *out_len);
    /* This side may send REQUEST_UPDATE as the sender of a PUBLISH (on its
     * own request) and as the sender of a FETCH, besides the subscriber's
     * updates every profile sends. */
    bool requester_updates;
    /* Encode one SETUP authorization token (by value) as its Setup Option /
     * Setup Parameter value. NULL on profiles whose SETUP encoder writes the
     * tokens itself. */
    moq_result_t (*encode_setup_token)(struct moq_buf_writer *w,
                                       uint64_t token_type, moq_bytes_t value);
    /* Encode the complete SETUP this side will send into `w` (no queueing),
     * so create can admit only configurations whose SETUP encodes. */
    moq_result_t (*encode_own_setup)(moq_session_t *s, struct moq_buf_writer *w);
} moq_profile_ops_t;

const moq_profile_ops_t *moq_profile_lookup(moq_version_t version);

/* The checks a Track Property set this side publishes must pass under `p`:
 * it parses and states nothing this side may not publish, and its delivery
 * timeouts and default publisher priority read. MOQ_OK or MOQ_ERR_INVAL. */
moq_result_t moq_profile_check_track_properties(const moq_profile_ops_t *p,
                                                const uint8_t *data, size_t len);

/* The Track Property set a track publishes under `p`: `in` with
 * DYNAMIC_GROUPS = 1 and DEFAULT_PUBLISHER_GROUP_ORDER = `group_order` added
 * when declared (MOQ_GROUP_ORDER_DEFAULT adds none), checked as
 * moq_profile_check_track_properties() checks it. *out is allocated from `a`
 * (*out_cap bytes; NULL for an empty set). MOQ_ERR_INVAL for a set the
 * session would refuse or a group order other than the three defined,
 * MOQ_ERR_NOMEM. */
moq_result_t moq_profile_build_track_properties(const moq_profile_ops_t *p,
                                                const moq_alloc_t *a,
                                                const uint8_t *in, size_t len,
                                                bool dynamic_groups,
                                                moq_group_order_t group_order,
                                                uint8_t **out, size_t *out_cap,
                                                size_t *out_len);

/* Every version this build has a profile for, newest first. Returns the
 * count written (at most cap). */
size_t moq_profile_versions(moq_version_t *out, size_t cap);

/* Draft-16 profile ops (defined in profile_d16.c). */
const moq_profile_ops_t *moq_d16_profile_ops(void);

/* Draft-22 profile ops (defined in profile_d22.c). */
const moq_profile_ops_t *moq_d22_profile_ops(void);

/* Draft-18 profile ops (defined in profile_d18.c). Returned by
 * moq_profile_lookup for MOQ_VERSION_DRAFT_18. */
const moq_profile_ops_t *moq_d18_profile_ops(void);

#endif /* MOQ_PROFILE_H */
