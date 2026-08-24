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

    moq_result_t (*encode_publish_ok)(moq_session_t *s,
                                       struct moq_buf_writer *w,
                                       const struct moq_publish_ok_encode_args *args);
    moq_result_t (*encode_publish_done)(moq_session_t *s,
                                         struct moq_buf_writer *w,
                                         const struct moq_finish_publish_encode_args *args);

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
                                         const struct moq_fetch_prior_object *prev,
                                         struct moq_decoded_fetch_object *out);
    moq_result_t (*encode_fetch_object)(moq_session_t *s,
                                         struct moq_buf_writer *w,
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
     * Capability: true if this profile's FETCH-response data plane can carry a
     * descending group order. Draft-16 sets true (fetch objects carry absolute
     * Group IDs); draft-18 sets false (group deltas with ascending-only
     * reconstruction), so the session refuses a DESCENDING fetch request rather
     * than emit one whose response it would mis-decode.
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
} moq_profile_ops_t;

const moq_profile_ops_t *moq_profile_lookup(moq_version_t version);

/* Draft-18 profile ops (defined in profile_d18.c). Returned by
 * moq_profile_lookup for MOQ_VERSION_DRAFT_18. */
const moq_profile_ops_t *moq_d18_profile_ops(void);

#endif /* MOQ_PROFILE_H */
