#ifndef MOQ_SESSION_INTERNAL_H
#define MOQ_SESSION_INTERNAL_H

#include "moq/session.h"
#include "moq/buf.h"
#include "moq/rcbuf.h"
#include "moq/wire.h"
#include "profile.h"
#include "session_transport.h"
#include <string.h>

/* -- Defaults ------------------------------------------------------ */

#define MOQ_DEFAULT_MAX_ACTIONS      64
#define MOQ_DEFAULT_MAX_EVENTS       16
#define MOQ_DEFAULT_SEND_BUF         4096
#define MOQ_DEFAULT_RECV_BUF         4096
#define MOQ_DEFAULT_MAX_SUBS         64
#define MOQ_DEFAULT_OUTPUT_SCRATCH   65536
#define MOQ_DEFAULT_MAX_SUBGROUPS    64
#define MOQ_DEFAULT_MAX_DATA_STREAMS 64
#define MOQ_DEFAULT_MAX_ANNOUNCEMENTS 64
#define MOQ_DEFAULT_MAX_OBJ_PAYLOAD  (4u * 1024 * 1024)
#define MOQ_DEFAULT_MAX_RECV_BUF (16u * 1024 * 1024)
#define MOQ_DEFAULT_MAX_NS_SUBS 16
/* Slot count for the pre-SUBSCRIBE_OK datagram reordering buffer (held bytes
 * are separately bounded by the receive-buffer budget).
 *
 * KEEP THIS SMALL. staged_replay() (session.c) preserves oldest-first replay by
 * repeatedly scanning the whole fixed array for the lowest seq, so its cost is
 * O(cap^2). That is fine only because cap is tiny (16 -> ~256 comparisons). Do
 * NOT raise this cap without first replacing the replay min-scan with an
 * indexed / heap / ordered structure. */
#define MOQ_STAGED_DG_SLOTS 16u
#define MOQ_DEFAULT_MAX_FETCHES 16
#define MOQ_DEFAULT_MAX_PUBLISHES 16
#define MOQ_DEFAULT_MAX_TRACK_STATUSES 8
#define MOQ_DEFAULT_MAX_TRACK_SUBS 16

/* -- Layout assertions --------------------------------------------- */

_Static_assert(offsetof(moq_action_t, u) == 16, "action prefix 16B");
_Static_assert(offsetof(moq_event_t, u) == 16, "event prefix 16B");
_Static_assert(sizeof(moq_send_control_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_close_session_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_send_data_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_reset_data_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_setup_complete_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_session_closed_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_request_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_error_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_updated_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_request_ready_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_object_received_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_stop_data_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_published_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_done_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_rejected_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_accepted_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_cancelled_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_open_bidi_stream_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_send_bidi_stream_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_close_bidi_stream_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_open_uni_control_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_send_uni_control_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_reset_bidi_stream_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_stop_bidi_stream_action_t) <= MOQ_ACTION_DETAIL_MAX, "");
_Static_assert(sizeof(moq_ns_sub_request_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_ns_sub_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_ns_sub_error_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_found_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_namespace_gone_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_request_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_error_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_cancelled_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_complete_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_object_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_fetch_gap_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publish_request_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publish_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publish_error_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publish_finished_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publish_updated_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_request_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_subscribe_updated_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_request_redirect_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_request_goaway_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_send_datagram_action_t) <= MOQ_ACTION_DETAIL_MAX, "");

/* -- Internal types ------------------------------------------------ */

typedef struct moq_setup_params {
    bool     has_max_request_id;
    uint64_t max_request_id;
    bool     has_max_auth_token_cache_size;
    uint64_t max_auth_token_cache_size;
    bool     has_path;
    bool     has_authority;
} moq_setup_params_t;

typedef enum moq_sub_state {
    MOQ_SUB_FREE = 0,
    MOQ_SUB_PENDING_SUBSCRIBER = 1,
    MOQ_SUB_PENDING_PUBLISHER  = 2,
    MOQ_SUB_ESTABLISHED        = 3,
    MOQ_SUB_TERMINATED         = 4,
    /* Inbound request bytes are arriving (possibly fragmented) on the request
     * bidi stream and the request message is not yet complete. Used only by
     * stream-correlated request profiles. */
    MOQ_SUB_RECVING_REQUEST    = 5,
} moq_sub_state_t;

typedef enum moq_sub_role {
    MOQ_SUB_ROLE_SUBSCRIBER = 1,
    MOQ_SUB_ROLE_PUBLISHER  = 2,
} moq_sub_role_t;

typedef struct moq_sub_entry {
    moq_sub_state_t    state;
    moq_sub_role_t     role;
    uint32_t           generation;
    moq_subscription_t handle;
    uint64_t           request_id;
    /* request_stream_ref: request bidi stream identity for stream-correlated
     * request profiles (profiles that open a dedicated bidi stream per request
     * and correlate responses by stream identity) -- here, the SUBSCRIBE
     * request stream. Empty (zero) under draft-16, which correlates by
     * request_id instead. */
    moq_stream_ref_t   request_stream_ref;
    /* Per-request-stream receive buffer for parsing control messages that
     * arrive (possibly fragmented) on the request's bidi stream, for
     * stream-correlated request profiles. Co-allocated in the session block;
     * unused under draft-16. */
    uint8_t           *req_recv_buf;
    size_t             req_recv_cap;
    size_t             req_recv_len;
    bool               req_recv_fin;
    uint64_t           track_alias;
    uint8_t           *track_id_buf;
    size_t             track_id_len;
    uint64_t           delivery_timeout_us;
    uint32_t           filter_type;
    bool               forward;
    bool               has_largest;
    uint64_t           largest_group;
    uint64_t           largest_object;
    bool               update_pending;
    uint64_t           update_request_id;
    /* A REQUEST_UPDATE failed (REQUEST_ERROR): the subscription is awaiting the
     * mandatory terminal PUBLISH_DONE(UPDATE_FAILED). No new update may be sent,
     * and only that PUBLISH_DONE is a valid next message. */
    bool               update_failed;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* The track supports dynamically started groups (DYNAMIC_GROUPS == 1 in
     * SUBSCRIBE_OK's track properties). Gates outbound new-group requests on
     * this subscription's updates. */
    bool dynamic_groups;
} moq_sub_entry_t;

typedef enum moq_ann_state {
    MOQ_ANN_FREE = 0,
    MOQ_ANN_PENDING_ANNOUNCER  = 1,  /* we sent PUBLISH_NAMESPACE, awaiting response */
    MOQ_ANN_PENDING_RECEIVER   = 2,  /* peer sent PUBLISH_NAMESPACE, awaiting app accept/reject */
    MOQ_ANN_ESTABLISHED        = 3,
} moq_ann_state_t;

typedef enum moq_ann_role {
    MOQ_ANN_ROLE_ANNOUNCER = 1,
    MOQ_ANN_ROLE_RECEIVER  = 2,
} moq_ann_role_t;

typedef struct moq_ann_entry {
    moq_ann_state_t    state;
    moq_ann_role_t     role;
    uint32_t           generation;
    moq_announcement_t handle;
    uint64_t           request_id;
    /* request_stream_ref: request bidi stream identity for stream-correlated
     * request profiles (the PUBLISH_NAMESPACE request stream). Empty (zero)
     * under draft-16. */
    moq_stream_ref_t   request_stream_ref;
    uint8_t           *ns_id_buf;   /* canonical namespace blob for dup detection */
    size_t             ns_id_len;
    /* req_recv_*: receive buffer for announce request-bidi control bytes
     * (stream-correlated profiles): the PUBLISH_NAMESPACE response on the
     * announcer side, REQUEST_UPDATE on the receiver side. Co-allocated in the
     * session block; the pointer/cap persist across entry reuse (only len/fin
     * reset on free). Unused under draft-16 (announce stays on the control
     * channel). */
    uint8_t           *req_recv_buf;
    size_t             req_recv_cap;
    size_t             req_recv_len;
    bool               req_recv_fin;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
} moq_ann_entry_t;

/* Max resolved authorization tokens surfaced per request (also bounds the
 * per-entry token storage used to defer a buffered Joining FETCH). */
#ifndef MOQ_DECODED_MAX_TOKENS
#define MOQ_DECODED_MAX_TOKENS 16
#endif

typedef enum moq_fetch_state {
    MOQ_FETCH_FREE              = 0,
    MOQ_FETCH_PENDING_FETCHER   = 1,
    MOQ_FETCH_PENDING_PUBLISHER = 2,
    MOQ_FETCH_ACCEPTED          = 3,
    /* Value 4 (formerly MOQ_FETCH_CANCELLED_LOCAL) is retired: a locally
     * cancelled fetch now frees its pool slot immediately and records the
     * request id in the bounded fetch-cancel tombstone cache instead. */
    /* A Joining FETCH (§10.12.2) whose associated subscription is still PENDING:
     * the request is buffered (request bidi registered, auth committed, tokens +
     * join params held in the entry) and no FETCH_REQUEST is surfaced until the
     * subscription is accepted (release) or rejected/torn down (reject). */
    MOQ_FETCH_PENDING_JOIN     = 7,
    /* A terminal request-bidi error (REQUEST_ERROR) was surfaced; the entry is
     * kept only to absorb the request stream's trailing FIN before release
     * (stream-correlated profiles). Distinct from the data-uni tombstone. */
    MOQ_FETCH_DRAINING_RESPONSE = 5,
    /* A request-stream GOAWAY (§10.4) migrated this FETCH, but the response data
     * uni has not yet presented its FETCH_HEADER. The entry is kept (request-id
     * key retained, handle generation-invalidated) only so a late FETCH_HEADER is
     * absorbed rather than mistaken for an unknown request id (session-fatal). The
     * data uni is left alone -- no STOP_SENDING -- unlike a locally cancelled
     * fetch, which STOPs the late data uni. */
    MOQ_FETCH_GOAWAY_LOCAL = 6,
} moq_fetch_state_t;

typedef enum moq_fetch_role {
    MOQ_FETCH_ROLE_FETCHER   = 1,
    MOQ_FETCH_ROLE_PUBLISHER = 2,
} moq_fetch_role_t;

typedef struct moq_fetch_prior_object {
    bool     has_prev;        /* prior Location (group/object) is known */
    /* draft-18: after an End-of-Range marker the prior Location comes from the
     * marker but the prior Subgroup/Priority come from the last ACTUAL object;
     * has_actual tracks whether such actual-object metadata exists. */
    bool     has_actual;
    uint64_t group_id;
    uint64_t subgroup_id;     /* valid only when has_actual */
    uint64_t object_id;
    uint8_t  publisher_priority;  /* valid only when has_actual */
} moq_fetch_prior_object_t;

typedef struct moq_fetch_entry {
    moq_fetch_state_t  state;
    moq_fetch_role_t   role;
    uint32_t           generation;
    moq_fetch_t        handle;
    uint64_t           request_id;
    /* request_stream_ref: the FETCH *request* bidi stream identity for
     * stream-correlated request profiles (the stream the FETCH request travels
     * on; FETCH_OK/REQUEST_ERROR correlate by it). Intentionally DISTINCT from
     * data_stream_ref below. Empty (zero) under draft-16. */
    moq_stream_ref_t   request_stream_ref;
    /* data_stream_ref: the *response object data* stream identity (the uni
     * stream, led by FETCH_HEADER, that carries fetched objects). Used by all
     * profiles. Intentionally distinct from request_stream_ref. */
    moq_stream_ref_t   data_stream_ref;
    bool               data_stream_started;
    bool               data_stream_fin;
    bool               control_response_seen;
    bool               control_ok;
    moq_fetch_prior_object_t prior;
    uint64_t           start_group;
    uint64_t           start_object;
    /* req_recv_*: receive buffer for request-bidi RESPONSE bytes (FETCH_OK /
     * REQUEST_ERROR) on stream-correlated request profiles. Co-allocated in the
     * session block; NULL/zero under profiles that respond on the shared control
     * channel. Intentionally NOT used for FETCH data-uni bytes -- those travel
     * on data_stream_ref through session_receive.c. */
    uint8_t           *req_recv_buf;
    size_t             req_recv_cap;
    size_t             req_recv_len;
    bool               req_recv_fin;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* MOQ_FETCH_PENDING_JOIN storage: a Joining FETCH buffered until its
     * associated subscription is accepted. Holds the join parameters and the
     * resolved auth tokens (ownership of any staged heap values transferred from
     * the decode), so FETCH_REQUEST can be surfaced (or rejected) at release. */
    uint8_t              join_fetch_type;          /* 2=relative, 3=absolute */
    uint8_t              join_subscriber_priority;
    moq_group_order_t    join_group_order;
    uint64_t             join_request_id;          /* associated subscription's id */
    uint64_t             join_start;
    moq_resolved_token_t join_tokens[MOQ_DECODED_MAX_TOKENS];
    bool                 join_token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t               join_token_count;
} moq_fetch_entry_t;

typedef enum moq_pub_state {
    MOQ_PUB_FREE               = 0,
    MOQ_PUB_PENDING_PUBLISHER   = 1,
    MOQ_PUB_PENDING_SUBSCRIBER  = 2,
    MOQ_PUB_ESTABLISHED         = 3,
    /* A terminal message was seen on the PUBLISH request bidi (the publisher's
     * PUBLISH_DONE on the subscriber side, or a REQUEST_ERROR rejecting our
     * outbound PUBLISH on the publisher side); the bidi is kept drainable until
     * the FIN. Stream-correlated profiles only. */
    MOQ_PUB_DRAINING_RESPONSE  = 4,
} moq_pub_state_t;

typedef enum moq_pub_role {
    MOQ_PUB_ROLE_PUBLISHER  = 1,
    MOQ_PUB_ROLE_SUBSCRIBER = 2,
} moq_pub_role_t;

typedef struct moq_pub_entry {
    moq_pub_state_t    state;
    moq_pub_role_t     role;
    uint32_t           generation;
    moq_publication_t  handle;
    uint64_t           request_id;
    /* request_stream_ref: request bidi stream identity for stream-correlated
     * request profiles (the PUBLISH request stream). Empty (zero) under
     * draft-16. */
    moq_stream_ref_t   request_stream_ref;
    uint64_t           track_alias;
    bool               send_allowed;
    uint8_t            subscriber_priority;
    uint8_t            group_order;
    bool               has_delivery_timeout;
    uint64_t           delivery_timeout_ms;
    bool               has_expires;
    uint64_t           expires_ms;
    bool               update_pending;
    uint64_t           update_request_id;
    /* A REQUEST_UPDATE was rejected (REQUEST_ERROR): the publication must now be
     * terminated by PUBLISH_DONE(UPDATE_FAILED); further updates are blocked. */
    bool               update_failed;
    /* Co-allocated request-bidi receive buffer (stream-correlated profiles): the
     * PUBLISH bidi carries the peer's response/lifecycle messages here. On the
     * publisher (opener) side: PUBLISH_OK / REQUEST_ERROR / REQUEST_UPDATE; on
     * the subscriber (responder) side: REQUEST_OK (update ack) / PUBLISH_DONE.
     * req_recv_fin records the peer's FIN. Reset, not freed, on entry reuse; the
     * pointer/cap are preserved. Unused under draft-16. */
    uint8_t           *req_recv_buf;
    size_t             req_recv_cap;
    size_t             req_recv_len;
    bool               req_recv_fin;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* The track supports dynamically started groups (DYNAMIC_GROUPS == 1 in
     * the PUBLISH track properties). Gates outbound new-group requests on
     * the accept and this publication's updates. */
    bool dynamic_groups;
    /* Subscriber-role PUBLISH_DONE Stream-Count gating (draft-16 §9.15 /
     * draft-18 §9.x): PUBLISH_DONE arrives on the control channel and is likely
     * to precede late-arriving / late-opening data streams. The subscriber keeps
     * the publication state live until it has processed the advertised number of
     * data streams, then removes it. processed_stream_count counts completed
     * (FIN'd) data streams for this publication. When a finite Stream Count is
     * not yet satisfied, the PUBLISH_DONE is deferred: done_pending records it
     * (status/count + an owned copy of the reason) and PUBLISH_FINISHED is held
     * until processed_stream_count >= done_stream_count. A Stream Count of 0 or
     * the 2^62-1 "unknown" sentinel finalizes immediately (no core timer). */
    uint64_t           processed_stream_count;
    bool               done_pending;
    uint64_t           done_status_code;
    uint64_t           done_stream_count;
    uint8_t           *done_reason_buf;   /* owned copy; freed in pub_free_entry */
    size_t             done_reason_len;
} moq_pub_entry_t;

typedef enum moq_ts_state {
    MOQ_TS_FREE               = 0,
    MOQ_TS_PENDING_REQUESTER  = 1,
    MOQ_TS_PENDING_PUBLISHER  = 2,
    /* Requester saw the terminal response (TRACK_STATUS_OK / REQUEST_ERROR) on a
     * stream-correlated profile; the bidi is kept drainable until the FIN. */
    MOQ_TS_DRAINING_RESPONSE  = 3,
} moq_ts_state_t;

typedef enum moq_ts_role {
    MOQ_TS_ROLE_REQUESTER = 1,
    MOQ_TS_ROLE_PUBLISHER = 2,
} moq_ts_role_t;

typedef struct moq_ts_entry {
    moq_ts_state_t              state;
    moq_ts_role_t               role;
    uint32_t                    generation;
    moq_track_status_handle_t   handle;
    uint64_t                    request_id;
    /* request_stream_ref: request bidi stream identity for stream-correlated
     * request profiles (the TRACK_STATUS request stream). Empty (zero) under
     * draft-16. */
    moq_stream_ref_t            request_stream_ref;
    /* Co-allocated request-bidi receive buffer (stream-correlated profiles): the
     * requester buffers the fragmentable TRACK_STATUS_OK / REQUEST_ERROR response
     * here. req_recv_fin also records the peer's FIN on the publisher side (the
     * requester sends TRACK_STATUS as its first and only message). Reset, not
     * freed, on entry reuse; the pointer/cap are preserved. Unused under draft-16. */
    uint8_t                    *req_recv_buf;
    size_t                      req_recv_cap;
    size_t                      req_recv_len;
    bool                        req_recv_fin;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
} moq_ts_entry_t;

typedef enum moq_ns_sub_state {
    MOQ_NS_SUB_FREE               = 0,
    MOQ_NS_SUB_PENDING_SUBSCRIBER  = 1,
    MOQ_NS_SUB_RECVING_PUBLISHER   = 2,
    MOQ_NS_SUB_PENDING_PUBLISHER   = 3,
    MOQ_NS_SUB_ESTABLISHED         = 4,
    MOQ_NS_SUB_CLOSING             = 5,
} moq_ns_sub_state_t;

/* -- Stream kind --------------------------------------------------- */

typedef enum moq_stream_kind {
    MOQ_STREAM_KIND_UNKNOWN       = 0,
    MOQ_STREAM_KIND_SUBGROUP      = 1,
    MOQ_STREAM_KIND_NAMESPACE_SUB = 2,
    MOQ_STREAM_KIND_FETCH         = 3,
    /* Classification is not yet possible: the leading stream-type integer is
     * incomplete (e.g. a multi-byte vi64 split across reads). The receiver must
     * buffer more bytes and re-classify; it is never a final stream kind. */
    MOQ_STREAM_KIND_NEED_MORE     = 4,
} moq_stream_kind_t;

/* -- Request endpoint ------------------------------------------------ */

typedef enum moq_request_kind {
    MOQ_REQ_NONE             = 0,
    MOQ_REQ_SUBSCRIPTION     = 1,
    MOQ_REQ_ANNOUNCEMENT     = 2,
    MOQ_REQ_NAMESPACE_SUB    = 3,
    MOQ_REQ_FETCH            = 4,
    MOQ_REQ_PUBLISH          = 5,
    MOQ_REQ_TRACK_STATUS     = 6,
    MOQ_REQ_SUBSCRIPTION_UPDATE  = 7,
    MOQ_REQ_PUBLICATION_UPDATE   = 8,
    MOQ_REQ_SUBSCRIBE_TRACKS = 9,
} moq_request_kind_t;

typedef struct moq_request_endpoint {
    moq_request_kind_t kind;
    int                slot;
    bool               has_request_id;
    uint64_t           request_id;
    bool               has_stream_ref;
    moq_stream_ref_t   stream_ref;
} moq_request_endpoint_t;

typedef enum moq_sg_state {
    MOQ_SG_FREE      = 0,
    MOQ_SG_OPEN      = 1,
    MOQ_SG_CLOSING   = 2,
    MOQ_SG_RESETTING = 3,
    MOQ_SG_STREAMING = 4,
} moq_sg_state_t;

typedef struct moq_sg_entry {
    moq_sg_state_t         state;
    uint32_t               generation;
    moq_subscription_t     sub;
    moq_publication_t      pub;
    moq_stream_ref_t       stream_ref;
    uint64_t               group_id;
    uint64_t               subgroup_id;
    uint64_t               prev_object_id;
    bool                   has_prev_object;
    uint64_t               streaming_payload_len;
    uint64_t               streaming_bytes_written;
    uint64_t               delivery_deadline_us;
    bool                   has_extensions;
} moq_sg_entry_t;

/* -- Auth token cache ---------------------------------------------- */

#define MOQ_TOKEN_ENTRY_OVERHEAD 16  /* spec: 16 + token_value_len per entry */
#define MOQ_DEFAULT_MAX_TOKEN_CACHE_ENTRIES 4096

typedef struct moq_token_cache_entry {
    uint64_t alias;
    uint64_t token_type;
    uint8_t *token_value;
    size_t   token_value_len;
    bool     active;
} moq_token_cache_entry_t;

typedef struct moq_token_cache {
    moq_alloc_t              alloc;
    moq_token_cache_entry_t *entries;
    size_t                   cap;
    size_t                   used_bytes;
    size_t                   max_bytes;
} moq_token_cache_t;

/* -- Cache overlay for staged commit ------------------------------- */

#define MOQ_CACHE_OVERLAY_CAP 16

typedef enum moq_ov_kind {
    MOQ_OV_REGISTER = 1,
    MOQ_OV_DELETE   = 2,
} moq_ov_kind_t;

typedef struct moq_ov_entry {
    moq_ov_kind_t  kind;
    uint64_t       alias;
    uint64_t       token_type;
    const uint8_t *value;
    size_t         value_len;
    uint8_t       *preowned;
} moq_ov_entry_t;

typedef struct moq_cache_overlay {
    moq_ov_entry_t entries[MOQ_CACHE_OVERLAY_CAP];
    size_t         count;
    const moq_token_cache_t *cache;
} moq_cache_overlay_t;

void moq_ov_init(moq_cache_overlay_t *ov, const moq_token_cache_t *cache);
void moq_ov_free_preowned(moq_cache_overlay_t *ov, const moq_alloc_t *alloc);
bool moq_ov_alias_live(const moq_cache_overlay_t *ov, uint64_t alias,
                        uint64_t *out_type, const uint8_t **out_val,
                        size_t *out_len);
void moq_ov_project(const moq_cache_overlay_t *ov,
                     size_t *out_bytes, size_t *out_active);

/* -- Auth transaction (staged cache mutations) --------------------- */

/* -- Semantic auth token input (profile → session auth) ------------- */

typedef enum moq_auth_op {
    MOQ_AUTH_OP_DELETE    = 0,
    MOQ_AUTH_OP_REGISTER  = 1,
    MOQ_AUTH_OP_USE_ALIAS = 2,
    MOQ_AUTH_OP_USE_VALUE = 3,
} moq_auth_op_t;

typedef struct moq_decoded_auth_token {
    moq_auth_op_t  op;
    uint64_t       alias;
    uint64_t       token_type;
    const uint8_t *token_value;
    size_t         token_value_len;
} moq_decoded_auth_token_t;

typedef struct moq_auth_txn {
    moq_cache_overlay_t ov;
} moq_auth_txn_t;

#define MOQ_DECODED_MAX_TOKENS 16

/* -- Namespace sub entry (publisher side, one per bidi stream) ------- */

typedef struct moq_ns_sub_entry {
    moq_ns_sub_state_t  state;
    moq_stream_kind_t   stream_kind;
    uint32_t            generation;
    moq_ns_sub_handle_t handle;
    uint64_t            request_id;
    moq_stream_ref_t    stream_ref;
    moq_request_endpoint_t request_ep;
    moq_namespace_interest_t namespace_interest;
    uint8_t            *prefix_buf;
    size_t              prefix_buf_len;
    moq_bytes_t        *prefix_parts;
    size_t              prefix_count;
    bool                prefix_valid;   /* a prefix has actually been parsed and
                                         * stored (set by ns_sub_store_prefix,
                                         * cleared by ns_sub_free_prefix). Until
                                         * then prefix_count==0 is *not* a real
                                         * empty "match-all" prefix and the entry
                                         * must be skipped by overlap scans. */
    bool                got_response;
    bool                parse_complete;
    bool                pending_fin;
    bool                closing_remote_error;
    bool                forward;
    bool                auth_processed;
    bool                auth_committed;
    uint64_t            auth_reject_code;
    moq_resolved_token_t resolved_tokens[MOQ_DECODED_MAX_TOKENS];
    bool                token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t              token_count;
    moq_auth_txn_t      auth_txn;
    uint8_t            *recv_buf;
    size_t              recv_len;
    size_t              recv_cap;
    void               *announced_suffixes;
    bool announced_suffixes_inbound;  /* true when announced_suffixes tracks
                                       * peer-announced suffixes (subscriber role)
                                       * and is charged against the receive budget;
                                       * false for the outbound publisher-side set,
                                       * which is never receive-budget counted. */
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
} moq_ns_sub_entry_t;

void ns_sub_destroy_all(moq_session_t *s);

/* -- Track-sub entry (SUBSCRIBE_TRACKS, draft-18 only) -------------- *
 * One per SUBSCRIBE_TRACKS bidi. Unlike the namespace-sub pool (which has a
 * draft-16 path and its own idx_ns_by_ref), SUBSCRIBE_TRACKS is draft-18-only
 * and rides the generic request-bidi staging/handoff: it is keyed in the shared
 * request registry by stream-ref (MOQ_REQ_SUBSCRIBE_TRACKS), exactly like FETCH
 * and TRACK_STATUS. After REQUEST_OK the bidi stays established and carries
 * PUBLISH_BLOCKED (the resulting PUBLISH messages travel on separate bidis, not
 * modelled yet). The overlap space is independent of the namespace-sub pool. */
typedef enum moq_track_sub_state {
    MOQ_TRACK_SUB_FREE               = 0,
    MOQ_TRACK_SUB_PENDING_SUBSCRIBER = 1,  /* our request, awaiting REQUEST_OK/ERROR */
    MOQ_TRACK_SUB_PENDING_PUBLISHER  = 2,  /* peer request decoded, awaiting accept/reject */
    MOQ_TRACK_SUB_ESTABLISHED        = 3,  /* REQUEST_OK seen/sent; carries PUBLISH_BLOCKED */
    /* Requester saw a terminal REQUEST_ERROR on a stream-correlated profile; the
     * bidi is kept drainable until the FIN (split-FIN handling). */
    MOQ_TRACK_SUB_DRAINING_RESPONSE  = 4,
} moq_track_sub_state_t;

typedef enum moq_track_sub_role {
    MOQ_TRACK_SUB_ROLE_SUBSCRIBER = 1,  /* we sent SUBSCRIBE_TRACKS */
    MOQ_TRACK_SUB_ROLE_PUBLISHER  = 2,  /* we received it */
} moq_track_sub_role_t;

typedef struct moq_track_sub_entry {
    moq_track_sub_state_t   state;
    moq_track_sub_role_t    role;
    uint32_t                generation;
    moq_track_sub_handle_t  handle;
    uint64_t                request_id;
    moq_request_endpoint_t  request_ep;
    moq_stream_ref_t        request_stream_ref;
    /* Stored namespace prefix (publisher side) for the independent overlap scan. */
    uint8_t                *prefix_buf;
    size_t                  prefix_buf_len;
    moq_bytes_t            *prefix_parts;
    size_t                  prefix_count;
    bool                    forward;
    /* Auth staging committed after the request event is pushed (publisher side). */
    bool                    auth_committed;
    uint64_t                auth_reject_code;
    moq_resolved_token_t    resolved_tokens[MOQ_DECODED_MAX_TOKENS];
    bool                    token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t                  token_count;
    moq_auth_txn_t          auth_txn;
    /* Co-allocated request-bidi receive buffer (stream-correlated): the requester
     * buffers the fragmentable REQUEST_OK / REQUEST_ERROR / PUBLISH_BLOCKED stream
     * here. req_recv_fin records the peer's FIN. Reset, not freed, on entry reuse;
     * the pointer/cap are preserved. */
    uint8_t                *req_recv_buf;
    size_t                  req_recv_cap;
    size_t                  req_recv_len;
    bool                    req_recv_fin;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
} moq_track_sub_entry_t;

void track_sub_destroy_all(moq_session_t *s);

moq_result_t moq_token_cache_init(moq_token_cache_t *cache,
                                   const moq_alloc_t *alloc,
                                   size_t max_bytes,
                                   size_t max_entries);
void moq_token_cache_free(moq_token_cache_t *cache);

#define MOQ_TOKEN_OK            0
#define MOQ_TOKEN_ERR_DUPLICATE 1
#define MOQ_TOKEN_ERR_OVERFLOW  2
#define MOQ_TOKEN_ERR_UNKNOWN   3
#define MOQ_TOKEN_ERR_NOMEM     4

int moq_token_cache_register(moq_token_cache_t *cache,
                              uint64_t alias, uint64_t token_type,
                              const uint8_t *value, size_t value_len);
int moq_token_cache_register_preowned(moq_token_cache_t *cache,
                                      uint64_t alias, uint64_t token_type,
                                      uint8_t *value, size_t value_len);
int moq_token_cache_delete(moq_token_cache_t *cache, uint64_t alias);
int moq_token_cache_lookup(const moq_token_cache_t *cache,
                            uint64_t alias,
                            uint64_t *out_type,
                            const uint8_t **out_value,
                            size_t *out_value_len);

/* -- Decoded inbound subgroup header (profile → session core) ------- */

typedef struct moq_decoded_subgroup_header {
    uint64_t track_alias;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint8_t  publisher_priority;
    bool     has_extensions;
    bool     end_of_group;
    bool     subgroup_id_resolved;
    bool     subgroup_id_from_first_object;
} moq_decoded_subgroup_header_t;

typedef struct moq_decoded_object_header {
    uint64_t           object_id;
    uint64_t           payload_len;
    moq_object_status_t status;
    const uint8_t     *properties;     /* borrowed from reader; NULL if none */
    size_t             properties_len;
    bool               has_properties;
} moq_decoded_object_header_t;

/* -- Outbound data-plane encode args (session core → profile) ------- */

typedef struct moq_subgroup_header_encode_args {
    uint64_t track_alias;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint8_t  publisher_priority;
    bool     has_extensions;
    bool     end_of_group;
} moq_subgroup_header_encode_args_t;

typedef struct moq_object_header_encode_args {
    uint64_t object_id;
    uint64_t prev_object_id;
    bool     has_prev_object;
    uint64_t payload_len;
    bool     has_extensions;
    size_t   properties_len;
    bool     header_only;
    uint64_t object_status;   /* wire status emitted for a zero-length object
                                 (0x0 NORMAL default; 0x4 END_OF_TRACK, etc.) */
} moq_object_header_encode_args_t;

typedef struct moq_goaway_encode_args {
    const uint8_t *uri;
    size_t         uri_len;
} moq_goaway_encode_args_t;

typedef struct moq_publish_namespace_encode_args {
    uint64_t        request_id;
    moq_namespace_t track_namespace;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_publish_namespace_encode_args_t;

typedef struct moq_publish_namespace_cancel_encode_args {
    uint64_t       request_id;
    uint64_t       error_code;
    const uint8_t *reason;
    size_t         reason_len;
} moq_publish_namespace_cancel_encode_args_t;

typedef struct moq_subscribe_namespace_encode_args {
    uint64_t request_id;
    moq_namespace_t prefix;
    uint64_t namespace_interest;  /* semantic: 0=PUBLISH, 1=NAMESPACE, 2=BOTH */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_subscribe_namespace_encode_args_t;

typedef struct moq_namespace_msg_encode_args {
    bool is_done;    /* false = NAMESPACE (found), true = NAMESPACE_DONE (gone) */
    moq_namespace_t suffix;
} moq_namespace_msg_encode_args_t;

typedef struct moq_subscribe_tracks_encode_args {
    uint64_t        request_id;
    moq_namespace_t prefix;
    bool            has_forward;
    bool            forward;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_subscribe_tracks_encode_args_t;

/* -- Receive data stream state ------------------------------------- */

#define MOQ_RX_HDR_BUF 64

typedef enum moq_rx_parse_state {
    MOQ_RX_AWAITING_HEADER  = 0,
    MOQ_RX_AWAITING_OBJECT  = 1,
    MOQ_RX_READING_PAYLOAD  = 2,
    MOQ_RX_PENDING_EMIT     = 3,
    MOQ_RX_NEED_STOP        = 4,
    MOQ_RX_STREAMING_PAYLOAD = 5,
    MOQ_RX_PENDING_CHUNK    = 6,
    /* Subgroup/uni stream whose header referenced an alias not yet
     * established by a SUBSCRIBE_OK (held briefly for control/data
     * reordering). The subgroup header is consumed; remaining object bytes
     * accumulate in input_buf WITHOUT being parsed until the alias is bound
     * (the entry then flips to AWAITING_OBJECT and the buffered objects are
     * parsed). */
    MOQ_RX_DEFERRED_ALIAS   = 7,
} moq_rx_parse_state_t;

typedef struct moq_rx_stream {
    bool                   active;
    moq_stream_kind_t      stream_kind;
    moq_rx_parse_state_t   parse_state;
    moq_stream_ref_t       stream_ref;
    moq_subscription_t     sub;
    moq_fetch_t            fetch;
    moq_publication_t      pub_handle;
    /* Set when this data uni is the late response to a locally-cancelled fetch:
     * the fetch-cancel tombstone for cancel_tomb_request_id is consumed once the
     * STOP_DATA is actually queued (which may be deferred via MOQ_RX_NEED_STOP),
     * so a retried stop still releases the tombstone. */
    bool                   stop_consumes_cancel_tomb;
    uint64_t               cancel_tomb_request_id;
    moq_fetch_prior_object_t fetch_prior;
    uint64_t               track_alias;
    uint64_t               group_id;
    uint64_t               subgroup_id;
    uint8_t                publisher_priority;
    bool                   has_extensions;
    bool                   has_prev_object;
    bool                   pending_fin;
    bool                   resumed_deferred;  /* was DEFERRED_ALIAS, now bound;
                                                 re-driven on poll retry until
                                                 its buffered objects drain */
    bool                   subgroup_id_from_first_object;
    bool                   subgroup_id_resolved;
    bool                   end_of_group;
    uint64_t               prev_object_id;

    uint8_t                hdr_buf[MOQ_RX_HDR_BUF];
    uint8_t                hdr_len;

    uint64_t               payload_expected;
    size_t                 payload_written;
    /* payload_rcbuf is the delivered object payload, allocated up front (once
     * payload_len is known) via moq_rcbuf_alloc_uninit and filled in place;
     * payload_buf is the writable cursor into its inline storage. On emit the
     * rcbuf is handed to the event without a copy. Both are cleared when
     * ownership transfers to the event; a torn-down stream decrefs the rcbuf. */
    moq_rcbuf_t           *payload_rcbuf;
    uint8_t               *payload_buf;
    uint64_t               cur_object_id;
    uint8_t                cur_status;
    moq_rcbuf_t           *cur_extensions;

    uint8_t               *input_buf;
    size_t                 input_len;
    size_t                 input_cap;

    /* Streaming mode: pending chunk for PENDING_CHUNK retry. */
    moq_rcbuf_t           *pending_chunk;
    size_t                 pending_data_len;
    bool                   pending_begin;
    bool                   pending_end;
    bool                   pending_from_input;
    moq_object_terminal_t  pending_terminal;
} moq_rx_stream_t;

/* A datagram that arrived for an alias not yet established by a SUBSCRIBE_OK;
 * its raw bytes are copied and held briefly to absorb control/data
 * reordering, then replayed once the OK assigns the alias, or discarded when
 * no forwarding subscription remains pending. */
typedef struct moq_staged_datagram {
    bool      in_use;
    uint64_t  seq;       /* arrival order, for drop-oldest eviction */
    uint64_t  alias;
    uint8_t  *bytes;
    size_t    len;
} moq_staged_datagram_t;

/* -- Request registry ---------------------------------------------- */

static inline int32_t req_pack(moq_request_kind_t kind, int slot) {
    return (int32_t)(((uint32_t)kind << 16) | ((uint32_t)slot & 0xFFFFu));
}
static inline moq_request_kind_t req_kind(int32_t packed) {
    return (moq_request_kind_t)((uint32_t)packed >> 16);
}
static inline int req_slot(int32_t packed) {
    return (int)(packed & 0xFFFF);
}

static inline moq_request_endpoint_t req_endpoint_none(void) {
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = MOQ_REQ_NONE;
    ep.slot = -1;
    return ep;
}

/* -- Open-addressing hash index ------------------------------------ */

typedef struct moq_index_entry {
    uint64_t key;
    int32_t  slot;   /* pool slot, or -1 for empty */
    int32_t  _pad;
} moq_index_entry_t;

static inline size_t moq_index_cap_for(size_t pool_cap) {
    size_t c = 1;
    while (c < 2 * pool_cap) c <<= 1;
    return c;
}

int moq_index_find(const moq_index_entry_t *tbl, size_t mask, uint64_t key);
void moq_index_insert(moq_index_entry_t *tbl, size_t mask,
                       uint64_t key, int slot);
void moq_index_remove(moq_index_entry_t *tbl, size_t mask, uint64_t key);

/* -- Session struct ------------------------------------------------ */

struct moq_session {
    moq_alloc_t         alloc;
    size_t              alloc_size;
    moq_session_state_t state;
    moq_perspective_t   perspective;
    uint64_t            last_now_us;
    uint64_t            borrow_epoch;

    const moq_profile_ops_t *profile;
    void                    *profile_state;

    moq_setup_params_t  local_setup;
    moq_setup_params_t  peer_setup;
    moq_token_cache_t   peer_token_cache;

    bool                send_auth_token_cache_size;
    uint64_t            auth_token_cache_size;

    uint8_t      *send_buf;
    size_t        send_cap;
    size_t        send_len;

    moq_action_t *actions;
    size_t        action_cap;
    size_t        action_head;
    size_t        action_tail;

    moq_event_t  *events;
    size_t        event_cap;
    size_t        event_head;
    size_t        event_tail;

    /* A deferred early-arrival request bidi (§3.3, buffered before setup
     * completed) hit WOULD_BLOCK during its establishment-time refeed (e.g.
     * SETUP_COMPLETE filled a tiny event queue). No bridge retry exists for
     * those bytes (they were accepted), so the event-drain path retries the
     * refeed while this is set. */
    bool          request_refeed_pending;

    uint8_t      *recv_buf;
    size_t        recv_cap;
    size_t        recv_len;

    moq_sub_entry_t *subs;
    size_t           sub_cap;

    moq_ann_entry_t *announcements;
    size_t           ann_cap;

    moq_fetch_entry_t *fetches;
    size_t             fetch_cap;

    moq_pub_entry_t   *publishes;
    size_t             pub_cap;

    moq_ts_entry_t    *track_statuses;
    size_t             ts_cap;

    uint8_t      *output_scratch;       /* transient: reset every advance */
    size_t        output_scratch_cap;
    size_t        output_scratch_len;

    /* Queued-event borrow arena: spans referenced by events that survive
     * push_event live here, NOT in output_scratch -- a second inbound
     * message in the same batch must not clobber a queued event's
     * borrowed data. Reset only once the event queue has drained. */
    uint8_t      *event_scratch;
    size_t        event_scratch_cap;
    size_t        event_scratch_len;

    uint16_t      session_tag;
    bool          goaway_sent;
    bool          goaway_received;
    bool          streaming_objects;

    moq_sg_entry_t *subgroups;
    size_t          sg_cap;
    uint64_t        next_stream_ref;

    moq_rx_stream_t *rx_streams;
    size_t           rx_cap;
    size_t           max_obj_payload;
    size_t           max_recv_buf;
    size_t           recv_payload_bytes;
    size_t           recv_input_bytes;

    /* Datagrams held for an alias not yet established by SUBSCRIBE_OK
     * (bounded; drop-oldest, counted). staged_dg is lazily allocated. */
    moq_staged_datagram_t *staged_dg;
    size_t           staged_cap;        /* slot count (0 = staging disabled) */
    size_t           staged_count;      /* slots in use */
    size_t           staged_bytes;      /* total payload bytes held */
    size_t           staged_bytes_cap;  /* byte budget (shares max_recv_buf) */
    uint64_t         staged_next_seq;   /* monotonic arrival counter */
    uint64_t         staged_dropped;    /* count evicted/dropped by bounds */

    uint64_t        *rx_finished;
    size_t           rx_fin_cap;
    size_t           rx_fin_head;
    size_t           rx_fin_count;

    uint64_t        *unsub_tombstones;
    size_t           unsub_tomb_cap;
    size_t           unsub_tomb_count;

    /* Bounded grace cache of locally-cancelled FETCH request IDs. The fetch
     * pool slot is freed immediately on cancel (so max_fetches is reusable);
     * this side cache lets a late REQUEST_ERROR / FETCH_OK / data stream for the
     * cancelled request be absorbed without reoccupying the pool. Drop-oldest
     * when full -- a stale-message grace window, not unbounded protocol state.
     * Capacity is tied to the fetch pool (fetch_cap). */
    uint64_t        *fetch_cancel_tombs;
    size_t           fetch_cancel_tomb_cap;
    size_t           fetch_cancel_tomb_count;

    /* Request-bidi stream_refs locally cancelled (unsubscribe / fetch cancel)
     * on stream-correlated profiles, draining any late in-flight response until
     * the bidi FINs or the peer resets it. Draft-16 leaves this empty. */
    uint64_t        *drain_refs;
    uint8_t         *drain_ref_reasons;   /* parallel to drain_refs (moq_drain_reason_t) */
    size_t           drain_ref_cap;
    size_t           drain_ref_count;

    uint64_t            goaway_timeout_us;
    uint64_t            goaway_deadline_us;
    uint64_t            subgroup_deadline_us;
    /* When a subgroup delivery-timeout reset cannot be enqueued because the
     * action queue is full (transport backpressure), the raw subgroup
     * deadline above stays expired so the pending reset is not lost. This
     * holds a bounded retry time in the future so moq_session_next_deadline_us()
     * reports a deferred deadline instead of the still-expired one -- timer
     * drivers then sleep until the backoff elapses rather than busy-spinning a
     * zero-length wait. UINT64_MAX when no reset is deferred. */
    uint64_t            subgroup_retry_deadline_us;
    uint64_t            idle_timeout_us;
    uint64_t            idle_deadline_us;

    moq_index_entry_t *idx_req_by_rid;
    size_t              idx_req_mask;
    /* Stream-ref request index: maps a request bidi stream_ref -> (kind, slot),
     * parallel to idx_req_by_rid. Used by stream-correlated request profiles --
     * those that open a dedicated bidi stream per request and correlate
     * responses by stream identity. Draft-16 correlates by request_id, so it
     * never populates or reads this index (it stays all -1) and draft-16
     * behavior is unaffected. */
    moq_index_entry_t *idx_req_by_streamref;
    size_t              idx_req_streamref_mask;
    moq_index_entry_t *idx_rx_by_ref;
    size_t              idx_rx_mask;

    /* alias -> slot for ESTABLISHED subscriber-role subscriptions (the receive
     * path alias lookup). Maintained at SUBSCRIBE_OK bind and on every exit from
     * ESTABLISHED; sub_find_by_alias_subscriber re-checks the predicate. */
    moq_index_entry_t *idx_sub_by_alias;
    size_t              idx_sub_alias_mask;

    moq_ns_sub_entry_t *ns_subs;
    size_t              ns_sub_cap;
    moq_index_entry_t  *idx_ns_by_ref;
    size_t              idx_ns_mask;

    /* SUBSCRIBE_TRACKS pool (draft-18 only). Keyed in idx_req_by_streamref via
     * the shared request registry (no dedicated index); independent overlap
     * space from ns_subs. */
    moq_track_sub_entry_t *track_subs;
    size_t                 track_sub_cap;
};

/* -- Decoded inbound OBJECT_DATAGRAM (profile -> session core) ------- */

typedef struct moq_decoded_object_datagram {
    int                  sub_slot;     /* subs pool index, or -1 */
    int                  pub_slot;     /* publishes pool index, or -1 */
    uint64_t             track_alias;  /* the wire alias (set even when both
                                          slots are -1, for pre-OK staging) */
    bool                 unknown_alias;/* true: parsed OK but no matching sub/
                                          pub alias (candidate for staging) */
    uint64_t             group_id;
    uint64_t             object_id;
    uint8_t              publisher_priority;
    bool                 default_priority;
    bool                 end_of_group;
    bool                 is_status;
    moq_object_status_t  status;
    const uint8_t       *properties;
    size_t               properties_len;
    const uint8_t       *payload;
    size_t               payload_len;
} moq_decoded_object_datagram_t;

/* -- Outbound datagram encode args (session core -> profile) -------- */

typedef struct moq_datagram_encode_args {
    uint64_t       track_alias;
    uint64_t       group_id;
    uint64_t       object_id;
    uint8_t        publisher_priority;
    bool           default_priority;
    bool           end_of_group;
    bool           is_status;
    uint64_t       object_status;
    const uint8_t *properties;
    size_t         properties_len;
    const uint8_t *payload;
    size_t         payload_len;
} moq_datagram_encode_args_t;

/* -- Datagram handlers (session_subgroup.c) -------------------------- */

moq_result_t session_core_on_object_datagram(moq_session_t *s,
    const moq_decoded_object_datagram_t *d,
    const uint8_t *payload_data, size_t payload_len,
    const uint8_t *props_data, size_t props_len);

/* -- Input normalization ------------------------------------------- */

typedef enum moq_input_kind {
    MOQ_INPUT_START = 1,
    MOQ_INPUT_CONTROL_BYTES = 2,
    MOQ_INPUT_TICK = 3,
    MOQ_INPUT_DATAGRAM = 4,
} moq_input_kind_t;

typedef struct moq_input {
    moq_input_kind_t kind;
    uint64_t         now_us;
    union {
        struct {
            const uint8_t *buf;
            size_t         len;
        } control_bytes;
        struct {
            const uint8_t *buf;
            size_t         len;
        } datagram;
    } u;
} moq_input_t;

/* -- Shared helpers (used by multiple .c files) -------------------- */

static inline uint64_t deadline_add(uint64_t now, uint64_t timeout) {
    uint64_t d = now + timeout;
    if (d < now || d == UINT64_MAX) return UINT64_MAX - 1;
    return d;
}

static inline bool session_is_active(const moq_session_t *s) {
    return s->state == MOQ_SESS_ESTABLISHED ||
           s->state == MOQ_SESS_DRAINING;
}

/* A locally-sent GOAWAY (§10.4) means we are draining: the peer must not open
 * NEW requests against us. Existing requests/responses and existing data
 * streams stay legal during DRAINING, so this is narrower than
 * !session_is_active -- it gates only the inbound request-initiating handlers.
 * (Inbound peer GOAWAY sets goaway_received, a separate flag, and does not
 * trip this.) */
static inline bool session_refuses_new_requests(const moq_session_t *s) {
    return s->goaway_sent;
}

void session_begin_advance(moq_session_t *s, uint64_t now_us);

void decref_queued_data_payloads(moq_session_t *s);
void decref_queued_event_payloads(moq_session_t *s);
void free_rx_stream_bufs(moq_session_t *s);

bool action_queue_full(const moq_session_t *s);
static inline size_t action_queue_avail(const moq_session_t *s) {
    size_t used = s->action_tail - s->action_head;
    return used < s->action_cap ? s->action_cap - used : 0;
}
moq_result_t push_action(moq_session_t *s, const moq_action_t *a);
bool event_queue_full(const moq_session_t *s);
static inline size_t event_queue_avail(const moq_session_t *s) {
    size_t used = s->event_tail - s->event_head;
    return used < s->event_cap ? s->event_cap - used : 0;
}
moq_result_t push_event(moq_session_t *s, const moq_event_t *e);

uint8_t *scratch_alloc(moq_session_t *s, size_t len);
void *scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align);
uint8_t *event_scratch_alloc(moq_session_t *s, size_t len);
void *event_scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align);
/* Preflight whether an aligned array (count * elem_size) plus tail_bytes of
 * unaligned data both fit in the event scratch from the current length, using
 * event_scratch_alloc_aligned's alignment math (accounts for padding; rejects
 * multiply/alignment overflow). Pure: does not mutate scratch. */
bool event_scratch_fits_aligned(const moq_session_t *s, size_t count,
                                size_t elem_size, size_t tail_bytes,
                                size_t align);
uint8_t *event_scratch_copy(moq_session_t *s, const uint8_t *data, size_t len);
uint8_t *scratch_copy(moq_session_t *s, const uint8_t *data, size_t len);

moq_result_t queue_send_control(moq_session_t *s,
                                 const uint8_t *data, size_t len);
/* Queue already-encoded bytes on a request bidi stream. Returns WOULD_BLOCK when
 * the bytes fit in send_cap but not the remaining space (retryable) and
 * MOQ_ERR_BUFFER only when they can never fit. */
moq_result_t queue_close_bidi(moq_session_t *s, moq_stream_ref_t ref);
moq_result_t queue_send_bidi(moq_session_t *s, moq_stream_ref_t ref,
                              const uint8_t *data, size_t len, bool fin);

/* Validate a reject's REDIRECT target against the error code and local role and,
 * when error_code == MOQ_REQUEST_ERROR_REDIRECT, populate the encode args' redirect
 * fields (always emitting the tail — an all-empty Redirect is valid). Returns
 * MOQ_ERR_INVAL for: a redirect target under a non-REDIRECT code; REDIRECT on a
 * non-request-stream (draft-16) profile; a client connect_uri; or a non-empty
 * track name on a namespace-scoped family. */
moq_result_t reject_apply_redirect(moq_session_t *s,
                                   struct moq_request_error_encode_args *args,
                                   const moq_redirect_target_t *rd,
                                   bool namespace_scoped);

/* Encode a terminal REQUEST_ERROR (optionally with a REDIRECT tail) into a
 * transient output-scratch buffer sized from the args, then queue it FIN on the
 * request bidi via queue_send_bidi (so BUFFER vs WOULD_BLOCK follow send_cap, not
 * the encode buffer). Reserve-before-mutate: on any error nothing is queued and
 * the scratch is restored. Used by the REDIRECT-send reject paths. */
moq_result_t queue_request_error_bidi(moq_session_t *s, moq_stream_ref_t ref,
                                      const struct moq_request_error_encode_args *args);
/* Open the local unidirectional control channel and write its first bytes
 * (used by uni-control-pair profiles). Emits MOQ_ACTION_OPEN_UNI_CONTROL with
 * the given session-minted stream_ref; data is copied into the send buffer. */
moq_result_t queue_open_uni_control(moq_session_t *s, moq_stream_ref_t ref,
                                    const uint8_t *data, size_t len);
/* reason must be a string literal or NULL (stored without copy). */
moq_result_t close_with_error(moq_session_t *s,
                               uint64_t code, const char *reason);

/* Draft-neutral inbound GOAWAY: enter DRAINING, arm the drain deadline, surface
 * MOQ_EVENT_GOAWAY. The profile decodes the wire form and does the active /
 * duplicate checks before calling this. */
moq_result_t session_core_on_goaway(moq_session_t *s,
                                    const uint8_t *uri, size_t uri_len);

/* Decoded REQUEST_ERROR Redirect (§10.6.1), profile → core. Spans borrow from the
 * profile's decode buffers (valid for the duration of the core call). */
typedef struct moq_decoded_redirect {
    const uint8_t  *connect_uri;
    size_t          connect_uri_len;
    moq_namespace_t track_namespace;   /* parts borrow from the profile buffer */
    const uint8_t  *track_name;
    size_t          track_name_len;
} moq_decoded_redirect_t;

/* Surface MOQ_EVENT_REQUEST_REDIRECT for a redirected request (draft-neutral).
 * Copies the connect URI / namespace / name / reason into output scratch and
 * pushes the event; the caller (the family error handler) performs the terminal
 * free/drain. handle_opaque is the request's public handle value. */
moq_result_t session_core_emit_request_redirect(
    moq_session_t *s, moq_request_family_t family, uint64_t handle_opaque,
    const moq_decoded_redirect_t *rd, uint64_t error_code,
    bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len);

void sg_free_entry(size_t slot, moq_sg_entry_t *entries);
void sg_reap_terminal(moq_session_t *s);

/* -- Inbound data reordering buffer (data before SUBSCRIBE_OK) ------ *
 * The Track Alias is assigned by the publisher in SUBSCRIBE_OK; data and
 * control travel on independent QUIC streams, so an object can arrive for an
 * alias before the OK that establishes it. Such data is held briefly to
 * absorb control/data reordering, then released once the OK assigns the
 * alias, or discarded when no forwarding subscription remains pending. */

/* True when at least one subscriber-role subscription is still PENDING with
 * Forward State 1 -- the only case where an as-yet-unestablished alias may be
 * established imminently by a SUBSCRIBE_OK, so early data for it is held
 * rather than dropped. */
bool session_has_forwarding_pending_subscriber(moq_session_t *s);

/* Release held datagrams whose alias was just established by a SUBSCRIBE_OK
 * (called after the OK event is queued, so released objects order after it).
 * Also flips any DEFERRED_ALIAS rx streams for the alias to AWAITING_OBJECT.
 * Backpressure-safe: items that cannot be delivered now stay held. */
void session_release_staged_for_alias(moq_session_t *s, uint64_t alias);

/* Retry releasing all held data (e.g. after event-queue capacity frees up).
 * Each held item is delivered to its now-established alias if one exists. */
void session_replay_staged(moq_session_t *s);

/* Discard all held datagrams and deferred-alias rx streams when no
 * forwarding subscription remains pending (their alias can never be
 * established). Safe to call whenever a pending subscription is freed. */
void session_discard_staged_if_no_pending(moq_session_t *s);

/* Bind + resume any subgroup/uni streams deferred on `alias` (a SUBSCRIBE_OK
 * just established it): flip them to AWAITING_OBJECT and drive their buffered
 * objects. Backpressure-safe; undrained streams are retried. */
void session_resume_deferred_for_alias(moq_session_t *s, uint64_t alias);

/* Re-drive deferred streams that were bound but could not fully drain under
 * event-queue backpressure (called after poll frees capacity). */
void session_retry_resumed_deferred(moq_session_t *s);

/* Stop + free all streams still deferred on an unestablished alias. */
void session_discard_deferred_streams(moq_session_t *s);
void sg_recompute_deadline(moq_session_t *s);
int sg_find_free(moq_session_t *s);
moq_subgroup_handle_t sg_make_handle(moq_session_t *s, size_t slot);

int sub_resolve_handle(moq_session_t *s, moq_subscription_t h);
int sub_find_by_request_id(moq_session_t *s, uint64_t request_id);
int sub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias);
bool sub_track_alias_in_use(moq_session_t *s, uint64_t alias);

/* -- Auth token processing (session_auth.c) ------------------------ */

/*
 * Process AUTH_TOKEN params from a request message (SUBSCRIBE,
 * PUBLISH_NAMESPACE).  Handles REGISTER/DELETE/USE_ALIAS/USE_VALUE.
 *
 * Returns < 0 for session-close errors (session already closed).
 * Returns MOQ_OK with *out_reject_code != 0 for request-level rejects
 * (REGISTERs already committed to cache).
 * Returns MOQ_OK with *out_reject_code == 0 on success.
 *
 * tokens_in is a pre-decoded array of semantic auth tokens (profile
 * extracts and decodes from wire format before calling this).
 * out_tokens must point to caller-owned array of max_tokens entries.
 * Token value pointers borrow from decode buffer or cache; caller must
 * copy to scratch before the event is pushed.
 */
moq_result_t process_auth_tokens(moq_session_t *s,
    const moq_decoded_auth_token_t *tokens_in, size_t tokens_in_count,
    moq_resolved_token_t *out_tokens, size_t *out_token_count,
    size_t max_tokens,
    uint64_t *out_reject_code,
    bool *out_staged,
    moq_auth_txn_t *txn);

void process_auth_tokens_commit_txn(moq_session_t *s, moq_auth_txn_t *txn);
void process_auth_tokens_abort_txn(moq_session_t *s, moq_auth_txn_t *txn);

void process_auth_tokens_free_staging(moq_session_t *s,
    moq_resolved_token_t *tokens, const bool *staged,
    size_t count);

/*
 * Semantic validation of a RESOLVED token value -- the "well-formed Token
 * structure but otherwise invalid AUTHORIZATION TOKEN parameter MUST [be
 * rejected] with an MALFORMED_AUTH_TOKEN error" rule (identical text in
 * draft-16 §9.2.2.1 and draft-18 §10.2.2, so the check is shared, not
 * capability-gated). Runs AFTER alias resolution yields a concrete value;
 * structural decoding failures stay on their existing close paths.
 *
 * Deliberately minimal and deterministic -- the seam where application-
 * level token validation will plug in later, NOT application policy:
 *   - a zero-length resolved value is semantically malformed;
 *   - a value containing a NUL byte is semantically malformed.
 */
bool moq_auth_token_value_semantically_valid(const uint8_t *value,
                                             size_t value_len);

/* Copy resolved token values + the token array into output scratch for
 * borrow-epoch-safe event delivery (session_subscribe.c; shared with the
 * publish update handler). */
moq_result_t session_stage_tokens_for_event(moq_session_t *s,
    moq_resolved_token_t *tokens, bool *staged, size_t count,
    size_t scratch_saved, moq_resolved_token_t **out);

/* -- Setup handler (session_setup.c → profile) --------------------- */

moq_result_t handle_start(moq_session_t *s);

/* -- Namespace handlers (session_namespace.c) ---------------------- */

/* handle_publish_namespace_done moved to profile_d16.c */
/* handle_publish_namespace_cancel moved to profile_d16.c */
typedef struct moq_decoded_announcement_ok {
    int target_slot;
} moq_decoded_announcement_ok_t;

moq_result_t session_core_on_announcement_ok(moq_session_t *s,
                                              const moq_decoded_announcement_ok_t *d);

typedef struct moq_decoded_announcement_error {
    int            target_slot;
    uint64_t       error_code;
    bool           can_retry;
    uint64_t       retry_after_ms;
    const uint8_t *reason;
    size_t         reason_len;
} moq_decoded_announcement_error_t;

moq_result_t session_core_on_announcement_error(moq_session_t *s,
                                                 const moq_decoded_announcement_error_t *d,
                                                 const moq_decoded_redirect_t *redirect);
int ann_find_by_request_id(moq_session_t *s, uint64_t request_id);

/* -- Outbound subscribe-family encode args (session core → profile) ---- */

typedef struct moq_subscribe_encode_args {
    uint64_t request_id;
    moq_namespace_t track_namespace;
    moq_bytes_t track_name;
    uint8_t subscriber_priority;
    uint8_t group_order;
    bool has_forward;
    bool forward;
    uint32_t filter;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
} moq_subscribe_encode_args_t;

typedef struct moq_subscribe_ok_encode_args {
    uint64_t request_id;
    uint64_t track_alias;
    bool has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool has_expires;
    uint64_t expires_ms;
    const uint8_t *track_properties;
    size_t track_properties_len;
} moq_subscribe_ok_encode_args_t;

typedef struct moq_request_error_encode_args {
    uint64_t       request_id;
    uint64_t       error_code;
    bool           can_retry;
    uint64_t       retry_after_ms;
    const uint8_t *reason;
    size_t         reason_len;
    /* REDIRECT (§10.6.1) tail: present only when has_redirect. The draft-18 op
     * emits the Redirect structure (an all-empty Redirect is valid); the draft-16
     * op rejects has_redirect (no REDIRECT in draft-16). */
    bool            has_redirect;
    const uint8_t  *connect_uri;
    size_t          connect_uri_len;
    moq_namespace_t redirect_namespace;   /* parts borrow from the caller */
    const uint8_t  *redirect_track_name;
    size_t          redirect_track_name_len;
} moq_request_error_encode_args_t;

typedef struct moq_request_update_encode_args {
    uint64_t request_id;
    uint64_t existing_request_id;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
    const moq_auth_token_t *auth_tokens;   /* USE_VALUE; may be NULL */
    size_t                  auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
} moq_request_update_encode_args_t;

#define MOQ_DECODED_MAX_NAMESPACE_PARTS 32

/* -- Decoded inbound SUBSCRIBE (profile → session core) --------------- */

typedef struct moq_decoded_subscribe {
    moq_request_endpoint_t endpoint;
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_namespace_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t      track_name;         /* borrowed from payload */
    uint64_t         request_id;         /* wire request ID (for reject paths) */
    uint8_t          subscriber_priority;/* default 128 */
    uint8_t          group_order;        /* MOQ_GROUP_ORDER_* */
    bool             has_forward;
    bool             forward;
    bool             has_delivery_timeout;
    uint64_t         delivery_timeout_us;
    bool             has_filter;
    uint32_t         filter_type;
    uint64_t         start_group;
    uint64_t         start_object;
    uint64_t         end_group;
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code; /* non-zero: message-level token reject */
    bool             has_new_group_request;
    uint64_t         new_group_request;
} moq_decoded_subscribe_t;

/* -- Decoded inbound SUBSCRIBE_OK (profile → session core) ----------- */

typedef struct moq_decoded_subscribe_ok {
    uint64_t request_id;
    uint64_t track_alias;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    const uint8_t *track_properties;   /* borrowed from payload */
    size_t   track_properties_len;
    bool     track_properties_unsupported;  /* unknown Mandatory Track Property */
    bool     has_deferred_param_error;
    const char *deferred_param_reason;
    bool     dynamic_groups;   /* Track Property/Extension 0x30 == 1 */
} moq_decoded_subscribe_ok_t;

/* -- Decoded inbound REQUEST_UPDATE (profile → session core) --------- */

typedef struct moq_decoded_request_update {
    moq_request_endpoint_t endpoint;
    uint64_t request_id;
    moq_request_kind_t target_kind;
    int      target_slot;
    bool     has_unsupported;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
    /* Resolved AUTHORIZATION_TOKEN parameters (both profiles; the auth
     * transaction commit/abort is a safe no-op on an empty transaction).
     * auth_reject_code is non-zero when a token failed at the message level
     * (the update is rejected rather than applied). */
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code;
    bool     has_new_group_request;
    uint64_t new_group_request;
} moq_decoded_request_update_t;

/* -- Decoded inbound PUBLISH_NAMESPACE (profile → session core) ------- */

typedef struct moq_decoded_publish_namespace {
    moq_request_endpoint_t endpoint;
    uint64_t         request_id;
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_namespace_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code; /* non-zero: message-level token reject */
} moq_decoded_publish_namespace_t;

moq_result_t session_core_on_publish_namespace(moq_session_t *s,
                                                moq_decoded_publish_namespace_t *d);

/* Dispatch bytes arriving on an established PUBLISH_NAMESPACE request bidi
 * (stream-correlated profiles), role-keyed: the announcer side parses the
 * REQUEST_OK/REQUEST_ERROR response, the receiver side parses an inbound
 * REQUEST_UPDATE. Buffers fragmented control messages in the announce entry.
 * Defined in session_namespace.c. */
moq_result_t handle_announcement_stream_bytes(moq_session_t *s, int slot,
                                              moq_stream_ref_t stream_ref,
                                              const uint8_t *buf, size_t len,
                                              bool fin);

/* A REQUEST_UPDATE on an established PUBLISH_NAMESPACE bidi is unsupported
 * (§10.9.1 mandates closing the bidi on a failed update): emit
 * REQUEST_ERROR(NOT_SUPPORTED) + FIN on the request bidi, commit the update's
 * request id, and terminate the announcement. `uep` is the validated update
 * endpoint. */
moq_result_t session_core_on_announce_update_rejected(
    moq_session_t *s, int slot, const moq_request_endpoint_t *uep);

/* Peer teardown (RESET/STOP) of an established PUBLISH_NAMESPACE bidi: role-keyed
 * NAMESPACE_DONE (receiver) / NAMESPACE_CANCELLED (announcer) + free. */
moq_result_t session_core_on_announce_torn_down(moq_session_t *s, int slot);

typedef struct moq_decoded_publish_namespace_done {
    uint64_t request_id;
    int      target_slot;
} moq_decoded_publish_namespace_done_t;

moq_result_t session_core_on_publish_namespace_done(
    moq_session_t *s, const moq_decoded_publish_namespace_done_t *d);

typedef struct moq_decoded_publish_namespace_cancel {
    uint64_t       request_id;
    int            target_slot;
    uint64_t       error_code;
    const uint8_t *reason;      /* borrowed from payload */
    size_t         reason_len;
} moq_decoded_publish_namespace_cancel_t;

moq_result_t session_core_on_publish_namespace_cancel(
    moq_session_t *s, const moq_decoded_publish_namespace_cancel_t *d);

/* -- Decoded inbound SUBSCRIBE_NAMESPACE (profile → session core) ---- */

#define MOQ_DECODED_NS_SUB_MAX_PARTS MOQ_DECODED_MAX_NAMESPACE_PARTS

typedef struct moq_decoded_ns_sub_request {
    uint64_t                 request_id;
    moq_namespace_interest_t namespace_interest;
    moq_namespace_t          prefix;                     /* parts → parts_buf */
    moq_bytes_t              parts_buf[MOQ_DECODED_NS_SUB_MAX_PARTS];
    bool                     has_trailing_bytes;
    bool                     forward;
    moq_decoded_auth_token_t auth_tokens[MOQ_DECODED_MAX_TOKENS];
    size_t                   auth_token_count;
} moq_decoded_ns_sub_request_t;

/* -- Decoded inbound NS sub response (profile → session core) -------- */

typedef enum moq_ns_sub_response_kind {
    MOQ_NS_RESP_OK             = 1,
    MOQ_NS_RESP_ERROR          = 2,
    MOQ_NS_RESP_NAMESPACE      = 3,  /* found */
    MOQ_NS_RESP_NAMESPACE_DONE = 4,  /* gone */
    MOQ_NS_RESP_GOAWAY         = 5,  /* request-stream GOAWAY (migrate) */
} moq_ns_sub_response_kind_t;

typedef struct moq_decoded_ns_sub_response {
    moq_ns_sub_response_kind_t kind;
    size_t consumed;                 /* bytes consumed from input */

    /* For ERROR: */
    uint64_t error_code;
    uint64_t retry_interval;         /* REQUEST_ERROR Retry Interval (0 = none) */
    const uint8_t *reason;           /* borrowed from recv_buf */
    size_t reason_len;
    bool has_trailing_bytes;         /* trailing after ERROR = close */
    /* For ERROR with code REDIRECT: the decoded Redirect (§10.6.1). The redirect
     * namespace parts borrow from parts_buf (unused by an ERROR otherwise). */
    bool has_redirect;
    moq_decoded_redirect_t redirect;
    /* For GOAWAY (request-stream migration, §10.4): */
    const uint8_t *goaway_uri;       /* borrowed; NULL/0 if none */
    size_t         goaway_uri_len;
    uint64_t       goaway_timeout_ms;

    /* For NAMESPACE/NAMESPACE_DONE: */
    moq_namespace_t suffix;          /* parts in parts_buf */
    moq_bytes_t parts_buf[MOQ_DECODED_NS_SUB_MAX_PARTS];

    /* Error detail for close paths */
    const char *close_reason;        /* set when profile returns PROTO */
} moq_decoded_ns_sub_response_t;

/* -- Decoded inbound FETCH (profile → session core) ------------------- */

typedef struct moq_decoded_fetch {
    moq_request_endpoint_t endpoint;
    uint64_t         request_id;
    uint32_t         fetch_type;
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_namespace_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t      track_name;
    uint64_t         start_group;
    uint64_t         start_object;
    uint64_t         end_group;
    uint64_t         end_object;
    uint64_t         joining_request_id;
    uint64_t         joining_start;
    int              joining_sub_slot;
    uint8_t          subscriber_priority;
    uint8_t          group_order;
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code; /* non-zero: message-level token reject */
    bool             request_fin;      /* the request bidi FIN'd in this chunk */
} moq_decoded_fetch_t;

typedef struct moq_decoded_fetch_cancel {
    uint64_t request_id;
    int      target_slot;
} moq_decoded_fetch_cancel_t;

/* -- Decoded inbound FETCH_OK (profile → session core) ---------------- */

typedef struct moq_decoded_fetch_ok {
    uint64_t       request_id;
    int            target_slot;
    bool           end_of_track;
    uint64_t       end_group;
    uint64_t       end_object;
    bool           has_deferred_param_error;
    const char    *deferred_param_reason;
    const uint8_t *track_properties;
    size_t         track_properties_len;
    bool           track_properties_unsupported;  /* unknown Mandatory Track Property */
} moq_decoded_fetch_ok_t;

/* -- Decoded inbound FETCH_HEADER (profile → session core) ------------ */

typedef struct moq_decoded_fetch_stream_header {
    uint64_t request_id;
} moq_decoded_fetch_stream_header_t;

/* -- Decoded inbound fetch object (profile → session core) ------------ */

typedef struct moq_decoded_fetch_object {
    bool           is_range_marker;
    uint32_t       range_kind;
    uint64_t       group_id;
    uint64_t       subgroup_id;   /* 0 when datagram (no subgroup, §11.4.4.1) */
    uint64_t       object_id;
    uint8_t        publisher_priority;
    bool           datagram;       /* serialization flag 0x40: Datagram forwarding
                                    * preference -- subgroup LSBs ignored */
    uint64_t       payload_len;
    const uint8_t *properties;
    size_t         properties_len;
} moq_decoded_fetch_object_t;

/* -- Outbound fetch object encode args (session core → profile) ------- */

typedef struct moq_fetch_object_encode_args {
    uint64_t group_id;
    uint64_t subgroup_id;
    uint64_t object_id;
    uint8_t  publisher_priority;
    bool     datagram;       /* forwarding preference Datagram (0x40): no subgroup */
    uint64_t payload_len;
    size_t   properties_len;
    bool     header_only;
} moq_fetch_object_encode_args_t;

/* -- Outbound accept-fetch encode args (session core → profile) ------- */

typedef struct moq_accept_fetch_encode_args {
    uint64_t request_id;
    bool     end_of_track;
    uint64_t end_group;
    uint64_t end_object;
    const uint8_t *track_properties;
    size_t   track_properties_len;
} moq_accept_fetch_encode_args_t;

/* -- Outbound FETCH encode args (session core → profile) -------------- */

typedef struct moq_fetch_encode_args {
    uint64_t        request_id;
    uint32_t        fetch_type;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    uint64_t        start_group;
    uint64_t        start_object;
    uint64_t        end_group;
    uint64_t        end_object;
    uint64_t        joining_request_id;
    uint64_t        joining_start;
    uint8_t         subscriber_priority;
    uint8_t         group_order;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_fetch_encode_args_t;

/* -- Fetch handlers (session_fetch.c) --------------------------------- */

moq_result_t session_core_on_fetch(moq_session_t *s,
                                    moq_decoded_fetch_t *d);

/* Buffered Joining FETCH (§10.12.2) lifecycle, driven by the associated
 * subscription. can_release is a reserve-before-mutate preflight for accept;
 * release surfaces FETCH_REQUEST (or INVALID_RANGE) for each buffered join once
 * the subscription is accepted; reject discards them when the subscription is
 * rejected or torn down before acceptance. */
/* Reserve-before-mutate preflight for resolving all PENDING_JOIN fetches bound to
 * subscription request id `sub_req_id`, plus the caller's own concurrent needs
 * (`extra_actions`/`extra_drains`/`extra_send`). The outcome decides each join:
 * with `has_largest` true a relative join (or an absolute join whose start is at
 * or before `largest_group`) releases as a FETCH_REQUEST event (scratch-copied
 * tokens), while an absolute join past `largest_group` rejects; `has_largest`
 * false models a reject-all outcome (every join rejects). Returns MOQ_OK if current
 * capacity covers the whole batch alongside the caller's extras, MOQ_ERR_BUFFER if
 * the batch can never fit (a permanent capacity shortfall — retry cannot help), or
 * MOQ_ERR_WOULD_BLOCK on a transient shortfall (retry after draining). */
moq_result_t session_core_pending_joins_can_resolve(
    moq_session_t *s, uint64_t sub_req_id,
    bool has_largest, uint64_t largest_group,
    size_t extra_actions, size_t extra_drains, size_t extra_send);
/* Resolve all PENDING_JOIN fetches for the now-accepted subscription in `sub_slot`
 * (release into FETCH_REQUEST, or reject INVALID_RANGE). Capacity MUST have been
 * reserved by session_core_pending_joins_can_resolve; returns MOQ_OK in that case
 * (the error return is defensive). */
moq_result_t session_core_release_pending_joins(moq_session_t *s, int sub_slot);
/* Reject all PENDING_JOIN fetches for `sub_req_id` (INVALID_JOINING_REQUEST_ID +
 * FIN on each join bidi, then free). Reserve-before-mutate per join; capacity MUST
 * have been reserved by the preflight so it returns MOQ_OK (error is defensive). */
moq_result_t session_core_reject_pending_joins(moq_session_t *s, uint64_t sub_req_id);
/* Free all PENDING_JOIN fetches for `sub_req_id` WITHOUT emitting any control
 * message (drops the entry-owned token storage). The session-teardown safety net
 * behind sub_free_entry: the alive teardown paths (public reject / inbound bidi
 * teardown) reject pending joins explicitly beforehand, so this only fires on a
 * closing session, where no late stream byte is delivered (the bytes handler
 * short-circuits on CLOSED). */
void session_core_discard_pending_joins(moq_session_t *s, uint64_t sub_req_id);

moq_result_t session_core_on_fetch_cancel(moq_session_t *s,
                                           const moq_decoded_fetch_cancel_t *d);

int fetch_resolve_handle(moq_session_t *s, moq_fetch_t h);
int fetch_find_free(moq_session_t *s);
void fetch_free_entry(moq_session_t *s, int slot);

/* Release a FETCH entry on a request-stream GOAWAY (§10.4 migration). If the
 * response data uni already presented its FETCH_HEADER, the cached rx handle
 * absorbs the remainder so the entry is freed outright. Otherwise a migration
 * tombstone (MOQ_FETCH_GOAWAY_LOCAL) is left so a late FETCH_HEADER is absorbed
 * instead of closing the session; the data uni is left alone (no STOP_SENDING). */
void fetch_on_request_goaway_release(moq_session_t *s, int slot);

/* Surface a FETCH error. free_now frees the entry immediately (responses on the
 * shared control channel); when false the entry is marked draining and kept so
 * the request bidi can absorb its trailing FIN (stream-correlated profiles). */
moq_result_t session_core_on_fetch_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now,
    const moq_decoded_redirect_t *redirect);

moq_result_t session_core_on_fetch_ok(moq_session_t *s,
                                       const moq_decoded_fetch_ok_t *d);

int fetch_find_by_request_id(moq_session_t *s, uint64_t request_id);

/* -- Decoded inbound PUBLISH (profile → session core) ----------------- */

typedef struct moq_decoded_publish {
    moq_request_endpoint_t endpoint;
    uint64_t         request_id;
    moq_namespace_t  track_namespace;
    moq_bytes_t      track_namespace_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t      track_name;
    uint64_t         track_alias;
    bool             has_forward;
    bool             forward;
    const uint8_t   *track_properties;
    size_t           track_properties_len;
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    /* Non-zero ⇒ a message-level authorization-token reject: the request fails
     * with this REQUEST_ERROR code and surfaces no event (stream-correlated
     * profiles route it on the request bidi; draft-16 handles reject in its
     * profile decode and leaves this zero). */
    uint64_t         auth_reject_code;
    bool             track_properties_unsupported;  /* unknown Mandatory Track Property */
    bool             dynamic_groups;   /* Track Property/Extension 0x30 == 1 */
} moq_decoded_publish_t;

typedef struct moq_decoded_publish_ok {
    uint64_t request_id;
    int      target_slot;
    bool     has_deferred_param_error;
    const char *deferred_param_reason;
    bool     forward;
    uint8_t  subscriber_priority;
    uint8_t  group_order;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_ms;
    bool     has_expires;
    uint64_t expires_ms;
    bool     has_new_group_request;
    uint64_t new_group_request;
} moq_decoded_publish_ok_t;

/* -- Outbound PUBLISH encode args (session core → profile) ------------ */

typedef struct moq_publish_encode_args {
    uint64_t        request_id;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    uint64_t        track_alias;
    bool            has_forward;
    bool            forward;
    const uint8_t  *track_properties;
    size_t          track_properties_len;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_publish_encode_args_t;

/* -- Outbound finish-publish encode args (session core → profile) ----- */

typedef struct moq_finish_publish_encode_args {
    uint64_t       request_id;
    uint64_t       status_code;
    uint64_t       stream_count;
    const uint8_t *reason;
    size_t         reason_len;
} moq_finish_publish_encode_args_t;

/* -- Decoded inbound PUBLISH_DONE (profile → session core) ------------ */

typedef struct moq_decoded_publish_done {
    uint64_t       request_id;
    int            target_slot;
    uint64_t       status_code;
    uint64_t       stream_count;
    const uint8_t *reason;
    size_t         reason_len;
} moq_decoded_publish_done_t;

/* -- Publish handlers (session_publish.c) ----------------------------- */

moq_result_t session_core_on_publish(moq_session_t *s,
                                      moq_decoded_publish_t *d);
moq_result_t session_core_on_publish_ok(moq_session_t *s,
                                         const moq_decoded_publish_ok_t *d);
/* Surface a PUBLISH error (REQUEST_ERROR rejecting our outbound PUBLISH).
 * free_now frees the entry immediately (responses on the shared control
 * channel); when false the entry is marked draining and kept so the request bidi
 * can absorb its trailing FIN, and our send half is closed reciprocally
 * (stream-correlated profiles). */
moq_result_t session_core_on_publish_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now);

int pub_resolve_handle(moq_session_t *s, moq_publication_t h);
int pub_find_free(moq_session_t *s);
void pub_free_entry(moq_session_t *s, int slot);
int pub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias);
/* Track-alias collision checks shared with the subscription paths (the data
 * alias namespace is shared between publications and subscriptions). _in_use
 * covers any established publication plus a pending subscriber-role one (inbound
 * data alias); _outbound covers non-free publisher-role publications (outbound
 * data alias). */
bool pub_track_alias_in_use(moq_session_t *s, uint64_t alias);
bool pub_outbound_alias_in_use(moq_session_t *s, uint64_t alias);
/* Count a completed data stream toward a subscriber-role publication's
 * PUBLISH_DONE Stream Count, finalizing a deferred PUBLISH_FINISHED when the
 * count is reached. pub_reap_deferred_dones retries a finalize that could not be
 * queued earlier (event queue full). */
void pub_note_stream_processed(moq_session_t *s, moq_publication_t pub);
void pub_reap_deferred_dones(moq_session_t *s);

/* Surface PUBLISH_DONE (the publisher's terminal on the subscriber side).
 * free_now frees the entry immediately (draft-16, control channel); when false
 * the entry is marked draining and kept so the request bidi can absorb its
 * trailing FIN, and our send half is closed reciprocally (stream-correlated). */
moq_result_t session_core_on_publish_done(moq_session_t *s,
                                           const moq_decoded_publish_done_t *d,
                                           bool free_now);
moq_result_t session_core_on_publish_unsubscribed(moq_session_t *s, int slot);
moq_result_t session_core_on_publish_request_update(
    moq_session_t *s, moq_decoded_request_update_t *d);

/* -- Track-status decoded structs and encode args -------------------- */

typedef struct moq_decoded_track_status_request {
    moq_request_endpoint_t    endpoint;
    moq_namespace_t           track_namespace;
    moq_bytes_t               track_namespace_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t               track_name;
    moq_resolved_token_t      tokens[MOQ_DECODED_MAX_TOKENS];
    bool                      token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t                    token_count;
    moq_auth_txn_t            auth_txn;
    /* Non-zero ⇒ a message-level authorization-token reject: the request fails
     * with this REQUEST_ERROR code and surfaces no event (a REGISTER still
     * commits its alias). */
    uint64_t                  auth_reject_code;
} moq_decoded_track_status_request_t;

typedef struct moq_track_status_encode_args {
    uint64_t        request_id;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_track_status_encode_args_t;

/* TRACK_STATUS_OK response (draft-neutral): the publisher's status reply. The
 * draft-16 profile carries it as a control-channel REQUEST_OK with LARGEST_OBJECT
 * / EXPIRES params (no Track Properties tail); draft-18 as a request-bidi
 * REQUEST_OK with the same params plus an opaque Track Properties tail. */
typedef struct moq_track_status_ok_encode_args {
    uint64_t       request_id;
    bool           has_largest;
    uint64_t       largest_group;
    uint64_t       largest_object;
    bool           has_expires;
    uint64_t       expires_ms;
    const uint8_t *track_properties;     /* draft-18 only; NULL/empty for draft-16 */
    size_t         track_properties_len;
} moq_track_status_ok_encode_args_t;

typedef struct moq_decoded_track_status_ok {
    int      target_slot;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    /* Opaque Track Properties tail (draft-18 TRACK_STATUS_OK); borrowed from the
     * decode buffer. Empty under draft-16 (no properties tail). */
    const uint8_t *track_properties;
    size_t         track_properties_len;
} moq_decoded_track_status_ok_t;

/* -- Track-status handlers (session_track_status.c) ------------------- */

moq_result_t session_core_on_track_status_request(moq_session_t *s,
    moq_decoded_track_status_request_t *d, bool request_fin);
moq_result_t session_core_on_track_status_ok(moq_session_t *s,
    const moq_decoded_track_status_ok_t *d);
moq_result_t session_core_on_track_status_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len,
    const moq_decoded_redirect_t *redirect);

int ts_resolve_handle(moq_session_t *s, moq_track_status_handle_t h);
int ts_find_free(moq_session_t *s);
void ts_free_entry(moq_session_t *s, int slot);

/* Per-family entry free, shared with the request-stream GOAWAY handler. */
void ann_free_entry(moq_session_t *s, size_t slot);
void ns_sub_free_entry(moq_session_t *s, size_t slot);

/* Surface MOQ_EVENT_REQUEST_GOAWAY for a request migrated by a request-stream
 * GOAWAY (§10.4): free the request entry, close our send half if still open, and
 * strict-drain the bidi. Leaves data streams alone (graceful migration). The
 * caller resolves family+slot from the bidi's stream ref. */
moq_result_t session_core_on_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    moq_stream_ref_t ref, const uint8_t *uri, size_t uri_len,
    uint64_t timeout_ms);

/* Outbound per-request GOAWAY (§10.4): the shared sender behind the seven typed
 * public wrappers. Enforces the family×state eligibility matrix, draft-16
 * MOQ_ERR_INVAL, and the client-zero-URI rule, with reserve-before-mutate; encodes
 * via moq_d18_encode_goaway_request and queues on the request bidi with fin=false
 * (our send half stays open for the deferred timeout-reset; data streams stay
 * alive). On success it only marks the entry's goaway_sent -- §10.4 "does not
 * impact subscription state", so the entry stays live and old-session output
 * continues until the peer tears the old stream down. The caller (wrapper) has
 * resolved `slot` for `family` after session_begin_advance. */
moq_result_t session_core_send_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms);

/* Send-side counterparts (the entry stays live after we send a GOAWAY):
 *  - free_on_teardown: the peer's empty FIN / RESET / STOP on a request carrying our
 *    GOAWAY marker silently retires it (free, no event); returns true if handled.
 *  - already_sent: true iff the request carries our GOAWAY marker, so a GOAWAY
 *    *received* on it is a second GOAWAY on the stream (PROTOCOL_VIOLATION). */
bool request_goaway_free_on_teardown(moq_session_t *s, moq_stream_ref_t ref);
bool request_goaway_already_sent(moq_session_t *s, moq_stream_ref_t ref);

/* Retire a request migrated by a per-request GOAWAY (sent or received): strict-
 * drain the request bidi (FIN/RESET/STOP retire it; a duplicate GOAWAY or stray
 * non-empty bytes close 0x3) and free/tombstone the entry, leaving data streams
 * intact. The caller has reserved the drain-ref slot. */
void request_goaway_retire(moq_session_t *s, moq_request_family_t family,
                           int slot, moq_stream_ref_t ref);

/* -- Shared namespace-prefix helpers (ns_sub + track_sub) ------------ *
 * Two namespace prefixes overlap when one is a prefix of the other (an empty
 * prefix matches all); §10.18/§10.19. The namespace-sub and track-sub pools have
 * independent overlap spaces, so each scans only its own pool, but the prefix
 * test and the store/free of a prefix into entry-owned buffers are identical. */
bool moq_prefix_overlaps(const moq_bytes_t *a_parts, size_t a_count,
                         const moq_bytes_t *b_parts, size_t b_count);
bool moq_prefix_store(moq_session_t *s, const moq_namespace_t *ns,
                      uint8_t **out_buf, size_t *out_buf_len,
                      moq_bytes_t **out_parts, size_t *out_count);
void moq_prefix_free(moq_session_t *s, uint8_t **buf, size_t *buf_len,
                     moq_bytes_t **parts, size_t *count);

/* -- SUBSCRIBE_TRACKS decoded structs + handlers (draft-18) ---------- */

typedef struct moq_decoded_subscribe_tracks_request {
    moq_request_endpoint_t endpoint;
    moq_namespace_t        track_namespace_prefix;   /* parts → prefix_parts */
    moq_bytes_t            prefix_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    bool                   forward;
    moq_resolved_token_t   tokens[MOQ_DECODED_MAX_TOKENS];
    bool                   token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t                 token_count;
    moq_auth_txn_t         auth_txn;
    /* Non-zero ⇒ a message-level authorization-token reject: the request fails
     * with this REQUEST_ERROR code and surfaces no event (a REGISTER still
     * commits its alias). */
    uint64_t               auth_reject_code;
} moq_decoded_subscribe_tracks_request_t;

typedef struct moq_decoded_publish_blocked {
    int             target_slot;
    moq_namespace_t track_namespace_suffix;          /* parts → suffix_parts */
    moq_bytes_t     suffix_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t     track_name;
} moq_decoded_publish_blocked_t;

moq_result_t session_core_on_subscribe_tracks(moq_session_t *s,
    moq_decoded_subscribe_tracks_request_t *d, bool request_fin);
moq_result_t session_core_on_subscribe_tracks_ok(moq_session_t *s, int slot);
moq_result_t session_core_on_subscribe_tracks_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len);
moq_result_t session_core_on_publish_blocked(moq_session_t *s, int slot,
    const moq_decoded_publish_blocked_t *d);
moq_result_t session_core_on_subscribe_tracks_torn_down(moq_session_t *s, int slot);
/* §10.9.1: a REQUEST_UPDATE on a SUBSCRIBE_TRACKS bidi is unsupported here;
 * REQUEST_ERROR + FIN on the bidi, commit the update's request id, terminate. */
moq_result_t session_core_on_track_sub_update_rejected(moq_session_t *s,
    int slot, const moq_request_endpoint_t *uep);

int track_sub_resolve_handle(moq_session_t *s, moq_track_sub_handle_t h);
int track_sub_find_free(moq_session_t *s);
void track_sub_free_entry(moq_session_t *s, int slot);
bool track_sub_prefix_conflicts(moq_session_t *s, const moq_bytes_t *parts,
                                size_t count, int exclude_slot);

/* -- Subscribe handlers (session_subscribe.c) ---------------------- */

/* Handle a decoded inbound SUBSCRIBE. reserved_slot < 0 means find a free slot
 * (profiles that carry requests on the shared control channel); reserved_slot
 * >= 0 commits into an already-reserved slot (stream-correlated profiles, where
 * the request bidi was reserved as MOQ_SUB_RECVING_REQUEST while its bytes were
 * buffered). */
moq_result_t session_core_on_subscribe(moq_session_t *s,
                                        moq_decoded_subscribe_t *d,
                                        int reserved_slot);

/* Inbound bidi bytes for a stream-correlated request stream: buffer into the
 * per-entry request-stream buffer and dispatch via the profile. */
moq_result_t handle_request_stream_bytes(moq_session_t *s,
                                          moq_stream_ref_t stream_ref,
                                          const uint8_t *buf, size_t len,
                                          bool fin);
/* Handle a decoded SUBSCRIBE_OK. resolved_slot < 0 looks the subscription up by
 * request id (profiles that carry responses on the shared control channel);
 * resolved_slot >= 0 targets that slot directly (stream-correlated profiles,
 * where the response is correlated by the request bidi stream identity). */
moq_result_t session_core_on_subscribe_ok(moq_session_t *s,
                                           const moq_decoded_subscribe_ok_t *d,
                                           int resolved_slot);
typedef struct moq_decoded_subscribe_error {
    int            target_slot;
    uint64_t       error_code;
    bool           can_retry;
    uint64_t       retry_after_ms;
    const uint8_t *reason;
    size_t         reason_len;
} moq_decoded_subscribe_error_t;

/* Surface a decoded request error for d->target_slot. Profiles whose responses
 * arrive on the shared control channel free the entry immediately; stream-
 * correlated profiles instead mark it terminated and keep it, so the request
 * bidi can absorb its trailing FIN before the slot is released. */
moq_result_t session_core_on_subscribe_error(moq_session_t *s,
                                              const moq_decoded_subscribe_error_t *d,
                                              bool free_now,
                                              const moq_decoded_redirect_t *redirect);

typedef struct moq_decoded_unsubscribe {
    int target_slot;
} moq_decoded_unsubscribe_t;

moq_result_t session_core_on_unsubscribe(moq_session_t *s,
                                          const moq_decoded_unsubscribe_t *d);
moq_result_t session_core_on_subscribe_done(moq_session_t *s,
                                             int slot,
                                             const moq_decoded_publish_done_t *d,
                                             bool free_now);
moq_result_t session_core_on_request_update(moq_session_t *s,
                                             moq_decoded_request_update_t *d);
moq_result_t session_core_on_subscribe_update_ok(moq_session_t *s, int slot);
moq_result_t session_core_on_subscribe_update_error(moq_session_t *s, int slot);
bool unsub_tomb_add(moq_session_t *s, uint64_t request_id);
bool unsub_tomb_consume(moq_session_t *s, uint64_t request_id);

/* Locally-cancelled FETCH request-id grace cache. add() dedupes and drops the
 * oldest entry when full (never fails). contains() peeks without removing;
 * consume() removes and reports presence. */
void fetch_cancel_tomb_add(moq_session_t *s, uint64_t request_id);
bool fetch_cancel_tomb_contains(const moq_session_t *s, uint64_t request_id);
bool fetch_cancel_tomb_consume(moq_session_t *s, uint64_t request_id);

/* Drain-ring of locally cancelled request-bidi stream_refs (stream-correlated
 * profiles). add returns false when the ring is full; contains/remove report
 * and retire membership while draining late responses. */
/* Drain-ref reason: NORMAL absorbs late in-flight bytes until FIN/reset; a
 * GOAWAY-strict ref additionally closes the session if the peer sends any more
 * bytes on a stream it already migrated via a request-stream GOAWAY (§10.4). */
typedef enum moq_drain_reason {
    MOQ_DRAIN_NORMAL        = 0,
    MOQ_DRAIN_GOAWAY_STRICT = 1,
} moq_drain_reason_t;

bool drain_ref_add(moq_session_t *s, moq_stream_ref_t ref);
bool drain_ref_add_strict(moq_session_t *s, moq_stream_ref_t ref);
/* Reason of a present ref (MOQ_DRAIN_NORMAL if absent). */
moq_drain_reason_t drain_ref_reason(const moq_session_t *s, moq_stream_ref_t ref);
bool drain_ref_contains(const moq_session_t *s, moq_stream_ref_t ref);
bool drain_ref_remove(moq_session_t *s, moq_stream_ref_t ref);

/* -- Request registry helpers --------------------------------------- */

moq_request_endpoint_t request_registry_find_by_id(
    const moq_session_t *s, uint64_t request_id);
void request_registry_insert_by_id(
    moq_session_t *s, uint64_t request_id, moq_request_endpoint_t ep);
void request_registry_remove_by_id(
    moq_session_t *s, uint64_t request_id);

/* Stream-ref-keyed registry, parallel to the by-id helpers above. Used by
 * stream-correlated request profiles -- those that open a dedicated bidi
 * stream per request and correlate responses by stream identity. These are
 * pure index operations over idx_req_by_streamref:
 *   - find_by_streamref returns an endpoint with has_stream_ref set
 *     (kind/slot recovered from the index).
 *   - insert/remove_by_streamref touch only the stream-ref index.
 * remove_by_streamref must NOT release or clear the request pool slot, and
 * does not touch the request-id index. The stream-ref and request-id indexes
 * are independent keys onto the same request pool: a profile that populates
 * both is responsible for removing both. More generally, a terminal request
 * path must remove every populated registry key for a request before reusing
 * or freeing its pool slot, so no stale index entry can outlive the slot. */
moq_request_endpoint_t request_registry_find_by_streamref(
    const moq_session_t *s, moq_stream_ref_t stream_ref);
void request_registry_insert_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref, moq_request_endpoint_t ep);
void request_registry_remove_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref);

/* -- Profile capability accessors ----------------------------------- *
 * NULL-safe wrappers so the session core asks the profile for a capability
 * rather than testing a draft version. Defaults preserve draft-16 behavior. */

/* True if the bound profile correlates requests by per-request bidi stream
 * identity (draft-16: false). */
bool moq_session_uses_request_streams(const moq_session_t *s);

/* moq_session_classify_peer_uni is declared in session_transport.h (the
 * bridge-facing contract), since the transport bridge calls it. */

/* Validate/register an inbound request that arrived on its own bidi stream.
 * If the profile does not open a bidi stream per request (hook NULL, e.g.
 * draft-16), clears *out and returns MOQ_ERR_INVAL. */
moq_result_t moq_session_validate_inbound_request_stream(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t msg_type,
    uint64_t wire_request_id, moq_request_endpoint_t *out);

/* -- Receive handlers (session_receive.c) -------------------------- */

moq_result_t handle_data_bytes(moq_session_t *s,
                                moq_stream_ref_t stream_ref,
                                const uint8_t *data, size_t len,
                                bool fin);
moq_result_t handle_data_bytes_rcbuf(moq_session_t *s,
                                      moq_stream_ref_t stream_ref,
                                      moq_rcbuf_t *input_rcbuf,
                                      bool fin);
moq_result_t handle_data_reset(moq_session_t *s,
                                moq_stream_ref_t stream_ref);

/* -- Namespace-sub handlers (session_namespace_sub.c) --------------- */

moq_result_t ns_sub_on_new_bidi(moq_session_t *s,
                                 moq_stream_ref_t stream_ref,
                                 const uint8_t *data, size_t len);

/* Drive an ns_sub entry in RECVING_PUBLISHER through decode/validate/auth/event/
 * commit. Shared by the draft-16 bidi router and the draft-18 request-stream
 * handoff. Retryable on WOULD_BLOCK (the entry keeps its staged auth/parse). */
moq_result_t ns_sub_process_recving_publisher(moq_session_t *s,
                                              int32_t ns_slot, bool fin);

moq_result_t handle_bidi_stream_bytes(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       const uint8_t *buf, size_t len,
                                       bool fin);

/* Dispatch request bidis whose bytes were buffered before establishment
 * (§3.3 early arrival). Called by the profile when the session transitions
 * to ESTABLISHED. */
moq_result_t request_streams_refeed_deferred(moq_session_t *s);

moq_result_t handle_bidi_stream_reset(moq_session_t *s,
                                       moq_stream_ref_t stream_ref);
moq_result_t handle_bidi_stream_stop(moq_session_t *s,
                                      moq_stream_ref_t stream_ref);

/* Terminate a stream-correlated request (SUBSCRIPTION or FETCH) whose request
 * bidi was torn down by the peer: free the entry and remove its registry keys,
 * surfacing a best-effort local event. Returns true if `stream_ref` matched a
 * stream-correlated request (so the caller need not look elsewhere). */
moq_result_t request_stream_teardown(moq_session_t *s,
                                     moq_stream_ref_t stream_ref);

#endif /* MOQ_SESSION_INTERNAL_H */
