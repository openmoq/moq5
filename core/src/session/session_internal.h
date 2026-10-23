#ifndef MOQ_SESSION_INTERNAL_H
#define MOQ_SESSION_INTERNAL_H

#include "moq/session.h"
#include "request_lifetime_internal.h"
#include "moq/buf.h"
#include "moq/rcbuf.h"
#include "moq/wire.h"
#include "profile.h"
#include "transport_binding_internal.h"
#include "session_transport.h"
#include "fill_owner_internal.h"
#include "fill_input_internal.h"
#include "publisher_fill_receipt.h"
#include <string.h>

/* -- Defaults ------------------------------------------------------ */

#define MOQ_DEFAULT_MAX_ACTIONS      64
#define MOQ_DEFAULT_MAX_EVENTS       16
#define MOQ_DEFAULT_SEND_BUF         4096
#define MOQ_DEFAULT_RECV_BUF         4096
#define MOQ_DEFAULT_MAX_SUBS         64
#define MOQ_DEFAULT_OUTPUT_SCRATCH   65536
#define MOQ_DEFAULT_MAX_SUBGROUPS    64
#define MOQ_DEFAULT_MAX_FILLS        16
#define MOQ_DEFAULT_MAX_DATA_STREAMS 64
#define MOQ_DEFAULT_MAX_ANNOUNCEMENTS 64
#define MOQ_DEFAULT_MAX_OBJ_PAYLOAD  (4u * 1024 * 1024)
#define MOQ_DEFAULT_MAX_RECV_BUF (16u * 1024 * 1024)
#define MOQ_DEFAULT_MAX_NS_SUBS 16
#define MOQ_DEFAULT_MAX_NS_SUFFIXES 4096
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
_Static_assert(sizeof(moq_subscription_update_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
_Static_assert(sizeof(moq_publication_update_ok_event_t) <= MOQ_EVENT_DETAIL_MAX, "");
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
    bool        has_implementation;
    moq_bytes_t implementation;   /* borrowed from the decoded setup */
} moq_setup_params_t;

/* Forward decl: entries hold a pointer to their reserved registry record
 * (defined below, after the entry structs). */
typedef struct moq_track_hist moq_track_hist_t;

/*
 * Resolved subscription filter window. Stored
 * on the publisher-role entry at accept / REQUEST_UPDATE against ONE registry
 * snapshot; the same snapshot feeds the outbound SUBSCRIBE_OK / REQUEST_OK
 * LARGEST_OBJECT. The session stores it (reachable via package-internal
 * accessors); the facade installs it and enforces membership.
 *
 * `start_*` is the resolved first-eligible Location. `has_end` marks a finite
 * upper bound (`end_group`); an open-ended window leaves it false.
 * `unsatisfiable` is the at-ceiling degenerate case: a relative
 * filter resolved past MOQ_QUIC_VARINT_MAX in both group and object -- the
 * window excludes everything, yet the response still carries the true largest.
 */
typedef struct moq_resolved_window {
    bool                   has_window;     /* a window has been resolved/stored */
    bool                   unsatisfiable;  /* at-ceiling: excludes everything */
    bool                   has_end;        /* finite end_group present */
    bool                   has_end_object; /* the end is an object within end_group */
    moq_subscribe_filter_t filter;         /* filter type the window came from */
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;      /* valid iff has_end */
    uint64_t               end_object;     /* valid iff has_end_object; inclusive */
} moq_resolved_window_t;

/* Filter forms a wire profile can express that the public filter codes
 * cannot. They live only in decoded records, entries and windows; an event
 * reports the semantic location instead. */
#define MOQ_SUBSCRIBE_FILTER_RELATIVE_GROUP_INTERNAL 0x101u /* D22 clamped relative offset, including zero */
#define MOQ_SUBSCRIBE_FILTER_OBJECT_END_INTERNAL     0x104u /* absolute start; inclusive end_group/end_object */

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
    /* Publisher role, ended by a failed update: its REQUEST_ERROR is out and
     * its PUBLISH_DONE(UPDATE_FAILED) is held until every subgroup reset it
     * owes is queued (session_release_held_dones). No operation applies. */
    MOQ_SUB_DONE_HELD          = 6,
} moq_sub_state_t;

typedef enum moq_sub_role {
    MOQ_SUB_ROLE_SUBSCRIBER = 1,
    MOQ_SUB_ROLE_PUBLISHER  = 2,
} moq_sub_role_t;

/* Observation only: validated input has determined automatic output, but its
 * handler may still be waiting for event/action/drain credit. */
typedef enum moq_retained_output {
    MOQ_RETAINED_OUTPUT_NONE = 0,
    MOQ_RETAINED_OUTPUT_FIN,
    MOQ_RETAINED_OUTPUT_CANCEL,
    MOQ_RETAINED_OUTPUT_RESPONSE,
} moq_retained_output_t;

/* Per-request state for profiles that carry each request on its own
 * bidirectional stream: the stream's identity, the receive buffer that
 * reassembles control messages arriving on it, the peer's FIN on it, and the
 * request's direction lifetime. Zero (an empty ref) on profiles that carry
 * requests on the control stream. Embedded by every request entry so the
 * request-stream engine handles each family the same way. */
typedef struct moq_request_stream {
    moq_stream_ref_t       ref;
    uint8_t               *recv_buf;
    size_t                 recv_cap;
    size_t                 recv_len;
    bool                   recv_fin;
    uint8_t                retained_output; /* moq_retained_output_t */
    moq_request_lifetime_t lifetime;
} moq_request_stream_t;

/* Original typed UPDATE processing view, retained through raw-input retries
 * and an owed answer. Values only; never aliases mutable parent state. */
typedef struct moq_sub_update_snapshot {
    bool valid;
    uint64_t request_id;
    uint64_t processed_at_us;
    bool has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    uint8_t subscriber_priority;
    uint8_t group_order;
    bool forward;
    uint32_t filter_type;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
} moq_sub_update_snapshot_t;

/* The parameters of a subscriber's REQUEST_UPDATE on a subscription this
 * side publishes to (one it accepted, or one its PUBLISH began), as the
 * session applies them. Held on the entry while the application owes the
 * answer; a further update before the answer is folded in, later values
 * overriding earlier ones. */
typedef struct moq_sub_update {
    uint64_t request_id;          /* the newest update covered */
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
    bool     dt_has_object;
    uint64_t dt_object_ms;
    bool     dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    bool     has_filter;
    uint32_t filter_type;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
    bool     has_new_group_request;
    uint64_t new_group_request;
    /* The fill request and the update that carried it (its initiating id). */
    uint64_t fill_request_id;
    moq_fill_input_t fill;
    moq_sub_update_snapshot_t snapshot;
} moq_sub_update_t;

/* Publisher role, deferred update answers: the application owes the answer
 * to `count` REQUEST_UPDATEs whose cumulative parameters are `rec` (the
 * newest one's id in rec.request_id). The older covered ids, in arrival
 * order, live in the owned `prior_ids` (NULL while only one update is
 * covered). Cleared, and `prior_ids` freed, on every answer and when the
 * entry is freed. */
typedef struct moq_owed_update {
    bool              owed;
    uint32_t          count;
    uint32_t          prior_cap;
    uint64_t         *prior_ids;
    moq_sub_update_t  rec;
} moq_owed_update_t;

typedef struct moq_sub_entry {
    uint8_t           subscriber_priority;
    /* Publisher role: the request asked for no Track Properties in its
     * answer (draft-22 INCLUDE_PROPERTIES = 0); SUBSCRIBE_OK carries none. */
    bool              omit_track_properties;
    /* The subscription's group order as requested (subscriber role) or
     * received (publisher role); MOQ_GROUP_ORDER_DEFAULT when unstated. A
     * fill child without its own order inherits it. */
    uint8_t           group_order;
    /* Publisher role: the DEFAULT_PUBLISHER_GROUP_ORDER this side put in its
     * SUBSCRIBE_OK Track Properties (DEFAULT when absent); see
     * session_group_order(). */
    uint8_t           track_group_order;
    moq_sub_state_t    state;
    moq_sub_role_t     role;
    uint32_t           generation;
    moq_subscription_t handle;
    uint64_t           request_id;
    /* rs.ref: request bidi stream identity for stream-correlated
     * request profiles (profiles that open a dedicated bidi stream per request
     * and correlate responses by stream identity) -- here, the SUBSCRIBE
     * request stream. Empty (zero) under draft-16, which correlates by
     * request_id instead. */
    moq_request_stream_t rs;
    /* Per-request-stream receive buffer for parsing control messages that
     * arrive (possibly fragmented) on the request's bidi stream, for
     * stream-correlated request profiles. Co-allocated in the session block;
     * unused under draft-16. */
    uint64_t           track_alias;
    /* Subscriber role, ESTABLISHED: the next established subscription the
     * peer gave the same alias (a concurrent subscription to the same
     * track), in establishment order; -1 ends the chain. The alias index
     * holds the chain's first entry. */
    int32_t            alias_next;
    /* Subscriber role, pending: the newest Largest Object known here when
     * the request was sent. The publisher resolves the filter against its
     * own Largest, never older than this, so the window resolved against it
     * contains the eventual one (used to hold objects for the subscription
     * before its answer, draft-22 §3.1.3). */
    bool               sent_has_largest;
    uint64_t           sent_largest_group;
    uint64_t           sent_largest_object;
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
    /* A pending update carries a Forward change: latched into `forward`
     * only when the update is ACKNOWLEDGED (object delivery gates on the
     * current acknowledged Forward state); dropped on error/retirement. */
    bool               update_has_forward;
    bool               update_forward;
    bool               update_has_subscriber_priority;
    uint8_t            update_subscriber_priority;
    /* a filter change pends until the ACK; the acknowledged filter TYPE
     * latches into filter_type (its requester-side consumer is the
     * Joining-FETCH eligibility gate). Cleared on REQUEST_ERROR/teardown. */
    bool               update_has_filter;
    uint32_t           update_filter_type;
    /* The pending update's filter locations (subscriber role), latched into
     * req_* alongside the type when the update is acknowledged, so a later
     * fill that inherits the subscription's filter resolves the same window
     * the publisher does. */
    uint64_t           update_start_group;
    uint64_t           update_start_object;
    uint64_t           update_end_group;
    uint64_t           update_end_object;
    /* A REQUEST_UPDATE failed (REQUEST_ERROR): the subscription is awaiting the
     * mandatory terminal PUBLISH_DONE(UPDATE_FAILED). No new update may be sent,
     * and only that PUBLISH_DONE is a valid next message. */
    bool               update_failed;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* Publisher role: the peer cancelled this subscription by RESET_STREAM or
     * STOP_SENDING on its request stream. Set when the cancellation is first
     * recognized, before any capacity check, and cleared only when the entry
     * is freed. While set, nothing more is produced or sent under the
     * subscription; the entry stays resolvable only so its teardown can reset
     * every subgroup still open under it, then surface UNSUBSCRIBED and free
     * it. */
    bool cancel_pending;
    /* The peer's RESET_STREAM ended its direction of the request stream: no
     * FIN or RESET follows it, so no receive-side identity is owed for it.
     * Distinct from req_recv_fin, which records an observed FIN. */
    bool req_recv_reset;
    /* Publisher role: an update answer the application owes. */
    moq_owed_update_t  owed_update;
    moq_sub_update_snapshot_t update_snapshot;
    /* The track supports dynamically started groups (DYNAMIC_GROUPS == 1 in
     * SUBSCRIBE_OK's track properties). Gates outbound new-group requests on
     * this subscription's updates. */
    bool dynamic_groups;
    /* Reserved per-track largest-location registry record. Reserved
     * at establishment (keyed by track_id_buf), released once in sub_free_entry.
     * Non-NULL on an established entry (the registry is always sized >= 1, so a
     * reserve failure rejects the request rather than establishing with a NULL
     * record); merges still guard on it defensively. Received objects (subscriber
     * role) and sent objects (publisher role) max-merge into it as a backstop;
     * the facade additionally merges through its own reference. */
    moq_track_hist_t *hist;
    /* Raw inbound SUBSCRIBE filter, captured at PENDING_PUBLISHER establishment
     * so moq_session_accept_subscribe can resolve the window against the accept
     * snapshot (the SUBSCRIBE_REQUEST event surfaces these; the entry did not
     * retain them before). Meaningful only for the publisher role. */
    uint64_t req_start_group;
    uint64_t req_start_object;
    uint64_t req_end_group;
    uint64_t req_end_object;
    /* Resolved subscription window. Publisher role: stored against the
     * accept / REQUEST_UPDATE snapshot, reached via the package-internal
     * accessor. Subscriber role: resolved against the Largest Object the
     * SUBSCRIBE_OK (or an acknowledged filter update) carried -- the same
     * snapshot the publisher resolved -- and used to attribute objects
     * arriving on an alias several subscriptions share. */
    moq_resolved_window_t window;
    /* Publisher role: the fill fetch streams the session itself opened for
     * this subscription -- through moq_session_open_fill and for a fill it
     * could not admit (header then RESET). Added to the application's own
     * data-stream count in PUBLISH_DONE, saturating to the profile's unknown
     * sentinel. */
    uint64_t           fill_streams_opened;
    /* Publisher role: the subgroup streams opened for this subscription,
     * saturating; with fill_streams_opened, the Stream Count of a
     * PUBLISH_DONE the session sends itself. */
    uint64_t           subgroup_streams_opened;
    /* MOQ_*_DONE_HELD: the Stream Count of the held PUBLISH_DONE. */
    uint64_t           held_done_count;
    /* Publisher role, pending only: the SUBSCRIBE carried a fill request the
     * pool could not admit. The effective request (inherited values filled
     * in) is kept so the accept can resolve its range against the accept
     * snapshot and, when a stream would have opened, fail it on the wire with
     * the priority it would have had. */
    bool               fill_unserved;
    moq_fill_input_t   fill_unserved_input;
    /* Facade-owned parents answer updates only after reserving fill credit.
     * Does not change the session-wide policy for unrelated raw parents. */
    bool               facade_defer_updates;
    /* Subscriber-role Stream-Count gating (draft-16 §9.15 / draft-18 §10.11),
     * mirroring the publication machinery: the terminal done arrives on the
     * control channel / request bidi and is likely to precede late-arriving
     * data streams. The subscription stays ESTABLISHED and alias-bindable
     * until processed_stream_count (FIN'd or identifiably-RESET data streams)
     * reaches done_stream_count; only then is SUBSCRIBE_DONE surfaced. When
     * the count is not yet satisfied the terminal is deferred: done_pending
     * records it (status/count + an OWNED copy of the reason, freed exactly
     * once in sub_free_entry). A Stream Count of 0 finalizes immediately;
     * the 2^62-1 "unknown" sentinel DEFERS and, like any unsatisfied gate,
     * is bounded by done_deadline_us (§9.8: expiry discards the remaining
     * streams and surfaces the retained terminal). */
    uint64_t           processed_stream_count;
    bool               done_pending;
    uint64_t           done_status_code;
    uint64_t           done_stream_count;
    uint8_t           *done_reason_buf;   /* owned copy; freed in sub_free_entry */
    size_t             done_reason_len;
    /* §9.8: terminal-wait deadline, stamped ONCE at defer commit via
     * deadline_add (never restamped); done_expired latches when a reap
     * observes the deadline passed -- the EXPIRED loop (scan/stop/rescan/
     * finalize) then owns finalization exclusively. */
    uint64_t           done_deadline_us;
    bool               done_expired;
    /* §9.8: exact per-type delivery timeouts, raw wire ms. dt_pub_* =
     * publisher-side (Track Properties/extensions, local or inbound);
     * dt_sub_* = subscriber-side (message parameters / acked updates).
     * has_ preserves d18 omission-vs-explicit-zero (§8: 0 = no timeout). */
    bool               dt_pub_has_object;
    uint64_t           dt_pub_object_ms;
    bool               dt_pub_has_subgroup;
    uint64_t           dt_pub_subgroup_ms;
    bool               dt_sub_has_object;
    uint64_t           dt_sub_object_ms;
    bool               dt_sub_has_subgroup;
    uint64_t           dt_sub_subgroup_ms;
    /* Pending update-carried timeout (latched only on REQUEST_OK; cleared
     * on rejection, crossed-terminal retirement, and entry reuse). */
    bool               dt_upd_has_object;
    uint64_t           dt_upd_object_ms;
    bool               dt_upd_has_subgroup;
    uint64_t           dt_upd_subgroup_ms;
    /*
     * Intrusive OCCUPANCY list, ascending slot order. A slot is linked iff it
     * is allocated (state != FREE / active). Every sweep that used to scan the
     * whole pool walks this list instead, so its cost follows the live owner
     * set rather than the configured capacity -- and because the list is kept
     * in slot order, owners are still served in exactly the order the full
     * scan served them, so no externally visible ordering changes.
     */
    int32_t occ_next;    /* next linked slot, -1 = end */
    int32_t occ_prev;    /* previous linked slot, -1 = head */
    bool    occ_linked;  /* membership, so link/unlink are idempotent-safe */
    /* Response-local Track Property; retained in the existing tail padding. */
    uint8_t default_publisher_priority;
    /* Subscriber role: the Track this subscription carries was found
     * malformed (draft-22 §12.1); its cancellation is owed and driven until
     * it commits (see session_fail_malformed_track). */
    bool    mt_failing;
} moq_sub_entry_t;

typedef enum moq_ann_state {
    MOQ_ANN_FREE = 0,
    MOQ_ANN_PENDING_ANNOUNCER  = 1,  /* we sent PUBLISH_NAMESPACE, awaiting response */
    MOQ_ANN_PENDING_RECEIVER   = 2,  /* peer sent PUBLISH_NAMESPACE, awaiting app accept/reject */
    MOQ_ANN_ESTABLISHED        = 3,
    MOQ_ANN_DRAINING           = 4,  /* stale app identity, private receive/output owner */
} moq_ann_state_t;

typedef enum moq_ann_role {
    MOQ_ANN_ROLE_ANNOUNCER = 1,
    MOQ_ANN_ROLE_RECEIVER  = 2,
} moq_ann_role_t;

/* Shared framing state for a GOAWAY-received late-input owner. */
#define MOQ_DRAIN_SCAN_HDR_MAX 11u
typedef struct moq_drain_scan {
    uint64_t skip;
    uint8_t hdr_len;
    uint8_t hdr[MOQ_DRAIN_SCAN_HDR_MAX];
} moq_drain_scan_t;

#ifndef MOQ_DECODED_MAX_TOKENS
#define MOQ_DECODED_MAX_TOKENS 16
#endif

/* A draft-22 namespace update (§9.5) on one of the three namespace request
 * owners (announcement, namespace subscription, track subscription). The
 * owed half is the responder's: a peer update the application has not yet
 * answered. The outstanding half is the requester's: this side's update
 * awaiting the peer's answer. Each half owns its prefix and token bytes;
 * ns_update_release frees both with the owner. At most one of each exists
 * per request (the advertised MAX_REQUEST_UPDATES is one, and this side
 * keeps at most one update of its own in flight). */
typedef struct moq_ns_update {
    moq_cluster_metadata_t owed_cluster;
    moq_cluster_metadata_t out_cluster;
    bool                 owed;
    uint32_t             owed_flags;
    uint64_t             owed_request_id;
    bool                 owed_forward;
    uint8_t             *owed_prefix_buf;
    size_t               owed_prefix_buf_len;
    moq_bytes_t         *owed_prefix_parts;
    size_t               owed_prefix_count;
    uint8_t             *owed_token_bytes;
    size_t               owed_token_bytes_len;
    moq_resolved_token_t owed_tokens[MOQ_DECODED_MAX_TOKENS];
    size_t               owed_token_count;
    bool                 outstanding;
    /* The request ended by migration -- a GOAWAY the peer sent on it, or one
     * this side sent followed by the peer's FIN -- while the answer above was
     * still owed. A GOAWAY does not change the request's state (draft-22
     * §9.2) and a FIN is directional (§6.4.2.2): the answer is still given,
     * and only then is the request retired (retire_close_half: this side's
     * direction is finished after the answer; retire_goaway_received: the
     * peer's GOAWAY arrived, so its later bytes are drained). */
    bool                 retire_after_answer;
    bool                 retire_close_half;
    bool                 retire_goaway_received;
    /* The peer refused this side's update; the request is ending and nothing
     * but its close may follow on the response stream. */
    bool                 failed;
    uint32_t             out_flags;
    uint64_t             out_request_id;
    bool                 out_forward;
    uint8_t             *out_prefix_buf;
    size_t               out_prefix_buf_len;
    moq_bytes_t         *out_prefix_parts;
    size_t               out_prefix_count;
} moq_ns_update_t;

typedef struct moq_ann_entry {
    moq_cluster_metadata_t cluster;
    moq_ann_state_t    state;
    moq_ann_role_t     role;
    uint32_t           generation;
    moq_announcement_t handle;
    uint64_t           request_id;
    /* rs.ref: request bidi stream identity for stream-correlated
     * request profiles (the PUBLISH_NAMESPACE request stream). Empty (zero)
     * under draft-16. */
    moq_request_stream_t rs;
    uint8_t           *ns_id_buf;   /* canonical namespace blob for dup detection */
    size_t             ns_id_len;
    /* rs.recv_*: receive buffer for announce request-bidi control bytes
     * (stream-correlated profiles): the PUBLISH_NAMESPACE response on the
     * announcer side, REQUEST_UPDATE on the receiver side. Co-allocated in the
     * session block; the pointer/cap persist across entry reuse (only len/fin
     * reset on free). Unused under draft-16 (announce stays on the control
     * channel). */
    /* Transitional FIN ownership, distinct from rs.recv_fin: the creating
     * PUBLISH_NAMESPACE carried the peer's FIN, and the commit that re-keyed
     * the bidi onto this entry took that FIN over with it. The announcement
     * handler consumes it into rs.recv_fin and retains the owner -- the
     * application still has to accept or reject -- so it never survives the
     * call that installed it. Cleared on every free/reuse. */
    bool               handoff_fin_pending;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* D22 retains validated bytes in rs.recv_buf and scalar cleanup intent in
     * this existing slot. App retirement never implies peer receive closure. */
    bool response_validated;
    bool response_fin;
    bool deferred_reset;
    bool deferred_stop;
    bool deferred_fin;
    bool terminal_notified;
    bool finish_fin;
    bool cancel_pending;
    bool migration_committed;
    moq_namespace_terminal_cause_t deferred_cause;
    moq_namespace_terminal_cause_t terminal_cause;
    uint64_t deferred_code;
    uint64_t terminal_code;
    moq_drain_scan_t migration_scan;
    moq_ns_update_t update;
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

/*
 * Advancing-call state machine.
 *
 * Stage order is fixed, so a suspended call resumes exactly where it stopped
 * and in the order it would have run uninterrupted:
 *
 *     REAP_SUBGROUPS -> PUB -> SUB -> DONE
 *
 * IDLE is the cleared state, not a stage: nothing runs in it and it is never
 * entered mid-sweep.
 *
 * The advance preamble is NOT part of this machine. Borrow-epoch invalidation
 * and send/scratch reclamation belong to the physical call
 * (session_call_prepare), because a continuation is a real advancing call and
 * owes both; logical time is stamped when a FRESH sweep is armed, so an
 * inherited sweep retires against the epoch it was suspended at rather than the
 * clock of the call that happened to finish it.
 *
 * REAP_SUBGROUPS charges one unit per ELIGIBLE reap (empty slots are free) and
 * recomputes the subgroup deadline only after the complete scan -- the same
 * complete-scan discipline expiry_retry_deadline_us follows once both owner
 * pools retire at DONE.
 */
typedef enum moq_sweep_stage {
    MOQ_SWEEP_STAGE_IDLE          = 0,  /* no sweep in flight */
    MOQ_SWEEP_STAGE_REAP_SUBGROUPS = 1,
    MOQ_SWEEP_STAGE_PUB            = 2,
    MOQ_SWEEP_STAGE_SUB            = 3,
    MOQ_SWEEP_STAGE_DONE           = 4,
} moq_sweep_stage_t;

/*
 * Per-owner phase within a pool.
 *
 * Legal transitions:  SELECT -> STOP_STREAMS -> FINALIZE -> SELECT (next owner).
 * An owner that needs no stream stop goes SELECT -> FINALIZE directly. Any
 * phase may abort to SELECT of the next owner when the owner is skipped.
 */
typedef enum moq_sweep_phase {
    MOQ_SWEEP_PHASE_SELECT       = 0,  /* choose/qualify the owner at sweep_slot */
    MOQ_SWEEP_PHASE_STOP_STREAMS = 1,  /* draining its bound streams (sweep_rx_pos) */
    MOQ_SWEEP_PHASE_FINALIZE     = 2,  /* emit completion and free the entry */
} moq_sweep_phase_t;

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
    uint8_t              subscriber_priority;
    /* Publisher role: the request asked for no Track Properties in its
     * answer (draft-22 INCLUDE_PROPERTIES = 0); FETCH_OK carries none. */
    bool                 omit_track_properties;
    moq_fetch_state_t  state;
    moq_fetch_role_t   role;
    moq_group_order_t    effective_group_order; /* immutable; DEFAULT -> ASCENDING */
    uint32_t           generation;
    moq_fetch_t        handle;
    uint64_t           request_id;
    /* rs.ref: the FETCH *request* bidi stream identity for
     * stream-correlated request profiles (the stream the FETCH request travels
     * on; FETCH_OK/REQUEST_ERROR correlate by it). Intentionally DISTINCT from
     * data_stream_ref below. Empty (zero) under draft-16. */
    moq_request_stream_t rs;
    /* data_stream_ref: the *response object data* stream identity (the uni
     * stream, led by FETCH_HEADER, that carries fetched objects). Used by all
     * profiles. Intentionally distinct from rs.ref. */
    moq_stream_ref_t   data_stream_ref;
    bool               data_stream_started;
    bool               data_stream_fin;
    /* Publisher role: bytes of the FETCH_HEADER as encoded on the response
     * data stream, the reliable size of a reset of that stream. */
    uint8_t            data_header_len;
    bool               control_response_seen;
    bool               control_ok;
    moq_fetch_prior_object_t prior;
    uint64_t           start_group;
    uint64_t           start_object;
    /* Publisher role: the request was surfaced with its range unresolved
     * (moq_session_cfg_t.defer_fetch_ranges); the accept's End is checked
     * against the request's form. start_group/start_object above hold the
     * request's explicit start, req_end_* its explicit end (exclusive end
     * object, zero for the whole group) unless req_end_at_largest. */
    bool               range_deferred;
    bool               req_start_relative;
    bool               req_start_next_object;
    bool               req_end_at_largest;
    uint64_t           req_relative_offset;
    uint64_t           req_end_group;
    uint64_t           req_end_object;
    /* rs.recv_*: receive buffer for request-bidi RESPONSE bytes (FETCH_OK /
     * REQUEST_ERROR) on stream-correlated request profiles. Co-allocated in the
     * session block; NULL/zero under profiles that respond on the shared control
     * channel. Intentionally NOT used for FETCH data-uni bytes -- those travel
     * on data_stream_ref through session_receive.c. */
    /* Transitional FIN ownership, distinct from rs.recv_fin: the creating
     * FETCH carried the peer's FIN, and the commit that re-keyed the bidi onto
     * this entry took that FIN over with it. Both publisher-role states consume
     * it synchronously into rs.recv_fin and retain the owner -- the response
     * and any data stream are still to come -- so it never survives the call
     * that installed it. Cleared on every free/reuse. */
    bool               handoff_fin_pending;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* Publisher role: REQUEST_UPDATEs that arrived before the fetch was
     * answered. FETCH_OK is the first message on the request stream, so their
     * answers follow it when the fetch is accepted: upd_ok_owed REQUEST_OKs,
     * then, if one was refused, a single REQUEST_ERROR (upd_fail_code) for it
     * and every later one, which also ends the fetch. upd_owed counts them
     * all against the advertised MAX_REQUEST_UPDATES. A rejected fetch drops
     * them with the request. */
    uint32_t upd_owed;
    uint32_t upd_ok_owed;
    bool     upd_failed;
    uint64_t upd_fail_code;
    /* Fetcher role: this side's REQUEST_UPDATE awaits its answer. */
    bool     own_update_pending;
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
    /*
     * Intrusive OCCUPANCY list, ascending slot order. A slot is linked iff it
     * is allocated (state != FREE / active). Every sweep that used to scan the
     * whole pool walks this list instead, so its cost follows the live owner
     * set rather than the configured capacity -- and because the list is kept
     * in slot order, owners are still served in exactly the order the full
     * scan served them, so no externally visible ordering changes.
     */
    int32_t occ_next;    /* next linked slot, -1 = end */
    int32_t occ_prev;    /* previous linked slot, -1 = head */
    bool    occ_linked;  /* membership, so link/unlink are idempotent-safe */
    /* Fetcher role: the canonical full-track-name key of the Track fetched
     * (owned; see moq_build_track_key), which ties this FETCH to the other
     * requests for that Track when one of them finds it malformed. NULL when
     * it could not be built. */
    uint8_t *track_key;
    size_t   track_key_len;
    /* Fetcher role: the Track was found malformed; the cancellation is owed
     * (see moq_sub_entry_t.mt_failing). */
    bool     mt_failing;
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
    /* Publisher role, ended by a failed update: as MOQ_SUB_DONE_HELD. */
    MOQ_PUB_DONE_HELD          = 5,
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
    /* rs.ref: request bidi stream identity for stream-correlated
     * request profiles (the PUBLISH request stream). Empty (zero) under
     * draft-16. */
    moq_request_stream_t rs;
    uint64_t           track_alias;
    bool               send_allowed;
    /* Publisher role: fill fetch streams the session opened for this
     * publication (see moq_sub_entry_t.fill_streams_opened). */
    uint64_t           fill_streams_opened;
    /* Publisher role: the subgroup streams opened for this publication (see
     * moq_sub_entry_t.subgroup_streams_opened). */
    uint64_t           subgroup_streams_opened;
    /* MOQ_*_DONE_HELD: the Stream Count of the held PUBLISH_DONE. */
    uint64_t           held_done_count;
    /* Publisher role: the Forward state this side put on its PUBLISH, the
     * subscription's initial setting where the response carries none. */
    bool               publish_forward;
    uint8_t            subscriber_priority;
    uint8_t            group_order;
    /* Immutable peer D22 PUBLISH values, separate from acknowledged updates. */
    moq_publish_initial_parameters_t d22_initial;
    /* Publisher role: the DEFAULT_PUBLISHER_GROUP_ORDER this side put in its
     * PUBLISH Track Properties (DEFAULT when absent). */
    uint8_t            track_group_order;
    uint8_t            default_publisher_priority;
    bool               has_delivery_timeout;
    uint64_t           delivery_timeout_ms;
    bool               has_expires;
    uint64_t           expires_ms;
    bool               update_pending;
    uint64_t           update_request_id;
    /* A pending update carries a Forward change: latched into send_allowed
     * only when the update is ACKNOWLEDGED (object delivery gates on the
     * current acknowledged Forward state); dropped on error/retirement. */
    bool               update_has_forward;
    bool               update_forward;
    /* Typed UPDATE selections latch on ACK, so later fills inherit the
     * acknowledged values rather than the initial PUBLISH parameters. */
    bool               update_has_subscriber_priority;
    uint8_t            update_subscriber_priority;
    bool               update_has_filter;
    uint32_t           update_filter_type;
    uint64_t           update_start_group;
    uint64_t           update_start_object;
    uint64_t           update_end_group;
    uint64_t           update_end_object;
    /* A REQUEST_UPDATE was rejected. Subscriber: awaiting PUBLISH_DONE
     * (UPDATE_FAILED); further updates are blocked. Publisher on a
     * stream-correlated profile: its own update was refused and it owes
     * PUBLISH_DONE(UPDATE_FAILED). Draft-16 publisher: the automatic
     * rejection teardown has begun; no new output may overtake it. */
    bool               update_failed;
    /* Publisher role: an update answer the application owes (see
     * moq_sub_entry_t.owed_update). */
    moq_owed_update_t  owed_update;
    moq_sub_update_snapshot_t update_snapshot;
    /* Co-allocated request-bidi receive buffer (stream-correlated profiles): the
     * PUBLISH bidi carries the peer's response/lifecycle messages here. On the
     * publisher (opener) side: PUBLISH_OK / REQUEST_ERROR / REQUEST_UPDATE; on
     * the subscriber (responder) side: REQUEST_OK (update ack) / PUBLISH_DONE.
     * rs.recv_fin records the peer's FIN. Reset, not freed, on entry reuse; the
     * pointer/cap are preserved. Unused under draft-16. */
    /* Transitional FIN ownership, distinct from rs.recv_fin: the creating
     * PUBLISH carried the peer's FIN, and the commit that re-keyed the bidi
     * onto this entry took that FIN over with it. It is consumed by this
     * family's own FIN handling; a teardown that blocks on event capacity
     * keeps it, so an empty re-feed resumes and completes exactly once.
     * Cleared on every free/reuse. */
    bool               handoff_fin_pending;
    /* -- Joining Location (draft-18 5.1, draft-16 5.1) ----------------
     * "A publisher MUST save the Largest Location communicated in
     * SUBSCRIBE_OK, PUBLISH or REQUEST_UPDATE_OK that changes the Forward
     * State from 0 to 1. This value is called the Joining Location and can be
     * used in a Joining FETCH ... while the subscription is in the Established
     * state."
     *
     * LATCHED, never a live read of track history: the value is frozen at the
     * transition that made this publication joinable, so objects published
     * afterwards cannot move the range a joined FETCH resolves to.
     *
     * publish_largest_* holds what the PUBLISH itself carried (advertised by
     * the local publisher, or received from the peer) until the establishing
     * PUBLISH_OK decides whether it becomes the Joining Location.
     * joining_* is the latched result; has_joining_loc gates every read.
     * Absence of a required Largest leaves no Joining Location at all -- there
     * is no fallback to stale or current state. */
    bool               publish_has_largest;
    uint64_t           publish_largest_group;
    uint64_t           publish_largest_object;
    bool               has_joining_loc;
    uint64_t           joining_group;
    uint64_t           joining_object;
    /* draft-16 9.16.2 gates a joining fetch on the associated subscription
     * having Filter Type Largest Object. A publication negotiates its filter
     * on PUBLISH_OK (draft-16 9.2.2.5) into the existing filter_type field
     * above; draft-18 replaced this gate with Forward State 1 and ignores it. */
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* The track supports dynamically started groups (DYNAMIC_GROUPS == 1 in
     * the PUBLISH track properties). Gates outbound new-group requests on
     * the accept and this publication's updates. */
    bool dynamic_groups;
    /* Subscriber-role PUBLISH_DONE Stream-Count gating (draft-16 §9.15 /
     * draft-18 §10.11): PUBLISH_DONE arrives on the control channel and is likely
     * to precede late-arriving / late-opening data streams. The subscriber keeps
     * the publication state live until it has processed the advertised number of
     * data streams, then removes it. processed_stream_count counts completed
     * (FIN'd or identifiably-RESET) data streams for this publication. When a finite Stream Count is
     * not yet satisfied, the PUBLISH_DONE is deferred: done_pending records it
     * (status/count + an owned copy of the reason) and PUBLISH_FINISHED is held
     * until processed_stream_count >= done_stream_count. A Stream Count of 0
     * finalizes immediately; the 2^62-1 "unknown" sentinel DEFERS and, like
     * any unsatisfied gate, is bounded by done_deadline_us (§9.8: expiry
     * discards the remaining streams and surfaces the retained terminal). */
    uint64_t           processed_stream_count;
    bool               done_pending;
    uint64_t           done_status_code;
    uint64_t           done_stream_count;
    uint8_t           *done_reason_buf;   /* owned copy; freed in pub_free_entry */
    size_t             done_reason_len;
    /* §9.8: terminal-wait deadline + expiry latch (see moq_sub_entry_t). */
    uint64_t           done_deadline_us;
    bool               done_expired;
    /* §9.8: exact per-type delivery timeouts (see moq_sub_entry_t). */
    bool               dt_pub_has_object;
    uint64_t           dt_pub_object_ms;
    bool               dt_pub_has_subgroup;
    uint64_t           dt_pub_subgroup_ms;
    bool               dt_sub_has_object;
    uint64_t           dt_sub_object_ms;
    bool               dt_sub_has_subgroup;
    uint64_t           dt_sub_subgroup_ms;
    bool               dt_upd_has_object;
    uint64_t           dt_upd_object_ms;
    bool               dt_upd_has_subgroup;
    uint64_t           dt_upd_subgroup_ms;
    /* Reserved registry record, keyed by the full track name at
     * establishment; released once in pub_free_entry (before its memset). */
    moq_track_hist_t *hist;
    /* Raw SUBSCRIPTION_FILTER negotiated on PUBLISH_OK:
     * publisher role stores what the subscriber sent; subscriber role stores
     * what it chose on accept. MOQ_SUBSCRIBE_FILTER_NONE = unfiltered. */
    uint32_t filter_type;
    uint64_t req_start_group;
    uint64_t req_start_object;
    uint64_t req_end_group;
    uint64_t req_end_object;
    /* Resolved window: on the publisher role, resolved from the PUBLISH_OK
     * filter (and re-resolved on REQUEST_UPDATE snapshots) against this
     * side's registry largest. */
    moq_resolved_window_t window;
    /*
     * Intrusive OCCUPANCY list, ascending slot order. A slot is linked iff it
     * is allocated (state != FREE / active). Every sweep that used to scan the
     * whole pool walks this list instead, so its cost follows the live owner
     * set rather than the configured capacity -- and because the list is kept
     * in slot order, owners are still served in exactly the order the full
     * scan served them, so no externally visible ordering changes.
     */
    int32_t occ_next;    /* next linked slot, -1 = end */
    int32_t occ_prev;    /* previous linked slot, -1 = head */
    bool    occ_linked;  /* membership, so link/unlink are idempotent-safe */
    bool facade_defer_updates;
    /* Subscriber role: the Track was found malformed; the cancellation is
     * owed (see moq_sub_entry_t.mt_failing). */
    bool    mt_failing;
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
    /* rs.ref: request bidi stream identity for stream-correlated
     * request profiles (the TRACK_STATUS request stream). Empty (zero) under
     * draft-16. */
    moq_request_stream_t rs;
    /* Co-allocated request-bidi receive buffer (stream-correlated profiles): the
     * requester buffers the fragmentable TRACK_STATUS_OK / REQUEST_ERROR response
     * here. rs.recv_fin also records the peer's FIN on the publisher side (the
     * requester sends TRACK_STATUS as its first and only message). Reset, not
     * freed, on entry reuse; the pointer/cap are preserved. Unused under draft-16. */
    /* Transitional FIN ownership, distinct from rs.recv_fin: the creating
     * TRACK_STATUS carried the peer's FIN, and the commit that re-keyed the
     * bidi onto this entry took that FIN over with it. The publisher-side FIN
     * handling consumes it into rs.recv_fin, retaining the owner.
     * Cleared on every free/reuse. */
    bool                        handoff_fin_pending;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    /* D22: the existing receive buffer retains a fully validated outcome until
     * its event commits. Later directional signals cannot replace that outcome. */
    bool response_validated;
    bool deferred_reset;
    bool deferred_stop;
    uint64_t deferred_reset_code;
    uint64_t deferred_stop_code;
    moq_track_status_terminal_cause_t terminal_cause;
    uint64_t terminal_code;
    bool terminal_notified;
    bool migration_committed;
    moq_drain_scan_t migration_scan;
    /* Owned canonical full-track-name key: TRACK_STATUS is the only
     * request family that did not retain the track name, but TRACK_STATUS_OK is
     * a registry merge source, so the response must be attributable to a track.
     * Built at establishment; freed once in ts_free_entry (before its memset). */
    uint8_t          *track_id_buf;
    size_t            track_id_len;
    /* Reserved registry record; released once in ts_free_entry. The
     * inbound TRACK_STATUS_OK max-merges its Largest Object into it; the
     * publisher side's answer reports at least its Largest Object. */
    moq_track_hist_t *hist;
    /* Publisher role: the request asked for no Track Properties in its
     * answer (INCLUDE_PROPERTIES = 0); TRACK_STATUS_OK carries none. */
    bool omit_track_properties;
} moq_ts_entry_t;

/* -- Per-track largest-location history record --------------------- */

/*
 * One registry slot: the largest Object Location published or received on a
 * track (§5.1.2 "Largest Object"), keyed by the canonical full-track-name
 * blob (same encoding as build_track_id). See moq_session.track_hist.
 *
 * Lifetime: `refs` counts the request entries (subscription / publish /
 * track-status) that reserved this record for a track name. While
 * `!has_largest` the record is an empty reservation, freed when `refs`
 * reaches 0. Once `has_largest` becomes true the record is pinned
 * (observed history must not be forgotten) and stays allocated until the
 * session is destroyed, regardless of `refs`.
 */
struct moq_track_hist {
    bool      in_use;        /* slot occupied */
    bool      has_largest;   /* a location has been observed (pins the record) */
    uint32_t  refs;          /* reserving request entries (empty-reservation GC) */
    uint8_t  *key;           /* owned canonical full-track-name blob */
    size_t    key_len;
    uint64_t  largest_group;
    uint64_t  largest_object;
    /* Subscriber-role subscriptions to this track awaiting their response:
     * while any is pending, an alias another subscription to the track holds
     * may turn out to be shared (draft-22 §3.1.3). */
    uint32_t  pending_subs;
};

/* -- Track-history registry internal API (session_track_hist.c) ----- */

/* Build the canonical full-track-name key blob (owned; caller frees via
 * s->alloc). Identical layout to build_track_id; shared so registry keys
 * match everywhere they are constructed. NULL on OOM (unless key_len 0). */
uint8_t *moq_build_track_key(moq_session_t *s,
                             const moq_namespace_t *ns,
                             moq_bytes_t name,
                             size_t *out_len);

/* The public, semantic form of a stored subscription filter: the core's
 * filter type and raw locations (including the internal relative-group and
 * object-end forms) onto a moq_location_filter_cfg_t, and back. The forward
 * mapping writes min(out_size, sizeof current) bytes of `out` and stamps that
 * count; the caller has checked out_size against the v0 size. The inverse
 * refuses an unknown kind or an end before the start. */
typedef struct moq_core_filter {
    uint32_t type;
    uint64_t start_group, start_object, end_group, end_object;
} moq_core_filter_t;

void moq_location_filter_from_core(const moq_core_filter_t *filter,
                                   moq_location_filter_cfg_t *out,
                                   size_t out_size);
moq_result_t moq_location_filter_to_core(const moq_location_filter_cfg_t *cfg,
                                         uint32_t *filter_type, uint64_t *start_group,
                                         uint64_t *start_object, uint64_t *end_group,
                                         uint64_t *end_object);

/* Normalize a local subscription filter before encoding or retaining it.
 * Both typed relative0 and the legacy NEXT_GROUP spelling must use the
 * negotiated profile's resolution rule; NEXT_GROUP ignores caller locations.
 * Fill ranges have their own clamped resolver and do not use this helper. */
void moq_local_filter_normalize(const moq_session_t *s, uint32_t *filter_type,
                                uint64_t *start_group);

/* Find the record for a canonical key, or NULL. */
moq_track_hist_t *track_hist_find(moq_session_t *s,
                                  const uint8_t *key, size_t key_len);

/*
 * Allocation-free canonical-identity comparison. Compares a decoded
 * (namespace parts, track name) identity DIRECTLY against a stored
 * canonical key blob in its existing layout
 *   [count u8][u16-be part len, bytes]... [u16-be name len, name bytes]
 * with checked arithmetic and full bounds validation, so a truncated or
 * malformed stored key can never match and can never read past its end.
 * No allocation, no normalization, no second key encoding. Returns true
 * only for a byte-exact identity match.
 */
bool moq_track_key_matches(const uint8_t *key, size_t key_len,
                           const moq_namespace_t *ns, moq_bytes_t name);

/* Find the record for a decoded identity without building a key. */
moq_track_hist_t *track_hist_find_id(moq_session_t *s,
                                     const moq_namespace_t *ns,
                                     moq_bytes_t name);

/*
 * Allocation-free selection of what a reservation for this identity WILL
 * do, evaluated before any funded execution. Distinguishes the three
 * outcomes track_hist_reserve() collapses into a single NULL:
 *   EXISTING  a record for this identity is present (take its ref)
 *   FREE_SLOT the registry has room (allocate + reserve when funded)
 *   FULL      no pool, or no free slot -- the funded REQUEST_ERROR path
 * An allocation failure during execution is NOT a selection outcome: it
 * is reported by track_hist_reserve_selected() as MOQ_ERR_NOMEM.
 */
typedef enum moq_track_hist_sel {
    MOQ_TH_SEL_EXISTING = 0,
    MOQ_TH_SEL_FREE_SLOT,
    MOQ_TH_SEL_FULL,
} moq_track_hist_sel_t;

moq_track_hist_sel_t moq_track_hist_select(moq_session_t *s,
                                           const moq_namespace_t *ns,
                                           moq_bytes_t name);

/*
 * Funded execution of a selection. Returns MOQ_OK with *out set (an
 * existing record's ref taken, or a new record created and reserved),
 * or MOQ_ERR_NOMEM with *out NULL and NOTHING mutated when the record's
 * key copy cannot be allocated. Must not be called for MOQ_TH_SEL_FULL
 * -- that outcome has its own funded REQUEST_ERROR path. Registry-full
 * and allocation failure are therefore never conflated.
 */
moq_result_t track_hist_reserve_selected(moq_session_t *s,
                                         const moq_namespace_t *ns,
                                         moq_bytes_t name,
                                         moq_track_hist_t **out);

/*
 * Reserve (refcount++) the record for a track, creating an empty one if
 * absent. Returns the record, or NULL if the registry is full / OOM (no
 * record created). The caller owns one reference until track_hist_release.
 * `key`/`key_len` is the canonical blob (borrowed; copied on create).
 */
moq_track_hist_t *track_hist_reserve(moq_session_t *s,
                                     const uint8_t *key, size_t key_len);

/* Drop one reference; frees the slot if it becomes unreferenced AND has no
 * observed largest. A NULL record is a no-op (rollback convenience). */
void track_hist_release(moq_session_t *s, moq_track_hist_t *rec);

/* Monotonic max-merge a location into an already-reserved record. Never
 * allocates, never fails; sets has_largest. */
void track_hist_merge(moq_track_hist_t *rec,
                      uint64_t group_id, uint64_t object_id);

/* Free all registry records and the pool (session teardown). */
void track_hist_destroy(moq_session_t *s);

/*
 * Location successor with a PROFILE-SPECIFIC varint ceiling (design §4).
 * Computes the Location immediately after {group, object}: normally
 * {group, object+1}, but when object == `ceiling` it carries to {group+1, 0}.
 * `ceiling` is the profile's max Location value (draft-16 MOQ_QUIC_VARINT_MAX,
 * draft-18 MOQ_VI64_MAX == UINT64_MAX) -- pass s->profile->location_varint_max.
 * Returns true and leaves the out-params unspecified when NO successor exists
 * (both group and object already at the ceiling) -- an unsatisfiable-empty
 * window. Never saturates to empty prematurely: a later group can still exist
 * after an at-ceiling object.
 *
 *   LargestObject (0x2) start = successor(LG, LO)
 *   NextGroup     (0x1) start = successor(LG, ceiling) = {LG+1, 0}
 */
bool moq_loc_successor(uint64_t group, uint64_t object, uint64_t ceiling,
                       uint64_t *out_group, uint64_t *out_object);

/*
 * Resolve a subscription filter against a single largest-object snapshot into a
 * stored window. `filter` is the
 * semantic filter type; `raw_*` the absolute start/end from the request (used
 * only by ABSOLUTE_START/RANGE); `has_snap`/`snap_*` the resolution snapshot
 * (max(registry, seed)); `ceiling` the profile's max Location value (see
 * moq_loc_successor). Fills *out (has_window is set true). Relative filters with
 * no snapshot resolve to an open window from the origin (never unsatisfiable);
 * legacy successors at the ceiling set `unsatisfiable`; the internal
 * relative-group form instead clamps to the location ceiling.
 */
void moq_resolve_filter_window(moq_subscribe_filter_t filter,
                               uint64_t raw_start_group,
                               uint64_t raw_start_object,
                               uint64_t raw_end_group,
                               uint64_t raw_end_object,
                               bool has_snap,
                               uint64_t snap_group, uint64_t snap_object,
                               uint64_t ceiling,
                               moq_resolved_window_t *out);

/* Write a stored window as the public moq_filter_window_t (size-aware: the
 * fields through end_object are required, struct_size is stamped). */
moq_result_t moq_filter_window_out(const moq_resolved_window_t *w,
                                   moq_filter_window_t *out, size_t out_size);

/*
 * Package-internal accessors for the resolved window stored on a publisher-role
 * entry. Return NULL if
 * the handle is stale, not publisher-role, or no window has been resolved.
 * Used by the facade  and the white-box tests.
 */
const moq_resolved_window_t *moq_session_sub_resolved_window(
    moq_session_t *s, moq_subscription_t sub);
const moq_resolved_window_t *moq_session_pub_resolved_window(
    moq_session_t *s, moq_publication_t pub);

typedef enum moq_ns_sub_state {
    MOQ_NS_SUB_FREE               = 0,
    MOQ_NS_SUB_PENDING_SUBSCRIBER  = 1,
    MOQ_NS_SUB_RECVING_PUBLISHER   = 2,
    MOQ_NS_SUB_PENDING_PUBLISHER   = 3,
    MOQ_NS_SUB_ESTABLISHED         = 4,
    MOQ_NS_SUB_CLOSING             = 5,
    MOQ_NS_SUB_DRAINING            = 6, /* app terminal committed; physical work retained */
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

/* How a request ended when a stream signal, or a FIN that failed it, ended
 * it rather than a message. cause zero means a message did. */
typedef struct moq_request_terminal {
    moq_request_terminal_cause_t cause;
    uint64_t                     error_code;
} moq_request_terminal_t;

static inline moq_request_terminal_t request_terminal_by_message(void)
{
    moq_request_terminal_t t = { MOQ_REQUEST_TERMINAL_NONE, 0 };
    return t;
}

static inline moq_request_terminal_t request_terminal_signal(bool reset,
                                                             uint64_t error_code)
{
    moq_request_terminal_t t;
    t.cause = reset ? MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM
                    : MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING;
    t.error_code = error_code;
    return t;
}

static inline moq_request_terminal_t request_terminal_fin(bool truncated)
{
    moq_request_terminal_t t;
    t.cause = truncated ? MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE
                        : MOQ_REQUEST_TERMINAL_MISSING_RESPONSE;
    t.error_code = 0;
    return t;
}

/* Private dispatch to the distinct typed scalar events. The caller has checked
 * event capacity; no generic event or family-tagged detail is public. */
void push_request_terminated(moq_session_t *s, moq_request_family_t family,
                             moq_request_role_t local_role,
                             moq_request_terminal_t term, uint64_t raw_handle);

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
    /* Ended by a teardown whose action queue had no room left for the
     * stream's reset: the reset is owed (owed_reset_code) and issued by
     * session_retry_owed_stream_actions once capacity returns; the entry takes no more
     * objects and is not reaped until the reset is queued. */
    MOQ_SG_RESET_OWED = 5,
} moq_sg_state_t;

typedef struct moq_sg_entry {
    uint8_t               publisher_priority;
    /* Bytes of the SUBGROUP_HEADER as encoded on this stream: the reliable
     * size of every reset the session sends on it. */
    uint8_t               header_len;
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
    /* The first object's OBJECT_DELIVERY_TIMEOUT / SUBGROUP_DELIVERY_TIMEOUT
     * properties: on profiles that honour them they replace the track-level
     * publisher values for this subgroup. */
    bool                   ovr_has_object;
    uint64_t               ovr_object_ms;
    bool                   ovr_has_subgroup;
    uint64_t               ovr_subgroup_ms;
    /* OBJECT_DELIVERY_TIMEOUT expiry of the streamed object in progress, or
     * UINT64_MAX: none running, or (clock at the first payload byte) not yet
     * started. */
    uint64_t               streaming_deadline_us;
    /* A queued object on this stream has exceeded its OBJECT_DELIVERY_TIMEOUT;
     * set and consumed within one expiry pass. */
    bool                   timeout_expired;
    /* MOQ_SG_RESET_OWED: the error code of the reset still owed. */
    uint64_t               owed_reset_code;
    /*
     * Intrusive OCCUPANCY list, ascending slot order. A slot is linked iff it
     * is allocated (state != FREE / active). Every sweep that used to scan the
     * whole pool walks this list instead, so its cost follows the live owner
     * set rather than the configured capacity -- and because the list is kept
     * in slot order, owners are still served in exactly the order the full
     * scan served them, so no externally visible ordering changes.
     */
    int32_t occ_next;    /* next linked slot, -1 = end */
    int32_t occ_prev;    /* previous linked slot, -1 = head */
    bool    occ_linked;  /* membership, so link/unlink are idempotent-safe */
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

/* -- No-slot admission carrier ------------------------------------- *
 * The minimal durable record of a no-slot request-bidi terminal owed but not
 * yet emittable for local resource pressure. The terminal is fixed and
 * content-independent, so only the stream identity and the observed FIN fact
 * are retained -- never the peer's request bytes. */
typedef struct moq_noslot_carrier {
    uint64_t stream_ref;   /* 0 == free slot */
    bool     fin;          /* the peer's send half already FIN'd */
} moq_noslot_carrier_t;

/* -- Namespace sub entry (publisher side, one per bidi stream) ------- */

typedef struct moq_ns_sub_entry {
    moq_ns_sub_state_t  state;
    moq_stream_kind_t   stream_kind;
    uint32_t            generation;
    moq_ns_sub_handle_t handle;
    uint64_t            request_id;
    moq_stream_ref_t    stream_ref;
    bool                local_subscriber;   /* this side issued the request */
    bool                terminal_notified;  /* REQUEST_TERMINATED already queued */
    uint8_t             retained_output;   /* moq_retained_output_t */
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
    /* Transitional FIN ownership, distinct from pending_fin: the creating
     * SUBSCRIBE_TRACKS-style handoff -- here the draft-18 SUBSCRIBE_NAMESPACE
     * request-stream commit -- carried the peer's FIN, and the commit that
     * moved the bidi into this entry took that FIN over with it. The reciprocal
     * close it drives reserves an action slot, so the fact stays here across a
     * refusal and an empty re-feed completes it. This pool's free is SELECTIVE,
     * so it is cleared there by name. */
    bool                handoff_fin_pending;
    /* A publisher-side extra-byte teardown was attempted and refused for
     * capacity. Draft-18 §10.9.1 makes such a teardown close the request bidi,
     * not buffer the bytes, so the obligation -- not the raw bytes -- is what
     * must survive: this durable marker re-drives ns_sub_local_teardown on the
     * documented empty re-feed, with no peer-byte redelivery. Cleared in the
     * selective free. */
    bool                local_teardown_pending;
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
    bool suffix_cap_terminating;  /* the per-subscription active-suffix cap was
                                   * exceeded: the offending bidi is being retired
                                   * by synthesizing NAMESPACE_GONE for the active
                                   * suffixes (the tree is the durable cursor). Set
                                   * before the first GONE so an event/action-
                                   * capacity block resumes the terminal on the
                                   * next empty re-feed instead of re-decoding the
                                   * over-cap NAMESPACE. Cleared on slot free. */
    /* D22 RESET/STOP: first application cause is independent of crossed
     * transport facts. Lifetime is initialized from this owner at cancellation;
     * the entry retains both event and direction obligations without allocation. */
    moq_request_terminal_t terminal;
    moq_request_lifetime_t cancel_lifetime;
    moq_ns_update_t update;
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
    moq_request_stream_t rs;
    /* Stored namespace prefix (publisher side) for the independent overlap scan. */
    uint8_t                *prefix_buf;
    size_t                  prefix_buf_len;
    moq_bytes_t            *prefix_parts;
    size_t                  prefix_count;
    bool                    forward;
    moq_subscribe_tracks_options_t options;
    /* Auth staging committed after the request event is pushed (publisher side). */
    bool                    auth_committed;
    uint64_t                auth_reject_code;
    moq_resolved_token_t    resolved_tokens[MOQ_DECODED_MAX_TOKENS];
    bool                    token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t                  token_count;
    /* Before SETUP completes this slot is inactive. Its auth transaction
     * storage lends a typed token array to SETUP without growing the pool. */
    union {
        moq_auth_txn_t          auth_txn;
        moq_decoded_auth_token_t setup_tokens[MOQ_DECODED_MAX_TOKENS];
    };
    /* Co-allocated request-bidi receive buffer (stream-correlated): the requester
     * buffers the fragmentable REQUEST_OK / REQUEST_ERROR / PUBLISH_BLOCKED stream
     * here. rs.recv_fin records the peer's FIN. Reset, not freed, on entry reuse;
     * the pointer/cap are preserved. */
    /* Transitional FIN ownership, distinct from rs.recv_fin: the creating
     * SUBSCRIBE_TRACKS carried the peer's FIN, and the commit that re-keyed the
     * bidi onto this entry took that FIN over with it. Unlike the track-status,
     * fetch and announcement families, THIS consumer can block -- the
     * cancellation reserves event capacity -- so the fact stays here across the
     * refusal and an empty re-feed completes it. Cleared by the whole-record
     * reset on free/reuse. */
    bool                    handoff_fin_pending;
    bool goaway_sent;   /* a per-request GOAWAY was emitted on this request
                         * bidi; the entry stays live until the peer tears the
                         * old stream down (FIN/RESET/STOP) or the timeout fires. */
    moq_ns_update_t update;
} moq_track_sub_entry_t;

_Static_assert(sizeof(moq_decoded_auth_token_t) * MOQ_DECODED_MAX_TOKENS <=
                   sizeof(moq_auth_txn_t), "SETUP tokens fit track auth storage");
_Static_assert(_Alignof(moq_decoded_auth_token_t) <= _Alignof(moq_auth_txn_t),
               "SETUP tokens do not increase track entry alignment");

typedef struct moq_setup_workspace {
    moq_ns_sub_entry_t    *auth;
    moq_track_sub_entry_t *decoded;
    size_t                count;
} moq_setup_workspace_t;

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
    bool     default_priority;
    bool     has_extensions;
    bool     end_of_group;
    bool     subgroup_id_resolved;
    bool     subgroup_id_from_first_object;
    bool     first_object;   /* FIRST_OBJECT bit (false on drafts without it) */
} moq_decoded_subgroup_header_t;

/* Why a received object makes its Track malformed (draft-22 §12.1, §10.7,
 * §10.8, §10.9, §3.7); MOQ_MALFORMED_NONE when it does not. The property
 * violations share their values with MOQ_OBJ_PROPS_* in the wire layer. */
#define MOQ_MALFORMED_NONE                0u
#define MOQ_MALFORMED_PROPS_UNPARSEABLE   1u
#define MOQ_MALFORMED_NESTED_IMMUTABLE    2u
#define MOQ_MALFORMED_DUPLICATE_IMMUTABLE 3u
#define MOQ_MALFORMED_MANDATORY_PROPERTY  4u
#define MOQ_MALFORMED_DUPLICATE_GAP       5u
#define MOQ_MALFORMED_PRIORITY            16u  /* §12.1 condition 1 */
#define MOQ_MALFORMED_PAST_SUBGROUP_END   17u  /* condition 2 */
#define MOQ_MALFORMED_SUBGROUP_END        18u  /* condition 3 */
#define MOQ_MALFORMED_PAST_GROUP_END      19u  /* condition 4 */
#define MOQ_MALFORMED_PAST_TRACK_END      20u  /* condition 5 */
#define MOQ_MALFORMED_GROUP_END           21u  /* conflicting End of Group */
#define MOQ_MALFORMED_TRACK_END           22u  /* conflicting End of Track */
#define MOQ_MALFORMED_ORDER               23u  /* FETCH Location not ascending */
#define MOQ_MALFORMED_GAP                 24u  /* §10.8 / §10.9 conditions */
#define MOQ_MALFORMED_TRACK_FAILED        25u  /* the Track already failed */

/* Prior Group ID Gap (0x3C) and Prior Object ID Gap (0x3E) as received. */
typedef struct moq_obj_gaps {
    bool     has_group_gap;
    bool     has_object_gap;
    uint64_t group_gap;
    uint64_t object_gap;
} moq_obj_gaps_t;

typedef struct moq_decoded_object_header {
    uint64_t           object_id;
    uint64_t           payload_len;
    moq_object_status_t status;
    const uint8_t     *properties;     /* borrowed from reader; NULL if none */
    size_t             properties_len;
    bool               has_properties;
    uint8_t            malformed;      /* MOQ_MALFORMED_*; property violations */
    moq_obj_gaps_t     gaps;
} moq_decoded_object_header_t;

/* -- Outbound data-plane encode args (session core → profile) ------- */

typedef struct moq_subgroup_header_encode_args {
    uint64_t track_alias;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint8_t  publisher_priority;
    bool     has_extensions;
    bool     end_of_group;
    bool     first_object;   /* FIRST_OBJECT bit; ignored without one */
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
    moq_cluster_metadata_t cluster;
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
    moq_cluster_metadata_t cluster;
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
    uint32_t options_flags;
    moq_group_order_t group_order;
    bool include_properties;
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
    /* Graceful FIN seen and all objects emitted, but the SUBGROUP_FINISHED
     * event could not be queued (event queue full). The stream stays live with
     * pending_fin set; rx_finish_stream is retried on the next drive until the
     * event is queued, then the entry is recorded-finished and freed. */
    MOQ_RX_PENDING_FINISHED = 8,
    /* A subgroup-level closure is owed for a bound, resolved subgroup -- the
     * reset had no application-visible object terminal to carry it (whole-object
     * mode, a streaming stream idle between objects or past its last one, an
     * object suppressed by Forward State 0, or after a final NORMAL chunk
     * drained) -- but the SUBGROUP_RESET event could not be queued (event queue
     * full). The stream stays live with reset_error_code retained; the emit is
     * retried on the next drive, by any route, until it lands, then the entry
     * is freed. */
    MOQ_RX_PENDING_RESET    = 9,
    /* The session STOPped this stream (STOP_DATA queued) but the peer has not
     * yet FIN'd or RESET it. STOP_SENDING cannot recall bytes already sent,
     * so the entry keeps the stream's identity -- its buffers, bindings and
     * any pending object are released at the STOP -- and every later chunk
     * is consumed without parsing or emitting until the peer's FIN or RESET
     * frees the entry (neither counts as a completed stream; the FIN is still
     * noted as the transport stream's end, so later bytes on the ref are
     * data after FIN). The entry holds
     * its receive slot meanwhile; a stream stopped because its subscription
     * ended on this side gives the slot up and keeps only its identity, in
     * rx_stopped_refs (rx_end_bound_streams). */
    MOQ_RX_STOPPED          = 10,
} moq_rx_parse_state_t;

/* One subscription a subgroup stream on a shared alias has delivered to
 * (see moq_rx_stream_t.fan). */
typedef struct moq_rx_fan {
    moq_subscription_t sub;   /* a SUBSCRIBE-initiated subscription, or */
    moq_publication_t  pub;   /* a PUBLISH-initiated one (exactly one valid) */
    /* A subscription to the same track still awaiting its response: the
     * alias may turn out to be its own (draft-22 §3.1.3), so the objects its
     * filter would accept are held for it instead of surfaced. */
    bool pending;
    bool cur;      /* the current object passes this subscription's filter */
    bool owed;     /* the current object's (or chunk's) event is still owed */
    bool closed;   /* an object terminal (RESET chunk) already closed it */
    bool closure;  /* its SUBGROUP_FINISHED / SUBGROUP_RESET was emitted */
} moq_rx_fan_t;

typedef struct moq_rx_stream {
    bool                   active;
    /* Preserve omission until delivery, including deferred alias binding. */
    bool                   default_priority;
    /* RESET_STREAM code retained while a SUBGROUP_RESET emit is parked, so a
     * re-drive cannot overwrite or lose the code the peer actually sent. */
    uint64_t               reset_error_code;
    /* Frozen per-object suppression decision (Forward State 0 prohibits
     * Objects): stamped ONCE at object-header admission and retained
     * through all chunks, retries, FIN and terminal RESET, so a Forward
     * flip acknowledged mid-object never truncates or orphans a chunk
     * sequence -- it affects the NEXT object only. */
    bool                   suppress_cur_object;
    moq_stream_kind_t      stream_kind;
    moq_rx_parse_state_t   parse_state;
    moq_stream_ref_t       stream_ref;
    moq_subscription_t     sub;
    moq_fetch_t            fetch;
    moq_publication_t      pub_handle;
    /* A subgroup stream on an alias that several subscriber-role
     * subscriptions share (draft-22 §3.1): `sub` stays invalid and each
     * object is attributed by re-applying every sharing subscription's
     * filter at its admission. `fan` (owned, `fan_cap` entries) lists, in
     * first-match order, every subscription the stream delivered to that
     * is still live; the stream counts toward one subscription's Stream
     * Count only when it never delivered to any other (`fan_ambiguous`
     * records that it did, even after the others ended). */
    bool                   fan_shared;
    bool                   fan_ambiguous;
    uint32_t               fan_count;
    uint32_t               fan_cap;
    moq_rx_fan_t          *fan;
    /* A fetch response stream bound to a fill child (by the initiating
     * request id in its header). Kept across a local STOP so the peer's FIN
     * or RESET still settles the child. sub / pub_handle carry the fill's
     * parent for stream counting only. On every bound data stream the
     * binding survives a local STOP for the same reason: the peer's terminal
     * on a stopped stream counts it toward the Stream Count gate. */
    bool                   fill_bound;
    moq_fill_ref_t         fill_ref;
    /* Set when this data uni is the late response to a locally-cancelled fetch:
     * the fetch-cancel tombstone for cancel_tomb_request_id is consumed once the
     * STOP_DATA is actually queued (which may be deferred via MOQ_RX_NEED_STOP),
     * so a retried stop still releases the tombstone. */
    bool                   stop_consumes_cancel_tomb;
    /* The subscription or publication this stream was bound to ended on this
     * side (rx_end_bound_streams): its STOP carries CANCELLED and, once
     * queued, the entry gives way to an identity record in rx_stopped_refs. */
    /* The session stops this stream because the subscription it belongs
     * to is being deleted while its state is kept (the Stream Count wait
     * expired): its STOP carries CANCELLED, as a binding_ended one does. */
    bool                   stop_cancels;
    bool                   binding_ended;
    uint64_t               cancel_tomb_request_id;
    moq_fetch_prior_object_t fetch_prior;
    /* RX owns this snapshot across FETCH/fill owner retirement and reuse.
     * An inherited unknown fill order waits for its authoritative ACK. */
    moq_group_order_t       effective_fetch_group_order;
    bool                   fetch_order_known;
    uint64_t               track_alias;
    uint64_t               group_id;
    uint64_t               subgroup_id;
    uint8_t                publisher_priority;
    bool                   has_extensions;
    bool                   has_prev_object;
    bool                   pending_fin;
    bool                   resumed_deferred;  /* resolved alias or FETCH order;
                                                 re-driven on poll retry until
                                                 its buffered objects drain */
    bool                   subgroup_id_from_first_object;
    bool                   subgroup_id_resolved;
    bool                   end_of_group;
    bool                   first_object;      /* header FIRST_OBJECT bit */
    bool                   cur_first_object;  /* current object is the
                                                 stream's first under that
                                                 bit */
    uint64_t               prev_object_id;
    /* The last Object the stream carried completely: an Object is complete
     * once the next one's header parses, or once the stream ends with no
     * payload of it outstanding. What a subgroup terminal reports. */
    bool                   has_last_complete;
    uint64_t               last_complete_object_id;

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
    /* The peer's RESET_STREAM code owed to the pending terminal chunk, copied
     * from the retained obligation below when the terminal is armed. */
    uint64_t               pending_reset_code;
    /* A peer RESET_STREAM was observed and its terminal is not yet emitted.
     * Captured BEFORE any operation that can block, so a refused flush cannot
     * lose the obligation, and never overwritten: the FIRST peer code is the
     * cause, whatever a later re-drive passes. Every drive route -- repeated
     * on_data_reset, or the generic pending-chunk path of on_data_bytes --
     * completes this same obligation. */
    bool                   reset_owed;
    /*
     * Intrusive OCCUPANCY list, ascending slot order. A slot is linked iff it
     * is allocated (state != FREE / active). Every sweep that used to scan the
     * whole pool walks this list instead, so its cost follows the live owner
     * set rather than the configured capacity -- and because the list is kept
     * in slot order, owners are still served in exactly the order the full
     * scan served them, so no externally visible ordering changes.
     */
    int32_t occ_next;    /* next linked slot, -1 = end */
    int32_t occ_prev;    /* previous linked slot, -1 = head */
    bool    occ_linked;  /* membership, so link/unlink are idempotent-safe */
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
    /* Held for a subscription still awaiting its response, under an alias
     * another subscription to its track holds (draft-22 §3.1.3): a decoded
     * object (`bytes` unused, `len` its charge against the ring's byte
     * budget), surfaced only to `for_sub` if its answer names `alias`. */
    moq_subscription_t for_sub;
    uint64_t  group_id, subgroup_id, object_id;
    uint8_t   publisher_priority;
    uint8_t   status;
    bool      end_of_group;
    bool      datagram;
    bool      default_priority;
    bool      first_object;
    bool      subgroup_properties;
    moq_rcbuf_t *payload;      /* owned reference */
    moq_rcbuf_t *properties;   /* owned reference */
} moq_staged_datagram_t;

/* Overwrite memory that held a credential before it is released. The stores go
 * through a volatile pointer so they are not elided as dead before a free. */
static inline void session_wipe(void *p, size_t n)
{
    volatile uint8_t *b = (volatile uint8_t *)p;
    while (n--) *b++ = 0;
}

/* An object to hold for a pending subscription. The references stay the
 * caller's; the ring takes its own. */
typedef struct moq_held_object {
    uint64_t group_id, subgroup_id, object_id;
    uint8_t  publisher_priority;
    uint8_t  status;
    bool     end_of_group;
    bool     datagram;
    bool     default_priority;
    bool     first_object;
    bool     subgroup_properties;
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;
} moq_held_object_t;

/* Hold `o` for the pending subscription `for_sub` in the held-object ring
 * (MOQ_STAGED_DG_SLOTS entries and the staged byte budget, drop-oldest,
 * counted in staged_dropped). Never fails the caller. */
void session_stage_held_object(moq_session_t *s, moq_subscription_t for_sub,
                               uint64_t alias, const moq_held_object_t *o);

uint8_t session_resolve_publisher_priority(moq_session_t *s,
                                           moq_subscription_t sub,
                                           moq_publication_t pub,
                                           bool omitted, uint8_t priority);
/* A pending subscription got its answer or ended: objects held for it under
 * another alias than `alias` (all of them when not `established`) are
 * dropped, and so are its held-for entries on shared streams. */
void session_settle_pending(moq_session_t *s, moq_subscription_t sub,
                            bool established, uint64_t alias);
void rx_settle_pending(moq_session_t *s, moq_subscription_t sub,
                       bool established, uint64_t alias);
/* Objects on `alias` are attributed by filter: several subscriptions hold
 * it, or a subscription to its track awaits its response. */
bool alias_attribution_shared(moq_session_t *s, uint64_t alias);
bool sub_pending_for_track(const moq_sub_entry_t *e, const moq_track_hist_t *h);
/* A pending subscription's filter, resolved against the newest Largest
 * Object known here, admits {group, object} and it forwards. */
bool sub_pending_accepts(moq_session_t *s, const moq_sub_entry_t *e,
                         uint64_t group, uint64_t object);

/* -- Peer FIN on a request bidi ------------------------------------ *
 * The peer has closed its send half when the durable latch records a wire FIN,
 * or when a same-call FIN handed over with the request still sits on the
 * destination owner as transitional ownership. A terminal that would reserve a
 * drain ref to absorb the peer's remaining send half must consult this, not
 * rs.recv_fin alone: a FIN already observed leaves nothing to drain, and a
 * full drain ring must not block a terminal the peer has already finished. */
static inline bool pub_peer_fin_observed(const moq_pub_entry_t *e) {
    return e->rs.recv_fin || e->handoff_fin_pending;
}
static inline bool ts_peer_fin_observed(const moq_ts_entry_t *e) {
    return e->rs.recv_fin || e->handoff_fin_pending;
}
static inline bool fetch_peer_fin_observed(const moq_fetch_entry_t *e) {
    return e->rs.recv_fin || e->handoff_fin_pending;
}
static inline bool ann_peer_fin_observed(const moq_ann_entry_t *e) {
    return e->rs.recv_fin || e->handoff_fin_pending;
}
static inline bool track_sub_peer_fin_observed(const moq_track_sub_entry_t *e) {
    return e->rs.recv_fin || e->handoff_fin_pending;
}
/* The namespace-sub family's durable latch is `pending_fin`, and its handler
 * also sees the wire FIN of the current call, so the unified fact takes all
 * three. */
static inline bool ns_sub_fin_observed(const moq_ns_sub_entry_t *e, bool fin) {
    return fin || e->pending_fin || e->handoff_fin_pending;
}

/* No-slot admission carriers. */
int  noslot_carrier_find(const moq_session_t *s, moq_stream_ref_t ref);
bool noslot_carrier_install(moq_session_t *s, moq_stream_ref_t ref, bool fin);
void noslot_carrier_remove(moq_session_t *s, moq_stream_ref_t ref);

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

/*
 * Scheduling and timing record of a queued action, kept beside the action
 * ring (same index). Only SEND_DATA and SEND_DATAGRAM carry one; every other
 * kind has a zeroed record and is never reordered. The adapter's poll takes
 * the best schedulable action rather than the oldest (design: Priorities,
 * rules 1-4), and an object whose OBJECT_DELIVERY_TIMEOUT has elapsed is
 * dropped instead of being handed to the transport.
 */
#define MOQ_SCHED_MODE_NONE     0u
#define MOQ_SCHED_MODE_SUBGROUP 1u   /* subscription-delivered, on a subgroup stream */
#define MOQ_SCHED_MODE_DATAGRAM 2u   /* subscription-delivered, as a datagram */
#define MOQ_SCHED_MODE_FILL     3u   /* fill-delivered, on the fill fetch stream */
#define MOQ_SCHED_MODE_FETCH    4u   /* a FETCH response */

typedef struct moq_action_meta {
    uint64_t deadline_us;        /* OBJECT_DELIVERY_TIMEOUT expiry; UINT64_MAX = none */
    uint64_t owner;              /* the request the object answers (handle bits); 0 = none */
    uint64_t group_id;
    uint64_t subgroup_id;        /* SUBGROUP */
    uint64_t object_id;          /* DATAGRAM / FILL */
    uint8_t  subscriber_priority;
    uint8_t  publisher_priority;
    uint8_t  mode;               /* MOQ_SCHED_MODE_* */
    uint8_t  group_order;        /* the owner's: MOQ_GROUP_ORDER_ASCENDING / DESCENDING */
    uint8_t  fill_group_order;   /* FILL: the fill's own order */
    bool     has_location;       /* group_id and the id field are meaningful */
} moq_action_meta_t;

typedef struct moq_state_location_cell {
    moq_location_filter_cfg_t value;
    size_t next;
} moq_state_location_cell_t;

struct moq_session {
    moq_alloc_t         alloc;
    size_t              alloc_size;
    moq_session_state_t state;
    moq_perspective_t   perspective;
    uint64_t            last_now_us;
    uint64_t            borrow_epoch;

    const moq_profile_ops_t *profile;
    void                    *profile_state;

    /* The transport binding, when the profile requires one (see
     * transport_binding_internal.h); zero and unbound otherwise. */
    moq_binding_state_t      binding;

    /* The one transport owner (a transport bridge) attached to this session,
     * or NULL. Opaque identity only; see moq_session_register_transport_owner. */
    const void              *transport_owner;
    /* The registered owner's own next deadline (its local hold-age expiry),
     * folded into moq_session_next_deadline_us. moq_session_tick never
     * consumes it: only the owner's own service rearms or clears it. */
    bool                     has_owner_deadline;
    uint64_t                 owner_deadline_us;

    moq_setup_params_t  local_setup;
    moq_setup_params_t  peer_setup;
    moq_token_cache_t   peer_token_cache;

    bool                send_auth_token_cache_size;
    uint64_t            auth_token_cache_size;
    /* The application answers subscription updates it can be offered
     * (moq_session_cfg_t.defer_subscription_updates). */
    bool                defer_subscription_updates;
    /* The application resolves FETCH ranges stated relative to the Largest
     * Object (moq_session_cfg_t.defer_fetch_ranges). */
    bool                defer_fetch_ranges;
    /* The application answers draft-22 namespace updates
     * (moq_session_set_defer_namespace_updates). */
    bool cluster_enabled;
    bool cluster_negotiated;
    bool cluster_has_relay_cost;
    bool cluster_peer_has_relay_cost;
    uint64_t cluster_hop_id;
    uint64_t cluster_relay_cost;
    uint64_t cluster_peer_hop_id;
    uint64_t cluster_peer_relay_cost;
    bool                defer_namespace_updates;
    /* The peer's implementation identifier, copied at setup completion. */
    uint8_t            *peer_implementation;
    size_t              peer_implementation_len;
    /* Authorization tokens this side presents in its SETUP, copied from the
     * cfg into the session arena: the values, and (on profiles with
     * encode_setup_token) each one's encoded Setup Option value. A session
     * that holds any has its whole block wiped before it is freed. */
    moq_auth_token_t   *setup_tokens;
    moq_bytes_t        *setup_token_wire;
    size_t              setup_token_count;

    uint8_t      *send_buf;
    size_t        send_cap;
    size_t        send_len;

    moq_action_t *actions;
    size_t        action_cap;
    size_t        action_head;
    size_t        action_tail;
    moq_action_meta_t *action_meta;   /* parallel to actions */

    moq_event_t  *events;
    size_t        event_cap;
    size_t        event_head;
    size_t        event_tail;
    /* Monotonic event-progress token: ++ per successful ENQUEUE (push_event)
     * AND per successful DEQUEUE (poll_events_ex transfer). Never reset (survives
     * event_head/event_tail resets); unsigned wrap is fine — read only for
     * equality by the transport bridge's private adapter SPI to detect that a
     * whole pump+service cycle moved events (a bounded pump drained some and/or
     * service refilled the queue). See moq_transport_bridge_event_progress_token. */
    uint64_t      event_progress_token;

    /* Independent monotonic terminal facts. They are recorded separately, not
     * as one ordinal state, because their order is not fixed: a local close
     * enqueues MOQ_EVENT_SESSION_CLOSED before any transport shutdown, while a
     * peer-initiated close can complete natively first. Neither becomes false,
     * and re-notifying either is a no-op.
     *
     *   terminal_enqueued  set only when SESSION_CLOSED was actually placed in
     *                      the event queue (push_event returned MOQ_OK).
     *   terminal_observed  set only when poll_events_ex transferred that event
     *                      to a caller -- observation, not mere availability.
     *
     * terminal_observed implies terminal_enqueued. Read through the private
     * lockstep SPI (session_transport.h -> transport bridge -> adapter); never
     * public API. */
    bool          terminal_enqueued;
    bool          terminal_observed;

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

    /*
     * Heads of the five intrusive occupancy lists (see the per-entry fields).
     * -1 means empty; they are initialized explicitly because the session slab
     * is zero-filled and slot 0 is a valid member.
     */
    int32_t          pub_occ_head;
    int32_t          sub_occ_head;
    int32_t          sg_occ_head;
    int32_t          rx_occ_head;
    int32_t          fetch_occ_head;

    moq_ann_entry_t *announcements;
    size_t           ann_cap;

    moq_fetch_entry_t *fetches;
    size_t             fetch_cap;

    moq_pub_entry_t   *publishes;
    size_t             pub_cap;

    moq_ts_entry_t    *track_statuses;
    size_t             ts_cap;

    /* Per-track largest-location history registry (see
     * moq_session_cfg_t.max_track_history_records). Pooled array of
     * `th_cap` records, each keyed by an owned canonical full-track-name
     * blob. Empty (unobserved) records are refcounted by the request
     * entries that reserved them and reclaimed when the last reference
     * drops; once `has_largest` becomes true a record is pinned until
     * session destruction. Separately allocated (not part of the single
     * session block) so cap changes never disturb the block layout. */
    moq_track_hist_t *track_hist;
    size_t            th_cap;

    /* Malformed Track detection state (draft-22 §12.1), one record per
     * received Track: by alias for subscription data, by fetch for a FETCH
     * response. Allocated on first use, only on profiles that fail a
     * malformed Track locally; see session_malformed.c. */
    struct moq_mt_track *mt_head;
    size_t               mt_count;
    /* Requests whose Track was found malformed and whose cancellation has
     * not committed yet (their mt_failing flags). */
    size_t               mt_failing_count;
    /* Event slots held back from every producer while a teardown that owes
     * its own terminal event runs nested work that could emit events (a
     * parent's fill children), so the parent's event keeps its slot. */
    size_t               event_reserve;

    /* Fill children of subscriptions (see moq_session_cfg_t.max_fills):
     * the ownership table over a separately allocated entry array, plus a
     * parallel per-slot record of what the table deliberately leaves out
     * (the effective request, cached handle, terminal error, write cursor).
     * Separately allocated so the cap never disturbs the block layout. */
    moq_fill_table_t   fills;
    moq_fill_entry_t  *fill_storage;
    struct moq_fill_side *fill_side;
    size_t             fill_cap;

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

    /* Immutable state-notification locations, one cell per event slot on
     * supported profiles.
     * Queued cells stay pinned; polled cells recycle only at the next advance.
     * Each list ends at state_location_cap. Retired tail enables O(1) recycle. */
    moq_state_location_cell_t *state_locations;
    size_t state_location_cap;
    size_t state_location_free;
    size_t state_location_retired;
    size_t state_location_retired_tail;

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

    /* Streams STOPped without a receive entry -- those of a subscription that
     * ended on this side (rx_end_bound_streams): a STOP_DATA was queued and
     * the stream's identity is kept here (arena-backed, bounded like
     * rx_finished, never evicted) so every later chunk is consumed without
     * parsing until the peer's FIN or RESET releases the record. The session
     * owns these streams (moq_session_has_transport_stream). With the records
     * exhausted such a stream keeps its entry in MOQ_RX_STOPPED instead. A new
     * stream that finds no free entry is refused with WOULD_BLOCK and nothing
     * retained -- no STOP, no ownership -- which the bridge treats as an
     * unowned refusal and discards that stream itself. */
    uint64_t        *rx_stopped_refs;
    size_t           rx_stopped_cap;
    size_t           rx_stopped_count;

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

    /* Bounded grace cache of request IDs of publisher-role fetches this side
     * ABORTED (moq_session_abort_fetch): the slot is freed at abort, and on
     * profiles whose cancellation is a control message (draft-16 FETCH_CANCEL)
     * the peer's cancel may cross the reset in flight. A cancel naming a
     * recorded id is consumed instead of being treated as an unknown request.
     * Separate from fetch_cancel_tombs on purpose: that ring holds ids this
     * side originated as a FETCHER, this one holds peer-originated ids this
     * side answered as a PUBLISHER, so neither ring's lookups can alias the
     * other's. Drop-oldest when full (capacity = fetch pool size): an evicted
     * id's late cancel fails closed on the unknown-request path. */
    uint64_t        *fetch_abort_tombs;
    size_t           fetch_abort_tomb_cap;
    size_t           fetch_abort_tomb_count;

    /* Request-bidi stream_refs locally cancelled (unsubscribe / fetch cancel)
     * on stream-correlated profiles, draining any late in-flight response until
     * the bidi FINs or the peer resets it. Draft-16 leaves this empty. */
    uint64_t        *drain_refs;
    uint8_t         *drain_ref_reasons;   /* parallel to drain_refs (moq_drain_reason_t) */
    struct moq_drain_scan *drain_ref_scans; /* parallel to drain_refs: framing of a
                                             * GOAWAY-received ref */
    size_t           drain_ref_cap;
    size_t           drain_ref_count;

    /* No-slot admission carriers. A draft-18 request bidi that arrives
     * with the subscription pool full owes a fixed, content-independent
     * terminal (pre-setup STOP+RESET, post-setup REQUEST_ERROR+FIN). When drain/
     * action/send capacity cannot admit that terminal yet, the OPERATION -- not
     * the peer bytes -- is retained here: just the stream ref and the observed
     * FIN fact, so the documented empty re-feed re-drives the terminal with no
     * peer-byte redelivery, and moq_session_has_transport_stream() reports the
     * stream live so the bridge retains rather than fatalizes. Arena-backed in
     * the session slab at create time (installation cannot allocation-fail); cap
     * is bounded by the total request-owner/drain lifecycle capacity. */
    moq_noslot_carrier_t *noslot_carriers;
    size_t           noslot_carrier_cap;
    size_t           noslot_carrier_count;

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
    /* §9.8: configured terminal wait (effective; cfg 0 -> library default)
     * and the session-level bounded retry deadline for expiry work blocked
     * on action/event capacity -- recomputed after the COMPLETE two-pool
     * reap sweep, UINT64_MAX when no expired entry has blocked work. */
    uint64_t            done_wait_timeout_us;
    uint64_t            expiry_retry_deadline_us;

    /*
     * Budgeted deferred-completion sweep.
     *
     * The deferred done sweeps scan both pools and,
     * per owner, can drive session_stop_bound_streams_resumable() -- itself a restart
     * scan over rx_cap. Under a budget the sweep must suspend and resume, so
     * the checkpoint is NESTED: pool -> owner slot -> rx scan position ->
     * finalize phase.
     *
     * sweep_now_us is the time the suspended sweep STARTED, and last_now_us is
     * stamped when a sweep is ARMED rather than per call, so a resumed sweep
     * retires against its own epoch and no slot -- nor any deadline derived
     * when it retires -- is evaluated against two clocks. The call that
     * finishes it then runs ONE fresh sweep at its own now_us regardless of
     * whether the clock advanced. expiry_retry_deadline_us is recomputed only
     * once a complete two-pool sweep retires -- never mid-suspension.
     *
     * The cursor is session-owned. Bridge service establishes the budgeted
     * context; ordinary session APIs run unlimited and complete any active
     * cursor (then catch up) before their own operation. Close/destroy
     * discards it.
     */
    bool                sweep_active;      /* a cursor is mid-sweep */
    moq_sweep_stage_t    sweep_stage;      /* current stage */
    size_t              sweep_slot;        /* owner index within that pool */
    size_t              sweep_rx_pos;      /* rx scan position for this owner */
    moq_sweep_phase_t   sweep_phase;
    uint64_t            sweep_now_us;      /* epoch the sweep began at */
    bool                sweep_reaped_subgroup;  /* a reap occurred this scan */
    bool                sweep_rx_found;         /* a STOP fired this rx scan */

    /* Budget context, active only inside
     * moq_transport_bridge_service_budgeted(). The unlimited
     * moq_transport_bridge_service() enters none, so the advancing entry points
     * it reaches cannot suspend. */
    bool                budget_active;
    uint32_t            budget_remaining;

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
    /* Per-subscription active peer-announced suffix cap (cfg-resolved, default
     * MOQ_DEFAULT_MAX_NS_SUFFIXES). Bounds the inbound suffix tree of one
     * SUBSCRIBE_NAMESPACE independently of the session-wide receive budget. */
    size_t              max_ns_suffixes;
    moq_index_entry_t  *idx_ns_by_ref;
    size_t              idx_ns_mask;

    /* SUBSCRIBE_TRACKS pool (draft-18 only). Keyed in idx_req_by_streamref via
     * the shared request registry (no dedicated index); independent overlap
     * space from ns_subs. */
    moq_track_sub_entry_t *track_subs;
    size_t                 track_sub_cap;
};

/* A size_t payload is unconditionally representable when the platform's
 * entire size_t range fits in draft-16's QUIC-varint ceiling. Compile the
 * comparison out on those targets (notably 32-bit embedded builds); wider
 * targets retain the profile-specific draft-16/18 bound. */
_Static_assert(MOQ_QUIC_VARINT_MAX == (UINT64_MAX >> 2),
               "QUIC varint ceiling changed");
static inline bool session_payload_size_fits_profile(
    const moq_session_t *s, size_t payload_len)
{
#if SIZE_MAX <= (UINT64_MAX >> 2)
    (void)s;
    (void)payload_len;
    return true;
#else
    return (uint64_t)payload_len <= s->profile->object_payload_len_max;
#endif
}

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
    uint8_t              malformed;    /* MOQ_MALFORMED_*; property violations */
    moq_obj_gaps_t       gaps;
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

/* Saturating wire-ms -> local-us conversion (§9.8 A5/A6): a legal vi64
 * millisecond value must never wrap; UINT64_MAX means "effectively
 * unbounded" for DURATIONS only -- absolute deadlines always go through
 * deadline_add(), which never returns UINT64_MAX. */
static inline uint64_t ms_to_us_sat(uint64_t ms) {
    return ms > UINT64_MAX / 1000u ? UINT64_MAX : ms * 1000u;
}

/* Per-type negotiation (d16 §9.2.2.2 / d18 §8): both sides nonzero -> MIN;
 * exactly one nonzero -> that one; none -> 0 (no timeout). A present
 * explicit zero counts as "no timeout set" for negotiation. */
static inline uint64_t dt_negotiate_ms(bool has_a, uint64_t a,
                                       bool has_b, uint64_t b) {
    uint64_t va = (has_a && a > 0) ? a : 0;
    uint64_t vb = (has_b && b > 0) ? b : 0;
    if (va && vb) return va < vb ? va : vb;
    return va ? va : vb;
}

static inline uint64_t deadline_add(uint64_t now, uint64_t timeout) {
    uint64_t d = now + timeout;
    if (d < now || d == UINT64_MAX) return UINT64_MAX - 1;
    return d;
}

/* A subscription's effective group order: the one its subscriber asked
 * for, else the publisher's preference from the track (the
 * DEFAULT_PUBLISHER_GROUP_ORDER Track Property), else Ascending -- the
 * property's own default. */
static inline uint8_t session_group_order(uint8_t requested, uint8_t track) {
    if (requested == MOQ_GROUP_ORDER_ASCENDING ||
        requested == MOQ_GROUP_ORDER_DESCENDING) return requested;
    if (track == MOQ_GROUP_ORDER_DESCENDING) return MOQ_GROUP_ORDER_DESCENDING;
    return MOQ_GROUP_ORDER_ASCENDING;
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

/* A Track Namespace whose first field is "." or ".session": reserved by
 * draft-18 §3.2.1-§3.2.2 and draft-22 §2.4.3, §6.5. */
bool session_namespace_reserved(const moq_namespace_t *ns);

/* Whether the session refuses a new inbound request itself, before the
 * application sees it, and with which REQUEST_ERROR code: after our GOAWAY
 * (the peer SHOULD NOT start one; we refuse it rather than close), or for a
 * session-level track or namespace this implementation does not recognize
 * -- any under "." or ".session" (DOES_NOT_EXIST). `ns` is the request's
 * namespace or namespace prefix, NULL when it names none. */
bool session_refuses_request(const moq_session_t *s, const moq_namespace_t *ns,
                             uint64_t *out_code);

void session_begin_advance(moq_session_t *s, uint64_t now_us);

/* Advancing-call preamble for the service-reachable entry points. Returns
 * MOQ_SESSION_SUSPENDED only inside a budget context; the caller must then
 * return it without refreshing idle or running its own transition. */
moq_result_t session_advance_entry(moq_session_t *s, uint64_t now_us);
/* Discard any in-flight completion-sweep cursor (close/destroy). */
void session_sweep_discard(moq_session_t *s);
void session_sweep_owner_reset(moq_session_t *s);

void decref_queued_data_payloads(moq_session_t *s);
void decref_queued_event_payloads(moq_session_t *s);
void free_rx_stream_bufs(moq_session_t *s);

bool action_queue_full(const moq_session_t *s);
static inline size_t action_queue_avail(const moq_session_t *s) {
    size_t used = s->action_tail - s->action_head;
    return used < s->action_cap ? s->action_cap - used : 0;
}
moq_result_t push_action(moq_session_t *s, const moq_action_t *a);
/* Queue a data action with its scheduling record. A NULL record is derived
 * from the stream the action belongs to (subgroup, fetch or fill stream);
 * deadline_us is the object's OBJECT_DELIVERY_TIMEOUT expiry (UINT64_MAX for
 * none) and is stored whichever way the record came. */
moq_result_t push_action_ex(moq_session_t *s, const moq_action_t *a,
                            const moq_action_meta_t *meta, uint64_t deadline_us);
/* Earliest OBJECT_DELIVERY_TIMEOUT expiry among the queued actions. */
uint64_t session_queued_data_deadline(const moq_session_t *s);
/* Drop every queued object whose OBJECT_DELIVERY_TIMEOUT has elapsed at
 * last_now_us: an expired datagram is discarded; an expired subgroup object
 * takes the rest of its stream's queued data with it and the stream is reset
 * with DELIVERY_TIMEOUT. */
void session_expire_queued_data(moq_session_t *s);
bool event_queue_full(const moq_session_t *s);
static inline size_t event_queue_avail(const moq_session_t *s) {
    size_t used = s->event_tail - s->event_head + s->event_reserve;
    return used < s->event_cap ? s->event_cap - used : 0;
}
moq_result_t push_event(moq_session_t *s, const moq_event_t *e);
moq_location_filter_cfg_t *event_location_alloc(moq_session_t *s);
void event_location_release(moq_session_t *s, const moq_location_filter_cfg_t *value);

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
/* Transactional namespace copy into event scratch: a zero-part namespace
 * succeeds as {NULL, 0} with the cursor byte-for-byte unchanged; any failure
 * restores the cursor exactly and leaves *dst unwritten. Shared by the
 * aligned-parts-plus-tails producers (subscribe, publish, fetch,
 * publish-namespace, track-status, subscribe-tracks request + publish-blocked);
 * specialized suffix/redirect copiers keep their own shapes. */
bool event_scratch_copy_namespace(moq_session_t *s,
                                  const moq_namespace_t *src,
                                  moq_namespace_t *dst);
/* Verdict for an event's complete event-scratch requirement. */
typedef enum {
    MOQ_EVENT_SCRATCH_FITS = 0,   /* copy now                                */
    MOQ_EVENT_SCRATCH_BLOCKED,    /* fits an empty arena, not the live tail  */
    MOQ_EVENT_SCRATCH_PERMANENT,  /* cannot fit even an empty arena          */
} moq_event_scratch_verdict_t;
/* Classify a single contiguous byte tail. A zero length needs nothing; a
 * length beyond the whole arena can never be satisfied; anything that fits an
 * empty arena but not the space left after the live cursor is retryable once
 * the event queue drains. Pure. */
moq_event_scratch_verdict_t event_scratch_classify_bytes(
    const moq_session_t *s, size_t len);
/* Classify the complete requirement -- token-value tails in order, then the
 * alignment padding and the aligned token array -- against both an empty arena
 * and the current cursor, without mutating scratch. BLOCKED is retryable once
 * the event queue drains and the arena recycles; PERMANENT never becomes
 * satisfiable, so its caller closes the session. */
moq_event_scratch_verdict_t setup_event_scratch_classify(
    const moq_session_t *s, const moq_resolved_token_t *toks,
    size_t token_count);
uint8_t *scratch_copy(moq_session_t *s, const uint8_t *data, size_t len);

/* Probe only after BUFFER with occupied send bytes. Call the same typed
 * encoder/arguments without changing the session's output scratch budget or
 * cursor, then end the probe on every encoder result. Never queue its bytes. */
moq_result_t send_probe_begin(moq_session_t *s, moq_buf_writer_t *probe);
moq_result_t send_probe_end(moq_session_t *s, moq_buf_writer_t *probe,
                            moq_result_t encoded);

/* queue_send_control for a request cancellation (UNSUBSCRIBE,
 * FETCH_CANCEL): a send buffer too full now because it still holds earlier
 * output is MOQ_ERR_WOULD_BLOCK -- the buffer is released as actions are
 * polled, so the caller retries -- and only a message larger than the whole
 * buffer is MOQ_ERR_BUFFER, a cancellation that can never be made. */
moq_result_t queue_send_cancel(moq_session_t *s, const uint8_t *data, size_t len);
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
moq_result_t queue_open_uni_control_in_place(moq_session_t *s,
                                             moq_stream_ref_t ref, size_t len);
/* reason must be a string literal or NULL (stored without copy). */
moq_result_t close_with_error(moq_session_t *s,
                               uint64_t code, const char *reason);

/* Draft-neutral inbound GOAWAY: enter DRAINING and surface MOQ_EVENT_GOAWAY
 * with the sender's Timeout when the wire carries one. The receiver arms no
 * drain deadline: GOAWAY_TIMEOUT is the sender's close code. The profile
 * decodes the wire form and does the active / duplicate checks before calling
 * this. */
moq_result_t session_core_on_goaway(moq_session_t *s,
                                    const uint8_t *uri, size_t uri_len,
                                    bool has_timeout, uint64_t timeout_ms);

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
 * Weighs the connect URI / namespace / name / reason as one requirement, then
 * copies them into the event-scratch arena and pushes the event.
 * handle_opaque is the request's public handle value.
 *
 * Result contract for the family error handlers that call this:
 *  - MOQ_OK with the session still open: the event was surfaced; perform the
 *    terminal free/drain.
 *  - MOQ_OK with the session CLOSED: a requirement no arena could hold ended
 *    the session instead. The queues were cleared and the terminal queued, so
 *    the caller must NOT run its teardown -- return immediately.
 *  - MOQ_ERR_WOULD_BLOCK: the arena and the request are untouched; retain the
 *    message and retry once events drain.
 *  - any other negative: propagate. */
moq_event_scratch_verdict_t redirect_event_scratch_classify(
    const moq_session_t *s, const moq_decoded_redirect_t *rd, size_t reason_len);
moq_result_t session_core_emit_request_redirect(
    moq_session_t *s, moq_request_family_t family, uint64_t handle_opaque,
    const moq_decoded_redirect_t *rd, uint64_t error_code,
    bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len);

void sg_free_entry(moq_session_t *s, size_t slot);
bool sg_reap_terminal_resumable(moq_session_t *s, uint32_t *budget);

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

/* STOP every stream still deferred on an unestablished alias. A STOP the
 * action queue refuses stays owed on its entry (NEED_STOP);
 * session_retry_owed_stops issues it once capacity returns. */
void session_discard_deferred_streams(moq_session_t *s);

/*
 * Owed stream actions. A teardown reserves only its own mandatory actions
 * (its event, its messages, fill-stream actions, what its caller queues
 * next). The per-stream actions it ends -- the receive side's STOP of each
 * bound data stream (rx_end_bound_streams) and the send side's RESET of each
 * open subgroup (sg_reset_or_owe) -- are queued while more than `keep`
 * action slots remain and otherwise owed on the stream's own entry
 * (MOQ_RX_NEED_STOP, MOQ_SG_RESET_OWED). A teardown therefore completes
 * however many streams are open, an owed action never takes a slot a
 * mandatory one needs, and none is dropped: session_retry_owed_stream_actions
 * issues them after each poll that frees capacity.
 */
void session_retry_owed_stream_actions(moq_session_t *s);
/* Queue the PUBLISH_DONE(UPDATE_FAILED) of each MOQ_SUB_DONE_HELD /
 * MOQ_PUB_DONE_HELD entry whose subgroup resets are all queued, and free the
 * entry; stops when the queue has no room. */
void session_release_held_dones(moq_session_t *s);
void pub_release_held_dones(moq_session_t *s);
/* The peer reset or stopped the request stream of a held terminal: free the
 * entry without it (its owed resets still go out). */
void held_done_retire(moq_session_t *s, moq_stream_ref_t ref);
/* The receive half: the STOP_DATA owed by every NEED_STOP receive entry that
 * action capacity now admits. */
void session_retry_owed_stops(moq_session_t *s);
/* End subgroup `slot` with a reset of `code`: queued now when more than
 * `keep` action slots are free, else owed on the entry. The stream takes no
 * more objects either way. */
void sg_reset_or_owe(moq_session_t *s, size_t slot, uint64_t code, size_t keep);
/* The send half: the resets owed by MOQ_SG_RESET_OWED subgroups. */
void sg_retry_owed_resets(moq_session_t *s);

/* Index of `ref_v` among the streams stopped without a receive entry, or -1. */
int rx_stopped_ref_find(const moq_session_t *s, uint64_t ref_v);

/* A subscriber-role subscription or publication (`sub` or `pub`, the other
 * invalid) is ending on this side; call before its entry is freed. Each
 * inbound stream bound to it that is still open is STOPped (CANCELLED) and
 * its buffers released; its receive entry gives way to an identity record, so
 * its late bytes are consumed without events until the peer's FIN or RESET.
 * STOPs are queued only while more than `keep_free` action slots remain --
 * the caller's own later actions keep their room -- and one that cannot be
 * queued now stays owed on the entry (MOQ_RX_NEED_STOP) and is issued once a
 * poll drains actions, so the ending itself never waits on these stops. A stream whose FIN or RESET already
 * arrived is dropped; one parked on its SUBGROUP_FINISHED or SUBGROUP_RESET is
 * left to complete; fill streams are left to the fill owner. Never fails. */
void rx_end_bound_streams(moq_session_t *s, moq_subscription_t sub,
                          moq_publication_t pub, size_t keep_free);
void sg_recompute_deadline(moq_session_t *s);
int sg_find_free(moq_session_t *s);
int sg_find_by_stream_ref(moq_session_t *s, moq_stream_ref_t ref);
/* The subgroup's effective OBJECT_DELIVERY_TIMEOUT expiry for an object whose
 * clock starts at now_us (UINT64_MAX when no timeout applies). */
uint64_t sg_object_deadline_us(moq_session_t *s, const moq_sg_entry_t *e,
                               uint64_t now_us);
/* The subgroup's effective SUBGROUP_DELIVERY_TIMEOUT in microseconds (0 = none). */
uint64_t sg_subgroup_timeout_us(moq_session_t *s, const moq_sg_entry_t *e);
/* Fill the scheduling record of an action on a subgroup stream. */
void sg_stream_meta(moq_session_t *s, const moq_sg_entry_t *e,
                    moq_action_meta_t *out);
/* Fill the scheduling record of an action on a fill fetch stream (no
 * location; the object writer adds group and object). */
void fill_stream_meta(const moq_session_t *s, const moq_fill_entry_t *f,
                      moq_action_meta_t *out);
moq_subgroup_handle_t sg_make_handle(moq_session_t *s, size_t slot);

int sub_resolve_handle(moq_session_t *s, moq_subscription_t h);
/* Whether `h` resolves to a subscription whose peer cancellation is pending
 * (moq_sub_entry_t.cancel_pending). */
bool sub_cancel_pending(moq_session_t *s, moq_subscription_t h);
int sub_find_by_request_id(moq_session_t *s, uint64_t request_id);
int sub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias);
bool sub_track_alias_in_use(moq_session_t *s, uint64_t alias);
/* The established subscriber-role subscriptions the peer gave `alias`, as a
 * chain: sub_find_by_alias_subscriber() is the first, sub_alias_next() the
 * following one (-1 ends it). More than one only on a profile that allows
 * concurrent subscriptions. */
int sub_alias_next(moq_session_t *s, int slot);
size_t sub_alias_count(moq_session_t *s, uint64_t alias);
/* A subscriber-role subscription accepts an object at {group, object}: its
 * acknowledged Forward state is 1 and its resolved window admits the
 * Location (draft-22 §3.3.3: Pass = Forward AND Location Filters). */
bool sub_accepts_object(const moq_sub_entry_t *e, uint64_t group, uint64_t object);
/* Its resolved window admits some Location in `group`. */
bool sub_accepts_group(const moq_sub_entry_t *e, uint64_t group);

/* Every established subscription the peer gave `alias` -- subscriber-role
 * subscriptions first, in establishment order, then subscriber-role
 * publications (draft-22 §3.1.3 lets a SUBSCRIBE- and a PUBLISH-initiated
 * subscription to one track share it). Exactly one of *sub / *pub is set. */
typedef struct moq_alias_iter {
    uint64_t alias;
    int      sub;      /* next subscription slot, -1 once exhausted */
    int32_t  pub;      /* next publication slot to examine, -1 at the end */
} moq_alias_iter_t;
void alias_iter_begin(moq_session_t *s, moq_alias_iter_t *it, uint64_t alias);
bool alias_iter_next(moq_session_t *s, moq_alias_iter_t *it,
                     moq_subscription_t *sub, moq_publication_t *pub);
size_t alias_owner_count(moq_session_t *s, uint64_t alias);
/* The owner accepts an object at {group, object} / some Location in
 * `group` (Forward AND Location filter); false for a stale handle. */
bool alias_owner_accepts_object(moq_session_t *s, moq_subscription_t sub,
                                moq_publication_t pub,
                                uint64_t group, uint64_t object);
bool alias_owner_accepts_group(moq_session_t *s, moq_subscription_t sub,
                               moq_publication_t pub, uint64_t group);
/* The track-history record of an owner (its track identity), or NULL. */
moq_track_hist_t *alias_owner_hist(moq_session_t *s, moq_subscription_t sub,
                                   moq_publication_t pub);
/* A subscriber-role publication accepts an object: its Forward state is 1
 * and the filter it negotiated, resolved against the Largest Object its
 * PUBLISH carried, admits the Location. */
bool pub_accepts_object(const moq_pub_entry_t *e, uint64_t loc_max,
                        uint64_t group, uint64_t object);
bool pub_accepts_group(const moq_pub_entry_t *e, uint64_t loc_max, uint64_t group);
/* Where concurrent subscriptions are allowed: the peer named `alias` for a
 * subscription to the track `hist`; true when the alias already names a
 * DIFFERENT track with an established (or pending inbound) subscription
 * (draft-22 §3.1.3). Elsewhere: any use of the alias. */
bool peer_alias_conflicts(moq_session_t *s, uint64_t alias,
                          const moq_track_hist_t *hist);
/* Where concurrent subscriptions are allowed: every subscription or
 * publication of ours already using outbound `alias` is one we publish of
 * the same track (given as a key blob, or as namespace and name when
 * `key` is NULL), so the application may share it (draft-22 §3.1). */
bool outbound_alias_shareable(moq_session_t *s, uint64_t alias,
                              const uint8_t *key, size_t key_len,
                              const moq_namespace_t *ns, moq_bytes_t name);

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
moq_result_t session_setup_workspace_acquire(moq_session_t *s,
                                             moq_setup_workspace_t *workspace);
void session_setup_workspace_release(moq_session_t *s,
                                      moq_setup_workspace_t *workspace);
/* Consumes workspace on every return, before any deferred-request refeed. */
moq_result_t session_setup_complete_peer(moq_session_t *s,
                                         moq_setup_workspace_t *workspace,
                                         const moq_setup_params_t *peer,
                                         bool *setup_received,
                                         bool setup_sent);

/* Where process_auth_tokens resolves into: up to max_tokens entries of
 * `tokens` (count in *token_count), the request refusal code, the per-token
 * staging flags and the transaction. */
typedef struct moq_auth_resolve_out {
    moq_resolved_token_t *tokens;
    size_t               *token_count;
    size_t                max_tokens;
    uint64_t             *reject_code;
    bool                 *staged;
    moq_auth_txn_t       *txn;
} moq_auth_resolve_out_t;

moq_result_t process_auth_tokens(moq_session_t *s,
    const moq_decoded_auth_token_t *tokens_in, size_t tokens_in_count,
    const moq_auth_resolve_out_t *out);

/* Borrows one prepared prefix during this call only; commit prefix before txn.
 * Abort/free each transaction independently. No prefix allocation is adopted. */
moq_result_t process_auth_tokens_after(moq_session_t *s,
    const moq_decoded_auth_token_t *tokens_in, size_t tokens_in_count,
    moq_resolved_token_t *out_tokens, size_t *out_token_count,
    size_t max_tokens, uint64_t *out_reject_code, bool *out_staged,
    moq_auth_txn_t *txn, const moq_auth_txn_t *prefix);

void process_auth_tokens_commit_txn(moq_session_t *s, moq_auth_txn_t *txn);
void process_auth_tokens_abort_txn(moq_session_t *s, moq_auth_txn_t *txn);

void process_auth_tokens_free_staging(moq_session_t *s,
    moq_resolved_token_t *tokens, const bool *staged,
    size_t count);

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
    uint64_t end_object;    /* OBJECT_END form only */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
    /* A fill request carried on the SUBSCRIBE, or NULL. Only profiles with
     * fills receive one (the core refuses the request otherwise). */
    const moq_fill_input_t *fill;
    bool has_location;
    bool dt_has_object;
    uint64_t dt_object_ms;
    bool dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    bool has_rendezvous_timeout;
    uint64_t rendezvous_timeout_ms;
    /* INCLUDE_PROPERTIES as the caller selected it (only on a profile with
     * include_properties_request). */
    bool has_include_properties;
    bool include_properties;
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
    /* Original properties remain available to the profile's omission policy. */
    bool omit_track_properties;
    bool inherit_track_group_order;
} moq_subscribe_ok_encode_args_t;

/* REQUEST_UPDATE_OK response (design §2b / §2 always-max): the REQUEST_OK sent
 * to acknowledge a peer's REQUEST_UPDATE, carrying the resolved Largest Object
 * (both drafts). request_id is used only by draft-16's control-channel
 * correlation; draft-18 correlates by the request bidi. The has_expires /
 * expires_ms fields exist for completeness (draft-18 vi64 REQUEST_UPDATE_OK MAY
 * carry EXPIRES, and the codec/decoder support it), but the session's outbound
 * ack path currently always passes has_expires=false -- we never originate an
 * EXPIRES on this path. draft-16 has no EXPIRES here regardless. */
/* A publisher's state notification on a subscription it serves. The filter
 * is the core's form (type plus raw locations, internal forms included). */
typedef struct moq_publish_state_encode_args {
    bool     has_forward;
    bool     forward;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_filter;
    uint32_t filter;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
} moq_publish_state_encode_args_t;

/* A decoded state notification (profile -> core), same shape. */
typedef struct moq_decoded_state_notify {
    bool     has_forward;
    bool     forward;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_filter;
    uint32_t filter_type;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
} moq_decoded_state_notify_t;

/* The publisher's state notification on a subscription this side holds as
 * subscriber (kind SUBSCRIPTION with the subscriber role, or PUBLISH with the
 * subscriber role): records the new filter and Largest Object and surfaces
 * MOQ_EVENT_PUBLISH_STATE. Nothing is sent back. */
moq_result_t session_core_on_publish_state_notify(moq_session_t *s,
    moq_request_kind_t kind, int slot, const moq_decoded_state_notify_t *d);

/* Selected notification values must match the acknowledged owner state. */
bool session_publish_state_matches(const moq_publish_state_encode_args_t *args,
    bool forward, uint32_t filter_type, uint64_t start_group,
    uint64_t start_object, uint64_t end_group, uint64_t end_object);

typedef struct moq_request_update_ok_encode_args {
    uint64_t request_id;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
} moq_request_update_ok_encode_args_t;

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

/* The REQUEST_ERROR for a request refused because a local pool or capacity
 * is exhausted: the profile's capacity code, retryable at once where the
 * profile says so (session.c). */
moq_request_error_encode_args_t session_capacity_request_error(
    const moq_session_t *s, uint64_t request_id, const char *reason,
    size_t reason_len);

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
    /* outbound subscription-filter update (locations only meaningful
     * for the ABSOLUTE_* forms; the encoders omit them otherwise). */
    bool     has_filter;
    uint32_t filter;
    uint64_t filter_start_group;
    uint64_t filter_start_object;
    uint64_t filter_end_group;
    uint64_t filter_end_object;
    /* Lossless, independent millisecond carriers on typed UPDATE profiles. */
    bool     dt_has_object;
    uint64_t dt_object_ms;
    bool     dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    /* A fill request carried on the update, or NULL (see the subscribe args). */
    const moq_fill_input_t *fill;
    /* Who sends the update: MOQ_UPDATE_SENDER_*. */
    uint8_t  sender;
} moq_request_update_encode_args_t;

/* The sender of an outbound REQUEST_UPDATE: the subscriber of a subscription
 * (begun with SUBSCRIBE or PUBLISH), the sender of a PUBLISH updating its own
 * request, or the sender of a FETCH. Only profiles with requester_updates
 * encode the last two. */
#define MOQ_UPDATE_SENDER_SUBSCRIBER 0u
#define MOQ_UPDATE_SENDER_PUBLISHER  1u
#define MOQ_UPDATE_SENDER_FETCHER    2u

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
    /* The answer is to carry no Track Properties (INCLUDE_PROPERTIES = 0). */
    bool             omit_track_properties;
    bool             has_delivery_timeout;
    uint64_t         delivery_timeout_us;
    /* §9.8 exact per-type carriers from THIS message (raw wire ms; d16
     * writes the single value into both types). */
    bool     dt_has_object;
    uint64_t dt_object_ms;
    bool     dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    bool             has_filter;
    uint32_t         filter_type;
    uint64_t         start_group;
    uint64_t         start_object;
    uint64_t         end_group;
    uint64_t         end_object;    /* inclusive; OBJECT_END form only */
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code; /* non-zero: message-level token reject */
    bool             has_new_group_request;
    uint64_t         new_group_request;
    /* The message's fill request, as decoded by the profile: requested is
     * false on profiles without fills or when the parameter is absent. */
    moq_fill_input_t fill;
    bool has_rendezvous_timeout;
    uint64_t rendezvous_timeout_ms;
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
    /* §9.8 exact per-type carriers from THIS message (raw wire ms; d16
     * writes the single value into both types). */
    bool     dt_has_object;
    uint64_t dt_object_ms;
    bool     dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    /* SUBSCRIPTION_FILTER (§9.2.2.5 / §10.2.9): a replacement Subscription
     * Filter for the target subscription; omitted means unchanged. */
    bool     has_filter;
    uint32_t filter_type;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;    /* inclusive; OBJECT_END form only */
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
    /* The update's fill request (see moq_decoded_subscribe_t). */
    moq_fill_input_t fill;
} moq_decoded_request_update_t;

/* Prepare retained original-sender UPDATE effects for explicit initial
 * rejection. endpoint.has_request_id marks a complete admitted update. The
 * caller commits its ID/auth only after terminal-output preflight succeeds,
 * or aborts auth/staging on refusal; incomplete input has no effects. */
moq_result_t moq_request_stream_prepare_pending_reject(moq_session_t *s,
    const moq_request_stream_t *rs, moq_request_family_t family, bool peer_fin,
    moq_decoded_request_update_t *out);

/* rs is a borrowed suffix-only view. The optional initial ID/auth prefix is
 * visible during preparation but not published; commit prefix then out.
 * Private ID capacity may grow, while committed ranges/cache remain unchanged. */
moq_result_t moq_request_stream_prepare_pending_reject_after(moq_session_t *s,
    const moq_request_stream_t *rs, moq_request_family_t family, bool peer_fin,
    const moq_request_endpoint_t *prefix_id, const moq_auth_txn_t *prefix_auth,
    moq_decoded_request_update_t *out);

/* Automatic initial refusal while staging still owns all input. Caller frees
 * initial token staging and aborts auth unless MOQ_OK with the session open. */
moq_result_t session_core_reject_initial_request(moq_session_t *s,
    const moq_request_endpoint_t *endpoint, moq_auth_txn_t *auth,
    moq_request_family_t family, bool reject_drain,
    const moq_request_error_encode_args_t *error);

/* -- Decoded inbound PUBLISH_NAMESPACE (profile → session core) ------- */

typedef struct moq_decoded_publish_namespace {
    moq_bytes_t cluster_hop_path;
    bool cluster_has_route_cost;
    uint64_t cluster_route_cost;
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

/* `request_fin_observed` says the request bidi's FIN has been observed for the
 * buffered request now being dispatched -- on this transport call, or on an
 * earlier one whose refusal the generic staging owner retained in its
 * `rs.recv_fin` latch (`handle_request_stream_bytes()` passes that latch, not
 * the raw call FIN, so an empty re-feed still carries the fact). Draft-16
 * passes false: its control-channel route has no per-request bidi FIN.
 *
 * A pre-commit rejection creates no destination owner and installs no
 * `handoff_fin_pending` marker; staging's latch is its whole retry carrier. The
 * fact suppresses the drain reference and nothing else -- wire output and
 * staging retirement still follow stream correlation. */
moq_result_t session_core_on_publish_namespace(moq_session_t *s,
                                                moq_decoded_publish_namespace_t *d,
                                                bool request_fin_observed);

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
moq_result_t session_core_on_announce_torn_down(moq_session_t *s, int slot,
                                                moq_request_terminal_t term);

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
    /* The answer to this side's REQUEST_UPDATE after the initial response
     * (profiles with namespace updates): REQUEST_OK / REQUEST_ERROR. */
    MOQ_NS_RESP_UPDATE_OK      = 6,
    MOQ_NS_RESP_UPDATE_ERROR   = 7,
} moq_ns_sub_response_kind_t;

/* REQUEST_ERROR Retry Interval on the wire is the retry time plus one, zero
 * meaning "do not retry"; the public API carries the time itself. */
static inline uint64_t retry_interval_to_ms(uint64_t wire)
{
    return wire != 0 ? wire - 1 : 0;
}

typedef struct moq_decoded_ns_sub_response {
    moq_bytes_t cluster_hop_path;
    bool cluster_has_route_cost;
    uint64_t cluster_route_cost;
    moq_ns_sub_response_kind_t kind;
    size_t consumed;                 /* bytes consumed from input */

    /* For ERROR: */
    uint64_t error_code;
    uint64_t retry_interval;         /* REQUEST_ERROR wire Retry Interval:
                                      * milliseconds + 1; 0 = no retry */
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

    /* For UPDATE_OK: */
    bool     has_expires;
    uint64_t expires_ms;

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
    /* Publication-origin join (draft-18 5.1): the joined owner is a
     * PUBLISH-initiated subscription in the publication pool. Exactly one of
     * joining_sub_slot / joining_pub_slot is >= 0 for a joining fetch. */
    int              joining_pub_slot;
    uint8_t          subscriber_priority;
    uint8_t          group_order;
    /* The answer is to carry no Track Properties (INCLUDE_PROPERTIES = 0). */
    bool             omit_track_properties;
    moq_resolved_token_t tokens[MOQ_DECODED_MAX_TOKENS];
    bool             token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t           token_count;
    moq_auth_txn_t   auth_txn;
    uint64_t         auth_reject_code; /* non-zero: message-level token reject */
    bool             request_fin;      /* the request bidi FIN'd in this chunk */
    /* A range given relative to the track's Largest Object, resolved by the
     * core against its track history before the request is surfaced:
     * start_relative puts the start `relative_offset` groups before the next
     * group (0 is the next group, 1 the current one), start_next_object puts
     * it just past the Largest Object, end_at_largest ends the range at the
     * Largest Object. A start past the Largest Object, or a track with no
     * objects, refuses the fetch with INVALID_RANGE. */
    bool             start_relative;
    uint64_t         relative_offset;
    bool             start_next_object;
    bool             end_at_largest;
    /* The profile's FETCH carries its range as a Location Filter, so the
     * request has the details moq_fetch_request_options_t surfaces:
     * has_location / location_* are the filter as sent (the core's filter
     * record: filter type, start, absolute end group, inclusive end object),
     * has_fill_timeout / fill_timeout_ms its FILL_TIMEOUT. */
    bool             has_options;
    bool             has_location;
    uint32_t         location_type;
    uint64_t         location_start_group;
    uint64_t         location_start_object;
    uint64_t         location_end_group;
    uint64_t         location_end_object;
    bool             has_fill_timeout;
    uint64_t         fill_timeout_ms;
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
    uint8_t        malformed;      /* MOQ_MALFORMED_*; property violations */
    moq_obj_gaps_t gaps;
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
    /* A standalone range in its semantic form, replacing start/end, and a
     * FILL_TIMEOUT (only on a profile with fetch_request_options); NULL
     * sends start/end. */
    const moq_location_filter_cfg_t *location;
    bool                    has_fill_timeout;
    uint64_t                fill_timeout_ms;
    /* INCLUDE_PROPERTIES as the caller selected it (only on a profile with
     * include_properties_request). */
    bool                    has_include_properties;
    bool                    include_properties;
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
moq_result_t fetch_finish_response(moq_session_t *s, int slot);
moq_result_t fetch_process_buffered_request(moq_session_t *s, int slot);

/* -- Fill children (session_fill.c) ----------------------------------- */

/* Per-slot state the ownership table deliberately leaves to the session. */
typedef struct moq_fill_side {
    moq_fill_t        handle;            /* public handle of the live generation */
    moq_fill_input_t  input;             /* effective request: inherited values filled in */
    bool              request_published; /* publisher: FILL_REQUEST surfaced */
    uint32_t          error_domain;      /* terminal error domain/code once known */
    uint64_t          error_code;
    moq_fetch_prior_object_t prior;      /* publisher write cursor */
    uint8_t           header_len;        /* publisher: FETCH_HEADER bytes on the
                                            bound stream (reset reliable size) */
} moq_fill_side_t;

/* Pool lifecycle: init records the capacity (0 admits nothing); the storage
 * is a separate allocation made by the first admission, so a session that
 * never sees a fill allocates nothing for them. */
moq_result_t fill_pool_init(moq_session_t *s, size_t cap);
void fill_pool_destroy(moq_session_t *s);

/* Drive every owed effect whose capacity is now available: a RESET or STOP a
 * child owes its bound stream, a FILL_REQUEST not yet surfaced, the FILL_TERMINAL
 * (exactly once), stream accounting and reclamation. Idempotent; a refusal
 * for want of capacity retains the obligation. Runs from the advancing-call
 * preamble, after every fill operation and when a poll frees capacity. */
void fill_service(moq_session_t *s);

/* Admission of a locally requested child (subscriber role): prepare checks the
 * request and the pool and reserves nothing; commit records the child and
 * pushes FILL_CREATED (the caller checked event capacity: fill_local_needs_event).
 * `parent` names the subscription (family/slot/generation of the live entry);
 * the inherited values are the parent's effective priority, group order and
 * filter at request time. */
moq_result_t fill_local_prepare(moq_session_t *s, moq_fill_parent_t parent,
                                uint64_t initiating_id, const moq_fill_input_t *req,
                                uint8_t inherit_priority, uint8_t inherit_group_order,
                                uint32_t inherit_filter, uint64_t inherit_sg,
                                uint64_t inherit_so, uint64_t inherit_eg,
                                uint64_t inherit_eo, moq_fill_reservation_t *out,
                                moq_fill_input_t *effective);
void fill_local_commit(moq_session_t *s, const moq_fill_reservation_t *r,
                       const moq_fill_input_t *effective);

/* Admission of a peer-requested child (publisher role). The same prepare /
 * commit split; the delivery is resolved later against a Largest snapshot
 * (fill_control_resolved) and FILL_REQUEST is surfaced by fill_service.
 * MOQ_ERR_REQUEST_BLOCKED means the pool has no slot for the child; the
 * effective request is still filled in, and the caller answers the fill on
 * the wire instead (fill_open_failed). Other refusals leave nothing changed. */
moq_result_t fill_inbound_prepare(moq_session_t *s, moq_fill_parent_t parent,
                                  uint64_t initiating_id, const moq_fill_input_t *req,
                                  uint8_t inherit_priority, uint8_t inherit_group_order,
                                  uint32_t inherit_filter, uint64_t inherit_sg,
                                  uint64_t inherit_so, uint64_t inherit_eg,
                                  uint64_t inherit_eo, moq_fill_reservation_t *out,
                                  moq_fill_input_t *effective);
void fill_inbound_commit(moq_session_t *s, const moq_fill_reservation_t *r,
                         const moq_fill_input_t *effective);
/* Whether a fill the pool could not admit would have opened a stream: its
 * effective range resolved against the response's Largest snapshot and the
 * subscription's paused state, exactly as an admitted child resolves. */
bool fill_unserved_opens_stream(const moq_fill_input_t *effective, bool has_largest,
                                uint64_t largest_group, uint64_t largest_object,
                                bool paused);
/* Fail a fill on the wire: a fill fetch stream carrying the FETCH_HEADER with
 * the initiating request id, reset immediately after it. Two actions, which
 * the caller reserved with its response. The stream is counted in
 * *streams_opened (saturating), as any fill stream the session opens. */
void fill_open_failed(moq_session_t *s, uint64_t initiating_id,
                      uint8_t subscriber_priority, uint64_t *streams_opened);
/* The Stream Count PUBLISH_DONE carries: the application's own data streams
 * plus the fill streams the session opened, saturating to the profile's
 * unknown sentinel; a sentinel from the application is sent unchanged. */
uint64_t fill_stream_count(const moq_session_t *s, uint64_t app_count,
                           uint64_t fill_streams_opened);

/* The initiating request was answered: accepted (with the Largest snapshot
 * the answer carried or was computed from, and whether the subscription is
 * paused at that point) or rejected with error_code. Resolves the child's
 * delivery on acceptance; a child whose stream already arrived keeps it. */
void fill_control_resolved(moq_session_t *s, moq_fill_parent_t parent,
                           uint64_t initiating_id, bool accepted, uint64_t error_code,
                           bool has_largest, uint64_t largest_group,
                           uint64_t largest_object, bool paused);
void fill_control_resolved_receipt(moq_session_t *s, moq_fill_parent_t parent,
    uint64_t initiating_id, bool accepted, uint64_t error_code,
    bool has_largest, uint64_t largest_group, uint64_t largest_object, bool paused,
    moq_pub_fill_receipt_t *receipt);

/* Parent teardown. The preflight counts the actions (RESET on the publisher,
 * STOP on the subscriber) the parent's bound children need so the caller can
 * reserve them with its own; the cancel marks every child and queues what
 * was reserved. error_domain/code describe the cause for the terminals. */
size_t fill_parent_actions_needed(const moq_session_t *s, moq_fill_parent_t parent);
void fill_parent_cancel(moq_session_t *s, moq_fill_parent_t parent,
                        uint32_t error_domain, uint64_t error_code);
/* The same cancellation, queuing stream actions only while more than `keep`
 * action slots stay free and leaving the general service alone, so a caller
 * that reserved slots for its own messages keeps them. What does not fit is
 * retained by the children and issued as actions drain. */
void fill_parent_cancel_keep(moq_session_t *s, moq_fill_parent_t parent,
                             uint32_t error_domain, uint64_t error_code,
                             size_t keep);

static inline moq_fill_parent_t fill_parent_sub(const moq_session_t *s, size_t slot)
{
    moq_fill_parent_t p = { MOQ_FILL_PARENT_SUBSCRIBE, slot, s->subs[slot].generation };
    return p;
}
static inline moq_fill_parent_t fill_parent_pub(const moq_session_t *s, size_t slot)
{
    moq_fill_parent_t p = { MOQ_FILL_PARENT_PUBLISH, slot, s->publishes[slot].generation };
    return p;
}

/* Receive-side binding of a fetch response stream whose header named no
 * fetch: binds it to the subscriber-role child with that initiating id and
 * returns MOQ_OK, MOQ_ERR_STALE_HANDLE when no such child exists, or
 * MOQ_ERR_PROTO when the child can take no stream (its range resolved to none,
 * or it already has one). On MOQ_OK the rx carries the fill binding and the
 * parent handle for stream counting. */
moq_result_t fill_rx_bind(moq_session_t *s, moq_rx_stream_t *rx, uint64_t initiating_id);
/* Stream signals on a bound fill stream (subscriber role). */
void fill_rx_peer_fin(moq_session_t *s, const moq_rx_stream_t *rx);
void fill_rx_peer_reset(moq_session_t *s, const moq_rx_stream_t *rx, uint64_t error_code);
/* The session itself stopped the stream (budget or expiry): the child is
 * cancelled locally and its STOP recorded. */
void fill_rx_local_stop(moq_session_t *s, const moq_rx_stream_t *rx);
/* The stopped stream was retired by the transport with no peer FIN or RESET
 * (local receive retirement): the child's stream is over, with the reason
 * its cancellation recorded. */
void fill_rx_local_retired(moq_session_t *s, const moq_rx_stream_t *rx);
/* The identity a bound rx's fill events carry (zeroed when the child is gone). */
void fill_rx_identity(const moq_session_t *s, const moq_rx_stream_t *rx,
                      moq_fill_t *fill, moq_subscription_t *sub,
                      moq_publication_t *pub, uint64_t *initiating_id);
/* True when the child still accepts objects (not cancelled or ended). */
bool fill_rx_delivering(const moq_session_t *s, const moq_rx_stream_t *rx);
/* Publisher side: the peer's STOP_SENDING on a bound fill stream. Reserves the
 * RESET it owes (MOQ_ERR_WOULD_BLOCK with nothing changed when it cannot) and
 * returns MOQ_ERR_STALE_HANDLE when no child owns the stream. */
moq_result_t fill_on_data_stop(moq_session_t *s, moq_stream_ref_t ref, uint64_t error_code);
/* Receive-side helper (session_receive.c): put a bound fill stream into the
 * stopped/discard state after its STOP_DATA was queued. */
void rx_fill_stopped(moq_session_t *s, moq_stream_ref_t ref);

/* Fetcher-role completion: the FETCH holds both its FETCH_OK and its data
 * stream's FIN. `_fits` reports whether the commit can run now -- one event,
 * and on stream-correlated profiles one action for the FIN of the request
 * stream's local half plus, while the publisher's half is still open, one
 * drain slot -- beyond `extra_events` the caller reserves for itself.
 * `_commit` then queues FETCH_COMPLETE, closes the request half, keeps the
 * drain reference that absorbs the publisher's FIN or RESET, and frees the
 * entry. It cannot fail after a true `_fits`. */
bool fetch_fetcher_completion_fits(const moq_session_t *s, int slot,
                                   size_t extra_events);
moq_result_t fetch_fetcher_completion_commit(moq_session_t *s, int slot);

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
    /* LARGEST_OBJECT / EXPIRES advertised by the publisher:
     * largest is max-merged into the entry's history at commit AND surfaced. */
    bool             has_largest;
    uint64_t         largest_group;
    uint64_t         largest_object;
    bool             has_expires;
    uint64_t         expires_ms;
    /* The publisher's initial subscription settings carried on the PUBLISH
     * (profiles whose PUBLISH has them); absent fields are the defaults. */
    bool             has_subscriber_priority;
    uint8_t          subscriber_priority;
    bool             has_group_order;
    uint8_t          group_order;
    bool             has_filter;
    uint32_t         filter_type;
    uint64_t         filter_start_group;
    uint64_t         filter_start_object;
    uint64_t         filter_end_group;
    uint64_t         filter_end_object;
    /* Decoded value-only initial parameters; populated only by D22. */
    moq_publish_initial_parameters_t initial;
} moq_decoded_publish_t;

typedef struct moq_decoded_publish_ok {
    uint64_t request_id;
    int      target_slot;
    bool     has_deferred_param_error;
    const char *deferred_param_reason;
    /* The response carried no subscription parameters: forward, priority and group
     * order below are placeholders and the publisher keeps the settings it
     * put on the PUBLISH. */
    bool     params_absent;
    bool     forward;
    uint8_t  subscriber_priority;
    uint8_t  group_order;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_ms;
    /* §9.8 exact per-type carriers from THIS message (raw wire ms; d16
     * writes the single value into both types). */
    bool     dt_has_object;
    uint64_t dt_object_ms;
    bool     dt_has_subgroup;
    uint64_t dt_subgroup_ms;
    bool     has_expires;
    uint64_t expires_ms;
    bool     has_new_group_request;
    uint64_t new_group_request;
    /* Raw SUBSCRIPTION_FILTER the subscriber chose; surfaced
     * raw on the event, resolved internally against the registry. */
    bool     has_filter;
    uint32_t filter_type;
    uint64_t filter_start_group;
    uint64_t filter_start_object;
    uint64_t filter_end_group;
    uint64_t filter_end_object;    /* inclusive; OBJECT_END form only */
} moq_decoded_publish_ok_t;

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

/* `request_fin_observed` carries the same fact as the announcement handler
 * above, on the same terms. */
moq_result_t session_core_on_publish(moq_session_t *s,
                                      moq_decoded_publish_t *d,
                                      bool request_fin_observed);
moq_result_t session_core_on_publish_ok(moq_session_t *s,
                                         const moq_decoded_publish_ok_t *d);
/* Surface a PUBLISH error (REQUEST_ERROR rejecting our outbound PUBLISH).
 * free_now frees the entry immediately (responses on the shared control
 * channel); when false the entry is marked draining and kept so the request bidi
 * can absorb its trailing FIN, and our send half is closed reciprocally
 * (stream-correlated profiles). */
/* The publisher's REQUEST_UPDATE on its own established PUBLISH, read by the
 * subscriber: answers REQUEST_OK (REQUEST_ERROR for an unsupported parameter
 * or a rejected token), surfaces MOQ_EVENT_PUBLISH_UPDATED with the carried
 * fields, and records the publisher's new Forward state and priority. */
moq_result_t session_core_on_publication_update_by_publisher(
    moq_session_t *s, moq_decoded_request_update_t *d);

/* The fetcher's REQUEST_UPDATE on a FETCH this side serves: applies the
 * priority to the fetch and its data-stream scheduling and answers
 * REQUEST_OK. A rejected update answers REQUEST_ERROR and resets the fetch
 * data stream when one is open. */
moq_result_t session_core_on_fetch_request_update(
    moq_session_t *s, moq_decoded_request_update_t *d);

moq_result_t session_core_on_publish_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now,
    const moq_decoded_redirect_t *redirect);

int pub_resolve_handle(const moq_session_t *s, moq_publication_t h);
int pub_find_free(moq_session_t *s);

/*
 * Occupancy-list maintenance. A slot must be linked exactly when it becomes
 * allocated and unlinked exactly when it is freed; the sweeps then walk the
 * list instead of the pool. `*_occ_unlink` also advances any live sweep cursor
 * that is parked on the slot being removed, so a suspended sweep resumes at
 * the successor rather than following a stale link.
 */
/* First slot of an occupancy list, or `cap` when the list is empty. Every
 * scan cursor that used to start at slot 0 starts here instead. */
static inline size_t moq_occ_first(int32_t head, size_t cap)
{
    return head >= 0 ? (size_t)head : cap;
}

void pub_occ_link(moq_session_t *s, size_t slot);
void pub_occ_unlink(moq_session_t *s, size_t slot);
void sub_occ_link(moq_session_t *s, size_t slot);
void sub_occ_unlink(moq_session_t *s, size_t slot);
void sg_occ_link(moq_session_t *s, size_t slot);
void sg_occ_unlink(moq_session_t *s, size_t slot);
void rx_occ_link(moq_session_t *s, size_t slot);
void rx_occ_unlink(moq_session_t *s, size_t slot);
void fetch_occ_link(moq_session_t *s, size_t slot);
void fetch_occ_unlink(moq_session_t *s, size_t slot);

void pub_free_entry(moq_session_t *s, int slot);
/* Alias resolution for subscriber-role publications, independent of Forward
 * State (streams bind/count regardless; prohibited objects are dropped at
 * the delivery gates, never routed as unknown-alias). */
int pub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias);
/* Track-alias collision checks shared with the subscription paths (the data
 * alias namespace is shared between publications and subscriptions).
 * peer_track_alias_in_use covers the aliases the peer assigned: established
 * subscriber-role subscriptions and established or pending subscriber-role
 * publications (inbound data alias); _outbound covers non-free
 * publisher-role publications (outbound data alias). */
bool peer_track_alias_in_use(moq_session_t *s, uint64_t alias);
bool pub_outbound_alias_in_use(moq_session_t *s, uint64_t alias);
/* Count a completed data stream toward a subscriber-role publication's
 * PUBLISH_DONE Stream Count, finalizing a deferred PUBLISH_FINISHED when the
 * count is reached IN THE GATED STATE (once EXPIRED, only records -- §9.8).
 * pub_reap_deferred_dones drives count-satisfied retries, deadline firing,
 * and the EXPIRED scan/stop/rescan/finalize loop. */
void pub_note_stream_processed(moq_session_t *s, moq_publication_t pub);
bool pub_reap_deferred_dones_resumable(moq_session_t *s, uint32_t *budget);

/* Count a completed data stream toward a subscriber-role subscription's
 * Stream Count, finalizing a deferred SUBSCRIBE_DONE when the count is
 * reached. sub_reap_deferred_dones retries a finalize that could not be
 * queued earlier (event queue full). Mirrors the publication pair above. */
void sub_note_stream_processed(moq_session_t *s, moq_subscription_t sub);

/* §9.8 shared timeout-property scan (session.c): dispatches to the pure
 * per-profile scanner. strict_local selects LOCAL wrapper semantics
 * (structural failure -> error); inbound callers pass false (d16 stays
 * lenient-opaque on structure; semantic violations error in both modes).
 * Returns <0 with *out zeroed on failure. */
/* §9.8 constants: finite library default for the terminal wait, and the
 * bounded retry interval for expiry work blocked on action/event capacity. */
#define MOQ_DONE_WAIT_DEFAULT_US   (30ull * 1000ull * 1000ull)
#define MOQ_DONE_EXPIRY_RETRY_US   (100ull * 1000ull)

/* §9.8 EXPIRED sweep (session_receive.c): stop-and-discard all live rx
 * bound to the handle; MOQ_OK = none remain, WOULD_BLOCK = action-blocked. */
#ifdef MOQ_SESSION_SWEEP_TESTING
extern uint64_t session_stop_scan_entries;

/*
 * Exact-work probe counters for the advancing-call preamble.
 *
 * OBSERVATIONAL ONLY: each is a plain increment at one loop-body entry or one
 * charged transition; none participates in any control decision, so removing
 * them cannot change behaviour. Compiled solely into moq-core-test-internals,
 * exactly like session_stop_scan_entries above.
 *
 * A PROBE is one visit to a candidate owner slot -- the unit that scales with
 * configured pool capacity when a scan is capacity-bounded rather than
 * ownership-bounded. A CHARGED transition is work performed on an owner that
 * actually had work. They are counted separately, and each stage keeps its own
 * counter, so a scan moving from one stage to another cannot hide inside an
 * aggregate.
 *
 * Tests assert DELTAS around one declared advancing call: several tests in one
 * binary accumulate into them.
 */
extern uint64_t session_work_sg_reap_probes;    /* subgroup slots visited, terminal reap */
extern uint64_t session_work_sg_reap_charged;   /* subgroups actually reaped */
extern uint64_t session_work_sg_deadline_probes;/* subgroup slots visited, deadline recompute */
extern uint64_t session_work_pub_reap_probes;   /* publication slots visited, deferred-done reap */
extern uint64_t session_work_pub_reap_pending;  /* publication owners found with deferred work */
extern uint64_t session_work_sub_reap_probes;   /* subscription slots visited, deferred-done reap */
extern uint64_t session_work_sub_reap_pending;  /* subscription owners found with deferred work */
extern uint64_t session_work_retry_sub_probes;  /* subscription slots visited, retry-deadline recompute */
extern uint64_t session_work_retry_pub_probes;  /* publication slots visited, retry-deadline recompute */
extern uint64_t session_work_rx_probes;         /* rx slots visited, bound-stream scan */

/*
 * The remaining configured-capacity scans transitively reachable from
 * session_advance_sweep(). Each is a distinct named site: a cleanup scan must
 * never fold into the bound-stream counter, or a correction that moved work
 * from one into the other would cancel in an aggregate.
 */
extern uint64_t session_work_close_sg_probes;   /* subgroup slots, close_with_error */
extern uint64_t session_work_close_rx_probes;   /* rx slots, free_rx_stream_bufs */
extern uint64_t session_work_fwd_pending_probes;/* subscription slots, session_has_forwarding_pending_subscriber */
extern uint64_t session_work_deferred_rx_probes;/* rx slots, session_discard_deferred_streams */
extern uint64_t session_work_join_probes;       /* fetch slots, session_core_discard_pending_joins */
extern uint64_t session_work_join_resolve_probes; /* fetch slots, session_core_pending_joins_can_resolve */
extern uint64_t session_work_join_reject_probes;  /* fetch slots, session_core_reject_pending_joins */
extern uint64_t session_work_staged_probes;     /* staged-datagram slots, staged_clear_unresolved */

/*
 * NON-CAPACITY loops in the same reachable set: one counter per LOOP SITE, the
 * same rule the capacity scans follow. Each is bounded by live occupancy, by a
 * caller-supplied count, or by the live index population -- never by a
 * configured pool size -- so they are excluded from the capacity-invariance
 * comparator and carry an absolute declared model instead.
 */
extern uint64_t session_work_queued_action;      /* queued actions visited, close-time decref */
extern uint64_t session_work_queued_event;       /* queued events visited, close-time decref */
extern uint64_t session_work_tomb_probes;        /* live fetch-cancel tombstones scanned */
extern uint64_t session_work_auth_staging;       /* auth staging slots freed (argument-bounded) */
extern uint64_t session_work_index_find;         /* moq_index_find probe steps */
extern uint64_t session_work_index_find_calls;   /* moq_index_find invocations */
extern uint64_t session_work_index_rm_calls;     /* moq_index_remove invocations */
extern uint64_t session_work_index_rm_search;    /* moq_index_remove search steps */
extern uint64_t session_work_index_rm_backshift; /* moq_index_remove backshift steps */

/*
 * CONTROL loops: bounded by active work, not by capacity and not by occupancy.
 * Counted so the image can claim completeness without describing them.
 */
extern uint64_t session_work_catchup_passes;     /* session_advance_sweep catch-up iterations */
extern uint64_t session_work_rx_rescan_passes;   /* bound-stream scan rescan passes */
/* One charged bound-stream terminal transition: the unit the sweep budget
 * spends, taken immediately before the STOP attempt. It counts the ATTEMPT, so
 * a stop that action capacity refuses is still one charged transition -- which
 * is what the budget itself charges. */
extern uint64_t session_work_rx_charged;
#endif
moq_result_t session_stop_bound_streams_resumable(moq_session_t *s,
                                                  moq_subscription_t sub,
                                                  moq_publication_t pub,
                                                  uint32_t *budget);

moq_result_t session_scan_dt_props(const moq_session_t *s,
                                   const uint8_t *data, size_t len,
                                   bool strict_local, moq_dt_scan_t *out);
bool sub_reap_deferred_dones_resumable(moq_session_t *s, uint32_t *budget);

/* Surface PUBLISH_DONE (the publisher's terminal on the subscriber side).
 * free_now frees the entry immediately (draft-16, control channel); when false
 * the entry is marked draining and kept so the request bidi can absorb its
 * trailing FIN, and our send half is closed reciprocally (stream-correlated). */
moq_result_t session_core_on_publish_done(moq_session_t *s,
                                           const moq_decoded_publish_done_t *d,
                                           bool free_now);
/* The subscriber ended a publication this side publishes. `reserve_after`
 * action slots stay free for what the caller queues next; every subgroup
 * stream still open is reset, or owes its reset when no slot is left. */
moq_result_t session_core_on_publish_unsubscribed(moq_session_t *s, int slot,
                                                  moq_request_terminal_t term,
                                                  size_t reserve_after);
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
    /* The request asked for an answer without Track Properties
     * (INCLUDE_PROPERTIES = 0). */
    bool                      omit_track_properties;
} moq_decoded_track_status_request_t;

typedef struct moq_track_status_encode_args {
    uint64_t        request_id;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* INCLUDE_PROPERTIES as the caller selected it (only on a profile with
     * include_properties_request). */
    bool                    has_include_properties;
    bool                    include_properties;
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
moq_result_t ts_receive_terminal(moq_session_t *s, int slot,
                                moq_request_terminal_t term);
moq_result_t ts_flush_terminal(moq_session_t *s, int slot);
void ts_apply_deferred_signals(moq_ts_entry_t *e);
moq_result_t ts_queue_cancellation(moq_session_t *s, int slot);
void ts_retry_pending(moq_session_t *s);

/* Per-family entry free, shared with the request-stream GOAWAY handler. */
void ann_free_entry(moq_session_t *s, size_t slot);
void ann_validate_response(moq_session_t *s, int slot);
void ann_retry_pending(moq_session_t *s);
moq_result_t ann_receive_goaway(moq_session_t *s, int slot,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms);
void ns_sub_free_entry(moq_session_t *s, size_t slot);

/* Surface MOQ_EVENT_REQUEST_GOAWAY for a request migrated by a request-stream
 * GOAWAY (§10.4): free the request entry, close our send half if still open, and
 * -- unless the peer's FIN has already been observed -- drain the bidi.
 * Leaves data streams alone (graceful migration). The caller resolves
 * family+slot from the bidi's stream ref.
 *
 * `peer_fin_observed` is the owner's cumulative fact, not a raw per-call flag:
 * the draft-18 dispatchers receive it from the owner's durable FIN latch, and
 * the namespace-subscription route passes `pending_fin`. Once the peer has
 * closed its send half nothing can arrive late, so the strict reference has
 * nothing to absorb and is not reserved -- output and retirement are
 * unaffected. */
moq_result_t session_core_on_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    moq_stream_ref_t ref, const uint8_t *uri, size_t uri_len,
    uint64_t timeout_ms, bool peer_fin_observed);

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
bool request_stream_is_publisher_subscription(moq_session_t *s,
                                              moq_stream_ref_t ref);
bool request_goaway_already_sent(moq_session_t *s, moq_stream_ref_t ref);

/* Retire a request migrated by a per-request GOAWAY (sent or received): when
 * `need_drain`, drain the request bidi (FIN/RESET/STOP retire it; messages
 * are discarded and a second GOAWAY closes 0x3); either way free or
 * tombstone the entry, leaving data streams intact. The caller decides
 * `need_drain` -- an already-observed peer FIN leaves nothing to absorb -- and
 * has reserved the drain-ref slot when it is true. Retirement itself is
 * unconditional. */
void request_goaway_retire(moq_session_t *s, moq_request_family_t family,
                           int slot, moq_stream_ref_t ref, bool need_drain);

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
    moq_subscribe_tracks_options_t options;
} moq_decoded_subscribe_tracks_request_t;

typedef struct moq_decoded_publish_blocked {
    int             target_slot;
    moq_namespace_t track_namespace_suffix;          /* parts → suffix_parts */
    moq_bytes_t     suffix_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_bytes_t     track_name;
} moq_decoded_publish_blocked_t;

/* A draft-22 REQUEST_UPDATE of a namespace request (§9.5): the selected
 * parameters its family permits. Spans borrow the message body. */
typedef struct moq_decoded_namespace_update {
    moq_bytes_t cluster_hop_path;
    bool cluster_has_route_cost;
    uint64_t cluster_route_cost;
    uint64_t        request_id;
    bool            has_prefix;
    moq_namespace_t prefix;
    moq_bytes_t     prefix_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    bool            has_forward;
    bool            forward;
} moq_decoded_namespace_update_t;

typedef struct moq_namespace_update_encode_args {
    moq_cluster_metadata_t cluster;
    uint64_t                request_id;
    moq_request_family_t    family;
    bool                    has_prefix;
    moq_namespace_t         prefix;
    bool                    has_forward;
    bool                    forward;
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_namespace_update_encode_args_t;

moq_result_t session_core_on_subscribe_tracks(moq_session_t *s,
    moq_decoded_subscribe_tracks_request_t *d, bool request_fin);
moq_result_t session_core_on_subscribe_tracks_ok(moq_session_t *s, int slot);
moq_result_t session_core_on_subscribe_tracks_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len,
    const moq_decoded_redirect_t *redirect);
moq_result_t session_core_on_publish_blocked(moq_session_t *s, int slot,
    const moq_decoded_publish_blocked_t *d);
moq_result_t session_core_on_subscribe_tracks_torn_down(moq_session_t *s, int slot,
                                                        moq_request_terminal_t term);
/* §10.9.1: a REQUEST_UPDATE on a SUBSCRIBE_TRACKS bidi is unsupported here;
 * REQUEST_ERROR + FIN on the bidi, commit the update's request id, terminate. */
moq_result_t session_core_on_track_sub_update_rejected(moq_session_t *s,
    int slot, const moq_request_endpoint_t *uep);

int track_sub_resolve_handle(moq_session_t *s, moq_track_sub_handle_t h);
int track_sub_find_free(moq_session_t *s);
void track_sub_free_entry(moq_session_t *s, int slot);
bool track_sub_prefix_conflicts(moq_session_t *s, const moq_bytes_t *parts,
                                size_t count, moq_track_sub_role_t role,
                                int exclude_slot);

/* -- Subscribe handlers (session_subscribe.c) ---------------------- */

/* Handle a decoded inbound SUBSCRIBE. reserved_slot < 0 means find a free slot
 * (profiles that carry requests on the shared control channel); reserved_slot
 * >= 0 commits into an already-reserved slot (stream-correlated profiles, where
 * the request bidi was reserved as MOQ_SUB_RECVING_REQUEST while its bytes were
 * buffered). */
moq_result_t session_core_on_subscribe(moq_session_t *s,
                                        moq_decoded_subscribe_t *d,
                                        int reserved_slot,
                                        bool request_fin);

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

/* -- Answering a subscriber's update (subscriptions and publications) --
 * Shared by the subscription (session_subscribe.c) and the PUBLISH-initiated
 * subscription (session_publish.c) paths, so both answer, coalesce and fail
 * an update the same way. */

/* What accepting an update needs, admitted before anything is queued. */
typedef struct moq_update_plan {
    /* The one registry snapshot: it feeds both the REQUEST_OK Largest Object
     * and the stored resolved window. */
    bool     snap_has;
    uint64_t snap_group;
    uint64_t snap_object;
    bool     has_fill;      /* the fill child is reserved */
    bool     fail_fill;     /* no pool slot: the fill fails on the wire */
    moq_fill_reservation_t fres;
    moq_fill_input_t       feff;
} moq_update_plan_t;

void session_update_from_decoded(const moq_decoded_request_update_t *d, moq_sub_update_t *u);
/* Preflight the complete options/token projection and stage selected values.
 * Legacy profiles return NULL options. Caller rolls scratch back on refusal. */
moq_result_t session_update_stage_options(moq_session_t *s,
    const moq_sub_update_t *u, const moq_resolved_token_t *tokens,
    size_t token_count, moq_subscription_update_options_t **out);

moq_result_t session_update_freeze_fill(const moq_sub_update_snapshot_t *v,
    moq_update_plan_t *p);
moq_result_t session_update_resolution_observe(const moq_session_t *s,
    const moq_owed_update_t *o, uint64_t update_request_id,
    moq_update_resolution_t *out, size_t out_size);
/* Forget the owed answer and free the ids it covered. */
void session_owed_update_clear(moq_session_t *s, moq_owed_update_t *o);
/* Observe the cumulative record. Caller validates the session, typed owner
 * and optional filter-output size before this writes any output. */
moq_result_t session_owed_update_observe(const moq_owed_update_t *o,
    uint64_t *out_update_request_id, bool *out_has_filter,
    moq_location_filter_cfg_t *out_filter, size_t out_filter_size);
/* The id of the i-th update of the owed run, in arrival order. */
uint64_t session_owed_update_id(const moq_owed_update_t *o, uint32_t i);
/* The owed run already holds as many updates as this side advertised it
 * would take on the stream (§9.1.7); never true on a profile that
 * advertises no bound. */
bool session_owed_update_at_bound(const moq_session_t *s, const moq_owed_update_t *o);
/* The owed record `u` would leave: `u` itself, or folded into the run
 * already owed. Grows the older-id storage; changes nothing else. */
moq_result_t session_owed_update_stage(moq_session_t *s, moq_owed_update_t *o,
                                       const moq_sub_update_t *u,
                                       moq_sub_update_t *cum, uint32_t *count);
/* Commit what session_owed_update_stage computed. Cannot fail. */
void session_owed_update_record(moq_owed_update_t *o, const moq_sub_update_t *cum,
                                uint32_t count);
moq_result_t session_update_encode_ok(moq_session_t *s, uint64_t request_id,
                                      const moq_update_plan_t *p,
                                      uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t session_update_accept_fits(moq_session_t *s, const moq_owed_update_t *o,
                                        const moq_update_plan_t *p);
moq_result_t session_update_reject_cfg_check(const moq_session_t *s,
                                             const moq_reject_subscription_update_cfg_t *cfg);
void session_update_reject_args(const moq_owed_update_t *o,
                                const moq_reject_subscription_update_cfg_t *cfg,
                                moq_request_error_encode_args_t *ea);
/* The number of REQUEST_ERRORs a failed update is answered with when
 * `n_before` owed updates fail with it. */
size_t session_update_error_count(const moq_session_t *s, uint32_t n_before);
/* The k-th of those `n_err` REQUEST_ERRORs: the owed run's earlier ids by
 * their own id, the last one as `ea` names it. */
moq_result_t session_update_encode_error(moq_session_t *s, const moq_owed_update_t *o,
                                         const moq_request_error_encode_args_t *ea,
                                         size_t k, size_t n_err,
                                         uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t session_update_fail_len(moq_session_t *s, const moq_owed_update_t *o,
                                     uint64_t done_request_id,
                                     uint64_t done_stream_count,
                                     const moq_request_error_encode_args_t *ea,
                                     uint32_t n_before, size_t *err_total,
                                     size_t *done_len);
/* The Stream Count of a PUBLISH_DONE the session sends on its own: every
 * data stream it opened for the request, subgroup and fill streams alike
 * (zero when it opened none), saturating to the profile's unknown
 * sentinel. */
uint64_t session_done_stream_count(const moq_session_t *s,
                                   uint64_t subgroup_streams_opened,
                                   uint64_t fill_streams_opened);
moq_result_t session_core_on_subscribe_update_ok(moq_session_t *s, int slot,
    bool has_largest, uint64_t largest_group, uint64_t largest_object,
    bool has_expires, uint64_t expires_ms);
moq_result_t session_core_on_subscribe_update_error(moq_session_t *s, int slot,
                                                    uint64_t error_code);
/* Publication-update ack: clears the pending update on a PUBLISH-established
 * subscription and emits MOQ_EVENT_PUBLICATION_UPDATE_OK exactly once (queue-full
 * WOULD_BLOCK before clearing). Mirrors the subscription-update-ok hook. */
moq_result_t session_core_on_fetch_update_ok(moq_session_t *s, int slot);
moq_result_t session_core_on_fetch_update_error(moq_session_t *s, int slot,
    uint64_t error_code, const uint8_t *reason, size_t reason_len);
moq_result_t session_core_on_own_publish_update_error(moq_session_t *s, int slot,
    uint64_t error_code, const uint8_t *reason, size_t reason_len);
moq_result_t session_core_on_publish_update_ok(moq_session_t *s, int slot,
    bool has_largest, uint64_t largest_group, uint64_t largest_object,
    bool has_expires, uint64_t expires_ms);
bool unsub_tomb_add(moq_session_t *s, uint64_t request_id);
bool unsub_tomb_consume(moq_session_t *s, uint64_t request_id);
/* True when a reply tombstone holds `request_id`; consumes nothing. */
bool unsub_tomb_contains(const moq_session_t *s, uint64_t request_id);
/* True when `request_id` is one of the inbound updates a subscription or a
 * publication still owes its answer to (every id of the owed run). */
bool session_owed_update_covers(const moq_session_t *s, uint64_t request_id);

/* Locally-cancelled FETCH request-id grace cache. add() dedupes and drops the
 * oldest entry when full (never fails). contains() peeks without removing;
 * consume() removes and reports presence. */
void fetch_cancel_tomb_add(moq_session_t *s, uint64_t request_id);
bool fetch_cancel_tomb_contains(const moq_session_t *s, uint64_t request_id);
bool fetch_cancel_tomb_consume(moq_session_t *s, uint64_t request_id);

/* Drain-ring of locally cancelled request-bidi stream_refs (stream-correlated
 * profiles). add returns false when the ring is full; contains/remove report
 * and retire membership while draining late responses. */
/* Drain-ref reason: NORMAL absorbs late in-flight bytes until FIN/reset. A
 * GOAWAY-received ref is a request stream the peer migrated with a
 * request-stream GOAWAY: the GOAWAY does not end the request, so the peer may
 * still send what it owes there (an update answer, PUBLISH_DONE); those
 * messages are discarded whole, and only a second GOAWAY on the stream closes
 * the session with PROTOCOL_VIOLATION. */
typedef enum moq_drain_reason {
    MOQ_DRAIN_NORMAL          = 0,
    MOQ_DRAIN_GOAWAY_RECEIVED = 1,
    /* ERROR+FIN is queued; cancellation of the pending UPDATE still owes
     * STOP on the peer's open direction. No additional drain storage. */
    MOQ_DRAIN_REJECT_STOP_OWED = 2,
    /* The peer stopped this side's direction of a live request: the
     * request is gone, but the peer's direction stays open, so only its FIN
     * or RESET retires the reference -- never another STOP, whatever the
     * profile's policy for streams this side cancelled. */
    MOQ_DRAIN_PEER_STOPPED = 3,
} moq_drain_reason_t;

/* Message framing state of a GOAWAY-received drain ref: the payload bytes of
 * the current message still to discard, then the next message's header
 * (Type vi64 and Length 16) gathered across reads. */
/* Advance `st` over `len` bytes of a GOAWAY-received request stream. A
 * GOAWAY among them closes the session with PROTOCOL_VIOLATION (returns
 * MOQ_OK, session closed), as does framing that cannot be a message header;
 * every other message is discarded. */
moq_result_t drain_goaway_scan(moq_session_t *s, moq_drain_scan_t *st,
                               const uint8_t *buf, size_t len);
/* The framing state of a present GOAWAY-received ref, or NULL. */
moq_drain_scan_t *drain_ref_scan(moq_session_t *s, moq_stream_ref_t ref);

bool drain_ref_add(moq_session_t *s, moq_stream_ref_t ref);
/* drain_ref_add for a stream whose live request the peer's STOP_SENDING
 * cancelled (MOQ_DRAIN_PEER_STOPPED). */
bool drain_ref_add_peer_stopped(moq_session_t *s, moq_stream_ref_t ref);
bool drain_ref_add_rejected(moq_session_t *s, moq_stream_ref_t ref,
                            bool cancel_update);
void drain_ref_retry_owed_stops(moq_session_t *s);
void fetch_abort_tomb_add(moq_session_t *s, uint64_t request_id);
/* Fetcher-side terminal for a peer RESET of the identified response data stream
 * of fetch `slot`: emits FETCH_RESET and retires the request (reserve-before-
 * mutate; MOQ_ERR_WOULD_BLOCK leaves the entry and the obligation intact). A
 * slot that already surfaced its terminal, or is not an active fetcher-role
 * request, is left alone and reports MOQ_OK. */
/* Cancel a fetch whose response delivered an object this side cannot take
 * (session_fetch.c); the caller stops the data stream. */
moq_result_t fetch_cancel_on_receive(moq_session_t *s, int slot);

/* -- Malformed Track (draft-22 §12.1) -------------------------------- */

/* Fail one request whose Track is malformed: the subscriber cancels it
 * (§6.4.2.3) with MALFORMED_TRACK and the application sees its terminal event
 * with the MOQ_REQUEST_TERMINAL_MALFORMED_TRACK cause. Each reserves before it
 * mutates (WOULD_BLOCK leaves the request as it was) and returns MOQ_OK for an
 * entry that owes no terminal any more. */
moq_result_t session_sub_fail_malformed(moq_session_t *s, int slot);
moq_result_t session_pub_fail_malformed(moq_session_t *s, int slot);
moq_result_t session_fetch_fail_malformed(moq_session_t *s, int slot);

/* A Track the peer publishes is malformed. Every request this side holds for
 * it from the peer -- each subscriber-role SUBSCRIBE (pending or
 * established), each PUBLISH the peer sent (accepted, or awaiting this side's
 * answer) and each FETCH this side sent -- is marked before any of them is
 * retired, and the cancellations are driven at once; one the queues cannot
 * take now stays owed and mt_retry_pending completes it on a later call,
 * without further input. Requests that already owe their terminal are left
 * alone; requests made after the verdict are not affected. The Track is
 * named by its canonical full-track-name key; a NULL key fails nothing. */
void session_fail_malformed_track(moq_session_t *s, const uint8_t *key, size_t key_len);
/* The same, for the Track the peer delivers on `alias`, for the Track a
 * FETCH (`fetch_slot`) carries, and for the Track of a fill's parent. */
void session_fail_malformed_alias(moq_session_t *s, uint64_t alias);
void session_fail_malformed_fetch(moq_session_t *s, int fetch_slot);
void session_fail_malformed_owner(moq_session_t *s, moq_subscription_t sub,
                                  moq_publication_t pub);
/* True when a request bound to `alias` (or the FETCH in `fetch_slot`) has a
 * malformed verdict whose cancellation is still owed: nothing more is
 * delivered to it. */
bool session_alias_failing(const moq_session_t *s, uint64_t alias);
/* Drive the owed malformed-Track cancellations. */
void mt_retry_pending(moq_session_t *s);
/* Refuse a PUBLISH still awaiting this side's answer because its Track is
 * malformed: REQUEST_ERROR MALFORMED_TRACK, then PUBLISH_FINISHED with the
 * MOQ_REQUEST_TERMINAL_MALFORMED_TRACK cause. Reserve-before-mutate. */
moq_result_t session_pub_reject_pending_malformed(moq_session_t *s, int slot);

/* One received object as the detector sees it. */
typedef struct moq_mt_object {
    uint64_t              group_id;
    uint64_t              object_id;
    bool                  in_subgroup;   /* subgroup_id/priority are meaningful */
    bool                  end_of_group;  /* the object is its group's final
                                            (status, or a datagram EOG bit) */
    uint8_t               priority;      /* resolved publisher priority */
    /* The priority was omitted on the wire and resolved from the default of
     * the request that received it. Two such objects state the same thing
     * (nothing), whatever default each request resolved them against. */
    bool                  priority_inherited;
    uint64_t              subgroup_id;
    moq_object_status_t   status;
    const moq_obj_gaps_t *gaps;          /* NULL when none */
} moq_mt_object_t;

typedef struct moq_mt_track moq_mt_track_t;

/* The detection record for the Track on `alias` / for one FETCH response,
 * created on first use. NULL when the profile does not fail malformed Tracks
 * locally or the record pool is full (detection is then skipped), with *rc
 * MOQ_OK; NULL with MOQ_ERR_NOMEM when creating it failed (the caller
 * retries the same object later). */
moq_mt_track_t *mt_track_for_alias(moq_session_t *s, uint64_t alias,
                                   moq_result_t *rc);
moq_mt_track_t *mt_track_for_fetch(moq_session_t *s, moq_fetch_t fetch,
                                   moq_result_t *rc);

/* Check one received object against what the Track has said so far and,
 * when it is consistent, record it. Returns MOQ_MALFORMED_NONE or the
 * reason. Recording is idempotent, so a re-driven object is checked again
 * with the same outcome. */
uint8_t mt_object(moq_mt_track_t *t, const moq_mt_object_t *o);

/* A subgroup stream ended with a FIN after `last` (when has_last): the final
 * Object of the subgroup, and of the group when the header set END_OF_GROUP. */
uint8_t mt_subgroup_fin(moq_mt_track_t *t, uint64_t group_id, uint64_t subgroup_id,
                        bool has_last, uint64_t last, bool end_of_group);

void mt_destroy_all(moq_session_t *s);
moq_result_t fetch_on_data_stream_reset(moq_session_t *s, int slot,
                                        uint64_t error_code);
bool fetch_abort_tomb_consume(moq_session_t *s, uint64_t request_id);
bool drain_ref_add_goaway_received(moq_session_t *s, moq_stream_ref_t ref);
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

/* Whether publication `pe` is in draft-16's automatic rejected-update
 * teardown (pub_reject_update_control): no producer output may overtake it. On a
 * stream-correlated profile a publisher's update_failed instead means it owes
 * PUBLISH_DONE(UPDATE_FAILED), and production is not refused. */
static inline bool pub_control_reject_teardown(const moq_session_t *s,
                                           const moq_pub_entry_t *pe)
{
    return pe->update_failed && pe->role == MOQ_PUB_ROLE_PUBLISHER &&
           !moq_session_uses_request_streams(s);
}

/*
 * Whether a PUBLISH-initiated subscription may be joined by a Joining FETCH.
 *
 * draft-18 10.12.2: "A Joining Fetch is only permitted when the associated
 * subscription has Forward State 1; otherwise the publisher MUST respond with
 * a REQUEST_ERROR with error code INVALID_RANGE." A publication's Forward
 * State is its acknowledged send_allowed.
 *
 * draft-16 9.16.2 instead requires the associated subscription to have Filter
 * Type Largest Object; a publication negotiates its filter on PUBLISH_OK
 * (9.2.2.5), so the established filter_type is what that gate reads. draft-18
 * has no filter requirement and must not inherit one.
 */
bool pub_joining_eligible(const moq_session_t *s, const moq_pub_entry_t *pe);

/*
 * Latch the Joining Location from the Largest this publication's PUBLISH
 * carried, if the state is now eligible (5.1). Idempotent. Does nothing when
 * no Largest was advertised -- absence leaves no Joining Location rather than
 * reusing stale or current state.
 */
void pub_latch_joining_location(const moq_session_t *s, moq_pub_entry_t *pe);
/* Latch an explicit location (an update-acknowledged snapshot). Leaves the
 * PUBLISH establishment record untouched. */
void pub_latch_joining_location_at(const moq_session_t *s, moq_pub_entry_t *pe,
                                   uint64_t group, uint64_t object);

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
                                uint64_t error_code,
                                moq_stream_ref_t stream_ref);

/* -- Namespace-sub handlers (session_namespace_sub.c) --------------- */

void ns_sub_retry_pending(moq_session_t *s);

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
                                       moq_stream_ref_t stream_ref,
                                       uint64_t error_code);
moq_result_t handle_bidi_stream_stop(moq_session_t *s,
                                      moq_stream_ref_t stream_ref,
                                      uint64_t error_code);

/* Terminate a stream-correlated request (SUBSCRIPTION or FETCH) whose request
 * bidi was torn down by the peer: free the entry and remove its registry keys,
 * surfacing a best-effort local event. Returns true if `stream_ref` matched a
 * stream-correlated request (so the caller need not look elsewhere). */
/* Peer teardown of a request bidi: `reset` distinguishes RESET_STREAM from
 * STOP_SENDING and `error_code` is the peer's stream code, both surfaced on the
 * fetcher-side FETCH_RESET terminal; the other request families ignore them. */
moq_result_t request_stream_teardown(moq_session_t *s,
                                     moq_stream_ref_t stream_ref,
                                     bool reset, uint64_t error_code);

/* The peer closed its sending direction of a request bidi before the messages
 * its role owes (the response; PUBLISH_DONE for an established subscription's
 * publisher). On a profile where a FIN is not a cancellation this fails just
 * that request: the family's terminal event carries MISSING_RESPONSE, or
 * TRUNCATED_RESPONSE when `truncated` says a partial message was buffered, the
 * entry is freed, and our own sending direction is reset with CANCELLED unless
 * the family already closed it (TRACK_STATUS opens its bidi with the FIN).
 * Reserve-before-mutate: a full event or action queue returns WOULD_BLOCK with
 * the entry intact for the re-feed. */
moq_result_t request_stream_failed_by_fin(moq_session_t *s,
                                          moq_stream_ref_t stream_ref,
                                          bool truncated);

/* Queue a RESET_STREAM (CANCELLED) of our sending direction on a request
 * bidi. The caller reserved the action slot. */
void request_bidi_reset_local(moq_session_t *s, moq_stream_ref_t stream_ref);

/* Owned cluster metadata helpers; mutations reserve the new path first. */
bool session_cluster_namespace_duplicate(moq_session_t *s, const moq_namespace_t *ns,
                                         bool local_sender, int excluded_ns_sub);
moq_result_t session_cluster_validate(const moq_cluster_metadata_t *m, bool required);
moq_result_t session_cluster_copy(moq_session_t *s, const moq_cluster_metadata_t *in,
                                  moq_cluster_metadata_t *out);
moq_result_t session_cluster_decode(moq_session_t *s, moq_bytes_t path, bool has_cost,
                                    uint64_t cost, moq_cluster_metadata_t *out);
void session_cluster_free(moq_session_t *s, moq_cluster_metadata_t *m);
moq_result_t session_cluster_event(moq_session_t *s, const moq_cluster_metadata_t *in,
                                   moq_cluster_metadata_t *out);
void session_cluster_apply(moq_session_t *s, moq_cluster_metadata_t *current,
                           moq_cluster_metadata_t *update);

/* -- Draft-22 namespace updates (session_namespace_update.c) ---------- */

struct moq_request_envelope;

/* A peer namespace update between decode and its outcome: the decoded
 * parameters, the admitted Request ID and the staged token transaction.
 * refuse_code is the REQUEST_ERROR an automatic refusal answers with (a token
 * failure, or INVALID_FILTER for a Range Filter); zero when the update passed
 * the message-level checks. */
typedef struct ns_update_admission {
    moq_decoded_namespace_update_t d;
    moq_request_endpoint_t endpoint;
    moq_resolved_token_t   tokens[MOQ_DECODED_MAX_TOKENS];
    bool                   token_staged[MOQ_DECODED_MAX_TOKENS];
    size_t                 token_count;
    moq_auth_txn_t         auth_txn;
    uint64_t               refuse_code;
    bool                   staged;
} ns_update_admission_t;

void ns_update_release(moq_session_t *s, moq_ns_update_t *u);
void ns_update_clear_owed(moq_session_t *s, moq_ns_update_t *u);
void ns_update_clear_outstanding(moq_session_t *s, moq_ns_update_t *u);
void ns_update_fill_record(const moq_ns_update_t *u,
                           moq_namespace_update_record_t *rec);
moq_result_t ns_update_owed_query(const moq_ns_update_t *u,
    moq_namespace_update_record_t *out, size_t out_size);
moq_result_t ns_update_push_request_event(moq_session_t *s,
    moq_request_family_t family, uint64_t handle_raw, const moq_ns_update_t *u);
moq_result_t ns_update_push_result_event(moq_session_t *s, moq_request_family_t family,
                                         uint64_t handle_raw, uint64_t update_request_id,
                                         moq_namespace_update_outcome_t outcome,
                                         moq_request_role_t role, uint64_t error_code,
                                         const uint8_t *reason, size_t reason_len, bool has_expires,
                                         uint64_t expires_ms,
                                         const moq_cluster_metadata_t *cluster);
/* Decode, admit the Request ID and stage the token transaction of a peer
 * namespace update. A malformed message closes the session. On MOQ_OK the
 * caller ends the admission with ns_update_defer, or with
 * ns_update_admission_commit after answering it, or ns_update_admission_abort. */
moq_result_t ns_update_admit(moq_session_t *s, moq_request_family_t family,
    moq_stream_ref_t ref, const struct moq_request_envelope *env,
    ns_update_admission_t *a);
void ns_update_admission_commit(moq_session_t *s, ns_update_admission_t *a);
void ns_update_admission_abort(moq_session_t *s, ns_update_admission_t *a);
moq_result_t ns_update_defer(moq_session_t *s, moq_request_family_t family,
    uint64_t handle_raw, moq_ns_update_t *u, ns_update_admission_t *a);
moq_result_t ns_update_check_accept(const moq_session_t *s, const moq_ns_update_t *u,
    const moq_accept_subscription_update_cfg_t *cfg);
moq_result_t ns_update_check_reject(const moq_session_t *s, const moq_ns_update_t *u,
    const moq_reject_subscription_update_cfg_t *cfg);
moq_result_t ns_update_encode_ok(moq_session_t *s, uint64_t request_id,
                                 uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t ns_update_encode_error(moq_session_t *s, uint64_t request_id,
    uint64_t code, const uint8_t *reason, size_t reason_len,
    uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t ns_update_cfg_read(const moq_session_t *s,
    const moq_namespace_update_cfg_t *cfg, moq_request_family_t family,
    moq_namespace_update_encode_args_t *out);
moq_result_t ns_update_send(moq_session_t *s, moq_stream_ref_t ref,
    moq_ns_update_t *u, moq_namespace_update_encode_args_t *args,
    uint64_t *out_update_request_id);
void ns_update_take_out_prefix(moq_ns_update_t *u, uint8_t **buf, size_t *buf_len,
                               moq_bytes_t **parts, size_t *count);
void ns_update_take_owed_prefix(moq_ns_update_t *u, uint8_t **buf, size_t *buf_len,
                                moq_bytes_t **parts, size_t *count);
bool ns_update_has_prefix(const moq_bytes_t *prefix, size_t prefix_count,
                          const moq_bytes_t *ns, size_t ns_count);

/* Family hooks. Each answers an inbound update on an established request it
 * responds to, or a response to this side's update on one it made; `env` is
 * the complete message and the caller consumes it on MOQ_OK. */
moq_result_t ann_on_namespace_update(moq_session_t *s, int slot,
    const struct moq_request_envelope *env);
moq_result_t ann_on_update_response(moq_session_t *s, int slot, bool ok,
    uint64_t error_code, const uint8_t *reason, size_t reason_len,
    bool has_expires, uint64_t expires_ms);
moq_result_t track_sub_on_namespace_update(moq_session_t *s, int slot,
    const struct moq_request_envelope *env);
moq_result_t track_sub_on_update_response(moq_session_t *s, int slot, bool ok,
    uint64_t error_code, const uint8_t *reason, size_t reason_len,
    bool has_expires, uint64_t expires_ms);
/* The same admission when updates are not left to the application, ending
 * in a refusal (its admission code, else NOT_SUPPORTED) without an event. */
moq_result_t ann_refuse_namespace_update(moq_session_t *s, int slot,
    const struct moq_request_envelope *env);
moq_result_t track_sub_refuse_namespace_update(moq_session_t *s, int slot,
    const struct moq_request_envelope *env);

#endif /* MOQ_SESSION_INTERNAL_H */
