/*
 * moq_media_receiver_t — receive-side media facade.
 *
 * Threading model: the receiver registers an internal endpoint pump hook
 * (endpoint_internal.h) that runs on the endpoint's network thread. ALL
 * session/subscriber work happens inside that hook. The application thread
 * only touches the mutex-protected track-event queue, the terminal flags,
 * and the endpoint's public wait/latch surface. The effective catalog and the
 * handle list are written exclusively by the hook (including across catalog
 * updates: handles are appended under r->mu and individual handles' desc bytes
 * are handle-owned and immutable once built); destroy frees everything after
 * detaching (no concurrency remains, which is also what makes the non-atomic
 * rcbuf refcounts safe to release on the destroying thread).
 */

#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#include "media_auth_internal.h"
#include <moq/msf.h>
#include <moq/msf_media.h>
#include <moq/media_object.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */
#include <moq/vi64.h>

#include "media_receiver_internal.h"
#include "media_receiver_order.h"
#include "endpoint_internal.h"
#include "../../core/src/spi/core_spi.h"

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
#include <moq/mpeg2ts.h>
#endif

#include <assert.h>
#include <errno.h>
#include "../../adapters/common/moq_posix_compat.h"   /* pthread + clock_gettime (Windows shim) */
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

#define RECEIVER_DEFAULT_TRACK_EVENTS 64u
#define RECEIVER_SUB_MAX_TRACKS  32u  /* catalog + auto-subscribed tracks */
#define RECEIVER_SUB_MAX_OBJECTS 64u
#define RECEIVER_SUB_MAX_FILLS   16u
/* The subscriber facade's default fetch capacity (max_fetches 0), which the
 * receiver keeps; its media fetch records are sized from it. */
#define RECEIVER_SUB_MAX_FETCHES 16u
#define RECEIVER_DEFAULT_FETCHES_PER_TRACK 4u
#define RECEIVER_DEFAULT_MAX_OBJECTS 256u
#define RECEIVER_DEFAULT_MAX_BYTES   (32u * 1024u * 1024u)
#define RECEIVER_PARSE_STACK_SAMPLES 64u
#define RECEIVER_DEFAULT_SAP_RECORDS 256u  /* bounded SAP-record queue depth */
#define RECEIVER_DEFAULT_MT_RECORDS  256u  /* bounded media-timeline queue depth */
#define RECEIVER_DEFAULT_STREAM_TERMINALS 64u /* stream-terminal lane depth */
#define RECEIVER_DEFAULT_TS_SLOTS    64u      /* MPEG2TS reorder slots per track */
#define RECEIVER_DEFAULT_TS_WINDOW_US 300000u /* MPEG2TS reorder window */
/* Receiver-lifetime caps on retained catalog-update state (availability): a peer
 * sending many valid independent catalog generations must not grow stable track
 * handles or retained CP snapshots without bound. Conservative defaults --
 * legitimate catalogs hold a handful of stable tracks and few CP generations;
 * these only bite a churning/abusive peer. */
#define RECEIVER_DEFAULT_MAX_STABLE_HANDLES 1024u
#define RECEIVER_DEFAULT_MAX_CP_SNAPS         64u
#define RECEIVER_DEFAULT_MAX_RETAINED_BYTES (16u * 1024u * 1024u)

/* SUBSCRIBE_DONE "Track Ended" status (draft-ietf-moq-transport SUBSCRIBE_DONE
 * status codes; the publisher emits it as MOQ_PUB_DONE_TRACK_ENDED for an MSF
 * §11.3 live->VOD conversion). Non-terminal: the track stays joinable. */
#define RECEIVER_DONE_TRACK_ENDED 0x2u

/* Frozen v0 base sizes for the caller-filled output structs: the minimum a
 * caller must provide. Computed from the last v0 field's offset so they stay
 * platform-correct AND constant when future versions APPEND fields -- an older
 * caller passing its smaller size still receives the v0 prefix. These must NOT
 * change when fields are appended; new fields get their own (larger) base.
 * Validation uses these (not sizeof(current)); writes are clamped to
 * min(caller_size, sizeof(current)). */
#define MEDIA_TRACK_EVENT_V0_SIZE \
    (offsetof(moq_media_track_event_t, config_generation) + sizeof(uint32_t))

/* ABI boundary: the v0 event, exactly as it was, and the appended UPDATE_OK
 * block starting at or after its complete allocation (sizeof, padding
 * included), so a v0 caller's storage is never written past. With the
 * _reserved_track_ev spacer the block starts exactly at the old sizeof where
 * v0 ended in four bytes of padding (LP64), and four bytes past it where v0
 * ended at config_generation (32-bit targets). The exact offsets are pinned
 * per platform in tests/unit/test_abi_layout_d22.c. */
typedef struct media_track_event_v0 {
    uint32_t                      struct_size;
    moq_media_track_event_kind_t  kind;
    moq_media_track_t            *track;
    const moq_media_track_desc_t *desc;
    uint32_t                      config_generation;
} media_track_event_v0_t;
_Static_assert(offsetof(moq_media_track_event_t, kind) ==
                   offsetof(media_track_event_v0_t, kind) &&
               offsetof(moq_media_track_event_t, track) ==
                   offsetof(media_track_event_v0_t, track) &&
               offsetof(moq_media_track_event_t, desc) ==
                   offsetof(media_track_event_v0_t, desc) &&
               offsetof(moq_media_track_event_t, config_generation) ==
                   offsetof(media_track_event_v0_t, config_generation),
               "v0 event fields keep their offsets");
_Static_assert(MEDIA_TRACK_EVENT_V0_SIZE <= sizeof(media_track_event_v0_t),
               "the v0 floor lies within the v0 allocation");
_Static_assert(offsetof(moq_media_track_event_t, has_largest) >=
                   sizeof(media_track_event_v0_t),
               "UPDATE_OK detail block must start past the complete v0 allocation");
/* The parse-drop detail block is a SECOND appended generation: it must start at
 * the align-rounded end of the UPDATE_OK block (right after expires_ms), so an
 * older caller passing the UPDATE_OK-generation sizeof is never written past. */
_Static_assert(offsetof(moq_media_track_event_t, parse_drop_class) ==
               ((offsetof(moq_media_track_event_t, expires_ms) +
                 sizeof(((moq_media_track_event_t *)0)->expires_ms) +
                 (_Alignof(moq_media_track_event_t) - 1)) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "parse-drop detail block must sit exactly after the UPDATE_OK block");
_Static_assert(offsetof(moq_media_track_event_t, local_result) ==
               ((offsetof(moq_media_track_event_t, parse_drops_delta) +
                 sizeof(uint64_t) + _Alignof(moq_media_track_event_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "local failure must follow the complete preceding event allocation");
_Static_assert(offsetof(moq_media_track_event_t, initiating_request_id) ==
               ((offsetof(moq_media_track_event_t, _reserved_local_failure) +
                 sizeof(uint32_t) + _Alignof(moq_media_track_event_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "fill terminal must follow the complete local failure allocation");
_Static_assert(offsetof(moq_media_track_event_t, delivery_origin) ==
               ((offsetof(moq_media_track_event_t, fill_error_code) +
                 sizeof(uint64_t) + _Alignof(moq_media_track_event_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "delivery failure must follow the complete fill terminal allocation");
_Static_assert(offsetof(moq_media_track_event_t, fill_unreported_loss_causes) ==
               ((offsetof(moq_media_track_event_t, delivery_initiating_request_id) +
                 sizeof(uint64_t) + _Alignof(moq_media_track_event_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "the fill loss summary must follow the complete delivery failure allocation");
/* The whole loss summary: an event is written with it only when this covers it. */
#define MEDIA_TRACK_EVENT_FILL_LOSS_END \
    (offsetof(moq_media_track_event_t, fill_unreported_loss_object) + sizeof(uint64_t))
_Static_assert(offsetof(moq_media_track_event_t, has_done_status) ==
               ((MEDIA_TRACK_EVENT_FILL_LOSS_END + _Alignof(moq_media_track_event_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_event_t) - 1)),
               "the done status must follow the complete fill loss allocation");
#define MEDIA_TRACK_EVENT_DONE_END \
    (offsetof(moq_media_track_event_t, done_status) + sizeof(uint64_t))
_Static_assert(offsetof(moq_media_track_event_t, replaces) ==
               MEDIA_TRACK_EVENT_DONE_END,
               "the replacement block must follow the complete done-status allocation");
#define MEDIA_TRACK_EVENT_REPLACED_END \
    (offsetof(moq_media_track_event_t, replaced_by) + sizeof(moq_media_track_t *))
_Static_assert(offsetof(moq_media_track_desc_t, init_property_type) ==
               ((offsetof(moq_media_track_desc_t, has_label) + sizeof(bool) +
                 _Alignof(moq_media_track_desc_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_desc_t) - 1)),
               "the init-source block must follow the complete has_label allocation");
#define MEDIA_RECEIVER_STATS_TRACKS_MALFORMED_END \
    (offsetof(moq_media_receiver_stats_t, tracks_malformed) + sizeof(uint64_t))
_Static_assert(offsetof(moq_media_receiver_stats_t, catalog_revision) ==
               MEDIA_RECEIVER_STATS_TRACKS_MALFORMED_END,
               "the catalog-revision block must follow the complete tracks_malformed allocation");
_Static_assert(offsetof(moq_media_track_desc_t, has_render_group) ==
               ((offsetof(moq_media_track_desc_t, alt_group) + sizeof(int) +
                 _Alignof(moq_media_track_desc_t) - 1) &
                ~(size_t)(_Alignof(moq_media_track_desc_t) - 1)),
               "the render group must follow the complete alt_group allocation");
#define MEDIA_OBJECT_V0_SIZE \
    (offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *))
/* ABI boundaries of the appended blocks: each starts at the sizeof of the
 * struct that ended with the block before it, never inside its padding. */
_Static_assert(offsetof(moq_media_object_t, has_loss_report) >=
               ((offsetof(moq_media_object_t, has_subgroup) + sizeof(bool) +
                 (_Alignof(moq_media_object_t) - 1)) &
                ~(size_t)(_Alignof(moq_media_object_t) - 1)),
               "the loss report must not consume old object padding");
_Static_assert(offsetof(moq_media_object_t, has_loss_report) ==
               offsetof(moq_media_object_t, _reserved_obj) + 6,
               "the loss report must follow the reserved object slot");
_Static_assert(offsetof(moq_media_track_desc_t, extensions) >=
               ((offsetof(moq_media_track_desc_t, is_live) + sizeof(bool) +
                 (_Alignof(moq_media_track_desc_t) - 1)) &
                ~(size_t)(_Alignof(moq_media_track_desc_t) - 1)),
               "extensions must not consume original descriptor padding");
_Static_assert(offsetof(moq_media_track_desc_t, extensions) ==
               ((offsetof(moq_media_track_desc_t, _reserved_desc) +
                 sizeof(uint32_t) + _Alignof(void *) - 1) &
                ~(size_t)(_Alignof(void *) - 1)),
               "extensions must follow the reserved descriptor slot");
_Static_assert(offsetof(moq_media_receiver_cfg_t, mpeg2ts_reorder_slots) >=
               MOQ_MEDIA_RECEIVER_CFG_V0_SIZE,
               "the mpeg2ts_ fields must not consume old config padding");
_Static_assert(offsetof(moq_media_receiver_cfg_t, mpeg2ts_reorder_slots) ==
               offsetof(moq_media_receiver_cfg_t, _reserved_cfg) + 4,
               "the reorder slots must follow the reserved config slot");
_Static_assert(offsetof(moq_media_receiver_track_subscribe_cfg_t,
                        mpeg2ts_program_number) >=
               MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE,
               "mpeg2ts_program_number must start outside the original sizeof "
               "through priority");
_Static_assert(offsetof(moq_media_receiver_track_subscribe_cfg_t,
                        _reserved_sub) ==
                   offsetof(moq_media_receiver_track_subscribe_cfg_t,
                            priority) + sizeof(uint8_t) &&
               offsetof(moq_media_receiver_track_subscribe_cfg_t,
                        mpeg2ts_program_number) >=
                   offsetof(moq_media_receiver_track_subscribe_cfg_t,
                            _reserved_sub) + 2,
               "_reserved_sub protects the tail padding after priority");
#define MEDIA_OBJECT_METADATA_SIZE \
    (offsetof(moq_media_object_t, track_properties_ref) + sizeof(moq_rcbuf_t *))
#define MEDIA_OBJECT_ORIGIN_SIZE \
    (offsetof(moq_media_object_t, initiating_request_id) + sizeof(uint64_t))
/* Explicit LOC-05 receivers require the object through codec_string. */
#define MEDIA_OBJECT_LOC_EXT_SIZE \
    (offsetof(moq_media_object_t, codec_string) + sizeof(moq_bytes_t))
_Static_assert(offsetof(moq_media_object_t, loc_ext_present) ==
               MEDIA_OBJECT_ORIGIN_SIZE,
               "the LOC-05 block starts at the complete sizeof through initiating_request_id");
_Static_assert(offsetof(moq_media_object_t, delivery_origin) ==
               ((MEDIA_OBJECT_METADATA_SIZE + _Alignof(moq_media_object_t) - 1) &
                ~(size_t)(_Alignof(moq_media_object_t) - 1)),
               "origin must follow the complete metadata/ref allocation");
/* The receiver config as it stood with the MPEG2TS block and before the
 * companion, declared independently: the companion starts at or past its
 * complete sizeof, never in its tail padding. */
typedef struct receiver_cfg_before_companion {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
    uint8_t  reserved_cfg[4];
    uint32_t mpeg2ts_reorder_slots;
    uint64_t mpeg2ts_reorder_window_us;
    bool     mpeg2ts_strip_prefix;
    bool     mpeg2ts_join_psi;
} receiver_cfg_before_companion_t;
_Static_assert(offsetof(moq_media_receiver_cfg_t, mpeg2ts_join_psi) ==
               offsetof(receiver_cfg_before_companion_t, mpeg2ts_join_psi),
               "the MPEG2TS block keeps its offsets");
_Static_assert(offsetof(moq_media_receiver_cfg_t, companion_profile) >=
               sizeof(receiver_cfg_before_companion_t),
               "companion must not reuse the MPEG2TS-era tail padding");
_Static_assert(offsetof(moq_media_receiver_cfg_t, resource_auth) >=
               ((offsetof(moq_media_receiver_cfg_t, surface_stream_terminals) +
                 sizeof(bool) + _Alignof(moq_media_receiver_cfg_t) - 1u) &
                ~((size_t)_Alignof(moq_media_receiver_cfg_t) - 1u)),
               "resource_auth must follow the complete stream-terminal allocation");
_Static_assert(offsetof(moq_media_receiver_cfg_t, max_fetches_per_track) >=
               ((offsetof(moq_media_receiver_cfg_t, resource_auth) +
                 sizeof(void *) + _Alignof(moq_media_receiver_cfg_t) - 1u) &
                ~((size_t)_Alignof(moq_media_receiver_cfg_t) - 1u)),
               "max_fetches_per_track must follow the complete resource_auth allocation");
_Static_assert(offsetof(moq_media_receiver_track_subscribe_cfg_t, location) >=
               offsetof(moq_media_receiver_track_subscribe_cfg_t,
                        mpeg2ts_program_number) + sizeof(uint32_t),
               "track options must follow the MPEG2TS program number");
_Static_assert(offsetof(moq_media_object_t, metadata) ==
               ((offsetof(moq_media_object_t, discontinuous) + sizeof(bool) +
                 _Alignof(moq_media_object_t) - 1) &
                ~(size_t)(_Alignof(moq_media_object_t) - 1)),
               "metadata must start after the complete loss-report object");
#define MEDIA_RECEIVER_STATS_V0_SIZE \
    (offsetof(moq_media_receiver_stats_t, paused) + sizeof(bool))
/* Appended-field generations (the v0 prefix stays frozen at `paused`).
 * get_stats validates against v0 and clamps writes to min(caller, current), so
 * a caller passing one of these sizes gets exactly that prefix. */
#define MEDIA_RECEIVER_STATS_CATALOG_DROPS_SIZE \
    (offsetof(moq_media_receiver_stats_t, catalog_drops) + sizeof(uint64_t))
#define MEDIA_RECEIVER_STATS_CATALOG_COMPLETE_SIZE \
    (offsetof(moq_media_receiver_stats_t, catalog_complete) + sizeof(bool))
#define MEDIA_SAP_RECORD_V0_SIZE \
    (offsetof(moq_media_sap_record_t, ept_ms) + sizeof(uint64_t))
#define MEDIA_TIMELINE_RECORD_V0_SIZE \
    (offsetof(moq_media_timeline_record_t, wallclock_ms) + sizeof(uint64_t))

/* -- Internal types --------------------------------------------------- */

/* Pointer-free desired state crosses from app to network under r->mu.
 * fill.location stays NULL; the network's local copy supplies its own pointer. */
typedef struct receiver_track_options {
    bool has_location;
    bool has_fill;
    bool has_fill_location;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_t fill_location;
    moq_fill_cfg_t fill;
    bool has_object_delivery_timeout;
    bool has_subgroup_delivery_timeout;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
    bool has_forward;
    bool forward;
} receiver_track_options_t;

struct moq_media_track {
    /* Selection is executor-owned; failure and intent are mu-published. */
    moq_media_auth_operation_t auth_operation;
    moq_media_auth_action_t auth_action;
    uint64_t auth_generation;
    bool auth_forward;
    bool auth_failed;
    moq_media_track_desc_t desc;
    moq_rcbuf_t           *init_buf;   /* owned; desc.init_data, desc.codec_config
                                          and the cmaf record's spans borrow */

    /* Handle-owned desc backing (so a handle outlives the effective catalog it
     * was built from -- catalogs are replaced on each update). desc string
     * spans point into desc_strings; desc.depends / .content_protection_ref_ids
     * point into the owned arrays. When the track's init data is a CMAF
     * header, desc_strings begins with the parsed record desc.cmaf points to. */
    uint8_t          *desc_strings;
    size_t            desc_strings_size;
    moq_bytes_t      *desc_depends_arr;
    moq_bytes_t      *desc_cp_refs_arr;
    /* Handle-owned extension list: the entries, then each record, its SI
     * PIDs and its name bytes, in one block of desc_ext_size. */
    uint8_t          *desc_ext;
    size_t            desc_ext_size;

    /* The receiver has a delivery path for this track (decided at build,
     * immutable): poll_object for a media track whose info derived and
     * whose packaging this build parses; poll_sap / poll_media_timeline for
     * the timelines it decodes. Media tracks with a path are what
     * auto-subscribe takes. */
    bool              has_media_path;
    bool              has_path;

    /* Owned MSF tuple key (namespace + name), the catalog identity used to match
     * a handle across updates. Namespace is not a desc field, so it is kept here
     * (bytes live in desc_strings). MSF allows the same local name in different
     * namespaces, so matching MUST use the full tuple, never name alone. */
    bool              key_has_ns;
    moq_bytes_t       key_ns;          /* points into desc_strings */

    bool              removed;         /* TRACK_REMOVED emitted; terminal handle */
    bool              recon_seen;       /* reconcile-walk scratch (network thread) */

    /* Desired subscription state (app intent). Written by the public
     * subscribe/unsubscribe calls under r->mu; read by the network-thread
     * hook which reconciles it against the actual subscription. */
    bool             desired_subscribed;
    moq_subscribe_filter_t desired_filter;   /* start point at (re)subscribe */
    bool             desired_has_priority;
    uint8_t          desired_priority;
    uint32_t         desired_ts_program;     /* MPEG2TS reference program */

    /* MPEG2TS (network thread): the reorder window state, allocated when
     * the subscription is issued. ts_loss_* (under r->mu) is a local loss
     * -- an Object released by the window but dropped, or evicted from the
     * queue with nothing of the track queued after it -- that the next
     * queued Object of the track reports as its gap. */
    void            *ts;
    bool             ts_loss_pending;
    uint64_t         ts_loss_group;
    uint64_t         ts_loss_object;
    /* The subscription generation LIVE Objects and their loss belong to:
     * advanced when a new subscription's window starts, so a loss of one
     * generation is never reported on an Object of the next. */
    uint32_t         ts_gen;
    uint32_t         ts_loss_gen;
    /* The join PSI (mpeg2ts_join_psi) went out with an Object of generation
     * ts_gen the application polled: no later Object of it carries it. */
    bool             ts_join_delivered;
    /* Executor only: a raise of this passive track's Forward State is under
     * way, so the relay starts it mid-Group; its Objects may start the
     * window without waiting for Object 0 (MOQ_MPEG2TS_OBJ_JOINABLE) until
     * the window releases its first Object. */
    bool             ts_rejoin;
    receiver_track_options_t desired_options;

    /* Data path (network-thread state). */
    moq_sub_track_t *sub_track;        /* non-NULL while subscribed */
    bool             ended;            /* TRACK_ENDED emitted */
    /* Removed by a terminal (isComplete) catalog while its subscription has
     * not ended: the broadcast completed, so the media it already sent -- and
     * its END_OF_TRACK, which can trail the catalog on another stream -- is
     * still delivered. The subscription stays until it is over at the
     * facade (its done, with every counted stream processed, or a refusal)
     * or the track fails locally; TRACK_REMOVED follows that. */
    bool             completion_pending;
    /* The application unsubscribed a completion_pending track: its queued
     * objects were purged, and the network thread retires the subscription
     * and removes the track without an end (receiver_settle_completions).
     * Set and read under mu. */
    bool             completion_cancelled;
    /* Set once, at the removal itself, when a terminal (isComplete) catalog
     * removed the track; a withdrawal leaves it false. Never changes after,
     * so a consumer reading it at a queued TRACK_REMOVED learns why that
     * removal happened, whatever later catalogs did. Under mu. */
    bool             removed_by_completion;
    /* ACK retention, UPDATE admission and LIVE delivery failures are permanent.
     * App-visible state and its allocation-free observation are mu-protected. */
    moq_result_t     local_result;
    moq_media_track_event_kind_t local_failure_kind;
    bool             local_failure_pending;
    /* A LIVE delivery failure recorded but not yet published: the facade
     * still holds input it admitted for the track, which is taken first. */
    bool             delivery_failure_staged;
    bool             delivery_failed; /* preserves already-admitted objects */
    /* Coalesced parse-drop diagnostic state (all under r->mu). A malformed
     * media/SAP/media-timeline object for this track bumps parse_drops and, if
     * no diagnostic is already pending, marks parse_drop_pending (and the
     * receiver's pending count). poll_track synthesizes ONE MOQ_MEDIA_TRACK_
     * PARSE_DROP event from this state -- reporting parse_drops (total) and
     * parse_drops - parse_drops_reported (delta since the last drain) -- then
     * clears the pending flag and advances parse_drops_reported. This bounds the
     * app-visible signal to one pending event per track regardless of how many
     * malformed objects a peer sends; parse drops are never fatal. */
    uint64_t         parse_drops;
    uint64_t         parse_drops_reported;
    bool             parse_drop_pending;
    moq_media_parse_drop_class_t parse_drop_last_class;
    /* SUBSCRIBE_DONE handling (network thread). on_subscribe_done records the
     * completion here and the pump processes it AFTER draining objects, so media
     * delivered in the same tick as the done is not lost. vod_rearmed is a
     * one-shot guard: a "Track Ended" (live->VOD step 1) re-subscribes once for
     * the now-VOD content; any further done is treated as terminal, so a peer
     * that keeps finishing cannot spin a re-subscribe loop (MSF §11.3).
     * was_live records whether the catalog that created the track declared it
     * live (a later generation cannot turn a VOD track live). Only such a track
     * can convert, and the conversion catalog may land before the done, so the
     * current is_live cannot decide. */
    bool             done_pending;
    uint64_t         done_status;
    bool             vod_rearmed;
    bool             was_live;
    bool             has_drop_filter;  /* monotonic: arrivals with
                                          group_id <= drop_below_group are
                                          stragglers of evicted content.
                                          DROP_TO_KEYFRAME exempts keyframes
                                          (an arriving sync point restarts
                                          the chain and clears the filter);
                                          the group policies drop the whole
                                          group. */
    uint64_t         drop_below_group;
    bool             walk_key_seen;    /* compaction-walk scratch */
    /* Stream-terminal lane (r->mu): the track's queued LIVE entries. */
    uint32_t         st_queued;
    uint64_t         walk_key_group;
    bool             forward_sent;     /* last forward state sent (pause) */
    bool             forward_desired;
    bool             passive;          /* subscribed with Forward 0 (option) */
    bool             raised;           /* r->mu: the app raised the passive track
                                          (moq_media_receiver_set_track_forward);
                                          reconciliation forwards it while set */
    /* -- update-acknowledgment attribution state machine --------------- *
     * app_gen bumps on every subscribe_track/unsubscribe_track call that
     * CHANGES desired_subscribed; reconciled_app_gen is the newest app
     * generation whose effect was either sent on the wire or proven
     * coalesced-away (a reconciler pass with nothing to send advances it
     * silently). A successful send stamps the outstanding update's
     * attribution ONCE: upd_notify_app = (app_gen > reconciled_app_gen).
     * The ack surfaces MOQ_MEDIA_TRACK_UPDATE_OK only when upd_notify_app
     * -- one event per APP-ATTRIBUTED WIRE ACKNOWLEDGMENT; pure
     * flow-control acks stay silent regardless of the generations they
     * happen to carry. Error/terminal/VOD-rearm clear the pending
     * attribution so a replacement subscription's first ack never
     * inherits it. Guarded by r->mu (toggles bump under it). */
    uint64_t         app_gen;
    uint64_t         reconciled_app_gen;
    bool             upd_notify_app;
    bool             update_inflight;  /* includes silent flow-only updates */
    uint64_t         upd_sent_app_gen;

    /* Subscription lifecycle, mu-published so the public track_state accessor
     * never races the network-thread-only sub_track pointer:
     *   requested   -- a SUBSCRIBE has been issued (reconcile); PENDING until OK.
     *   established  -- SUBSCRIBE_OK received (on_subscribed callback); ACTIVE.
     * Both persist once set: a disable keeps the subscription (paused), so a
     * re-enable stays ACTIVE rather than going back to PENDING. */
    bool             requested;
    bool             established;
    /* Advanced (under mu) each time a finished subscription is torn down, so
     * a fill command recorded for one never goes out on its replacement. */
    uint64_t         parent_gen;

    /* The explicit fill command of moq_media_receiver_fill_track (under mu):
     *   NONE     the slot is free;
     *   QUEUED   recorded, not yet taken by the executor;
     *   CLAIMED  taken: the executor is admitting it without the mutex, so
     *            a disable only marks it withdrawn and admission may win;
     *   ADMITTED its UPDATE was admitted; cmd_token names the facade child
     *            whose CREATED item becomes the receipt;
     *   RECEIPT  its FILL_CREATED is queued; polling it frees the slot.
     * cmd_seq is the command's receiver-wide, never-reused identity, so a
     * stale step can neither send nor settle a newer command. */
    uint8_t          cmd_state;
    bool             cmd_withdrawn;
    uint64_t         cmd_seq;
    uint64_t         cmd_parent_gen;
    moq_fill_cfg_t   cmd_fill;          /* location borrowed per admission */
    moq_location_filter_cfg_t cmd_location;
    bool             cmd_has_location;
    moq_sub_fill_t   cmd_token;
    /* Executor-owned: the REQUEST_UPDATE credential selected for command
     * `cmd_auth_seq`, NULL when none. It is the command's own, apart from the
     * Forward operation's, and survives that command's transient admission
     * retries. Allocated when a command is first taken and freed once it is
     * admitted, refused or gone, so a track pays for one only while a
     * command holds a selection. */
    moq_media_auth_operation_t *cmd_auth;
    uint64_t         cmd_auth_seq;

    /* The standalone fetch command of moq_media_receiver_fetch_track (under
     * mu), apart from the fill command above:
     *   NONE     the slot is free;
     *   QUEUED   recorded, not yet taken by the executor;
     *   CLAIMED  taken: the executor is admitting it without the mutex, so
     *            an unsubscribe only marks it withdrawn and admission may win;
     *   RECEIPT  its FETCH_CREATED (or NOT_SENT FETCH_TERMINAL) is queued;
     *            polling it frees the slot.
     * fetch_cmd_seq is the command's receiver-wide, never-reused identity. */
    uint8_t          fetch_cmd_state;
    bool             fetch_cmd_withdrawn;
    uint64_t         fetch_cmd_seq;
    moq_media_fetch_cfg_t fetch_cmd_cfg;
    /* Executor-owned: the FETCH credential selected for command
     * `fetch_auth_seq`, kept across its transient admission retries and
     * freed once it is admitted, refused or gone (as cmd_auth). */
    moq_media_auth_operation_t *fetch_auth;
    uint64_t         fetch_auth_seq;
    /* Objects of this track's standalone fetches taken from the facade
     * (under mu). Never part of the LIVE counters. */
    uint64_t         fetch_objects_received;

    /* Timeline classification (network thread). is_event_timeline is any
     * packaging=="eventtimeline" track; is_media_timeline is any
     * packaging=="mediatimeline" track (MSF §7.2). Neither is auto-subscribed
     * and neither is fed to the media object parser. is_sap_timeline is the CMSF
     * SAP subtype of an event timeline (eventType "org.ietf.moq.cmsf.sap" +
     * mimeType "application/json"); ONLY those route to the SAP parser and
     * surface via poll_sap. Any other timeline object is dropped (not SAP, not
     * media -- the explicit media-timeline payload codec is not implemented).
     * sap_last tracks the highest media Location surfaced so §8.3 independent
     * re-sends de-dup. */
    bool             is_event_timeline;
    bool             is_media_timeline;
    bool             is_sap_timeline;
    bool             sap_has_last;
    uint64_t         sap_last_group;
    uint64_t         sap_last_object;
    /* §7.3 media-timeline dedup: last surfaced media Location for this track, so
     * an independent re-send of accumulated records does not re-surface them. */
    bool             mt_has_last;
    uint64_t         mt_last_group;
    uint64_t         mt_last_object;

    /* -- MSF-02 description generations ------------------------------- *
     * desc is generation 0. An advisory change of the declared track
     * publishes a complete new description (receiver_desc_gen_t, newest
     * first on gens) and never rewrites a published one; every generation
     * stays valid until the receiver is destroyed. desc_cur is the current
     * generation, written only by the network thread under r->mu; desc_pub
     * mirrors it for the lock-free moq_media_track_desc_get (release store,
     * acquire load). The live-to-VOD triple is written in place on desc_cur,
     * under r->mu. config_gen is desc_cur's generation number. */
    moq_media_track_desc_t *desc_cur;
    const moq_media_track_desc_t *_Atomic desc_pub;
    struct receiver_desc_gen *gens;
    uint32_t         config_gen;

    /* The in-group ordering stage of an MSF-02 "object"-mapped LOC track's
     * live subscription (network thread), or NULL; `ordered` (set at build,
     * immutable): the track takes one when its subscription is issued. */
    struct receiver_ordering *order;
    bool             ordered;
    /* An MSF-02 CMAF track with a parsed init record: every fragment's
     * tfhd.track_ID must be the init's (moq_cmaf_validate_object), the
     * configuration identity CMAF carries (set at build, immutable). */
    bool             cmaf_track_id_check;

    /* The MSF-02 Full Track Name namespace (11.2): the catalog's namespace
     * for a track without "namespace" (MSF-02 5.2.2), else that string split
     * into fields; the fields live in ns_block. has_ns_fields false when the
     * track's namespace string does not split (an MSF-01 track), so it never
     * matches an MSF-02 identity. ns_override: an MSF-02 track whose
     * namespace differs, field by field, from the receiver's; it subscribes
     * under its own namespace. */
    bool             has_ns_fields;
    bool             ns_override;
    moq_namespace_t  ns_fields;
    void            *ns_block;
    size_t           ns_block_size;

    /* -- Replacement (MSF-02 critical change) ------------------------- *
     * replaced: this handle was removed by replacement; successor names the
     * handle that took its place (a chain, newest last), so a command issued
     * on it acts on the chain's head. gate: the predecessor whose facade
     * slot must be released before this handle may subscribe (drafts 16/18
     * allow one subscription per track; draft 22 discourages overlapping
     * filters); NULL once open. All under r->mu; handles are never recycled
     * before destroy, so the pointers stay valid. */
    bool             replaced;
    moq_media_track_t *successor;
    moq_media_track_t *gate;
};

/* One later description of a track (see moq_media_track.gens): the complete
 * description and the storage it alone owns. */
typedef struct receiver_desc_gen {
    struct receiver_desc_gen *next;
    moq_media_track_desc_t desc;
    moq_rcbuf_t           *init_buf;
    uint8_t               *desc_strings;
    size_t                 desc_strings_size;
    moq_bytes_t           *desc_depends_arr;
    moq_bytes_t           *desc_cp_refs_arr;
    uint8_t               *desc_ext;
    size_t                 desc_ext_size;
} receiver_desc_gen_t;

typedef struct receiver_event {
    moq_media_track_event_kind_t kind;
    moq_media_track_t           *track;
    /* per-event copies of the UPDATE_OK scalars, filled at PUSH time
     * (deriving them at poll from track state would corrupt multiple
     * queued acknowledgments). Zero for every other kind. */
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    uint64_t initiating_request_id;
    uint32_t fill_reason, fill_error_domain;
    uint64_t fill_error_code;
    int32_t local_result;
    uint32_t delivery_origin;
    uint64_t delivery_initiating_request_id;
    bool     has_done_status;              /* TRACK_ENDED from a done */
    uint64_t done_status;
    uint64_t fill_cmd_seq;                 /* FILL_CREATED: its command */
    /* The description and generation the event announces, taken when it
     * is queued, so a later generation never changes what an earlier event
     * says. */
    const moq_media_track_desc_t *desc;
    uint32_t config_generation;
    moq_media_track_t *replaces;           /* TRACK_ADDED of a replacement */
    moq_media_track_t *replaced_by;        /* TRACK_REMOVED of a replacement */
    uint64_t fetch_cmd_seq;                /* FETCH_CREATED, or a NOT_SENT
                                              FETCH_TERMINAL: its command */
} receiver_event_t;

/* Identity/intent only, not a second item queue. The facade keeps each child
 * slot until its terminal is polled; this table uses exactly that same bound.
 * A service handle is stable, even when its facade generation is replaced. */
typedef struct receiver_fill {
    moq_sub_fill_t token;
    moq_media_track_t *track;
    uint64_t initiating_request_id;
    bool cancel_pending;
    bool terminal_seen;
    bool delivery_failed;
} receiver_fill_t;

/* moq_media_track.cmd_state */
enum { FILL_CMD_NONE = 0, FILL_CMD_QUEUED, FILL_CMD_CLAIMED, FILL_CMD_ADMITTED,
       FILL_CMD_RECEIPT };
/* moq_media_track.fetch_cmd_state */
enum { FETCH_CMD_NONE = 0, FETCH_CMD_QUEUED, FETCH_CMD_CLAIMED,
       FETCH_CMD_RECEIPT };

typedef struct receiver_catalog_bootstrap {
    bool active, ack_seen, has_largest, have_base, terminal_seen;
    moq_sub_fill_t child;
    uint32_t terminal_reason;
    uint64_t largest_group, largest_object;
    uint64_t group, object, retained_bytes;
} receiver_catalog_bootstrap_t;

/* A loss not yet reported to the application, coalesced: the OR of its
 * causes (MOQ_MEDIA_FILL_LOSS_*, zero for none) and where the contribution
 * earliest in delivery order starts, with that delivery position. */
typedef struct receiver_loss_carry {
    uint32_t causes;
    uint64_t pos;
    uint64_t group, object;
} receiver_loss_carry_t;

/* One fill of an MPEG2TS media track: its own delivery domain. The record
 * is taken at the facade's CREATED and kept until its FILL_TERMINAL is
 * polled. LIVE until the facade terminal is read, then RETIRED until the
 * last of its queued Objects is polled or dropped, then READY: pollable,
 * placed in event order at ready_seq. `pending` is loss with no later
 * queued Object of the fill to report it; at READY it is the summary. The
 * catalog fill and fills of other tracks never take one. */
#define RECEIVER_FILL_DOMS (2u * RECEIVER_SUB_MAX_FILLS)
/* One standalone fetch of a media track (moq_media_receiver_fetch_track)
 * takes a record of its own, of any packaging, from FETCH_CREATED until its
 * FETCH_TERMINAL is polled: the same life as a fill's, keyed by the facade
 * request while LIVE. Fetch records follow the fill records in one table, so
 * an Object's `dom` names either; fills allocate and look up only in the
 * first RECEIVER_FILL_DOMS, fetches only in the rest, and neither pool
 * takes from the other. */
#define RECEIVER_FETCH_DOMS (2u * RECEIVER_SUB_MAX_FETCHES)
#define RECEIVER_DOMS (RECEIVER_FILL_DOMS + RECEIVER_FETCH_DOMS)
_Static_assert(RECEIVER_DOMS <= UINT8_MAX, "an Object names its record + 1 in a uint8_t");
enum { DOM_FREE = 0, DOM_LIVE, DOM_RETIRED, DOM_READY };
typedef struct receiver_fill_dom {
    uint8_t state;
    bool emit;                     /* a silent duplicate terminal is false */
    moq_sub_fill_t token;          /* the facade child, while LIVE */
    moq_media_track_t *track;
    uint32_t gen;                  /* the track generation it was created in */
    uint64_t initiating_request_id;
    uint32_t queued;               /* its Objects in the object queue */
    uint64_t next_pos;             /* next delivery-order position */
    bool has_prev;                 /* the previous Object or range end */
    uint64_t prev_group, prev_object;
    receiver_loss_carry_t pending;
    uint32_t reason, error_domain;
    uint64_t error_code;
    uint32_t ready_seq;            /* event-queue position it is served at */
    uint64_t ready_order;          /* oldest READY first among equal ones */
    /* An MPEG2TS fill: its loss is also carried as Location-tracked gaps.
     * With the stream-terminal lane, every fill's record orders its terminal
     * and keys its loss records. */
    bool ts;
    uint32_t st_owed;              /* its RECEIVER_DROP records not yet polled */
    /* A standalone fetch's record: reason/error_domain/error_code hold a
     * MOQ_MEDIA_FETCH_* reason and domain, local_result its local cause.
     * fetch_req is the facade request while LIVE; cancel_pending is the
     * application's cancel_fetch, which the executor carries out. */
    bool fetch;
    bool cancel_pending;
    int32_t local_result;
    moq_sub_fetch_req_t *fetch_req;
} receiver_fill_dom_t;

struct moq_media_receiver {
    moq_alloc_t     alloc;
    moq_media_auth_source_t auth_source;
    moq_media_auth_operation_t catalog_auth;
    moq_media_auth_operation_t fetch_auth;
    moq_endpoint_t *ep;
    bool            owns_endpoint;

    /* The negotiated transport draft, latched from the LIVE SESSION at the
     * top of every receiver_hook and read under mu wherever an object's
     * properties are parsed. Not endpoint configuration, not an offered
     * preference. Zero means "not yet observed" and is refused by
     * moq_media_object_parse rather than being read as a draft. */
    moq_version_t   transport_version;
    /* The companion in effect. Explicit LEGACY/D22 is fixed at creation.
     * AUTO stays MOQ_MEDIA_COMPANION_AUTO until the first latch of a
     * draft-16/18/22 session replaces it, once and under mu, with the
     * matching companion; the network thread reads it freely, the app
     * thread under mu. */
    moq_media_companion_profile_t companion_profile;
    /* The configuration asked for AUTO. Set at creation and never changed:
     * an AUTO receiver resolved to LOC-05 keeps the poll floor through
     * initiating_request_id, while an explicit LOC-05 one requires the
     * codec_string block. */
    bool companion_auto;

    /* Owned copies of the configuration inputs. */
    moq_bytes_t    *ns_parts;
    uint8_t        *ns_data;
    size_t          ns_data_size;
    moq_namespace_t namespace_;
    uint8_t        *catalog_name;
    size_t          catalog_name_len;
    bool            auto_subscribe;       /* stored; data path acts on it */
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;    /* stored; data path consumes it */

    /* Network-thread state. */
    moq_subscriber_t  *sub;
    /* Reserved when the receiver is built on an endpoint, so destroy never
     * allocates: the subscriber's teardown context and the endpoint task
     * node that carries it to the network thread (receiver_teardown_t). */
    struct receiver_teardown *teardown;
    moq_endpoint_task_slot_t *teardown_slot;
    moq_sub_track_t   *catalog_sub;
    moq_sub_fetch_req_t *catalog_fetch;   /* joining FETCH(offset=0); MSF-01 §5 */
    bool               catalog_fetch_issued;  /* issued exactly once */
    /* How this session joins an ongoing track, asked of the session when the
     * catalog subscription is made: a draft with fills (draft-22 section
     * 3.5) joins at the current group with a Next Object filter plus a fill
     * whose location is the current group from its start; a draft with
     * joining FETCH (16, 18) joins with a Joining FETCH(offset 0). */
    bool               joins_by_fill;
    /* The session was already draining when this receiver first saw it: the
     * subscriber facade exists only to own the session's events (and so
     * record the GOAWAY); nothing is subscribed on the draining session. */
    bool               observe_only;
    moq_sub_fill_t     catalog_fill;     /* the catalog SUBSCRIBE's fill */
    bool               catalog_ack_failed; /* ACK/bootstrap/delivery loss: retire after fatal */
    bool               catalog_done_pending;
    receiver_catalog_bootstrap_t catalog_bootstrap; /* executor-owned */

    /* The current effective independent catalog (MSF-01 §5). `has_effective`
     * gates first-catalog handling; `effective` is the base for the next update
     * and what find_content_protection resolves against. Replaced on each
     * accepted update; the previous one is freed unless it carried
     * contentProtections, in which case it is retained in `cp_snaps` so any
     * pointer find_content_protection previously returned stays valid for the
     * receiver's life. */
    bool               has_effective;
    moq_msf_catalog_t  effective;
    moq_msf_catalog_t *cp_snaps;          /* retained old effectives w/ CP */
    size_t             cp_snap_count, cp_snap_cap;

    /* Catalog-update ordering (§5): the latest group seen and whether we are
     * desynced (a dropped/failed update; in-group deltas are then ignored until
     * the next independent object 0 re-syncs). */
    uint64_t           latest_group;
    bool               have_latest_group;
    bool               desynced;
    uint64_t           cur_obj;       /* highest catalog object id applied in
                                         latest_group (0 after its independent;
                                         deltas advance it 1,2,3...) */

    /* Handle list: an array of individually-allocated handles, so the array can
     * grow across updates without moving a handle a caller holds. */
    moq_media_track_t **tracks;
    size_t             track_count, track_cap;

    /* Receiver-lifetime caps on retained catalog-update state (availability).
     * Stable handles (incl. removed ones, kept alive per the public lifetime
     * contract) and CP snapshots are never freed before destroy, so a peer
     * feeding endless valid generations could grow them without bound. Caps are
     * preflighted before any mutation; once an effective catalog exists, an
     * over-cap update is a bounded drop (catalog_drops, current effective kept),
     * and a first catalog over cap terminalizes CATALOG_UNUSABLE. retained_bytes
     * is the estimated bytes held by stable handles + CP snapshots; it only ever
     * grows. Defaults are conservative; unit tests lower them via the seam. */
    size_t             max_stable_handles;
    size_t             max_cp_snaps;
    uint64_t           max_retained_bytes;
    uint64_t           retained_bytes;

    /* Data-path config + network-thread state. */
    uint32_t         obj_cap;     /* policy threshold */
    uint32_t         ring_cap;    /* physical ring size (>= obj_cap;
                                     FLOW_CONTROL's absorb ceiling) */
    uint64_t         byte_cap;
    bool             subscribed_media;   /* >= 1 media subscription issued */
    bool             paused;             /* FLOW_CONTROL pause in effect */
    bool             resume_nudged;      /* wake task posted, not yet seen
                                            by the hook (mu-protected) */
    bool             epoch_set;          /* SHARED_EPOCH anchor latched */
    uint64_t         epoch_us;

    /* MPEG2TS options (copied at create), and the earliest reorder-window
     * deadline over the TS tracks, cached by the hook (r->mu) for the
     * endpoint's next_deadline_us query. */
    uint32_t         ts_slots;
    uint64_t         ts_window_us;
    bool             ts_strip_prefix;
    bool             ts_join_psi;
    uint64_t         ts_deadline_us;

    /* App-visible state (mu-protected). */
    pthread_mutex_t  mu;
    receiver_event_t *events;
    uint32_t         ev_cap, ev_head, ev_tail;
    receiver_fill_t fills[RECEIVER_SUB_MAX_FILLS]; /* mu-protected */
    uint64_t        fill_cmd_next;    /* last fill command identity issued */
    bool            fill_blocked; /* app event drain must wake the hook */
    receiver_fill_dom_t fill_doms[RECEIVER_DOMS]; /* fills, then fetches;
                                                     mu-protected */
    uint32_t        max_fetches_per_track; /* fixed at creation */
    uint64_t        fetch_cmd_next;   /* last fetch command identity issued */
    uint32_t        fill_ready_count;  /* READY records */
    uint64_t        fill_ready_order;
    bool            fill_dom_wake;     /* a record became READY or freed:
                                          wake after unlocking */
    /* Number of tracks with a parse-drop diagnostic pending (see
     * moq_media_track). Lets wait()/poll_track test for pending parse drops
     * cheaply without scanning every track. */
    uint32_t         parse_drop_pending_count;
    uint32_t         local_failure_pending_count;
    struct receiver_obj_entry *objs;     /* ring; entries own their refs */
    uint64_t         obj_sizes_total;    /* queued payload bytes */
    uint64_t         obj_budget_total;   /* payload + D22 track copies, including
                                           the network executor's conversion */
    uint32_t         obj_head, obj_tail;
    uint32_t         catalog_objects_queued; /* tagged bootstrap slots, mu-protected */

    /* Bounded SAP-record queue (CMSF §3.6.1). Plain value records (no refs);
     * drops oldest when the app under-polls. */
    moq_media_sap_record_t *saps;
    uint32_t         sap_cap, sap_head, sap_tail;
    /* Bounded media-timeline record queue (MSF §7.1.1). Same shape as the SAP
     * queue: plain values, drop-oldest under-poll. */
    moq_media_timeline_record_t *mts;
    uint32_t         mt_cap, mt_head, mt_tail;
    bool             fatal;
    uint64_t         fatal_code;
    moq_media_receiver_stats_t stats;    /* counter fields only */
    uint64_t         track_events_polled;

    /* Stream-terminal lane (surface_stream_terminals), mu-protected. Each
     * Object taken in gets the next ingress sequence; a record waits for
     * every queued or held Object of its track below its barrier. */
    bool             st_enabled;
    struct receiver_st_entry *st;
    uint32_t         st_cap, st_head, st_tail;
    bool             st_blocked;       /* intake stopped for lane room */
    bool             st_collect;       /* an admission's policy drops are recorded */
    uint64_t         ingress_seq;      /* next sequence to hand out */
    uint64_t         intake_seq;       /* the Object being taken in */
    /* Delivery domains with queued entries: a track's LIVE entries, or one
     * fill's. */
    uint32_t         domains_queued;
    uint32_t         st_admit_from;    /* lane tail when the admission began */
    /* Lane slots reserved for records owed later: one for the admission in
     * progress (st_adm), and one per Object, status Object or END_OF_TRACK
     * a reorder window holds or has parked. */
    uint32_t         st_reserved;
    bool             st_adm;
    /* Reorder windows whose next release waits for room; ts_wake asks the
     * application's next poll to wake the network thread. Intake waits for
     * them: the application's polls can return the room before the window
     * resumes, and an intake then must not replace what the window has
     * parked. */
    uint32_t         ts_stalled;
    bool             ts_wake;
    /* A stream terminal taken from the facade that waits for lane room or
     * for its track's pending records; intake stops while it does. */
    bool             st_has_held;
    moq_media_stream_terminal_t st_held;
};

typedef struct receiver_st_entry {
    moq_media_stream_terminal_t rec;
    uint64_t                    barrier;
    uint8_t                     dom;     /* RECEIVER_DROP: its fill record + 1 */
    /* Clear while an MPEG2TS reorder window may still hold Objects of the
     * track taken in before it; the network thread sets it. */
    bool                        ready;
} receiver_st_entry_t;

typedef struct receiver_obj_entry {
    moq_media_object_t obj;
    uint64_t           group_id;   /* wire identity for group-aware drops */
    uint64_t           budget;     /* this entry's admitted byte charge */
    bool               catalog;    /* internal live catalog staging, not app data */
    /* Delivery domain: a fill record index + 1, or 0 for a LIVE Object of
     * its track's subscription generation `gen`. A fill Object's `pos` is
     * its delivery-order position in the fill and `carry` the loss its
     * report holds. */
    uint8_t            dom;
    uint32_t           gen;
    uint64_t           pos;
    receiver_loss_carry_t carry;
    /* Queued while generation `gen`'s join PSI was not yet delivered: the
     * payload starts with join_psi_len octets of that PSI (zero when none was
     * prepended). Whether the PSI goes out with it is decided when the
     * application polls it; otherwise the payload skips them. */
    bool               join_candidate;
    uint32_t           join_psi_len;
    /* Ingress sequence (stream-terminal lane). */
    uint64_t           seq;
} receiver_obj_entry_t;

static bool track_is_ts(const moq_media_track_t *t);
static void dom_ready_locked(moq_media_receiver_t *r, receiver_fill_dom_t *d);

/* Release one queue entry's owned buffers (any thread that has exclusive
 * access: the network thread for evictions, the destroying thread once
 * detached). */
static void obj_entry_release(moq_media_receiver_t *r,
                              moq_media_object_t *o)
{
    if (o->payload_ref)    moq_rcbuf_decref(o->payload_ref);
    if (o->properties_ref) moq_rcbuf_decref(o->properties_ref);
    if (o->track_properties_ref) moq_rcbuf_decref(o->track_properties_ref);
    if (o->samples_owned)
        r->alloc.free(o->samples_owned,
                      o->sample_count * sizeof(moq_cmaf_sample_t),
                      r->alloc.ctx);
    memset(o, 0, sizeof(*o));
}

static size_t obj_entry_bytes(const moq_media_object_t *o)
{
    return o->payload_ref ? moq_rcbuf_len(o->payload_ref) : 0;
}

/* Each charge has one owner: conversion, then a queue entry, then none. */
static void obj_uncharge_locked(moq_media_receiver_t *r, uint64_t budget)
{
    assert(r->obj_budget_total >= budget);
    r->obj_budget_total -= budget;
}

static void obj_unqueue_locked(moq_media_receiver_t *r, receiver_obj_entry_t *e)
{
    if (r->st_enabled && !e->catalog && !e->dom && e->obj.track) {
        moq_media_track_t *t = e->obj.track;
        assert(t->st_queued > 0);
        if (--t->st_queued == 0) {
            assert(r->domains_queued > 0);
            r->domains_queued--;
        }
    }
    if (e->catalog) {
        assert(r->catalog_objects_queued > 0);
        r->catalog_objects_queued--;
    } else {
        size_t payload = obj_entry_bytes(&e->obj);
        assert(r->obj_sizes_total >= payload);
        r->obj_sizes_total -= payload;
    }
    obj_uncharge_locked(r, e->budget);
    e->budget = 0;
}

static bool obj_bytes_fit(const moq_media_receiver_t *r, uint64_t need,
                          uint64_t limit)
{
    return need <= limit && r->obj_budget_total <= limit - need;
}

/* The draft-22 companions differ only in LOC revision. */
static bool receiver_companion_d22(moq_media_companion_profile_t c)
{
    return c == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC ||
           c == MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
}

/* Latch the session's draft (r->mu held): the transport version, and for
 * AUTO the companion matching it. Draft 22 is not a match for SHARED_EPOCH,
 * which the draft-22 companion refuses, so that pair stays unresolved and
 * fails like a mismatch. */
static void receiver_latch_version_locked(moq_media_receiver_t *r, moq_version_t v)
{
    r->transport_version = v;
    if (r->companion_profile == MOQ_MEDIA_COMPANION_AUTO) {
        if (v == MOQ_VERSION_DRAFT_22 &&
            r->time_mode != MOQ_MEDIA_TIME_SHARED_EPOCH)
            r->companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
        else if (v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18)
            r->companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    }
}

/* Shared by network reconciliation and app-thread poll nudge, under r->mu.
 * D22 includes any in-flight conversion charge; legacy remains depth-only. */
static bool obj_resume_ready_locked(const moq_media_receiver_t *r)
{
    return r->obj_tail - r->obj_head <= r->obj_cap / 2 &&
        (!receiver_companion_d22(r->companion_profile) ||
         r->obj_budget_total <= r->byte_cap / 2);
}

/* -- Stream-terminal lane (r->mu) -------------------------------------- */

static uint32_t st_free_locked(const moq_media_receiver_t *r)
{
    return r->st_cap - (r->st_tail - r->st_head);
}

/* Free slots that no owed record has reserved. */
static uint32_t st_unreserved_locked(const moq_media_receiver_t *r)
{
    uint32_t f = st_free_locked(r);
    assert(f >= r->st_reserved);
    return f - r->st_reserved;
}

/* Room for an intake: a RECEIVER_DROP per delivery domain with queued
 * entries, and the slot the admission reserves for its own record (its
 * drop or its END_OF_TRACK). A reorder window keeps that slot for the
 * Object while it holds it. Intake takes this room before it can destroy
 * anything, so no record ever waits for room; a lane of one record takes
 * in one Object at a time. */
static bool st_room_locked(const moq_media_receiver_t *r)
{
    return !r->st_enabled || st_unreserved_locked(r) > r->domains_queued;
}

/* The admission in progress gives up its reserved slot, which its own
 * record then takes. */
static void st_adm_release_locked(moq_media_receiver_t *r)
{
    if (!r->st_adm) return;
    assert(r->st_reserved > 0);
    r->st_adm = false;
    r->st_reserved--;
}

/* Whether the lane's head record may be polled: the network thread found
 * it clear of reorder windows, and no queued entry of its track was taken
 * in before it. */
static bool st_head_pollable_locked(const moq_media_receiver_t *r)
{
    if (!r->st_enabled || r->st_head == r->st_tail) return false;
    const receiver_st_entry_t *e = &r->st[r->st_head % r->st_cap];
    if (!e->ready) return false;
    for (uint32_t i = r->obj_head; i != r->obj_tail; i++) {
        const receiver_obj_entry_t *o = &r->objs[i % r->ring_cap];
        if (!o->catalog && o->obj.track == e->rec.track && o->seq < e->barrier)
            return false;
    }
    return true;
}

/* Queue a record behind everything taken in so far; during an admission,
 * behind everything taken in before the Object being admitted, which its
 * drops precede. The caller has checked room. A fill's loss record keeps
 * its fill's terminal back until it is polled. */
static void st_push_locked(moq_media_receiver_t *r,
                           const moq_media_stream_terminal_t *rec, uint8_t dom)
{
    assert(st_free_locked(r) > r->st_reserved);
    receiver_st_entry_t *e = &r->st[r->st_tail % r->st_cap];
    e->rec = *rec;
    e->rec.struct_size = (uint32_t)sizeof(e->rec);
    e->barrier = r->st_collect ? r->intake_seq : r->ingress_seq;
    e->dom = dom;
    e->ready = !track_is_ts(rec->track) && !rec->track->order;
    if (dom) r->fill_doms[dom - 1].st_owed++;
    r->st_tail++;
}

/* Place the held stream terminal when the lane has room for it. */
static void st_place_held_locked(moq_media_receiver_t *r)
{
    if (!r->st_enabled || !r->st_has_held || st_unreserved_locked(r) == 0)
        return;
    st_push_locked(r, &r->st_held, 0);
    r->st_has_held = false;
}

/* Take in the next Object only with room for its records and while no
 * reorder window waits for room; on success it gets the next ingress
 * sequence and its reserved slot. */
static bool receiver_intake_begin(moq_media_receiver_t *r)
{
    if (!r->st_enabled) return true;
    pthread_mutex_lock(&r->mu);
    st_place_held_locked(r);
    bool ok = !r->st_has_held && r->ts_stalled == 0 && st_room_locked(r);
    if (ok) {
        r->intake_seq = r->ingress_seq++;
        r->st_adm = true;
        r->st_reserved++;
    } else {
        r->st_blocked = true;
    }
    pthread_mutex_unlock(&r->mu);
    return ok;
}

/* The intake of one Object is over: a slot it neither used nor passed to a
 * reorder window returns, and a held stream terminal may take it. */
static void receiver_intake_end(moq_media_receiver_t *r)
{
    if (!r->st_enabled) return;
    pthread_mutex_lock(&r->mu);
    st_adm_release_locked(r);
    st_place_held_locked(r);
    pthread_mutex_unlock(&r->mu);
}

/* An overflow-policy drop of an Object of `t` in `group`, from delivery
 * domain `dom` (0 for LIVE, else its fill record + 1), while an admission
 * is recording them: one RECEIVER_DROP per track and domain per admission,
 * its range growing with each drop. The admission reserved the room. */
static void st_note_drop_locked(moq_media_receiver_t *r, moq_media_track_t *t,
                                uint8_t dom, uint32_t origin,
                                uint64_t initiating_request_id, uint64_t group)
{
    if (!r->st_enabled || !r->st_collect || !t) return;
    for (uint32_t i = r->st_admit_from; i != r->st_tail; i++) {
        receiver_st_entry_t *e = &r->st[i % r->st_cap];
        if (e->rec.kind != MOQ_MEDIA_STREAM_RECEIVER_DROP ||
            e->rec.track != t || e->dom != dom)
            continue;
        if (group < e->rec.group_id) e->rec.group_id = group;
        if (group > e->rec.last_group_id) e->rec.last_group_id = group;
        return;
    }
    moq_media_stream_terminal_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = MOQ_MEDIA_STREAM_RECEIVER_DROP;
    rec.track = t;
    rec.group_id = group;
    rec.last_group_id = group;
    rec.delivery_origin = origin;
    rec.initiating_request_id =
        origin != MOQ_MEDIA_DELIVERY_LIVE ? initiating_request_id : 0;
    st_push_locked(r, &rec, dom);
}

/* A media track's END_OF_TRACK, at its Location, in the slot the admission
 * it comes from reserved. */
static void st_end_of_track(moq_media_receiver_t *r, moq_media_track_t *t,
                            uint64_t group, uint64_t object)
{
    pthread_mutex_lock(&r->mu);
    st_adm_release_locked(r);
    if (r->st_enabled) {
        moq_media_stream_terminal_t rec;
        memset(&rec, 0, sizeof(rec));
        rec.kind = MOQ_MEDIA_STREAM_END_OF_TRACK;
        rec.track = t;
        rec.group_id = group;
        rec.object_id = object;
        st_push_locked(r, &rec, 0);
    }
    pthread_mutex_unlock(&r->mu);
}

/* -- Event queue (r->mu) ----------------------------------------------- */

/* Queue a track event with r->mu held; returns the queued entry, or NULL
 * after terminalizing on overflow. The entry carries the track's current
 * description and generation. */
static receiver_event_t *rq_push_locked(moq_media_receiver_t *r,
                                        moq_media_track_event_kind_t kind,
                                        moq_media_track_t *track)
{
    if (r->ev_tail - r->ev_head >= r->ev_cap) {
        /* Losing a discovery event is unrecoverable miswiring (§6.3):
         * terminalize, never drop silently. */
        r->fatal = true;
        r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW;
        return NULL;
    }
    receiver_event_t *e = &r->events[r->ev_tail % r->ev_cap];
    memset(e, 0, sizeof(*e));
    e->kind = kind;
    e->track = track;
    if (track) {
        e->desc = track->desc_cur;
        e->config_generation = track->config_gen;
    }
    r->ev_tail++;
    return e;
}

/* Free event-queue slots now (r->mu held). */
static uint32_t rq_room_locked(const moq_media_receiver_t *r)
{
    uint32_t used = r->ev_tail - r->ev_head;
    return used >= r->ev_cap ? 0 : r->ev_cap - used;
}

static bool rq_push(moq_media_receiver_t *r, moq_media_track_event_kind_t kind,
                    moq_media_track_t *track)
{
    pthread_mutex_lock(&r->mu);
    bool ok = rq_push_locked(r, kind, track) != NULL;
    pthread_mutex_unlock(&r->mu);
    return ok;
}

/* Coalesced, NON-FATAL parse-drop bookkeeping with r->mu ALREADY held. Bumps the
 * global and per-track counters; if this is the first pending diagnostic for the
 * track, marks it pending and bumps the receiver pending count. Returns true iff
 * a new diagnostic became pending (the caller must wake the endpoint after it
 * unlocks). Deliberately does NOT use the ordinary event queue (rq_push
 * terminalizes on overflow): a malformed-object flood must never make the
 * receiver fatal. */
static bool receiver_note_parse_drop_locked(moq_media_receiver_t *r,
                                            moq_media_track_t *track,
                                            moq_media_parse_drop_class_t cls)
{
    r->stats.parse_drops++;
    track->parse_drops++;
    track->parse_drop_last_class = cls;
    if (track->parse_drop_pending)
        return false;
    track->parse_drop_pending = true;
    r->parse_drop_pending_count++;
    return true;
}

/* Locking wrapper for the sites that do not already hold r->mu. Must NOT be
 * called while holding r->mu. */
static void receiver_note_parse_drop(moq_media_receiver_t *r,
                                     moq_media_track_t *track,
                                     moq_media_parse_drop_class_t cls)
{
    pthread_mutex_lock(&r->mu);
    bool newly_pending = receiver_note_parse_drop_locked(r, track, cls);
    pthread_mutex_unlock(&r->mu);
    if (newly_pending)
        moq_endpoint_wake(r->ep);
}

#ifdef MOQ_MEDIA_RECEIVER_TESTING
/* Models a concurrent app cancellation after selection, without reentry from
 * inside the selector. Never present in the shipping library. */
static void (*g_auth_hook)(moq_media_receiver_t *, moq_media_track_t *, void *);
static void *g_auth_hook_ctx;
void moq_media_receiver_test_set_auth_hook(
    void (*fn)(moq_media_receiver_t *, moq_media_track_t *, void *), void *ctx);
void moq_media_receiver_test_set_auth_hook(
    void (*fn)(moq_media_receiver_t *, moq_media_track_t *, void *), void *ctx)
{
    g_auth_hook = fn;
    g_auth_hook_ctx = ctx;
}
#define AUTH_PRE_ADMIT(r, t) \
    do { if (g_auth_hook) g_auth_hook((r), (t), g_auth_hook_ctx); } while (0)
/* Test-only interleaving hook: invoked by the reconciler BETWEEN the
 * {desired, app_gen} snapshot and the wire send, with no locks held, so a
 * test can toggle app intent at exactly the racy instant and prove the
 * snapshot rule causally. Never compiled into the shipping library. */
static void (*g_reconcile_race_hook)(void *ctx);
static void *g_reconcile_race_hook_ctx;
void moq_media_receiver_test_set_race_hook(void (*fn)(void *), void *ctx);
void moq_media_receiver_test_set_race_hook(void (*fn)(void *), void *ctx)
{
    g_reconcile_race_hook = fn;
    g_reconcile_race_hook_ctx = ctx;
}
#define RECONCILE_RACE_HOOK() \
    do { \
        if (g_reconcile_race_hook) \
            g_reconcile_race_hook(g_reconcile_race_hook_ctx); \
    } while (0)

/* Publication-contract phase observer, distinct from the reconcile/ack hook
 * above. One callback is invoked at four points that BRACKET the two critical
 * sections a correct publication contract must serialize: the effective-catalog
 * publication (writer PRE, before any access to has_effective/effective; writer
 * POST, after old-generation retention/detachment, publication of effective and
 * has_effective, and the catalog_complete mutation, but before the intended
 * unlock) and the public content-protection lookup (reader PRE, after the null
 * check but before reading has_effective/selecting effective; reader POST, after
 * the traversal result is selected, before the intended unlock). A
 * single-threaded test uses it, with moq_media_receiver_test_mu_held (below), to
 * probe whether the code under test holds r->mu at each boundary. The hook
 * observes; it neither locks the code under test nor supplies the missing lock.
 * `phase` is one of the constants below. Never compiled into the shipping lib. */
enum {
    MOQ_MR_WRITER_PRE  = 0,
    MOQ_MR_WRITER_POST = 1,
    MOQ_MR_READER_PRE  = 2,
    MOQ_MR_READER_POST = 3,
    /* Live->VOD descriptor writer (receiver_reconcile): PRE before the first
     * mutable-scalar write, POST after the last, before the intended unlock.
     * Observation only: it neither locks nor changes the write. */
    MOQ_MR_VOD_WRITER_PRE  = 4,
    MOQ_MR_VOD_WRITER_POST = 5,
    /* Description copy reader (moq_media_receiver_track_desc_copy): PRE after
     * the argument checks, before the ownership walk; POST after the copy,
     * before the intended unlock. */
    MOQ_MR_DESC_READER_PRE  = 6,
    MOQ_MR_DESC_READER_POST = 7
};
static void (*g_pub_phase_hook)(const moq_media_receiver_t *r, int phase,
                                void *ctx);
static void *g_pub_phase_hook_ctx;
void moq_media_receiver_test_set_pub_phase_hook(
    void (*fn)(const moq_media_receiver_t *, int, void *), void *ctx);
void moq_media_receiver_test_set_pub_phase_hook(
    void (*fn)(const moq_media_receiver_t *, int, void *), void *ctx)
{
    g_pub_phase_hook = fn;
    g_pub_phase_hook_ctx = ctx;
}
#define PUB_PHASE_HOOK(r, ph) \
    do { if (g_pub_phase_hook) g_pub_phase_hook((r), (ph), g_pub_phase_hook_ctx); } \
    while (0)
#else
#define AUTH_PRE_ADMIT(r, t) ((void)0)
#define RECONCILE_RACE_HOOK() ((void)0)
#define PUB_PHASE_HOOK(r, ph) ((void)0)
#endif

/* Pure attribution classifier: given the generation
 * SNAPSHOT taken together with the target computation and the current
 * reconciled generation, decide whether a wire update carries app
 * attribution and advance the reconciled generation -- never past the
 * snapshot (a later toggle stays unreconciled for the next pass). Used by
 * BOTH reconciler branches; exposed to tests via the seam below. */
static void attribution_classify(uint64_t gen_snap, uint64_t reconciled,
                                 bool *out_notify, uint64_t *out_reconciled)
{
    *out_notify = gen_snap > reconciled;
    *out_reconciled = gen_snap > reconciled ? gen_snap : reconciled;
}

static void receiver_set_fatal(moq_media_receiver_t *r, uint64_t code)
{
    pthread_mutex_lock(&r->mu);
    if (!r->fatal) {
        r->fatal = true;
        r->fatal_code = code;
    }
    pthread_mutex_unlock(&r->mu);
}

/* -- Object queue + overflow (network thread; r->mu inside) ------------ */

static moq_media_track_t *track_for_sub(moq_media_receiver_t *r,
                                        moq_sub_track_t *st)
{
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->sub_track == st) return r->tracks[i];
    return NULL;
}

/* -- Local loss on a Location-tracked track (r->mu) ---------------------- *
 * An MPEG2TS Object the reorder window released but the receiver then drops
 * -- by queue eviction, a purge, or refusal at admission -- is a hole the
 * window cannot see. The next Object of the track the app polls reports it
 * as its gap, so a gap covers network loss, rejection and local drops
 * alike. */

static bool loc_before(uint64_t g1, uint64_t o1, uint64_t g2, uint64_t o2)
{
    return g1 < g2 || (g1 == g2 && o1 < o2);
}

/* Merge a local loss starting at (g, o) into an Object's loss report. A
 * local loss is a known loss, so the run is no unconfirmed tail; its PCR span
 * is unknown; and the Object is discontinuous unless it holds a random
 * access point. */
static void loss_merge(moq_media_object_t *o, uint64_t g, uint64_t ob)
{
    if (!o->gap || loc_before(g, ob, o->gap_group, o->gap_object)) {
        o->gap_group = g;
        o->gap_object = ob;
        o->gap_duration_known = false;
        o->gap_duration_27mhz = 0;
    }
    o->has_loss_report = true;
    o->gap = true;
    o->gap_tail_unknown = false;
    if (!o->keyframe) o->discontinuous = true;
}

/* The Location a dropped Object's loss starts at: its own gap when it
 * reported one (that earlier run is lost to the app too), else itself. */
static void loss_start(const moq_media_object_t *o, uint64_t *g, uint64_t *ob)
{
    if (o->gap) { *g = o->gap_group; *ob = o->gap_object; }
    else        { *g = o->group_id;  *ob = o->object_id; }
}

/* Record a local loss of track `t`'s LIVE subscription generation `gen`
 * starting at (g, ob): the first queued LIVE Object of that generation at
 * ring position `from` or later reports it, or, when none is queued, the
 * next one the generation queues. A fill's Objects never take it. */
static void ts_note_loss_locked(moq_media_receiver_t *r, moq_media_track_t *t,
                                uint32_t gen, uint64_t g, uint64_t ob,
                                uint32_t from)
{
    for (uint32_t i = from; i != r->obj_tail; i++) {
        receiver_obj_entry_t *n = &r->objs[i % r->ring_cap];
        if (n->obj.track == t && n->dom == 0 && n->gen == gen &&
            n->obj.has_loss_report) {
            loss_merge(&n->obj, g, ob);
            return;
        }
    }
    if (t->ts_loss_pending && t->ts_loss_gen != gen)
        t->ts_loss_pending = false;
    if (!t->ts_loss_pending ||
        loc_before(g, ob, t->ts_loss_group, t->ts_loss_object)) {
        t->ts_loss_group = g;
        t->ts_loss_object = ob;
    }
    t->ts_loss_gen = gen;
    t->ts_loss_pending = true;
}

/* Merge carried loss `c` into `into`: the causes are ORed, and the earliest
 * delivery position, with its Location, is kept. Positions, not Locations,
 * are compared, so a descending fill keeps its first contribution. */
static void carry_merge(receiver_loss_carry_t *into, const receiver_loss_carry_t *c)
{
    if (!c->causes) return;
    if (!into->causes || c->pos < into->pos) {
        into->pos = c->pos;
        into->group = c->group;
        into->object = c->object;
    }
    into->causes |= c->causes;
}

/* A queued fill Object reports its carried loss as its gap: a known loss of
 * unknown duration, discontinuous unless it holds a random access point. */
static void dom_apply_carry(receiver_obj_entry_t *e)
{
    moq_media_object_t *o = &e->obj;
    o->has_loss_report = true;
    o->gap = true;
    o->gap_group = e->carry.group;
    o->gap_object = e->carry.object;
    o->gap_tail_unknown = false;
    o->gap_duration_known = false;
    o->gap_duration_27mhz = 0;
    if (!o->keyframe) o->discontinuous = true;
}

/* Record loss `c` of fill record `idx`: the first queued Object of the fill
 * at ring position `from` or later reports it, else the record keeps it for
 * the fill's next Object or, failing one, its FILL_TERMINAL. */
static void dom_note_loss_locked(moq_media_receiver_t *r, size_t idx,
                                 const receiver_loss_carry_t *c, uint32_t from)
{
    /* Only an MPEG2TS fill reports loss as Location-tracked gaps. */
    if (!r->fill_doms[idx].ts) return;
    for (uint32_t i = from; i != r->obj_tail; i++) {
        receiver_obj_entry_t *n = &r->objs[i % r->ring_cap];
        if (n->dom == idx + 1) {
            carry_merge(&n->carry, c);
            dom_apply_carry(n);
            return;
        }
    }
    carry_merge(&r->fill_doms[idx].pending, c);
}

/* A record's last Object left the queue after its facade terminal, or the
 * terminal came with none queued: it is pollable from now, after every event
 * already queued. A silent record is freed instead. */
static void dom_ready_locked(moq_media_receiver_t *r, receiver_fill_dom_t *d)
{
    if (!d->emit) {
        memset(d, 0, sizeof(*d));
    } else {
        d->state = DOM_READY;
        d->ready_seq = r->ev_tail;
        d->ready_order = r->fill_ready_order++;
        r->fill_ready_count++;
    }
    r->fill_dom_wake = true;
}

/* A queue entry leaves the queue, delivered or dropped. */
static void dom_unqueue_locked(moq_media_receiver_t *r, const receiver_obj_entry_t *e)
{
    if (!e->dom) return;
    receiver_fill_dom_t *d = &r->fill_doms[e->dom - 1];
    assert(d->queued > 0);
    d->queued--;
    if (r->st_enabled && d->queued == 0) {
        assert(r->domains_queued > 0);
        r->domains_queued--;
    }
    if (d->queued == 0 && d->st_owed == 0 && d->state == DOM_RETIRED)
        dom_ready_locked(r, d);
}

/* A queue entry at ring position `idx` is being dropped: carry its loss to
 * the next Object of its own delivery domain when it tracks Locations. A
 * fill Object's drop is a local drop at its own Location, or at the start of
 * the loss it was to report. */
static void obj_note_drop_locked(moq_media_receiver_t *r,
                                 const receiver_obj_entry_t *e, uint32_t idx)
{
    if (!e->catalog)
        st_note_drop_locked(r, e->obj.track, e->dom, e->obj.delivery_origin,
                            e->obj.initiating_request_id, e->group_id);
    if (e->dom) {
        receiver_loss_carry_t c = e->carry;
        if (!c.causes) {
            c.pos = e->pos;
            c.group = e->obj.group_id;
            c.object = e->obj.object_id;
        }
        c.causes |= MOQ_MEDIA_FILL_LOSS_LOCAL_DROP;
        dom_note_loss_locked(r, (size_t)e->dom - 1, &c, idx + 1);
        return;
    }
    if (!e->obj.track || !e->obj.has_loss_report) return;
    uint64_t g, ob;
    loss_start(&e->obj, &g, &ob);
    ts_note_loss_locked(r, e->obj.track, e->gen, g, ob, idx + 1);
}

/* Evict the queue head, counting it, and arm the owning track's
 * straggler filter (monotonic: every group at or below the evicted one
 * is dropped content). Caller holds r->mu. */
static void obj_evict_head_locked(moq_media_receiver_t *r)
{
    receiver_obj_entry_t *h = &r->objs[r->obj_head % r->ring_cap];
    moq_media_track_t *t = h->obj.track;
    if (t && (!t->has_drop_filter || h->group_id > t->drop_below_group)) {
        t->has_drop_filter = true;
        t->drop_below_group = h->group_id;
    }
    obj_note_drop_locked(r, h, r->obj_head);
    dom_unqueue_locked(r, h);
    obj_unqueue_locked(r, h);
    r->stats.objects_dropped++;
    if (h->obj.keyframe) r->stats.keyframes_dropped++;
    obj_entry_release(r, &h->obj);
    r->obj_head++;
}

/* Drop the entry at ring position `idx` during a compaction walk
 * (counting). Caller holds r->mu. */
static void obj_release_entry_locked(moq_media_receiver_t *r,
                                     receiver_obj_entry_t *e, uint32_t idx)
{
    obj_note_drop_locked(r, e, idx);
    dom_unqueue_locked(r, e);
    obj_unqueue_locked(r, e);
    r->stats.objects_dropped++;
    if (e->obj.keyframe) r->stats.keyframes_dropped++;
    obj_entry_release(r, &e->obj);
}

/* Compact the queue, keeping only entries `keep()` accepts; relative
 * order is preserved. Subgroup streams deliver concurrently, so entries
 * of an evicted group can sit ANYWHERE in the queue, not just in the
 * head run -- every purge must walk the whole ring. Caller holds r->mu. */
typedef bool (*obj_keep_fn)(moq_media_receiver_t *r,
                            const receiver_obj_entry_t *e, void *ctx);

static void obj_compact_locked(moq_media_receiver_t *r,
                               obj_keep_fn keep, void *ctx)
{
    uint32_t rd = r->obj_head, wr = r->obj_head;
    while (rd != r->obj_tail) {
        receiver_obj_entry_t *e = &r->objs[rd % r->ring_cap];
        if (keep(r, e, ctx)) {
            if (wr != rd) {
                r->objs[wr % r->ring_cap] = *e;
                memset(e, 0, sizeof(*e));
            }
            wr++;
        } else {
            obj_release_entry_locked(r, e, rd);
            memset(e, 0, sizeof(*e));
        }
        rd++;
    }
    r->obj_tail = wr;
}

typedef struct {
    moq_media_track_t *track;
    uint64_t           group;
} purge_group_ctx_t;

static bool keep_not_group(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r;
    const purge_group_ctx_t *p = (const purge_group_ctx_t *)ctx;
    return e->obj.track != p->track || e->group_id != p->group;
}

/* Keep everything except entries belonging to `ctx` (a track handle): used to
 * purge queued objects when a track is unsubscribed. */
static bool keep_not_track(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r;
    return e->obj.track != (const moq_media_track_t *)ctx;
}

/* Evict the head's entire group, wherever its entries sit in the queue
 * (a partial group is useless to a decoder), and arm the straggler
 * filter so in-flight remnants are discarded too. Caller holds r->mu. */
static void obj_evict_head_group_locked(moq_media_receiver_t *r,
                                        uint64_t *out_group,
                                        moq_media_track_t **out_track)
{
    receiver_obj_entry_t *h = &r->objs[r->obj_head % r->ring_cap];
    moq_media_track_t *track = h->obj.track;
    *out_track = track;
    *out_group = h->group_id;
    purge_group_ctx_t ctx = { track, h->group_id };
    obj_compact_locked(r, keep_not_group, &ctx);
    r->stats.groups_dropped++;
}

/* DROP_TO_KEYFRAME chain compaction: after a prefix eviction, a delta is
 * decodable only if its group's chain is still intact -- through a
 * keyframe kept earlier in the walk, or because the group was never
 * touched by an eviction (its older objects were already delivered).
 * Caller holds r->mu; uses the per-track walk scratch. */
static bool keep_decodable(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r; (void)ctx;
    moq_media_track_t *t = e->obj.track;
    if (!t) return true;
    if (e->obj.keyframe) {
        t->walk_key_seen = true;
        t->walk_key_group = e->group_id;
        return true;
    }
    if (t->walk_key_seen && e->group_id == t->walk_key_group)
        return true;
    return !t->has_drop_filter || e->group_id > t->drop_below_group;
}

static void obj_compact_to_keyframes_locked(moq_media_receiver_t *r)
{
    for (size_t i = 0; i < r->track_count; i++)
        r->tracks[i]->walk_key_seen = false;
    obj_compact_locked(r, keep_decodable, NULL);
}

/* The per-track straggler filter, applied to an arriving (already
 * parsed) object. DROP_TO_KEYFRAME exempts keyframes: an arriving sync
 * point restarts the decode chain and clears the filter. The group
 * policies drop everything of a dropped-or-older group; a newer group
 * clears. Caller holds r->mu. */
static bool obj_filtered_locked(moq_media_receiver_t *r,
                                moq_media_track_t *track,
                                uint64_t group_id, bool keyframe)
{
    if (!track->has_drop_filter) return false;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME) {
        if (keyframe) {
            track->has_drop_filter = false;
            return false;
        }
        return group_id <= track->drop_below_group;
    }
    if (group_id > track->drop_below_group) {
        track->has_drop_filter = false;
        return false;
    }
    return true;
}

/* Make room for one incoming object of `need` bytes per the configured
 * policy. Returns true when the object should be enqueued, false when the
 * policy says discard it (already counted). Caller holds r->mu. */
static bool obj_make_room_locked(moq_media_receiver_t *r,
                                 moq_media_track_t *track,
                                 uint64_t group_id, bool keyframe,
                                 uint64_t need)
{
    bool full = (r->obj_tail - r->obj_head >= r->obj_cap) ||
                !obj_bytes_fit(r, need, r->byte_cap);
    if (!full) return true;

    r->stats.overflow_events++;

    switch (r->overflow.policy) {
    case MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME:
        /* Evict from the oldest end until there is room, advance the
         * head to a decodable start (keyframe), then purge mid-queue
         * deltas whose chain broke (subgroup streams interleave, so an
         * evicted group's remnants can sit behind a newer keyframe).
         * Never plain drop-oldest: a keyframe is never evicted while
         * its deltas stay. */
        while (r->obj_tail != r->obj_head &&
               ((r->obj_tail - r->obj_head >= r->obj_cap) ||
                !obj_bytes_fit(r, need, r->byte_cap)))
            obj_evict_head_locked(r);
        while (r->obj_tail != r->obj_head &&
               !r->objs[r->obj_head % r->ring_cap].obj.keyframe)
            obj_evict_head_locked(r);
        obj_compact_to_keyframes_locked(r);
        break;

    case MOQ_MEDIA_OVERFLOW_DROP_GROUP:
        /* Discard the oldest whole group (wherever its entries sit);
         * in-flight stragglers are discarded by the per-track filter. */
        while (r->obj_tail != r->obj_head &&
               ((r->obj_tail - r->obj_head >= r->obj_cap) ||
                !obj_bytes_fit(r, need, r->byte_cap))) {
            uint64_t g; moq_media_track_t *t;
            obj_evict_head_group_locked(r, &g, &t);
            if (t && (!t->has_drop_filter || g > t->drop_below_group)) {
                t->has_drop_filter = true;
                t->drop_below_group = g;
            }
        }
        break;

    case MOQ_MEDIA_OVERFLOW_FLOW_CONTROL: {
        /* Pause upstream (handled by the hook seeing r->paused) and absorb
         * the in-flight overshoot up to a soft ceiling standing in for the
         * session receive budget; past it the receiver degrades to a
         * counted group drop -- never a connection teardown. */
        if (!r->paused) {
            r->paused = true;
            r->stats.pause_transitions++;
        }
        uint32_t ceiling = r->obj_cap + r->obj_cap / 2;
        uint64_t byte_ceiling = r->byte_cap + r->byte_cap / 2;
        if ((r->obj_tail - r->obj_head < ceiling) &&
            obj_bytes_fit(r, need, byte_ceiling))
            return true;
        if (r->obj_tail != r->obj_head) {
            uint64_t g; moq_media_track_t *t;
            obj_evict_head_group_locked(r, &g, &t);
            if (t && (!t->has_drop_filter || g > t->drop_below_group)) {
                t->has_drop_filter = true;
                t->drop_below_group = g;
            }
        }
        break;
    }

    default:
        return true;   /* unreachable: policy validated at create */
    }

    /* The eviction may have taken the incoming object's own chain with
     * it -- it is subject to the same straggler rule as a late arrival. */
    if (obj_filtered_locked(r, track, group_id, keyframe))
        return false;

    /* Hard budget: eviction empties at most the whole queue, so an
     * object that alone exceeds the byte budget (or the FLOW_CONTROL
     * ceiling) can never fit. Drop it and arm the straggler filter --
     * the rest of its group is undecodable without it. */
    uint64_t byte_limit = r->byte_cap;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
        byte_limit = r->byte_cap + r->byte_cap / 2;
    if (!obj_bytes_fit(r, need, byte_limit)) {
        if (!track->has_drop_filter || group_id > track->drop_below_group) {
            track->has_drop_filter = true;
            track->drop_below_group = group_id;
        }
        return false;
    }
    return true;
}

/* -- Catalog processing (network thread) -------------------------------- */

static bool span_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len) return false;
    return a.len == 0 || memcmp(a.data, b.data, a.len) == 0;
}

static bool span_eq_lit(moq_bytes_t a, const char *lit, size_t len)
{
    return a.len == len && memcmp(a.data, lit, len) == 0;
}

static void receiver_ts_free(moq_media_receiver_t *r, moq_media_track_t *t);

/* Release the selection a fill command holds, if any. */
static void receiver_cmd_auth_release(moq_media_receiver_t *r,
                                      moq_media_track_t *t)
{
    if (t->cmd_auth) {
        moq_media_auth_operation_clear(t->cmd_auth, &r->alloc);
        r->alloc.free(t->cmd_auth, sizeof(*t->cmd_auth), r->alloc.ctx);
        t->cmd_auth = NULL;
    }
    t->cmd_auth_seq = 0;
}

/* Free a description generation and the storage it owns. NULL-safe. */
static void receiver_gen_free(moq_media_receiver_t *r, receiver_desc_gen_t *g)
{
    if (!g) return;
    if (g->desc_ext) r->alloc.free(g->desc_ext, g->desc_ext_size, r->alloc.ctx);
    if (g->init_buf) moq_rcbuf_decref(g->init_buf);
    if (g->desc_depends_arr)
        r->alloc.free(g->desc_depends_arr,
                      g->desc.depends_count * sizeof(moq_bytes_t), r->alloc.ctx);
    if (g->desc_cp_refs_arr)
        r->alloc.free(g->desc_cp_refs_arr,
                      g->desc.content_protection_ref_id_count * sizeof(moq_bytes_t),
                      r->alloc.ctx);
    if (g->desc_strings)
        r->alloc.free(g->desc_strings, g->desc_strings_size, r->alloc.ctx);
    r->alloc.free(g, sizeof(*g), r->alloc.ctx);
}

static void receiver_order_free(moq_media_receiver_t *r, moq_media_track_t *t);

/* Release the selection a fetch command holds, if any. */
static void receiver_fetch_auth_release(moq_media_receiver_t *r,
                                        moq_media_track_t *t)
{
    if (t->fetch_auth) {
        moq_media_auth_operation_clear(t->fetch_auth, &r->alloc);
        r->alloc.free(t->fetch_auth, sizeof(*t->fetch_auth), r->alloc.ctx);
        t->fetch_auth = NULL;
    }
    t->fetch_auth_seq = 0;
}

static void receiver_track_free(moq_media_receiver_t *r, moq_media_track_t *t)
{
    if (!t) return;
    moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
    receiver_cmd_auth_release(r, t);
    receiver_fetch_auth_release(r, t);
    receiver_ts_free(r, t);
    receiver_order_free(r, t);
    while (t->gens) {
        receiver_desc_gen_t *g = t->gens;
        t->gens = g->next;
        receiver_gen_free(r, g);
    }
    if (t->ns_block) r->alloc.free(t->ns_block, t->ns_block_size, r->alloc.ctx);
    if (t->desc_ext)
        r->alloc.free(t->desc_ext, t->desc_ext_size, r->alloc.ctx);
    if (t->init_buf) moq_rcbuf_decref(t->init_buf);
    if (t->desc_depends_arr)
        r->alloc.free(t->desc_depends_arr,
                      t->desc.depends_count * sizeof(moq_bytes_t), r->alloc.ctx);
    if (t->desc_cp_refs_arr)
        r->alloc.free(t->desc_cp_refs_arr,
                      t->desc.content_protection_ref_id_count *
                          sizeof(moq_bytes_t), r->alloc.ctx);
    if (t->desc_strings)
        r->alloc.free(t->desc_strings, t->desc_strings_size, r->alloc.ctx);
    r->alloc.free(t, sizeof(*t), r->alloc.ctx);
}

static size_t ext_align8(size_t n) { return (n + 7u) & ~(size_t)7u; }

/* The handle-owned bytes of one supplement record: the record, then its
 * spans and its renderGroup array. SIZE_MAX when not representable. */
static size_t supplement_copy_size(const moq_msf_track_supplement_t *s)
{
    size_t n = ext_align8(sizeof(moq_msf_track_supplement_t));
    if (s->struct_size < MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE) return n;
    const moq_bytes_t spans[5] = { s->packaging_metadata, s->encryption_scheme,
                                   s->cipher_suite, s->key_id, s->track_base_key };
    for (size_t i = 0; i < 5; i++) {
        if (spans[i].len > SIZE_MAX / 4) return SIZE_MAX;
        size_t add = ext_align8(spans[i].len);
        if (add > SIZE_MAX / 2 - n) return SIZE_MAX;
        n += add;
    }
    if (s->depends_render_group_count > (SIZE_MAX / 4) / sizeof(int)) return SIZE_MAX;
    size_t add = ext_align8(s->depends_render_group_count * sizeof(int));
    if (add > SIZE_MAX / 2 - n) return SIZE_MAX;
    return n + add;
}

/* Whether the receiver copies the extension record `e` onto a handle: the
 * kinds it knows, with a record. */
static bool ext_copied(const moq_msf_track_ext_t *e)
{
    return (e->kind == MOQ_MSF_EXT_MPEG2TS && e->u.mpeg2ts) ||
           (e->kind == MOQ_MSF_EXT_SUPPLEMENT && e->u.supplement);
}

/* The handle-owned size of a track's extension list (see
 * receiver_copy_extensions); 0 when it carries no record this receiver
 * knows. Saturates at SIZE_MAX. */
static size_t ext_copy_size(const moq_msf_track_t *mt)
{
    size_t n = 0, entries = 0;
    for (size_t i = 0; i < mt->extension_count; i++) {
        const moq_msf_track_ext_t *e = &mt->extensions[i];
        if (!ext_copied(e)) continue;
        entries++;
        size_t add;
        if (e->kind == MOQ_MSF_EXT_SUPPLEMENT) {
            add = supplement_copy_size(e->u.supplement);
            if (add == SIZE_MAX) return SIZE_MAX;
        } else {
            const moq_msf_mpeg2ts_t *ts = e->u.mpeg2ts;
            add = ext_align8(sizeof(moq_msf_mpeg2ts_t));
            if (ts->si_pid_count > (SIZE_MAX / 2) / sizeof(uint32_t)) return SIZE_MAX;
            add += ext_align8(ts->si_pid_count * sizeof(uint32_t));
            add += ext_align8(ts->mode_name.len) +
                   ext_align8(ts->timestamp_mode_name.len);
        }
        if (add > SIZE_MAX / 2 - n) return SIZE_MAX;
        n += add;
    }
    if (entries == 0) return 0;
    return n + ext_align8(entries * sizeof(moq_msf_track_ext_t));
}

/* Copy `len` bytes to blk + *off and return the handle-owned span. */
static moq_bytes_t ext_copy_span(uint8_t *blk, size_t *off, moq_bytes_t src)
{
    if (src.len == 0) return (moq_bytes_t){ NULL, 0 };
    memcpy(blk + *off, src.data, src.len);
    moq_bytes_t out = { blk + *off, src.len };
    *off += ext_align8(src.len);
    return out;
}

/* Deep-copy the records this receiver knows (MOQ_MSF_EXT_MPEG2TS and
 * MOQ_MSF_EXT_SUPPLEMENT) into one handle-owned block: the parsed record
 * borrows the catalog DOM, which a later catalog replaces. The block is
 * ext_copy_size(mt) bytes, zeroed. */
static void receiver_copy_extensions_to(uint8_t *blk, const moq_msf_track_t *mt,
                                        const moq_msf_track_ext_t **out_list,
                                        size_t *out_count)
{
    size_t entries = 0;
    for (size_t i = 0; i < mt->extension_count; i++)
        if (ext_copied(&mt->extensions[i])) entries++;
    moq_msf_track_ext_t *list = (moq_msf_track_ext_t *)(void *)blk;
    size_t off = ext_align8(entries * sizeof(moq_msf_track_ext_t));
    size_t k = 0;
    for (size_t i = 0; i < mt->extension_count; i++) {
        const moq_msf_track_ext_t *e = &mt->extensions[i];
        if (!ext_copied(e)) continue;
        if (e->kind == MOQ_MSF_EXT_SUPPLEMENT) {
            const moq_msf_track_supplement_t *src = e->u.supplement;
            moq_msf_track_supplement_t *rec =
                (moq_msf_track_supplement_t *)(void *)(blk + off);
            off += ext_align8(sizeof(*rec));
            moq_msf_track_supplement_init(rec);
            if (src->struct_size >= MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE) {
                size_t n = src->struct_size < sizeof(*rec) ? src->struct_size
                                                           : sizeof(*rec);
                memcpy(rec, src, n);
                rec->struct_size = (uint32_t)sizeof(*rec);
                rec->packaging_metadata = ext_copy_span(blk, &off, src->packaging_metadata);
                rec->encryption_scheme = ext_copy_span(blk, &off, src->encryption_scheme);
                rec->cipher_suite = ext_copy_span(blk, &off, src->cipher_suite);
                rec->key_id = ext_copy_span(blk, &off, src->key_id);
                rec->track_base_key = ext_copy_span(blk, &off, src->track_base_key);
                rec->depends_render_groups = NULL;
                if (src->depends_render_group_count > 0 && src->depends_render_groups) {
                    int *groups = (int *)(void *)(blk + off);
                    memcpy(groups, src->depends_render_groups,
                           src->depends_render_group_count * sizeof(int));
                    rec->depends_render_groups = groups;
                    off += ext_align8(src->depends_render_group_count * sizeof(int));
                } else {
                    rec->depends_render_group_count = 0;
                }
            }
            list[k].kind = MOQ_MSF_EXT_SUPPLEMENT;
            list[k].u.supplement = rec;
            k++;
            continue;
        }
        const moq_msf_mpeg2ts_t *src = e->u.mpeg2ts;
        moq_msf_mpeg2ts_t *rec = (moq_msf_mpeg2ts_t *)(void *)(blk + off);
        off += ext_align8(sizeof(*rec));
        moq_msf_mpeg2ts_init(rec);
        size_t n = src->struct_size < sizeof(*rec) ? src->struct_size
                                                   : sizeof(*rec);
        memcpy(rec, src, n);
        rec->struct_size = (uint32_t)sizeof(*rec);
        rec->si_pids = NULL;
        rec->mode_name = (moq_bytes_t){ NULL, 0 };
        rec->timestamp_mode_name = (moq_bytes_t){ NULL, 0 };
        if (src->si_pid_count > 0 && src->si_pids) {
            uint32_t *pids = (uint32_t *)(void *)(blk + off);
            memcpy(pids, src->si_pids, src->si_pid_count * sizeof(uint32_t));
            rec->si_pids = pids;
            off += ext_align8(src->si_pid_count * sizeof(uint32_t));
        } else {
            rec->si_pid_count = 0;
        }
        rec->mode_name = ext_copy_span(blk, &off, src->mode_name);
        rec->timestamp_mode_name = ext_copy_span(blk, &off, src->timestamp_mode_name);
        list[k].kind = MOQ_MSF_EXT_MPEG2TS;
        list[k].u.mpeg2ts = rec;
        k++;
    }
    *out_list = list;
    *out_count = k;
}

static void receiver_copy_extensions(moq_media_track_t *t,
                                     const moq_msf_track_t *mt)
{
    receiver_copy_extensions_to(t->desc_ext, mt, &t->desc.extensions,
                                &t->desc.extension_count);
}

/* Whether this build can turn an Object of a track with `info` into a
 * poll_object record: the normalizer accepts the info and its packaging.
 * Decided with a status-only Object, which needs no payload. */
static uint32_t receiver_ts_packet_size(const moq_media_track_desc_t *desc)
{
    const moq_msf_mpeg2ts_t *ts =
        moq_msf_ext_mpeg2ts(desc->extensions, desc->extension_count);
    return ts ? ts->packet_size : 0;
}

/* Where a catalog track's decoder configuration comes from (see
 * moq_media_track_desc_t.init_source), as a handle build resolves it.
 * `data` is the inline init (empty for the in-band kinds and NONE); `usable`
 * false means the track has no delivery path: an MSF-02 initDataList entry
 * of a type this receiver does not know, or an in-band configuration this
 * receiver cannot take (see receiver_track_init). */
typedef struct receiver_init {
    moq_bytes_t data;
    uint32_t    source;
    uint64_t    property_type;
    bool        usable;
} receiver_init_t;

/* Resolve a catalog track's init against `src` (its own catalog).
 * MSF-01: inline initData wins, else the initRef entry's data, whatever its
 * type, as before. MSF-02 (5.1.7): inline initData wins; an initRef entry of
 * type "inline" gives its data; "track-property" and "object-property" carry
 * the configuration in band, in the property their data names (12.2), which
 * this receiver takes only for a "loc" track on a draft-22 companion, as
 * VIDEO_CONFIG (0x0D) on role "video" or AUDIO_CONFIG (0x0F) on role
 * "audio": LOC is the packaging those properties configure, and the legacy
 * companion carries no per-Object configuration. Any other type, or another
 * pairing, is not usable. An initRef naming no entry resolves to none, with
 * the path unchanged, in both revisions. */
static receiver_init_t receiver_track_init(const moq_media_receiver_t *r,
                                           const moq_msf_track_t *mt,
                                           const moq_msf_catalog_t *src)
{
    receiver_init_t in = { { NULL, 0 }, MOQ_MEDIA_INIT_SOURCE_NONE, 0, true };
    if (mt->has_init_data && mt->init_data.len > 0) {
        in.data = mt->init_data;
        in.source = MOQ_MEDIA_INIT_SOURCE_INLINE;
        return in;
    }
    if (!mt->has_init_ref || !src) return in;
    const moq_msf_init_data_entry_t *e =
        moq_msf_catalog_find_init_data(src, mt->init_ref);
    if (!e) return in;
    if (src->revision != MOQ_MSF_REVISION_02 || span_eq_lit(e->type, "inline", 6)) {
        in.data = e->data;
        in.source = in.data.len > 0 ? MOQ_MEDIA_INIT_SOURCE_INLINE
                                    : MOQ_MEDIA_INIT_SOURCE_NONE;
        return in;
    }
    bool track_prop = span_eq_lit(e->type, "track-property", 14);
    bool object_prop = span_eq_lit(e->type, "object-property", 15);
    if (!track_prop && !object_prop) { in.usable = false; return in; }
    in.source = track_prop ? MOQ_MEDIA_INIT_SOURCE_TRACK_PROPERTY
                           : MOQ_MEDIA_INIT_SOURCE_OBJECT_PROPERTY;
    uint64_t type = 0;
    bool typed = moq_msf_init_data_property_type(e, &type) == MOQ_OK;
    if (typed) in.property_type = type;
    bool video = mt->has_role && span_eq_lit(mt->role, "video", 5);
    bool audio = mt->has_role && span_eq_lit(mt->role, "audio", 5);
    in.usable = typed && span_eq_lit(mt->packaging, "loc", 3) &&
                ((video && type == 0x0Du) || (audio && type == 0x0Fu)) &&
                receiver_companion_d22(r->companion_profile);
    return in;
}

/* A catalog track's inline init data as a handle build resolves it; empty
 * when it has none. */
static moq_bytes_t receiver_resolved_init(const moq_media_receiver_t *r,
                                          const moq_msf_track_t *mt,
                                          const moq_msf_catalog_t *src)
{
    return receiver_track_init(r, mt, src).data;
}

/* Whether a handle built from `mt` holds a parsed CMAF header record: a
 * "cmaf" track with init data to parse. Every other packaging keeps a NULL
 * desc.cmaf and pays nothing for it. */
static bool receiver_track_holds_cmaf(const moq_media_receiver_t *r,
                                      const moq_msf_track_t *mt,
                                      const moq_msf_catalog_t *src)
{
    return span_eq_lit(mt->packaging, "cmaf", 4) &&
           receiver_resolved_init(r, mt, src).len > 0;
}

static bool receiver_parse_probe(const moq_media_receiver_t *r,
                                 const moq_media_track_desc_t *desc)
{
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.status = MOQ_OBJECT_END_OF_GROUP;
    moq_media_parsed_object_t out;
    moq_media_parsed_object_init(&out);
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.companion_profile = r->companion_profile;
    cfg.mpeg2ts_packet_size = receiver_ts_packet_size(desc);
    return moq_media_object_parse_sized(&desc->info, &in, &cfg, NULL, 0,
                                        &out, sizeof(out), NULL) == MOQ_OK;
}

/* Whether a namespace's fields equal another's, field by field, by exact
 * bytes. */
static bool ns_equal(moq_namespace_t a, moq_namespace_t b)
{
    if (a.count != b.count) return false;
    for (size_t i = 0; i < a.count; i++)
        if (!span_eq(a.parts[i], b.parts[i])) return false;
    return true;
}

/* MSF-02 delivery rules a handle build applies on top of the info and
 * parse probe (the receiver's policy for publisher MUSTs, and the
 * encryption boundary). True when the track keeps its delivery path. */
static bool receiver_track_policy_ok(const moq_msf_track_t *mt,
                                     const moq_msf_catalog_t *src,
                                     const receiver_init_t *init)
{
    /* MSF 4.4.1: an encrypted payload must be decrypted before processing.
     * The receiver interprets CMAF and MPEG-2 TS payloads and the timelines'
     * JSON, which MoQ Secure Objects would encrypt (MSF defines its scheme
     * for LOC, CMSF-01 4), so those have no path; a "loc" payload is handed
     * over as received and the application decrypts it. Both revisions. */
    const moq_msf_track_supplement_t *sup = moq_msf_track_supplement(mt);
    if (sup && sup->has_encryption_scheme &&
        !span_eq_lit(mt->packaging, "loc", 3))
        return false;
    if (!init->usable) return false;
    if (!src || src->revision != MOQ_MSF_REVISION_02) return true;
    /* MSF-02 5.2.4.1 / 5.2.19: a loc audio or video track names its codec;
     * without it no decoder can be chosen. */
    bool av = mt->has_role && (span_eq_lit(mt->role, "audio", 5) ||
                               span_eq_lit(mt->role, "video", 5));
    if (span_eq_lit(mt->packaging, "loc", 3) && av && !mt->has_codec)
        return false;
    return true;
}

/* Build a handle from a catalog track, copying every desc span into
 * handle-owned storage so the handle outlives the effective catalog. `src` is
 * the track's own catalog: it resolves an initRef and gives the revision the
 * track follows. Returns NULL on allocation failure. */
static moq_media_track_t *receiver_track_build(moq_media_receiver_t *r,
                                               const moq_msf_track_t *mt,
                                               const moq_msf_catalog_t *src)
{
    moq_media_track_t *t = (moq_media_track_t *)r->alloc.alloc(
        sizeof(*t), r->alloc.ctx);
    if (!t) return NULL;
    memset(t, 0, sizeof(*t));
    moq_media_track_desc_t *d = &t->desc;
    d->struct_size = (uint32_t)sizeof(*d);
    t->desc_cur = d;
    atomic_store_explicit(&t->desc_pub, d, memory_order_relaxed);
    bool r02 = src && src->revision == MOQ_MSF_REVISION_02;
    receiver_init_t init = receiver_track_init(r, mt, src);

    /* One owned buffer for the parsed CMAF header record (first, when the
     * track has one) then all string spans + the depends/cp-ref bytes + the
     * tuple-key namespace. The record sits at the start of the allocation, so
     * the allocator's alignment for any object -- which every typed block in
     * this file relies on -- is the record's alignment; the strings after it
     * are bytes. */
    bool holds_cmaf = receiver_track_holds_cmaf(r, mt, src);
    size_t strings_at = holds_cmaf ? sizeof(moq_cmaf_init_info_t) : 0;
    size_t need = mt->name.len + mt->packaging.len;
    if (mt->has_namespace) need += mt->namespace_.len;
    if (mt->has_role) need += mt->role.len;
    if (mt->has_codec) need += mt->codec.len;
    if (mt->has_lang) need += mt->lang.len;
    if (mt->has_label) need += mt->label.len;
    if (mt->has_channel_config) need += mt->channel_config.len;
    if (mt->has_event_type) need += mt->event_type.len;
    if (mt->has_mime_type) need += mt->mime_type.len;
    for (size_t i = 0; i < mt->depends_count; i++) need += mt->depends[i].len;
    for (size_t i = 0; i < mt->cp_ref_id_count; i++) need += mt->cp_ref_ids[i].len;
    if (need > SIZE_MAX - strings_at) { receiver_track_free(r, t); return NULL; }
    need += strings_at;

    if (need > 0) {
        t->desc_strings = (uint8_t *)r->alloc.alloc(need, r->alloc.ctx);
        if (!t->desc_strings) { receiver_track_free(r, t); return NULL; }
        t->desc_strings_size = need;
    }
    if (mt->depends_count > 0) {
        t->desc_depends_arr = (moq_bytes_t *)r->alloc.alloc(
            mt->depends_count * sizeof(moq_bytes_t), r->alloc.ctx);
        if (!t->desc_depends_arr) { receiver_track_free(r, t); return NULL; }
    }
    if (mt->cp_ref_id_count > 0) {
        t->desc_cp_refs_arr = (moq_bytes_t *)r->alloc.alloc(
            mt->cp_ref_id_count * sizeof(moq_bytes_t), r->alloc.ctx);
        if (!t->desc_cp_refs_arr) { receiver_track_free(r, t); return NULL; }
    }
    {
        size_t ext = ext_copy_size(mt);
        if (ext == SIZE_MAX) { receiver_track_free(r, t); return NULL; }
        if (ext > 0) {
            t->desc_ext = (uint8_t *)r->alloc.alloc(ext, r->alloc.ctx);
            if (!t->desc_ext) { receiver_track_free(r, t); return NULL; }
            memset(t->desc_ext, 0, ext);
            t->desc_ext_size = ext;
            receiver_copy_extensions(t, mt);
        }
    }
    /* The MSF-02 Full Track Name namespace of a track with "namespace":
     * its fields, split from the handle-owned copy below. */
    size_t ns_count = 0;
    if (r02 && mt->has_namespace) {
        if (moq_msf_namespace_split(mt->namespace_, NULL, 0, &ns_count) !=
                MOQ_ERR_BUFFER || ns_count == 0) {
            ns_count = 0;   /* does not split: no MSF-02 identity */
        } else {
            t->ns_block_size = ns_count * sizeof(moq_bytes_t);
            t->ns_block = r->alloc.alloc(t->ns_block_size, r->alloc.ctx);
            if (!t->ns_block) { receiver_track_free(r, t); return NULL; }
        }
    }

    size_t off = strings_at;
#define COPY_DESC_SPAN(dst, src_span) do { \
        if ((src_span).len) { \
            memcpy(t->desc_strings + off, (src_span).data, (src_span).len); \
            (dst) = (moq_bytes_t){ t->desc_strings + off, (src_span).len }; \
            off += (src_span).len; \
        } } while (0)
    COPY_DESC_SPAN(d->name, mt->name);
    COPY_DESC_SPAN(d->packaging, mt->packaging);
    if (mt->has_namespace) {
        t->key_has_ns = true;
        COPY_DESC_SPAN(t->key_ns, mt->namespace_);
    }
    if (mt->has_role) COPY_DESC_SPAN(d->role, mt->role);
    if (mt->has_codec) COPY_DESC_SPAN(d->codec, mt->codec);
    if (mt->has_lang) COPY_DESC_SPAN(d->lang, mt->lang);
    if (mt->has_label) COPY_DESC_SPAN(d->label, mt->label);
    if (mt->has_channel_config) COPY_DESC_SPAN(d->channel_config, mt->channel_config);
    if (mt->has_event_type) COPY_DESC_SPAN(d->event_type, mt->event_type);
    if (mt->has_mime_type) COPY_DESC_SPAN(d->mime_type, mt->mime_type);
    for (size_t i = 0; i < mt->depends_count; i++)
        COPY_DESC_SPAN(t->desc_depends_arr[i], mt->depends[i]);
    for (size_t i = 0; i < mt->cp_ref_id_count; i++)
        COPY_DESC_SPAN(t->desc_cp_refs_arr[i], mt->cp_ref_ids[i]);
#undef COPY_DESC_SPAN

    /* The namespace a subscription of this track names: the receiver's for a
     * track without one (both revisions); under MSF-02 a track's own fields
     * (5.2.2), which may override it; an MSF-01 namespace string has no
     * defined MoQT mapping, so such a track has no namespace to subscribe. */
    if (!mt->has_namespace) {
        t->has_ns_fields = true;
        t->ns_fields = r->namespace_;
    } else if (ns_count > 0) {
        moq_bytes_t *fields = (moq_bytes_t *)t->ns_block;
        size_t got = 0;
        if (moq_msf_namespace_split(t->key_ns, fields, ns_count, &got) == MOQ_OK &&
            got == ns_count) {
            t->has_ns_fields = true;
            t->ns_fields = (moq_namespace_t){ .parts = fields, .count = got };
            t->ns_override = !ns_equal(t->ns_fields, r->namespace_);
        }
    }

    d->depends = t->desc_depends_arr;
    d->depends_count = mt->depends_count;
    d->content_protection_ref_ids = t->desc_cp_refs_arr;
    d->content_protection_ref_id_count = mt->cp_ref_id_count;

    d->has_width = mt->has_width;             d->width = mt->width;
    d->has_height = mt->has_height;           d->height = mt->height;
    d->has_samplerate = mt->has_samplerate;   d->samplerate = mt->samplerate;
    d->has_framerate = mt->has_framerate;     d->framerate_millis = mt->framerate_millis;
    d->has_bitrate = mt->has_bitrate;         d->bitrate = mt->bitrate;
    d->has_max_grp_sap = mt->has_max_grp_sap; d->max_grp_sap = mt->max_grp_sap;
    d->has_max_obj_sap = mt->has_max_obj_sap; d->max_obj_sap = mt->max_obj_sap;
    /* MSF §7.4 inline template: a plain value, no backing allocation. */
    d->has_template = mt->has_template;       d->template_ = mt->template_;
    /* MSF §5.2.35 track duration (VOD/ended tracks). */
    d->has_track_duration = mt->has_track_duration;
    d->track_duration_ms = mt->track_duration_ms;
    d->is_live = mt->is_live;                 /* §5.2.7 (mutable via §11.3) */
    t->was_live = mt->is_live;
    d->has_alt_group = mt->has_alt_group;     d->alt_group = mt->alt_group;
    d->has_render_group = mt->has_render_group;
    d->render_group = mt->render_group;
    d->has_label = mt->has_label;
    d->init_source = init.source;
    d->init_property_type = init.property_type;
    d->config_generation = 0;

    t->is_event_timeline = span_eq_lit(d->packaging, "eventtimeline", 13);
    t->is_media_timeline = span_eq_lit(d->packaging, "mediatimeline", 13);
    t->is_sap_timeline = t->is_event_timeline &&
        mt->has_event_type && span_eq_lit(d->event_type, "org.ietf.moq.cmsf.sap", 21) &&
        mt->has_mime_type && span_eq_lit(d->mime_type, "application/json", 16);

    moq_media_track_info_init(&d->info);

    /* Resolve init: the inline init (initData, or an initRef entry that
     * carries it) decodes into init_buf, so the spans that borrow it stay
     * valid after src is freed. An in-band configuration has none here. */
    moq_msf_track_t rt = *mt;
    if (init.data.len > 0) { rt.has_init_data = true; rt.init_data = init.data; }
    /* The SIZED companion, not the legacy entry plus a hand-widened
     * struct_size: this descriptor is a current-size struct we own, and the
     * negotiated draft is the authority for the object-property codec. The
     * sized helper refuses an unobserved (zero) or unsupported latch rather
     * than downgrading it to draft-16, so a descriptor either carries the
     * session's real draft or is not built at all. */
    moq_result_t irc;
    if (rt.has_init_data && rt.init_data.len > 0) {
        moq_cmaf_init_info_t parsed;
        irc = moq_msf_track_to_media_info_sized(
            &r->alloc, &rt, &d->info, sizeof(d->info), r->transport_version,
            &parsed, sizeof(parsed), &t->init_buf);
        if (irc == MOQ_OK && t->init_buf) {
            d->init_data = (moq_bytes_t){ moq_rcbuf_data(t->init_buf),
                                          moq_rcbuf_len(t->init_buf) };
            /* The mapping reports decoder extradata in codec_config for every
             * packaging; only a CMAF header is a parsed record. */
            d->codec_config = parsed.codec_config;
            if (holds_cmaf && d->info.packaging == MOQ_MEDIA_PACKAGING_CMAF) {
                moq_cmaf_init_info_t *rec =
                    (moq_cmaf_init_info_t *)(void *)t->desc_strings;
                memcpy(rec, &parsed, sizeof(*rec));
                d->cmaf = rec;
            }
        }
    } else {
        irc = moq_msf_track_to_media_info_sized(
            &r->alloc, &rt, &d->info, sizeof(d->info), r->transport_version,
            NULL, 0, NULL);
    }
    if (irc != MOQ_OK) {
        moq_media_track_info_init(&d->info);
        d->codec_config = (moq_bytes_t){ NULL, 0 };
        d->init_data = (moq_bytes_t){ NULL, 0 };
        d->cmaf = NULL;
    }

    /* The delivery path. A track without a namespace to subscribe (an MSF-01
     * namespace override) has none (see subscribe_track); the decoded
     * timelines have theirs; a media track has poll_object when its info
     * derived and this build parses it; the policy above can take either
     * away. */
    bool policy = receiver_track_policy_ok(mt, src, &init);
    if (t->is_sap_timeline || t->is_media_timeline)
        t->has_path = policy;
    else if (!t->is_event_timeline)
        t->has_media_path = t->has_path =
            policy && irc == MOQ_OK && receiver_parse_probe(r, d);
    if (!t->has_ns_fields)
        t->has_media_path = t->has_path = false;
    t->cmaf_track_id_check = r02 && d->cmaf && d->cmaf->track_id != 0;
    /* MSF-02 4.1.1: an "object" layout (or one not recognized) may deliver a
     * Group's Objects on separate streams, out of order; LOC playback takes
     * Object order as decode order, so such a track restores it. */
    moq_msf_stream_mapping_t mapping = moq_msf_track_stream_mapping(mt);
    t->ordered = r02 && t->has_media_path && span_eq_lit(d->packaging, "loc", 3) &&
                 mapping != MOQ_MSF_STREAM_MAPPING_GROUP;
    return t;
}

/* Whether handle `t` is the declared track `mt` of a catalog of revision
 * `rev`. MSF-01: the name and the namespace string as written (the same
 * local name may appear in several namespaces, so both must match). MSF-02:
 * the Full Track Name (11.2.2), a track without "namespace" inheriting the
 * receiver's namespace. */
static bool track_is(const moq_media_receiver_t *r, const moq_media_track_t *t,
                     const moq_msf_track_t *mt, moq_msf_revision_t rev)
{
    if (rev == MOQ_MSF_REVISION_02)
        return t->has_ns_fields &&
               moq_msf_track_matches(mt, r->namespace_, t->ns_fields, t->desc.name);
    if (!span_eq(t->desc.name, mt->name)) return false;
    if (t->key_has_ns != mt->has_namespace) return false;
    return !mt->has_namespace || span_eq(t->key_ns, mt->namespace_);
}

/* The live (non-removed) handle of a catalog track, judged by `rev`. */
static moq_media_track_t *receiver_find_live(const moq_media_receiver_t *r,
                                             const moq_msf_track_t *mt,
                                             moq_msf_revision_t rev)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->removed && track_is(r, t, mt, rev)) return t;
    }
    return NULL;
}

/* The track of `cat` that handle `t` was built from, judged by `cat`'s own
 * revision; NULL when it has none. */
static const moq_msf_track_t *catalog_track_of(const moq_media_receiver_t *r,
                                               const moq_msf_catalog_t *cat,
                                               const moq_media_track_t *t)
{
    for (size_t i = 0; i < cat->track_count; i++)
        if (track_is(r, t, &cat->tracks[i], cat->revision))
            return &cat->tracks[i];
    return NULL;
}

/* Compare two catalog tracks' metadata EXCEPT the two fields a live->VOD
 * conversion (§11.3) may change -- is_live and trackDuration. Everything else is
 * immutable (§5.3); classify_track_change() layers the VOD exception on top. */
/* Two draft-gregoire-moq-msfts records describe the same track. NULL
 * (absent) equals only NULL. */
static bool ts_record_equal(const moq_msf_mpeg2ts_t *a,
                            const moq_msf_mpeg2ts_t *b)
{
    if (!a || !b) return a == b;
    if (a->has_mode != b->has_mode || a->mode != b->mode ||
        !span_eq(a->mode_name, b->mode_name)) return false;
    if (a->has_packet_size != b->has_packet_size ||
        a->packet_size != b->packet_size) return false;
    if (a->has_es_pid != b->has_es_pid || a->es_pid != b->es_pid) return false;
    if (a->has_program_number != b->has_program_number ||
        a->program_number != b->program_number) return false;
    if (a->has_pcr_pid != b->has_pcr_pid || a->pcr_pid != b->pcr_pid)
        return false;
    if (a->has_mux_rate != b->has_mux_rate || a->mux_rate != b->mux_rate)
        return false;
    if (a->has_si_pids != b->has_si_pids ||
        a->si_pid_count != b->si_pid_count) return false;
    for (size_t i = 0; i < a->si_pid_count; i++)
        if (a->si_pids[i] != b->si_pids[i]) return false;
    if (a->has_random_access != b->has_random_access ||
        a->random_access != b->random_access) return false;
    if (a->has_timestamp_mode != b->has_timestamp_mode ||
        a->timestamp_mode != b->timestamp_mode ||
        !span_eq(a->timestamp_mode_name, b->timestamp_mode_name))
        return false;
    if (a->has_scte35_pid != b->has_scte35_pid ||
        a->scte35_pid != b->scte35_pid) return false;
    return a->malformed == b->malformed;
}

static bool meta_equal_except_vod(const moq_msf_track_t *a,
                                  const moq_msf_track_t *b)
{
    if (a->has_namespace != b->has_namespace ||
        (a->has_namespace && !span_eq(a->namespace_, b->namespace_))) return false;
    if (!span_eq(a->packaging, b->packaging)) return false;
    if (a->has_role != b->has_role || (a->has_role && !span_eq(a->role, b->role))) return false;
    if (a->has_codec != b->has_codec || (a->has_codec && !span_eq(a->codec, b->codec))) return false;
    if (a->has_init_data != b->has_init_data || (a->has_init_data && !span_eq(a->init_data, b->init_data))) return false;
    if (a->has_init_track != b->has_init_track || (a->has_init_track && !span_eq(a->init_track, b->init_track))) return false;
    if (a->has_init_ref != b->has_init_ref || (a->has_init_ref && !span_eq(a->init_ref, b->init_ref))) return false;
    if (a->has_event_type != b->has_event_type || (a->has_event_type && !span_eq(a->event_type, b->event_type))) return false;
    if (a->has_mime_type != b->has_mime_type || (a->has_mime_type && !span_eq(a->mime_type, b->mime_type))) return false;
    if (a->has_channel_config != b->has_channel_config || (a->has_channel_config && !span_eq(a->channel_config, b->channel_config))) return false;
    if (a->has_lang != b->has_lang || (a->has_lang && !span_eq(a->lang, b->lang))) return false;
    if (a->has_label != b->has_label || (a->has_label && !span_eq(a->label, b->label))) return false;
    if (a->has_width != b->has_width || a->width != b->width) return false;
    if (a->has_height != b->has_height || a->height != b->height) return false;
    if (a->has_samplerate != b->has_samplerate || a->samplerate != b->samplerate) return false;
    if (a->has_bitrate != b->has_bitrate || a->bitrate != b->bitrate) return false;
    if (a->has_framerate != b->has_framerate || a->framerate_millis != b->framerate_millis) return false;
    if (a->has_render_group != b->has_render_group || a->render_group != b->render_group) return false;
    if (a->has_alt_group != b->has_alt_group || a->alt_group != b->alt_group) return false;
    if (a->has_timescale != b->has_timescale || a->timescale != b->timescale) return false;
    if (a->has_target_latency != b->has_target_latency || a->target_latency != b->target_latency) return false;
    if (a->has_max_grp_sap != b->has_max_grp_sap || a->max_grp_sap != b->max_grp_sap) return false;
    if (a->has_max_obj_sap != b->has_max_obj_sap || a->max_obj_sap != b->max_obj_sap) return false;
    if (a->depends_count != b->depends_count) return false;
    for (size_t i = 0; i < a->depends_count; i++)
        if (!span_eq(a->depends[i], b->depends[i])) return false;
    if (a->cp_ref_id_count != b->cp_ref_id_count) return false;
    for (size_t i = 0; i < a->cp_ref_id_count; i++)
        if (!span_eq(a->cp_ref_ids[i], b->cp_ref_ids[i])) return false;
    /* §7.4.2: the media timeline template is part of the immutable tuple too --
     * a later catalog that adds/removes/changes a surviving track's template is
     * an immutable-tuple violation. */
    if (a->has_template != b->has_template) return false;
    if (a->has_template &&
        (a->template_.start_media_ms != b->template_.start_media_ms ||
         a->template_.delta_media_ms != b->template_.delta_media_ms ||
         a->template_.start_group != b->template_.start_group ||
         a->template_.start_object != b->template_.start_object ||
         a->template_.delta_group != b->template_.delta_group ||
         a->template_.delta_object != b->template_.delta_object ||
         a->template_.start_wallclock_ms != b->template_.start_wallclock_ms ||
         a->template_.delta_wallclock_ms != b->template_.delta_wallclock_ms))
        return false;
    /* The packaging-extension records belong to the tuple too. */
    if (!ts_record_equal(moq_msf_track_mpeg2ts(a), moq_msf_track_mpeg2ts(b)))
        return false;
    return true;
}

/* Classify how a surviving tuple's metadata changed from `old` to `new`:
 *  - TM_SAME: identical (the normal survivor case).
 *  - TM_VOD: the only changes are the sanctioned live->VOD conversion (§11.3 /
 *    §5.2.7) -- is_live true->false and/or trackDuration absent->present, all
 *    other fields unchanged, and trackDuration is never added while still live.
 *  - TM_VIOLATION: any other change (§5.3 immutable-tuple violation), including
 *    isLive false->true, changing or removing an existing trackDuration, or
 *    adding trackDuration to a track that stays live. */
typedef enum { TM_SAME, TM_VOD, TM_VIOLATION } track_meta_class_t;

static track_meta_class_t classify_track_change(const moq_msf_track_t *old,
                                                const moq_msf_track_t *new)
{
    if (!meta_equal_except_vod(old, new)) return TM_VIOLATION;
    bool live_changed = old->is_live != new->is_live;
    bool live_to_vod = old->is_live && !new->is_live;
    if (live_changed && !live_to_vod)
        return TM_VIOLATION;   /* only true->false is allowed */
    if (old->has_track_duration && new->has_track_duration &&
        old->track_duration_ms != new->track_duration_ms)
        return TM_VIOLATION;   /* changing an existing duration */
    if (old->has_track_duration && !new->has_track_duration)
        return TM_VIOLATION;   /* removing a duration */
    bool dur_added = !old->has_track_duration && new->has_track_duration;
    if (dur_added && new->is_live)
        return TM_VIOLATION;   /* duration added while still live (§5.2.35) */
    if (live_to_vod && !new->has_track_duration)
        return TM_VIOLATION;   /* §11.3: a conversion MUST add a trackDuration --
                                * isLive true->false without one is a half
                                * conversion, not a sanctioned change */
    if (!live_changed && !dur_added) return TM_SAME;
    return TM_VOD;
}

/* How a declared track changed between catalogs. MSF-01 knows SAME, VOD
 * and FORBIDDEN (every other change breaks the immutable tuple, 5.3). MSF-02
 * (5.1.6, 5.3) changes a declared track in place: ADVISORY changes keep the
 * handle and publish a new description generation; CRITICAL changes, which
 * drive how Objects are parsed or delivered, replace the handle; FORBIDDEN
 * changes drop the catalog. */
typedef enum { TC_SAME, TC_VOD, TC_ADVISORY, TC_CRITICAL, TC_FORBIDDEN } track_change_t;

static bool opt_span_differs(bool ha, moq_bytes_t a, bool hb, moq_bytes_t b)
{
    return ha != hb || (ha && !span_eq(a, b));
}

static bool template_equal(const moq_msf_media_template_t *a,
                           const moq_msf_media_template_t *b)
{
    return a->start_media_ms == b->start_media_ms &&
           a->delta_media_ms == b->delta_media_ms &&
           a->start_group == b->start_group && a->start_object == b->start_object &&
           a->delta_group == b->delta_group && a->delta_object == b->delta_object &&
           a->start_wallclock_ms == b->start_wallclock_ms &&
           a->delta_wallclock_ms == b->delta_wallclock_ms;
}

static bool cmsf_url_equal(const moq_cmsf_url_t *a, const moq_cmsf_url_t *b)
{
    return a->present == b->present && (!a->present || span_eq(a->url, b->url)) &&
           !opt_span_differs(a->has_type, a->type, b->has_type, b->type);
}

/* Two resolved content-protection entries describe the same protection. */
static bool cp_entry_equal(const moq_cmsf_content_protection_t *a,
                           const moq_cmsf_content_protection_t *b)
{
    if (!a || !b) return a == b;
    if (!span_eq(a->scheme, b->scheme) || a->default_kid_count != b->default_kid_count)
        return false;
    for (size_t i = 0; i < a->default_kid_count; i++)
        if (!span_eq(a->default_kids[i], b->default_kids[i])) return false;
    const moq_cmsf_drm_system_t *x = &a->drm_system, *y = &b->drm_system;
    return span_eq(x->system_id, y->system_id) &&
           cmsf_url_equal(&x->la_url, &y->la_url) &&
           cmsf_url_equal(&x->cert_url, &y->cert_url) &&
           cmsf_url_equal(&x->auth_url, &y->auth_url) &&
           !opt_span_differs(x->has_pssh, x->pssh, y->has_pssh, y->pssh) &&
           !opt_span_differs(x->has_robustness, x->robustness,
                             y->has_robustness, y->robustness);
}

/* An optional supplement span compared across two (possibly absent)
 * records. */
#define SUP_HAS(s, f) ((s) != NULL && (s)->has_##f)
#define SUP_SPAN(s, f) (SUP_HAS(s, f) ? (s)->f : (moq_bytes_t){ NULL, 0 })
#define SUP_SPAN_DIFFERS(a, b, f) \
    opt_span_differs(SUP_HAS(a, f), SUP_SPAN(a, f), SUP_HAS(b, f), SUP_SPAN(b, f))

/* Classify how an MSF-02 catalog changes declared track `a` (of catalog
 * `ac`) into `b` (of `bc`). */
static track_change_t classify_r02(const moq_media_receiver_t *r,
                                   const moq_msf_track_t *a,
                                   const moq_msf_catalog_t *ac,
                                   const moq_msf_track_t *b,
                                   const moq_msf_catalog_t *bc)
{
    /* Forbidden: isLive never turns true again (5.2.8); a present template
     * never changes (7.4.2). */
    if (!a->is_live && b->is_live) return TC_FORBIDDEN;
    if (a->has_template &&
        (!b->has_template || !template_equal(&a->template_, &b->template_)))
        return TC_FORBIDDEN;

    /* Critical: what parses or delivers the Objects. */
    const moq_msf_track_supplement_t *sa = moq_msf_track_supplement(a);
    const moq_msf_track_supplement_t *sb = moq_msf_track_supplement(b);
    if (!span_eq(a->packaging, b->packaging) ||
        opt_span_differs(a->has_role, a->role, b->has_role, b->role) ||
        opt_span_differs(a->has_codec, a->codec, b->has_codec, b->codec) ||
        opt_span_differs(a->has_event_type, a->event_type,
                         b->has_event_type, b->event_type) ||
        opt_span_differs(a->has_channel_config, a->channel_config,
                         b->has_channel_config, b->channel_config) ||
        opt_span_differs(a->has_init_track, a->init_track,
                         b->has_init_track, b->init_track) ||
        a->has_timescale != b->has_timescale || a->timescale != b->timescale ||
        a->has_samplerate != b->has_samplerate || a->samplerate != b->samplerate ||
        !ts_record_equal(moq_msf_track_mpeg2ts(a), moq_msf_track_mpeg2ts(b)) ||
        moq_msf_track_stream_mapping(a) != moq_msf_track_stream_mapping(b) ||
        SUP_SPAN_DIFFERS(sa, sb, encryption_scheme))
        return TC_CRITICAL;
    receiver_init_t ia = receiver_track_init(r, a, ac);
    receiver_init_t ib = receiver_track_init(r, b, bc);
    if (ia.source != ib.source || ia.property_type != ib.property_type ||
        ia.usable != ib.usable || !span_eq(ia.data, ib.data))
        return TC_CRITICAL;
    if (a->cp_ref_id_count != b->cp_ref_id_count) return TC_CRITICAL;
    for (size_t i = 0; i < a->cp_ref_id_count; i++) {
        if (!span_eq(a->cp_ref_ids[i], b->cp_ref_ids[i])) return TC_CRITICAL;
        if (!cp_entry_equal(
                moq_msf_catalog_find_content_protection(ac, a->cp_ref_ids[i]),
                moq_msf_catalog_find_content_protection(bc, b->cp_ref_ids[i])))
            return TC_CRITICAL;
    }

    /* The live-to-VOD conversion: isLive true to false, or a trackDuration
     * appearing on a track that is not live. */
    bool vod = (a->is_live && !b->is_live) ||
               (!a->has_track_duration && b->has_track_duration);

    /* Advisory: descriptive fields no Object depends on. */
    bool adv =
        (a->has_track_duration && (!b->has_track_duration ||
                                   a->track_duration_ms != b->track_duration_ms)) ||
        a->has_bitrate != b->has_bitrate || a->bitrate != b->bitrate ||
        a->has_framerate != b->has_framerate ||
        a->framerate_millis != b->framerate_millis ||
        a->has_width != b->has_width || a->width != b->width ||
        a->has_height != b->has_height || a->height != b->height ||
        opt_span_differs(a->has_label, a->label, b->has_label, b->label) ||
        opt_span_differs(a->has_lang, a->lang, b->has_lang, b->lang) ||
        opt_span_differs(a->has_mime_type, a->mime_type, b->has_mime_type, b->mime_type) ||
        a->has_target_latency != b->has_target_latency ||
        a->target_latency != b->target_latency ||
        a->has_render_group != b->has_render_group || a->render_group != b->render_group ||
        a->has_alt_group != b->has_alt_group || a->alt_group != b->alt_group ||
        a->has_max_grp_sap != b->has_max_grp_sap || a->max_grp_sap != b->max_grp_sap ||
        a->has_max_obj_sap != b->has_max_obj_sap || a->max_obj_sap != b->max_obj_sap ||
        a->depends_count != b->depends_count ||
        (!a->has_template && b->has_template) ||
        SUP_SPAN_DIFFERS(sa, sb, packaging_metadata) ||
        SUP_SPAN_DIFFERS(sa, sb, cipher_suite) ||
        SUP_SPAN_DIFFERS(sa, sb, key_id) ||
        SUP_SPAN_DIFFERS(sa, sb, track_base_key);
    for (size_t i = 0; !adv && i < a->depends_count; i++)
        adv = !span_eq(a->depends[i], b->depends[i]);
    if (!adv) {
        bool hka = sa && sa->has_keyframe_spacing, hkb = sb && sb->has_keyframe_spacing;
        bool hwa = sa && sa->has_accessible_window_ms, hwb = sb && sb->has_accessible_window_ms;
        size_t na = sa ? sa->depends_render_group_count : 0;
        size_t nb = sb ? sb->depends_render_group_count : 0;
        adv = hka != hkb || (hka && sa->keyframe_spacing != sb->keyframe_spacing) ||
              hwa != hwb || (hwa && sa->accessible_window_ms != sb->accessible_window_ms) ||
              na != nb;
        for (size_t i = 0; !adv && i < na; i++)
            adv = sa->depends_render_groups[i] != sb->depends_render_groups[i];
    }
    if (adv) return TC_ADVISORY;
    return vod ? TC_VOD : TC_SAME;
}

/* Classify the change of declared track `a` into `b` by the revision of the
 * candidate catalog `bc`. */
static track_change_t classify_change(const moq_media_receiver_t *r,
                                      const moq_msf_track_t *a,
                                      const moq_msf_catalog_t *ac,
                                      const moq_msf_track_t *b,
                                      const moq_msf_catalog_t *bc)
{
    if (bc->revision == MOQ_MSF_REVISION_02) return classify_r02(r, a, ac, b, bc);
    switch (classify_track_change(a, b)) {
    case TM_SAME: return TC_SAME;
    case TM_VOD:  return TC_VOD;
    default:      return TC_FORBIDDEN;
    }
}

/* Whether a candidate changes any declared track of `eff` in a way its
 * revision forbids (MSF-01: anything but live-to-VOD, 5.3). */
static bool receiver_change_forbidden(const moq_media_receiver_t *r,
                                      const moq_msf_catalog_t *eff,
                                      const moq_msf_catalog_t *cand)
{
    for (size_t i = 0; i < cand->track_count; i++) {
        const moq_msf_track_t *c = &cand->tracks[i];
        for (size_t j = 0; j < eff->track_count; j++) {
            const moq_msf_track_t *e = &eff->tracks[j];
            bool same;
            if (cand->revision == MOQ_MSF_REVISION_02) {
                moq_bytes_t fields[MOQ_MSF_NAMESPACE_MAX_FIELDS];
                moq_namespace_t ns = r->namespace_;
                size_t n = 0;
                if (c->has_namespace) {
                    if (moq_msf_namespace_split(c->namespace_, fields,
                                                MOQ_MSF_NAMESPACE_MAX_FIELDS,
                                                &n) != MOQ_OK)
                        continue;
                    ns = (moq_namespace_t){ .parts = fields, .count = n };
                }
                same = moq_msf_track_matches(e, r->namespace_, ns, c->name);
            } else {
                same = span_eq(c->name, e->name) &&
                       c->has_namespace == e->has_namespace &&
                       (!c->has_namespace || span_eq(c->namespace_, e->namespace_));
            }
            if (!same) continue;
            if (classify_change(r, e, eff, c, cand) == TC_FORBIDDEN) return true;
            break;
        }
    }
    return false;
}

/* Saturating add: never wraps, so byte budgets stay sound even against
 * adversarially huge inputs (returns UINT64_MAX on overflow, which trips any
 * finite cap). */
static uint64_t sat_add(uint64_t a, uint64_t b)
{
    return a > UINT64_MAX - b ? UINT64_MAX : a + b;
}

/* Conservative estimate of the bytes a handle built from `mt` retains for the
 * receiver's life: the handle struct, the desc string arena (every copied span,
 * matching receiver_track_build's `need`), the depends / cp-ref arrays, and the
 * decoded init buffer. The init estimate MUST mirror receiver_track_build's
 * resolution -- inline initData wins, else initRef is resolved against `src`'s
 * initDataList -- otherwise an initRef-bearing track retains a large init_buf
 * while charging near-zero. The encoded init length is >= the decoded init_buf
 * length, so charging it is conservative. Reads only parsed MSF fields. */
static uint64_t est_track_retained_bytes(const moq_media_receiver_t *r,
                                         const moq_msf_track_t *mt,
                                         const moq_msf_catalog_t *src)
{
    uint64_t b = sizeof(moq_media_track_t);
    b = sat_add(b, (uint64_t)mt->name.len + mt->packaging.len);
    if (mt->has_namespace)      b = sat_add(b, mt->namespace_.len);
    if (mt->has_role)           b = sat_add(b, mt->role.len);
    if (mt->has_codec)          b = sat_add(b, mt->codec.len);
    if (mt->has_lang)           b = sat_add(b, mt->lang.len);
    if (mt->has_label)          b = sat_add(b, mt->label.len);
    if (mt->has_channel_config) b = sat_add(b, mt->channel_config.len);
    if (mt->has_event_type)     b = sat_add(b, mt->event_type.len);
    if (mt->has_mime_type)      b = sat_add(b, mt->mime_type.len);
    for (size_t i = 0; i < mt->depends_count; i++)
        b = sat_add(b, mt->depends[i].len);
    for (size_t i = 0; i < mt->cp_ref_id_count; i++)
        b = sat_add(b, mt->cp_ref_ids[i].len);
    b = sat_add(b, (uint64_t)mt->depends_count   * sizeof(moq_bytes_t));
    b = sat_add(b, (uint64_t)mt->cp_ref_id_count * sizeof(moq_bytes_t));
    /* The handle-owned extension block (records, SI PIDs, names). */
    b = sat_add(b, (uint64_t)ext_copy_size(mt));
    /* Init: inline wins; else resolve initRef against src (mirrors build). */
    b = sat_add(b, receiver_resolved_init(r, mt, src).len);
    /* The parsed header record, which shares desc_strings. */
    if (receiver_track_holds_cmaf(r, mt, src))
        b = sat_add(b, sizeof(moq_cmaf_init_info_t));
    /* An MSF-02 namespace's fields (at most MOQ_MSF_NAMESPACE_MAX_FIELDS). */
    if (mt->has_namespace)
        b = sat_add(b, (uint64_t)MOQ_MSF_NAMESPACE_MAX_FIELDS * sizeof(moq_bytes_t));
    return b;
}

/* The parser-owned arrays of one parsed track that cleanup frees: the
 * track itself, its depends / cp-ref arrays and its extension block, which
 * holds at most one entry and record per kind and the records' arrays
 * (counted at the largest such block, so the estimate never falls short). */
static uint64_t est_parsed_track_bytes(const moq_msf_track_t *t)
{
    uint64_t b = sizeof(moq_msf_track_t);
    b = sat_add(b, (uint64_t)t->depends_count   * sizeof(moq_bytes_t));
    b = sat_add(b, (uint64_t)t->cp_ref_id_count * sizeof(moq_bytes_t));
    if (t->extension_count == 0) return b;
    b = sat_add(b, 2u * sizeof(moq_msf_track_ext_t) + sizeof(moq_msf_mpeg2ts_t) +
                   sizeof(moq_msf_track_supplement_t) + sizeof(uint64_t));
    const moq_msf_mpeg2ts_t *ts = moq_msf_track_mpeg2ts(t);
    if (ts) b = sat_add(b, (uint64_t)ts->si_pid_count * sizeof(uint32_t));
    const moq_msf_track_supplement_t *sup = moq_msf_track_supplement(t);
    if (sup) b = sat_add(b, (uint64_t)sup->depends_render_group_count * sizeof(int));
    return b;
}

/* Conservative estimate of the bytes a retained CP snapshot (a whole prior
 * effective catalog moved into cp_snaps) holds. Strings borrow from the parse
 * arena, so the dominant term is `_dom_size` (the whole DOM, freed by
 * moq_msf_catalog_cleanup); the remaining terms are the parser-owned arrays
 * that cleanup also frees: tracks and publishTracks with their per-track
 * arrays and extension blocks, the init-data list, content protections and
 * their defaultKIDs, delta ops. */
static uint64_t est_catalog_retained_bytes(const moq_msf_catalog_t *c)
{
    uint64_t b = sizeof(moq_msf_catalog_t);
    b = sat_add(b, c->_dom_size);                       /* all borrowed strings */
    for (size_t i = 0; i < c->track_count; i++)
        b = sat_add(b, est_parsed_track_bytes(&c->tracks[i]));
    for (size_t i = 0; i < c->publish_track_count; i++)
        b = sat_add(b, est_parsed_track_bytes(&c->publish_tracks[i]));
    b = sat_add(b, (uint64_t)c->init_data_count *
                   sizeof(moq_msf_init_data_entry_t));
    b = sat_add(b, (uint64_t)c->content_protection_count *
                   sizeof(moq_cmsf_content_protection_t));
    for (size_t i = 0; i < c->content_protection_count; i++)
        b = sat_add(b, (uint64_t)c->content_protections[i].default_kid_count *
                       sizeof(moq_bytes_t));
    b = sat_add(b, (uint64_t)c->delta_update_count * sizeof(moq_msf_delta_op_t));
    return b;
}

static bool receiver_facade_drained(moq_media_receiver_t *r);
static void receiver_retire_subscription(moq_media_receiver_t *r,
                                         moq_sub_track_t **slot,
                                         uint64_t now_us, bool drained);

/* Reserve room for one more CP snapshot when the current effective carries
 * contentProtections, so the commit below cannot fail (no visible state is
 * mutated until after this succeeds). A no-op when there is nothing to retain. */
static moq_result_t receiver_reserve_cp_snap(moq_media_receiver_t *r)
{
    if (!r->has_effective || r->effective.content_protection_count == 0)
        return MOQ_OK;
    if (r->cp_snap_count < r->cp_snap_cap) return MOQ_OK;
    size_t nc = r->cp_snap_cap ? r->cp_snap_cap * 2 : 4;
    moq_msf_catalog_t *na = (moq_msf_catalog_t *)r->alloc.alloc(
        nc * sizeof(moq_msf_catalog_t), r->alloc.ctx);
    if (!na) return MOQ_ERR_NOMEM;
    if (r->cp_snaps) {
        memcpy(na, r->cp_snaps, r->cp_snap_count * sizeof(moq_msf_catalog_t));
        r->alloc.free(r->cp_snaps, r->cp_snap_cap * sizeof(moq_msf_catalog_t),
                      r->alloc.ctx);
    }
    r->cp_snaps = na;
    r->cp_snap_cap = nc;
    return MOQ_OK;
}

/* Commit a candidate as the new effective catalog, with r->mu held. NO-FAIL:
 * cp_snaps capacity was reserved before, so this only moves pointers. The
 * previous effective is retained in cp_snaps if it carried
 * contentProtections (so any pointer find_content_protection returned stays
 * valid), else freed. Takes ownership of *cand (caller must not clean it up
 * afterward). The caller holds r->mu across the whole publication, so the
 * public find_content_protection() reader (which takes the same mutex) never
 * observes a torn effective/has_effective transition. */
static void receiver_commit_effective_locked(moq_media_receiver_t *r,
                                             moq_msf_catalog_t *cand)
{
    /* Writer PRE: before any access to r->has_effective / r->effective. */
    PUB_PHASE_HOOK(r, MOQ_MR_WRITER_PRE);
    if (r->has_effective) {
        if (r->effective.content_protection_count > 0) {
            /* Retain for CP-pointer validity. Account its bytes (slot + cap
             * room were preflighted in receiver_adopt). */
            r->retained_bytes = sat_add(r->retained_bytes,
                                        est_catalog_retained_bytes(&r->effective));
            r->cp_snaps[r->cp_snap_count++] = r->effective;   /* room reserved */
        } else {
            moq_msf_catalog_cleanup(&r->alloc, &r->effective);
        }
    }
    r->effective = *cand;     /* move ownership */
    r->has_effective = true;
    r->stats.catalog_revision = r->effective.revision;
    /* MSF §5.1.3: latch terminal completion for app observation (get_stats). */
    if (r->effective.is_complete)
        r->stats.catalog_complete = true;
    /* Writer POST: after retention/detach, publication of effective and
     * has_effective, and the catalog_complete mutation, before the unlock. */
    PUB_PHASE_HOOK(r, MOQ_MR_WRITER_POST);
}

static void receiver_count_drop(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    r->stats.catalog_drops++;
    pthread_mutex_unlock(&r->mu);
}

/* Drop the latest group's remaining deltas: count once on the transition into
 * desync (a burst in an already-desynced group must not inflate the stat). */
static void receiver_desync(moq_media_receiver_t *r)
{
    if (!r->desynced) {
        r->desynced = true;
        receiver_count_drop(r);
    }
}

typedef enum {
    ADOPT_OK = 0,       /* committed; cand ownership moved into r->effective */
    ADOPT_VIOLATION,    /* a change the candidate's revision forbids (MSF-01
                           5.3 immutable tuple; MSF-02 5.2.8, 7.4.2); cand
                           still owned */
    ADOPT_NOMEM,        /* allocation failure; cand still owned (caller fatals) */
    ADOPT_LIMIT,        /* a receiver-lifetime retained-state cap, or the
                           event queue's room for the adoption's events, would
                           be exceeded; cand still owned. After an effective
                           exists this is a bounded drop; on the first catalog
                           it is fatal. */
} adopt_result_t;

#ifdef MOQ_MEDIA_RECEIVER_TESTING
/* Test-only hook, called with no lock held just before an adoption's commit
 * takes r->mu (phase 0) and just after it releases it (phase 1), so a test
 * can issue application commands at exactly those instants. Never compiled
 * into the shipping library. */
static void (*g_adopt_hook)(void *ctx, int phase);
static void *g_adopt_hook_ctx;
void moq_media_receiver_test_set_adopt_hook(void (*fn)(void *, int), void *ctx);
void moq_media_receiver_test_set_adopt_hook(void (*fn)(void *, int), void *ctx)
{
    g_adopt_hook = fn;
    g_adopt_hook_ctx = ctx;
}
#define ADOPT_HOOK(phase) \
    do { if (g_adopt_hook) g_adopt_hook(g_adopt_hook_ctx, (phase)); } while (0)
#else
#define ADOPT_HOOK(phase) ((void)0)
#endif

/* One candidate track's plan in an adoption. */
typedef struct adopt_item {
    const moq_msf_track_t *mt;     /* the candidate's track */
    moq_media_track_t     *live;   /* its declared handle, or NULL (NEW) */
    track_change_t         change; /* how it changed (live only) */
    moq_media_track_t     *built;  /* NEW or CRITICAL: the handle to add */
    receiver_desc_gen_t   *gen;    /* ADVISORY: the generation to publish */
    uint64_t               bytes;  /* retained bytes built/gen add */
} adopt_item_t;

/* Move the description storage of a freshly built handle into a new
 * generation of `live`, and free the handle shell. NULL on allocation
 * failure (`t` is then freed whole). */
static receiver_desc_gen_t *receiver_gen_from(moq_media_receiver_t *r,
                                              moq_media_track_t *t)
{
    receiver_desc_gen_t *g =
        (receiver_desc_gen_t *)r->alloc.alloc(sizeof(*g), r->alloc.ctx);
    if (!g) { receiver_track_free(r, t); return NULL; }
    memset(g, 0, sizeof(*g));
    g->desc = t->desc;
    g->init_buf = t->init_buf;           t->init_buf = NULL;
    g->desc_strings = t->desc_strings;   t->desc_strings = NULL;
    g->desc_strings_size = t->desc_strings_size;
    g->desc_depends_arr = t->desc_depends_arr; t->desc_depends_arr = NULL;
    g->desc_cp_refs_arr = t->desc_cp_refs_arr; t->desc_cp_refs_arr = NULL;
    g->desc_ext = t->desc_ext;           t->desc_ext = NULL;
    g->desc_ext_size = t->desc_ext_size;
    receiver_track_free(r, t);
    return g;
}

/* Whether a rebuilt handle `b` delivers exactly as `a`: the decisions a
 * build derives (delivery path, decoder, timeline kind, init source,
 * subscription namespace). An "advisory" field that changes any of them is
 * delivery-critical for this receiver. */
static bool receiver_same_delivery(const moq_media_track_t *a,
                                   const moq_media_track_t *b)
{
    return a->has_path == b->has_path && a->has_media_path == b->has_media_path &&
           a->is_event_timeline == b->is_event_timeline &&
           a->is_media_timeline == b->is_media_timeline &&
           a->is_sap_timeline == b->is_sap_timeline &&
           a->has_ns_fields == b->has_ns_fields &&
           a->ns_override == b->ns_override &&
           a->desc.info.media_type == b->desc.info.media_type &&
           a->desc.info.packaging == b->desc.info.packaging &&
           a->desc.init_source == b->desc.init_source &&
           a->desc.init_property_type == b->desc.init_property_type;
}

static void adopt_items_free(moq_media_receiver_t *r, adopt_item_t *items,
                             size_t n)
{
    if (!items) return;
    for (size_t i = 0; i < n; i++) {
        receiver_track_free(r, items[i].built);
        receiver_gen_free(r, items[i].gen);
    }
    r->alloc.free(items, n * sizeof(*items), r->alloc.ctx);
}

/* Grow the handle array so `extra` more handles fit without allocating
 * (under r->mu: the app thread reads it there). */
static bool receiver_tracks_reserve(moq_media_receiver_t *r, size_t extra)
{
    if (extra <= r->track_cap - r->track_count) return true;
    if (extra > SIZE_MAX / 2 - r->track_count) return false;
    size_t need = r->track_count + extra;
    size_t nc = r->track_cap ? r->track_cap : 8;
    while (nc < need) nc *= 2;
    if (nc > SIZE_MAX / sizeof(moq_media_track_t *)) return false;
    moq_media_track_t **na = (moq_media_track_t **)r->alloc.alloc(
        nc * sizeof(moq_media_track_t *), r->alloc.ctx);
    if (!na) return false;
    pthread_mutex_lock(&r->mu);
    if (r->tracks) {
        memcpy(na, r->tracks, r->track_count * sizeof(moq_media_track_t *));
        r->alloc.free(r->tracks, r->track_cap * sizeof(moq_media_track_t *),
                      r->alloc.ctx);
    }
    r->tracks = na;
    r->track_cap = nc;
    pthread_mutex_unlock(&r->mu);
    return true;
}

/* Retire a removed handle's subscription and its held state (network
 * thread, no lock): its facade slot stays mapped until released, so stream
 * terminals still queued for it reach the lane under the removed handle;
 * receiver_retire_tracks retries a refused unsubscribe or release on every
 * pass. */
static void receiver_retire_removed(moq_media_receiver_t *r, moq_media_track_t *t,
                                    uint64_t now_us)
{
    moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
    if (t->sub_track && t->local_result == MOQ_OK && !t->auth_failed)
        receiver_retire_subscription(r, &t->sub_track, now_us,
                                     receiver_facade_drained(r));
    receiver_ts_free(r, t);
    receiver_order_free(r, t);
}

/* Adopt a candidate catalog in two phases (network thread).
 *
 * Phase 1 plans and preflights with no visible mutation: each candidate
 * track is matched to its declared handle by the candidate's identity rule
 * and classified (track_change_t); a forbidden change refuses the whole
 * candidate; every new handle, every replacement handle and every advisory
 * generation is built, unpublished; then the handle cap, the retained-byte
 * cap, the CP snapshot slot and the event queue's room for every event the
 * adoption queues are checked. Any failure frees what was built and leaves
 * the effective catalog, every handle and every selection exactly as they
 * were.
 *
 * Phase 2 commits under one r->mu hold and cannot fail, since every
 * allocation and event slot is already held: in candidate order a VOD
 * change rewrites the current generation's live-to-VOD triple, an advisory
 * change publishes its generation, a replacement moves the old handle's
 * selection to its successor and retires the old handle (REMOVED with
 * replaced_by, then ADDED with replaces), and a new track is added; then
 * tracks the candidate no longer declares are removed, and the candidate
 * becomes the effective catalog. Every application command takes r->mu, so
 * a command either precedes the commit, and its effect is in the moved
 * selection, or follows it, and is forwarded to the successor
 * (receiver_chain_head_locked). Old subscriptions are retired after the
 * unlock. On ADOPT_OK the caller must NOT clean up cand. */
static void receiver_settle_completions(moq_media_receiver_t *r, uint64_t now_us);

static adopt_result_t receiver_adopt(moq_media_receiver_t *r,
                                     moq_msf_catalog_t *cand, uint64_t now_us)
{
    const moq_msf_catalog_t *eff = r->has_effective ? &r->effective : NULL;
    size_t n = cand->track_count;
    adopt_item_t *items = NULL;
    if (n > 0) {
        if (n > SIZE_MAX / sizeof(*items)) return ADOPT_NOMEM;
        items = (adopt_item_t *)r->alloc.alloc(n * sizeof(*items), r->alloc.ctx);
        if (!items) return ADOPT_NOMEM;
        memset(items, 0, n * sizeof(*items));
    }

    /* Match and classify. */
    for (size_t i = 0; i < r->track_count; i++) r->tracks[i]->recon_seen = false;
    for (size_t i = 0; i < n; i++) {
        adopt_item_t *it = &items[i];
        it->mt = &cand->tracks[i];
        it->live = receiver_find_live(r, it->mt, cand->revision);
        if (!it->live) continue;
        it->live->recon_seen = true;
        const moq_msf_track_t *old = eff ? catalog_track_of(r, eff, it->live) : NULL;
        it->change = old ? classify_change(r, old, eff, it->mt, cand) : TC_CRITICAL;
        if (it->change == TC_FORBIDDEN) {
            adopt_items_free(r, items, n);
            return ADOPT_VIOLATION;
        }
        if (it->change == TC_CRITICAL && cand->revision != MOQ_MSF_REVISION_02) {
            adopt_items_free(r, items, n);
            return ADOPT_VIOLATION;
        }
    }

    /* Build, unpublished. */
    size_t add_handles = 0;
    uint64_t add_bytes = 0;
    uint32_t events = r->has_effective ? 0u : 1u;   /* CATALOG_READY */
    for (size_t i = 0; i < n; i++) {
        adopt_item_t *it = &items[i];
        if (it->live && (it->change == TC_SAME || it->change == TC_VOD)) {
            if (it->change == TC_VOD) events++;
            continue;
        }
        moq_media_track_t *t = receiver_track_build(r, it->mt, cand);
        if (!t) { adopt_items_free(r, items, n); return ADOPT_NOMEM; }
        it->bytes = est_track_retained_bytes(r, it->mt, cand);
        if (it->live && it->change == TC_ADVISORY &&
            !receiver_same_delivery(it->live, t))
            it->change = TC_CRITICAL;
        if (it->live && it->change == TC_ADVISORY) {
            if (it->live->config_gen == UINT32_MAX) {
                receiver_track_free(r, t);
                adopt_items_free(r, items, n);
                return ADOPT_LIMIT;
            }
            it->gen = receiver_gen_from(r, t);
            if (!it->gen) { adopt_items_free(r, items, n); return ADOPT_NOMEM; }
            it->gen->desc.config_generation = it->live->config_gen + 1u;
            events++;
        } else {
            it->built = t;
            add_handles++;
            events += it->live ? 2u : 1u;
        }
        add_bytes = sat_add(add_bytes, it->bytes);
    }
    for (size_t i = 0; i < r->track_count; i++)
        if (!r->tracks[i]->removed && !r->tracks[i]->recon_seen) events++;

    /* Caps and room. An update whose events would not all fit is dropped
     * whole; the first catalog keeps the overflow rule (the event that does
     * not fit terminalizes the receiver), as nothing is visible yet. */
    bool will_snap = r->has_effective && r->effective.content_protection_count > 0;
    uint64_t snap_bytes = will_snap ? est_catalog_retained_bytes(&r->effective) : 0;
    pthread_mutex_lock(&r->mu);
    uint32_t room = rq_room_locked(r);
    pthread_mutex_unlock(&r->mu);
    if (add_handles > r->max_stable_handles ||
        r->track_count > r->max_stable_handles - add_handles ||
        r->cp_snap_count + (will_snap ? 1u : 0u) > r->max_cp_snaps ||
        sat_add(sat_add(r->retained_bytes, add_bytes), snap_bytes) >
            r->max_retained_bytes ||
        (r->has_effective && events > room)) {
        adopt_items_free(r, items, n);
        return ADOPT_LIMIT;
    }
    if ((will_snap && receiver_reserve_cp_snap(r) != MOQ_OK) ||
        !receiver_tracks_reserve(r, add_handles)) {
        adopt_items_free(r, items, n);
        return ADOPT_NOMEM;
    }

    /* Commit. */
    ADOPT_HOOK(0);
    bool completing = cand->is_complete;
    pthread_mutex_lock(&r->mu);
    for (size_t i = 0; i < n; i++) {
        adopt_item_t *it = &items[i];
        moq_media_track_t *live = it->live;
        if (live && it->change == TC_VOD) {
            /* MSF 11.3/11.4: the one in-place write of a published
             * description, on its current generation, serialized against
             * moq_media_receiver_track_desc_copy() by r->mu. */
            PUB_PHASE_HOOK(r, MOQ_MR_VOD_WRITER_PRE);
            live->desc_cur->is_live = it->mt->is_live;
            live->desc_cur->has_track_duration = it->mt->has_track_duration;
            live->desc_cur->track_duration_ms = it->mt->track_duration_ms;
            PUB_PHASE_HOOK(r, MOQ_MR_VOD_WRITER_POST);
            (void)rq_push_locked(r, MOQ_MEDIA_TRACK_UPDATED, live);
        } else if (live && it->change == TC_ADVISORY) {
            receiver_desc_gen_t *g = it->gen;
            it->gen = NULL;
            g->next = live->gens;
            live->gens = g;
            live->config_gen = g->desc.config_generation;
            live->desc_cur = &g->desc;
            atomic_store_explicit(&live->desc_pub, &g->desc, memory_order_release);
            r->retained_bytes = sat_add(r->retained_bytes, it->bytes);
            (void)rq_push_locked(r, MOQ_MEDIA_TRACK_UPDATED, live);
        } else if (live && it->change == TC_CRITICAL) {
            moq_media_track_t *nt = it->built;
            it->built = NULL;
            /* The latest selection, moved whole: every command took r->mu. A
             * successor without a delivery path cannot take a subscription. */
            nt->desired_subscribed = live->desired_subscribed && nt->has_path;
            nt->desired_filter = live->desired_filter;
            nt->desired_has_priority = live->desired_has_priority;
            nt->desired_priority = live->desired_priority;
            nt->desired_ts_program = live->desired_ts_program;
            nt->desired_options = live->desired_options;
            /* A successor's first wire action is a SUBSCRIBE, not an
             * UPDATE: no acknowledgment attribution carries over. */
            nt->app_gen = nt->reconciled_app_gen = 0;
            nt->gate = live->gate ? live->gate : (live->sub_track ? live : NULL);
            live->successor = nt;
            live->replaced = true;
            live->removed = true;
            live->desired_subscribed = false;
            /* Fill commands belong to the old subscription generation. */
            if (live->cmd_state == FILL_CMD_QUEUED) live->cmd_state = FILL_CMD_NONE;
            else if (live->cmd_state == FILL_CMD_CLAIMED) live->cmd_withdrawn = true;
            obj_compact_locked(r, keep_not_track, live);
            live->has_drop_filter = false;
            r->tracks[r->track_count++] = nt;       /* room reserved */
            nt->recon_seen = true;
            r->retained_bytes = sat_add(r->retained_bytes, it->bytes);
            receiver_event_t *e = rq_push_locked(r, MOQ_MEDIA_TRACK_REMOVED, live);
            if (e) e->replaced_by = nt;
            e = rq_push_locked(r, MOQ_MEDIA_TRACK_ADDED, nt);
            if (e) e->replaces = live;
        } else if (!live) {
            moq_media_track_t *t = it->built;
            it->built = NULL;
            t->recon_seen = true;
            r->tracks[r->track_count++] = t;        /* room reserved */
            /* Auto-subscribe takes every media track with a delivery path:
             * timelines are subscribed explicitly, and a track without a
             * path (info not derived, packaging not parseable in this build,
             * no namespace to subscribe, or a receiver policy) would only
             * cost bitrate. See subscribe_track, which refuses those. */
            if (r->auto_subscribe && t->has_media_path) {
                t->desired_subscribed = true;
                t->desired_filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            }
            r->retained_bytes = sat_add(r->retained_bytes, it->bytes);
            (void)rq_push_locked(r, MOQ_MEDIA_TRACK_ADDED, t);
        }
    }
    /* Tracks the candidate no longer declares. A terminal catalog (isComplete,
     * MSF 5.1.3 / 11.3) completes the broadcast rather than withdrawing the
     * track: what the track already delivered stays queued for the
     * application, and a track whose subscription has not ended keeps it
     * until its subscription's done is processed, with every stream the
     * done counts taken in. Every such track settles in
     * receiver_settle_completions, one already over at once: its
     * TRACK_REMOVED then follows the commit that latches
     * stats.catalog_complete, so a consumer reading it at that event sees
     * the broadcast complete. */
    size_t scan = r->track_count;
    for (size_t i = 0; i < scan; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed || t->recon_seen) continue;
        t->removed = true;
        t->removed_by_completion = completing;
        /* Keep wanted media in flight after completion; do not revive a
         * subscription the application already cancelled. */
        if (!completing) t->desired_subscribed = false;
        if (completing && t->local_result == MOQ_OK && !t->auth_failed) {
            t->completion_pending = true;
            t->completion_cancelled = !t->desired_subscribed;
            continue;
        }
        if (!completing)
            obj_compact_locked(r, keep_not_track, t);   /* purge its queued objects */
        (void)rq_push_locked(r, MOQ_MEDIA_TRACK_REMOVED, t);
    }
    receiver_commit_effective_locked(r, cand);
    pthread_mutex_unlock(&r->mu);
    ADOPT_HOOK(1);

    /* Retire what the commit removed (no lock held). */
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed && !t->completion_pending && (t->sub_track || t->ts ||
                                                     t->order))
            receiver_retire_removed(r, t, now_us);
    }
    adopt_items_free(r, items, n);
    /* A terminal catalog's removals settle after its commit, so a track
     * that is already over (never subscribed, or its subscription done) is
     * removed in this same adoption, with stats.catalog_complete set. */
    if (completing) receiver_settle_completions(r, now_us);
    return ADOPT_OK;
}

/* MSF §5.2.35: a live track MUST NOT carry trackDuration. The encode path
 * enforces this (validate_track), but inbound catalogs are parsed and adopted
 * without re-encoding, so a malicious relay/publisher could otherwise expose a
 * live track with a finite VOD duration through the public track descriptor
 * (media_receiver.h documents track_duration_ms as non-live/VOD-only). The
 * receiver enforces this inbound semantic invariant before adoption -- on both
 * the independent and the delta-applied candidate -- so the parser stays lenient
 * for raw/fuzz users while the receiver contract holds. (Survivor tracks that
 * change live/duration are already caught by the adoption's classification; this
 * additionally catches NEW live+duration tracks, e.g. a first/later independent
 * catalog or a delta `add`.) */
static bool catalog_has_live_track_with_duration(const moq_msf_catalog_t *c)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (c->tracks[i].is_live && c->tracks[i].has_track_duration)
            return true;
    return false;
}

static moq_result_t receiver_parse_media_object(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    const moq_media_object_input_t *in, moq_bytes_t track_properties,
    moq_cmaf_sample_t *samples,
    size_t sample_cap, moq_media_parsed_object_t *parsed,
    moq_media_drop_reason_t *why)
{
    /* The sized parser carries the draft-22 companion and an MPEG2TS track's
     * packet size; every other legacy track keeps the frozen-prefix parser. */
    bool d22 = receiver_companion_d22(r->companion_profile);
    if (!d22 && track->desc.info.packaging != MOQ_MEDIA_PACKAGING_MPEG2TS)
        return moq_media_object_parse(&track->desc.info, in, samples, sample_cap,
                                       parsed, why);
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.mpeg2ts_packet_size = receiver_ts_packet_size(&track->desc);
    moq_loc_metadata_t loc;
    moq_loc_ext_t ext;
    if (d22) {
        bool loc05 =
            r->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
        cfg.companion_profile = r->companion_profile;
        if (track_properties.len) {
            moq_result_t rc = moq_loc_parse_metadata_ex(MOQ_VERSION_DRAFT_22,
                loc05 ? MOQ_LOC_PROFILE_05 : MOQ_LOC_PROFILE_04,
                track_properties, &loc, sizeof(loc),
                loc05 ? &ext : NULL, sizeof(ext));
            if (rc != MOQ_OK) return rc;
            cfg.track_loc_metadata = &loc;
            if (loc05) cfg.track_loc_ext = &ext;
        }
        cfg.catalog_codec = track->desc.codec;
    }
    return moq_media_object_parse_sized(&track->desc.info, in, &cfg,
                samples, sample_cap, parsed, sizeof(*parsed), why);
}

static void receiver_on_media_object(moq_media_receiver_t *r,
                                     moq_media_track_t *track,
                                     moq_sub_object_t *so, uint64_t now_us);
static void receiver_on_media_object_with_origin(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, uint32_t delivery_origin,
    uint64_t initiating_request_id, uint64_t now_us);
static void receiver_on_fill_ts_object(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, size_t dom,
    uint64_t initiating_request_id);
static void receiver_on_fill_media_object(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, size_t dom,
    uint64_t initiating_request_id);
static moq_result_t receiver_fill_ts_payload(moq_media_receiver_t *r,
    moq_media_track_t *t, bool join, const moq_rcbuf_t *payload,
    moq_rcbuf_t **out, uint32_t *psi_len);
static bool ts_join_wanted_locked(const moq_media_receiver_t *r,
                                  const moq_media_track_t *t, uint32_t gen);

/* The draft-22 section 3.5 join at the current group: a Location Filter
 * that starts at the Next Object, and a fill whose Location filter is the
 * current group (StartGroup=1, relative), which delivers that group from its
 * start. The three configs borrow for the subscribe call only. */
static void receiver_join_current_group(moq_location_filter_cfg_t *loc,
                                        moq_location_filter_cfg_t *fill_loc,
                                        moq_fill_cfg_t *fill)
{
    moq_location_filter_cfg_init(loc);
    loc->kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(fill_loc);
    fill_loc->kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    fill_loc->relative_group_offset = 1;
    moq_fill_cfg_init(fill);
    fill->location = fill_loc;
}

/* -- MSF properties (MSF-02 12) ----------------------------------------- *
 * MSF_COMPRESSION (12.1, an integer) and MSF_EVENT_TIMELINE_HEADER (12.3.1,
 * bytes) in a Track or Object Properties block encoded for the session's
 * draft, searched as LOC reads its properties: the outer list and one
 * Immutable Properties (0x0B) inner list. Draft 16 encodes the KVP fields as
 * QUIC varints, drafts 18 and 22 as vi64 (draft-16 1.4.2, draft-18 1.4.3,
 * draft-22 8.3); any other draft has no registered codec. Bounded: every
 * field is checked against the bytes left before it is read, and each step
 * consumes at least one byte. */
#define RECEIVER_MSF_COMPRESSION           UINT64_C(0x78)
#define RECEIVER_MSF_EVENT_TIMELINE_HEADER UINT64_C(0x7B)
#define RECEIVER_MSF_IMMUTABLE_PROPERTIES  UINT64_C(0x0B)

typedef struct receiver_prop_walk {
    bool           d16;
    const uint8_t *p;
    size_t         n;
    uint64_t       prev;
    bool           first;
} receiver_prop_walk_t;

typedef struct receiver_prop {
    uint64_t       type;
    uint64_t       value;      /* even type */
    moq_bytes_t    bytes;      /* odd type, borrowed from the block */
} receiver_prop_t;

static size_t receiver_prop_int(bool d16, const uint8_t *p, size_t n, uint64_t *v)
{
    return d16 ? moq_quic_varint_decode(p, n, v) : moq_vi64_decode(p, n, v);
}

/* MOQ_OK with *out, MOQ_DONE at the end, MOQ_ERR_PROTO when malformed. */
static moq_result_t receiver_prop_next(receiver_prop_walk_t *w, receiver_prop_t *out)
{
    if (w->n == 0) return MOQ_DONE;
    uint64_t delta = 0;
    size_t k = receiver_prop_int(w->d16, w->p, w->n, &delta);
    if (k == 0) return MOQ_ERR_PROTO;
    w->p += k;
    w->n -= k;
    if (!w->first && delta > UINT64_MAX - w->prev) return MOQ_ERR_PROTO;
    out->type = w->first ? delta : w->prev + delta;
    w->prev = out->type;
    w->first = false;
    out->value = 0;
    out->bytes = (moq_bytes_t){ NULL, 0 };
    uint64_t v = 0;
    k = receiver_prop_int(w->d16, w->p, w->n, &v);
    if (k == 0) return MOQ_ERR_PROTO;
    w->p += k;
    w->n -= k;
    if ((out->type & 1u) == 0) { out->value = v; return MOQ_OK; }
    if (v > 0xFFFFu || v > w->n) return MOQ_ERR_PROTO;   /* odd Length cap */
    out->bytes = (moq_bytes_t){ w->p, (size_t)v };
    w->p += (size_t)v;
    w->n -= (size_t)v;
    return MOQ_OK;
}

/* Find property `type` in `props` for draft `v`. MOQ_OK with *present (and
 * *out when present); MOQ_ERR_UNSUPPORTED for a draft without a registered
 * codec or the property repeated; MOQ_ERR_PROTO for a malformed block. */
static moq_result_t receiver_prop_find(moq_version_t v, moq_bytes_t props,
                                       uint64_t type, bool *present,
                                       receiver_prop_t *out)
{
    *present = false;
    if (props.len == 0) return MOQ_OK;
    if (v != MOQ_VERSION_DRAFT_16 && v != MOQ_VERSION_DRAFT_18 &&
        v != MOQ_VERSION_DRAFT_22)
        return MOQ_ERR_UNSUPPORTED;
    bool d16 = v == MOQ_VERSION_DRAFT_16;
    receiver_prop_walk_t outer = { d16, props.data, props.len, 0, true };
    receiver_prop_t it;
    moq_result_t rc;
    while ((rc = receiver_prop_next(&outer, &it)) == MOQ_OK) {
        if (it.type == RECEIVER_MSF_IMMUTABLE_PROPERTIES) {
            receiver_prop_walk_t inner = { d16, it.bytes.data, it.bytes.len, 0, true };
            receiver_prop_t in;
            while ((rc = receiver_prop_next(&inner, &in)) == MOQ_OK) {
                if (in.type == RECEIVER_MSF_IMMUTABLE_PROPERTIES) return MOQ_ERR_PROTO;
                if (in.type != type) continue;
                if (*present) return MOQ_ERR_UNSUPPORTED;
                *present = true;
                *out = in;
            }
            if (rc != MOQ_DONE) return rc;
            continue;
        }
        if (it.type != type) continue;
        if (*present) return MOQ_ERR_UNSUPPORTED;
        *present = true;
        *out = it;
    }
    return rc == MOQ_DONE ? MOQ_OK : rc;
}

/* What MSF_COMPRESSION (MSF-02 12.1) makes of one catalog or timeline
 * Object, from its origin's Track Properties and its own Object Properties.
 * This is the one place the receiver decides it. Value 0 (or no property)
 * is uncompressed. A publisher must not use both properties on a track
 * (12.1), so an Object carrying the Object property where the Track
 * property is set is dropped, whatever the values. MSF defines GZIP (1),
 * but this build does not decompress, so a compressed payload is refused
 * as an algorithm it does not support (12.1.1: "MUST NOT attempt to
 * process the payload"; 12.1.2: "MUST NOT attempt to process that
 * object"), like any value it does not know, a repeated property and a
 * malformed block: it is never processed as if uncompressed. */
typedef enum {
    RECEIVER_PAYLOAD_PLAIN = 0,       /* process as received */
    RECEIVER_PAYLOAD_CONFLICT,        /* drop the Object */
    RECEIVER_PAYLOAD_OBJECT_REFUSED,  /* drop the Object */
    RECEIVER_PAYLOAD_ORIGIN_REFUSED,  /* refuse every Object of the origin */
} receiver_payload_t;

static receiver_payload_t receiver_payload_compression(moq_version_t v,
                                                       moq_bytes_t track_props,
                                                       moq_bytes_t object_props)
{
    bool tp = false, op = false;
    receiver_prop_t t, o;
    if (receiver_prop_find(v, track_props, RECEIVER_MSF_COMPRESSION, &tp, &t) != MOQ_OK ||
        (tp && t.value != 0))
        return RECEIVER_PAYLOAD_ORIGIN_REFUSED;
    moq_result_t rc = receiver_prop_find(v, object_props, RECEIVER_MSF_COMPRESSION, &op, &o);
    if (tp && (op || rc == MOQ_ERR_UNSUPPORTED)) return RECEIVER_PAYLOAD_CONFLICT;
    if (rc != MOQ_OK || (op && o.value != 0)) return RECEIVER_PAYLOAD_OBJECT_REFUSED;
    return RECEIVER_PAYLOAD_PLAIN;
}

/* Whether a Properties block carries MSF_COMPRESSION at all (any value,
 * repeated or not, inside a well-formed block or not). */
static bool receiver_props_mention_compression(moq_version_t v, moq_bytes_t props)
{
    bool present = false;
    receiver_prop_t p;
    moq_result_t rc = receiver_prop_find(v, props, RECEIVER_MSF_COMPRESSION, &present, &p);
    return present || rc == MOQ_ERR_UNSUPPORTED;
}

static moq_bytes_t rcbuf_bytes(const moq_rcbuf_t *b)
{
    return b ? (moq_bytes_t){ moq_rcbuf_data(b), moq_rcbuf_len(b) }
             : (moq_bytes_t){ NULL, 0 };
}

/* The Track Properties of a subscription generation (its SUBSCRIBE_OK, or
 * PUBLISH on draft 22); empty when unknown. A fill inherits its parent's. */
static moq_bytes_t receiver_sub_track_props(const moq_media_receiver_t *r,
                                            const moq_sub_track_t *generation)
{
    moq_sub_track_result_t res;
    if (!r->sub || !generation ||
        moq_sub_get_track_result(r->sub, generation, &res, sizeof(res)) != MOQ_OK)
        return (moq_bytes_t){ NULL, 0 };
    return res.track_properties;
}

/* The Track Properties of a fetch's own response (FETCH_OK), its origin's:
 * a fetch never inherits its subscription's. Empty before the response is
 * seen (on drafts 16 and 18 a fetch's Objects may arrive ahead of it); such
 * an Object is then judged by its own properties. */
static moq_bytes_t receiver_fetch_track_props(const moq_media_receiver_t *r,
                                              const moq_sub_fetch_req_t *req)
{
    moq_sub_fetch_result_t res;
    if (!r->sub || !req ||
        moq_sub_get_fetch_result(r->sub, req, &res, sizeof(res)) != MOQ_OK ||
        !(res.flags & MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN))
        return (moq_bytes_t){ NULL, 0 };
    return res.track_properties;
}

/* Parse a catalog object, both revisions (the full extent of the catalog
 * API: MSF-02 vocabulary, publishTracks and track supplements included). */
static moq_result_t receiver_parse_catalog(moq_media_receiver_t *r,
                                           moq_bytes_t json,
                                           moq_msf_catalog_t *out)
{
    memset(out, 0, sizeof(*out));
    return moq_msf_catalog_parse_sized(&r->alloc, json, out, sizeof(*out));
}

/* What a catalog of this receiver is judged against: its namespace, which
 * a track without "namespace" inherits (MSF-02 5.2.2), and its handle cap,
 * which bounds a delta's effective catalog before any O(n) work. */
static moq_msf_validate_cfg_t receiver_catalog_vcfg(const moq_media_receiver_t *r)
{
    moq_msf_validate_cfg_t vc;
    moq_msf_validate_cfg_init(&vc);
    vc.catalog_namespace = r->namespace_;
    vc.max_effective_tracks = r->max_stable_handles;
    return vc;
}

/* The inbound semantic check of an independent catalog (moq_msf_catalog_check):
 * the raw parser is lenient, but a peer catalog with duplicate Full Track
 * Names, initDataList ids or contentProtection refIDs, or MSF-02 "depends"
 * entries that do not resolve, is rejected rather than coalesced. */
static bool receiver_catalog_ok(const moq_media_receiver_t *r,
                                const moq_msf_catalog_t *cat)
{
    moq_msf_validate_cfg_t vc = receiver_catalog_vcfg(r);
    return moq_msf_catalog_check(&r->alloc, cat, &vc, NULL) == MOQ_OK;
}

/* Apply a delta onto `base`, judged by the base's revision. Identity lookups
 * inside apply are indexed, so a large valid delta is O(base + delta). */
static moq_result_t receiver_apply_delta(const moq_media_receiver_t *r,
                                         const moq_msf_catalog_t *base,
                                         const moq_msf_catalog_t *delta,
                                         moq_msf_catalog_t *out)
{
    moq_msf_validate_cfg_t vc = receiver_catalog_vcfg(r);
    moq_alloc_t alloc = r->alloc;
    return moq_msf_catalog_apply_delta_sized(&alloc, base, delta, &vc, out,
                                             sizeof(*out));
}

/* Ingest one catalog object (network thread). object 0 = an independent catalog
 * generation (parse → adopt). object >= 1 in the latest group = a delta applied
 * in strict sequence onto the current effective catalog
 * (moq_msf_catalog_apply_delta → adopt). Any failure keeps the current
 * effective, counts a drop, and desyncs the group until the next independent
 * (object 0 of a newer group) re-syncs. */
/* Count a catalog Object MSF_COMPRESSION keeps from being processed;
 * false when the payload is plain. */
static bool receiver_catalog_compressed(moq_media_receiver_t *r,
                                        moq_bytes_t track_props,
                                        moq_bytes_t object_props)
{
    receiver_payload_t pc = receiver_payload_compression(r->transport_version,
                                                         track_props, object_props);
    if (pc == RECEIVER_PAYLOAD_PLAIN) return false;
    pthread_mutex_lock(&r->mu);
    if (pc == RECEIVER_PAYLOAD_CONFLICT) r->stats.compression_conflicts++;
    else r->stats.compression_refused++;
    pthread_mutex_unlock(&r->mu);
    return true;
}

/* `object_props` are the Object's properties and `track_props` its origin's
 * Track Properties (the catalog subscription's, or the Joining FETCH's own):
 * an Object MSF_COMPRESSION keeps from being processed fails as a malformed
 * catalog Object does. */
static void receiver_ingest_catalog(moq_media_receiver_t *r,
                                    uint64_t group_id, uint64_t object_id,
                                    moq_rcbuf_t *payload, moq_bytes_t object_props,
                                    moq_bytes_t track_props, uint64_t now_us)
{
    if (!payload) return;

    /* §5: ignore updates preceding the first Object of the latest Group. */
    if (r->have_latest_group && group_id < r->latest_group) return;

    bool first = !r->has_effective;
    moq_bytes_t json = { moq_rcbuf_data(payload), moq_rcbuf_len(payload) };

    if (object_id == 0) {
        /* Dedupe this generation's independent object (SUBSCRIBE and the Joining
         * FETCH both deliver object 0 of the latest group). */
        if (r->have_latest_group && group_id == r->latest_group) return;

        /* MSF §5.1.3: a complete catalog is terminal -- a later independent
         * generation admits no new tracks/content, so drop it (re-delivery of
         * the same generation was already deduped above, so this never inflates
         * the count on benign SUBSCRIBE/FETCH re-sends). A post-complete delta is
         * likewise refused by apply_delta and counted via the desync path. */
        if (r->has_effective && r->effective.is_complete) {
            receiver_count_drop(r);
            return;
        }

        moq_msf_catalog_t cand;
        if (receiver_catalog_compressed(r, track_props, object_props)) {
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        moq_result_t rc = receiver_parse_catalog(r, json, &cand);
        if (rc != MOQ_OK || cand.delta_update) {        /* delta on object 0 */
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        /* §5.2.35: reject a live track carrying trackDuration before adoption.
         * Treated like a malformed independent catalog: fatal on the first,
         * a counted drop (current effective preserved) on a later generation. */
        if (catalog_has_live_track_with_duration(&cand)) {
            moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        /* Inbound semantic validation (§5.2.3/§5.1.7/CMSF §4.1.1.1): the raw
         * parser is lenient, but before adopting a peer catalog we reject
         * duplicate track tuples / initDataList ids / contentProtection refIDs
         * rather than silently coalescing them. Treated like a malformed
         * independent catalog: fatal on the first, a counted drop afterwards. */
        if (!receiver_catalog_ok(r, &cand)) {
            moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        adopt_result_t a = receiver_adopt(r, &cand, now_us);
        if (a != ADOPT_OK) {
            moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (a == ADOPT_NOMEM) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            /* A first catalog that overflows a retained-state cap leaves no
             * usable effective: terminalize rather than hang unready. After an
             * effective exists, an over-cap (or immutable-violation) update is a
             * bounded drop that preserves the current effective. */
            if (a == ADOPT_LIMIT && first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_desync(r);   /* ADOPT_VIOLATION / post-ready ADOPT_LIMIT: drop */
            return;
        }
        r->latest_group = group_id;
        r->have_latest_group = true;
        r->cur_obj = 0;
        r->desynced = false;
        if (first) (void)rq_push(r, MOQ_MEDIA_CATALOG_READY, NULL);
        return;
    }

    /* object_id >= 1: a delta object. */
    if (first) {
        /* No independent base ever seen -- the broadcast must start with an
         * independent catalog. */
        receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
        return;
    }
    if (group_id > r->latest_group) {
        /* A delta for a newer group whose independent object 0 we have not seen
         * (out-of-order delivery): we cannot apply it. Drop + desync once; the
         * group's object 0, when it arrives, re-syncs via the object_id==0 path. */
        receiver_desync(r);
        return;
    }
    /* group_id == r->latest_group */
    if (r->desynced) return;                 /* ignore until next independent */
    if (object_id <= r->cur_obj) return;     /* duplicate already-applied delta */
    if (object_id != r->cur_obj + 1) {       /* gap / out-of-order */
        receiver_desync(r);
        return;
    }

    moq_msf_catalog_t delta;
    if (receiver_catalog_compressed(r, track_props, object_props)) {
        receiver_desync(r);
        return;
    }
    moq_result_t rc = receiver_parse_catalog(r, json, &delta);
    if (rc != MOQ_OK || !delta.delta_update) {   /* not a delta object */
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&r->alloc, &delta);
        receiver_desync(r);
        return;
    }
    moq_msf_catalog_t cand;
    /* The receiver never retains more than max_stable_handles tracks, so a
     * delta that would exceed that is refused (MOQ_ERR_PROTO -> desync). */
    rc = receiver_apply_delta(r, &r->effective, &delta, &cand);
    moq_msf_catalog_cleanup(&r->alloc, &delta);
    if (rc != MOQ_OK) { receiver_desync(r); return; }   /* apply failure */

    /* §5.2.35: a delta that yields a live track with trackDuration (e.g. a
     * delta `add` of a new live track carrying a duration) is dropped; the
     * current effective is preserved and the group desyncs until the next
     * independent catalog. */
    if (catalog_has_live_track_with_duration(&cand)) {
        moq_msf_catalog_cleanup(&r->alloc, &cand);
        receiver_desync(r);
        return;
    }
    adopt_result_t a = receiver_adopt(r, &cand, now_us);
    if (a != ADOPT_OK) {
        moq_msf_catalog_cleanup(&r->alloc, &cand);
        if (a == ADOPT_NOMEM) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
        receiver_desync(r);
        return;
    }
    r->cur_obj = object_id;   /* advance within the group */
}

/* During the initial D22 fill, effective is an unpublished staging catalog.
 * has_effective stays false, so no app view or media handle can borrow it. */
static void receiver_bootstrap_discard(moq_media_receiver_t *r)
{
    receiver_catalog_bootstrap_t *b = &r->catalog_bootstrap;
    if (!b->have_base) return;
    assert(!r->has_effective && r->retained_bytes >= b->retained_bytes);
    moq_msf_catalog_cleanup(&r->alloc, &r->effective);
    memset(&r->effective, 0, sizeof(r->effective));
    r->retained_bytes -= b->retained_bytes;
    b->retained_bytes = 0;
    b->have_base = false;
}

static void receiver_bootstrap_purge_live(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    /* No media handles are published before bootstrap commits. */
    while (r->catalog_objects_queued) {
        receiver_obj_entry_t *e = &r->objs[r->obj_head % r->ring_cap];
        assert(e->catalog);
        obj_unqueue_locked(r, e);
        obj_entry_release(r, &e->obj);
        memset(e, 0, sizeof(*e));
        r->obj_head++;
    }
    pthread_mutex_unlock(&r->mu);
}

static void receiver_bootstrap_fail(moq_media_receiver_t *r)
{
    receiver_bootstrap_discard(r);
    receiver_bootstrap_purge_live(r);
    r->catalog_bootstrap.active = false;
    r->catalog_ack_failed = true;
    receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
    moq_endpoint_wake(r->ep);
}

/* -- Handoff of retained buffers --------------------------------------- *
 * A buffer the receiver keeps for the application (a queued object's payload
 * and properties, staged catalog JSON, a TS window's held Object or parked
 * status) may be released on the application's thread, or on whichever
 * thread destroys the receiver. It is kept as is only when that is provably
 * safe: the buffer is the only reference to everything it keeps alive, it
 * keeps no data beyond its own bytes, and all of it is rcbuf-owned storage
 * released through the receiver's allocator (the thread-safe default) or the
 * endpoint's, which the endpoint contract requires to be thread-safe
 * (moq_core_internal_rcbuf_transferable). The check runs on the network
 * thread, which owns every reference, before anything retains the buffer.
 * Otherwise the buffer is replaced by an independent copy from the receiver's
 * allocator, and the shared one is released here, on its own thread. */
#ifdef MOQ_MEDIA_RECEIVER_TESTING
static uint64_t g_handoff_copies;
uint64_t moq_media_receiver_test_handoff_copies(void);
uint64_t moq_media_receiver_test_handoff_copies(void) { return g_handoff_copies; }
#define RECEIVER_NOTE_HANDOFF_COPY() (g_handoff_copies++)
/* Test-only interleaving hook, invoked with no locks held at the named
 * site so a test can change the application's intent at that instant:
 * 0, an object about to be admitted to the queue; 1, the window where a
 * reserved object's copies are made; 2, the window where a staged catalog
 * Object's copy is made. Never compiled into the shipping library. */
static void (*g_handoff_hook)(void *ctx, int site);
static void *g_handoff_hook_ctx;
void moq_media_receiver_test_set_handoff_hook(void (*fn)(void *, int), void *ctx);
void moq_media_receiver_test_set_handoff_hook(void (*fn)(void *, int), void *ctx)
{
    g_handoff_hook = fn;
    g_handoff_hook_ctx = ctx;
}
#define HANDOFF_HOOK(site) \
    do { if (g_handoff_hook) g_handoff_hook(g_handoff_hook_ctx, (site)); } while (0)
#else
#define RECEIVER_NOTE_HANDOFF_COPY() ((void)0)
#define HANDOFF_HOOK(site) ((void)0)
#endif

static bool receiver_transferable(const moq_media_receiver_t *r, const moq_rcbuf_t *b)
{
    if (!b) return true;
    const moq_alloc_t *trusted[2] = { &r->alloc, moq_endpoint_internal_alloc(r->ep) };
    return moq_core_internal_rcbuf_transferable(b, trusted, trusted[1] ? 2 : 1);
}

/* Replace *slot by an independent copy and release the shared buffer here.
 * On failure *slot is unchanged. */
static moq_result_t receiver_isolate(moq_media_receiver_t *r, moq_rcbuf_t **slot)
{
    moq_rcbuf_t *copy = NULL;
    moq_result_t rc = moq_rcbuf_clone(&r->alloc, *slot, &copy);
    if (rc != MOQ_OK) return rc;
    moq_rcbuf_decref(*slot);
    *slot = copy;
    RECEIVER_NOTE_HANDOFF_COPY();
    return MOQ_OK;
}

/* Keep *slot as is when transferable, else isolate it. False when the copy
 * cannot be allocated (*slot unchanged). The reorder windows' and the
 * ordering stage's retention points use it; queued objects and staged
 * catalog JSON isolate inside their own reservations. */
static bool receiver_own_for_handoff(moq_media_receiver_t *r, moq_rcbuf_t **slot)
{
    return receiver_transferable(r, *slot) || receiver_isolate(r, slot) == MOQ_OK;
}

/* Retain only the JSON payload in the existing object ring. These entries
 * share its physical count/byte ceiling, but are not app-pollable media. */
static void receiver_bootstrap_stage_live(moq_media_receiver_t *r,
                                           moq_sub_object_t *object)
{
    /* Live transport status markers carry no catalog JSON. */
    if (object->status != MOQ_OBJECT_NORMAL) return;

    size_t bytes = object->payload ? moq_rcbuf_len(object->payload) : 0;
    /* The properties travel with the JSON: MSF_COMPRESSION is judged when
     * the staged Object is replayed. */
    size_t prop_bytes = object->properties ? moq_rcbuf_len(object->properties) : 0;
    if (prop_bytes > SIZE_MAX - bytes) { receiver_bootstrap_fail(r); return; }
    pthread_mutex_lock(&r->mu);
    for (uint32_t i = r->obj_head; i != r->obj_tail; i++) {
        const receiver_obj_entry_t *e = &r->objs[i % r->ring_cap];
        if (e->catalog && e->obj.group_id == object->group_id &&
            e->obj.object_id == object->object_id) {
            pthread_mutex_unlock(&r->mu);
            return; /* An immutable catalog location occupies only one slot. */
        }
    }
    uint64_t ceiling = r->byte_cap;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
        ceiling += ceiling / 2; /* construction checked this sum */
    bool fits = r->obj_tail - r->obj_head < r->ring_cap &&
        obj_bytes_fit(r, bytes + prop_bytes, ceiling);
    if (fits && object->properties && !receiver_transferable(r, object->properties)) {
        r->obj_budget_total += bytes + prop_bytes;
        pthread_mutex_unlock(&r->mu);
        moq_result_t crc = receiver_isolate(r, &object->properties);
        pthread_mutex_lock(&r->mu);
        r->obj_budget_total -= bytes + prop_bytes;
        fits = crc == MOQ_OK && r->obj_tail - r->obj_head < r->ring_cap;
    }
    if (fits && object->payload && !receiver_transferable(r, object->payload)) {
        /* The staged JSON stays in the ring, which a destroying thread may
         * release, so a shared payload is replaced by a copy first. Its bytes
         * are reserved before the copy is made outside the lock; only this
         * (network) thread adds ring entries, so the slot stays free, and
         * an unusable copy returns the reservation and fails the bootstrap
         * as an entry that does not fit does. */
        r->obj_budget_total += bytes;
        pthread_mutex_unlock(&r->mu);
        moq_result_t crc = receiver_isolate(r, &object->payload);
        HANDOFF_HOOK(2);
        pthread_mutex_lock(&r->mu);
        r->obj_budget_total -= bytes;
        fits = crc == MOQ_OK && r->obj_tail - r->obj_head < r->ring_cap;
    }
    if (fits && object->payload) {
        receiver_obj_entry_t *e = &r->objs[r->obj_tail++ % r->ring_cap];
        memset(e, 0, sizeof(*e));
        e->catalog = true;
        e->budget = bytes + prop_bytes;
        e->obj.group_id = object->group_id;
        e->obj.object_id = object->object_id;
        e->obj.payload_ref = object->payload;
        object->payload = NULL;
        e->obj.properties_ref = object->properties;
        object->properties = NULL;
        r->obj_budget_total += bytes + prop_bytes;
        r->catalog_objects_queued++;
    }
    pthread_mutex_unlock(&r->mu);
    if (!fits || !bytes) receiver_bootstrap_fail(r);
}

static void receiver_bootstrap_replay_live(moq_media_receiver_t *r, uint64_t now_us)
{
    for (;;) {
        pthread_mutex_lock(&r->mu);
        if (!r->catalog_objects_queued) {
            pthread_mutex_unlock(&r->mu);
            return;
        }
        uint32_t first = r->obj_head;
        for (uint32_t i = first + 1; i != r->obj_tail; i++) {
            const moq_media_object_t *a = &r->objs[i % r->ring_cap].obj;
            const moq_media_object_t *b = &r->objs[first % r->ring_cap].obj;
            if (a->group_id < b->group_id ||
                (a->group_id == b->group_id && a->object_id < b->object_id))
                first = i;
        }
        receiver_obj_entry_t *selected = &r->objs[first % r->ring_cap];
        receiver_obj_entry_t *head = &r->objs[r->obj_head % r->ring_cap];
        assert(selected->catalog && head->catalog);
        moq_rcbuf_t *payload = selected->obj.payload_ref;
        moq_rcbuf_t *props = selected->obj.properties_ref;
        uint64_t group = selected->obj.group_id, object = selected->obj.object_id;
        obj_unqueue_locked(r, selected);
        if (selected != head) *selected = *head;
        memset(head, 0, sizeof(*head));
        r->obj_head++;
        pthread_mutex_unlock(&r->mu);
        receiver_ingest_catalog(r, group, object, payload, rcbuf_bytes(props),
                                receiver_sub_track_props(r, r->catalog_sub), now_us);
        moq_rcbuf_decref(payload);
        if (props) moq_rcbuf_decref(props);
        pthread_mutex_lock(&r->mu);
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal) {
            receiver_bootstrap_purge_live(r);
            return;
        }
    }
}

static void receiver_bootstrap_finish(moq_media_receiver_t *r, uint64_t now_us)
{
    receiver_catalog_bootstrap_t *b = &r->catalog_bootstrap;
    if (!b->active || !b->ack_seen || !b->terminal_seen) return;
    if (!b->has_largest && b->terminal_reason == MOQ_FILL_NO_STREAM && !b->have_base) {
        b->active = false; /* genuinely empty track; later live base is lawful */
        receiver_bootstrap_replay_live(r, now_us);
        return;
    }
    if (b->terminal_reason != MOQ_FILL_COMPLETE || !b->has_largest ||
        !b->have_base || b->group != b->largest_group || b->object != b->largest_object) {
        receiver_bootstrap_fail(r);
        return;
    }
    moq_msf_catalog_t candidate = r->effective;
    memset(&r->effective, 0, sizeof(r->effective));
    assert(r->retained_bytes >= b->retained_bytes);
    r->retained_bytes -= b->retained_bytes;
    b->retained_bytes = 0;
    b->have_base = false;
    b->active = false;
    adopt_result_t result = receiver_adopt(r, &candidate, now_us);
    if (result != ADOPT_OK) {
        moq_msf_catalog_cleanup(&r->alloc, &candidate);
        receiver_bootstrap_fail(r);
        return;
    }
    r->latest_group = b->group;
    r->have_latest_group = true;
    r->cur_obj = b->object;
    r->desynced = false;
    if (!rq_push(r, MOQ_MEDIA_CATALOG_READY, NULL)) {
        receiver_bootstrap_purge_live(r);
        return;
    }
    receiver_bootstrap_replay_live(r, now_us);
}

static void receiver_bootstrap_object(moq_media_receiver_t *r,
                                       const moq_sub_fill_item_t *item)
{
    receiver_catalog_bootstrap_t *b = &r->catalog_bootstrap;
    uint64_t group = item->u.object.group_id, object = item->u.object.object_id;
    if (!item->u.object.payload || item->u.object.subgroup_id != 0 ||
        (!b->have_base && object != 0) ||
        (b->have_base && (group != b->group || b->object == UINT64_MAX || object != b->object + 1)) ||
        (b->ack_seen && (!b->has_largest || group != b->largest_group || object > b->largest_object))) {
        receiver_bootstrap_fail(r);
        return;
    }
    /* The fill inherits the catalog subscription's Track Properties; an
     * Object it cannot process breaks the base-and-deltas sequence. */
    if (receiver_catalog_compressed(r, receiver_sub_track_props(r, r->catalog_sub),
                                    rcbuf_bytes(item->u.object.properties))) {
        receiver_bootstrap_fail(r);
        return;
    }
    moq_bytes_t json = { moq_rcbuf_data(item->u.object.payload), moq_rcbuf_len(item->u.object.payload) };
    moq_msf_catalog_t parsed, candidate;
    memset(&candidate, 0, sizeof(candidate));
    moq_result_t rc = receiver_parse_catalog(r, json, &parsed);
    if (rc != MOQ_OK) { receiver_bootstrap_fail(r); return; }
    if (!b->have_base) {
        if (parsed.delta_update) {
            moq_msf_catalog_cleanup(&r->alloc, &parsed);
            receiver_bootstrap_fail(r);
            return;
        }
        candidate = parsed;
    } else {
        rc = parsed.delta_update
            ? receiver_apply_delta(r, &r->effective, &parsed, &candidate)
            : MOQ_ERR_PROTO;
        moq_msf_catalog_cleanup(&r->alloc, &parsed);
        if (rc != MOQ_OK) { receiver_bootstrap_fail(r); return; }
    }
    uint64_t cost = est_catalog_retained_bytes(&candidate);
    uint64_t other = r->retained_bytes - b->retained_bytes;
    if (catalog_has_live_track_with_duration(&candidate) ||
        !receiver_catalog_ok(r, &candidate) ||
        (b->have_base && receiver_change_forbidden(r, &r->effective, &candidate)) ||
        candidate.track_count > r->max_stable_handles || cost > r->max_retained_bytes ||
        other > r->max_retained_bytes - cost) {
        moq_msf_catalog_cleanup(&r->alloc, &candidate);
        receiver_bootstrap_fail(r);
        return;
    }
    receiver_bootstrap_discard(r);
    r->effective = candidate;
    r->retained_bytes += cost;
    b->retained_bytes = cost;
    b->have_base = true;
    b->group = group;
    b->object = object;
}

static void receiver_bootstrap_fill(moq_media_receiver_t *r,
                                    const moq_sub_fill_item_t *item, uint64_t now_us)
{
    receiver_catalog_bootstrap_t *b = &r->catalog_bootstrap;
    if (!b->active) return;
    if (item->kind == MOQ_SUB_FILL_CREATED) {
        if (moq_sub_fill_is_valid(b->child)) receiver_bootstrap_fail(r);
        else b->child = item->request;
        return;
    }
    if (!moq_sub_fill_eq(b->child, item->request)) {
        receiver_bootstrap_fail(r);
        return;
    }
    if (item->kind == MOQ_SUB_FILL_OBJECT) receiver_bootstrap_object(r, item);
    else if (item->kind == MOQ_SUB_FILL_GAP) receiver_bootstrap_fail(r);
    else if (item->kind == MOQ_SUB_FILL_TERMINAL) {
        b->terminal_seen = true;
        b->terminal_reason = item->u.terminal.reason;
        if (b->terminal_reason != MOQ_FILL_COMPLETE && b->terminal_reason != MOQ_FILL_NO_STREAM)
            receiver_bootstrap_fail(r);
        else receiver_bootstrap_finish(r, now_us);
    }
}

/* Catalog bootstrap runs on the network thread. Legacy MSF-01 uses one Joining
 * FETCH(offset=0) after SUBSCRIBE_OK; its retained/live objects share the ingest
 * path, and a failed FETCH can still be followed by a live independent catalog.
 * D22 carries an initial fill instead and uses the transaction above: a failed
 * selected fill cannot be replaced by a later live generation. */
static bool receiver_fill_poll_ready(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    bool ready = r->ev_tail - r->ev_head < r->ev_cap;
    r->fill_blocked = !ready;
    pthread_mutex_unlock(&r->mu);
    return ready;
}

/* The facade's fill queue is read only while its next item, whatever it
 * is, can be taken: the event ring has room for an ordinary terminal and a
 * fill record is free for a CREATED that takes one (an MPEG2TS track's, or
 * any media track's with the stream-terminal lane). The item is never
 * inspected first and none is skipped or held back. A full record pool
 * frees by application polling alone: at most RECEIVER_SUB_MAX_FILLS
 * records are LIVE, and every other one is READY (freed by poll_track) or
 * waits only for its queued Objects to be polled or dropped and for its
 * RECEIVER_DROP records to be polled. */
static bool receiver_fill_items_ready(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    bool ready = r->ev_tail - r->ev_head < r->ev_cap;
    bool dom_free = false;
    for (size_t i = 0; i < RECEIVER_FILL_DOMS && !dom_free; i++)
        dom_free = r->fill_doms[i].state == DOM_FREE;
    ready = ready && dom_free;
    r->fill_blocked = !ready;
    pthread_mutex_unlock(&r->mu);
    return ready;
}

/* Wake once after unlocking for a record that became READY or was freed:
 * a waiting application has a terminal to poll, a gated executor room. */
static void receiver_dom_wake(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    bool wake = r->fill_dom_wake;
    r->fill_dom_wake = false;
    pthread_mutex_unlock(&r->mu);
    if (wake) moq_endpoint_wake(r->ep);
}

/* The LIVE fill record of facade child `token`, or -1. */
static int dom_find_locked(const moq_media_receiver_t *r, moq_sub_fill_t token)
{
    for (size_t i = 0; i < RECEIVER_FILL_DOMS; i++)
        if (r->fill_doms[i].state == DOM_LIVE &&
            moq_sub_fill_eq(r->fill_doms[i].token, token))
            return (int)i;
    return -1;
}

static bool track_is_ts(const moq_media_track_t *t);

/* A fill's End of Range (draft-22 section 11.4.1.2): every Location after
 * the previous Object or range end, through (g, ob), is in that state. An
 * Unknown or Timed-Out range is loss of the fill; a Non-Existent one is not.
 * Fills carry no Range Filters, so a jump the publisher leaves unmarked is
 * non-existent (section 3.2.2) and never reaches here. */
static void receiver_fill_ts_range_locked(moq_media_receiver_t *r, size_t idx,
                                          uint32_t kind, uint64_t g, uint64_t ob)
{
    receiver_fill_dom_t *d = &r->fill_doms[idx];
    receiver_loss_carry_t c = { 0, d->next_pos++, g, ob };
    if (d->has_prev) {
        c.group = d->prev_group;
        c.object = d->prev_object + 1;
        if (d->prev_object == UINT64_MAX) {
            c.group = d->prev_group + 1;
            c.object = 0;
        }
    }
    d->has_prev = true;
    d->prev_group = g;
    d->prev_object = ob;
    if (kind == MOQ_FETCH_RANGE_UNKNOWN) c.causes = MOQ_MEDIA_FILL_LOSS_UNKNOWN;
    else if (kind == MOQ_FETCH_RANGE_TIMED_OUT) c.causes = MOQ_MEDIA_FILL_LOSS_TIMED_OUT;
    if (c.causes) dom_note_loss_locked(r, idx, &c, r->obj_tail);
}

/* Called after receiver_fill_items_ready, or, retiring a failed catalog,
 * after receiver_fill_poll_ready. Only the executor produces ordinary
 * events. */
static void receiver_fill_observe(moq_media_receiver_t *r,
                                  const moq_sub_fill_item_t *item)
{
    if (item->track == r->catalog_sub) {
        if (item->kind == MOQ_SUB_FILL_CREATED) r->catalog_fill = item->request;
        if (item->kind == MOQ_SUB_FILL_TERMINAL &&
            moq_sub_fill_eq(item->request, r->catalog_fill))
            r->catalog_fill = (moq_sub_fill_t){0};
        return;
    }
    if (item->kind != MOQ_SUB_FILL_CREATED && item->kind != MOQ_SUB_FILL_TERMINAL)
        return;
    pthread_mutex_lock(&r->mu);
    receiver_fill_t *child = NULL, *free_slot = NULL;
    for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
        if (moq_sub_fill_eq(r->fills[i].token, item->request)) child = &r->fills[i];
        if (!moq_sub_fill_is_valid(r->fills[i].token)) free_slot = &r->fills[i];
    }
    if (item->kind == MOQ_SUB_FILL_CREATED) {
        moq_media_track_t *track = track_for_sub(r, item->track);
        /* The child of an admitted fill_track command, found by its token:
         * its subscription may already be released (a done), and the
         * receipt still belongs to the command. */
        moq_media_track_t *cmd = NULL;
        for (size_t i = 0; i < r->track_count && !cmd; i++)
            if (r->tracks[i]->cmd_state == FILL_CMD_ADMITTED &&
                moq_sub_fill_eq(r->tracks[i]->cmd_token, item->request))
                cmd = r->tracks[i];
        if (!track) track = cmd;
        if (track && !child) {
            /* No facade child slot can be reused before we consume its terminal. */
            assert(free_slot);
            *free_slot = (receiver_fill_t){ .token = item->request, .track = track,
                .initiating_request_id = item->initiating_request_id };
        }
        /* A failed catalog's retirement discards every fill Object, so its
         * fills take no record. Otherwise the gate held one free; were none
         * free, the fill would keep the immediate terminal of other tracks. */
        if (!r->catalog_ack_failed && track &&
            (track_is_ts(track) || r->st_enabled) &&
            dom_find_locked(r, item->request) < 0) {
            receiver_fill_dom_t *d = NULL;
            for (size_t i = 0; i < RECEIVER_FILL_DOMS && !d; i++)
                if (r->fill_doms[i].state == DOM_FREE) d = &r->fill_doms[i];
            if (d) {
                memset(d, 0, sizeof(*d));
                d->state = DOM_LIVE;
                d->token = item->request;
                d->track = track;
                d->gen = track->ts_gen;
                d->initiating_request_id = item->initiating_request_id;
                d->ts = track_is_ts(track);
            }
        }
        /* The receipt goes ahead of anything later of this child: its
         * terminal is queued, or made READY, behind it. */
        if (cmd) {
            assert(r->ev_tail - r->ev_head < r->ev_cap);
            receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
            *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_FILL_CREATED,
                .track = cmd, .initiating_request_id = item->initiating_request_id,
                .fill_cmd_seq = cmd->cmd_seq };
            cmd->cmd_state = FILL_CMD_RECEIPT;
            cmd->cmd_token = (moq_sub_fill_t){0};
        }
    } else {
        moq_media_track_t *track = child ? child->track : track_for_sub(r, item->track);
        bool duplicate = track && (track->delivery_failed ||
            (child && child->delivery_failed)) &&
            item->u.terminal.reason == MOQ_FILL_LOCAL_CANCELLED;
        int dom = dom_find_locked(r, item->request);
        if (dom >= 0) {
            /* The fill's terminal waits for its queued Objects and for its
             * loss records on the lane. */
            receiver_fill_dom_t *d = &r->fill_doms[dom];
            d->state = DOM_RETIRED;
            d->token = (moq_sub_fill_t){0};
            d->emit = !duplicate;
            d->reason = item->u.terminal.reason;
            d->error_domain = item->u.terminal.error_domain;
            d->error_code = item->u.terminal.error_code;
            d->initiating_request_id = item->initiating_request_id;
            if (d->queued == 0 && d->st_owed == 0) dom_ready_locked(r, d);
        } else if (track && !duplicate) {
            assert(r->ev_tail - r->ev_head < r->ev_cap);
            receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
            *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_FILL_TERMINAL,
                .track = track, .initiating_request_id = item->initiating_request_id,
                .fill_reason = item->u.terminal.reason,
                .fill_error_domain = item->u.terminal.error_domain,
                .fill_error_code = item->u.terminal.error_code };
        }
        if (child) memset(child, 0, sizeof(*child));
    }
    pthread_mutex_unlock(&r->mu);
}

static void receiver_cancel_fills(moq_media_receiver_t *r, uint64_t now_us)
{
    for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
        pthread_mutex_lock(&r->mu);
        receiver_fill_t *child = &r->fills[i];
        bool pending = child->cancel_pending;
        moq_sub_fill_t token = child->token;
        pthread_mutex_unlock(&r->mu);
        if (!pending) continue;
        moq_result_t rc = moq_sub_cancel_fill(r->sub, token, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_BUFFER) continue;
        /* A peer terminal may already be queued in the facade. Do not replace
         * it with a synthetic cancellation or free its identity prematurely. */
        pthread_mutex_lock(&r->mu);
        child->cancel_pending = false;
        child->terminal_seen = true;
        pthread_mutex_unlock(&r->mu);
        if (rc == MOQ_OK) moq_endpoint_wake(r->ep);
    }
}

/* One facade fill item, taken under receiver_fill_items_ready. Its refs
 * pass to the intake or are released here. */
static void receiver_take_fill_item(moq_media_receiver_t *r,
                                    moq_sub_fill_item_t *fi, uint64_t now_us)
{
    receiver_fill_observe(r, fi);
    receiver_dom_wake(r);
    if (fi->track == r->catalog_sub && r->joins_by_fill) {
        receiver_bootstrap_fill(r, fi, now_us);
        moq_sub_fill_item_cleanup(fi);
        return;
    }
    if (fi->kind == MOQ_SUB_FILL_CREATED) {
        moq_sub_fill_item_cleanup(fi);
        return;
    }
    if (fi->track == r->catalog_sub) {
        if (fi->kind == MOQ_SUB_FILL_OBJECT)
            receiver_ingest_catalog(r, fi->u.object.group_id,
                                    fi->u.object.object_id,
                                    fi->u.object.payload,
                                    rcbuf_bytes(fi->u.object.properties),
                                    receiver_sub_track_props(r, r->catalog_sub),
                                    now_us);
        moq_sub_fill_item_cleanup(fi);
        return;
    }
    pthread_mutex_lock(&r->mu);
    int dom = dom_find_locked(r, fi->request);
    if (dom >= 0 && fi->kind == MOQ_SUB_FILL_GAP)
        receiver_fill_ts_range_locked(r, (size_t)dom, fi->u.gap.range_kind,
                                      fi->u.gap.group_id, fi->u.gap.object_id);
    pthread_mutex_unlock(&r->mu);
    if (fi->kind == MOQ_SUB_FILL_OBJECT) {
        moq_media_track_t *t = track_for_sub(r, fi->track);
        if (t && !t->auth_failed &&
            (t->local_result == MOQ_OK || t->delivery_failed)) {
            /* Ownership of the item's refs passes to the intake. */
            moq_sub_object_t so;
            memset(&so, 0, sizeof(so));
            so.track = fi->track;
            so.group_id = fi->u.object.group_id;
            so.subgroup_id = fi->u.object.subgroup_id;
            so.object_id = fi->u.object.object_id;
            so.publisher_priority = fi->u.object.publisher_priority;
            so.status = MOQ_OBJECT_NORMAL;
            so.datagram = fi->u.object.datagram;
            so.payload = fi->u.object.payload;
            so.properties = fi->u.object.properties;
            fi->u.object.payload = NULL;
            fi->u.object.properties = NULL;
            if (t->is_sap_timeline || t->is_media_timeline ||
                t->is_event_timeline)
                moq_sub_object_cleanup(&so);
            else if (dom >= 0 && track_is_ts(t))
                receiver_on_fill_ts_object(r, t, &so, (size_t)dom,
                                           fi->initiating_request_id);
            else if (dom >= 0)
                receiver_on_fill_media_object(r, t, &so, (size_t)dom,
                                              fi->initiating_request_id);
            else
                receiver_on_media_object_with_origin(r, t, &so,
                    MOQ_MEDIA_DELIVERY_FILL, fi->initiating_request_id, now_us);
        } else if (dom >= 0) {
            /* Not taken by a failed track: a local drop of the fill. */
            pthread_mutex_lock(&r->mu);
            receiver_fill_dom_t *d = &r->fill_doms[dom];
            receiver_loss_carry_t c = { MOQ_MEDIA_FILL_LOSS_LOCAL_DROP,
                d->next_pos++, fi->u.object.group_id, fi->u.object.object_id };
            d->has_prev = true;
            d->prev_group = fi->u.object.group_id;
            d->prev_object = fi->u.object.object_id;
            dom_note_loss_locked(r, (size_t)dom, &c, r->obj_tail);
            pthread_mutex_unlock(&r->mu);
        }
    }
    moq_sub_fill_item_cleanup(fi);
}

static moq_media_auth_request_t receiver_auth_request(moq_media_receiver_t *r,
    moq_media_auth_action_t action, moq_media_track_t *track)
{
    moq_media_auth_request_t request = {0};
    request.struct_size = sizeof(request);
    request.action = action;
    request.track_namespace = track ? track->ns_fields : r->namespace_;
    request.track_name = track ? track->desc.name :
        (moq_bytes_t){r->catalog_name, r->catalog_name_len};
    return request;
}

static bool receiver_admission_retry(moq_result_t rc)
{
    return rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_REQUEST_BLOCKED;
}

static void receiver_auth_clear_pending(moq_media_receiver_t *r)
{
    moq_media_auth_operation_clear(&r->catalog_auth, &r->alloc);
    moq_media_auth_operation_clear(&r->fetch_auth, &r->alloc);
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_auth_operation_clear(&r->tracks[i]->auth_operation, &r->alloc);
        receiver_cmd_auth_release(r, r->tracks[i]);
        receiver_fetch_auth_release(r, r->tracks[i]);
    }
}

/* Native INVAL can describe identity conflicts, not just token encoding.
 * Preserve ordinary handling instead of inventing an auth diagnosis. */
static bool receiver_auth_encoding_error(moq_media_receiver_t *r,
    moq_media_auth_operation_t *op, moq_result_t rc)
{
    if (!r->auth_source.configured) return false;
    if (rc == MOQ_ERR_BUFFER) op->outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT;
    else if (rc == MOQ_ERR_UNSUPPORTED)
        op->outcome = MOQ_MEDIA_AUTH_OUTCOME_UNSUPPORTED_PROFILE;
    else return false;
    op->status = op->result = rc;
    return true;
}

static void receiver_auth_fail(moq_media_receiver_t *r,
    moq_media_auth_operation_t *op, const moq_media_auth_request_t *request,
    moq_media_auth_scope_t scope, moq_media_track_t *t)
{
    pthread_mutex_lock(&r->mu);
    if (scope == MOQ_MEDIA_AUTH_SCOPE_SERVICE) {
        r->fatal = true;
        r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED;
        /* Reuse the child-aware executor drain, not an auth-only shortcut. */
        r->catalog_ack_failed = true;
    } else if (scope == MOQ_MEDIA_AUTH_SCOPE_REQUEST) {
        r->catalog_fetch_issued = true;
    } else {
        t->auth_failed = true;
        t->desired_subscribed = false;
        t->upd_notify_app = false;
        t->update_inflight = false;
        t->upd_sent_app_gen = 0;
        t->done_pending = false;
        obj_compact_locked(r, keep_not_track, t);
        uint32_t out = r->sap_head;
        for (uint32_t in = r->sap_head; in != r->sap_tail; in++) {
            if (r->saps[in % r->sap_cap].track != t)
                r->saps[out++ % r->sap_cap] = r->saps[in % r->sap_cap];
        }
        r->sap_tail = out;
        out = r->mt_head;
        for (uint32_t in = r->mt_head; in != r->mt_tail; in++) {
            if (r->mts[in % r->mt_cap].track != t)
                r->mts[out++ % r->mt_cap] = r->mts[in % r->mt_cap];
        }
        r->mt_tail = out;
    }
    pthread_mutex_unlock(&r->mu);
    if (t) {
        receiver_ts_free(r, t);
        receiver_order_free(r, t);
    }
    moq_media_auth_operation_notify(op, &r->auth_source, request, scope, t);
    moq_media_auth_operation_clear(op, &r->alloc);
    moq_endpoint_wake(r->ep);
}

/* -- Standalone fetches of media tracks (moq_media_receiver_fetch_track) -- *
 * A sent fetch owns a record in the fetch half of fill_doms from its
 * FETCH_CREATED until its FETCH_TERMINAL is polled. Its Objects take the
 * fill intake with that record, so they queue, drop and report loss as a
 * fill's do, and its terminal waits for them (dom_unqueue_locked). The
 * executor creates, retires and cancels records; the application frees a
 * READY one by polling it and marks a LIVE one for cancellation. */

/* The LIVE fetch record of facade request `req`, or -1. */
static int dom_fetch_find_locked(const moq_media_receiver_t *r,
                                 const moq_sub_fetch_req_t *req)
{
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++)
        if (r->fill_doms[i].state == DOM_LIVE && r->fill_doms[i].fetch_req == req)
            return (int)i;
    return -1;
}

static bool dom_fetch_any_live(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    bool live = false;
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS && !live; i++)
        live = r->fill_doms[i].state == DOM_LIVE;
    pthread_mutex_unlock(&r->mu);
    return live;
}

/* The fetch ended with `reason`: its FETCH_TERMINAL is pollable once its
 * queued Objects have left and its loss records on the lane are polled. */
static void dom_fetch_retire_locked(moq_media_receiver_t *r, receiver_fill_dom_t *d,
                                    uint32_t reason, uint32_t domain,
                                    uint64_t code, int32_t local_result)
{
    assert(d->fetch && d->state == DOM_LIVE);
    d->state = DOM_RETIRED;
    d->fetch_req = NULL;
    d->cancel_pending = false;
    d->emit = true;
    d->reason = reason;
    d->error_domain = domain;
    d->error_code = code;
    d->local_result = local_result;
    if (d->queued == 0 && d->st_owed == 0) dom_ready_locked(r, d);
}

/* A facade terminal item of LIVE fetch record `d` ends it; any other item
 * kind is left to the caller. Caller holds r->mu. */
static void dom_fetch_item_terminal_locked(moq_media_receiver_t *r,
                                           receiver_fill_dom_t *d,
                                           const moq_sub_fetch_item_t *item)
{
    if (item->kind == MOQ_SUB_FETCH_ERROR)
        dom_fetch_retire_locked(r, d, MOQ_MEDIA_FETCH_PEER_ERROR,
                                MOQ_MEDIA_FETCH_ERROR_REQUEST,
                                (uint64_t)item->u.error.error_code, 0);
    else if (item->kind == MOQ_SUB_FETCH_RESET)
        dom_fetch_retire_locked(r, d, MOQ_MEDIA_FETCH_RESET,
                                MOQ_MEDIA_FETCH_ERROR_STREAM,
                                item->u.reset.error_code, 0);
    else if (item->kind == MOQ_SUB_FETCH_COMPLETE)
        dom_fetch_retire_locked(r, d, MOQ_MEDIA_FETCH_COMPLETE,
                                MOQ_MEDIA_FETCH_ERROR_NONE, 0, 0);
}

/* One facade item of a media fetch. Its refs pass to the intake or are
 * released here. An item of a fetch whose record already ended (cancelled
 * locally, failed locally or closed) is discarded. */
static void receiver_take_fetch_item(moq_media_receiver_t *r,
                                     moq_sub_fetch_item_t *item)
{
    pthread_mutex_lock(&r->mu);
    int dom = dom_fetch_find_locked(r, item->request);
    if (dom < 0) {
        pthread_mutex_unlock(&r->mu);
        moq_sub_fetch_item_cleanup(item);
        return;
    }
    receiver_fill_dom_t *d = &r->fill_doms[dom];
    moq_media_track_t *t = d->track;
    uint64_t id = d->initiating_request_id;
    bool object = false;
    switch (item->kind) {
    case MOQ_SUB_FETCH_OBJECT:
        object = true;
        break;
    case MOQ_SUB_FETCH_GAP:
        receiver_fill_ts_range_locked(r, (size_t)dom, (uint32_t)item->u.gap.range_kind,
                                      item->u.gap.group_id, item->u.gap.object_id);
        break;
    default:
        dom_fetch_item_terminal_locked(r, d, item);
        break;
    }
    bool usable = object && !t->auth_failed &&
                  (t->local_result == MOQ_OK || t->delivery_failed);
    if (object) t->fetch_objects_received++;
    if (object && !usable) {
        /* Not taken by a failed track: a local drop of the fetch. */
        receiver_loss_carry_t c = { MOQ_MEDIA_FILL_LOSS_LOCAL_DROP,
            d->next_pos++, item->u.object.group_id, item->u.object.object_id };
        d->has_prev = true;
        d->prev_group = item->u.object.group_id;
        d->prev_object = item->u.object.object_id;
        dom_note_loss_locked(r, (size_t)dom, &c, r->obj_tail);
    }
    pthread_mutex_unlock(&r->mu);
    if (usable) {
        /* Ownership of the item's refs passes to the intake. A fetched
         * Object has no subgroup in the facade item; the track's
         * subscription (read here, on the executor) supplies its Track
         * Properties. */
        moq_sub_object_t so;
        memset(&so, 0, sizeof(so));
        so.track = t->sub_track;
        so.group_id = item->u.object.group_id;
        so.object_id = item->u.object.object_id;
        so.publisher_priority = item->u.object.publisher_priority;
        so.status = MOQ_OBJECT_NORMAL;
        so.payload = item->u.object.payload;
        so.properties = item->u.object.properties;
        item->u.object.payload = NULL;
        item->u.object.properties = NULL;
        if (t->is_sap_timeline || t->is_media_timeline || t->is_event_timeline)
            moq_sub_object_cleanup(&so);
        else if (track_is_ts(t))
            receiver_on_fill_ts_object(r, t, &so, (size_t)dom, id);
        else
            receiver_on_fill_media_object(r, t, &so, (size_t)dom, id);
    }
    moq_sub_fetch_item_cleanup(item);
}

/* Drain the facade's fetch items: the catalog's joining FETCH (D16/18) and
 * the media fetches. While a media fetch is open each item is an intake
 * (receiver_intake_begin), as a fill item is; otherwise the loop is the
 * catalog's alone, unchanged. */
static void receiver_poll_fetch_items(moq_media_receiver_t *r, uint64_t now_us)
{
    moq_sub_fetch_item_t item;
    for (;;) {
        bool media = dom_fetch_any_live(r);
        if (media && !receiver_intake_begin(r)) break;
        if (moq_sub_poll_fetch(r->sub, &item) != MOQ_OK) {
            if (media) receiver_intake_end(r);
            break;
        }
        if (item.request == r->catalog_fetch) {
            if (item.kind == MOQ_SUB_FETCH_OBJECT)
                receiver_ingest_catalog(r, item.u.object.group_id,
                                        item.u.object.object_id,
                                        item.u.object.payload,
                                        rcbuf_bytes(item.u.object.properties),
                                        receiver_fetch_track_props(r, item.request),
                                        now_us);
            /* moq_sub_poll_fetch already released the request slot for terminal
             * items; drop our handle (only on our own fetch) so a recycled slot
             * can't later match. */
            if (item.kind == MOQ_SUB_FETCH_ERROR ||
                item.kind == MOQ_SUB_FETCH_COMPLETE ||
                item.kind == MOQ_SUB_FETCH_RESET)
                r->catalog_fetch = NULL;
            moq_sub_fetch_item_cleanup(&item);
        } else if (media) {
            receiver_take_fetch_item(r, &item);
        } else {
            moq_sub_fetch_item_cleanup(&item);
        }
        if (media) receiver_intake_end(r);
    }
    /* A terminal read here may have made a fetch record ready. */
    receiver_dom_wake(r);
}

static void receiver_pump_catalog_fetch(moq_media_receiver_t *r,
                                        uint64_t now_us)
{
    if (!r->sub || !r->catalog_sub) return;

    moq_sub_track_state_t state = moq_sub_track_get_state(r->catalog_sub);
    if (!r->catalog_fetch_issued &&
        (state == MOQ_SUB_TRACK_DONE || state == MOQ_SUB_TRACK_ERROR)) {
        moq_media_auth_operation_clear(&r->fetch_auth, &r->alloc);
        r->catalog_fetch_issued = true;
    }
    if (!r->joins_by_fill &&
        !r->catalog_fetch_issued && moq_sub_track_is_active(r->catalog_sub)) {
        moq_sub_joining_fetch_cfg_t fcfg;
        moq_sub_joining_fetch_cfg_init_sized(&fcfg, sizeof(fcfg));
        fcfg.track = r->catalog_sub;
        fcfg.relative = true;     /* offset measured from the largest group */
        fcfg.joining_start = 0;   /* §5: offset = 0 -> the latest group */
        moq_media_auth_request_t request = receiver_auth_request(r,
            MOQ_MEDIA_AUTH_ACTION_FETCH, NULL);
        moq_result_t rc = moq_media_auth_operation_prepare(&r->fetch_auth,
            &r->auth_source, &request, r->transport_version, &r->alloc);
        AUTH_PRE_ADMIT(r, NULL);
        pthread_mutex_lock(&r->mu);
        bool current = !r->fatal && !r->catalog_fetch_issued;
        pthread_mutex_unlock(&r->mu);
        if (!current || !moq_sub_track_is_active(r->catalog_sub)) {
            moq_media_auth_operation_clear(&r->fetch_auth, &r->alloc);
            return;
        }
        if (rc != MOQ_OK) {
            receiver_auth_fail(r, &r->fetch_auth, &request,
                MOQ_MEDIA_AUTH_SCOPE_REQUEST, NULL);
            return;
        }
        fcfg.auth_tokens = r->fetch_auth.list.tokens;
        fcfg.auth_token_count = r->fetch_auth.list.count;
        rc = moq_sub_joining_fetch(r->sub, &fcfg, now_us,
                                                &r->catalog_fetch);
        /* WOULD_BLOCK (no fetch slot / action queue full) is transient: leave
         * the flag clear so the next pump cycle retries. Any other error means
         * the FETCH cannot be placed -- give up and rely on the SUBSCRIBE path
         * rather than spin. */
        if (receiver_auth_encoding_error(r, &r->fetch_auth, rc))
            receiver_auth_fail(r, &r->fetch_auth, &request,
                MOQ_MEDIA_AUTH_SCOPE_REQUEST, NULL);
        else if (!receiver_admission_retry(rc)) {
            r->catalog_fetch_issued = true;
            moq_media_auth_operation_clear(&r->fetch_auth, &r->alloc);
        }
    }

    receiver_poll_fetch_items(r, now_us);

    /* Catalog fills stay private until bootstrap commits. Media fills share
     * live-object conversion and preserve their distinct origin/terminal;
     * a failed media fill does not end its parent subscription. */
    moq_sub_fill_item_t fi;
    while (receiver_fill_items_ready(r) && receiver_intake_begin(r) &&
           moq_sub_poll_fill(r->sub, &fi, sizeof(fi)) == MOQ_OK) {
        receiver_take_fill_item(r, &fi, now_us);
        receiver_intake_end(r);
    }
    receiver_intake_end(r);
}

/* Mark a media track ended (network thread) and emit TRACK_ENDED exactly once.
 * `ended` is read cross-thread by the public subscribe_track(), so the RMW is
 * done under r->mu; rq_push takes the lock itself, so emit after releasing. */
static void receiver_mark_track_ended(moq_media_receiver_t *r,
                                      moq_media_track_t *t)
{
    pthread_mutex_lock(&r->mu);
    bool first = !t->ended && t->local_result == MOQ_OK && !t->auth_failed &&
                 !t->completion_cancelled;
    if (first) t->ended = true;
    pthread_mutex_unlock(&r->mu);
    moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
    if (first)
        (void)rq_push(r, MOQ_MEDIA_TRACK_ENDED, t);
}

/* As receiver_mark_track_ended, for an end its subscription's done gave,
 * with that done's status. */
static void receiver_mark_track_done(moq_media_receiver_t *r,
                                     moq_media_track_t *t, uint64_t status)
{
    pthread_mutex_lock(&r->mu);
    bool first = !t->ended && t->local_result == MOQ_OK && !t->auth_failed &&
                 !t->completion_cancelled;
    if (first) t->ended = true;
    pthread_mutex_unlock(&r->mu);
    moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
    if (!first || !rq_push(r, MOQ_MEDIA_TRACK_ENDED, t)) return;
    pthread_mutex_lock(&r->mu);
    receiver_event_t *e = &r->events[(r->ev_tail - 1) % r->ev_cap];
    e->has_done_status = true;
    e->done_status = status;
    pthread_mutex_unlock(&r->mu);
}

static void receiver_mark_track_local_failure(moq_media_receiver_t *r,
                                               moq_media_track_t *t,
                                               moq_result_t result,
                                               moq_media_track_event_kind_t kind)
{
    pthread_mutex_lock(&r->mu);
    bool first = t->local_result == MOQ_OK;
    if (first) {
        t->local_result = result;
        t->local_failure_kind = kind;
        t->local_failure_pending = true;
        r->local_failure_pending_count++;
        t->desired_subscribed = false;
        t->upd_notify_app = false;
        t->update_inflight = false;
        t->upd_sent_app_gen = 0;
        obj_compact_locked(r, keep_not_track, t);
    }
    pthread_mutex_unlock(&r->mu);
    if (first) moq_endpoint_wake(r->ep);
}

/* The facade owns a notice until we have credit for its possible FILL event.
 * Poll only on the executor, before object conversion or owner retirement.
 * TRACK reuses its existing latch; FILL uses the existing event ring. */
static void receiver_inspect_delivery_failures(moq_media_receiver_t *r)
{
    moq_sub_local_failure_t failure;
    while (receiver_fill_poll_ready(r) &&
           moq_sub_poll_local_failure(r->sub, &failure, sizeof(failure)) == MOQ_OK) {
        bool catalog = failure.owner_kind == MOQ_SUB_LOCAL_FAILURE_FETCH
            ? failure.fetch == r->catalog_fetch
            : failure.track == r->catalog_sub;
        if (catalog) {
            if (r->catalog_bootstrap.active) receiver_bootstrap_fail(r);
            else {
                r->catalog_ack_failed = true;
                receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
                moq_endpoint_wake(r->ep);
            }
            continue;
        }
        if (failure.owner_kind == MOQ_SUB_LOCAL_FAILURE_FETCH) {
            /* A media fetch's Objects exceeded the facade's budget: the
             * facade cancels it; that fetch alone fails. */
            pthread_mutex_lock(&r->mu);
            int dom = dom_fetch_find_locked(r, failure.fetch);
            if (dom >= 0) {
                receiver_fill_dom_t *d = &r->fill_doms[dom];
                receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
                *event = (receiver_event_t){
                    .kind = MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE,
                    .track = d->track, .local_result = failure.local_result,
                    .delivery_origin = MOQ_MEDIA_DELIVERY_FETCH,
                    .delivery_initiating_request_id = d->initiating_request_id };
                dom_fetch_retire_locked(r, d, MOQ_MEDIA_FETCH_LOCAL_FAILURE,
                                        MOQ_MEDIA_FETCH_ERROR_NONE, 0,
                                        failure.local_result);
            }
            pthread_mutex_unlock(&r->mu);
            receiver_dom_wake(r);
            if (dom >= 0) moq_endpoint_wake(r->ep);
            continue;
        }
        moq_media_track_t *track = track_for_sub(r, failure.track);
        if (!track) continue;
        bool wake = false;
        pthread_mutex_lock(&r->mu);
        if (failure.owner_kind == MOQ_SUB_LOCAL_FAILURE_TRACK &&
            track->local_result == MOQ_OK) {
            track->local_result = failure.local_result;
            track->local_failure_kind = MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE;
            track->delivery_failed = true;
            track->delivery_failure_staged = true;
            track->upd_notify_app = false;
            track->update_inflight = false;
            track->upd_sent_app_gen = 0;
            /* Keep app intent and admitted data. Facade cancellation stops
             * new intake; an explicit app unsubscribe still purges as usual. */
            wake = true;
        } else if (failure.owner_kind == MOQ_SUB_LOCAL_FAILURE_FILL) {
            receiver_fill_t *child = NULL, *free_slot = NULL;
            for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
                if (moq_sub_fill_eq(r->fills[i].token, failure.fill)) child = &r->fills[i];
                if (!moq_sub_fill_is_valid(r->fills[i].token)) free_slot = &r->fills[i];
            }
            /* CREATED may still be in the facade behind discovery pressure. */
            if (!child) {
                assert(free_slot);
                child = free_slot;
                *child = (receiver_fill_t){ .token = failure.fill, .track = track,
                    .initiating_request_id = failure.initiating_request_id };
            }
            child->delivery_failed = true;
            receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
            *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE,
                .track = track, .local_result = failure.local_result,
                .delivery_origin = MOQ_MEDIA_DELIVERY_FILL,
                .delivery_initiating_request_id = failure.initiating_request_id };
            wake = true;
        }
        pthread_mutex_unlock(&r->mu);
        if (wake) moq_endpoint_wake(r->ep);
    }
}

/* Publish each staged LIVE delivery failure once its track can deliver
 * nothing more: no input it admitted is still in the facade (transferred
 * into the receiver's queues, where the application takes it before the
 * failure takes effect, or disposed of), and no fill of it is still open.
 * A failed subscription is cancelled by the facade's own retry, which
 * cancels its fills locally; a fill slot stays until its terminal is
 * taken, so an empty queue alone cannot rule out a fill's later Object.
 * Executor only; the facade is not read from any other thread. */
static void receiver_publish_delivery_failures(moq_media_receiver_t *r)
{
    bool wake = false;
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->delivery_failure_staged) continue;
        if (t->sub_track && (moq_sub_track_has_queued_input(r->sub, t->sub_track) ||
                             moq_sub_fill_is_valid(moq_sub_track_fill(t->sub_track))))
            continue;
        pthread_mutex_lock(&r->mu);
        t->delivery_failure_staged = false;
        t->local_failure_pending = true;
        r->local_failure_pending_count++;
        pthread_mutex_unlock(&r->mu);
        wake = true;
    }
    if (wake) moq_endpoint_wake(r->ep);
}

/* Neither tick's result nor on_subscribed identifies every retained error:
 * terminal dispatch can mask it, and failed retention has no success callback.
 * Query before converting any objects or releasing any subscription handle. */
static void receiver_inspect_ack_results(moq_media_receiver_t *r, uint64_t now_us)
{
    moq_sub_track_result_t result;
    if (r->catalog_sub) {
        moq_result_t rc = moq_sub_get_track_result(r->sub, r->catalog_sub,
                                                   &result, sizeof(result));
        if (rc == MOQ_ERR_NOMEM || rc == MOQ_ERR_BUFFER) {
            if (r->catalog_bootstrap.active) receiver_bootstrap_fail(r);
            else {
                r->catalog_ack_failed = true;
                receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
                moq_endpoint_wake(r->ep);
            }
        } else if (rc == MOQ_OK && r->catalog_bootstrap.active &&
                   !r->catalog_bootstrap.ack_seen &&
                   (result.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN)) {
            receiver_catalog_bootstrap_t *b = &r->catalog_bootstrap;
            b->ack_seen = true;
            b->has_largest = (result.flags & MOQ_SUB_TRACK_RESULT_HAS_LARGEST) != 0;
            b->largest_group = result.largest_group;
            b->largest_object = result.largest_object;
            if (b->have_base && (!b->has_largest || b->group != b->largest_group ||
                                 b->object > b->largest_object))
                receiver_bootstrap_fail(r);
            else receiver_bootstrap_finish(r, now_us);
        }
    }
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->sub_track || t->local_result != MOQ_OK || t->auth_failed) continue;
        moq_result_t rc = moq_sub_get_track_result(r->sub, t->sub_track,
                                                   &result, sizeof(result));
        if (rc == MOQ_ERR_NOMEM || rc == MOQ_ERR_BUFFER)
            receiver_mark_track_local_failure(r, t, rc, MOQ_MEDIA_TRACK_LOCAL_FAILURE);
    }
}

/* Subscriber callback (network thread): a media track's SUBSCRIBE_OK landed, so
 * it is now ACTIVE (not just requested). Publish `established` under r->mu for
 * the track_state accessor. The catalog subscription's OK is irrelevant here. */
static void receiver_on_subscribed(void *ctx, moq_sub_track_t *track)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) return;
    moq_media_track_t *t = track_for_sub(r, track);
    if (!t) return;
    pthread_mutex_lock(&r->mu);
    t->established = true;
    pthread_mutex_unlock(&r->mu);
}

/* Subscriber callback (fires synchronously inside moq_sub_tick on the
 * network thread): a rejected catalog subscription is terminal -- the
 * receiver can never discover anything, and silently idling forever would
 * strand wait()/poll_track() in DONE. */
static void receiver_on_subscribe_error(void *ctx, moq_sub_track_t *track,
                                        moq_request_error_t error_code,
                                        moq_bytes_t reason)
{
    (void)error_code; (void)reason;
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) {
        receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_REJECTED);
        if (r->catalog_bootstrap.active) receiver_bootstrap_fail(r);
        return;
    }
    /* A refused media-track subscription is track data, never receiver
     * EOF: the track ends before it began. */
    moq_media_track_t *t = track_for_sub(r, track);
    if (t)
        receiver_mark_track_ended(r, t);
}

/* the peer acknowledged one of our Forward updates. Runs inside
 * moq_sub_tick on the network thread. Emits MOQ_MEDIA_TRACK_UPDATE_OK only
 * when the acknowledged update carries app attribution; pure
 * flow-control acknowledgments are consumed silently. The event's scalars
 * are copied at push time (queue-entry copies). */
/* Subscriber callback on the network thread inside moq_sub_tick (receiver_hook,
 * ep->mu held): the peer's session-wide GOAWAY. The endpoint keeps its New
 * Session URI for moq_endpoint_get_goaway_uri(). */
static void receiver_on_goaway(void *ctx, moq_bytes_t new_session_uri)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    moq_endpoint_note_goaway_locked(r->ep, new_session_uri);
}

static void receiver_on_update_ok(void *ctx, moq_sub_track_t *track,
                                  const moq_sub_update_result_t *result)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    moq_media_track_t *t = NULL;
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->sub_track == track) { t = r->tracks[i]; break; }
    if (!t) return;
    pthread_mutex_lock(&r->mu);
    bool notify = t->upd_notify_app && !t->auth_failed;
    t->upd_notify_app = false;
    t->update_inflight = false;
    t->upd_sent_app_gen = 0;
    if (!notify) { pthread_mutex_unlock(&r->mu); return; }
    if (r->ev_tail - r->ev_head >= r->ev_cap) {
        r->fatal = true;
        r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW;
        pthread_mutex_unlock(&r->mu);
        return;
    }
    receiver_event_t *e = &r->events[r->ev_tail % r->ev_cap];
    memset(e, 0, sizeof(*e));
    e->kind = MOQ_MEDIA_TRACK_UPDATE_OK;
    e->track = t;
    e->has_largest = result->has_largest;
    e->largest_group = result->largest_group;
    e->largest_object = result->largest_object;
    e->has_expires = result->has_expires;
    e->expires_ms = result->expires_ms;
    r->ev_tail++;
    pthread_mutex_unlock(&r->mu);
}

/* Record completion here and process it after draining this tick's objects.
 * D22 catalog completion likewise follows queued fill items, preserving an
 * already committed COMPLETE before deciding bootstrap failed. */
static void receiver_on_subscribe_done(void *ctx, moq_sub_track_t *track,
                                       uint64_t status_code)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) {
        if (r->joins_by_fill && !r->has_effective) r->catalog_done_pending = true;
        return;
    }
    moq_media_track_t *t = track_for_sub(r, track);
    if (!t || t->auth_failed) return;
    t->done_pending = true;
    t->done_status = status_code;
    if (status_code == MOQ_STREAM_ERROR_MALFORMED_TRACK) {
        pthread_mutex_lock(&r->mu);
        r->stats.tracks_malformed++;
        pthread_mutex_unlock(&r->mu);
    }
}

/* -- Media object intake (network thread) -------------------------------- */

/* An Object's loss report as its reorder window produced it (MPEG2TS). */
typedef struct receiver_loss {
    bool     gap;
    uint64_t gap_group;
    uint64_t gap_object;
    bool     gap_tail_unknown;
    bool     gap_duration_known;
    uint64_t gap_duration_27mhz;
    bool     discontinuous;
} receiver_loss_t;

/* A fill Object of an MPEG2TS track: its record and its delivery-order
 * position in the fill. */
typedef struct receiver_dom_ref {
    size_t   idx;
    uint64_t pos;
} receiver_dom_ref_t;

/* A TS Object prepared as a candidate for its subscription's join PSI: the
 * PSI octets its payload starts with (see receiver_obj_entry_t). */
typedef struct receiver_join {
    uint32_t psi_len;
} receiver_join_t;

/* A Location-tracked Object (loss != NULL, or a fill Object with `dom`)
 * that will not be queued is a local loss: the next queued Object of its
 * delivery domain reports it. Caller holds r->mu. */
static void receiver_loss_dropped_locked(moq_media_receiver_t *r,
                                         moq_media_track_t *track,
                                         const moq_sub_object_t *so,
                                         const receiver_loss_t *loss,
                                         const receiver_dom_ref_t *dom)
{
    if (dom) {
        receiver_loss_carry_t c = { MOQ_MEDIA_FILL_LOSS_LOCAL_DROP, dom->pos,
                                    so->group_id, so->object_id };
        dom_note_loss_locked(r, dom->idx, &c, r->obj_tail);
        return;
    }
    if (!loss) return;
    uint64_t g = so->group_id, ob = so->object_id;
    if (loss->gap) { g = loss->gap_group; ob = loss->gap_object; }
    ts_note_loss_locked(r, track, track->ts_gen, g, ob, r->obj_tail);
}

/* Parse one facade object for a media track and enqueue it. Takes over the
 * facade object's payload/properties refs on success or releases them on
 * drop -- the caller must NOT also call moq_sub_object_cleanup.
 *
 * `loss` is the Object's loss report when its track tracks Locations
 * (MPEG2TS), else NULL. `deliver` (owned, may be NULL) replaces the payload
 * the app receives -- the Object is still parsed from its own payload.
 * `delivery_origin` and `initiating_request_id` name the source of the whole
 * object: LIVE, or a FILL child and its initiating request. `dom` is the
 * fill record of a fill Object of an MPEG2TS track, else NULL. */
/* SHARED_EPOCH: report the queued object's times against the receiver-wide
 * anchor, saturating at zero for a time before it; payload bytes are never
 * modified. RAW leaves them as parsed. Applied once, to the final record. */
static void receiver_rebase_times(const moq_media_receiver_t *r,
                                  moq_media_object_t *o)
{
    if (r->time_mode != MOQ_MEDIA_TIME_SHARED_EPOCH || !r->epoch_set) return;
    uint64_t e = r->epoch_us;
    o->decode_time_us = o->decode_time_us > e ? o->decode_time_us - e : 0;
    o->presentation_time_us =
        o->presentation_time_us > e ? o->presentation_time_us - e : 0;
    if (o->has_capture_time)
        o->capture_time_us = o->capture_time_us > e ? o->capture_time_us - e : 0;
}

static void receiver_deliver_object(moq_media_receiver_t *r,
                                    moq_media_track_t *track,
                                    moq_sub_object_t *so,
                                    const receiver_loss_t *loss,
                                    moq_rcbuf_t *deliver,
                                    uint32_t delivery_origin,
                                    uint64_t initiating_request_id,
                                    const receiver_dom_ref_t *dom,
                                    const receiver_join_t *join)
{
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    if (moq_media_object_input_from_sub_object(so, &in) != MOQ_OK) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
        pthread_mutex_lock(&r->mu);
        receiver_loss_dropped_locked(r, track, so, loss, dom);
        pthread_mutex_unlock(&r->mu);
        if (deliver) moq_rcbuf_decref(deliver);
        moq_sub_object_cleanup(so);
        return;
    }

    moq_cmaf_sample_t stack_samples[RECEIVER_PARSE_STACK_SAMPLES];
    moq_cmaf_sample_t *samples = stack_samples;
    size_t sample_cap = RECEIVER_PARSE_STACK_SAMPLES;
    moq_cmaf_sample_t *heap_samples = NULL;

    /* The selected facade generation is borrowed only on this executor. The
     * first parse determines admission/keyframe policy; none of its inherited
     * spans may cross the queue boundary until reparsed from a private copy. */
    moq_bytes_t track_properties = {0};
    if (receiver_companion_d22(r->companion_profile)) {
        const moq_sub_track_t *generation = so->track ? so->track : track->sub_track;
        if (generation) {
            moq_sub_track_result_t result;
            if (moq_sub_get_track_result(r->sub, generation, &result,
                                         sizeof(result)) != MOQ_OK) {
                receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
                pthread_mutex_lock(&r->mu);
                receiver_loss_dropped_locked(r, track, so, loss, dom);
                pthread_mutex_unlock(&r->mu);
                if (deliver) moq_rcbuf_decref(deliver);
                moq_sub_object_cleanup(so);
                return;
            }
            track_properties = result.track_properties;
        }
    }

    moq_media_parsed_object_t parsed = {0};
    moq_media_parsed_object_init(&parsed);
    moq_media_drop_reason_t why;
    moq_result_t rc = receiver_parse_media_object(r, track, &in, track_properties,
                                                 samples, sample_cap,
                                                 &parsed, &why);
    if (rc == MOQ_ERR_BUFFER && parsed.sample_count > sample_cap) {
        sample_cap = parsed.sample_count;
        heap_samples = (moq_cmaf_sample_t *)r->alloc.alloc(
            sample_cap * sizeof(moq_cmaf_sample_t), r->alloc.ctx);
        if (heap_samples) {
            samples = heap_samples;
            rc = receiver_parse_media_object(r, track, &in, track_properties, samples,
                                             sample_cap, &parsed, &why);
        }
    }
    if (rc != MOQ_OK) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
        pthread_mutex_lock(&r->mu);
        receiver_loss_dropped_locked(r, track, so, loss, dom);
        pthread_mutex_unlock(&r->mu);
        if (heap_samples)
            r->alloc.free(heap_samples,
                          sample_cap * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        if (deliver) moq_rcbuf_decref(deliver);
        moq_sub_object_cleanup(so);
        return;
    }

    /* The fragment must belong to the track the handle's init describes. A
     * stale Group of another configuration is caught here when its track_ID
     * differs; one with the same track_ID is not (see media_receiver.h). */
    if (rc == MOQ_OK && track->cmaf_track_id_check &&
        parsed.status == MOQ_OBJECT_NORMAL && parsed.fragment.len > 0) {
        moq_cmaf_object_report_t report;
        moq_cmaf_object_report_init(&report);
        (void)moq_cmaf_validate_object(track->desc.cmaf, parsed.fragment, &report);
        if (report.track_id != 0 && report.track_id != track->desc.cmaf->track_id)
            rc = MOQ_ERR_PROTO;
    }
    if (rc != MOQ_OK) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
        pthread_mutex_lock(&r->mu);
        receiver_loss_dropped_locked(r, track, so, loss, dom);
        pthread_mutex_unlock(&r->mu);
        if (heap_samples)
            r->alloc.free(heap_samples,
                          sample_cap * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        if (deliver) moq_rcbuf_decref(deliver);
        moq_sub_object_cleanup(so);
        return;
    }

    /* A terminal-status object is track-event data, never an object. */
    if (parsed.status == MOQ_OBJECT_END_OF_TRACK) {
        st_end_of_track(r, track, so->group_id, so->object_id);
        receiver_mark_track_ended(r, track);
        if (heap_samples)
            r->alloc.free(heap_samples,
                          sample_cap * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        if (deliver) moq_rcbuf_decref(deliver);
        moq_sub_object_cleanup(so);
        return;
    }

    /* SHARED_EPOCH: the receiver-wide anchor is the first parsed object's
     * decode time, latched here whether or not the object is then queued.
     * The rebase itself is applied once, to the record that is queued
     * (receiver_rebase_times): a reparse in the reservation window below
     * produces a fresh, raw record. */
    if (r->time_mode == MOQ_MEDIA_TIME_SHARED_EPOCH && !r->epoch_set) {
        r->epoch_set = true;
        r->epoch_us = parsed.decode_time_us;
    }

    /* Per-object owned sample table (the parse wrote into stack/heap
     * scratch; queue entries must own theirs). */
    moq_cmaf_sample_t *owned = NULL;
    if (parsed.sample_count > 0) {
        if (heap_samples && samples == heap_samples) {
            owned = heap_samples;            /* take it over as-is */
            heap_samples = NULL;
        } else {
            owned = (moq_cmaf_sample_t *)r->alloc.alloc(
                parsed.sample_count * sizeof(moq_cmaf_sample_t),
                r->alloc.ctx);
            if (!owned) {
                receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
                pthread_mutex_lock(&r->mu);
                receiver_loss_dropped_locked(r, track, so, loss, dom);
                pthread_mutex_unlock(&r->mu);
                if (deliver) moq_rcbuf_decref(deliver);
                moq_sub_object_cleanup(so);
                return;
            }
            memcpy(owned, samples,
                   parsed.sample_count * sizeof(moq_cmaf_sample_t));
        }
    }
    if (heap_samples)
        r->alloc.free(heap_samples, sample_cap * sizeof(moq_cmaf_sample_t),
                      r->alloc.ctx);

    moq_rcbuf_t *queued = deliver ? deliver : so->payload;
    size_t need = queued ? moq_rcbuf_len(queued) : 0;
    uint64_t budget = 0;
    bool budget_valid = track_properties.len <= UINT64_MAX - (uint64_t)need;
    if (budget_valid) budget = (uint64_t)need + track_properties.len;

    HANDOFF_HOOK(0);
    pthread_mutex_lock(&r->mu);
    /* Every successfully parsed object is "received" (the stat counts parsed
     * objects whether they end up queued or dropped); the drop counters below
     * are then a subset. */
    r->stats.objects_received++;
    /* Disable check under the same lock the unsubscribe purge uses: a non-
     * terminal object for a track disabled (or disabled while this object was
     * in flight) must not land in the queue after the purge ran. Terminal
     * END_OF_TRACK was already handled above, so a disabled track still ends.
     * A track that failed locally admits nothing further unless its failure
     * preserves delivery. A straggler of evicted content is dropped even when
     * there is room; otherwise the overflow policy makes room and re-checks
     * the incoming object against the filter the eviction may have armed. */
    bool wanted = track->desired_subscribed && !track->auth_failed &&
                  (track->local_result == MOQ_OK || track->delivery_failed);
    /* What the overflow policy drops here, the incoming Object included, is
     * safety loss recorded on the stream-terminal lane. */
    r->st_collect = wanted;
    r->st_admit_from = r->st_tail;
    bool keep = wanted && budget_valid &&
                !obj_filtered_locked(r, track, so->group_id,
                                     parsed.keyframe) &&
                obj_make_room_locked(r, track, so->group_id,
                                     parsed.keyframe, budget);
    /* Its own drop, if any, takes the slot the admission reserved. */
    st_adm_release_locked(r);
    if (wanted && !keep)
        st_note_drop_locked(r, track, dom ? (uint8_t)(dom->idx + 1) : 0,
                            delivery_origin, initiating_request_id,
                            so->group_id);
    r->st_collect = false;
    if (!keep) {
        r->stats.objects_dropped++;
        if (parsed.keyframe) r->stats.keyframes_dropped++;
        if (parsed.status == MOQ_OBJECT_NORMAL)
            receiver_loss_dropped_locked(r, track, so, loss, dom);
        pthread_mutex_unlock(&r->mu);
        if (owned)
            r->alloc.free(owned,
                          parsed.sample_count * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        if (deliver) moq_rcbuf_decref(deliver);
        moq_sub_object_cleanup(so);
        return;
    }

    /* Admission bounds this checked addition. Reserve before allocating, with
     * the single network producer owning the charge while polls/purges may
     * remove only committed entries. Only allocation and bounded parsing
     * intervene before commit/rollback; no session callbacks or waits. */
    r->obj_budget_total += budget;
    moq_rcbuf_t *track_copy = NULL;
    /* What crosses to the application besides the track properties: the
     * replacement payload when there is one (created here, so transferable;
     * the facade's own payload is then released here after publication), else
     * the facade's payload; and the properties. Each that cannot be shown
     * transferable is copied in the same reserved window, and the object is
     * reparsed from what is queued. */
    bool iso_payload = !deliver && !receiver_transferable(r, so->payload);
    bool iso_props = !receiver_transferable(r, so->properties);
    if (track_properties.len || iso_payload || iso_props) {
        uint64_t app_gen = track->app_gen;
        size_t owned_count = parsed.sample_count;
        pthread_mutex_unlock(&r->mu);
        rc = MOQ_OK;
        if (iso_payload) rc = receiver_isolate(r, &so->payload);
        if (rc == MOQ_OK && iso_props) rc = receiver_isolate(r, &so->properties);
        if (rc == MOQ_OK && track_properties.len)
            rc = moq_rcbuf_create(&r->alloc, track_properties.data,
                                  track_properties.len, &track_copy);
        if (rc == MOQ_OK && (iso_payload || iso_props))
            rc = moq_media_object_input_from_sub_object(so, &in);
        if (rc == MOQ_OK) {
            moq_media_parsed_object_t copied;
            moq_bytes_t bytes = { NULL, 0 };
            if (track_copy)
                bytes = (moq_bytes_t){ moq_rcbuf_data(track_copy), moq_rcbuf_len(track_copy) };
            rc = receiver_parse_media_object(r, track, &in, bytes,
                owned ? owned : stack_samples, owned ? owned_count : RECEIVER_PARSE_STACK_SAMPLES,
                &copied, &why);
            if (rc == MOQ_OK) parsed = copied;
        }
        HANDOFF_HOOK(1);
        pthread_mutex_lock(&r->mu);
        if (rc != MOQ_OK || !track->desired_subscribed || track->app_gen != app_gen) {
            obj_uncharge_locked(r, budget);
            r->stats.objects_dropped++;
            if (parsed.keyframe) r->stats.keyframes_dropped++;
            if (parsed.status == MOQ_OBJECT_NORMAL)
                receiver_loss_dropped_locked(r, track, so, loss, dom);
            bool notify = rc != MOQ_OK && receiver_note_parse_drop_locked(
                r, track, MOQ_MEDIA_PARSE_DROP_MEDIA);
            pthread_mutex_unlock(&r->mu);
            if (track_copy) moq_rcbuf_decref(track_copy);
            if (owned) r->alloc.free(owned, owned_count * sizeof(*owned), r->alloc.ctx);
            if (deliver) moq_rcbuf_decref(deliver);
            moq_sub_object_cleanup(so);
            if (notify) moq_endpoint_wake(r->ep);
            return;
        }
    }

    receiver_obj_entry_t *e = &r->objs[r->obj_tail % r->ring_cap];
    memset(e, 0, sizeof(*e));
    e->group_id = so->group_id;
    e->budget = budget;
    moq_media_object_t *o = &e->obj;
    o->track = track;
    o->config_generation = 0;
    o->packaging = parsed.packaging;
    o->status = parsed.status;
    o->end_of_group = parsed.end_of_group;
    o->datagram = parsed.datagram;
    o->keyframe = parsed.keyframe;
    o->has_capture_time = parsed.has_capture_time;
    o->capture_time_us = parsed.capture_time_us;
    o->decode_time_us = parsed.decode_time_us;
    o->composition_offset_us = parsed.composition_offset_us;
    o->presentation_time_us = parsed.presentation_time_us;
    receiver_rebase_times(r, o);
    o->payload = parsed.payload;
    o->fragment = parsed.fragment;
    o->mdat_offset = parsed.mdat_offset;
    o->mdat_len = parsed.mdat_len;
    o->sample_count = parsed.sample_count;
    o->samples = owned;
    o->samples_owned = owned;
    /* The wire identity, as the facade delivered it: a datagram has no
     * subgroup on the wire, so its subgroup_id is zero and has_subgroup false. */
    o->group_id = so->group_id;
    o->subgroup_id = so->datagram ? 0 : so->subgroup_id;
    o->object_id = so->object_id;
    o->publisher_priority = so->publisher_priority;
    o->has_subgroup = !so->datagram;
    /* The loss report of a Location-tracked Object, with any local loss
     * recorded since the track's previous queued Object merged in. */
    e->gen = track->ts_gen;
    if (loss && parsed.status == MOQ_OBJECT_NORMAL) {
        o->has_loss_report = true;
        o->gap = loss->gap;
        o->gap_group = loss->gap_group;
        o->gap_object = loss->gap_object;
        o->gap_tail_unknown = loss->gap_tail_unknown;
        o->gap_duration_known = loss->gap_duration_known;
        o->gap_duration_27mhz = loss->gap_duration_27mhz;
        o->discontinuous = loss->discontinuous;
        if (track->ts_loss_pending && track->ts_loss_gen == track->ts_gen)
            loss_merge(o, track->ts_loss_group, track->ts_loss_object);
        track->ts_loss_pending = false;
    }
    /* A fill Object reports the fill's loss not yet reported, and its record
     * counts it until it leaves the queue. */
    if (join) {
        e->join_candidate = true;
        e->join_psi_len = join->psi_len;
    }
    if (dom) {
        receiver_fill_dom_t *d = &r->fill_doms[dom->idx];
        e->gen = d->gen;
        e->dom = (uint8_t)(dom->idx + 1);
        e->pos = dom->pos;
        if (d->ts) {
            o->has_loss_report = true;
            if (d->pending.causes) {
                e->carry = d->pending;
                memset(&d->pending, 0, sizeof(d->pending));
                dom_apply_carry(e);
            }
        }
        if (r->st_enabled && d->queued == 0) r->domains_queued++;
        d->queued++;
    }
    o->delivery_origin = delivery_origin;
    o->initiating_request_id = delivery_origin != MOQ_MEDIA_DELIVERY_LIVE
        ? initiating_request_id : 0;
    if (receiver_companion_d22(r->companion_profile)) {
        o->metadata = parsed.metadata;
        /* Zero on LOC-04; on LOC-05 the effective Codec String, borrowing
         * properties_ref or this object's track_properties_ref. */
        o->loc_ext_present = parsed.loc_ext_present;
        o->codec_string = parsed.codec_string;
    }
    /* Steal the facade object's refs (no moq_sub_object_cleanup). A
     * replacement payload is what the app receives; the facade's own payload
     * reference is then released. */
    if (deliver) {
        o->payload = (moq_bytes_t){ moq_rcbuf_data(deliver),
                                    moq_rcbuf_len(deliver) };
        o->payload_ref = deliver;
    } else {
        o->payload_ref = so->payload;
    }
    o->properties_ref = so->properties;
    o->track_properties_ref = track_copy;
    e->seq = r->intake_seq;
    if (r->st_enabled && !e->dom && track->st_queued++ == 0)
        r->domains_queued++;
    r->obj_sizes_total += need;
    r->obj_tail++;
    pthread_mutex_unlock(&r->mu);
    if (deliver && so->payload) moq_rcbuf_decref(so->payload);
}

/* A non-Location-tracked media object: drop it unparsed while its track is
 * disabled or has failed locally, else parse and enqueue it (see
 * receiver_deliver_object). */
static void receiver_on_media_object_with_origin(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, uint32_t delivery_origin,
    uint64_t initiating_request_id, uint64_t now_us)
{
    (void)now_us;

    /* A track disabled via unsubscribe keeps its facade subscription alive
     * (delivery is paused with forward=false rather than torn down). Objects
     * already in flight when the pause took effect must not surface -- drop them
     * here without the cost of parsing. The one exception is a terminal
     * END_OF_TRACK status: a track the publisher ended must still surface
     * TRACK_ENDED even while disabled, so let it through to the parse + terminal
     * branch below (so->status carries the wire status before parse). A late
     * non-terminal straggler that slips past this is still dropped at queue
     * admission. */
    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    if (track->auth_failed ||
        (track->local_result != MOQ_OK && !track->delivery_failed) ||
        (!wanted && so->status != MOQ_OBJECT_END_OF_TRACK)) {
        /* Count it as received-then-dropped so objects_dropped stays a subset of
         * objects_received (we skip the parse only as an optimization). */
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }
    pthread_mutex_unlock(&r->mu);
    receiver_deliver_object(r, track, so, NULL, NULL, delivery_origin,
                            initiating_request_id, NULL, NULL);
}

static void receiver_on_media_object(moq_media_receiver_t *r,
                                     moq_media_track_t *track,
                                     moq_sub_object_t *so, uint64_t now_us)
{
    receiver_on_media_object_with_origin(r, track, so, MOQ_MEDIA_DELIVERY_LIVE, 0, now_us);
}

/* A fill Object of an MPEG2TS track, in the fill stream's transport order:
 * it takes the next delivery position of its fill record, gets the payload
 * transform a LIVE Object gets, and a drop on the way is a local drop of
 * that fill. */
/* A fill Object of a RAW or CMAF track with its fill record (the
 * stream-terminal lane): the record keys its loss and holds the fill's
 * terminal until its queued Objects have left. A standalone fetch's Object
 * of such a track takes the same path with its fetch record. */
static void receiver_on_fill_media_object(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, size_t dom,
    uint64_t initiating_request_id)
{
    pthread_mutex_lock(&r->mu);
    receiver_fill_dom_t *d = &r->fill_doms[dom];
    receiver_dom_ref_t ref = { dom, d->next_pos++ };
    uint32_t origin = d->fetch ? MOQ_MEDIA_DELIVERY_FETCH : MOQ_MEDIA_DELIVERY_FILL;
    if ((track->local_result != MOQ_OK && !track->delivery_failed) ||
        !track->desired_subscribed) {
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }
    pthread_mutex_unlock(&r->mu);
    receiver_deliver_object(r, track, so, NULL, NULL, origin,
                            initiating_request_id, &ref, NULL);
}

static void receiver_on_fill_ts_object(moq_media_receiver_t *r,
    moq_media_track_t *track, moq_sub_object_t *so, size_t dom,
    uint64_t initiating_request_id)
{
    pthread_mutex_lock(&r->mu);
    receiver_fill_dom_t *d = &r->fill_doms[dom];
    receiver_dom_ref_t ref = { dom, d->next_pos++ };
    uint32_t origin = d->fetch ? MOQ_MEDIA_DELIVERY_FETCH : MOQ_MEDIA_DELIVERY_FILL;
    d->has_prev = true;
    d->prev_group = so->group_id;
    d->prev_object = so->object_id;
    if (track->auth_failed ||
        (track->local_result != MOQ_OK && !track->delivery_failed) ||
        !track->desired_subscribed) {
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        receiver_loss_dropped_locked(r, track, so, NULL, &ref);
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }
    /* A standalone fetch is a gap fill, not a join: it takes no join PSI. */
    bool join = !d->fetch && ts_join_wanted_locked(r, track, d->gen);
    pthread_mutex_unlock(&r->mu);
    moq_rcbuf_t *deliver = NULL;
    receiver_join_t jr = { 0 };
    if (receiver_fill_ts_payload(r, track, join, so->payload, &deliver,
                                 &jr.psi_len) != MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        receiver_loss_dropped_locked(r, track, so, NULL, &ref);
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }
    receiver_deliver_object(r, track, so, NULL, deliver, origin,
                            initiating_request_id, &ref, join ? &jr : NULL);
}

/* -- Held items and their lane slots (network thread) ---------------------- *
 * What a reorder window or the in-group ordering stage holds keeps the lane
 * slot its intake reserved, and is admitted later under its own ingress
 * sequence. */

/* The intake's reserved slot passes to what the window now holds. */
static void ts_keep_slot(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    if (r->st_enabled) {
        assert(r->st_adm);
        r->st_adm = false;
    }
    pthread_mutex_unlock(&r->mu);
}

/* What the window held leaves it with no record: its slot returns. */
static void ts_drop_slot(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    if (r->st_enabled) {
        assert(r->st_reserved > 0);
        r->st_reserved--;
    }
    pthread_mutex_unlock(&r->mu);
}

/* An admission on a held item's behalf: it takes the item's reserved slot
 * and the ingress sequence the item was taken in with. receiver_ts_adm_end
 * restores the intake in progress, on every exit. */
typedef struct { bool adm; uint64_t seq; } receiver_ts_adm_t;

static receiver_ts_adm_t receiver_ts_adm_begin(moq_media_receiver_t *r,
                                               uint64_t seq)
{
    pthread_mutex_lock(&r->mu);
    receiver_ts_adm_t saved = { r->st_adm, r->intake_seq };
    r->st_adm = r->st_enabled;
    r->intake_seq = seq;
    pthread_mutex_unlock(&r->mu);
    return saved;
}

static void receiver_ts_adm_end(moq_media_receiver_t *r, receiver_ts_adm_t saved)
{
    pthread_mutex_lock(&r->mu);
    st_adm_release_locked(r);
    r->st_adm = saved.adm;
    r->intake_seq = saved.seq;
    pthread_mutex_unlock(&r->mu);
}

/* The byte charge an Object of `t` with `payload_len` octets takes in the
 * queue (receiver_deliver_object's budget). */
static uint64_t receiver_charge(moq_media_receiver_t *r, moq_media_track_t *t,
                                size_t payload_len)
{
    uint64_t need = (uint64_t)payload_len;
    if (receiver_companion_d22(r->companion_profile) &&
        t->sub_track) {
        moq_sub_track_result_t result;
        if (moq_sub_get_track_result(r->sub, t->sub_track, &result,
                                     sizeof(result)) == MOQ_OK &&
            result.track_properties.len <= UINT64_MAX - need)
            need += result.track_properties.len;
    }
    return need;
}

/* Whether a held item charging `need` may go to the queue now: one that
 * fits the queue as it is destroys nothing and needs only its own reserved
 * slot; otherwise the overflow policy may drop from every delivery domain
 * with queued entries, and each needs room for its RECEIVER_DROP. */
static bool receiver_release_room(moq_media_receiver_t *r, uint64_t need)
{
    if (!r->st_enabled) return true;
    pthread_mutex_lock(&r->mu);
    bool fits = r->obj_tail - r->obj_head < r->obj_cap &&
                obj_bytes_fit(r, need, r->byte_cap);
    bool ok = fits || st_unreserved_locked(r) >= r->domains_queued;
    pthread_mutex_unlock(&r->mu);
    return ok;
}

/* -- MPEG2TS intake (network thread) -------------------------------------- *
 * Each subscribed TS track runs a <moq/mpeg2ts.h> receiver: Objects are
 * offered to its reorder window as they arrive and released in Location
 * order, each with the loss report the window computed, into the common
 * queue. The window holds the facade's payload reference (in a held record)
 * until release. Group ends come from the facade's FIN surface and from
 * END_OF_GROUP status Objects; END_OF_TRACK and the subscription's end
 * finish the window, so no held Object is lost behind TRACK_ENDED. */

/* Whether the track has a join PSI to give (mpeg2ts_join_psi and PSI init
 * data of whole packets). */
static bool ts_join_psi_usable(const moq_media_receiver_t *r,
                               const moq_media_track_t *t)
{
    uint32_t ps = receiver_ts_packet_size(&t->desc);
    return r->ts_join_psi && t->desc.init_data.len > 0 &&
           t->desc.init_data.len % ps == 0;
}

/* Whether an Object of subscription generation `gen` is prepared as a
 * candidate for the join PSI: the generation is current and its PSI has not
 * gone out yet. Caller holds r->mu. */
static bool ts_join_wanted_locked(const moq_media_receiver_t *r,
                                  const moq_media_track_t *t, uint32_t gen)
{
    return ts_join_psi_usable(r, t) && gen == t->ts_gen && !t->ts_join_delivered;
}

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
typedef struct receiver_ts {
    moq_mpeg2ts_receiver_t      rx;
    moq_mpeg2ts_reorder_slot_t *slots;
    size_t                      slot_count;
    /* A delivery the window released that waits for lane room; a status
     * Object that goes out once what was due at its arrival has; an end
     * that waits for the finished window to empty. Each keeps the slot its
     * intake reserved. */
    bool                        has_parked;
    bool                        stalled;
    moq_mpeg2ts_delivery_t      parked;
    bool                        has_status;
    moq_sub_object_t            status;
    uint64_t                    status_seq, status_us;
    bool                        end_pending;
    uint64_t                    end_group, end_object;
    bool                        released_any;   /* the window released an Object */
} receiver_ts_t;

/* The window ctx of a track's END_OF_TRACK (an address no holder has). */
static const char receiver_ts_end_mark;
#define RECEIVER_TS_END_MARK ((void *)(uintptr_t)&receiver_ts_end_mark)

/* What a held Object needs besides its Location and payload. */
typedef struct receiver_ts_held {
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;
    uint64_t     seq;          /* its ingress sequence */
    uint64_t     subgroup_id;
    uint8_t      priority;
    bool         datagram;
    bool         end_of_group;
} receiver_ts_held_t;

static void ts_held_free(moq_media_receiver_t *r, receiver_ts_held_t *h)
{
    if (!h || (void *)h == RECEIVER_TS_END_MARK) return;
    if (h->payload) moq_rcbuf_decref(h->payload);
    if (h->properties) moq_rcbuf_decref(h->properties);
    r->alloc.free(h, sizeof(*h), r->alloc.ctx);
}

/* The payload the app receives for an Object of a TS track, LIVE or fill:
 * with `join`, the catalog's PSI ahead of the Object, its continuity counters
 * aligned to it (*psi_len receives the PSI's octets in the result, zero when
 * none was prepended); and the 192-octet prefixes removed
 * (mpeg2ts_strip_prefix). NULL with MOQ_OK when nothing changes. The
 * received buffer is never modified, and no join state changes here: the
 * PSI goes out only with an Object the application polls. */
static moq_result_t receiver_ts_payload(moq_media_receiver_t *r,
                                        moq_media_track_t *t,
                                        bool join,
                                        const moq_rcbuf_t *payload,
                                        moq_rcbuf_t **out, uint32_t *psi_len)
{
    *out = NULL;
    *psi_len = 0;
    uint32_t ps = receiver_ts_packet_size(&t->desc);
    moq_bytes_t obj = { moq_rcbuf_data(payload), moq_rcbuf_len(payload) };
    moq_bytes_t psi = { NULL, 0 };
    if (join && ts_join_psi_usable(r, t))
        psi = t->desc.init_data;
    bool strip = r->ts_strip_prefix && ps == MOQ_MPEG2TS_M2TS_PACKET_SIZE;
    if (psi.len == 0 && !strip) return MOQ_OK;

    if (psi.len > SIZE_MAX - obj.len) return MOQ_ERR_NOMEM;
    size_t len = psi.len + obj.len;
    uint8_t *buf = (uint8_t *)r->alloc.alloc(len, r->alloc.ctx);
    if (!buf) return MOQ_ERR_NOMEM;
    if (psi.len > 0) memcpy(buf, psi.data, psi.len);
    if (obj.len > 0) memcpy(buf + psi.len, obj.data, obj.len);
    size_t psi_in = psi.len;
    if (psi.len > 0 &&
        moq_mpeg2ts_align_join_psi(buf, psi.len, ps, obj) != MOQ_OK) {
        /* PSI the Object cannot follow is not prepended. */
        memmove(buf, buf + psi.len, obj.len);
        len = obj.len;
        psi_in = 0;
    }
    size_t out_len = len;
    if (strip &&
        moq_mpeg2ts_strip_prefix((moq_bytes_t){ buf, len }, buf, len,
                                 &out_len) != MOQ_OK) {
        r->alloc.free(buf, psi.len + obj.len, r->alloc.ctx);
        return MOQ_ERR_PROTO;
    }
    moq_result_t rc = moq_rcbuf_create(&r->alloc, buf, out_len, out);
    r->alloc.free(buf, psi.len + obj.len, r->alloc.ctx);
    if (rc == MOQ_OK)
        *psi_len = (uint32_t)(strip ? psi_in / ps * MOQ_MPEG2TS_TS_PACKET_SIZE : psi_in);
    return rc;
}

/* Hand one released Object to the common queue, or drop a rejected one.
 * False when it needs lane room that is not there yet: nothing changed. */
static bool receiver_ts_release(moq_media_receiver_t *r, moq_media_track_t *t,
                                const moq_mpeg2ts_delivery_t *d)
{
    if (d->ctx == RECEIVER_TS_END_MARK) {
        /* The track's END_OF_TRACK, released in Location order. */
        receiver_ts_adm_t saved = receiver_ts_adm_begin(r, r->ingress_seq);
        st_end_of_track(r, t, d->group, d->object);
        receiver_ts_adm_end(r, saved);
        receiver_mark_track_ended(r, t);
        return true;
    }
    receiver_ts_held_t *h = (receiver_ts_held_t *)d->ctx;
    if (!h) {
        /* The placeholder of an Object lost at intake (counted there): the
         * window has opened its loss run at this Location. A gap it reports
         * passes to the track's next queued Object. */
        if (!d->result.accepted && d->result.gap) {
            pthread_mutex_lock(&r->mu);
            ts_note_loss_locked(r, t, t->ts_gen, d->result.gap_group,
                                d->result.gap_object, r->obj_tail);
            pthread_mutex_unlock(&r->mu);
        }
        return true;
    }
    if (!d->result.accepted) {
        /* Object Payload Format: a subscriber MUST reject it. The window
         * reports a gap before it on its own result, which never reaches the
         * app: that gap, and the Object, go with the track's next queued
         * Object. */
        pthread_mutex_lock(&r->mu);
        if (d->result.gap)
            ts_note_loss_locked(r, t, t->ts_gen, d->result.gap_group,
                                d->result.gap_object, r->obj_tail);
        pthread_mutex_unlock(&r->mu);
        receiver_note_parse_drop(r, t, MOQ_MEDIA_PARSE_DROP_MEDIA);
        ts_held_free(r, h);
        ts_drop_slot(r);
        return true;
    }
    receiver_loss_t loss;
    loss.gap = d->result.gap;
    loss.gap_group = d->result.gap_group;
    loss.gap_object = d->result.gap_object;
    loss.gap_tail_unknown = d->result.gap_tail_unknown;
    loss.gap_duration_known = d->result.gap_duration_known;
    loss.gap_duration_27mhz = d->result.gap_duration_27mhz;
    loss.discontinuous = d->result.discontinuous;

    pthread_mutex_lock(&r->mu);
    bool join = ts_join_wanted_locked(r, t, t->ts_gen);
    pthread_mutex_unlock(&r->mu);
    moq_rcbuf_t *deliver = NULL;
    receiver_join_t jr = { 0 };
    moq_result_t prc = receiver_ts_payload(r, t, join, h->payload, &deliver,
                                           &jr.psi_len);
    if (prc == MOQ_OK) {
        const moq_rcbuf_t *q = deliver ? deliver : h->payload;
        if (!receiver_release_room(r, receiver_charge(r, t, moq_rcbuf_len(q)))) {
            if (deliver) moq_rcbuf_decref(deliver);
            return false;
        }
    }

    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = t->sub_track;
    so.group_id = d->group;
    so.subgroup_id = h->subgroup_id;
    so.object_id = d->object;
    so.publisher_priority = h->priority;
    so.status = MOQ_OBJECT_NORMAL;
    so.end_of_group = h->end_of_group;
    so.datagram = h->datagram;
    so.payload = h->payload;
    so.properties = h->properties;
    uint64_t seq = h->seq;
    r->alloc.free(h, sizeof(*h), r->alloc.ctx);

    receiver_ts_adm_t saved = receiver_ts_adm_begin(r, seq);
    if (prc != MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        receiver_loss_dropped_locked(r, t, &so, &loss, NULL);
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(&so);
    } else {
        receiver_deliver_object(r, t, &so, &loss, deliver,
                                MOQ_MEDIA_DELIVERY_LIVE, 0, NULL,
                                join ? &jr : NULL);
    }
    receiver_ts_adm_end(r, saved);
    return true;
}

/* A parked status Object goes to the queue under its own sequence. False
 * when it needs lane room that is not there yet. */
static bool receiver_ts_release_status(moq_media_receiver_t *r,
                                       moq_media_track_t *t, receiver_ts_t *ts)
{
    size_t len = ts->status.payload ? moq_rcbuf_len(ts->status.payload) : 0;
    if (!receiver_release_room(r, receiver_charge(r, t, len))) return false;
    moq_sub_object_t so = ts->status;
    ts->has_status = false;
    memset(&ts->status, 0, sizeof(ts->status));
    receiver_ts_adm_t saved = receiver_ts_adm_begin(r, ts->status_seq);
    receiver_deliver_object(r, t, &so, NULL, NULL, MOQ_MEDIA_DELIVERY_LIVE, 0,
                            NULL, NULL);
    receiver_ts_adm_end(r, saved);
    return true;
}

static void ts_set_stalled(moq_media_receiver_t *r, receiver_ts_t *ts,
                           bool stalled)
{
    pthread_mutex_lock(&r->mu);
    if (stalled != ts->stalled) {
        ts->stalled = stalled;
        if (stalled) r->ts_stalled++;
        else r->ts_stalled--;
    }
    if (stalled) {
        r->ts_wake = true;
        r->st_blocked = true;
    }
    pthread_mutex_unlock(&r->mu);
}

/* Release every Object the track's window has due at now_us, in Location
 * order, each with the room its admission needs; a parked status Object
 * after everything that was due when it arrived; then a pending end once
 * the finished window is empty. True when it stopped for room: what it
 * stopped at stays parked, and the application's next poll wakes the
 * network thread to resume. */
static bool receiver_ts_drain(moq_media_receiver_t *r, moq_media_track_t *t,
                              uint64_t now_us)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (!ts) return false;
    for (;;) {
        if (!ts->has_parked) {
            uint64_t at = ts->has_status && ts->status_us < now_us
                ? ts->status_us : now_us;
            if (moq_mpeg2ts_receiver_poll(&ts->rx, at, &ts->parked,
                                          sizeof(ts->parked)) != MOQ_OK) {
                if (!ts->has_status) break;
                if (!receiver_ts_release_status(r, t, ts)) {
                    ts_set_stalled(r, ts, true);
                    return true;
                }
                continue;
            }
            ts->has_parked = true;
        }
        if (!receiver_ts_release(r, t, &ts->parked)) {
            ts_set_stalled(r, ts, true);
            return true;
        }
        ts->has_parked = false;
        ts->released_any = true;
        t->ts_rejoin = false;      /* the window has started */
    }
    ts_set_stalled(r, ts, false);
    if (ts->end_pending) {
        ts->end_pending = false;
        receiver_ts_adm_t saved = receiver_ts_adm_begin(r, r->ingress_seq);
        st_end_of_track(r, t, ts->end_group, ts->end_object);
        receiver_ts_adm_end(r, saved);
        receiver_mark_track_ended(r, t);
    }
    return false;
}

/* Whether `t`'s window holds, or has parked, an Object taken in before
 * `barrier` (network thread). */
static bool ts_holds_before(const moq_media_track_t *t, uint64_t barrier)
{
    const receiver_ts_t *ts = (const receiver_ts_t *)t->ts;
    if (!ts) return false;
    if (ts->has_status && ts->status_seq < barrier) return true;
    if (ts->has_parked && ts->parked.ctx &&
        ts->parked.ctx != RECEIVER_TS_END_MARK &&
        ((const receiver_ts_held_t *)ts->parked.ctx)->seq < barrier)
        return true;
    for (size_t i = 0; i < ts->slot_count; i++) {
        const moq_mpeg2ts_reorder_slot_t *sl = &ts->slots[i];
        if (!sl->used || !sl->ctx || sl->ctx == RECEIVER_TS_END_MARK) continue;
        if (((const receiver_ts_held_t *)sl->ctx)->seq < barrier) return true;
    }
    return false;
}

/* The track's subscription is being issued: a fresh window for it. Returns
 * false on allocation failure (the subscribe is then retried later). */
static bool receiver_ts_start(moq_media_receiver_t *r, moq_media_track_t *t,
                              uint32_t program_number)
{
    receiver_ts_free(r, t);
    receiver_ts_t *ts = (receiver_ts_t *)r->alloc.alloc(sizeof(*ts),
                                                        r->alloc.ctx);
    if (!ts) return false;
    memset(ts, 0, sizeof(*ts));
    ts->slot_count = r->ts_slots;
    ts->slots = (moq_mpeg2ts_reorder_slot_t *)r->alloc.alloc(
        ts->slot_count * sizeof(moq_mpeg2ts_reorder_slot_t), r->alloc.ctx);
    if (!ts->slots) { r->alloc.free(ts, sizeof(*ts), r->alloc.ctx); return false; }
    memset(ts->slots, 0, ts->slot_count * sizeof(moq_mpeg2ts_reorder_slot_t));

    const moq_msf_mpeg2ts_t *rec =
        moq_msf_ext_mpeg2ts(t->desc.extensions, t->desc.extension_count);
    moq_mpeg2ts_receiver_cfg_t c;
    moq_mpeg2ts_receiver_cfg_init(&c);
    c.packet_size = receiver_ts_packet_size(&t->desc);
    if (rec && rec->has_mode && rec->mode == MOQ_MSF_MPEG2TS_MODE_ES_PACKETS &&
        rec->has_es_pid) {
        c.es_packets = true;
        c.es_pid = (uint16_t)rec->es_pid;
    }
    if (program_number == 0 && rec && rec->has_program_number)
        program_number = rec->program_number;
    c.program_number = (uint16_t)program_number;
    c.reorder_slots = ts->slots;
    c.reorder_slot_count = ts->slot_count;
    c.reorder_window_us = r->ts_window_us;
    if (moq_mpeg2ts_receiver_init(&ts->rx, sizeof(ts->rx), &c) != MOQ_OK) {
        r->alloc.free(ts->slots,
                      ts->slot_count * sizeof(moq_mpeg2ts_reorder_slot_t),
                      r->alloc.ctx);
        r->alloc.free(ts, sizeof(*ts), r->alloc.ctx);
        return false;
    }
    t->ts = ts;
    pthread_mutex_lock(&r->mu);
    t->ts_gen++;
    t->ts_join_delivered = false;
    pthread_mutex_unlock(&r->mu);
    return true;
}

/* The subscription ended: every held Object is released, in order. True
 * when that stopped for lane room; it resumes with the window's next
 * drain. */
static bool receiver_ts_finish(moq_media_receiver_t *r, moq_media_track_t *t,
                               uint64_t now_us)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (!ts) return false;
    moq_mpeg2ts_receiver_finish(&ts->rx);
    return receiver_ts_drain(r, t, now_us);
}

/* The track ends at (g, o) after everything its finished window holds:
 * now, or once a stopped drain completes, keeping the intake's slot for
 * the END_OF_TRACK. */
static void receiver_ts_end_now(moq_media_receiver_t *r, moq_media_track_t *t,
                                uint64_t g, uint64_t o, uint64_t now_us)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (receiver_ts_finish(r, t, now_us)) {
        assert(!ts->end_pending);
        ts->end_pending = true;
        ts->end_group = g;
        ts->end_object = o;
        ts_keep_slot(r);
        return;
    }
    st_end_of_track(r, t, g, o);
    receiver_mark_track_ended(r, t);
}

/* One arriving Object of a TS track. Objects are offered even while the app
 * has the track disabled, so the window's Location accounting stays exact
 * across a pause; what it releases then is dropped at queue admission and
 * reported as a gap on the next Object the app gets. */
static void receiver_on_ts_object(moq_media_receiver_t *r,
                                  moq_media_track_t *t,
                                  moq_sub_object_t *so, uint64_t now_us)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (!ts) { moq_sub_object_cleanup(so); return; }

    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        /* No Object exists at or after this Location, but earlier ones can
         * still arrive on other subgroup streams. The end takes its place in
         * the window, so the track ends when the window releases it: after
         * every earlier Location, or when the window gives up on them. */
        uint64_t g = so->group_id, o = so->object_id;
        moq_sub_object_cleanup(so);
        /* A repeat of the END_OF_TRACK the window already holds changes
         * nothing. An Object held at the same Location is one the
         * END_OF_TRACK declares non-existent (an Object known to exist may
         * become non-existent, drafts 16 and 18 section 2.1); the window
         * cannot place the end after it, so the track ends now, as for any
         * END_OF_TRACK the window cannot take. */
        for (size_t i = 0; i < ts->slot_count; i++) {
            const moq_mpeg2ts_reorder_slot_t *sl = &ts->slots[i];
            if (sl->used && sl->group == g && sl->object == o) {
                if (sl->ctx == RECEIVER_TS_END_MARK) return;
                receiver_ts_end_now(r, t, g, o, now_us);
                return;
            }
        }
        /* Release what is due first. The window releases its first held
         * Object whenever every slot is in use, so after this drain a slot
         * is free and the offer below cannot meet a full window
         * (MOQ_ERR_BUFFER). */
        receiver_ts_drain(r, t, now_us);
        moq_result_t rc = moq_mpeg2ts_receiver_offer(&ts->rx, now_us, g, o, 0,
                                                     (moq_bytes_t){ NULL, 0 },
                                                     RECEIVER_TS_END_MARK);
        if (rc == MOQ_OK) {
            ts_keep_slot(r);
            receiver_ts_drain(r, t, now_us);
            return;
        }
        /* MOQ_ERR_PROTO, the only refusal left: the window has already
         * passed this Location. After the track's own end (a repeat that
         * arrives later) nothing changes. Otherwise Objects at or after it
         * were already delivered and the END_OF_TRACK now declares them
         * non-existent; what was delivered cannot be taken back, so the
         * track ends now. */
        if (t->ended || ts->end_pending) return;
        receiver_ts_end_now(r, t, g, o, now_us);
        return;
    }
    if (so->status != MOQ_OBJECT_NORMAL) {
        /* An END_OF_GROUP status ends its Group just before its own Object
         * ID; the status Object is then surfaced as for any track. */
        if (so->status == MOQ_OBJECT_END_OF_GROUP)
            moq_mpeg2ts_receiver_end_group_at(&ts->rx, so->group_id,
                                              so->object_id);
        /* It goes out after what is due now, as its own admission. Intake
         * waits while a window waits, so no status is parked here yet. A
         * parked status may be released by a destroying thread, so its
         * buffers are made transferable first; one that cannot be is lost
         * as a parse drop. */
        assert(!ts->has_status);
        if (!receiver_own_for_handoff(r, &so->payload) ||
            !receiver_own_for_handoff(r, &so->properties)) {
            receiver_note_parse_drop(r, t, MOQ_MEDIA_PARSE_DROP_MEDIA);
            moq_sub_object_cleanup(so);
            receiver_ts_drain(r, t, now_us);
            return;
        }
        ts->has_status = true;
        ts->status = *so;
        pthread_mutex_lock(&r->mu);
        ts->status_seq = r->intake_seq;
        pthread_mutex_unlock(&r->mu);
        ts->status_us = now_us;
        ts_keep_slot(r);
        receiver_ts_drain(r, t, now_us);
        return;
    }

    /* Release what is due first, so the window always has a free slot. */
    receiver_ts_drain(r, t, now_us);

    receiver_ts_held_t *h = (receiver_ts_held_t *)r->alloc.alloc(
        sizeof(*h), r->alloc.ctx);
    /* The window keeps the holder, and spans into its payload, until the
     * Object is released or the receiver is destroyed, possibly on another
     * thread: its buffers are made transferable before the window sees them.
     * One that cannot be is lost exactly as when the holder is refused. */
    if (h && (!receiver_own_for_handoff(r, &so->payload) ||
              !receiver_own_for_handoff(r, &so->properties))) {
        r->alloc.free(h, sizeof(*h), r->alloc.ctx);
        h = NULL;
    }
    if (!h) {
        /* The Object is lost at its own Location: the window takes an
         * empty placeholder there (no holder, so ctx NULL), which it
         * rejects in Location order, so the gap is reported on the next
         * Object after it and never on an earlier one. */
        pthread_mutex_lock(&r->mu);
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        pthread_mutex_unlock(&r->mu);
        uint32_t lflags =
            so->end_of_group ? MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP : 0u;
        moq_sub_object_cleanup(so);
        (void)moq_mpeg2ts_receiver_offer(&ts->rx, now_us, so->group_id,
                                         so->object_id, lflags,
                                         (moq_bytes_t){ NULL, 0 }, NULL);
        receiver_ts_drain(r, t, now_us);
        return;
    }
    h->payload = so->payload;
    h->properties = so->properties;
    pthread_mutex_lock(&r->mu);
    h->seq = r->intake_seq;
    pthread_mutex_unlock(&r->mu);
    h->subgroup_id = so->datagram ? 0 : so->subgroup_id;
    h->priority = so->publisher_priority;
    h->datagram = so->datagram;
    h->end_of_group = so->end_of_group;
    moq_bytes_t pl = { so->payload ? moq_rcbuf_data(so->payload) : NULL,
                       so->payload ? moq_rcbuf_len(so->payload) : 0 };
    uint32_t flags = so->end_of_group ? MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP : 0u;
    if (t->ts_rejoin && !ts->released_any) flags |= MOQ_MPEG2TS_OBJ_JOINABLE;
    moq_result_t rc = moq_mpeg2ts_receiver_offer(&ts->rx, now_us,
                                                 so->group_id, so->object_id,
                                                 flags, pl, h);
    if (rc != MOQ_OK) {
        /* Late or duplicated: its Location was already released or is
         * held. */
        pthread_mutex_lock(&r->mu);
        r->stats.reorder_refused++;
        pthread_mutex_unlock(&r->mu);
        ts_held_free(r, h);
        return;
    }
    ts_keep_slot(r);
    receiver_ts_drain(r, t, now_us);
}

/* The facade surfaced the end of a Group (a FIN on its END_OF_GROUP
 * subgroup), after every Object of that subgroup. */
static void receiver_on_ts_group_end(moq_media_receiver_t *r,
                                     moq_media_track_t *t, uint64_t group,
                                     uint64_t now_us)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (!ts) return;
    moq_mpeg2ts_receiver_end_group(&ts->rx, group);
    receiver_ts_drain(r, t, now_us);
}

static uint64_t receiver_ts_deadline(const moq_media_track_t *t)
{
    const receiver_ts_t *ts = (const receiver_ts_t *)t->ts;
    return ts ? moq_mpeg2ts_receiver_next_deadline_us(&ts->rx) : UINT64_MAX;
}

static void receiver_ts_free(moq_media_receiver_t *r, moq_media_track_t *t)
{
    receiver_ts_t *ts = (receiver_ts_t *)t->ts;
    if (!ts) return;
    /* Release the held payloads without delivering them, with the slots
     * they reserved. */
    if (ts->has_parked && ts->parked.ctx) {
        ts_held_free(r, (receiver_ts_held_t *)ts->parked.ctx);
        ts_drop_slot(r);
    }
    if (ts->has_status) {
        moq_sub_object_cleanup(&ts->status);
        ts_drop_slot(r);
    }
    if (ts->end_pending) ts_drop_slot(r);
    ts_set_stalled(r, ts, false);
    moq_mpeg2ts_receiver_finish(&ts->rx);
    moq_mpeg2ts_delivery_t d;
    while (moq_mpeg2ts_receiver_poll(&ts->rx, UINT64_MAX, &d, sizeof(d)) == MOQ_OK) {
        if (!d.ctx) continue;
        ts_held_free(r, (receiver_ts_held_t *)d.ctx);
        ts_drop_slot(r);
    }
    r->alloc.free(ts->slots,
                  ts->slot_count * sizeof(moq_mpeg2ts_reorder_slot_t),
                  r->alloc.ctx);
    r->alloc.free(ts, sizeof(*ts), r->alloc.ctx);
    t->ts = NULL;
}

/* The payload the app receives for a fill Object of a TS track: the same
 * transform as a LIVE Object's, the join PSI prepared when `join` (the
 * caller decided it for the fill's generation). */
static moq_result_t receiver_fill_ts_payload(moq_media_receiver_t *r,
                                             moq_media_track_t *t, bool join,
                                             const moq_rcbuf_t *payload,
                                             moq_rcbuf_t **out, uint32_t *psi_len)
{
    if (!payload) { *out = NULL; *psi_len = 0; return MOQ_OK; }
    return receiver_ts_payload(r, t, join, payload, out, psi_len);
}
#else
/* A build without moq::mpeg2ts gives TS tracks no delivery path, so none of
 * these is ever reached with live state. */
static void receiver_on_ts_object(moq_media_receiver_t *r,
                                  moq_media_track_t *t,
                                  moq_sub_object_t *so, uint64_t now_us)
{
    (void)r; (void)t; (void)now_us;
    moq_sub_object_cleanup(so);
}
static void receiver_on_ts_group_end(moq_media_receiver_t *r,
                                     moq_media_track_t *t, uint64_t group,
                                     uint64_t now_us)
{
    (void)r; (void)t; (void)group; (void)now_us;
}
static bool receiver_ts_drain(moq_media_receiver_t *r, moq_media_track_t *t,
                              uint64_t now_us)
{
    (void)r; (void)t; (void)now_us;
    return false;
}
static bool receiver_ts_start(moq_media_receiver_t *r, moq_media_track_t *t,
                              uint32_t program_number)
{
    (void)r; (void)t; (void)program_number;
    return false;
}
static bool receiver_ts_finish(moq_media_receiver_t *r, moq_media_track_t *t,
                               uint64_t now_us)
{
    (void)r; (void)t; (void)now_us;
    return false;
}
static uint64_t receiver_ts_deadline(const moq_media_track_t *t)
{
    (void)t;
    return UINT64_MAX;
}
static void receiver_ts_free(moq_media_receiver_t *r, moq_media_track_t *t)
{
    (void)r; (void)t;
}
static moq_result_t receiver_fill_ts_payload(moq_media_receiver_t *r,
                                             moq_media_track_t *t, bool join,
                                             const moq_rcbuf_t *payload,
                                             moq_rcbuf_t **out, uint32_t *psi_len)
{
    (void)r; (void)t; (void)join; (void)payload;
    *out = NULL;
    *psi_len = 0;
    return MOQ_OK;
}
#endif

static bool track_is_ts(const moq_media_track_t *t)
{
    return t->desc.info.packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
}

/* -- In-group ordering stage (network thread) ------------------------------ *
 * An MSF-02 "loc" track whose streamMapping is "object", or a value this
 * receiver does not recognize, restores (Group, Object) order on its live
 * subscription (media_receiver_order.h); fills and Joining FETCHes are one
 * ordered stream each and bypass it. What the stage holds keeps the lane slot
 * its intake reserved, as an MPEG2TS window's Objects do; what it releases
 * goes to the common queue with its loss report (has_loss_report). Facts come
 * from the stream terminals the receiver always surfaces internally and from
 * END_OF_GROUP status Objects. The bounds are the MPEG2TS window's defaults:
 * the stage is a policy, so what it gives up is not proof the network lost
 * it. */
#define RECEIVER_LOC_ORDER_GROUPS    4u
#define RECEIVER_LOC_ORDER_SLOTS     64u
#define RECEIVER_LOC_ORDER_WINDOW_US 300000u

/* One Object the stage holds: the facade's record, its refs owned. */
typedef struct receiver_order_held {
    moq_sub_object_t so;
    uint64_t         seq;      /* its ingress sequence */
} receiver_order_held_t;

typedef struct receiver_ordering {
    receiver_order_t         *stage;
    /* A release that waits for lane room, and an END_OF_TRACK that waits for
     * the finished stage to empty, each keeping the slot its intake
     * reserved. */
    bool                      has_parked;
    receiver_order_delivery_t parked;
    bool                      stalled;
    bool                      end_pending;
    uint64_t                  end_group, end_object;
    /* The stage's totals already added to the receiver's stats. */
    uint64_t                  gaps_counted, forced_counted;
} receiver_ordering_t;

static void order_held_free(moq_media_receiver_t *r, receiver_order_held_t *h)
{
    if (!h) return;
    moq_sub_object_cleanup(&h->so);
    r->alloc.free(h, sizeof(*h), r->alloc.ctx);
}

/* The stage gives a held item up without releasing it: its slot returns. */
static void order_item_release(void *ctx, void *item)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    order_held_free(r, (receiver_order_held_t *)item);
    ts_drop_slot(r);
}

static void order_set_stalled(moq_media_receiver_t *r, receiver_ordering_t *o,
                              bool stalled)
{
    pthread_mutex_lock(&r->mu);
    if (stalled != o->stalled) {
        o->stalled = stalled;
        if (stalled) r->ts_stalled++;
        else r->ts_stalled--;
    }
    if (stalled) {
        r->ts_wake = true;
        r->st_blocked = true;
    }
    pthread_mutex_unlock(&r->mu);
}

/* Fold the stage's new gap and forced counts into the stats. */
static void order_count(moq_media_receiver_t *r, receiver_ordering_t *o)
{
    uint64_t g = receiver_order_gaps_total(o->stage);
    uint64_t f = receiver_order_forced_total(o->stage);
    if (g == o->gaps_counted && f == o->forced_counted) return;
    pthread_mutex_lock(&r->mu);
    r->stats.loc_order_gaps = sat_add(r->stats.loc_order_gaps, g - o->gaps_counted);
    r->stats.loc_order_forced = sat_add(r->stats.loc_order_forced, f - o->forced_counted);
    pthread_mutex_unlock(&r->mu);
    o->gaps_counted = g;
    o->forced_counted = f;
}

/* Release a track's ordering stage and what it holds, with the lane slots
 * they reserved, without delivering anything. */
static void receiver_order_free(moq_media_receiver_t *r, moq_media_track_t *t)
{
    receiver_ordering_t *o = t->order;
    if (!o) return;
    if (o->has_parked) order_item_release(r, o->parked.ctx);
    if (o->end_pending) ts_drop_slot(r);
    order_set_stalled(r, o, false);
    order_count(r, o);
    receiver_order_destroy(o->stage, order_item_release, r);
    r->alloc.free(o, sizeof(*o), r->alloc.ctx);
    t->order = NULL;
}

/* The track's subscription is being issued: a fresh stage for it, and a new
 * loss generation. False on allocation failure (the subscribe is retried). */
static bool receiver_order_start(moq_media_receiver_t *r, moq_media_track_t *t)
{
    receiver_order_free(r, t);
    receiver_ordering_t *o = (receiver_ordering_t *)r->alloc.alloc(sizeof(*o),
                                                                  r->alloc.ctx);
    if (!o) return false;
    memset(o, 0, sizeof(*o));
    receiver_order_cfg_t cfg = { RECEIVER_LOC_ORDER_GROUPS, RECEIVER_LOC_ORDER_SLOTS,
                                 RECEIVER_LOC_ORDER_WINDOW_US };
    if (receiver_order_create(&r->alloc, &cfg, &o->stage) != MOQ_OK) {
        r->alloc.free(o, sizeof(*o), r->alloc.ctx);
        return false;
    }
    t->order = o;
    pthread_mutex_lock(&r->mu);
    t->ts_gen++;
    pthread_mutex_unlock(&r->mu);
    return true;
}

/* Hand one released item to the common queue, with its loss report. False
 * when it needs lane room that is not there yet: nothing changed. */
static bool receiver_order_release(moq_media_receiver_t *r, moq_media_track_t *t,
                                   const receiver_order_delivery_t *d)
{
    receiver_order_held_t *h = (receiver_order_held_t *)d->ctx;
    size_t len = h->so.payload ? moq_rcbuf_len(h->so.payload) : 0;
    if (!receiver_release_room(r, receiver_charge(r, t, len))) return false;
    receiver_loss_t loss;
    memset(&loss, 0, sizeof(loss));
    loss.gap = d->gap;
    loss.gap_group = d->gap_group;
    loss.gap_object = d->gap_object;
    loss.gap_tail_unknown = d->gap_tail_unknown;
    moq_sub_object_t so = h->so;
    uint64_t seq = h->seq;
    r->alloc.free(h, sizeof(*h), r->alloc.ctx);
    receiver_ts_adm_t saved = receiver_ts_adm_begin(r, seq);
    receiver_deliver_object(r, t, &so, &loss, NULL, MOQ_MEDIA_DELIVERY_LIVE, 0,
                            NULL, NULL);
    receiver_ts_adm_end(r, saved);
    return true;
}

/* Release every item the stage has due at now_us, in order, each with the
 * room its admission needs; then a pending END_OF_TRACK once the finished
 * stage is empty. True when it stopped for room: what it stopped at stays
 * parked, and the application's next poll wakes the network thread to
 * resume. */
static bool receiver_order_drain(moq_media_receiver_t *r, moq_media_track_t *t,
                                 uint64_t now_us)
{
    receiver_ordering_t *o = t->order;
    if (!o) return false;
    for (;;) {
        if (!o->has_parked) {
            if (receiver_order_poll(o->stage, now_us, &o->parked) != MOQ_OK) break;
            o->has_parked = true;
        }
        if (!receiver_order_release(r, t, &o->parked)) {
            order_count(r, o);
            order_set_stalled(r, o, true);
            return true;
        }
        o->has_parked = false;
    }
    order_count(r, o);
    order_set_stalled(r, o, false);
    if (o->end_pending && !receiver_order_holds(o->stage)) {
        o->end_pending = false;
        receiver_ts_adm_t saved = receiver_ts_adm_begin(r, r->ingress_seq);
        st_end_of_track(r, t, o->end_group, o->end_object);
        receiver_ts_adm_end(r, saved);
        receiver_mark_track_ended(r, t);
    }
    return false;
}

/* Hold `item` (one Object's facts) in the stage: `offer` is the intake
 * call. A full stage makes room by its capacity policy, one forced step at
 * a time, draining what each step releases; a step that cannot make room
 * refuses the fact. Bounded: each step frees a slot or retires a record. */
typedef moq_result_t (*order_intake_fn)(receiver_order_t *o, uint64_t now_us,
                                        uint64_t group, uint64_t object, void *item);

static moq_result_t receiver_order_intake(moq_media_receiver_t *r, moq_media_track_t *t,
                                          order_intake_fn offer, uint64_t now_us,
                                          uint64_t group, uint64_t object, void *item)
{
    receiver_ordering_t *o = t->order;
    moq_result_t rc = offer(o->stage, now_us, group, object, item);
    for (uint32_t i = 0;
         rc == MOQ_ERR_BUFFER && i <= RECEIVER_LOC_ORDER_GROUPS + RECEIVER_LOC_ORDER_SLOTS;
         i++) {
        /* What is already due leaves by a poll; only then does a forced
         * step make room. */
        if (receiver_order_drain(r, t, now_us)) break;   /* waits for lane room */
        rc = offer(o->stage, now_us, group, object, item);
        if (rc != MOQ_ERR_BUFFER) break;
        if (receiver_order_force(o->stage, now_us) != MOQ_OK) break;
        if (receiver_order_drain(r, t, now_us)) break;
        rc = offer(o->stage, now_us, group, object, item);
    }
    order_count(r, o);
    return rc;
}

static moq_result_t order_offer_fact_fin(receiver_order_t *o, uint64_t now_us,
                                         uint64_t group, uint64_t object, void *item)
{
    (void)item;
    return receiver_order_fin(o, now_us, group, object);
}

static moq_result_t order_offer_fact_loss(receiver_order_t *o, uint64_t now_us,
                                          uint64_t group, uint64_t object, void *item)
{
    (void)item;
    return receiver_order_loss(o, now_us, group, object);
}

/* One arriving Object of an ordered track. Objects are taken in even while
 * the application has the track disabled, so the stage's Location accounting
 * stays exact across a pause; what it releases then is dropped at queue
 * admission and reported as a gap on the next Object the app gets. */
static void receiver_on_ordered_object(moq_media_receiver_t *r, moq_media_track_t *t,
                                       moq_sub_object_t *so, uint64_t now_us)
{
    receiver_ordering_t *o = t->order;
    if (!o) { moq_sub_object_cleanup(so); return; }
    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        /* Subscription end: everything held is released, Group by Group,
         * then the track ends at its END_OF_TRACK. */
        uint64_t g = so->group_id, ob = so->object_id;
        moq_sub_object_cleanup(so);
        if (t->ended || o->end_pending) return;
        receiver_order_finish(o->stage);
        if (receiver_order_drain(r, t, now_us) || receiver_order_holds(o->stage)) {
            o->end_pending = true;
            o->end_group = g;
            o->end_object = ob;
            ts_keep_slot(r);
            return;
        }
        st_end_of_track(r, t, g, ob);
        receiver_mark_track_ended(r, t);
        return;
    }
    bool status = so->status == MOQ_OBJECT_END_OF_GROUP;
    if (!status && so->status != MOQ_OBJECT_NORMAL) {
        /* Another status says nothing the stage orders: it goes through as
         * for any track. */
        receiver_on_media_object(r, t, so, now_us);
        return;
    }
    receiver_order_held_t *h = (receiver_order_held_t *)r->alloc.alloc(sizeof(*h),
                                                                      r->alloc.ctx);
    /* The stage keeps the holder until it is released or the receiver is
     * destroyed, possibly on another thread: its buffers are made
     * transferable first. One that cannot be is lost exactly as when the
     * holder is refused: at its own Location, which the next released Object
     * of the track reports as a gap. */
    if (h && (!receiver_own_for_handoff(r, &so->payload) ||
              !receiver_own_for_handoff(r, &so->properties))) {
        r->alloc.free(h, sizeof(*h), r->alloc.ctx);
        h = NULL;
    }
    moq_result_t rc;
    if (!h) {
        pthread_mutex_lock(&r->mu);
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        pthread_mutex_unlock(&r->mu);
        uint64_t g = so->group_id, ob = so->object_id;
        moq_sub_object_cleanup(so);
        if (!status) {
            rc = receiver_order_intake(r, t, order_offer_fact_loss, now_us, g, ob, NULL);
            if (rc != MOQ_OK && rc != MOQ_DONE) {
                pthread_mutex_lock(&r->mu);
                ts_note_loss_locked(r, t, t->ts_gen, g, ob, r->obj_tail);
                pthread_mutex_unlock(&r->mu);
            }
        }
        (void)receiver_order_drain(r, t, now_us);
        return;
    }
    h->so = *so;
    memset(so, 0, sizeof(*so));
    pthread_mutex_lock(&r->mu);
    h->seq = r->intake_seq;
    pthread_mutex_unlock(&r->mu);
    rc = receiver_order_intake(r, t, status ? receiver_order_offer_status
                                            : receiver_order_offer,
                               now_us, h->so.group_id, h->so.object_id, h);
    if (rc == MOQ_OK) {
        ts_keep_slot(r);
    } else {
        /* Late, duplicated, past its Group's known end, or above an exhausted
         * Group (MOQ_ERR_PROTO); or no room the capacity policy could make
         * (MOQ_ERR_BUFFER), which is a local loss of this Object. */
        uint64_t g = h->so.group_id, ob = h->so.object_id;
        pthread_mutex_lock(&r->mu);
        if (rc == MOQ_ERR_PROTO) {
            r->stats.reorder_refused++;
        } else {
            r->stats.objects_received++;
            r->stats.objects_dropped++;
            if (!status) ts_note_loss_locked(r, t, t->ts_gen, g, ob, r->obj_tail);
        }
        pthread_mutex_unlock(&r->mu);
        order_held_free(r, h);
    }
    (void)receiver_order_drain(r, t, now_us);
}

/* A stream terminal's facts for the track's ordering stage, when it has
 * one: a FIN on the END_OF_GROUP subgroup ends the Group after its last
 * Object (MOQ_SUB_STREAM_FIN_END_OF_GROUP); a RESET that cut an Object short
 * lost that Object. A plain FIN, or a RESET without a cut Object, says
 * nothing about the Group (with one Object per subgroup, nothing was lost;
 * of an unknown layout nothing more can be inferred). */
static void receiver_order_terminal(moq_media_receiver_t *r, moq_media_track_t *t,
                                    const moq_sub_stream_terminal_t *st,
                                    uint64_t now_us)
{
    if (!t->order || t->sub_track != st->track) return;
    if (st->kind == MOQ_SUB_STREAM_FIN_END_OF_GROUP && st->has_last_object)
        (void)receiver_order_intake(r, t, order_offer_fact_fin, now_us,
                                    st->group_id, st->last_object_id, NULL);
    else if (st->kind == MOQ_SUB_STREAM_RESET && st->has_cut_object)
        (void)receiver_order_intake(r, t, order_offer_fact_loss, now_us,
                                    st->group_id, st->cut_object_id, NULL);
    else
        return;
    (void)receiver_order_drain(r, t, now_us);
}

/* The subscription ended: every held item is released, in order. True
 * when that stopped for lane room; it resumes with the next drain. */
static bool receiver_order_finish_now(moq_media_receiver_t *r, moq_media_track_t *t,
                                      uint64_t now_us)
{
    if (!t->order) return false;
    receiver_order_finish(t->order->stage);
    return receiver_order_drain(r, t, now_us) || receiver_order_holds(t->order->stage);
}

typedef struct { uint64_t barrier; bool found; } order_barrier_t;

static void order_barrier_probe(void *ctx, void *item)
{
    order_barrier_t *b = (order_barrier_t *)ctx;
    if (((const receiver_order_held_t *)item)->seq < b->barrier) b->found = true;
}

/* Whether `t`'s stage holds, or has parked, an Object taken in before
 * `barrier` (network thread). */
static bool order_holds_before(const moq_media_track_t *t, uint64_t barrier)
{
    const receiver_ordering_t *o = t->order;
    if (!o) return false;
    if (o->has_parked &&
        ((const receiver_order_held_t *)o->parked.ctx)->seq < barrier)
        return true;
    order_barrier_t b = { barrier, false };
    receiver_order_for_each(o->stage, order_barrier_probe, &b);
    return b.found;
}

/* -- SAP event-timeline intake (network thread) ------------------------- *
 * A self-contained, tolerant scanner for the fixed CMSF §3.6.1 payload: a JSON
 * array of {"l":[group,object],"data":[sapType,ept_ms]} records. It rejects
 * malformed input (the whole object is then a parse drop) rather than pulling
 * the msf-module's vendored JSON parser across the module boundary. */

typedef struct { const char *p, *end; } sap_cur_t;

static void sap_ws(sap_cur_t *c)
{
    while (c->p < c->end) {
        char ch = *c->p;
        if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') c->p++;
        else break;
    }
}

static bool sap_ch(sap_cur_t *c, char ch)
{
    sap_ws(c);
    if (c->p < c->end && *c->p == ch) { c->p++; return true; }
    return false;
}

static bool sap_uint(sap_cur_t *c, uint64_t *out)
{
    sap_ws(c);
    if (c->p >= c->end || *c->p < '0' || *c->p > '9') return false;
    uint64_t v = 0;
    int digits = 0;
    while (c->p < c->end && *c->p >= '0' && *c->p <= '9') {
        if (digits >= 19) return false;   /* overflow guard */
        v = v * 10u + (uint64_t)(*c->p - '0');
        c->p++; digits++;
    }
    *out = v;
    return true;
}

static bool sap_pair(sap_cur_t *c, uint64_t *a, uint64_t *b)
{
    return sap_ch(c, '[') && sap_uint(c, a) && sap_ch(c, ',') &&
           sap_uint(c, b) && sap_ch(c, ']');
}

/* Skip a JSON string starting at the opening quote (already positioned by the
 * caller via sap_ws). Returns false on an unterminated string. */
static bool sap_skip_string(sap_cur_t *c)
{
    c->p++;   /* opening quote */
    while (c->p < c->end && *c->p != '"') {
        if (*c->p == '\\') { c->p++; if (c->p >= c->end) return false; }
        c->p++;
    }
    if (c->p >= c->end) return false;
    c->p++;   /* closing quote */
    return true;
}

/* Skip an arbitrary JSON value (for unknown record keys): string, array/object
 * (balanced, string-aware), or a bareword number/true/false/null. */
static bool sap_skip_value(sap_cur_t *c)
{
    sap_ws(c);
    if (c->p >= c->end) return false;
    char ch = *c->p;
    if (ch == '"') return sap_skip_string(c);
    if (ch == '[' || ch == '{') {
        char open = ch, close = (ch == '[') ? ']' : '}';
        int depth = 0;
        while (c->p < c->end) {
            char x = *c->p;
            if (x == '"') { if (!sap_skip_string(c)) return false; continue; }
            if (x == open) depth++;
            else if (x == close) { depth--; c->p++; if (depth == 0) return true; continue; }
            c->p++;
        }
        return false;
    }
    while (c->p < c->end) {
        char x = *c->p;
        if (x == ',' || x == ']' || x == '}' ||
            x == ' ' || x == '\t' || x == '\n' || x == '\r') break;
        c->p++;
    }
    return true;
}

/* Parse one record object into (group, object, sap_type, ept_ms). Accepts "l"
 * and "data" in any order and tolerates (skips) unknown keys. */
static bool sap_record(sap_cur_t *c, uint64_t *g, uint64_t *o,
                       uint64_t *st, uint64_t *ept)
{
    if (!sap_ch(c, '{')) return false;
    sap_ws(c);
    if (c->p < c->end && *c->p == '}') return false;  /* empty: missing keys */
    bool has_l = false, has_upper_l = false, has_d = false;
    for (;;) {
        sap_ws(c);
        if (c->p >= c->end || *c->p != '"') return false;
        const char *kstart = c->p + 1;
        if (!sap_skip_string(c)) return false;
        size_t klen = (size_t)((c->p - 1) - kstart);   /* between the quotes */
        if (!sap_ch(c, ':')) return false;
        if (klen == 1 && (kstart[0] == 'l' || kstart[0] == 'L')) {
            /* The Location index is "l" in CMSF-01 3.6.1 and "L" in MSF-02
             * 8.2; either is read, but "only one of these index values may
             * be used within each record" (MSF-02 8.2). */
            bool upper = kstart[0] == 'L';
            if (upper ? (has_l && !has_upper_l) : has_upper_l) return false;
            if (!sap_pair(c, g, o)) return false;
            has_l = true;
            has_upper_l = upper;
        } else if (klen == 4 && memcmp(kstart, "data", 4) == 0) {
            if (!sap_pair(c, st, ept)) return false;
            has_d = true;
        } else if (!sap_skip_value(c)) {
            return false;
        }
        if (sap_ch(c, ',')) continue;
        if (sap_ch(c, '}')) break;
        return false;
    }
    if (!has_l || !has_d || *st > 3) return false;
    return true;
}

/* Append one record to the bounded SAP queue (r->mu held). Drops the oldest
 * record when the app under-polls -- harmless because the next §8.3 independent
 * object re-sends the accessible history (and the receiver's per-track dedup
 * keeps that from looping). */
static void sap_queue_push(moq_media_receiver_t *r, moq_media_track_t *track,
                           uint64_t group, uint64_t object,
                           moq_sap_type_t sap_type, uint64_t ept_ms)
{
    if (r->sap_tail - r->sap_head >= r->sap_cap)
        r->sap_head++;   /* drop oldest */
    moq_media_sap_record_t *rec = &r->saps[r->sap_tail % r->sap_cap];
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->track = track;
    rec->group = group;
    rec->object = object;
    rec->sap_type = sap_type;
    rec->ept_ms = ept_ms;
    r->sap_tail++;
}

/* Parse a SAP timeline object and enqueue its NEW records. Takes over / releases
 * the facade object's refs (the caller must NOT also cleanup). */
/* MSF_COMPRESSION of a timeline Object (MSF-02 12.1), judged once the
 * effective catalog is MSF-02; under MSF-01 the property is not interpreted
 * (an unknown property is ignored, draft-18 11.1) and the Object is counted
 * in compression_ignored. True when the Object is to be processed; otherwise
 * it was dropped (with a parse drop when the Object itself is refused), or
 * its origin was refused: a property the receiver cannot honor on the
 * Track ends the subscription as a local failure (MOQ_ERR_UNSUPPORTED), the
 * 12.1.1 SHOULD to unsubscribe. Takes no ownership. */
static bool receiver_timeline_payload_ok(moq_media_receiver_t *r,
                                         moq_media_track_t *track,
                                         const moq_sub_object_t *so,
                                         moq_media_parse_drop_class_t cls)
{
    moq_bytes_t tp = receiver_sub_track_props(r, so->track ? so->track : track->sub_track);
    moq_bytes_t op = rcbuf_bytes(so->properties);
    pthread_mutex_lock(&r->mu);
    bool r02 = r->has_effective && r->effective.revision == MOQ_MSF_REVISION_02;
    pthread_mutex_unlock(&r->mu);
    if (!r02) {
        if (receiver_props_mention_compression(r->transport_version, tp) ||
            receiver_props_mention_compression(r->transport_version, op)) {
            pthread_mutex_lock(&r->mu);
            r->stats.compression_ignored++;
            pthread_mutex_unlock(&r->mu);
        }
        return true;
    }
    receiver_payload_t pc = receiver_payload_compression(r->transport_version, tp, op);
    if (pc == RECEIVER_PAYLOAD_PLAIN) return true;
    pthread_mutex_lock(&r->mu);
    if (pc == RECEIVER_PAYLOAD_CONFLICT) r->stats.compression_conflicts++;
    else r->stats.compression_refused++;
    pthread_mutex_unlock(&r->mu);
    if (pc == RECEIVER_PAYLOAD_OBJECT_REFUSED)
        receiver_note_parse_drop(r, track, cls);
    else if (pc == RECEIVER_PAYLOAD_ORIGIN_REFUSED)
        receiver_mark_track_local_failure(r, track, MOQ_ERR_UNSUPPORTED,
                                          MOQ_MEDIA_TRACK_LOCAL_FAILURE);
    return false;
}

static void receiver_on_sap_object(moq_media_receiver_t *r,
                                   moq_media_track_t *track,
                                   moq_sub_object_t *so, uint64_t now_us)
{
    (void)now_us;

    /* A terminal END_OF_TRACK ends the timeline track, like the media path. */
    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        receiver_mark_track_ended(r, track);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    pthread_mutex_unlock(&r->mu);
    if (!wanted) { moq_sub_object_cleanup(so); return; }
    if (!receiver_timeline_payload_ok(r, track, so, MOQ_MEDIA_PARSE_DROP_SAP)) {
        moq_sub_object_cleanup(so);
        return;
    }
    if (!so->payload) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_SAP);
        moq_sub_object_cleanup(so);
        return;
    }
    /* MSF-02 8.1 asks a subscriber to check for MSF_EVENT_TIMELINE_HEADER
     * before parsing, and 12.3.1 lets the eventType define its serialization;
     * CMSF-01 3.6.1 defines the SAP payload and no header, so a header here
     * has no defined meaning: it is noted, not interpreted, and the records
     * parse as without it. */
    {
        bool hdr = false;
        receiver_prop_t hp;
        moq_bytes_t tp = receiver_sub_track_props(r, so->track ? so->track : track->sub_track);
        pthread_mutex_lock(&r->mu);
        bool r02 = r->has_effective && r->effective.revision == MOQ_MSF_REVISION_02;
        pthread_mutex_unlock(&r->mu);
        if (r02 && receiver_prop_find(r->transport_version, tp,
                                      RECEIVER_MSF_EVENT_TIMELINE_HEADER, &hdr,
                                      &hp) == MOQ_OK && hdr) {
            pthread_mutex_lock(&r->mu);
            r->stats.event_timeline_headers_ignored++;
            pthread_mutex_unlock(&r->mu);
        }
    }

    /* Parse the WHOLE array into a temporary list first; enqueue and advance the
     * dedup cursor only after the entire payload validates, so a valid record
     * followed by a malformed tail leaves nothing visible and does not poison
     * sap_last_* (which would suppress a later independent re-send). */
    moq_media_sap_record_t stack_tmp[64];
    moq_media_sap_record_t *tmp = stack_tmp;
    moq_media_sap_record_t *heap_tmp = NULL;
    size_t tmp_cap = sizeof(stack_tmp) / sizeof(stack_tmp[0]), tmp_n = 0;

    sap_cur_t c = { (const char *)moq_rcbuf_data(so->payload),
                    (const char *)moq_rcbuf_data(so->payload)
                        + moq_rcbuf_len(so->payload) };
    bool malformed = false;
    if (!sap_ch(&c, '[')) {
        malformed = true;
    } else {
        sap_ws(&c);
        if (c.p < c.end && *c.p == ']') {
            c.p++;   /* empty array: valid, no records */
        } else {
            for (;;) {
                uint64_t g, o, st, ept;
                if (!sap_record(&c, &g, &o, &st, &ept)) { malformed = true; break; }
                if (tmp_n == tmp_cap) {
                    size_t nc = tmp_cap * 2;
                    moq_media_sap_record_t *grown =
                        (moq_media_sap_record_t *)r->alloc.realloc(
                            heap_tmp, heap_tmp ? tmp_cap * sizeof(*tmp) : 0,
                            nc * sizeof(*tmp), r->alloc.ctx);
                    if (!grown) { malformed = true; break; }  /* OOM: drop whole */
                    if (!heap_tmp) memcpy(grown, stack_tmp, tmp_cap * sizeof(*tmp));
                    heap_tmp = grown; tmp = grown; tmp_cap = nc;
                }
                tmp[tmp_n].group = g;
                tmp[tmp_n].object = o;
                tmp[tmp_n].sap_type = (moq_sap_type_t)st;
                tmp[tmp_n].ept_ms = ept;
                tmp_n++;
                if (sap_ch(&c, ',')) continue;
                if (sap_ch(&c, ']')) break;
                malformed = true; break;
            }
        }
    }

    bool parse_drop_newly_pending = false;
    pthread_mutex_lock(&r->mu);
    if (malformed) {
        /* Already under r->mu here, so use the locked variant and wake below. */
        parse_drop_newly_pending = receiver_note_parse_drop_locked(
            r, track, MOQ_MEDIA_PARSE_DROP_SAP);
    } else {
        for (size_t i = 0; i < tmp_n; i++) {
            uint64_t g = tmp[i].group, o = tmp[i].object;
            /* Dedup the §8.3 independent re-sends: surface a media Location only
             * once, in (group, object) order. */
            bool newer = !track->sap_has_last ||
                g > track->sap_last_group ||
                (g == track->sap_last_group && o > track->sap_last_object);
            if (newer) {
                sap_queue_push(r, track, g, o, tmp[i].sap_type, tmp[i].ept_ms);
                track->sap_has_last = true;
                track->sap_last_group = g;
                track->sap_last_object = o;
            }
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (parse_drop_newly_pending)
        moq_endpoint_wake(r->ep);
    if (heap_tmp) r->alloc.free(heap_tmp, tmp_cap * sizeof(*tmp), r->alloc.ctx);
    moq_sub_object_cleanup(so);
}

/* Append one media-timeline record to the bounded queue (r->mu held). Drops the
 * oldest when the app sustainedly under-polls; because the per-track dedup cursor
 * has already advanced past a surfaced Location, a dropped record is NOT
 * re-surfaced by a later §7.3 independent re-send (same trade-off as the SAP
 * queue: bound memory over guaranteeing delivery to a stalled consumer). */
static void mt_queue_push(moq_media_receiver_t *r, moq_media_track_t *track,
                          const moq_msf_media_timeline_record_t *src)
{
    if (r->mt_tail - r->mt_head >= r->mt_cap)
        r->mt_head++;   /* drop oldest */
    moq_media_timeline_record_t *rec = &r->mts[r->mt_tail % r->mt_cap];
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->track = track;
    rec->media_time_ms = src->media_time_ms;
    rec->group = src->group;
    rec->object = src->object;
    rec->wallclock_ms = src->wallclock_ms;
    r->mt_tail++;
}

/* Parse an MSF §7.1.1 explicit media-timeline object and enqueue its NEW records
 * (those with a media Location past this track's last surfaced one). Takes over /
 * releases the facade object's refs (the caller must NOT also cleanup).
 *
 * All-or-nothing: a malformed payload enqueues nothing, counts one parse_drop,
 * leaves the dedup cursor untouched (so a later independent re-send still
 * surfaces), and is never fatal. */
static void receiver_on_media_timeline_object(moq_media_receiver_t *r,
                                              moq_media_track_t *track,
                                              moq_sub_object_t *so,
                                              uint64_t now_us)
{
    (void)now_us;

    /* A terminal END_OF_TRACK ends the timeline track, like the media path. */
    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        receiver_mark_track_ended(r, track);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    pthread_mutex_unlock(&r->mu);
    if (!wanted) { moq_sub_object_cleanup(so); return; }
    if (!receiver_timeline_payload_ok(r, track, so, MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE)) {
        moq_sub_object_cleanup(so);
        return;
    }
    if (!so->payload) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE);
        moq_sub_object_cleanup(so);
        return;
    }

    /* Decode the whole payload into a temp array first (stack, growing to heap
     * once for a large history), then enqueue+advance the dedup cursor only if it
     * fully validated -- so a malformed tail leaves nothing visible and does not
     * poison mt_last_* (which would suppress a later independent re-send). */
    moq_bytes_t json = { moq_rcbuf_data(so->payload),
                         moq_rcbuf_len(so->payload) };
    moq_msf_media_timeline_record_t stack_tmp[64];
    moq_msf_media_timeline_record_t *tmp = stack_tmp;
    moq_msf_media_timeline_record_t *heap_tmp = NULL;
    size_t cap = sizeof(stack_tmp) / sizeof(stack_tmp[0]), n = 0;

    moq_result_t rc = moq_msf_media_timeline_decode(json, tmp, cap, &n);
    if (rc == MOQ_ERR_BUFFER) {
        /* More records than the stack buffer: grow once to the needed count. */
        size_t need = n;
        heap_tmp = (moq_msf_media_timeline_record_t *)r->alloc.alloc(
            need * sizeof(*heap_tmp), r->alloc.ctx);
        if (!heap_tmp) {
            receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE);
            moq_sub_object_cleanup(so);
            return;
        }
        tmp = heap_tmp;
        cap = need;
        rc = moq_msf_media_timeline_decode(json, tmp, cap, &n);
    }
    if (rc != MOQ_OK) {
        receiver_note_parse_drop(r, track, MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE);
        if (heap_tmp) r->alloc.free(heap_tmp, cap * sizeof(*heap_tmp),
                                    r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    for (size_t i = 0; i < n; i++) {
        uint64_t g = tmp[i].group, o = tmp[i].object;
        /* Dedup the §7.3 independent re-sends by media Location (not by the
         * timeline object's own location): surface each Location once, in order. */
        bool newer = !track->mt_has_last ||
            g > track->mt_last_group ||
            (g == track->mt_last_group && o > track->mt_last_object);
        if (newer) {
            mt_queue_push(r, track, &tmp[i]);
            track->mt_has_last = true;
            track->mt_last_group = g;
            track->mt_last_object = o;
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (heap_tmp) r->alloc.free(heap_tmp, cap * sizeof(*heap_tmp), r->alloc.ctx);
    moq_sub_object_cleanup(so);
}

/* Process recorded SUBSCRIBE_DONEs (network thread), AFTER objects were drained
 * this tick so media delivered alongside the done is not lost.
 *
 * "Track Ended" (status 0x2) may precede either live->VOD conversion or a
 * terminal empty catalog (MSF §11.3). The formerly-live track is not yet
 * terminalized: already-delivered media stays queued, no TRACK_ENDED is
 * emitted, and a conversion catalog (isLive:false +
 * trackDuration) lands separately on the live catalog SUBSCRIBE and emits
 * TRACK_UPDATED on the SAME handle. The old facade slot is freed and sub_track
 * cleared so reconcile can re-subscribe once the conversion catalog confirms
 * VOD content (auto- or explicitly-subscribed alike -- both reconcile on
 * desired_subscribed). A terminal empty catalog instead projects this drained
 * done before removing the handle. vod_rearmed makes the re-arm one-shot.
 * Only a track declared live by its creation catalog can convert:
 * Track Ended on a never-live track
 * is terminal, like any other status or a repeat done after re-arm, and the
 * track ends exactly once. */
static void receiver_process_subscription_dones(moq_media_receiver_t *r,
                                                uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->done_pending) continue;
        if (r->joins_by_fill && !receiver_fill_poll_ready(r)) continue;
        /* Failed generations retire separately, after both intake paths and
         * any pending child terminal have drained. Never project peer ENDED
         * or turn a local metadata failure into a VOD resubscribe. */
        if (t->local_result != MOQ_OK || t->auth_failed) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            t->done_pending = false;
            continue;
        }
        uint64_t status = t->done_status;

        /* Every Object the subscription delivered is released before the
         * track is re-armed or ends: none is lost behind TRACK_ENDED. A
         * release that waits for lane room keeps the done for a later pass.
         * A re-armed subscription starts a fresh window, so the finished
         * one is freed once the facade slot is released. */
        if (receiver_ts_finish(r, t, now_us)) continue;
        if (receiver_order_finish_now(r, t, now_us)) continue;

        /* The subscription is already complete: free its facade slot (no
         * UNSUBSCRIBE is sent) and drop our handle so reconcile may re-subscribe. */
        if (t->sub_track) {
            /* A refused release keeps the mapping, on every draft: queued
             * facade records still name this slot. Retried next pass. */
            moq_result_t rc = moq_sub_release_track(r->sub, t->sub_track);
            if (rc != MOQ_OK) continue;
            t->sub_track = NULL;
        }
        receiver_ts_free(r, t);
        receiver_order_free(r, t);
        t->done_pending = false;
        moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
        /* the live subscription is gone; a pending app attribution
         * dies with it. The VOD-rearm replacement subscription's first
         * ack must not inherit it (and forward_sent resets with the fresh
         * SUBSCRIBE default below/at establish). */
        pthread_mutex_lock(&r->mu);
        t->upd_notify_app = false;
        t->update_inflight = false;
        t->upd_sent_app_gen = 0;
        t->parent_gen++;
        pthread_mutex_unlock(&r->mu);

        if (status == RECEIVER_DONE_TRACK_ENDED && t->was_live && !t->vod_rearmed &&
            !t->ended && !t->removed) {
            /* Eligible for VOD: keep desired_subscribed; reset the lifecycle
             * flags so track_state reads PENDING while awaiting conversion
             * and then SUBSCRIBE_OK. Delivered media stays in the queue. */
            t->vod_rearmed = true;
            pthread_mutex_lock(&r->mu);
            t->established = false;
            t->requested = false;
            pthread_mutex_unlock(&r->mu);
        } else {
            receiver_mark_track_done(r, t, status); /* terminal: end exactly once */
        }
    }
}

/* auto_subscribe seeding now happens per-track inside receiver_reconcile when a
 * new media tuple is discovered (any generation), so a track added after the
 * initial catalog is auto-subscribed too. The cross-queue guarantee still
 * holds: a new track's TRACK_ADDED is enqueued before reconcile_subscriptions
 * issues any SUBSCRIBE for it. */

/* Establish a facade subscription for every track the app wants that does not
 * have one yet, on the network thread. Idempotent: a track that already has a
 * handle is left alone -- disabling a track does NOT tear its subscription down
 * (see receiver_reconcile_delivery), so repeated enable/disable never allocates
 * a fresh handle and the facade's bounded track table is never exhausted by
 * toggling. Only documented capacity/credit refusals retain a selection. */
static void receiver_reconcile_subscriptions(moq_media_receiver_t *r,
                                             uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed || t->auth_failed) continue;
        /* A track without a namespace to subscribe (an MSF-01 namespace
         * override) is never subscribed (it is also refused at
         * subscribe_track). */
        if (!t->has_ns_fields) continue;
        /* A replacement subscribes only once its predecessor's facade slot is
         * released: drafts 16 and 18 allow one subscription per track, and
         * on draft 22 two overlapping subscriptions would duplicate delivery
         * and blur which handle an Object belongs to. */
        if (t->gate) {
            if (t->gate->sub_track) continue;
            pthread_mutex_lock(&r->mu);
            t->gate = NULL;
            pthread_mutex_unlock(&r->mu);
        }

        pthread_mutex_lock(&r->mu);
        bool want = t->desired_subscribed;
        moq_subscribe_filter_t filt = t->desired_filter;
        bool has_prio = t->desired_has_priority;
        uint8_t prio = t->desired_priority;
        uint32_t ts_program = t->desired_ts_program;
        receiver_track_options_t options = t->desired_options;
        uint64_t gen = t->app_gen;
        pthread_mutex_unlock(&r->mu);

        if (t->ended || (!want &&
                        t->auth_action == MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE))
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
        if (!want || t->sub_track || t->ended || t->local_result != MOQ_OK) continue;
        /* The original done may precede either conversion or a terminal empty
         * catalog (MSF 11.3). Wait for the validated conversion before owning
         * a replacement's streams, fill or TS window. */
        if (t->was_live && t->vod_rearmed && t->desc_cur->is_live) continue;
        /* A TS track's reorder window exists before its first Object can. */
        if (track_is_ts(t) && !receiver_ts_start(r, t, ts_program))
            continue;   /* allocation failed: retried next cycle */
        /* So does an ordered track's stage. */
        if (t->ordered && !receiver_order_start(r, t))
            continue;   /* allocation failed: retried next cycle */

        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        tcfg.track_namespace = t->ns_fields;
        tcfg.track_name = t->desc.name;
        tcfg.filter = filt ? filt : MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tcfg.has_subscriber_priority = has_prio;
        tcfg.subscriber_priority = prio;
        if (options.has_object_delivery_timeout)
            tcfg.object_delivery_timeout_ms = &options.object_delivery_timeout_ms;
        if (options.has_subgroup_delivery_timeout)
            tcfg.subgroup_delivery_timeout_ms = &options.subgroup_delivery_timeout_ms;
        if (options.has_forward)
            tcfg.forward = &options.forward;
        /* The live join (LargestObject) on a draft with fills is the section
         * 3.5 join at the current group, so playback can start at that
         * group's start instead of waiting for the next one. A publisher
         * that retains nothing fails the fill on the wire and delivery
         * proceeds from the Next Object as before. */
        moq_location_filter_cfg_t loc;
        moq_location_filter_cfg_t fill_loc;
        moq_fill_cfg_t fill;
        if (options.has_location || options.has_fill) {
            if (options.has_location) tcfg.location = &options.location;
            if (options.has_fill) {
                if (options.has_fill_location)
                    options.fill.location = &options.fill_location;
                tcfg.fill = &options.fill;
            }
        } else if (r->joins_by_fill &&
            tcfg.filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT &&
            !(options.has_forward && !options.forward)) {
            /* A passive track (Forward 0) takes no join fill: it would pull
             * the current group, which the passive subscription avoids. */
            receiver_join_current_group(&loc, &fill_loc, &fill);
            tcfg.location = &loc;
            tcfg.fill = &fill;
        }
        if (t->auth_action != MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE ||
            t->auth_generation != gen)
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
        t->auth_action = MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE;
        t->auth_generation = gen;
        moq_media_auth_request_t request = receiver_auth_request(r, t->auth_action, t);
        moq_result_t rc = moq_media_auth_operation_prepare(&t->auth_operation,
            &r->auth_source, &request, r->transport_version, &r->alloc);
        AUTH_PRE_ADMIT(r, t);
        pthread_mutex_lock(&r->mu);
        bool current = !r->fatal && !t->removed && !t->ended &&
                       !t->auth_failed && t->local_result == MOQ_OK &&
                       t->desired_subscribed && t->app_gen == gen;
        pthread_mutex_unlock(&r->mu);
        if (!current) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            receiver_ts_free(r, t);
            receiver_order_free(r, t);
            continue;
        }
        if (rc != MOQ_OK) {
            receiver_auth_fail(r, &t->auth_operation, &request,
                MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
            continue;
        }
        tcfg.auth_tokens = t->auth_operation.list.tokens;
        tcfg.auth_token_count = t->auth_operation.list.count;
        rc = moq_sub_subscribe(r->sub, &tcfg, now_us,
                                            &t->sub_track);
        if (rc == MOQ_OK) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            /* A fresh SUBSCRIBE forwards by default; a passive one does not. */
            t->passive = options.has_forward && !options.forward;
            t->forward_sent = !t->passive;
            t->forward_desired = !t->passive;
            t->has_drop_filter = false;
            r->subscribed_media = true;
            /* SUBSCRIBE issued, not yet accepted -> PENDING. Published under mu
             * for the app-thread track_state accessor; established is set later
             * from the on_subscribed (SUBSCRIBE_OK) callback. */
            pthread_mutex_lock(&r->mu);
            t->requested = true;
            pthread_mutex_unlock(&r->mu);
        } else if (receiver_admission_retry(rc)) {
            t->sub_track = NULL;        /* transient: retry next cycle */
            receiver_ts_free(r, t);
            receiver_order_free(r, t);
        } else if (receiver_auth_encoding_error(r, &t->auth_operation, rc)) {
            receiver_auth_fail(r, &t->auth_operation, &request,
                MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
        } else {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
    }
}

/* A command that can no longer be admitted is withdrawn, with no event: its
 * track ended, failed or was removed, or the subscription it was recorded
 * for was replaced. A taken one is settled by the executor step itself. */
static void receiver_cmd_withdraw_dead_locked(moq_media_track_t *t)
{
    if (t->cmd_state == FILL_CMD_QUEUED &&
        (t->ended || t->removed || t->local_result != MOQ_OK || t->auth_failed ||
         t->parent_gen != t->cmd_parent_gen))
        t->cmd_state = FILL_CMD_NONE;
}

/* Settle a taken command that was not admitted: back in the queue for a
 * transient refusal, otherwise -- or once a disable withdrew it -- the slot is
 * freed. Only the command `seq` still holding the claim is touched. */
static void receiver_cmd_unclaim_locked(moq_media_track_t *t, uint64_t seq,
                                        bool retry)
{
    if (t->cmd_state != FILL_CMD_CLAIMED || t->cmd_seq != seq) return;
    t->cmd_state = retry && !t->cmd_withdrawn ? FILL_CMD_QUEUED : FILL_CMD_NONE;
    t->cmd_withdrawn = false;
}

/* Admit the track's queued fill command as a fill-only UPDATE on its own
 * subscription. Called only from a delivery pass that found Forward already
 * at a forwarding target, with no UPDATE awaiting its response, so a pause or
 * resume always goes first and a resume is acknowledged before the fill
 * (draft-22 section 3.4.1: a paused subscription opens no fill). The claim is
 * taken under mu and held, without the mutex, across credential selection and
 * the facade call; a disable meanwhile only marks it withdrawn. */
static void receiver_admit_fill_command(moq_media_receiver_t *r,
                                        moq_media_track_t *t, uint64_t now_us)
{
    if (t->update_inflight || !moq_sub_track_is_active(t->sub_track)) return;
    pthread_mutex_lock(&r->mu);
    if (t->cmd_state != FILL_CMD_QUEUED || !t->desired_subscribed) {
        pthread_mutex_unlock(&r->mu);
        return;
    }
    t->cmd_state = FILL_CMD_CLAIMED;
    t->cmd_withdrawn = false;
    uint64_t seq = t->cmd_seq;
    uint64_t parent = t->cmd_parent_gen;
    moq_fill_cfg_t fill = t->cmd_fill;
    moq_location_filter_cfg_t location = t->cmd_location;
    fill.location = t->cmd_has_location ? &location : NULL;
    pthread_mutex_unlock(&r->mu);

    /* The command's selection is made once and reused by its retries; a
     * different command never inherits it. */
    if (t->cmd_auth_seq != seq) receiver_cmd_auth_release(r, t);
    if (!t->cmd_auth) {
        t->cmd_auth = (moq_media_auth_operation_t *)r->alloc.alloc(
            sizeof(*t->cmd_auth), r->alloc.ctx);
        if (!t->cmd_auth) {
            /* No storage for a selection now: the command waits its turn. */
            pthread_mutex_lock(&r->mu);
            receiver_cmd_unclaim_locked(t, seq, true);
            pthread_mutex_unlock(&r->mu);
            return;
        }
        memset(t->cmd_auth, 0, sizeof(*t->cmd_auth));
        t->cmd_auth_seq = seq;
    }
    moq_media_auth_request_t request =
        receiver_auth_request(r, MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE, t);
    moq_result_t rc = moq_media_auth_operation_prepare(t->cmd_auth,
        &r->auth_source, &request, r->transport_version, &r->alloc);
    AUTH_PRE_ADMIT(r, t);
    pthread_mutex_lock(&r->mu);
    bool current = !r->fatal && !t->removed && !t->ended && !t->auth_failed &&
                   t->local_result == MOQ_OK && t->desired_subscribed &&
                   !t->cmd_withdrawn && t->cmd_state == FILL_CMD_CLAIMED &&
                   t->cmd_seq == seq && t->parent_gen == parent;
    if (!current) receiver_cmd_unclaim_locked(t, seq, true);
    else if (rc != MOQ_OK) receiver_cmd_unclaim_locked(t, seq, false);
    pthread_mutex_unlock(&r->mu);
    /* A command put back in the queue keeps its selection; a withdrawn one
     * is released by the next delivery pass. */
    if (!current) return;
    if (rc != MOQ_OK) {
        receiver_auth_fail(r, t->cmd_auth, &request,
            MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
        receiver_cmd_auth_release(r, t);
        return;
    }
    moq_sub_update_cfg_t ucfg;
    moq_sub_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.fill = &fill;
    ucfg.auth_tokens = t->cmd_auth->list.tokens;
    ucfg.auth_token_count = t->cmd_auth->list.count;
    rc = moq_sub_update_subscription(r->sub, t->sub_track, &ucfg, now_us);
    if (rc == MOQ_OK) {
        /* The newest child of the track is the one just admitted. */
        moq_sub_fill_t token = moq_sub_track_fill(t->sub_track);
        receiver_cmd_auth_release(r, t);
        pthread_mutex_lock(&r->mu);
        /* Admission won, even over a disable that came during the call. */
        t->cmd_state = FILL_CMD_ADMITTED;
        t->cmd_withdrawn = false;
        t->cmd_token = token;
        /* A fill-only UPDATE carries no app pause/resume: its response is
         * never an UPDATE_OK, and it holds off the next UPDATE until then. */
        t->upd_notify_app = false;
        t->update_inflight = true;
        t->upd_sent_app_gen = 0;
        pthread_mutex_unlock(&r->mu);
        /* The child's CREATED is taken by the fill drain of a later pass,
         * not by any packet: run one. */
        moq_endpoint_wake(r->ep);
        return;
    }
    bool retry = receiver_admission_retry(rc);
    pthread_mutex_lock(&r->mu);
    receiver_cmd_unclaim_locked(t, seq, retry);
    pthread_mutex_unlock(&r->mu);
    if (receiver_auth_encoding_error(r, t->cmd_auth, rc)) {
        receiver_auth_fail(r, t->cmd_auth, &request,
            MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
        receiver_cmd_auth_release(r, t);
    } else if (!retry) {
        receiver_cmd_auth_release(r, t);
        receiver_mark_track_local_failure(r, t, rc,
                                           MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE);
    }
}

/* Settle a taken fetch command that was not sent: back in the queue for a
 * transient refusal, otherwise -- or once an unsubscribe withdrew it -- the
 * slot is freed with no event. Only the command `seq` still holding the
 * claim is touched. */
static void receiver_fetch_unclaim_locked(moq_media_track_t *t, uint64_t seq,
                                          bool retry)
{
    if (t->fetch_cmd_state != FETCH_CMD_CLAIMED || t->fetch_cmd_seq != seq) return;
    t->fetch_cmd_state = retry && !t->fetch_cmd_withdrawn ? FETCH_CMD_QUEUED
                                                          : FETCH_CMD_NONE;
    t->fetch_cmd_withdrawn = false;
}

/* A taken fetch command was refused for good before any FETCH went out:
 * its receipt is a FETCH_TERMINAL (MOQ_MEDIA_FETCH_NOT_SENT) carrying the
 * cause. The admission checked event room before taking the command, and
 * only the executor queues events. */
static void receiver_fetch_refused_locked(moq_media_receiver_t *r,
                                          moq_media_track_t *t, uint64_t seq,
                                          moq_result_t cause)
{
    if (t->fetch_cmd_state != FETCH_CMD_CLAIMED || t->fetch_cmd_seq != seq) return;
    t->fetch_cmd_withdrawn = false;
    if (r->ev_tail - r->ev_head >= r->ev_cap) {
        t->fetch_cmd_state = FETCH_CMD_NONE;
        r->fatal = true;
        r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW;
        return;
    }
    receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
    *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_FETCH_TERMINAL,
        .track = t, .fill_reason = MOQ_MEDIA_FETCH_NOT_SENT,
        .fill_error_domain = MOQ_MEDIA_FETCH_ERROR_NONE,
        .local_result = (int32_t)cause, .fetch_cmd_seq = seq };
    t->fetch_cmd_state = FETCH_CMD_RECEIPT;
}

/* A sent fetch's record: LIVE, keyed by its facade request, with its
 * FETCH_CREATED queued as the receipt of command `seq` (0: none). The
 * caller checked a free fetch record and event room under r->mu, which it
 * holds; only the executor takes records and queues events. */
static bool receiver_fetch_open_locked(moq_media_receiver_t *r, moq_media_track_t *t,
                                       moq_sub_fetch_req_t *req, uint64_t request_id,
                                       uint64_t seq)
{
    receiver_fill_dom_t *d = NULL;
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS && !d; i++)
        if (r->fill_doms[i].state == DOM_FREE) d = &r->fill_doms[i];
    if (!d || r->ev_tail - r->ev_head >= r->ev_cap) {
        /* Unreachable while only the executor takes records and queues
         * events; a broken invariant ends the receiver (mu is held). */
        assert(!"fetch record or event room vanished");
        if (!r->fatal) {
            r->fatal = true;
            r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW;
        }
        return false;
    }
    memset(d, 0, sizeof(*d));
    d->state = DOM_LIVE;
    d->fetch = true;
    d->fetch_req = req;
    d->track = t;
    d->gen = t->ts_gen;
    d->initiating_request_id = request_id;
    d->ts = track_is_ts(t);
    receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
    *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_FETCH_CREATED,
        .track = t, .initiating_request_id = request_id, .fetch_cmd_seq = seq };
    return true;
}

/* Send the track's queued fetch command: moq_sub_fetch with the track's
 * namespace and name on the receiver's subscriber. It is taken only with
 * room for its receipt and a free fetch record; both are returned by the
 * application's polls, which then wake the executor. The claim is held,
 * without the mutex, across credential selection and the facade call; an
 * unsubscribe meanwhile only marks it withdrawn, and admission may win. */
static void receiver_admit_fetch_command(moq_media_receiver_t *r,
                                         moq_media_track_t *t, uint64_t now_us)
{
    pthread_mutex_lock(&r->mu);
    if (t->fetch_cmd_state != FETCH_CMD_QUEUED || !t->desired_subscribed) {
        pthread_mutex_unlock(&r->mu);
        return;
    }
    bool room = r->ev_tail - r->ev_head < r->ev_cap;
    bool record = false;
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS && !record; i++)
        record = r->fill_doms[i].state == DOM_FREE;
    if (!room || !record) {
        r->fill_blocked = true;
        pthread_mutex_unlock(&r->mu);
        return;
    }
    t->fetch_cmd_state = FETCH_CMD_CLAIMED;
    t->fetch_cmd_withdrawn = false;
    uint64_t seq = t->fetch_cmd_seq;
    moq_media_fetch_cfg_t cfg = t->fetch_cmd_cfg;
    pthread_mutex_unlock(&r->mu);

    /* The command's selection is made once and reused by its retries; a
     * different command never inherits it. */
    if (t->fetch_auth_seq != seq) receiver_fetch_auth_release(r, t);
    if (!t->fetch_auth) {
        t->fetch_auth = (moq_media_auth_operation_t *)r->alloc.alloc(
            sizeof(*t->fetch_auth), r->alloc.ctx);
        if (!t->fetch_auth) {
            /* No storage for a selection now: the command waits its turn. */
            pthread_mutex_lock(&r->mu);
            receiver_fetch_unclaim_locked(t, seq, true);
            pthread_mutex_unlock(&r->mu);
            return;
        }
        memset(t->fetch_auth, 0, sizeof(*t->fetch_auth));
        t->fetch_auth_seq = seq;
    }
    moq_media_auth_request_t request =
        receiver_auth_request(r, MOQ_MEDIA_AUTH_ACTION_FETCH, t);
    moq_result_t rc = moq_media_auth_operation_prepare(t->fetch_auth,
        &r->auth_source, &request, r->transport_version, &r->alloc);
    AUTH_PRE_ADMIT(r, t);
    pthread_mutex_lock(&r->mu);
    bool current = !r->fatal && !t->removed && !t->ended && !t->auth_failed &&
                   t->local_result == MOQ_OK && t->desired_subscribed &&
                   !t->fetch_cmd_withdrawn && t->fetch_cmd_state == FETCH_CMD_CLAIMED &&
                   t->fetch_cmd_seq == seq;
    if (!current) receiver_fetch_unclaim_locked(t, seq, true);
    else if (rc != MOQ_OK) receiver_fetch_refused_locked(r, t, seq, rc);
    pthread_mutex_unlock(&r->mu);
    /* A command put back in the queue keeps its selection; a withdrawn one
     * is released by the next admission pass. */
    if (!current) return;
    if (rc != MOQ_OK) {
        /* The request alone fails: the track and its subscription stay. */
        moq_media_auth_operation_notify(t->fetch_auth, &r->auth_source, &request,
                                        MOQ_MEDIA_AUTH_SCOPE_REQUEST, t);
        receiver_fetch_auth_release(r, t);
        moq_endpoint_wake(r->ep);
        return;
    }

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init_sized(&fcfg, sizeof(fcfg));
    fcfg.track_namespace = r->namespace_;
    fcfg.track_name = t->desc.name;
    fcfg.start_group = cfg.start_group;
    fcfg.start_object = cfg.start_object;
    fcfg.end_group = cfg.end_group;
    fcfg.end_object = cfg.end_object;
    fcfg.has_subscriber_priority = cfg.has_subscriber_priority;
    fcfg.subscriber_priority = cfg.subscriber_priority;
    fcfg.auth_tokens = t->fetch_auth->list.tokens;
    fcfg.auth_token_count = t->fetch_auth->list.count;
    moq_sub_fetch_req_t *req = NULL;
    rc = moq_sub_fetch(r->sub, &fcfg, now_us, &req);
    if (rc == MOQ_OK) {
        /* The session entry exists from this call on: its Request ID is
         * the fetch's identity for the application. */
        uint64_t request_id = 0;
        moq_result_t idrc = moq_core_internal_sub_fetch_request_id(r->sub, req,
                                                                   &request_id);
        assert(idrc == MOQ_OK);
        (void)idrc;
        receiver_fetch_auth_release(r, t);
        pthread_mutex_lock(&r->mu);
        /* Only this executor takes fetch records and queues events, so the
         * record and the room checked above are still there. */
        if (!receiver_fetch_open_locked(r, t, req, request_id, seq)) {
            t->fetch_cmd_state = FETCH_CMD_NONE;
            pthread_mutex_unlock(&r->mu);
            return;
        }
        /* Admission won, even over an unsubscribe that came during the call. */
        t->fetch_cmd_state = FETCH_CMD_RECEIPT;
        t->fetch_cmd_withdrawn = false;
        pthread_mutex_unlock(&r->mu);
        moq_endpoint_wake(r->ep);
        return;
    }
    bool auth_error = receiver_auth_encoding_error(r, t->fetch_auth, rc);
    bool retry = !auth_error && receiver_admission_retry(rc);
    pthread_mutex_lock(&r->mu);
    if (retry) receiver_fetch_unclaim_locked(t, seq, true);
    else receiver_fetch_refused_locked(r, t, seq, rc);
    pthread_mutex_unlock(&r->mu);
    if (auth_error)
        moq_media_auth_operation_notify(t->fetch_auth, &r->auth_source, &request,
                                        MOQ_MEDIA_AUTH_SCOPE_REQUEST, t);
    if (!retry) {
        receiver_fetch_auth_release(r, t);
        moq_endpoint_wake(r->ep);
    }
}

/* Each track's queued fetch command goes out once its subscription is
 * established. A command of a track that ended, failed or was removed is
 * withdrawn with no event, as a fill command is. */
static void receiver_admit_fetch_commands(moq_media_receiver_t *r, uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        pthread_mutex_lock(&r->mu);
        if (t->fetch_cmd_state == FETCH_CMD_QUEUED &&
            (t->ended || t->removed || t->local_result != MOQ_OK || t->auth_failed))
            t->fetch_cmd_state = FETCH_CMD_NONE;
        bool queued = t->fetch_cmd_state == FETCH_CMD_QUEUED;
        bool selection_live = queued && t->fetch_cmd_seq == t->fetch_auth_seq;
        pthread_mutex_unlock(&r->mu);
        if (t->fetch_auth && !selection_live) receiver_fetch_auth_release(r, t);
        if (!queued || !t->sub_track || !moq_sub_track_is_active(t->sub_track))
            continue;
        receiver_admit_fetch_command(r, t, now_us);
    }
}

/* Carry out the application's cancel_fetch calls. A successful facade
 * cancel discards the fetch's items still in the facade and ends the fetch
 * as CANCELLED; any other final answer means the fetch already ended there,
 * and its own terminal (or local failure notice) is still to be read. */
static void receiver_cancel_fetches(moq_media_receiver_t *r, uint64_t now_us)
{
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++) {
        receiver_fill_dom_t *d = &r->fill_doms[i];
        pthread_mutex_lock(&r->mu);
        bool pending = d->state == DOM_LIVE && d->cancel_pending;
        moq_sub_fetch_req_t *req = d->fetch_req;
        pthread_mutex_unlock(&r->mu);
        if (!pending) continue;
        moq_result_t rc = moq_sub_cancel_fetch(r->sub, req, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_BUFFER) continue;
        pthread_mutex_lock(&r->mu);
        if (d->state == DOM_LIVE && d->fetch_req == req) {
            if (rc == MOQ_OK)
                dom_fetch_retire_locked(r, d, MOQ_MEDIA_FETCH_CANCELLED,
                                        MOQ_MEDIA_FETCH_ERROR_NONE, 0, 0);
            else
                d->cancel_pending = false;
        }
        pthread_mutex_unlock(&r->mu);
    }
    receiver_dom_wake(r);
}

/* The session closed: the facade ends its fetches with no terminal item, so
 * every fetch still open ends here, after the items already read (the
 * fetch items were drained earlier in this pass). A command not yet sent is
 * withdrawn with no event. */
static void receiver_close_fetches(moq_media_receiver_t *r, moq_session_t *session)
{
    if (moq_session_state(session) != MOQ_SESS_CLOSED) return;
    pthread_mutex_lock(&r->mu);
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++)
        if (r->fill_doms[i].state == DOM_LIVE)
            dom_fetch_retire_locked(r, &r->fill_doms[i], MOQ_MEDIA_FETCH_SESSION_CLOSED,
                                    MOQ_MEDIA_FETCH_ERROR_NONE, 0, 0);
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->fetch_cmd_state == FETCH_CMD_QUEUED)
            r->tracks[i]->fetch_cmd_state = FETCH_CMD_NONE;
    pthread_mutex_unlock(&r->mu);
    receiver_dom_wake(r);
}

/* Reconcile per-track delivery (the subscription's Forward State) with the two
 * things that can suppress it: an app unsubscribe (desired_subscribed=false) and
 * FLOW_CONTROL backpressure. A disabled track keeps its handle but pauses with
 * forward=false and has its queued objects purged once, so re-enabling is a cheap
 * forward=true with no new subscribe. The facade's one-outstanding-update rule is
 * tolerated by retrying on later cycles. */
static void receiver_reconcile_delivery(moq_media_receiver_t *r,
                                        uint64_t now_us)
{
    /* FLOW_CONTROL pauses every subscribed track together. D22 must drain both
     * object depth and combined bytes to low-water before resuming. */
    bool flow_allows = true;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
        r->subscribed_media) {
        pthread_mutex_lock(&r->mu);
        bool paused = r->paused;
        r->resume_nudged = false;   /* the hook ran; polls may nudge again */
        if (paused && obj_resume_ready_locked(r)) {
            r->paused = false;      /* reached low-water: resume */
            r->stats.pause_transitions++;
            paused = false;
        }
        pthread_mutex_unlock(&r->mu);
        flow_allows = !paused;
    }

    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        pthread_mutex_lock(&r->mu);
        receiver_cmd_withdraw_dead_locked(t);
        bool cmd_live = t->cmd_state != FILL_CMD_NONE && t->cmd_seq == t->cmd_auth_seq;
        pthread_mutex_unlock(&r->mu);
        if (t->cmd_auth && !cmd_live) receiver_cmd_auth_release(r, t);
        if (!t->sub_track || t->ended || t->removed || t->local_result != MOQ_OK ||
            t->auth_failed) continue;

        /* Race rule: {desired_subscribed, app_gen} are
         * snapshotted TOGETHER under r->mu, and ONLY the snapshot
         * generation feeds the no-wire reconciliation and the send
         * attribution. A toggle landing after this snapshot influenced
         * neither the computed target nor the sent update, so it must stay
         * UNRECONCILED for the next pass -- advancing to the live app_gen
         * here would either misattribute a flow-only update to the later
         * toggle or silently swallow the toggle's own future update. */
        pthread_mutex_lock(&r->mu);
        bool want = t->desired_subscribed;
        bool raised = t->raised;
        uint64_t gen_snap = t->app_gen;
        pthread_mutex_unlock(&r->mu);
        /* Queued objects for a disabled track are purged in unsubscribe_track
         * (independent of forward state); here we only drive the Forward State. */

        /* Test-only racy instant: a hook-driven toggle here lands AFTER
         * the snapshot -- it must influence neither this pass's target nor
         * its attribution, and must stay unreconciled for the next pass. */
        RECONCILE_RACE_HOOK();

        /* A passive track forwards only while the app raised it; raising
         * sends this update alone (never a join fill, on any draft). */
        bool target = want && flow_allows && (!t->passive || raised);
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        /* A raise of a passive MPEG-TS track: the relay starts forwarding it
         * mid-Group, so its first Objects may start the window at once
         * instead of waiting for Object 0 or the whole window. A window that
         * already released Objects before a lower is started afresh, since
         * the raised stream resumes past its last Location. */
        if (target && t->passive && !t->forward_sent && !t->ts_rejoin && t->ts) {
            receiver_ts_t *ts = (receiver_ts_t *)t->ts;
            if (!ts->released_any) {
                t->ts_rejoin = true;
            } else {
                /* Objects still held from before the lower are not wanted;
                 * they count as received and dropped, so none vanishes. */
                uint64_t held = 0;
                for (size_t i = 0; i < ts->slot_count; i++)
                    if (ts->slots[i].used && ts->slots[i].ctx &&
                        ts->slots[i].ctx != RECEIVER_TS_END_MARK) held++;
                if (ts->has_parked && ts->parked.ctx &&
                    ts->parked.ctx != RECEIVER_TS_END_MARK) held++;
                if (receiver_ts_start(r, t, t->desired_ts_program)) {
                    t->ts_rejoin = true;
                    if (held) {
                        pthread_mutex_lock(&r->mu);
                        r->stats.objects_received += held;
                        r->stats.objects_dropped += held;
                        pthread_mutex_unlock(&r->mu);
                    }
                }
                /* else: allocation failed; the next pass tries again */
            }
        }
#endif
        t->forward_desired = target;
        if (t->forward_desired == t->forward_sent) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            /* Wire-neutral pass: the SNAPSHOTTED app changes are proven
             * coalesced-away; reconcile them (and only them) silently. */
            pthread_mutex_lock(&r->mu);
            bool dummy_notify;
            attribution_classify(gen_snap, t->reconciled_app_gen,
                                 &dummy_notify, &t->reconciled_app_gen);
            pthread_mutex_unlock(&r->mu);
            /* Forward is settled: a forwarding subscription may take its
             * queued fill command. */
            if (target) receiver_admit_fill_command(r, t, now_us);
            continue;
        }
        /* The initial response must establish the track before UPDATE is
         * admissible. Preserve this pass's intent until that response lands. */
        if (moq_sub_track_get_state(t->sub_track) == MOQ_SUB_TRACK_PENDING ||
            t->update_inflight)
            continue;
        moq_sub_update_cfg_t ucfg;
        moq_sub_update_cfg_init_sized(&ucfg, sizeof(ucfg));
        ucfg.has_forward = true;
        ucfg.forward = t->forward_desired;
        if (t->auth_action != MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE ||
            t->auth_generation != gen_snap || t->auth_forward != target)
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
        t->auth_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
        t->auth_generation = gen_snap;
        t->auth_forward = target;
        moq_media_auth_request_t request = receiver_auth_request(r, t->auth_action, t);
        moq_result_t rc = moq_media_auth_operation_prepare(&t->auth_operation,
            &r->auth_source, &request, r->transport_version, &r->alloc);
        AUTH_PRE_ADMIT(r, t);
        pthread_mutex_lock(&r->mu);
        bool current = !r->fatal && !t->removed && !t->ended &&
                       !t->auth_failed && t->local_result == MOQ_OK &&
                       t->app_gen == gen_snap;
        pthread_mutex_unlock(&r->mu);
        /* Anonymous operation retains the existing snapshot-attribution rule. */
        if (r->auth_source.configured && !current) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            continue;
        }
        if (rc != MOQ_OK) {
            receiver_auth_fail(r, &t->auth_operation, &request,
                MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
            continue;
        }
        ucfg.auth_tokens = t->auth_operation.list.tokens;
        ucfg.auth_token_count = t->auth_operation.list.count;
        rc = moq_sub_update_subscription(r->sub, t->sub_track, &ucfg, now_us);
        if (rc == MOQ_OK) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            t->forward_sent = t->forward_desired;
            /* Attribution is decided ONCE, from the SNAPSHOT taken with
             * the target computation (the race rule above). */
            pthread_mutex_lock(&r->mu);
            attribution_classify(gen_snap, t->reconciled_app_gen,
                                 &t->upd_notify_app,
                                 &t->reconciled_app_gen);
            t->upd_sent_app_gen = gen_snap;
            t->update_inflight = true;
            pthread_mutex_unlock(&r->mu);
        } else if (receiver_auth_encoding_error(r, &t->auth_operation, rc)) {
            receiver_auth_fail(r, &t->auth_operation, &request,
                MOQ_MEDIA_AUTH_SCOPE_TRACK, t);
        } else if (rc < 0 && !receiver_admission_retry(rc)) {
            moq_media_auth_operation_clear(&t->auth_operation, &r->alloc);
            receiver_mark_track_local_failure(r, t, rc,
                                               MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE);
        }
    }
}

/* Apply the Group ends that are pollable now. A Group end becomes pollable
 * once the objects queued before it are polled, so calling this after each
 * polled object keeps the session's event order exactly: an end is applied
 * after the last object of its subgroup and before any object that followed
 * it. Only a TS track's window uses them. */
static void receiver_poll_group_ends(moq_media_receiver_t *r, uint64_t now_us)
{
    moq_sub_group_end_t ge;
    while (moq_sub_poll_group_end(r->sub, &ge) == MOQ_OK) {
        moq_media_track_t *t = track_for_sub(r, ge.track);
        if (t && !t->auth_failed && track_is_ts(t))
            receiver_on_ts_group_end(r, t, ge.group_id, now_us);
    }
}

/* Take the facade's pollable stream terminals, in order: the receiver always
 * surfaces them internally, since a track's in-group ordering stage needs
 * the FIN of its END_OF_GROUP subgroup and the Object a RESET cut. Each goes
 * to the stage of its track, when it has one, and onto the application's
 * lane only when the application enabled it; with the lane off it is then
 * dropped, so the facade queue never holds the session. Each is pollable
 * from the facade only once its stream's Objects were taken in, so its lane
 * barrier follows them. One that cannot go on the lane yet is held, and
 * intake stops until it does. */
static void receiver_poll_stream_terminals(moq_media_receiver_t *r, uint64_t now_us)
{
    if (!r->sub) return;
    for (;;) {
        if (r->st_enabled) {
            pthread_mutex_lock(&r->mu);
            st_place_held_locked(r);
            bool held = r->st_has_held;
            if (held) r->st_blocked = true;
            pthread_mutex_unlock(&r->mu);
            if (held) return;
        }
        moq_sub_stream_terminal_t st;
        if (moq_sub_poll_stream_terminal(r->sub, &st, sizeof(st)) != MOQ_OK)
            return;
        moq_media_track_t *t = track_for_sub(r, st.track);
        if (!t || t->is_sap_timeline || t->is_media_timeline ||
            t->is_event_timeline)
            continue;
        receiver_order_terminal(r, t, &st, now_us);
        if (!r->st_enabled) continue;
        moq_media_stream_terminal_t rec;
        memset(&rec, 0, sizeof(rec));
        rec.kind = st.kind == MOQ_SUB_STREAM_FIN_END_OF_GROUP
            ? MOQ_MEDIA_STREAM_FIN_END_OF_GROUP
            : st.kind == MOQ_SUB_STREAM_FIN ? MOQ_MEDIA_STREAM_FIN
                                            : MOQ_MEDIA_STREAM_RESET;
        rec.track = t;
        rec.group_id = st.group_id;
        rec.subgroup_id = st.subgroup_id;
        rec.error_code = st.error_code;
        rec.has_last_object = st.has_last_object;
        rec.last_object_id = st.last_object_id;
        rec.has_cut_object = st.has_cut_object;
        rec.cut_object_id = st.cut_object_id;
        pthread_mutex_lock(&r->mu);
        r->st_held = rec;
        r->st_has_held = true;
        st_place_held_locked(r);
        pthread_mutex_unlock(&r->mu);
    }
}

/* Pending facade items are not necessarily pollable yet. Conservatively keep
 * terminal handles until a later tick has exposed and drained those borrows. */
static bool receiver_facade_drained(moq_media_receiver_t *r)
{
    moq_sub_stats_t stats;
    if (moq_sub_get_stats(r->sub, &stats, sizeof(stats)) != MOQ_OK) return false;
    return stats.objects_queued == 0 && stats.chunks_queued == 0 &&
           stats.fetch_items_queued == 0 && stats.fill_items_queued == 0;
}

static void receiver_retire_subscription(moq_media_receiver_t *r,
                                         moq_sub_track_t **slot,
                                         uint64_t now_us, bool drained)
{
    moq_sub_track_t *t = *slot;
    if (!t) return;
    moq_sub_track_state_t state = moq_sub_track_get_state(t);
    if (state == MOQ_SUB_TRACK_PENDING || state == MOQ_SUB_TRACK_ACTIVE) {
        /* A refused unsubscribe owns exactly the same live handle. Retry on
         * the next pump; releasing now would lose both wire and child work. */
        if (moq_sub_unsubscribe(r->sub, t, now_us) != MOQ_OK) return;
    }
    if (!drained || moq_sub_fill_is_valid(moq_sub_track_fill(t))) return;
    if (moq_sub_release_track(r->sub, t) == MOQ_OK) *slot = NULL;
}

/* Retire the subscriptions of failed and removed tracks. */
/* Settle the tracks a terminal catalog removed while their subscriptions
 * were still delivering (completion_pending): once the subscription is over
 * at the facade -- its done, which the facade surfaces only after every
 * stream the done counts has been processed, or a refusal -- or the track
 * failed locally, the subscription is released and TRACK_REMOVED follows.
 * An END_OF_TRACK alone is not enough: it travels on its own stream and
 * can overtake the track's last Objects, and ending the subscription on it
 * would make the peer stop their stream. No timer settles it: the catalog
 * says the broadcast completed, not that its last objects were delivered.
 * A drained original done waiting for conversion also settles here, but only
 * if no replacement owns the slot and no done remains to be processed.
 * Its delivered media stays queued for the application either way. The
 * application's unsubscribe (completion_cancelled) settles it too, without
 * synthesizing an end: its queued media was purged by that call, and a refused
 * unsubscribe or release is retried by receiver_retire_tracks. */
static void receiver_settle_completions(moq_media_receiver_t *r, uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        pthread_mutex_lock(&r->mu);
        bool pending = t->completion_pending;
        bool cancelled = t->completion_cancelled;
        pthread_mutex_unlock(&r->mu);
        if (!pending) continue;
        if (!cancelled && t->local_result == MOQ_OK && t->vod_rearmed &&
            !t->sub_track && !t->done_pending)
            receiver_mark_track_done(r, t, RECEIVER_DONE_TRACK_ENDED);
        if (!cancelled && t->local_result == MOQ_OK && !t->auth_failed &&
            t->sub_track) {
            /* Live, or its done still to be processed (which releases the
             * slot once what it delivered has been taken in). */
            moq_sub_track_state_t st = moq_sub_track_get_state(t->sub_track);
            if (st == MOQ_SUB_TRACK_PENDING || st == MOQ_SUB_TRACK_ACTIVE ||
                t->done_pending)
                continue;
        }
        pthread_mutex_lock(&r->mu);
        t->completion_pending = false;
        pthread_mutex_unlock(&r->mu);
        if (t->sub_track && t->local_result == MOQ_OK)
            receiver_retire_subscription(r, &t->sub_track, now_us,
                                         receiver_facade_drained(r));
        receiver_ts_free(r, t);
        receiver_order_free(r, t);
        if (!rq_push(r, MOQ_MEDIA_TRACK_REMOVED, t)) return;   /* overflow terminalized */
    }
}

static void receiver_retire_tracks(moq_media_receiver_t *r, uint64_t now_us)
{
    bool drained = receiver_facade_drained(r);
    for (size_t i = 0; i < r->track_count; i++)
        if ((r->tracks[i]->local_result != MOQ_OK || r->tracks[i]->auth_failed ||
             r->tracks[i]->removed) &&
            !r->tracks[i]->completion_pending)
            receiver_retire_subscription(r, &r->tracks[i]->sub_track, now_us, drained);
}

static void receiver_st_mark_ready(moq_media_receiver_t *r);

/* A catalog ACK/bootstrap/delivery failure is receiver-local, not an endpoint/session failure.
 * Keep running on the executor after fatalization until cancellation and child
 * drain finish; no post node or failure-time allocation is needed. */
static void receiver_retire_failed_catalog(moq_media_receiver_t *r, uint64_t now_us)
{
    moq_sub_object_t object;
    while (moq_sub_poll_object(r->sub, &object) == MOQ_OK)
        moq_sub_object_cleanup(&object);
    moq_sub_fetch_item_t fetch;
    while (moq_sub_poll_fetch(r->sub, &fetch) == MOQ_OK) {
        if (fetch.request == r->catalog_fetch) {
            if (fetch.kind == MOQ_SUB_FETCH_ERROR || fetch.kind == MOQ_SUB_FETCH_COMPLETE ||
                fetch.kind == MOQ_SUB_FETCH_RESET)
                r->catalog_fetch = NULL;
        } else {
            /* As the fill drain below: Objects are discarded, a media
             * fetch's terminal still ends it. */
            pthread_mutex_lock(&r->mu);
            int dom = dom_fetch_find_locked(r, fetch.request);
            if (dom >= 0) dom_fetch_item_terminal_locked(r, &r->fill_doms[dom], &fetch);
            pthread_mutex_unlock(&r->mu);
        }
        moq_sub_fetch_item_cleanup(&fetch);
    }
    if (r->catalog_fetch &&
        moq_sub_cancel_fetch(r->sub, r->catalog_fetch, now_us) == MOQ_OK)
        r->catalog_fetch = NULL;
    moq_sub_fill_item_t fill;
    while (receiver_fill_poll_ready(r) &&
           moq_sub_poll_fill(r->sub, &fill, sizeof(fill)) == MOQ_OK) {
        receiver_fill_observe(r, &fill);
        if (fill.kind == MOQ_SUB_FILL_CREATED) {
            moq_sub_fill_item_cleanup(&fill);
            continue;
        }
        moq_sub_fill_item_cleanup(&fill);
    }
    /* A fill record made ready by a terminal read here wakes its waiters. */
    receiver_dom_wake(r);
    /* The stream terminals and Group ends of the discarded media go with
     * it, unapplied: they name the subscriptions released below. */
    moq_sub_stream_terminal_t st;
    while (moq_sub_poll_stream_terminal(r->sub, &st, sizeof(st)) == MOQ_OK) {}
    moq_sub_group_end_t ge;
    while (moq_sub_poll_group_end(r->sub, &ge) == MOQ_OK) {}
    /* So do what the reorder windows hold or have parked, with the lane
     * slots they reserved; records already on the lane that waited for
     * them become pollable. */
    for (size_t i = 0; i < r->track_count; i++) {
        receiver_ts_free(r, r->tracks[i]);
        receiver_order_free(r, r->tracks[i]);
    }
    receiver_st_mark_ready(r);
    bool drained = receiver_facade_drained(r);
    receiver_retire_subscription(r, &r->catalog_sub, now_us, drained);
    for (size_t i = 0; i < r->track_count; i++)
        receiver_retire_subscription(r, &r->tracks[i]->sub_track, now_us, drained);
}

/* Reorder windows release on time as well as on arrival; cache the next
 * time one is due for the endpoint's wake (receiver_next_deadline_us). A
 * window stopped for lane room has no deadline: the application's poll
 * wakes it. */
static void receiver_ts_timers(moq_media_receiver_t *r, uint64_t now_us)
{
    uint64_t deadline = UINT64_MAX;
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->order && !receiver_order_drain(r, t, now_us)) {
            uint64_t d = receiver_order_next_deadline_us(t->order->stage);
            if (d < deadline) deadline = d;
        }
        if (!t->ts) continue;
        if (receiver_ts_drain(r, t, now_us)) continue;
        uint64_t d = receiver_ts_deadline(t);
        if (d < deadline) deadline = d;
    }
    pthread_mutex_lock(&r->mu);
    r->ts_deadline_us = deadline;
    pthread_mutex_unlock(&r->mu);
}

/* Resume the windows stopped for lane room before intake, which waits for
 * them (network thread): the application's poll that returned the room
 * woke this pass. */
static void receiver_ts_resume(moq_media_receiver_t *r, uint64_t now_us)
{
    pthread_mutex_lock(&r->mu);
    uint32_t stalled = r->ts_stalled;
    pthread_mutex_unlock(&r->mu);
    if (stalled == 0) return;
    for (size_t i = 0; i < r->track_count; i++) {
        if (r->tracks[i]->ts) (void)receiver_ts_drain(r, r->tracks[i], now_us);
        if (r->tracks[i]->order) (void)receiver_order_drain(r, r->tracks[i], now_us);
    }
}

/* Mark ready the records of MPEG2TS tracks whose reorder window holds no
 * Object taken in before them, after this pass's window progress (network
 * thread). Nothing joins a window below a record's barrier later, so a
 * record once ready stays ready. */
static void receiver_st_mark_ready(moq_media_receiver_t *r)
{
    if (!r->st_enabled) return;
    bool woke = false;
    pthread_mutex_lock(&r->mu);
    for (uint32_t i = r->st_head; i != r->st_tail; i++) {
        receiver_st_entry_t *e = &r->st[i % r->st_cap];
        if (e->ready) continue;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        if (ts_holds_before(e->rec.track, e->barrier)) continue;
#endif
        if (order_holds_before(e->rec.track, e->barrier)) continue;
        e->ready = true;
        woke = true;
    }
    pthread_mutex_unlock(&r->mu);
    if (woke) moq_endpoint_wake(r->ep);
}

/* On an endpoint shared with a sender, the receiver's subscriber reads the
 * session's events and offers each to the sender first (endpoint_internal.h). */
static moq_core_internal_route_t receiver_offer_event(void *ctx, const moq_event_t *event,
                                                      uint64_t now_us)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (!r->ep) return MOQ_CORE_INTERNAL_ROUTE_LEFT;
    return moq_endpoint_route_event_locked(r->ep, MOQ_ENDPOINT_HOOK_RECEIVER, event, now_us);
}

static moq_result_t receiver_tick(moq_media_receiver_t *r, uint64_t now_us)
{
    return moq_core_internal_sub_tick_routed(r->sub, now_us, receiver_offer_event, r);
}

/* The reader while its subscriber exists and the hook ticks it: whenever the
 * receiver is not fatal (every pump that leaves it so has ticked), and while a
 * failed catalog is retired, which ticks every pump although fatal. */
static bool receiver_reads_events(void *ctx)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (!r->sub) return false;
    pthread_mutex_lock(&r->mu);
    bool reads = !r->fatal || r->catalog_ack_failed;
    pthread_mutex_unlock(&r->mu);
    return reads;
}

/* The endpoint pump hook: runs on the network thread under the endpoint
 * mutex (endpoint_internal.h contract -- no public endpoint API calls). */
static void receiver_hook(moq_endpoint_t *ep, moq_session_t *session,
                          uint64_t now_us, void *ctx)
{
    (void)ep;
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;

    pthread_mutex_lock(&r->mu);
    bool fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (!session) return;
    if (r->catalog_ack_failed) {
        receiver_auth_clear_pending(r);
        (void)receiver_tick(r, now_us);
        receiver_inspect_delivery_failures(r);
        receiver_retire_failed_catalog(r, now_us);
        /* Fetches still open end here too when the session closes. */
        receiver_close_fetches(r, session);
        return;
    }
    if (fatal) {
        receiver_auth_clear_pending(r);
        return;
    }

    /* Read the negotiated draft from the live session, under our own mutex,
     * before any catalog is ingested or any object is parsed on this pass.
     * The session's version is fixed at creation, so this can never disagree
     * with the bytes on the wire. Same reconnect note as the sender's latch. */
    {
        moq_version_t v = moq_session_version(session);
        pthread_mutex_lock(&r->mu);
        receiver_latch_version_locked(r, v);
        bool usable =
            receiver_companion_d22(r->companion_profile)
                ? v == MOQ_VERSION_DRAFT_22
            : r->companion_profile == MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01
                ? v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18
                : false;
        pthread_mutex_unlock(&r->mu);
        if (!usable) {
            if (moq_session_state(session) == MOQ_SESS_ESTABLISHED)
                receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
    }

    if (!r->sub) {
        moq_session_state_t st = moq_session_state(session);
        /* SETUP and a session-wide GOAWAY can both precede this receiver's
         * first pass (or the receiver is attached after them): the facade is
         * still created, as the session's one event owner, so the GOAWAY and
         * its New Session URI are observed; nothing is subscribed on a
         * session that is going away. */
        bool observe = st == MOQ_SESS_DRAINING;
        if (st != MOQ_SESS_ESTABLISHED && !observe)
            return;
        moq_sub_cfg_t scfg;
        /* Full current struct: this receiver sets the appended on_subscribe_done
         * hook, so it must use the sized initializer -- the pointer-only init
         * stamps only the frozen prefix and would disable that callback. */
        moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.max_tracks = RECEIVER_SUB_MAX_TRACKS;
        scfg.max_objects = RECEIVER_SUB_MAX_OBJECTS;
        scfg.max_fills = RECEIVER_SUB_MAX_FILLS;
        moq_sub_callbacks_init(&scfg.callbacks);
        scfg.callbacks.ctx = r;
        scfg.callbacks.on_subscribed = receiver_on_subscribed;
        scfg.callbacks.on_subscribe_error = receiver_on_subscribe_error;
        scfg.on_subscribe_done = receiver_on_subscribe_done;
        scfg.on_update_ok = receiver_on_update_ok;
        /* Group ends let a TS track's reorder window release the next Group
         * at once instead of after the window. */
        scfg.surface_group_ends = true;
        /* Always on: the in-group ordering stage takes its terminal facts
         * from them; the application's lane stays gated by its own option
         * (receiver_poll_stream_terminals). */
        scfg.surface_stream_terminals = true;
        scfg.on_goaway = receiver_on_goaway;
        if (moq_sub_create(session, &r->alloc, &scfg, &r->sub) != MOQ_OK) {
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
        r->observe_only = observe;
    }
    if (r->observe_only) {
        (void)receiver_tick(r, now_us);
        return;
    }
    if (!r->catalog_sub) {
        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        /* LargestObject is mandatory for the catalog: MSF-01 §5 requires the
         * catalog be obtained via SUBSCRIBE + a Joining FETCH(offset=0), and a
         * joining FETCH can only attach to a LargestObject subscription
         * (session_fetch.c). The SUBSCRIBE then carries subsequent live catalog
         * updates. On a draft with fills the same retrieval is the section
         * 3.5 join at the current group: the subscription starts at the Next
         * Object and a fill delivers the current catalog group from its
         * start (the independent object 0 plus its deltas). */
        tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tcfg.track_namespace = r->namespace_;
        tcfg.track_name = (moq_bytes_t){ r->catalog_name,
                                         r->catalog_name_len };
        r->joins_by_fill = moq_session_supports_fill(session);
        r->catalog_bootstrap.active = r->joins_by_fill;
        /* MSF 5: the catalog subscription SHOULD have a higher subscriber
         * priority than any other concurrent one; 0 is the highest, and the
         * revision is not known before subscribing, so every revision gets
         * it. */
        tcfg.has_subscriber_priority = true;
        tcfg.subscriber_priority = 0;
        moq_location_filter_cfg_t loc;
        moq_location_filter_cfg_t fill_loc;
        moq_fill_cfg_t fill;
        if (r->joins_by_fill) {
            receiver_join_current_group(&loc, &fill_loc, &fill);
            fill.has_subscriber_priority = true;
            fill.subscriber_priority = 0;
            tcfg.location = &loc;
            tcfg.fill = &fill;
        }
        moq_media_auth_request_t request = receiver_auth_request(r,
            MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE, NULL);
        moq_result_t rc = moq_media_auth_operation_prepare(&r->catalog_auth,
            &r->auth_source, &request, r->transport_version, &r->alloc);
        AUTH_PRE_ADMIT(r, NULL);
        pthread_mutex_lock(&r->mu);
        fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal) {
            moq_media_auth_operation_clear(&r->catalog_auth, &r->alloc);
            return;
        }
        if (rc != MOQ_OK) {
            receiver_auth_fail(r, &r->catalog_auth, &request,
                MOQ_MEDIA_AUTH_SCOPE_SERVICE, NULL);
            return;
        }
        tcfg.auth_tokens = r->catalog_auth.list.tokens;
        tcfg.auth_token_count = r->catalog_auth.list.count;
        rc = moq_sub_subscribe(r->sub, &tcfg, now_us, &r->catalog_sub);
        if (receiver_admission_retry(rc)) {
            (void)receiver_tick(r, now_us);
            return;
        }
        if (receiver_auth_encoding_error(r, &r->catalog_auth, rc)) {
            receiver_auth_fail(r, &r->catalog_auth, &request,
                MOQ_MEDIA_AUTH_SCOPE_SERVICE, NULL);
            return;
        }
        moq_media_auth_operation_clear(&r->catalog_auth, &r->alloc);
        if (rc != MOQ_OK) {
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
    }

    (void)receiver_tick(r, now_us);
    receiver_inspect_ack_results(r, now_us);
    receiver_inspect_delivery_failures(r);
    if (r->catalog_ack_failed) {
        receiver_retire_failed_catalog(r, now_us);
        receiver_close_fetches(r, session);
        return;
    }

    receiver_ts_resume(r, now_us);
    moq_sub_object_t obj;
    while (receiver_intake_begin(r) &&
           moq_sub_poll_object(r->sub, &obj) == MOQ_OK) {
        if (obj.track == r->catalog_sub) {
            if (r->catalog_bootstrap.active)
                receiver_bootstrap_stage_live(r, &obj);
            else if (!r->catalog_ack_failed)
                receiver_ingest_catalog(r, obj.group_id, obj.object_id,
                                        obj.payload, rcbuf_bytes(obj.properties),
                                        receiver_sub_track_props(r,
                                            obj.track ? obj.track : r->catalog_sub),
                                        now_us);
            moq_sub_object_cleanup(&obj);
            /* Every polled Object can make a Group end ready, whichever
             * track it belongs to. */
            receiver_intake_end(r);
            receiver_poll_group_ends(r, now_us);
            receiver_poll_stream_terminals(r, now_us);
            continue;
        }
        moq_media_track_t *t = track_for_sub(r, obj.track);
        if (!t || t->auth_failed ||
            (t->local_result != MOQ_OK && !t->delivery_failed)) {
            moq_sub_object_cleanup(&obj);
            receiver_intake_end(r);
            receiver_poll_group_ends(r, now_us);
            receiver_poll_stream_terminals(r, now_us);
            continue;
        }
        /* Ownership of obj's refs passes to the intake. A CMSF SAP timeline is
         * JSON metadata parsed to typed SAP records (poll_sap); an MSF §7 media
         * timeline is JSON parsed to typed records (poll_media_timeline) once
         * explicitly subscribed; any OTHER event timeline is neither SAP nor
         * media, so it is dropped (not surfaced and not leaked into poll_object). */
        if (t->is_sap_timeline)
            receiver_on_sap_object(r, t, &obj, now_us);
        else if (t->is_media_timeline)
            receiver_on_media_timeline_object(r, t, &obj, now_us);
        else if (t->is_event_timeline)
            moq_sub_object_cleanup(&obj);
        else if (track_is_ts(t))
            receiver_on_ts_object(r, t, &obj, now_us);
        else if (t->order)
            receiver_on_ordered_object(r, t, &obj, now_us);
        else
            receiver_on_media_object(r, t, &obj, now_us);
        receiver_intake_end(r);
        receiver_poll_group_ends(r, now_us);
        receiver_poll_stream_terminals(r, now_us);
    }
    receiver_intake_end(r);
    receiver_poll_group_ends(r, now_us);
    receiver_poll_stream_terminals(r, now_us);

    /* Process subscription completions after draining this tick's objects.
     * The first "Track Ended" of a track created live may re-arm it for VOD;
     * a never-live track or a repeat done ends it. */
    if (!r->joins_by_fill) receiver_process_subscription_dones(r, now_us);
    /* Issue + drain the catalog Joining FETCH (MSF-01 §5). Runs after the
     * subscribe poll so a SUBSCRIBE-delivered catalog update dedupes a later
     * FETCH object; may itself terminalize (catalog unusable), caught below. */
    receiver_pump_catalog_fetch(r, now_us);
    if (r->catalog_done_pending && !r->has_effective && !r->catalog_ack_failed)
        receiver_bootstrap_fail(r);
    r->catalog_done_pending = false;
    receiver_cancel_fills(r, now_us);
    /* Standalone fetches: the application's cancellations, then, on a closed
     * session, the end of every fetch still open. */
    receiver_cancel_fetches(r, now_us);
    receiver_close_fetches(r, session);
    if (r->joins_by_fill) receiver_process_subscription_dones(r, now_us);
    receiver_settle_completions(r, now_us);
    receiver_retire_tracks(r, now_us);
    receiver_publish_delivery_failures(r);

    /* Catalog handling above may have terminalized (e.g. track-event
     * queue overflow): a terminal receiver must not start media
     * subscriptions in the same pump cycle. */
    pthread_mutex_lock(&r->mu);
    fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (fatal) {
        receiver_auth_clear_pending(r);
        receiver_dom_wake(r);
        return;
    }

    /* auto_subscribe desired state is seeded per-track in receiver_reconcile
     * (any generation), so the hook only needs to issue the subscriptions. */
    if (r->has_effective)
        receiver_reconcile_subscriptions(r, now_us);

    pthread_mutex_lock(&r->mu);
    fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (fatal) {
        receiver_auth_clear_pending(r);
        receiver_dom_wake(r);
        return;
    }
    receiver_reconcile_delivery(r, now_us);
    receiver_admit_fetch_commands(r, now_us);

    receiver_ts_timers(r, now_us);
    receiver_st_mark_ready(r);
    /* Evictions and purges in this pass may have made a fill terminal ready. */
    receiver_dom_wake(r);
}

/* The earliest reorder-window deadline, as the last pump cached it: a pure
 * read, as the endpoint's next_deadline_us contract requires. */
static uint64_t receiver_next_deadline_us(void *ctx)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    pthread_mutex_lock(&r->mu);
    uint64_t d = r->fatal ? UINT64_MAX : r->ts_deadline_us;
    pthread_mutex_unlock(&r->mu);
    return d;
}

/* Subscriber teardown must happen where session calls are legal: posted to the
 * network thread. Its object queue, pending slot and fill/fetch items hold
 * payloads taken before intake. Each was allocated through the session's
 * allocator -- the session's own copy, or a transport input the session
 * shared because it came from that same allocator -- and a service session's
 * allocator is the endpoint's, which the endpoint contract requires to be
 * valid on any thread. A shared input may still be referenced by the
 * transport, so while the network thread runs the subscriber is destroyed
 * there, never on the destroying thread. Once stop() has joined that thread
 * the teardown runs on stop()'s caller (its terminal drain), or on the
 * destroying thread when the endpoint was stopped before the post: no other
 * thread then holds those references, and their allocator is valid there.
 *
 * The context is independent of the receiver (which may be freed before a
 * queued task drains): it owns the subscriber plus a snapshot of its live
 * (PENDING/ACTIVE) subscription handles -- the catalog subscription and every
 * still-subscribed media/timeline track. The handles live inside the
 * subscriber, not the receiver, so they stay valid until moq_sub_destroy; they
 * are distinct slots of a subscriber built with RECEIVER_SUB_MAX_TRACKS, so the
 * context holds them inline. A receiver built on an endpoint reserves the
 * context and its task node at construction (receiver_reserve_teardown), so
 * destroy allocates nothing and cannot be refused for want of memory. */
typedef struct receiver_teardown {
    moq_alloc_t        alloc;     /* the context's own allocator */
    moq_subscriber_t  *sub;
    moq_sub_track_t   *handles[RECEIVER_SUB_MAX_TRACKS];  /* live handles to cancel */
    size_t             count;
    size_t             cursor;    /* next handle to cancel (resume after retry) */
} receiver_teardown_t;

static receiver_teardown_t *receiver_teardown_new(const moq_alloc_t *alloc)
{
    receiver_teardown_t *td =
        (receiver_teardown_t *)alloc->alloc(sizeof(*td), alloc->ctx);
    if (!td) return NULL;
    memset(td, 0, sizeof(*td));
    td->alloc = *alloc;
    return td;
}

static void receiver_teardown_free(receiver_teardown_t *td)
{
    moq_alloc_t alloc = td->alloc;
    alloc.free(td, sizeof(*td), alloc.ctx);
}

/* Reserve the teardown context and its endpoint task node (both from the
 * endpoint's allocator, which the endpoint contract requires to be
 * thread-safe: the network thread frees them after the task). False on
 * allocation failure; anything reserved stays for receiver_release_teardown. */
static bool receiver_reserve_teardown(moq_media_receiver_t *r)
{
    const moq_alloc_t *alloc = moq_endpoint_internal_alloc(r->ep);
    if (!alloc) return false;
    if (!r->teardown) r->teardown = receiver_teardown_new(alloc);
    if (!r->teardown) return false;
    if (!r->teardown_slot) r->teardown_slot = moq_endpoint_internal_reserve_task(r->ep);
    return r->teardown_slot != NULL;
}

/* Free whatever of the reservation was not handed to the endpoint. Runs
 * while r->ep is still valid. */
static void receiver_release_teardown(moq_media_receiver_t *r)
{
    if (r->teardown) receiver_teardown_free(r->teardown);
    r->teardown = NULL;
    if (r->teardown_slot) moq_endpoint_internal_release_task(r->ep, r->teardown_slot);
    r->teardown_slot = NULL;
}

/* Subscriber teardown on the network thread (an endpoint post() task).
 *
 * Cancel every still-live receiver-owned subscription (moq_sub_unsubscribe ->
 * UNSUBSCRIBE, freeing the session entry) BEFORE destroying the facade. Without
 * this, a destroyed-receiver-but-still-alive-endpoint peer could keep streaming
 * objects for subscriptions nothing drains -- the facade hook is already gone.
 * moq_sub_destroy alone (the old behavior) only frees facade-local state and
 * leaves those session subscriptions active.
 *
 * Resumable via the task-return contract: a transiently full session queue
 * yields MOQ_ERR_WOULD_BLOCK (action queue full) or MOQ_ERR_BUFFER (the control
 * send scratch is momentarily full of not-yet-flushed actions -- and an
 * UNSUBSCRIBE, always far smaller than the buffer, can never be the permanent
 * "cannot ever fit" overflow). Both are transient: the task preserves its cursor
 * (and the subscriber + remaining handles) and returns MOQ_ERR_WOULD_BLOCK,
 * which tells the pump to REQUEUE this same task node for a later cycle -- no
 * re-allocation, no dependence on a second post() succeeding. The subscriber is
 * destroyed only after every live handle is cancelled, or the session is gone.
 *
 * session == NULL is the terminal drain (stop()) or an endpoint that never
 * produced a session: nothing live to leak into, so destroy locally and return
 * OK (the terminal drain must never requeue). WRONG_STATE (already terminal/DONE)
 * and STALE_HANDLE (entry already freed) are nothing-to-do; advance past them. */
static moq_result_t receiver_destroy_sub_task(moq_endpoint_t *ep,
                                              moq_session_t *session,
                                              uint64_t now_us, void *ctx)
{
    receiver_teardown_t *td = (receiver_teardown_t *)ctx;
    (void)ep;

    if (session) {
        while (td->cursor < td->count) {
            moq_sub_track_t *h = td->handles[td->cursor];
            if (h) {
                moq_result_t rc = moq_sub_unsubscribe(td->sub, h, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_BUFFER)
                    return MOQ_ERR_WOULD_BLOCK;   /* keep state; retry next cycle */
            }
            td->cursor++;
        }
    }

    moq_sub_destroy(td->sub);
    receiver_teardown_free(td);
    return MOQ_OK;
}

static void teardown_add_handle(receiver_teardown_t *td, moq_sub_track_t *h)
{
    if (!h) return;
    for (size_t i = 0; i < td->count; i++)
        if (td->handles[i] == h) return;
    /* Distinct handles are distinct slots of the subscriber's track array. */
    assert(td->count < RECEIVER_SUB_MAX_TRACKS);
    if (td->count < RECEIVER_SUB_MAX_TRACKS) td->handles[td->count++] = h;
}

/* Move r->sub and a snapshot of its live subscription handles into the
 * reserved teardown context (r->teardown, taken; the caller nulls r->sub). A
 * subscriber with no live handles yields a zero-handle context (the task just
 * destroys the facade). Runs on the app thread after the hook is detached, so
 * r is no longer touched by the network thread and this read is
 * single-threaded. */
static receiver_teardown_t *receiver_take_teardown(moq_media_receiver_t *r)
{
    receiver_teardown_t *td = r->teardown;
    r->teardown = NULL;
    td->sub = r->sub;
    td->count = 0;
    td->cursor = 0;
    teardown_add_handle(td, r->catalog_sub);
    for (size_t i = 0; i < r->track_count; i++)
        teardown_add_handle(td, r->tracks[i]->sub_track);
    return td;
}

/* -- cfg ---------------------------------------------------------------- */

/* What the v0 prefix holds, and so what the pointer-only initializers set:
 * overflow.policy lies inside it (UNSET, zero, unless a preset names one);
 * companion_profile lies past it and is not written. */
_Static_assert(offsetof(moq_media_receiver_cfg_t, overflow) +
                       offsetof(moq_media_overflow_cfg_t, policy) +
                       sizeof(((moq_media_overflow_cfg_t *)0)->policy) <=
                   MOQ_MEDIA_RECEIVER_CFG_V0_SIZE,
               "overflow.policy is inside the v0 prefix");
_Static_assert(MOQ_MEDIA_OVERFLOW_UNSET == 0, "UNSET is the cleared value");
_Static_assert(offsetof(moq_media_receiver_cfg_t, companion_profile) >=
                   MOQ_MEDIA_RECEIVER_CFG_V0_SIZE,
               "companion_profile lies past the v0 prefix");

/* The pointer-only initializers clear and stamp only the frozen v0 prefix:
 * writing the current sizeof would overflow a caller compiled against the
 * smaller original struct. The plain one leaves overflow.policy UNSET on
 * purpose: forced choice (§6.4). */
void moq_media_receiver_cfg_init(moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_cfg_init_sized(cfg, MOQ_MEDIA_RECEIVER_CFG_V0_SIZE);
}

void moq_media_receiver_cfg_init_live(moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_cfg_init_live_sized(cfg, MOQ_MEDIA_RECEIVER_CFG_V0_SIZE);
}

void moq_media_receiver_cfg_init_flow_control(moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_cfg_init_flow_control_sized(cfg, MOQ_MEDIA_RECEIVER_CFG_V0_SIZE);
}

/* min(cfg_size, sizeof) cleared and stamped; the preset is applied only
 * when that prefix covers overflow.policy. Returns the stamped size, 0 when
 * nothing was written. The caller's object may be an older, smaller record,
 * so every store goes through its bytes at the field's offset, never
 * through an lvalue of the current record type. */
_Static_assert(sizeof(((moq_media_receiver_cfg_t *)0)->struct_size) == sizeof(uint32_t),
               "struct_size is stored as a uint32_t");
_Static_assert(sizeof(((moq_media_overflow_cfg_t *)0)->policy) ==
               sizeof(moq_media_overflow_policy_t),
               "overflow.policy is stored as the policy type");
static size_t receiver_cfg_init_prefix(moq_media_receiver_cfg_t *cfg,
                                       size_t cfg_size,
                                       moq_media_overflow_policy_t policy)
{
    if (!cfg) return 0;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return 0;
    unsigned char *bytes = (unsigned char *)cfg;
    memset(bytes, 0, n);
    const uint32_t stamp = (uint32_t)n;
    memcpy(bytes + offsetof(moq_media_receiver_cfg_t, struct_size), &stamp, sizeof(stamp));
    const size_t policy_at = offsetof(moq_media_receiver_cfg_t, overflow) +
                             offsetof(moq_media_overflow_cfg_t, policy);
    if (n >= policy_at + sizeof(policy))
        memcpy(bytes + policy_at, &policy, sizeof(policy));
    const moq_media_companion_profile_t companion = MOQ_MEDIA_COMPANION_AUTO;
    const size_t companion_at = offsetof(moq_media_receiver_cfg_t, companion_profile);
    if (n >= companion_at + sizeof(companion))
        memcpy(bytes + companion_at, &companion, sizeof(companion));
    return n;
}

void moq_media_receiver_cfg_init_sized(moq_media_receiver_cfg_t *cfg,
                                       size_t cfg_size)
{
    (void)receiver_cfg_init_prefix(cfg, cfg_size, MOQ_MEDIA_OVERFLOW_UNSET);
}

void moq_media_receiver_cfg_init_live_sized(moq_media_receiver_cfg_t *cfg,
                                            size_t cfg_size)
{
    (void)receiver_cfg_init_prefix(cfg, cfg_size,
                                   MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME);
}

void moq_media_receiver_cfg_init_flow_control_sized(
    moq_media_receiver_cfg_t *cfg, size_t cfg_size)
{
    (void)receiver_cfg_init_prefix(cfg, cfg_size,
                                   MOQ_MEDIA_OVERFLOW_FLOW_CONTROL);
}

static moq_media_companion_profile_t receiver_cfg_companion(const void *cfg)
{
    uint32_t size;
    memcpy(&size, cfg, sizeof(size));
    moq_media_companion_profile_t profile = MOQ_MEDIA_COMPANION_AUTO;
    const size_t offset = offsetof(moq_media_receiver_cfg_t, companion_profile);
    if (size >= offset + sizeof(profile))
        memcpy(&profile, (const uint8_t *)cfg + offset, sizeof(profile));
    return profile;
}

/* Read a caller's cfg into a full current struct: the v0 prefix, then each
 * appended field only when struct_size covers all of it (half a field is
 * not a field). Fields left out keep their zero default, except
 * companion_profile, whose default is AUTO. The caller has checked
 * struct_size >= MOQ_MEDIA_RECEIVER_CFG_V0_SIZE. */
static void receiver_cfg_read(const moq_media_receiver_cfg_t *in,
                              moq_media_receiver_cfg_t *out)
{
    uint32_t size;
    memcpy(&size, in, sizeof(size));
    memset(out, 0, sizeof(*out));
    memcpy(out, in, MOQ_MEDIA_RECEIVER_CFG_V0_SIZE);
#define RCFG_COPY(f) do { \
    const size_t offset = offsetof(moq_media_receiver_cfg_t, f); \
    if (size >= offset + sizeof(out->f)) \
        memcpy(&out->f, (const uint8_t *)in + offset, sizeof(out->f)); \
} while (0)
    RCFG_COPY(mpeg2ts_reorder_slots);
    RCFG_COPY(mpeg2ts_reorder_window_us);
    RCFG_COPY(mpeg2ts_strip_prefix);
    RCFG_COPY(mpeg2ts_join_psi);
    RCFG_COPY(resource_auth);
    RCFG_COPY(max_fetches_per_track);
#undef RCFG_COPY
    {
        const size_t end = offsetof(moq_media_receiver_cfg_t,
                                    surface_stream_terminals) + sizeof(bool);
        const size_t at = offsetof(moq_media_receiver_cfg_t, max_stream_terminals);
        if (size >= end)
            memcpy((uint8_t *)out + at, (const uint8_t *)in + at, end - at);
    }
    out->companion_profile = receiver_cfg_companion(in);
    out->struct_size = (uint32_t)sizeof(*out);
}

/* -- Construction -------------------------------------------------------- */

#ifdef MOQ_MEDIA_RECEIVER_TESTING
/* Test-only allocator injection for construction-arithmetic oracles: lets a
 * counting/refusing allocator observe every request receiver_new makes, in
 * order, and refuse a nominated one. NULL restores the default. Never compiled
 * into the shipping library. */
static const moq_alloc_t *g_test_alloc;
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc)
{
    g_test_alloc = alloc;
}
/* Test-only observation of the catalog Joining-FETCH correlation handle: true
 * while the receiver still correlates fetch items to its catalog fetch. */
bool moq_media_receiver_test_catalog_fetch_pending(const moq_media_receiver_t *r);
bool moq_media_receiver_test_catalog_fetch_pending(const moq_media_receiver_t *r)
{
    return r != NULL && r->catalog_fetch != NULL;
}

bool moq_media_receiver_test_catalog_fill_pending(const moq_media_receiver_t *r);
bool moq_media_receiver_test_catalog_fill_pending(const moq_media_receiver_t *r)
{
    return r != NULL && moq_sub_fill_is_valid(r->catalog_fill);
}

bool moq_media_receiver_test_joins_by_fill(const moq_media_receiver_t *r);
bool moq_media_receiver_test_joins_by_fill(const moq_media_receiver_t *r)
{
    return r != NULL && r->joins_by_fill;
}
void moq_media_receiver_test_entry_sizes(size_t *event_entry, size_t *obj_entry);
void moq_media_receiver_test_entry_sizes(size_t *event_entry, size_t *obj_entry)
{
    if (event_entry) *event_entry = sizeof(receiver_event_t);
    if (obj_entry)   *obj_entry   = sizeof(receiver_obj_entry_t);
}
#define RECEIVER_CTOR_ALLOC() (g_test_alloc ? g_test_alloc : moq_alloc_default())
/* Test-only endpoint boundary for the owning constructor: a counting /
 * refusing stand-in for moq_endpoint_connect, so the owning path's ordering
 * (preflight before any connect) is observable without an adapter. NULL
 * restores the real call. */
static moq_result_t (*g_test_connect)(const moq_endpoint_cfg_t *, moq_endpoint_t **, void *);
static void *g_test_connect_ctx;
void moq_media_receiver_test_set_connect(
    moq_result_t (*fn)(const moq_endpoint_cfg_t *, moq_endpoint_t **, void *), void *ctx);
void moq_media_receiver_test_set_connect(
    moq_result_t (*fn)(const moq_endpoint_cfg_t *, moq_endpoint_t **, void *), void *ctx)
{
    g_test_connect = fn;
    g_test_connect_ctx = ctx;
}
#define RECEIVER_CONNECT(cfg, out) \
    (g_test_connect ? g_test_connect((cfg), (out), g_test_connect_ctx) \
                    : moq_endpoint_connect((cfg), (out)))
/* Test-only pre-traversal observation for configuration validation: called
 * with the namespace part count immediately before the parts are walked.
 * Returning false stops validation with MOQ_ERR_INVAL, which keeps a fixture
 * with an intrinsically unrepresentable count bounded (no element is read)
 * while still recording that the walk was about to start. NULL disables. */
static bool (*g_test_parts_walk)(size_t count, void *ctx);
static void *g_test_parts_walk_ctx;
void moq_media_receiver_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx);
void moq_media_receiver_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx)
{
    g_test_parts_walk = fn;
    g_test_parts_walk_ctx = ctx;
}
#define CFG_PARTS_WALK_HOOK(count) \
    do { if (g_test_parts_walk && !g_test_parts_walk((count), g_test_parts_walk_ctx)) \
             return MOQ_ERR_INVAL; } while (0)
#else
#define RECEIVER_CTOR_ALLOC() moq_alloc_default()
#define RECEIVER_CONNECT(cfg, out) moq_endpoint_connect((cfg), (out))
#define CFG_PARTS_WALK_HOOK(count) ((void)0)
#endif


static moq_result_t receiver_validate_cfg(const moq_media_receiver_cfg_t *cfg)
{
    if (!cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MOQ_MEDIA_RECEIVER_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_companion_profile_t companion = receiver_cfg_companion(cfg);
    if (companion != MOQ_MEDIA_COMPANION_AUTO &&
        companion != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 &&
        !receiver_companion_d22(companion))
        return MOQ_ERR_INVAL;
    if (receiver_companion_d22(companion) &&
        cfg->time_mode == MOQ_MEDIA_TIME_SHARED_EPOCH)
        return MOQ_ERR_UNSUPPORTED;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts)
        return MOQ_ERR_INVAL;
    /* count * sizeof(moq_bytes_t) must be representable before a single part
     * is read: an intrinsically impossible count is refused, not traversed. */
    if (cfg->namespace_.count > SIZE_MAX / sizeof(moq_bytes_t))
        return MOQ_ERR_INVAL;
    CFG_PARTS_WALK_HOOK(cfg->namespace_.count);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        if (cfg->namespace_.parts[i].len == 0 ||
            !cfg->namespace_.parts[i].data)
            return MOQ_ERR_INVAL;
    }
    if (cfg->catalog_track.len > 0 && !cfg->catalog_track.data)
        return MOQ_ERR_INVAL;
    if (cfg->time_mode != MOQ_MEDIA_TIME_RAW &&
        cfg->time_mode != MOQ_MEDIA_TIME_SHARED_EPOCH)
        return MOQ_ERR_INVAL;
    switch (cfg->overflow.policy) {
    case MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME:
    case MOQ_MEDIA_OVERFLOW_DROP_GROUP:
    case MOQ_MEDIA_OVERFLOW_FLOW_CONTROL:
        break;
    default:
        return MOQ_ERR_INVAL;   /* UNSET: the overflow choice is forced */
    }
    return MOQ_OK;
}

static void receiver_free(moq_media_receiver_t *r)
{
    receiver_release_teardown(r);
    moq_media_auth_operation_clear(&r->catalog_auth, &r->alloc);
    moq_media_auth_operation_clear(&r->fetch_auth, &r->alloc);
    moq_media_auth_source_clear(&r->auth_source, &r->alloc);
    /* Tracks are freed before the queue: unqueueing must not count. */
    r->st_enabled = false;
    if (r->st)
        r->alloc.free(r->st, r->st_cap * sizeof(receiver_st_entry_t), r->alloc.ctx);
    receiver_bootstrap_discard(r);
    for (size_t i = 0; i < r->track_count; i++)
        receiver_track_free(r, r->tracks[i]);
    if (r->tracks)
        r->alloc.free(r->tracks, r->track_cap * sizeof(moq_media_track_t *),
                      r->alloc.ctx);
    if (r->has_effective)
        moq_msf_catalog_cleanup(&r->alloc, &r->effective);
    for (size_t i = 0; i < r->cp_snap_count; i++)
        moq_msf_catalog_cleanup(&r->alloc, &r->cp_snaps[i]);
    if (r->cp_snaps)
        r->alloc.free(r->cp_snaps, r->cp_snap_cap * sizeof(moq_msf_catalog_t),
                      r->alloc.ctx);
    if (r->events)
        r->alloc.free(r->events, r->ev_cap * sizeof(receiver_event_t),
                      r->alloc.ctx);
    if (r->objs) {
        while (r->obj_head != r->obj_tail) {
            receiver_obj_entry_t *e = &r->objs[r->obj_head % r->ring_cap];
            obj_unqueue_locked(r, e); /* Detached: no concurrency remains. */
            obj_entry_release(r, &e->obj);
            r->obj_head++;
        }
        r->alloc.free(r->objs, r->ring_cap * sizeof(receiver_obj_entry_t),
                      r->alloc.ctx);
    }
    if (r->saps)
        r->alloc.free(r->saps, r->sap_cap * sizeof(moq_media_sap_record_t),
                      r->alloc.ctx);
    if (r->mts)
        r->alloc.free(r->mts, r->mt_cap * sizeof(moq_media_timeline_record_t),
                      r->alloc.ctx);
    if (r->ns_parts)
        r->alloc.free(r->ns_parts,
                      r->namespace_.count * sizeof(moq_bytes_t),
                      r->alloc.ctx);
    if (r->ns_data)
        r->alloc.free(r->ns_data, r->ns_data_size, r->alloc.ctx);
    pthread_mutex_destroy(&r->mu);
    r->alloc.free(r, sizeof(*r), r->alloc.ctx);
}


/* Every size receiver_new allocates, derived once and checked before the
 * first allocation. A configuration whose derived capacity or byte count is
 * not representable is refused with MOQ_ERR_INVAL and has no effect; the
 * allocations below then use exactly these values, so validation and
 * allocation cannot drift. Bounds are written as division/subtraction
 * comparisons so the check itself never overflows. */
typedef struct {
    moq_bytes_t catalog;         /* configured or the MSF default name */
    size_t      ns_parts_bytes;  /* namespace_.count * sizeof(moq_bytes_t) */
    size_t      ns_bytes;        /* sum of the part lengths */
    size_t      ns_data_bytes;   /* ns_bytes + catalog.len */
    uint32_t    ev_cap;
    size_t      events_bytes;
    uint32_t    obj_cap;
    uint32_t    ring_cap;
    uint64_t    byte_cap;        /* FLOW_CONTROL ceiling byte_cap + byte_cap/2 must fit */
    size_t      objs_bytes;
    uint32_t    sap_cap;
    size_t      saps_bytes;
    uint32_t    mt_cap;
    size_t      mts_bytes;
    uint32_t    st_cap;          /* 0 when the lane is off */
    size_t      st_bytes;
} receiver_sizes_t;

/* count * elem fits in size_t. Taking the count as uint64_t keeps the check
 * meaningful on a 32-bit size_t without a tautological comparison on LP64. */
static bool receiver_product_fits(uint64_t count, size_t elem)
{
    return count <= (uint64_t)(SIZE_MAX / elem);
}

static moq_result_t receiver_derive_sizes(const moq_media_receiver_cfg_t *cfg,
                                          receiver_sizes_t *s)
{
    memset(s, 0, sizeof(*s));
    s->catalog = cfg->catalog_track;
    if (s->catalog.len == 0)
        s->catalog = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            MOQ_MSF_CATALOG_TRACK_NAME_LEN };

    if (cfg->namespace_.count > SIZE_MAX / sizeof(moq_bytes_t))
        return MOQ_ERR_INVAL;   /* already refused by receiver_validate_cfg */
    s->ns_parts_bytes = cfg->namespace_.count * sizeof(moq_bytes_t);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        if (cfg->namespace_.parts[i].len > SIZE_MAX - s->ns_bytes)
            return MOQ_ERR_INVAL;
        s->ns_bytes += cfg->namespace_.parts[i].len;
    }
    if (s->catalog.len > SIZE_MAX - s->ns_bytes) return MOQ_ERR_INVAL;
    s->ns_data_bytes = s->ns_bytes + s->catalog.len;

    s->ev_cap = cfg->max_track_events ? cfg->max_track_events
                                      : RECEIVER_DEFAULT_TRACK_EVENTS;
    if (!receiver_product_fits(s->ev_cap, sizeof(receiver_event_t))) return MOQ_ERR_INVAL;
    s->events_bytes = (size_t)s->ev_cap * sizeof(receiver_event_t);

    s->obj_cap = cfg->overflow.max_objects ? cfg->overflow.max_objects
                                           : RECEIVER_DEFAULT_MAX_OBJECTS;
    s->byte_cap = cfg->overflow.max_bytes ? cfg->overflow.max_bytes
                                          : RECEIVER_DEFAULT_MAX_BYTES;
    if (cfg->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL) {
        /* FLOW_CONTROL absorbs in-flight overshoot past the threshold (up to
         * the soft ceiling); the physical ring must hold it, and the byte
         * ceiling byte_cap + byte_cap/2 is computed at service time. */
        uint32_t half = s->obj_cap / 2;
        if (s->obj_cap > UINT32_MAX - half - 1) return MOQ_ERR_INVAL;
        s->ring_cap = s->obj_cap + half + 1;
        if (s->byte_cap > UINT64_MAX - s->byte_cap / 2) return MOQ_ERR_INVAL;
    } else {
        s->ring_cap = s->obj_cap;
    }
    if (!receiver_product_fits(s->ring_cap, sizeof(receiver_obj_entry_t))) return MOQ_ERR_INVAL;
    s->objs_bytes = (size_t)s->ring_cap * sizeof(receiver_obj_entry_t);

    s->sap_cap = RECEIVER_DEFAULT_SAP_RECORDS;
    if (!receiver_product_fits(s->sap_cap, sizeof(moq_media_sap_record_t))) return MOQ_ERR_INVAL;
    s->saps_bytes = (size_t)s->sap_cap * sizeof(moq_media_sap_record_t);
    s->mt_cap = RECEIVER_DEFAULT_MT_RECORDS;
    if (!receiver_product_fits(s->mt_cap, sizeof(moq_media_timeline_record_t))) return MOQ_ERR_INVAL;
    s->mts_bytes = (size_t)s->mt_cap * sizeof(moq_media_timeline_record_t);
    if (cfg->surface_stream_terminals) {
        s->st_cap = cfg->max_stream_terminals ? cfg->max_stream_terminals
                                              : RECEIVER_DEFAULT_STREAM_TERMINALS;
        if (!receiver_product_fits(s->st_cap, sizeof(receiver_st_entry_t)))
            return MOQ_ERR_INVAL;
        s->st_bytes = (size_t)s->st_cap * sizeof(receiver_st_entry_t);
    }
    return MOQ_OK;
}

/* The ONE preflight every constructor runs before any allocating, connecting
 * or attaching effect: basic configuration validation (including the
 * namespace-count bound, before any part is read) and complete size
 * derivation. The checked sizes are then passed into receiver_new, never
 * recomputed. */
static moq_result_t receiver_prepare(const moq_media_receiver_cfg_t *cfg,
                                     moq_media_receiver_cfg_t *local,
                                     receiver_sizes_t *sz)
{
    /* A caller built against an older header passes a smaller struct: any
     * size from the v0 prefix up is read, field by field. */
    if (!cfg || cfg->struct_size < MOQ_MEDIA_RECEIVER_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    receiver_cfg_read(cfg, local);
    moq_result_t rc = receiver_validate_cfg(local);
    if (rc < 0) return rc;
    return receiver_derive_sizes(local, sz);
}

static moq_result_t receiver_new(moq_endpoint_t *ep, bool owns,
                                 const moq_media_receiver_cfg_t *cfg,
                                 const receiver_sizes_t *szp,
                                 const moq_alloc_t *alloc,
                                 moq_media_auth_source_t *auth,
                                 moq_media_receiver_t **out)
{
    const receiver_sizes_t sz = *szp;
    moq_media_receiver_t *r = (moq_media_receiver_t *)alloc->alloc(
        sizeof(*r), alloc->ctx);
    if (!r) return MOQ_ERR_NOMEM;
    memset(r, 0, sizeof(*r));
    r->alloc = *alloc;
    r->auth_source = *auth;
    memset(auth, 0, sizeof(*auth));
    r->ep = ep;
    r->owns_endpoint = owns;
    r->auto_subscribe = cfg->auto_subscribe;
    r->time_mode = cfg->time_mode;
    r->companion_profile = receiver_cfg_companion(cfg);
    r->companion_auto = r->companion_profile == MOQ_MEDIA_COMPANION_AUTO;
    r->overflow = cfg->overflow;
    r->max_stable_handles = RECEIVER_DEFAULT_MAX_STABLE_HANDLES;
    r->max_cp_snaps       = RECEIVER_DEFAULT_MAX_CP_SNAPS;
    r->max_retained_bytes = RECEIVER_DEFAULT_MAX_RETAINED_BYTES;
    r->ts_slots = cfg->mpeg2ts_reorder_slots ? cfg->mpeg2ts_reorder_slots
                                             : RECEIVER_DEFAULT_TS_SLOTS;
    r->ts_window_us = cfg->mpeg2ts_reorder_window_us
                          ? cfg->mpeg2ts_reorder_window_us
                          : RECEIVER_DEFAULT_TS_WINDOW_US;
    r->ts_strip_prefix = cfg->mpeg2ts_strip_prefix;
    r->ts_join_psi = cfg->mpeg2ts_join_psi;
    r->max_fetches_per_track = cfg->max_fetches_per_track
        ? cfg->max_fetches_per_track : RECEIVER_DEFAULT_FETCHES_PER_TRACK;
    r->ts_deadline_us = UINT64_MAX;
    pthread_mutex_init(&r->mu, NULL);

    /* Deep-copy the namespace and catalog track name into one buffer:
     * [ns part bytes...][catalog name bytes]. */
    r->ns_parts = (moq_bytes_t *)r->alloc.alloc(sz.ns_parts_bytes, r->alloc.ctx);
    r->ns_data = (uint8_t *)r->alloc.alloc(sz.ns_data_bytes, r->alloc.ctx);
    if (!r->ns_parts || !r->ns_data) {
        if (r->ns_parts)
            r->alloc.free(r->ns_parts, sz.ns_parts_bytes, r->alloc.ctx);
        if (r->ns_data)
            r->alloc.free(r->ns_data, sz.ns_data_bytes, r->alloc.ctx);
        r->ns_parts = NULL;
        r->ns_data = NULL;
        moq_media_auth_source_clear(&r->auth_source, &r->alloc);
        pthread_mutex_destroy(&r->mu);
        r->alloc.free(r, sizeof(*r), r->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    size_t off = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        memcpy(r->ns_data + off, cfg->namespace_.parts[i].data,
               cfg->namespace_.parts[i].len);
        r->ns_parts[i] = (moq_bytes_t){ r->ns_data + off,
                                        cfg->namespace_.parts[i].len };
        off += cfg->namespace_.parts[i].len;
    }
    memcpy(r->ns_data + off, sz.catalog.data, sz.catalog.len);
    r->catalog_name = r->ns_data + off;
    r->catalog_name_len = sz.catalog.len;
    r->namespace_ = (moq_namespace_t){ r->ns_parts, cfg->namespace_.count };
    /* receiver_free() releases ns_data with its full size. */
    r->ns_data_size = sz.ns_data_bytes;

    r->ev_cap = sz.ev_cap;
    r->events = (receiver_event_t *)r->alloc.alloc(sz.events_bytes, r->alloc.ctx);
    if (!r->events) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }

    r->obj_cap = sz.obj_cap;
    r->byte_cap = sz.byte_cap;
    r->ring_cap = sz.ring_cap;
    r->objs = (receiver_obj_entry_t *)r->alloc.alloc(sz.objs_bytes, r->alloc.ctx);
    if (!r->objs) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->objs, 0, sz.objs_bytes);

    r->sap_cap = sz.sap_cap;
    r->saps = (moq_media_sap_record_t *)r->alloc.alloc(sz.saps_bytes, r->alloc.ctx);
    if (!r->saps) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->saps, 0, sz.saps_bytes);

    r->mt_cap = sz.mt_cap;
    r->mts = (moq_media_timeline_record_t *)r->alloc.alloc(sz.mts_bytes, r->alloc.ctx);
    if (!r->mts) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->mts, 0, sz.mts_bytes);
    if (sz.st_cap) {
        r->st = (receiver_st_entry_t *)r->alloc.alloc(sz.st_bytes, r->alloc.ctx);
        if (!r->st) {
            receiver_free(r);
            return MOQ_ERR_NOMEM;
        }
        memset(r->st, 0, sz.st_bytes);
        r->st_cap = sz.st_cap;
        r->st_enabled = true;
    }
    r->stats.struct_size = (uint32_t)sizeof(r->stats);

    /* Production always passes a real endpoint. The MOQ_MEDIA_RECEIVER_TESTING
     * seam (below) calls receiver_new with ep == NULL to build a fully
     * initialized receiver with no endpoint, so the scripted-peer harness can
     * drive receiver_hook against a session it controls. The guard is a no-op
     * in shipping builds (ep is never NULL there). */
    if (ep && !receiver_reserve_teardown(r)) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    if (ep) {
        /* The receiver's only time-based deadline is the MPEG2TS reorder
         * window's; otherwise it wakes on transport/event activity. */
        moq_endpoint_hook_ops_t ops = {
            .pump = receiver_hook,
            .next_deadline_us = receiver_next_deadline_us,
            .reads_events = receiver_reads_events,
        };
        moq_result_t arc = moq_endpoint_attach_hook(
            ep, MOQ_ENDPOINT_HOOK_RECEIVER, &ops, r);
        if (arc < 0) {
            receiver_free(r);
            return arc;
        }
    }

    *out = r;
    return MOQ_OK;
}

moq_result_t moq_media_receiver_attach(moq_endpoint_t *ep,
                                       const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    if (!ep || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    receiver_sizes_t sz;
    moq_media_receiver_cfg_t lc;
    moq_result_t rc = receiver_prepare(cfg, &lc, &sz);
    if (rc < 0) return rc;
    if (lc.endpoint != NULL) return MOQ_ERR_INVAL;  /* attach borrows */
    moq_alloc_t alloc = *RECEIVER_CTOR_ALLOC();
    moq_media_auth_source_t auth = {0};
    rc = moq_media_auth_source_copy(&auth, lc.resource_auth, &alloc);
    if (rc != MOQ_OK) return rc;
    rc = receiver_new(ep, false, &lc, &sz, &alloc, &auth, out);
    moq_media_auth_source_clear(&auth, &alloc);
    return rc;
}

moq_result_t moq_media_receiver_create(const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    /* Everything that can be refused without effects is refused here, before
     * the owned endpoint is connected. */
    receiver_sizes_t sz;
    moq_media_receiver_cfg_t lc;
    moq_result_t rc = receiver_prepare(cfg, &lc, &sz);
    if (rc < 0) return rc;
    if (lc.endpoint == NULL) return MOQ_ERR_INVAL;  /* create owns */

    moq_alloc_t alloc = *RECEIVER_CTOR_ALLOC();
    moq_media_auth_source_t auth = {0};
    rc = moq_media_auth_source_copy(&auth, lc.resource_auth, &alloc);
    if (rc != MOQ_OK) return rc;
    moq_endpoint_t *ep = NULL;
    rc = RECEIVER_CONNECT(lc.endpoint, &ep);
    if (rc < 0) {
        moq_media_auth_source_clear(&auth, &alloc);
        return rc;
    }

    rc = receiver_new(ep, true, &lc, &sz, &alloc, &auth, out);
    moq_media_auth_source_clear(&auth, &alloc);
    if (rc < 0) {
        (void)moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return rc;
    }
    return MOQ_OK;
}

void moq_media_receiver_destroy(moq_media_receiver_t *r)
{
    if (!r) return;

    moq_endpoint_detach_hook(r->ep, MOQ_ENDPOINT_HOOK_RECEIVER, r);

    /* Tear the subscriber down on the network thread (receiver_teardown_t):
     * the reserved context takes it and a snapshot of its live handles, and
     * the reserved node carries the task that cancels each (UNSUBSCRIBE)
     * before destroying the facade. Nothing here allocates, so nothing can be
     * refused for want of memory. The post is refused only once stop() has
     * joined the network thread (MOQ_ERR_CLOSED): no thread then runs the
     * session, the transport or this subscriber, and every buffer the
     * subscriber holds is from the endpoint's allocator (receiver_teardown_t),
     * so the NULL-session teardown runs here, with nothing live to cancel. A
     * receiver with no endpoint (test builds only) has no network thread at
     * all. */
    if (r->sub) {
        if (!r->ep) {
            moq_sub_destroy(r->sub);
        } else {
            assert(r->teardown && r->teardown_slot);  /* receiver_new reserved them */
            receiver_teardown_t *td = receiver_take_teardown(r);
            if (moq_endpoint_internal_post_reserved(r->ep, r->teardown_slot,
                                                    receiver_destroy_sub_task, td) == MOQ_OK)
                r->teardown_slot = NULL;      /* the endpoint's now */
            else
                (void)receiver_destroy_sub_task(r->ep, NULL, 0, td);
        }
        r->sub = NULL;
    }
    receiver_release_teardown(r);

    if (r->owns_endpoint) {
        (void)moq_endpoint_stop(r->ep);
        moq_endpoint_destroy(r->ep);
    }
    receiver_free(r);
}

/* -- App-thread surface --------------------------------------------------- */

static bool receiver_terminal(const moq_media_receiver_t *r)
{
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    pthread_mutex_unlock(&mr->mu);
    return fatal || moq_endpoint_is_closed(r->ep);
}

bool moq_media_receiver_track_removed_by_completion_internal(
    moq_media_receiver_t *r, const moq_media_track_t *track)
{
    if (!r || !track) return false;
    bool by_completion = false;
    pthread_mutex_lock(&r->mu);
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i] == track) {
            by_completion = r->tracks[i]->removed &&
                            r->tracks[i]->removed_by_completion;
            break;
        }
    pthread_mutex_unlock(&r->mu);
    return by_completion;
}

size_t moq_media_receiver_track_input_internal(moq_media_receiver_t *r,
                                              const moq_media_track_t *track)
{
    if (!r || !track) return 0;
    size_t n = 0;
    pthread_mutex_lock(&r->mu);
    for (uint32_t i = r->obj_head; i != r->obj_tail; i++) {
        const receiver_obj_entry_t *e = &r->objs[i % r->ring_cap];
        if (!e->catalog && e->obj.track == track) n++;
    }
    if (r->st_enabled)
        for (uint32_t i = r->st_head; i != r->st_tail; i++)
            if (r->st[i % r->st_cap].rec.track == track) n++;
    pthread_mutex_unlock(&r->mu);
    return n;
}

bool moq_media_receiver_has_input_internal(moq_media_receiver_t *r)
{
    if (!r) return false;
    pthread_mutex_lock(&r->mu);
    bool have = (r->ev_tail != r->ev_head) ||
                (r->obj_tail - r->obj_head != r->catalog_objects_queued) ||
                (r->sap_tail != r->sap_head) || (r->mt_tail != r->mt_head) ||
                (r->parse_drop_pending_count > 0) ||
                (r->local_failure_pending_count > 0) ||
                (r->fill_ready_count > 0) || st_head_pollable_locked(r);
    pthread_mutex_unlock(&r->mu);
    return have;
}

moq_result_t moq_media_receiver_wait(moq_media_receiver_t *r,
                                     uint64_t timeout_us)
{
    if (!r) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&r->mu);
    bool have = (r->ev_tail != r->ev_head) ||
                (r->obj_tail - r->obj_head != r->catalog_objects_queued) ||
                (r->sap_tail != r->sap_head) || (r->mt_tail != r->mt_head) ||
                (r->parse_drop_pending_count > 0) ||
                (r->local_failure_pending_count > 0) ||
                (r->fill_ready_count > 0) || st_head_pollable_locked(r);
    pthread_mutex_unlock(&r->mu);
    if (have) return MOQ_OK;
    if (receiver_terminal(r)) return MOQ_ERR_CLOSED;

    moq_result_t rc = moq_endpoint_wait(r->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;

    pthread_mutex_lock(&r->mu);
    have = (r->ev_tail != r->ev_head) ||
           (r->obj_tail - r->obj_head != r->catalog_objects_queued) ||
           (r->sap_tail != r->sap_head) || (r->mt_tail != r->mt_head) ||
           (r->parse_drop_pending_count > 0) ||
           (r->local_failure_pending_count > 0) ||
           (r->fill_ready_count > 0) || st_head_pollable_locked(r);
    pthread_mutex_unlock(&r->mu);
    if (have) return MOQ_OK;
    if (receiver_terminal(r)) return MOQ_ERR_CLOSED;
    return rc < 0 ? rc : MOQ_OK;
}

static moq_result_t receiver_poll_track(moq_media_receiver_t *r,
                                        moq_media_track_event_t *ev,
                                        size_t ev_size);

moq_result_t moq_media_receiver_poll_track(moq_media_receiver_t *r,
                                           moq_media_track_event_t *ev,
                                           size_t ev_size)
{
    moq_result_t rc = receiver_poll_track(r, ev, ev_size);
    if (rc == MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->track_events_polled++;
        pthread_mutex_unlock(&r->mu);
    }
    return rc;
}

static moq_result_t receiver_poll_track(moq_media_receiver_t *r,
                                        moq_media_track_event_t *ev,
                                        size_t ev_size)
{
    /* Require the frozen v0 floor (not sizeof(current)), so an older caller is
     * still served by a future library; copy only n = min(caller, current)
     * bytes so a newer library never writes past an older caller's struct. */
    if (!r || !ev || ev_size < MEDIA_TRACK_EVENT_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = ev_size < sizeof(*ev) ? ev_size : sizeof(*ev);
    memset(ev, 0, n);

    pthread_mutex_lock(&r->mu);
    /* A READY fill or fetch record whose place has come -- every event
     * queued before it became ready is read -- goes ahead of the next queued
     * event; among those, the one ready first. */
    receiver_fill_dom_t *ready = NULL;
    if (r->fill_ready_count > 0) {
        for (size_t i = 0; i < RECEIVER_DOMS; i++) {
            receiver_fill_dom_t *d = &r->fill_doms[i];
            if (d->state != DOM_READY ||
                (int32_t)(r->ev_head - d->ready_seq) < 0) continue;
            if (!ready || d->ready_order < ready->ready_order) ready = d;
        }
    }
    if (ready) {
        moq_media_track_event_t full;
        memset(&full, 0, sizeof(full));
        full.struct_size = (uint32_t)n;
        full.kind = ready->fetch ? MOQ_MEDIA_TRACK_FETCH_TERMINAL
                                 : MOQ_MEDIA_TRACK_FILL_TERMINAL;
        full.track = ready->track;
        full.desc = ready->track->desc_cur;
        full.config_generation = ready->track->config_gen;
        full.initiating_request_id = ready->initiating_request_id;
        full.fill_reason = ready->reason;
        full.fill_error_domain = ready->error_domain;
        full.fill_error_code = ready->error_code;
        full.local_result = ready->local_result;   /* zero for a fill */
        if (n >= MEDIA_TRACK_EVENT_FILL_LOSS_END && ready->pending.causes) {
            full.fill_unreported_loss_causes = ready->pending.causes;
            full.fill_unreported_loss_group = ready->pending.group;
            full.fill_unreported_loss_object = ready->pending.object;
        }
        memset(ready, 0, sizeof(*ready));
        r->fill_ready_count--;
        bool wake_fill = r->fill_blocked;
        pthread_mutex_unlock(&r->mu);
        if (wake_fill) moq_endpoint_wake(r->ep);
        memcpy(ev, &full, n);
        return MOQ_OK;
    }
    if (r->ev_tail != r->ev_head) {
        receiver_event_t e = r->events[r->ev_head % r->ev_cap];
        r->ev_head++;
        /* Events queued without a description (fill and delivery records)
         * name the track's current one. */
        if (!e.desc && e.track) {
            e.desc = e.track->desc_cur;
            e.config_generation = e.track->config_gen;
        }
        /* Polled at any output size: the command's receipt is consumed. */
        if (e.kind == MOQ_MEDIA_TRACK_FILL_CREATED && e.track &&
            e.track->cmd_state == FILL_CMD_RECEIPT &&
            e.track->cmd_seq == e.fill_cmd_seq)
            e.track->cmd_state = FILL_CMD_NONE;
        /* Likewise a fetch command's receipt: its FETCH_CREATED, or the
         * NOT_SENT FETCH_TERMINAL of a refused one. */
        if ((e.kind == MOQ_MEDIA_TRACK_FETCH_CREATED ||
             e.kind == MOQ_MEDIA_TRACK_FETCH_TERMINAL) && e.track &&
            e.fetch_cmd_seq != 0 &&
            e.track->fetch_cmd_state == FETCH_CMD_RECEIPT &&
            e.track->fetch_cmd_seq == e.fetch_cmd_seq)
            e.track->fetch_cmd_state = FETCH_CMD_NONE;
        bool wake_fill = r->fill_blocked;
        pthread_mutex_unlock(&r->mu);
        if (wake_fill) moq_endpoint_wake(r->ep);

        moq_media_track_event_t full;
        memset(&full, 0, sizeof(full));
        full.struct_size = (uint32_t)n;
        full.kind = e.kind;
        full.track = e.track;
        full.desc = e.desc;
        full.config_generation = e.config_generation;
        /* the UPDATE_OK scalars come from the QUEUE ENTRY's per-event
         * copies, never from current track state. Zero for other kinds. */
        full.has_largest = e.has_largest;
        full.largest_group = e.largest_group;
        full.largest_object = e.largest_object;
        full.has_expires = e.has_expires;
        full.expires_ms = e.expires_ms;
        full.initiating_request_id = e.initiating_request_id;
        full.fill_reason = e.fill_reason;
        full.fill_error_domain = e.fill_error_domain;
        full.fill_error_code = e.fill_error_code;
        full.local_result = e.local_result;
        full.delivery_origin = e.delivery_origin;
        full.delivery_initiating_request_id = e.delivery_initiating_request_id;
        if (n >= MEDIA_TRACK_EVENT_DONE_END) {
            full.has_done_status = e.has_done_status;
            full.done_status = e.done_status;
        }
        if (n >= MEDIA_TRACK_EVENT_REPLACED_END) {
            full.replaces = e.replaces;
            full.replaced_by = e.replaced_by;
        }
        memcpy(ev, &full, n);
        return MOQ_OK;
    }

    /* Discovery stays ahead of this allocation-free terminal observation. */
    if (r->local_failure_pending_count > 0) {
        for (size_t i = 0; i < r->track_count; i++) {
            moq_media_track_t *t = r->tracks[i];
            if (!t->local_failure_pending) continue;
            t->local_failure_pending = false;
            r->local_failure_pending_count--;
            moq_result_t result = t->local_result;
            moq_media_track_event_kind_t kind = t->local_failure_kind;
            bool delivery = t->delivery_failed;
            const moq_media_track_desc_t *desc = t->desc_cur;
            uint32_t generation = t->config_gen;
            pthread_mutex_unlock(&r->mu);

            moq_media_track_event_t full;
            memset(&full, 0, sizeof(full));
            full.struct_size = (uint32_t)n;
            full.kind = kind;
            full.track = t;
            full.desc = desc;
            full.config_generation = generation;
            full.local_result = (int32_t)result;
            if (delivery) full.delivery_origin = MOQ_MEDIA_DELIVERY_LIVE;
            memcpy(ev, &full, n);
            return MOQ_OK;
        }
    }

    /* Ordinary discovery/update events are drained first (so a consumer always
     * sees TRACK_ADDED before any later event for a handle); only once the queue
     * is empty do we synthesize a coalesced parse-drop diagnostic from pending
     * per-track state. This never touches the ordinary event queue, so a
     * malformed-object flood cannot overflow it or terminalize the receiver. */
    if (r->parse_drop_pending_count > 0) {
        moq_media_track_t *t = NULL;
        for (size_t i = 0; i < r->track_count; i++) {
            if (r->tracks[i]->parse_drop_pending) { t = r->tracks[i]; break; }
        }
        if (t) {
            /* Snapshot total/delta/class under r->mu, clear the pending flag,
             * and advance parse_drops_reported; the public event is filled from
             * the snapshot, never re-read from the track after unlock. */
            uint64_t total = t->parse_drops;
            uint64_t delta = t->parse_drops - t->parse_drops_reported;
            moq_media_parse_drop_class_t cls = t->parse_drop_last_class;
            t->parse_drops_reported = t->parse_drops;
            t->parse_drop_pending = false;
            r->parse_drop_pending_count--;
            const moq_media_track_desc_t *desc = t->desc_cur;
            uint32_t generation = t->config_gen;
            pthread_mutex_unlock(&r->mu);

            moq_media_track_event_t full;
            memset(&full, 0, sizeof(full));
            full.struct_size = (uint32_t)n;
            full.kind = MOQ_MEDIA_TRACK_PARSE_DROP;
            full.track = t;
            full.desc = desc;
            full.config_generation = generation;
            full.parse_drop_class = cls;
            full.parse_drops_total = total;
            full.parse_drops_delta = delta;
            memcpy(ev, &full, n);
            return MOQ_OK;
        }
    }

    bool fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (fatal || moq_endpoint_is_closed(r->ep))
        return MOQ_ERR_CLOSED;
    return MOQ_DONE;
}

const moq_media_track_desc_t *moq_media_track_desc_get(
    const moq_media_track_t *track)
{
    if (!track) return NULL;
    /* The release store that published a generation orders every write of
     * its description before this load. */
    return atomic_load_explicit(
        (const moq_media_track_desc_t *_Atomic *)(uintptr_t)&track->desc_pub,
        memory_order_acquire);
}

moq_result_t moq_media_receiver_track_desc_copy(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_desc_t *out, size_t out_size)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_TRACK_DESC_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);

    /* Ownership is a pointer comparison against the receiver's handle table
     * (appended under r->mu, retained until receiver_free), so an unowned
     * pointer is never dereferenced. The copy runs under the same mutex the
     * live->VOD writer holds, so the mutable triple is read whole. */
    pthread_mutex_t *mu = (pthread_mutex_t *)&r->mu;
    pthread_mutex_lock(mu);
    PUB_PHASE_HOOK(r, MOQ_MR_DESC_READER_PRE);
    bool owned = false;
    for (size_t i = 0; i < r->track_count; i++) {
        if (r->tracks[i] == track) { owned = true; break; }
    }
    if (owned) {
        memcpy(out, track->desc_cur, n);
        out->struct_size = (uint32_t)n;
    }
    PUB_PHASE_HOOK(r, MOQ_MR_DESC_READER_POST);
    pthread_mutex_unlock(mu);
    return owned ? MOQ_OK : MOQ_ERR_INVAL;
}

const moq_cmsf_content_protection_t *
moq_media_receiver_find_content_protection(
    const moq_media_receiver_t *r, moq_bytes_t ref_id)
{
    if (!r) return NULL;                  /* null receiver: no probe, no lock */
    /* Hold r->mu across the whole effective-generation selection and traversal,
     * the same mutex receiver_adopt holds while publishing. The
     * returned pointer stays valid past the unlock only because a later commit
     * retains any previous effective catalog that carried contentProtections
     * (cp_snaps) -- the retained-snapshot lifetime contract is unchanged. */
    pthread_mutex_t *mu = (pthread_mutex_t *)&r->mu;
    pthread_mutex_lock(mu);
    /* Reader PRE: after the null check + lock, before reading has_effective /
     * selecting the effective generation. */
    PUB_PHASE_HOOK(r, MOQ_MR_READER_PRE);
    /* Both outcomes -- the empty (no effective catalog) selection and a real
     * traversal -- run inside the lock, so the selection is computed
     * unconditionally and POST fires for both. */
    const moq_cmsf_content_protection_t *cp =
        r->has_effective
            ? moq_msf_catalog_find_content_protection(&r->effective, ref_id)
            : NULL;
    /* Reader POST: after the traversal result is selected, before the unlock. */
    PUB_PHASE_HOOK(r, MOQ_MR_READER_POST);
    pthread_mutex_unlock(mu);
    return cp;
}

/* -- Per-track subscription control (app thread) ---------------------- *
 * These only validate + record desired state under r->mu, then nudge the
 * network thread with the internal non-allocating moq_endpoint_wake() (it
 * queues no task and cannot fail, so the command is atomic -- no receiver
 * pointer is handed to a deferred task either). The hook reconciles desired
 * state against the actual subscriptions, so no facade call ever runs
 * off-thread. */

void moq_media_receiver_track_subscribe_cfg_init(
    moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    moq_media_receiver_track_subscribe_cfg_init_sized(cfg,
        MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE);
}

void moq_media_receiver_track_subscribe_cfg_init_sized(
    moq_media_receiver_track_subscribe_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    unsigned char *storage = (unsigned char *)cfg;
    memset(storage, 0, n);
    /* The original pointer-free config may have weaker alignment. */
    uint32_t stamp = (uint32_t)n;
    memcpy(storage, &stamp, sizeof(stamp));
}

/* The app-thread boundary copies semantic values, never protocol bytes or
 * borrowed pointers. Wire representation remains the core's responsibility. */
static moq_result_t receiver_copy_location(const moq_location_filter_cfg_t *in,
                                         moq_location_filter_cfg_t *out)
{
    if (in->struct_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE)
        return MOQ_ERR_ABI_MISMATCH;
    moq_location_filter_cfg_init(out);
    out->kind = in->kind;
    switch (in->kind) {
    case MOQ_LOCATION_FILTER_NONE:
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        break;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        out->relative_group_offset = in->relative_group_offset;
        break;
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        out->start_group = in->start_group;
        out->start_object = in->start_object;
        break;
    case MOQ_LOCATION_FILTER_GROUP_END:
    case MOQ_LOCATION_FILTER_OBJECT_END:
        if (in->end_group < in->start_group) return MOQ_ERR_INVAL;
        out->start_group = in->start_group;
        out->start_object = in->start_object;
        out->end_group = in->end_group;
        if (in->kind == MOQ_LOCATION_FILTER_OBJECT_END)
            out->end_object = in->end_object;
        break;
    default:
        return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

static moq_result_t receiver_copy_fill(const moq_fill_cfg_t *in,
                                     receiver_track_options_t *out)
{
    if (in->struct_size < MOQ_FILL_CFG_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    moq_fill_cfg_init(&out->fill);
    out->has_fill = true;
    if (in->has_subscriber_priority) {
        out->fill.has_subscriber_priority = true;
        out->fill.subscriber_priority = in->subscriber_priority;
    }
    if (in->has_group_order) {
        if (in->group_order != MOQ_GROUP_ORDER_ASCENDING &&
            in->group_order != MOQ_GROUP_ORDER_DESCENDING)
            return MOQ_ERR_INVAL;
        out->fill.has_group_order = true;
        out->fill.group_order = in->group_order;
    }
    if (in->has_fill_timeout) {
        out->fill.has_fill_timeout = true;
        out->fill.fill_timeout_ms = in->fill_timeout_ms;
    }
    if (in->location) {
        moq_result_t rc = receiver_copy_location(in->location, &out->fill_location);
        if (rc != MOQ_OK) return rc;
        out->has_fill_location = true;
    }
    return MOQ_OK;
}

/* Reconciliation is deferred: refuse an unencodable selection before the
 * app's desired state commits, not later as a receiver-fatal setup failure. */
static moq_result_t receiver_check_track_options(
    const moq_media_receiver_t *r, const receiver_track_options_t *options)
{
    /* Draft 16 carries one shared delivery timeout, as
     * session_check_dt_selection enforces: nonzero, a varint, and one value. */
    if (r->transport_version == MOQ_VERSION_DRAFT_16) {
        bool ho = options->has_object_delivery_timeout;
        bool hs = options->has_subgroup_delivery_timeout;
        uint64_t o = options->object_delivery_timeout_ms;
        uint64_t sg = options->subgroup_delivery_timeout_ms;
        if ((ho && (o == 0 || o > MOQ_QUIC_VARINT_MAX)) ||
            (hs && (sg == 0 || sg > MOQ_QUIC_VARINT_MAX)) ||
            (ho && hs && o != sg))
            return MOQ_ERR_INVAL;
    }
    if (!options->has_location && !options->has_fill) return MOQ_OK;
    if (r->transport_version == MOQ_VERSION_DRAFT_22)
        return receiver_companion_d22(r->companion_profile)
            ? MOQ_OK : MOQ_ERR_UNSUPPORTED;
    if ((r->transport_version != MOQ_VERSION_DRAFT_16 &&
         r->transport_version != MOQ_VERSION_DRAFT_18) ||
        r->companion_profile != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 ||
        options->has_fill)
        return MOQ_ERR_UNSUPPORTED;

    const moq_location_filter_cfg_t *location = &options->location;
    if (location->kind == MOQ_LOCATION_FILTER_OBJECT_END ||
        (location->kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP &&
         location->relative_group_offset != 0))
        return MOQ_ERR_UNSUPPORTED;
    if (r->transport_version == MOQ_VERSION_DRAFT_16) {
        if ((location->kind == MOQ_LOCATION_FILTER_ABSOLUTE_START ||
             location->kind == MOQ_LOCATION_FILTER_GROUP_END) &&
            (location->start_group > MOQ_QUIC_VARINT_MAX ||
             location->start_object > MOQ_QUIC_VARINT_MAX))
            return MOQ_ERR_INVAL;
        if (location->kind == MOQ_LOCATION_FILTER_GROUP_END &&
            location->end_group > MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

/* True when `track` is one of this receiver's catalog track handles. The
 * handle list grows across catalog updates (network thread, under r->mu), so
 * the read happens under r->mu to order against the building hook. */
static bool receiver_owns_track(moq_media_receiver_t *r,
                                const moq_media_track_t *track)
{
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i] == track) return true;
    return false;
}

/* The live end of a handle's replacement chain (r->mu): a command on a
 * handle an MSF-02 replacement removed acts on its newest successor, so the
 * latest intent reaches the live handle even when it was issued before the
 * application saw the replacement. Every successor is a newer handle, so
 * the walk ends within the handle count. Other handles are their own head. */
static moq_media_track_t *receiver_chain_head_locked(const moq_media_receiver_t *r,
                                                     moq_media_track_t *t)
{
    for (size_t i = 0; i < r->track_count && t->replaced && t->successor; i++)
        t = t->successor;
    return t;
}

moq_result_t moq_media_receiver_subscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;

    moq_subscribe_filter_t filt = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    bool has_prio = false;
    uint8_t prio = 0;
    uint32_t ts_program = 0;
    receiver_track_options_t options = {0};
    if (cfg) {
        const unsigned char *storage = (const unsigned char *)cfg;
        uint32_t cfg_size;
        memcpy(&cfg_size, storage, sizeof(cfg_size));
        if (cfg_size < MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE)
            return MOQ_ERR_INVAL;
        /* Old clients need only the old alignment. Never dereference their
         * storage as the pointer-extended type, including partial tails. */
        moq_media_receiver_track_subscribe_cfg_t aligned = {0};
        size_t n = cfg_size < sizeof(aligned) ? cfg_size : sizeof(aligned);
        memcpy(&aligned, storage, n);
        cfg = &aligned;
        if (cfg->struct_size >= offsetof(moq_media_receiver_track_subscribe_cfg_t, location) +
                                sizeof(cfg->location) && cfg->location) {
            moq_result_t rc = receiver_copy_location(cfg->location, &options.location);
            if (rc != MOQ_OK) return rc;
            options.has_location = true;
        } else {
            if (cfg->start != MOQ_MEDIA_START_CURRENT &&
                cfg->start != MOQ_MEDIA_START_NEXT_GROUP)
                return MOQ_ERR_INVAL;
            if (cfg->start == MOQ_MEDIA_START_NEXT_GROUP)
                filt = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        }
        if (cfg->struct_size >= offsetof(moq_media_receiver_track_subscribe_cfg_t, fill) +
                                sizeof(cfg->fill) && cfg->fill) {
            moq_result_t rc = receiver_copy_fill(cfg->fill, &options);
            if (rc != MOQ_OK) return rc;
        }
        if (cfg->struct_size >= offsetof(moq_media_receiver_track_subscribe_cfg_t,
                                         object_delivery_timeout_ms) +
                                sizeof(cfg->object_delivery_timeout_ms) &&
            cfg->object_delivery_timeout_ms) {
            options.has_object_delivery_timeout = true;
            options.object_delivery_timeout_ms = *cfg->object_delivery_timeout_ms;
        }
        if (cfg->struct_size >= offsetof(moq_media_receiver_track_subscribe_cfg_t,
                                         subgroup_delivery_timeout_ms) +
                                sizeof(cfg->subgroup_delivery_timeout_ms) &&
            cfg->subgroup_delivery_timeout_ms) {
            options.has_subgroup_delivery_timeout = true;
            options.subgroup_delivery_timeout_ms = *cfg->subgroup_delivery_timeout_ms;
        }
        if (cfg->struct_size >= offsetof(moq_media_receiver_track_subscribe_cfg_t, forward) +
                                sizeof(cfg->forward) &&
            cfg->forward) {
            options.has_forward = true;
            options.forward = *cfg->forward;
        }
        has_prio = cfg->has_priority;
        if (has_prio) prio = cfg->priority;
        if (cfg->struct_size >=
                offsetof(moq_media_receiver_track_subscribe_cfg_t,
                         mpeg2ts_program_number) +
                sizeof(cfg->mpeg2ts_program_number)) {
            ts_program = cfg->mpeg2ts_program_number;
            if (ts_program > 0xFFFFu) return MOQ_ERR_INVAL;
        }
    }

    pthread_mutex_lock(&r->mu);
    if (r->fatal) { pthread_mutex_unlock(&r->mu); return MOQ_ERR_CLOSED; }
    if (!receiver_owns_track(r, track)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    track = receiver_chain_head_locked(r, track);
    /* A track that has ended (peer-rejected, or end-of-track) or been removed
     * from the catalog can never be re-established: the reconcile permanently
     * skips such tracks, so refuse here rather than recording desired state
     * that will never take effect. */
    if (track->ended || track->removed || track->local_result != MOQ_OK ||
        track->auth_failed) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* No delivery path: refuse rather than subscribe something that can only
     * be dropped. An MSF-01 namespace override is one -- the MSF-01 namespace
     * string has no defined mapping to a MoQT namespace tuple, and
     * subscribing under the receiver namespace would target the wrong
     * namespace (an MSF-02 one names its own fields, 5.2.2, and is
     * subscribed under them). The others are info that did not derive, a
     * packaging this build does not parse, a timeline the receiver does not
     * decode, and the receiver's own delivery policy. */
    if (!track->has_ns_fields || !track->has_path) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_UNSUPPORTED;
    }
    moq_result_t options_rc = receiver_check_track_options(r, &options);
    if (options_rc != MOQ_OK) {
        pthread_mutex_unlock(&r->mu);
        return options_rc;
    }
    /* Record start mode + priority ONLY on the off->on transition. They
     * configure the subscription when it is first established; a redundant or
     * rapid second subscribe_track (still "wanted", whether pending, active, or
     * paused) is an idempotent enable and must not overwrite them before the
     * network hook issues the first SUBSCRIBE (header contract). */
    if (!track->desired_subscribed) {
        track->desired_filter = filt;
        track->desired_has_priority = has_prio;
        track->desired_priority = prio;
        track->desired_ts_program = ts_program;
        track->desired_options = options;
        track->app_gen++;          /* state-changing app intent */
    }
    track->desired_subscribed = true;
    pthread_mutex_unlock(&r->mu);

    /* Non-allocating wake: the desired state is recorded; nudge the network
     * thread to reconcile it. This cannot fail, so the command is atomic --
     * MOQ_OK means the state change is committed and scheduled. */
    moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_unsubscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&r->mu);
    if (r->fatal) { pthread_mutex_unlock(&r->mu); return MOQ_ERR_CLOSED; }
    if (!receiver_owns_track(r, track)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    track = receiver_chain_head_locked(r, track);
    if (track->auth_failed) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_OK;
    }
    if (track->removed && !track->completion_pending) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* A terminal catalog removed it while its tail is still owed: the same
     * disable, and the network thread then removes the track, with no end. */
    if (track->removed) track->completion_cancelled = true;
    if (track->desired_subscribed)
        track->app_gen++;          /* state-changing app intent */
    track->desired_subscribed = false;
    /* A fill command the executor has not taken never goes out. A taken one
     * may still be admitted; it keeps its slot until its receipt is polled.
     * Admitted fills are not cancelled. */
    if (track->cmd_state == FILL_CMD_QUEUED) track->cmd_state = FILL_CMD_NONE;
    else if (track->cmd_state == FILL_CMD_CLAIMED) track->cmd_withdrawn = true;
    /* The same for a fetch command; sent fetches are not cancelled. */
    if (track->fetch_cmd_state == FETCH_CMD_QUEUED)
        track->fetch_cmd_state = FETCH_CMD_NONE;
    else if (track->fetch_cmd_state == FETCH_CMD_CLAIMED)
        track->fetch_cmd_withdrawn = true;
    /* Purge the track's queued objects now, under the same lock that guards the
     * queue (a pure data operation -- no session calls -- so it is safe off the
     * network thread). Doing it here rather than in the delivery reconcile makes
     * the purge independent of the current forward state: a track already paused
     * by FLOW_CONTROL (forward already false) is purged just the same. Objects
     * that arrive before the forward=false update lands are dropped at intake. */
    obj_compact_locked(r, keep_not_track, track);
    track->has_drop_filter = false;
    pthread_mutex_unlock(&r->mu);

    moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_set_track_forward(
    moq_media_receiver_t *r, moq_media_track_t *track, bool forward)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&r->mu);
    if (r->fatal) { pthread_mutex_unlock(&r->mu); return MOQ_ERR_CLOSED; }
    if (!receiver_owns_track(r, track)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    /* Only a track the app subscribed passively, still wanted and alive. */
    bool passive = track->desired_options.has_forward && !track->desired_options.forward;
    if (!passive || !track->desired_subscribed || track->removed || track->ended ||
        track->auth_failed || track->local_result != MOQ_OK) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    if (track->raised != forward) {
        track->raised = forward;
        track->app_gen++;          /* state-changing app intent */
    }
    pthread_mutex_unlock(&r->mu);

    moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_fill_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_fill_cfg_t *fill)
{
    if (!r || !track || !fill) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
    receiver_track_options_t options = {0};
    moq_result_t rc = receiver_copy_fill(fill, &options);
    if (rc != MOQ_OK) return rc;

    pthread_mutex_lock(&r->mu);
    if (!r->fatal && receiver_owns_track(r, track))
        track = receiver_chain_head_locked(r, track);
    if (r->fatal) rc = MOQ_ERR_CLOSED;
    else if (!receiver_owns_track(r, track)) rc = MOQ_ERR_INVAL;
    else if (r->transport_version != MOQ_VERSION_DRAFT_22 ||
             !receiver_companion_d22(r->companion_profile))
        rc = MOQ_ERR_UNSUPPORTED;
    else if (track->ended || track->removed || track->local_result != MOQ_OK ||
             track->auth_failed || !track->desired_subscribed)
        rc = MOQ_ERR_WRONG_STATE;
    else if (track->cmd_state != FILL_CMD_NONE) rc = MOQ_ERR_WOULD_BLOCK;
    else if (r->fill_cmd_next == UINT64_MAX) rc = MOQ_ERR_LIMIT;
    else {
        track->cmd_state = FILL_CMD_QUEUED;
        track->cmd_withdrawn = false;
        track->cmd_seq = ++r->fill_cmd_next;
        track->cmd_parent_gen = track->parent_gen;
        track->cmd_fill = options.fill;
        track->cmd_fill.location = NULL;
        track->cmd_location = options.fill_location;
        track->cmd_has_location = options.has_fill_location;
        track->cmd_token = (moq_sub_fill_t){0};
    }
    pthread_mutex_unlock(&r->mu);
    if (rc == MOQ_OK) moq_endpoint_wake(r->ep);
    return rc;
}

moq_result_t moq_media_receiver_cancel_fill(
    moq_media_receiver_t *r, moq_media_track_t *track,
    uint64_t initiating_request_id)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
    pthread_mutex_lock(&r->mu);
    moq_result_t rc = MOQ_ERR_STALE_HANDLE;
    if (r->fatal) rc = MOQ_ERR_CLOSED;
    else if (!receiver_owns_track(r, track)) rc = MOQ_ERR_INVAL;
    else if (r->transport_version != MOQ_VERSION_DRAFT_22 ||
             !receiver_companion_d22(r->companion_profile))
        rc = MOQ_ERR_UNSUPPORTED;
    else {
        for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
            receiver_fill_t *child = &r->fills[i];
            if (moq_sub_fill_is_valid(child->token) && child->track == track &&
                child->initiating_request_id == initiating_request_id && !child->terminal_seen) {
                child->cancel_pending = true;
                rc = MOQ_OK;
                break;
            }
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (rc == MOQ_OK) moq_endpoint_wake(r->ep);
    return rc;
}

void moq_media_fetch_cfg_init(moq_media_fetch_cfg_t *cfg)
{
    moq_media_fetch_cfg_init_sized(cfg, MOQ_MEDIA_FETCH_CFG_V0_SIZE);
}

void moq_media_fetch_cfg_init_sized(moq_media_fetch_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    unsigned char *storage = (unsigned char *)cfg;
    memset(storage, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(storage, &stamp, sizeof(stamp));
}

/* Open standalone fetches of `track`: created, with a FETCH_TERMINAL not yet
 * polled. Caller holds r->mu. */
static uint32_t receiver_open_fetches_locked(const moq_media_receiver_t *r,
                                             const moq_media_track_t *track)
{
    uint32_t n = 0;
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++)
        if (r->fill_doms[i].state != DOM_FREE && r->fill_doms[i].track == track)
            n++;
    return n;
}

moq_result_t moq_media_receiver_fetch_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_media_fetch_cfg_t *cfg)
{
    if (!r || !track || !cfg) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
    if (cfg->struct_size < MOQ_MEDIA_FETCH_CFG_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    /* Every field is within the v0 floor: copy the values, never the
     * caller's storage beyond it. */
    moq_media_fetch_cfg_t copy;
    memset(&copy, 0, sizeof(copy));
    copy.struct_size = (uint32_t)sizeof(copy);
    copy.start_group = cfg->start_group;
    copy.start_object = cfg->start_object;
    copy.end_group = cfg->end_group;
    copy.end_object = cfg->end_object;
    copy.has_subscriber_priority = cfg->has_subscriber_priority;
    copy.subscriber_priority = cfg->subscriber_priority;
    /* The range ends after its start: end_object 0 is the whole end group,
     * otherwise the end is exclusive. */
    bool empty = copy.end_group < copy.start_group ||
        (copy.end_group == copy.start_group && copy.end_object != 0 &&
         copy.end_object <= copy.start_object);
    if (empty) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&r->mu);
    moq_result_t rc = MOQ_OK;
    if (r->fatal) rc = MOQ_ERR_CLOSED;
    else if (!receiver_owns_track(r, track)) rc = MOQ_ERR_INVAL;
    else if (r->transport_version == MOQ_VERSION_DRAFT_16 &&
             (copy.start_group > MOQ_QUIC_VARINT_MAX ||
              copy.start_object > MOQ_QUIC_VARINT_MAX ||
              copy.end_group > MOQ_QUIC_VARINT_MAX ||
              copy.end_object > MOQ_QUIC_VARINT_MAX))
        rc = MOQ_ERR_INVAL;
    else if (!track->has_media_path) rc = MOQ_ERR_UNSUPPORTED;
    else if (track->ended || track->removed || track->local_result != MOQ_OK ||
             track->auth_failed || !track->desired_subscribed)
        rc = MOQ_ERR_WRONG_STATE;
    else if (track->fetch_cmd_state != FETCH_CMD_NONE) rc = MOQ_ERR_WOULD_BLOCK;
    else if (r->fetch_cmd_next == UINT64_MAX) rc = MOQ_ERR_LIMIT;
    /* The bound replenishes as terminals are polled: a capacity refusal. */
    else if (receiver_open_fetches_locked(r, track) >= r->max_fetches_per_track)
        rc = MOQ_ERR_CAPACITY;
    else {
        track->fetch_cmd_state = FETCH_CMD_QUEUED;
        track->fetch_cmd_withdrawn = false;
        track->fetch_cmd_seq = ++r->fetch_cmd_next;
        track->fetch_cmd_cfg = copy;
    }
    pthread_mutex_unlock(&r->mu);
    if (rc == MOQ_OK) moq_endpoint_wake(r->ep);
    return rc;
}

moq_result_t moq_media_receiver_cancel_fetch(
    moq_media_receiver_t *r, moq_media_track_t *track,
    uint64_t fetch_request_id)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
    pthread_mutex_lock(&r->mu);
    moq_result_t rc = MOQ_ERR_STALE_HANDLE;
    if (r->fatal) rc = MOQ_ERR_CLOSED;
    else if (!receiver_owns_track(r, track)) rc = MOQ_ERR_INVAL;
    else {
        for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++) {
            receiver_fill_dom_t *d = &r->fill_doms[i];
            if (d->state == DOM_LIVE && d->track == track &&
                d->initiating_request_id == fetch_request_id) {
                d->cancel_pending = true;
                rc = MOQ_OK;
                break;
            }
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (rc == MOQ_OK) moq_endpoint_wake(r->ep);
    return rc;
}

moq_result_t moq_media_receiver_withdraw_fetch(
    moq_media_receiver_t *r, moq_media_track_t *track)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
    pthread_mutex_lock(&r->mu);
    moq_result_t rc;
    if (r->fatal) rc = MOQ_ERR_CLOSED;
    else if (!receiver_owns_track(r, track)) rc = MOQ_ERR_INVAL;
    else if (track->fetch_cmd_state == FETCH_CMD_QUEUED) {
        track->fetch_cmd_state = FETCH_CMD_NONE;
        rc = MOQ_OK;
    } else if (track->fetch_cmd_state == FETCH_CMD_NONE) rc = MOQ_DONE;
    else rc = MOQ_ERR_WRONG_STATE;
    pthread_mutex_unlock(&r->mu);
    return rc;
}

moq_result_t moq_media_receiver_track_state(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_state_t *out)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;

    /* Snapshot endpoint closure OUTSIDE r->mu (it takes the endpoint mutex; the
     * pump takes endpoint-mu then r->mu, so locking the other way could deadlock).
     * A clean close is terminal -> ENDED, same as a receiver-fatal. */
    bool closed = r->ep && moq_endpoint_is_closed(r->ep);

    /* The mutex is logically mutable; this is a read-only snapshot. Every field
     * read below is mu-protected: ended / requested / established /
     * desired_subscribed are written under r->mu, and paused is the FLOW_CONTROL
     * flag. */
    moq_media_receiver_t *rr = (moq_media_receiver_t *)r;
    pthread_mutex_lock(&rr->mu);

    bool owns = false;
    for (size_t i = 0; i < rr->track_count; i++)
        if (rr->tracks[i] == track) { owns = true; break; }
    if (!owns) {
        pthread_mutex_unlock(&rr->mu);
        return MOQ_ERR_INVAL;                 /* foreign handle: programmer error */
    }
    /* A track a terminal catalog removed is reported removed once its
     * TRACK_REMOVED follows its end (completion_pending clears then). */
    if (track->removed && !track->completion_pending) {
        pthread_mutex_unlock(&rr->mu);
        return MOQ_ERR_WRONG_STATE;           /* removed from catalog: terminal */
    }

    moq_media_track_state_t st;
    bool issued = track->requested || track->established;
    if (track->local_result != MOQ_OK || track->auth_failed) {
        st = MOQ_MEDIA_TRACK_STATE_FAILED;
    } else if (rr->fatal || closed || track->ended) {
        st = MOQ_MEDIA_TRACK_STATE_ENDED;
    } else if (!track->desired_subscribed) {
        /* Disabled: PAUSED_APP if it ever had a subscription issued (it persists,
         * paused), else never subscribed. */
        st = issued ? MOQ_MEDIA_TRACK_STATE_PAUSED_APP
                    : MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    } else if (!track->established) {
        st = MOQ_MEDIA_TRACK_STATE_PENDING;   /* wanted; SUBSCRIBE not yet accepted */
    } else if (rr->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
               rr->paused) {
        st = MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW;
    } else {
        st = MOQ_MEDIA_TRACK_STATE_ACTIVE;
    }

    pthread_mutex_unlock(&rr->mu);
    *out = st;
    return MOQ_OK;
}

/* What a queued entry's object holds: its buffers and owned sample table. */
static uint64_t entry_charge(const moq_media_object_t *o)
{
    uint64_t c = 0;
    if (o->payload_ref) c += moq_rcbuf_len(o->payload_ref);
    if (o->properties_ref) c += moq_rcbuf_len(o->properties_ref);
    if (o->track_properties_ref) c += moq_rcbuf_len(o->track_properties_ref);
    if (o->samples_owned) c += (uint64_t)o->sample_count * sizeof(moq_cmaf_sample_t);
    return c;
}

uint64_t moq_media_object_charge(const moq_media_object_t *obj)
{
    if (!obj || obj->struct_size < MEDIA_OBJECT_V0_SIZE) return 0;
    uint64_t c = 0;
    if (obj->payload_ref) c += moq_rcbuf_len(obj->payload_ref);
    if (obj->properties_ref) c += moq_rcbuf_len(obj->properties_ref);
    if (obj->struct_size >= MEDIA_OBJECT_METADATA_SIZE && obj->track_properties_ref)
        c += moq_rcbuf_len(obj->track_properties_ref);
    if (obj->samples_owned)
        c += (uint64_t)obj->sample_count * sizeof(moq_cmaf_sample_t);
    return c;
}

static moq_result_t receiver_poll_object(moq_media_receiver_t *r,
                                         uint64_t max_charge, uint64_t *charge,
                                         moq_media_object_t *obj,
                                         size_t obj_size);

moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r,
                                            moq_media_object_t *obj,
                                            size_t obj_size)
{
    return receiver_poll_object(r, UINT64_MAX, NULL, obj, obj_size);
}

moq_result_t moq_media_receiver_poll_object_within(
    moq_media_receiver_t *r, uint64_t max_charge, uint64_t *charge,
    moq_media_object_t *obj, size_t obj_size)
{
    return receiver_poll_object(r, max_charge, charge, obj, obj_size);
}

static moq_result_t receiver_poll_object(moq_media_receiver_t *r,
                                         uint64_t max_charge, uint64_t *charge,
                                         moq_media_object_t *obj,
                                         size_t obj_size)
{
    /* See poll_track: frozen v0 floor, copy clamped to min(caller, current). The
     * ref fields live in the v0 prefix, so n >= v0 always carries ownership. */
    if (!r || !obj) return MOQ_ERR_INVAL;
    /* The companion is read in the same critical section that dequeues, so
     * an AUTO receiver resolving to draft 22 cannot hand an object to a
     * buffer below the draft-22 floor. */
    pthread_mutex_lock(&r->mu);
    if (receiver_companion_d22(r->companion_profile) &&
        obj_size < (r->companion_profile ==
                            MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC &&
                        !r->companion_auto
                    ? MEDIA_OBJECT_LOC_EXT_SIZE : MEDIA_OBJECT_ORIGIN_SIZE)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_UNSUPPORTED;
    }
    if (obj_size < MEDIA_OBJECT_V0_SIZE) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    size_t n = obj_size < sizeof(*obj) ? obj_size : sizeof(*obj);
    memset(obj, 0, n);

    if (moq_endpoint_interrupted_internal(r->ep)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INTERRUPTED;
    }

    if (r->obj_tail - r->obj_head == r->catalog_objects_queued) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep))
            return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    receiver_obj_entry_t *e = &r->objs[r->obj_head % r->ring_cap];
    uint64_t c = entry_charge(&e->obj);
    if (charge) *charge = c;
    if (c > max_charge) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_BUFFER;
    }
    memcpy(obj, &e->obj, n);            /* exclusive ownership transfers */
    obj->struct_size = (uint32_t)n;
    /* The join PSI goes out with the first Object of the current
     * subscription generation the application polls that carries it; any
     * other prepared Object is delivered without it. An Object prepared with
     * no PSI octets never spends the join. */
    if (e->join_candidate && e->join_psi_len) {
        moq_media_track_t *t = e->obj.track;
        if (t && e->gen == t->ts_gen && !t->ts_join_delivered) {
            t->ts_join_delivered = true;
        } else {
            obj->payload.data += e->join_psi_len;
            obj->payload.len -= e->join_psi_len;
        }
    }
    if (e->obj.gap) {
        r->stats.gaps_reported++;
        if (e->obj.gap_tail_unknown) r->stats.gap_tails_unknown++;
    }
    dom_unqueue_locked(r, e);
    obj_unqueue_locked(r, e);
    memset(e, 0, sizeof(*e));
    r->obj_head++;
    /* FLOW_CONTROL: a paused subscription stops the traffic that would
     * otherwise drive pump cycles, so crossing the low-water mark must
     * nudge the network thread to run the resume reconcile. A fill
     * terminal made ready here wakes waiters and a gated executor. */
    bool nudge = r->fill_dom_wake;
    r->fill_dom_wake = false;
    /* A stopped intake resumes once its lane room returns; a window
     * stopped for room may now fit the queue. */
    if (r->st_blocked && st_room_locked(r)) {
        r->st_blocked = false;
        nudge = true;
    }
    if (r->ts_wake) {
        r->ts_wake = false;
        nudge = true;
    }
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
        r->paused && !r->resume_nudged &&
        obj_resume_ready_locked(r)) {
        r->resume_nudged = true;
        nudge = true;
    }
    pthread_mutex_unlock(&r->mu);
    if (nudge)
        moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_poll_stream_terminal(
    moq_media_receiver_t *r, moq_media_stream_terminal_t *out, size_t out_size)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    pthread_mutex_lock(&r->mu);
    if (!st_head_pollable_locked(r)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_DONE;
    }
    receiver_st_entry_t *e = &r->st[r->st_head % r->st_cap];
    size_t n = out_size < sizeof(e->rec) ? out_size : sizeof(e->rec);
    memcpy(out, &e->rec, n);
    out->struct_size = (uint32_t)n;
    uint8_t dom = e->dom;
    memset(e, 0, sizeof(*e));
    r->st_head++;
    /* A fill's last owed loss record makes its retired record ready. */
    if (dom) {
        receiver_fill_dom_t *d = &r->fill_doms[dom - 1];
        assert(d->st_owed > 0);
        if (--d->st_owed == 0 && d->queued == 0 && d->state == DOM_RETIRED)
            dom_ready_locked(r, d);
    }
    /* Room returned: place the held terminal, and wake the network
     * thread when a stopped intake or reorder window can resume, or a
     * gated executor has a record back. */
    st_place_held_locked(r);
    bool wake = r->fill_dom_wake;
    r->fill_dom_wake = false;
    if (r->st_blocked || r->ts_wake) {
        r->st_blocked = false;
        r->ts_wake = false;
        wake = true;
    }
    pthread_mutex_unlock(&r->mu);
    if (wake) moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

void moq_media_object_cleanup(moq_media_object_t *obj)
{
    if (!obj) return;
    /* The ref fields live in the v0 prefix, so a poll-filled obj (struct_size >=
     * v0) or a zeroed obj (NULL refs) both read safely. Bound the final zeroing
     * by the stamped struct_size so a smaller caller's struct is never overrun. */
    if (obj->payload_ref)    moq_rcbuf_decref(obj->payload_ref);
    if (obj->properties_ref) moq_rcbuf_decref(obj->properties_ref);
    if (obj->struct_size >= MEDIA_OBJECT_METADATA_SIZE && obj->track_properties_ref)
        moq_rcbuf_decref(obj->track_properties_ref);
    if (obj->samples_owned) {
        const moq_alloc_t *a = moq_alloc_default();
        a->free(obj->samples_owned,
                obj->sample_count * sizeof(moq_cmaf_sample_t), a->ctx);
    }
    size_t n = obj->struct_size;
    if (n > sizeof(*obj)) n = sizeof(*obj);
    if (n) memset(obj, 0, n);
}

moq_result_t moq_media_receiver_poll_sap(moq_media_receiver_t *r,
                                         moq_media_sap_record_t *out,
                                         size_t out_size)
{
    if (!r || !out || out_size < MEDIA_SAP_RECORD_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);

    pthread_mutex_lock(&r->mu);
    if (r->sap_tail == r->sap_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    moq_media_sap_record_t rec = r->saps[r->sap_head % r->sap_cap];
    r->sap_head++;
    pthread_mutex_unlock(&r->mu);
    rec.struct_size = (uint32_t)n;
    memcpy(out, &rec, n);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_poll_media_timeline(
    moq_media_receiver_t *r,
    moq_media_timeline_record_t *out,
    size_t out_size)
{
    if (!r || !out || out_size < MEDIA_TIMELINE_RECORD_V0_SIZE)
        return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);

    pthread_mutex_lock(&r->mu);
    if (r->mt_tail == r->mt_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    moq_media_timeline_record_t rec = r->mts[r->mt_head % r->mt_cap];
    r->mt_head++;
    pthread_mutex_unlock(&r->mu);
    rec.struct_size = (uint32_t)n;
    memcpy(out, &rec, n);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_get_stats(const moq_media_receiver_t *r,
                                          moq_media_receiver_stats_t *out,
                                          size_t out_size)
{
    if (!r || !out || out_size < MEDIA_RECEIVER_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    /* Snapshot into a full local, then copy the clamped prefix to the caller. */
    moq_media_receiver_stats_t snap;
    pthread_mutex_lock(&mr->mu);
    snap = mr->stats;
    snap.objects_queued = mr->obj_tail - mr->obj_head - mr->catalog_objects_queued;
    snap.bytes_queued = mr->obj_sizes_total;
    snap.paused = mr->paused;
    pthread_mutex_unlock(&mr->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

moq_endpoint_t *moq_media_receiver_endpoint_internal(const moq_media_receiver_t *r)
{
    return r ? r->ep : NULL;
}

moq_result_t moq_media_receiver_get_info(const moq_media_receiver_t *r,
                                         moq_media_receiver_info_t *out,
                                         size_t out_size)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_RECEIVER_INFO_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    moq_media_receiver_info_t full;
    memset(&full, 0, sizeof(full));
    pthread_mutex_t *mu = (pthread_mutex_t *)&r->mu;
    pthread_mutex_lock(mu);
    full.time_mode = (uint32_t)r->time_mode;
    full.overflow_policy = (uint32_t)r->overflow.policy;
    full.max_objects = r->obj_cap;
    full.max_bytes = r->byte_cap;
    full.auto_subscribe = r->auto_subscribe;
    full.surface_stream_terminals = r->st_enabled;
    full.companion_profile = r->companion_profile;
    full.transport_version = (uint32_t)r->transport_version;
    full.max_stream_terminals = r->st_enabled ? r->st_cap : 0;
    full.track_events_polled = r->track_events_polled;
    pthread_mutex_unlock(mu);
    size_t n = out_size < sizeof(full) ? out_size : sizeof(full);
    full.struct_size = (uint32_t)n;
    memcpy(out, &full, n);
    return MOQ_OK;
}

bool moq_media_receiver_is_closed(const moq_media_receiver_t *r)
{
    if (!r) return true;
    return moq_endpoint_is_closed(r->ep);
}

bool moq_media_receiver_is_fatal(const moq_media_receiver_t *r)
{
    if (!r) return false;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    pthread_mutex_unlock(&mr->mu);
    return fatal || moq_endpoint_is_fatal(r->ep);
}

uint64_t moq_media_receiver_fatal_code(const moq_media_receiver_t *r)
{
    if (!r) return 0;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    uint64_t code = mr->fatal_code;
    pthread_mutex_unlock(&mr->mu);
    if (fatal) return code;
    return moq_endpoint_fatal_code(r->ep);
}

#ifdef MOQ_MEDIA_RECEIVER_TESTING
/* -- White-box test seam (not built into the shipping library) ---------- *
 * Drives the catalog ingest/reconcile path with crafted (group,object,json)
 * objects, with no endpoint or network. The receiver's per-handle public APIs
 * tolerate a NULL endpoint (treated as not-closed), so subscribe/unsubscribe/
 * track_state can be exercised against a test receiver too. */

/* Observation-only probe of the publication mutex, exact tri-state:
 *   0  = FREE    (trylock acquired it; released immediately)
 *   1  = HELD    (trylock returned EBUSY)
 *  -1  = INVALID (any other trylock error) -- the caller MUST turn this into a
 *        named failure, so an unexpected mutex error never satisfies the held
 *        contract. Single-threaded use inside a phase hook. */
int moq_media_receiver_test_mu_held(const moq_media_receiver_t *r);
int moq_media_receiver_test_mu_held(const moq_media_receiver_t *r)
{
    pthread_mutex_t *mu = (pthread_mutex_t *)&r->mu;
    int rc = pthread_mutex_trylock(mu);
    if (rc == 0) {
        pthread_mutex_unlock(mu);
        return 0;           /* FREE -> code under test does NOT hold r->mu */
    }
    if (rc == EBUSY) return 1;   /* HELD */
    return -1;                   /* INVALID */
}

bool moq_media_receiver_test_auth_latched(moq_media_receiver_t *r,
                                         moq_media_track_t *track);
bool moq_media_receiver_test_auth_latched(moq_media_receiver_t *r,
                                         moq_media_track_t *track)
{
    pthread_mutex_lock(&r->mu);
    bool latched = track ? track->auth_failed :
                          (r->fatal || r->catalog_fetch_issued);
    pthread_mutex_unlock(&r->mu);
    return latched;
}

/* Number of retained CP-bearing catalog snapshots (retention contract). Lets
 * the writer fixture prove a snapshot was actually retained before free. */
size_t moq_media_receiver_test_cp_snap_count(const moq_media_receiver_t *r);
size_t moq_media_receiver_test_cp_snap_count(const moq_media_receiver_t *r)
{
    return r ? r->cp_snap_count : 0;
}

moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_receiver_t *r = (moq_media_receiver_t *)alloc->alloc(
        sizeof(*r), alloc->ctx);
    if (!r) return NULL;
    memset(r, 0, sizeof(*r));
    r->alloc = *alloc;
    r->ep = NULL;
    r->auto_subscribe = auto_subscribe;
    r->max_stable_handles = RECEIVER_DEFAULT_MAX_STABLE_HANDLES;
    r->max_cp_snaps       = RECEIVER_DEFAULT_MAX_CP_SNAPS;
    r->max_retained_bytes = RECEIVER_DEFAULT_MAX_RETAINED_BYTES;
    r->ts_slots = RECEIVER_DEFAULT_TS_SLOTS;
    r->ts_window_us = RECEIVER_DEFAULT_TS_WINDOW_US;
    r->ts_deadline_us = UINT64_MAX;
    pthread_mutex_init(&r->mu, NULL);
    r->ev_cap = RECEIVER_DEFAULT_TRACK_EVENTS;
    r->events = (receiver_event_t *)alloc->alloc(
        r->ev_cap * sizeof(receiver_event_t), alloc->ctx);
    r->obj_cap = RECEIVER_DEFAULT_MAX_OBJECTS;
    r->ring_cap = r->obj_cap;
    r->byte_cap = RECEIVER_DEFAULT_MAX_BYTES;
    r->objs = (receiver_obj_entry_t *)alloc->alloc(
        r->ring_cap * sizeof(receiver_obj_entry_t), alloc->ctx);
    if (!r->events || !r->objs) { receiver_free(r); return NULL; }
    memset(r->objs, 0, r->ring_cap * sizeof(receiver_obj_entry_t));
    return r;
}

/* The draft a live session would have latched before any catalog arrives,
 * for a sessionless receiver: track info derives as it would there. */
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r,
                                                   moq_version_t v);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r,
                                                   moq_version_t v)
{
    pthread_mutex_lock(&r->mu);
    r->transport_version = v;
    pthread_mutex_unlock(&r->mu);
}

/* The deadline the endpoint would query (receiver_next_deadline_us). */
uint64_t moq_media_receiver_test_next_deadline_us(moq_media_receiver_t *r);
uint64_t moq_media_receiver_test_next_deadline_us(moq_media_receiver_t *r)
{
    return receiver_next_deadline_us(r);
}

void moq_media_receiver_test_free(moq_media_receiver_t *r)
{
    if (r) receiver_free(r);
}

/* Replace a test receiver's allocator (the caller's delegates to the one it
 * replaces, so earlier allocations free through it). */
void moq_media_receiver_test_set_receiver_alloc(moq_media_receiver_t *r,
                                                const moq_alloc_t *alloc);
void moq_media_receiver_test_set_receiver_alloc(moq_media_receiver_t *r,
                                                const moq_alloc_t *alloc)
{
    r->alloc = *alloc;
}

/* Resize a test receiver's empty track-event queue. */
bool moq_media_receiver_test_set_event_cap(moq_media_receiver_t *r, uint32_t cap);
bool moq_media_receiver_test_set_event_cap(moq_media_receiver_t *r, uint32_t cap)
{
    if (cap == 0 || r->ev_tail != r->ev_head) return false;
    receiver_event_t *ev = (receiver_event_t *)r->alloc.alloc(
        cap * sizeof(receiver_event_t), r->alloc.ctx);
    if (!ev) return false;
    r->alloc.free(r->events, r->ev_cap * sizeof(receiver_event_t), r->alloc.ctx);
    r->events = ev;
    r->ev_cap = cap;
    r->ev_head = r->ev_tail = 0;
    return true;
}

/* A track's recorded selection, as the network thread would read it. */
void moq_media_receiver_test_track_intent(moq_media_receiver_t *r,
    const moq_media_track_t *t, bool *subscribed, uint32_t *filter,
    bool *has_priority, uint8_t *priority, uint32_t *ts_program,
    uint64_t *app_gen);
void moq_media_receiver_test_track_intent(moq_media_receiver_t *r,
    const moq_media_track_t *t, bool *subscribed, uint32_t *filter,
    bool *has_priority, uint8_t *priority, uint32_t *ts_program,
    uint64_t *app_gen)
{
    pthread_mutex_lock(&r->mu);
    *subscribed = t->desired_subscribed;
    *filter = (uint32_t)t->desired_filter;
    *has_priority = t->desired_has_priority;
    *priority = t->desired_priority;
    *ts_program = t->desired_ts_program;
    *app_gen = t->app_gen;
    pthread_mutex_unlock(&r->mu);
}

/* A replacement handle's subscription gate and its fill command state. */
const moq_media_track_t *moq_media_receiver_test_track_gate(moq_media_receiver_t *r,
                                                            const moq_media_track_t *t);
const moq_media_track_t *moq_media_receiver_test_track_gate(moq_media_receiver_t *r,
                                                            const moq_media_track_t *t)
{
    pthread_mutex_lock(&r->mu);
    const moq_media_track_t *g = t->gate;
    pthread_mutex_unlock(&r->mu);
    return g;
}
int moq_media_receiver_test_track_cmd_state(moq_media_receiver_t *r,
                                            const moq_media_track_t *t);
int moq_media_receiver_test_track_cmd_state(moq_media_receiver_t *r,
                                            const moq_media_track_t *t)
{
    pthread_mutex_lock(&r->mu);
    int st = t->cmd_state;
    pthread_mutex_unlock(&r->mu);
    return st;
}

/* The ordering stage of an ordered track, as its subscription would start
 * it; false for a track without one. */
bool moq_media_receiver_test_order_start(moq_media_receiver_t *r, moq_media_track_t *t);
bool moq_media_receiver_test_order_start(moq_media_receiver_t *r, moq_media_track_t *t)
{
    return t->ordered && receiver_order_start(r, t);
}

/* One live Object of `t` taken in at now_us, as the pump would. */
void moq_media_receiver_test_order_push(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint64_t group, uint64_t subgroup, uint64_t object,
    moq_object_status_t status, const uint8_t *payload, size_t len);
void moq_media_receiver_test_order_push(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint64_t group, uint64_t subgroup, uint64_t object,
    moq_object_status_t status, const uint8_t *payload, size_t len)
{
    moq_sub_object_t so;
    if (!receiver_intake_begin(r)) return;
    memset(&so, 0, sizeof so);
    so.track = t->sub_track;
    so.group_id = group;
    so.subgroup_id = subgroup;
    so.object_id = object;
    so.publisher_priority = 0x80;
    so.status = status;
    if (status == MOQ_OBJECT_NORMAL && payload &&
        moq_rcbuf_create(&r->alloc, payload, len, &so.payload) != MOQ_OK) {
        receiver_intake_end(r);
        return;
    }
    if (t->order) receiver_on_ordered_object(r, t, &so, now_us);
    else receiver_on_media_object(r, t, &so, now_us);
    receiver_intake_end(r);
}

/* One stream terminal of `t`'s subscription taken at now_us. */
void moq_media_receiver_test_order_terminal(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint32_t kind, uint64_t group, bool has_last, uint64_t last,
    bool has_cut, uint64_t cut);
void moq_media_receiver_test_order_terminal(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint32_t kind, uint64_t group, bool has_last, uint64_t last,
    bool has_cut, uint64_t cut)
{
    moq_sub_stream_terminal_t st;
    memset(&st, 0, sizeof(st));
    st.struct_size = (uint32_t)sizeof(st);
    st.kind = kind;
    st.track = t->sub_track;
    st.group_id = group;
    st.has_last_object = has_last;
    st.last_object_id = last;
    st.has_cut_object = has_cut;
    st.cut_object_id = cut;
    receiver_order_terminal(r, t, &st, now_us);
}

/* The pump's timer step at now_us (reorder windows and ordering stages). */
void moq_media_receiver_test_timers(moq_media_receiver_t *r, uint64_t now_us);
void moq_media_receiver_test_timers(moq_media_receiver_t *r, uint64_t now_us)
{
    receiver_ts_timers(r, now_us);
}

/* Give a sessionless test receiver the namespace its catalog track was
 * received under (borrowed: the caller keeps it alive until free). */
void moq_media_receiver_test_set_namespace(moq_media_receiver_t *r,
                                           moq_namespace_t ns);
void moq_media_receiver_test_set_namespace(moq_media_receiver_t *r,
                                           moq_namespace_t ns)
{
    r->namespace_ = ns;
}

/* Stage one live catalog Object exactly as the bootstrap's live intake does
 * (receiver_bootstrap_stage_live), on the calling thread acting as the
 * network side. `payload` is borrowed; the Object takes its own reference.
 * Returns the count of catalog Objects staged so far. */
uint64_t moq_media_receiver_test_stage_catalog(moq_media_receiver_t *r, uint64_t group,
                                               uint64_t object, moq_rcbuf_t *payload);
uint64_t moq_media_receiver_test_stage_catalog(moq_media_receiver_t *r, uint64_t group,
                                               uint64_t object, moq_rcbuf_t *payload)
{
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.group_id = group;
    so.object_id = object;
    so.status = MOQ_OBJECT_NORMAL;
    so.payload = payload ? moq_rcbuf_incref(payload) : NULL;
    receiver_bootstrap_stage_live(r, &so);
    moq_sub_object_cleanup(&so);
    pthread_mutex_lock(&r->mu);
    uint64_t n = r->catalog_objects_queued;
    pthread_mutex_unlock(&r->mu);
    return n;
}

void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len)
{
    /* The payload stands for the facade's Object: it is not the receiver's
     * allocation, so a test failing the receiver's allocator fails only what
     * the receiver allocates. */
    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)json, len, &buf) != MOQ_OK)
        return;
    receiver_ingest_catalog(r, group, object, buf, (moq_bytes_t){ NULL, 0 },
                            (moq_bytes_t){ NULL, 0 }, 0);
    moq_rcbuf_decref(buf);
}

/* The same with the Object's properties and its origin's Track Properties
 * (borrowed for the call), encoded for the receiver's latched draft. */
void moq_media_receiver_test_ingest_props(moq_media_receiver_t *r, uint64_t group,
    uint64_t object, const char *json, size_t len, moq_bytes_t track_props,
    moq_bytes_t object_props);
void moq_media_receiver_test_ingest_props(moq_media_receiver_t *r, uint64_t group,
    uint64_t object, const char *json, size_t len, moq_bytes_t track_props,
    moq_bytes_t object_props)
{
    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)json, len, &buf) != MOQ_OK)
        return;
    receiver_ingest_catalog(r, group, object, buf, object_props, track_props, 0);
    moq_rcbuf_decref(buf);
}

bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track)
{
    pthread_mutex_lock(&r->mu);
    if (r->ev_tail == r->ev_head) { pthread_mutex_unlock(&r->mu); return false; }
    receiver_event_t e = r->events[r->ev_head % r->ev_cap];
    r->ev_head++;
    pthread_mutex_unlock(&r->mu);
    if (kind) *kind = e.kind;
    if (track) *track = e.track;
    return true;
}

uint64_t moq_media_receiver_test_catalog_drops(const moq_media_receiver_t *r)
{
    return r->stats.catalog_drops;
}

bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r)
{
    return r->fatal;
}

uint64_t moq_media_receiver_test_fatal_code(const moq_media_receiver_t *r)
{
    return r->fatal_code;
}

/* Lower the receiver-lifetime retained-state caps so a unit test can exercise
 * the bounded-drop / first-catalog-fatal paths without feeding thousands of
 * generations. */
void moq_media_receiver_test_set_limits(moq_media_receiver_t *r,
                                        size_t max_stable_handles,
                                        size_t max_cp_snaps,
                                        uint64_t max_retained_bytes)
{
    r->max_stable_handles = max_stable_handles;
    r->max_cp_snaps       = max_cp_snaps;
    r->max_retained_bytes = max_retained_bytes;
}

size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r)
{
    return r->track_count;
}

/* Enqueue one media-timeline record directly (the internal mt_queue_push under
 * r->mu), so a wait-contract test can make the media-timeline queue the ONLY
 * ready queue without driving a full §7 timeline ingest. The minimal
 * test_new() receiver has no mt ring; allocate a small one lazily here
 * (receiver_free releases it like the production ring). */
void moq_media_receiver_test_push_media_timeline(moq_media_receiver_t *r,
                                                 uint64_t group,
                                                 uint64_t object,
                                                 uint64_t media_time_ms,
                                                 uint64_t wallclock_ms)
{
    if (!r->mts) {
        r->mt_cap = 8;
        r->mts = (moq_media_timeline_record_t *)r->alloc.alloc(
            r->mt_cap * sizeof(moq_media_timeline_record_t), r->alloc.ctx);
        if (!r->mts) { r->mt_cap = 0; return; }
        memset(r->mts, 0, r->mt_cap * sizeof(moq_media_timeline_record_t));
    }
    moq_msf_media_timeline_record_t src;
    memset(&src, 0, sizeof(src));
    src.group = group;
    src.object = object;
    src.media_time_ms = media_time_ms;
    src.wallclock_ms = wallclock_ms;
    pthread_mutex_lock(&r->mu);
    mt_queue_push(r, NULL, &src);
    pthread_mutex_unlock(&r->mu);
}

/* Build a fully-initialized receiver (namespace, catalog name, overflow ring,
 * event/object/sap/mt queues) from a cfg, with NO endpoint -- so a deterministic
 * scripted-peer harness can drive the REAL receiver_hook (auto-subscribe,
 * catalog ingest, object routing) against a session it owns. receiver_new with
 * ep == NULL skips the endpoint hook attach (see the if (ep) guard there). */
moq_result_t moq_media_receiver_test_construct(
    const moq_media_receiver_cfg_t *cfg, moq_media_receiver_t **out);
void moq_media_receiver_test_set_fatal(moq_media_receiver_t *r, uint64_t code);

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_t *r = NULL;
    if (moq_media_receiver_test_construct(cfg, &r) != MOQ_OK) return NULL;
    return r;
}

/* The same construction as moq_media_receiver_test_new_cfg, surfacing the
 * REAL result (INVAL from validation vs NOMEM from the allocator) so a
 * construction oracle can distinguish a refusal from an allocation failure.
 * No endpoint is attached. */
moq_result_t moq_media_receiver_test_construct(
    const moq_media_receiver_cfg_t *cfg, moq_media_receiver_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    receiver_sizes_t sz;
    moq_media_receiver_cfg_t lc;
    moq_result_t rc = receiver_prepare(cfg, &lc, &sz);
    if (rc < 0) return rc;
    moq_alloc_t alloc = *RECEIVER_CTOR_ALLOC();
    moq_media_auth_source_t auth = {0};
    rc = moq_media_auth_source_copy(&auth, lc.resource_auth, &alloc);
    if (rc != MOQ_OK) return rc;
    rc = receiver_new(NULL, false, &lc, &sz, &alloc, &auth, out);
    moq_media_auth_source_clear(&auth, &alloc);
    return rc;
}

/* Drive the receiver into its terminal (fatal) state through the one
 * transition every fatal path takes, so terminal-receiver rows do not need a
 * scripted peer. The code is reported by fatal_code() exactly as production. */
void moq_media_receiver_test_set_fatal(moq_media_receiver_t *r, uint64_t code)
{
    receiver_set_fatal(r, code);
}

/* Point a test receiver at a (bare) endpoint so its callbacks record into it;
 * pair with set_ep(r, NULL) before free. */
void moq_media_receiver_test_set_ep(moq_media_receiver_t *r, moq_endpoint_t *ep)
{
    r->ep = ep;
}

/* Drive the real endpoint pump hook with a caller-controlled session. The hook
 * itself ignores the endpoint argument (receiver_hook does (void)ep;), so a
 * NULL endpoint is sound here. */
/* Run the receiver on `ep` (a bare test endpoint), so its closed state is
 * the endpoint's. The caller keeps `ep` and clears this before freeing it. */
void moq_media_receiver_test_set_endpoint(moq_media_receiver_t *r, moq_endpoint_t *ep);
void moq_media_receiver_test_set_endpoint(moq_media_receiver_t *r, moq_endpoint_t *ep)
{
    r->ep = ep;
}

void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us)
{
    receiver_hook(NULL, session, now_us, r);
}

/* A delivery-capacity failure of `track`, as the facade reports one. LIVE
 * fails the track and keeps the objects already admitted (staged, then
 * published by the next pump once the facade holds none); FILL fails only
 * a child, without ending its parent. */
void moq_media_receiver_test_fail_delivery(moq_media_receiver_t *r, moq_media_track_t *track,
                                           bool fill);
void moq_media_receiver_test_fail_delivery(moq_media_receiver_t *r, moq_media_track_t *track,
                                           bool fill)
{
    pthread_mutex_lock(&r->mu);
    if (!fill) {
        if (track->local_result == MOQ_OK) {
            track->local_result = MOQ_ERR_NOMEM;
            track->local_failure_kind = MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE;
            track->delivery_failed = true;
            track->delivery_failure_staged = true;
            track->upd_notify_app = false;
            track->update_inflight = false;
            track->upd_sent_app_gen = 0;
        }
    } else if (r->ev_tail - r->ev_head < r->ev_cap) {
        receiver_event_t *event = &r->events[r->ev_tail++ % r->ev_cap];
        *event = (receiver_event_t){ .kind = MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE,
            .track = track, .local_result = MOQ_ERR_NOMEM,
            .delivery_origin = MOQ_MEDIA_DELIVERY_FILL,
            .delivery_initiating_request_id = 1 };
    }
    pthread_mutex_unlock(&r->mu);
    moq_endpoint_wake(r->ep);
}

/* Fail `track` locally, as a retention failure of its initial metadata
 * does: MOQ_MEDIA_TRACK_LOCAL_FAILURE, without TRACK_ENDED. */
void moq_media_receiver_test_fail_track(moq_media_receiver_t *r, moq_media_track_t *track);
void moq_media_receiver_test_fail_track(moq_media_receiver_t *r, moq_media_track_t *track)
{
    receiver_mark_track_local_failure(r, track, MOQ_ERR_NOMEM,
                                      MOQ_MEDIA_TRACK_LOCAL_FAILURE);
}

/* Queue `track`'s END_OF_TRACK at (group, object) on the stream-terminal
 * lane behind everything taken in so far, as its peer's End of Track
 * arriving before objects still in flight on other streams. False without
 * lane room. */
bool moq_media_receiver_test_end_of_track(moq_media_receiver_t *r, moq_media_track_t *track,
                                          uint64_t group, uint64_t object);
bool moq_media_receiver_test_end_of_track(moq_media_receiver_t *r, moq_media_track_t *track,
                                          uint64_t group, uint64_t object)
{
    pthread_mutex_lock(&r->mu);
    bool ok = r->st_enabled && st_free_locked(r) > r->st_reserved;
    if (ok) {
        moq_media_stream_terminal_t rec;
        memset(&rec, 0, sizeof(rec));
        rec.kind = MOQ_MEDIA_STREAM_END_OF_TRACK;
        rec.track = track;
        rec.group_id = group;
        rec.object_id = object;
        st_push_locked(r, &rec, 0);
    }
    pthread_mutex_unlock(&r->mu);
    return ok;
}

/* Queue a RAW LOC-less media object for `track` at (group, object), taken
 * in behind everything so far, with `payload` (a ref is taken) and
 * `time_us` as its capture, decode and presentation time: an object
 * reaching the receiver by another route, or after the application's view
 * of its track has moved on. False when the object queue is full. */
bool moq_media_receiver_test_inject_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                           uint64_t group, uint64_t object, bool keyframe,
                                           uint64_t time_us, moq_rcbuf_t *payload);
bool moq_media_receiver_test_inject_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                           uint64_t group, uint64_t object, bool keyframe,
                                           uint64_t time_us, moq_rcbuf_t *payload)
{
    pthread_mutex_lock(&r->mu);
    if (r->obj_tail - r->obj_head >= r->obj_cap) {
        pthread_mutex_unlock(&r->mu);
        return false;
    }
    receiver_obj_entry_t *e = &r->objs[r->obj_tail % r->ring_cap];
    memset(e, 0, sizeof(*e));
    size_t len = moq_rcbuf_len(payload);
    e->group_id = group;
    e->budget = len;
    e->gen = track->ts_gen;
    moq_media_object_t *o = &e->obj;
    o->track = track;
    o->packaging = MOQ_MEDIA_PACKAGING_RAW;
    o->status = MOQ_OBJECT_NORMAL;
    o->keyframe = keyframe;
    o->has_capture_time = true;
    o->capture_time_us = time_us;
    o->decode_time_us = time_us;
    o->presentation_time_us = time_us;
    moq_rcbuf_incref(payload);
    o->payload_ref = payload;
    o->payload = (moq_bytes_t){ moq_rcbuf_data(payload), len };
    o->group_id = group;
    o->object_id = object;
    o->has_subgroup = true;
    e->seq = r->ingress_seq++;
    if (r->st_enabled && track->st_queued++ == 0) r->domains_queued++;
    r->obj_budget_total += len;
    r->obj_sizes_total += len;
    r->obj_tail++;
    pthread_mutex_unlock(&r->mu);
    return true;
}

/* Run the production subscriber-teardown path (live-handle snapshot + per-handle
 * UNSUBSCRIBE + facade destroy) against a caller-supplied session, then free the
 * receiver. The no-endpoint analogue of moq_media_receiver_destroy()'s
 * network-thread teardown post: production posts receiver_destroy_sub_task to the
 * endpoint thread; the scriptable harness has no endpoint, so it supplies the
 * session directly. Exercises receiver_take_teardown + receiver_destroy_sub_task
 * exactly as production does. */
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us)
{
    if (r->sub) {
        /* A receiver built without an endpoint has no reservation; this
         * single-threaded harness makes its context here. */
        if (!r->teardown) r->teardown = receiver_teardown_new(&r->alloc);
        if (r->teardown) {
            receiver_teardown_t *td = receiver_take_teardown(r);
            (void)receiver_destroy_sub_task(NULL, session, now_us, td);
        } else {
            moq_sub_destroy(r->sub);
        }
        r->sub = NULL;
    }
    receiver_free(r);
}

/* Post the production teardown task to a (test) endpoint exactly as
 * moq_media_receiver_destroy() does, then free the receiver shell. Ownership of
 * the subscriber + handle snapshot moves into the task node; driving the
 * endpoint's pump (moq_endpoint_test_drain_posted) then exercises the real
 * requeue-on-WOULD_BLOCK retry across cycles. Returns the post result. */
moq_result_t moq_media_receiver_test_post_teardown(moq_media_receiver_t *r,
                                                   moq_endpoint_t *ep)
{
    moq_result_t rc = MOQ_OK;
    if (r->sub) {
        /* Reserve on `ep` as receiver_new does, then post as destroy does. */
        moq_endpoint_t *own = r->ep;
        r->ep = ep;
        if (!receiver_reserve_teardown(r)) {
            receiver_release_teardown(r);
            r->ep = own;
            moq_sub_destroy(r->sub);
            r->sub = NULL;
            receiver_free(r);
            return MOQ_ERR_NOMEM;
        }
        receiver_teardown_t *td = receiver_take_teardown(r);
        rc = moq_endpoint_internal_post_reserved(ep, r->teardown_slot,
                                                 receiver_destroy_sub_task, td);
        if (rc == MOQ_OK)
            r->teardown_slot = NULL;
        else
            (void)receiver_destroy_sub_task(ep, NULL, 0, td);
        receiver_release_teardown(r);
        r->ep = own;
        r->sub = NULL;
    }
    receiver_free(r);
    return rc;
}
/* Test seams: the pure attribution classifier (deterministic interleaving
 * proofs run against the PRODUCTION logic) and a per-track attribution
 * inspector (causal proof that terminal/rearm cleanup really cleared the
 * pending app attribution). Test-internals only; never exported. */
void moq_media_receiver_test_classify(uint64_t gen_snap, uint64_t reconciled,
                                      bool *out_notify,
                                      uint64_t *out_reconciled)
{
    attribution_classify(gen_snap, reconciled, out_notify, out_reconciled);
}

void moq_media_receiver_test_track_attrib(moq_media_receiver_t *r,
                                          moq_media_track_t *t,
                                          bool *out_notify,
                                          uint64_t *out_sent_gen,
                                          uint64_t *out_app_gen,
                                          uint64_t *out_reconciled)
{
    pthread_mutex_lock(&r->mu);
    if (out_notify) *out_notify = t->upd_notify_app;
    if (out_sent_gen) *out_sent_gen = t->upd_sent_app_gen;
    if (out_app_gen) *out_app_gen = t->app_gen;
    if (out_reconciled) *out_reconciled = t->reconciled_app_gen;
    pthread_mutex_unlock(&r->mu);
}


/* Preconditions and intake for a receiver with no session, each setting or
 * driving exactly what production sets or drives elsewhere: the track at a
 * catalog index, the desire a subscribe records, and one object through the
 * real intake, routed by the track's kind as the pump routes it. The intake
 * takes over the object's refs. */
void moq_media_receiver_test_latch_session_version(moq_media_receiver_t *r, moq_version_t v);
void moq_media_receiver_test_latch_session_version(moq_media_receiver_t *r, moq_version_t v)
{
    pthread_mutex_lock(&r->mu);
    receiver_latch_version_locked(r, v);
    pthread_mutex_unlock(&r->mu);
}

moq_media_track_t *moq_media_receiver_test_track_at(moq_media_receiver_t *r, size_t index);
moq_media_track_t *moq_media_receiver_test_track_at(moq_media_receiver_t *r, size_t index)
{
    pthread_mutex_lock(&r->mu);
    moq_media_track_t *t = index < r->track_count ? r->tracks[index] : NULL;
    pthread_mutex_unlock(&r->mu);
    return t;
}

void moq_media_receiver_test_mark_desired(moq_media_receiver_t *r, moq_media_track_t *track);
void moq_media_receiver_test_mark_desired(moq_media_receiver_t *r, moq_media_track_t *track)
{
    pthread_mutex_lock(&r->mu);
    track->desired_subscribed = true;
    pthread_mutex_unlock(&r->mu);
}

void moq_media_receiver_test_push_object_ex(moq_media_receiver_t *r, moq_media_track_t *track,
                                            uint64_t group, uint64_t subgroup, uint64_t object,
                                            uint8_t publisher_priority, moq_object_status_t status,
                                            bool end_of_group, bool datagram,
                                            const uint8_t *payload, size_t len,
                                            const uint8_t *properties, size_t plen);
void moq_media_receiver_test_push_object_ex(moq_media_receiver_t *r, moq_media_track_t *track,
                                            uint64_t group, uint64_t subgroup, uint64_t object,
                                            uint8_t publisher_priority, moq_object_status_t status,
                                            bool end_of_group, bool datagram,
                                            const uint8_t *payload, size_t len,
                                            const uint8_t *properties, size_t plen)
{
    moq_sub_object_t so;
    if (!receiver_intake_begin(r)) return;     /* no room: the pump would wait */
    memset(&so, 0, sizeof so);
    so.track = track->sub_track;
    so.group_id = group;
    so.subgroup_id = subgroup;
    so.object_id = object;
    so.publisher_priority = publisher_priority;
    so.status = status;
    so.end_of_group = end_of_group;
    so.datagram = datagram;
    /* Only a NORMAL object carries a payload. */
    if ((status == MOQ_OBJECT_NORMAL && payload &&
         moq_rcbuf_create(&r->alloc, payload, len, &so.payload) != MOQ_OK) ||
        (properties && plen &&
         moq_rcbuf_create(&r->alloc, properties, plen, &so.properties) != MOQ_OK)) {
        moq_sub_object_cleanup(&so);
        receiver_intake_end(r);
        return;
    }
    if (track->is_sap_timeline)
        receiver_on_sap_object(r, track, &so, 0);
    else if (track->is_media_timeline)
        receiver_on_media_timeline_object(r, track, &so, 0);
    else if (track->is_event_timeline)
        moq_sub_object_cleanup(&so);
    else if (track_is_ts(track))
        receiver_on_ts_object(r, track, &so, 0);
    else if (track->order)
        receiver_on_ordered_object(r, track, &so, 0);
    else
        receiver_on_media_object(r, track, &so, 0);
    receiver_intake_end(r);
}

void moq_media_receiver_test_push_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                         uint64_t group, uint64_t object,
                                         moq_object_status_t status, bool end_of_group,
                                         bool datagram, const uint8_t *payload, size_t len,
                                         const uint8_t *properties, size_t plen);
void moq_media_receiver_test_push_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                         uint64_t group, uint64_t object,
                                         moq_object_status_t status, bool end_of_group,
                                         bool datagram, const uint8_t *payload, size_t len,
                                         const uint8_t *properties, size_t plen)
{
    moq_media_receiver_test_push_object_ex(r, track, group, 0, object, 0, status, end_of_group,
                                           datagram, payload, len, properties, plen);
}

#endif /* MOQ_MEDIA_RECEIVER_TESTING */
