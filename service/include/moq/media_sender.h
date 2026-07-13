#ifndef MOQ_MEDIA_SENDER_H
#define MOQ_MEDIA_SENDER_H

/*
 * moq_media_sender_t — the send-side media facade over an endpoint
 * (MEDIA_SERVICE_DESIGN.md §7). Owns a publisher on the endpoint's
 * network thread: it advertises a namespace, derives and publishes a
 * retained MSF catalog from the configured tracks, and reports readiness
 * once the namespace is accepted and the catalog is published. The
 * application adds tracks and submits media objects from its own thread.
 *
 * Current status: tracks, catalog, readiness, and the media write path are
 * live -- objects written from the app thread are enqueued in decode order
 * into a bounded, per-track sync-anchored queue and drained to the session
 * on the network thread, generating standard LOC-01 properties from the
 * typed fields and abandoning open subgroups wire-correctly under
 * backpressure. LOC-02 (the draft-18 KVP profile) is a coordinated
 * stack-wide future item; today everything is LOC-01.
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/endpoint.h>
#include <moq/media_object.h>
#include <moq/msf.h>     /* moq_cmsf_content_protection_t (CMSF authoring) */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_media_sender moq_media_sender_t;

/* The opaque media-track handle. Shared name with the receiver
 * (MEDIA_SERVICE_DESIGN.md uses moq_media_track_t on both sides); each
 * facade completes the struct privately in its own translation unit, so
 * including both headers is safe -- the handle is only ever an opaque
 * pointer across the boundary. */
#ifndef MOQ_MEDIA_TRACK_T_DEFINED
#define MOQ_MEDIA_TRACK_T_DEFINED
typedef struct moq_media_track moq_media_track_t;
#endif

/* -- Backpressure (§7.5, normative: no silent drop, honest naming) ---- *
 * The send path is bounded; when the network cannot keep up the policy is
 * a REQUIRED choice (zero/unset fails creation with MOQ_ERR_INVAL). The
 * drop policies abandon whole groups wire-correctly; the lossless policy
 * blocks the writer and surfaces the drop decision to the app rather than
 * discarding on its own. */

typedef enum moq_media_send_backpressure {
    MOQ_MEDIA_SEND_BP_UNSET = 0,            /* creation fails: MOQ_ERR_INVAL */
    MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME,     /* live default preset */
    MOQ_MEDIA_SEND_BP_DROP_GROUP,
    MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT,        /* lossless preset (FFmpeg) */
    MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK
} moq_media_send_backpressure_t;

/* -- Demand visibility (§7.2) ----------------------------------------- *
 * Subscriber/demand callbacks. media_sender already gates media emission on
 * downstream demand (it never writes a media object to a track with no
 * subscriber), but is_ready() reports only "announced + catalog published",
 * NOT whether any relay/player has subscribed. These callbacks surface the
 * arrival/departure of demand so an app can distinguish "no subscriber yet"
 * from network backpressure, and start/stop encoding accordingly.
 *
 * Threading: invoked on the endpoint network thread, NOT under any lock the
 * query functions take, and never re-entrant with respect to the sender. Like
 * the core publisher callbacks they are NON-REENTRANT: do NOT call
 * moq_media_sender_* MUTATORS (write/add_track/remove_track/end_track/
 * complete/convert_to_vod) from within a callback -- record the state change
 * and act from your own loop. The read-only demand queries below ARE safe to
 * call from within a callback.
 *
 * `track` is the stable media-track handle (the one add_track returned).
 * `active_subscriptions` is that track's subscriber count AFTER this event.
 * Callbacks fire for app-visible media tracks only -- never for the internal
 * catalog track or generated SAP/media-timeline tracks. */
typedef struct moq_media_sender_callbacks {
    uint32_t struct_size;
    void    *ctx;
    void (*on_subscriber_joined)(
        void *ctx, moq_media_sender_t *sender, moq_media_track_t *track,
        size_t active_subscriptions);
    void (*on_subscriber_left)(
        void *ctx, moq_media_sender_t *sender, moq_media_track_t *track,
        size_t active_subscriptions);
    void (*on_ready)(void *ctx, moq_media_sender_t *sender);
    void (*on_closed)(void *ctx, moq_media_sender_t *sender,
                      bool is_fatal, uint64_t fatal_code);
    void (*on_track_closed)(void *ctx, moq_media_sender_t *sender,
                            moq_media_track_t *track);
} moq_media_sender_callbacks_t;

MOQ_API void moq_media_sender_callbacks_init(moq_media_sender_callbacks_t *cb);

/* -- Configuration (§7.1) --------------------------------------------- */

typedef struct moq_media_sender_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint; /* REQUIRED by *_create() (owns a
                                           private endpoint built from it);
                                           MUST be NULL for *_attach() */
    moq_namespace_t namespace_;        /* REQUIRED: what to announce */
    moq_bytes_t catalog_track;         /* empty = "catalog" */
    moq_media_send_backpressure_t backpressure; /* REQUIRED: UNSET fails;
                                           presets set it */
    uint64_t block_timeout_us;         /* BLOCK_TIMEOUT only; preset default */
    uint32_t queue_max_objects;        /* send-queue depth; 0 = default */
    uint32_t queue_max_bytes;          /* send-queue byte budget; 0 = default */
    uint32_t pre_ready_max_objects;    /* pre-ready depth; 0 = default */
    uint32_t pre_ready_max_bytes;      /* pre-ready byte budget; 0 = default */
    bool     validate_cmaf;            /* STRICT BY DEFAULT (the cfg_init*
                                          initializers set this true): validate
                                          CMAF objects against CMSF §3.3/§3.4 on
                                          write and reject malformed ones. When
                                          the track has a CMAF init segment the
                                          object track_ID is checked against it;
                                          a group-start object whose first sample
                                          is known not to be a SAP is rejected (a
                                          sync sample of indeterminate exact SAP
                                          type is allowed). Affects CMAF tracks
                                          only; RAW/LOC is unaffected. Set to
                                          false AFTER cfg_init for deliberate
                                          passthrough (no per-object checks). */

    /* CMSF §4 root contentProtections: DRM metadata entries that tracks
     * reference by refID (see moq_media_track_cfg_t.content_protection_ref_ids).
     * The service DEEP-COPIES every field at create/attach -- refID,
     * defaultKID[], scheme, and the drmSystem systemID/URLs/pssh/robustness --
     * so your buffers need not outlive the call, and the DOM-borrow caveat that
     * applies to a parsed moq_cmsf_content_protection_t does NOT apply to what
     * you pass here. Empty = none (the default; existing senders emit an
     * identical catalog). Entries are validated against CMSF §4 catalog SYNTAX
     * (not DRM semantics): each requires a non-empty refID, a scheme that is the
     * CENC enum "cenc" or "cbcs" (§4.1.1.3), a drmSystem.systemID that is a UUID
     * string (§4.1.1.4.1), and at least one defaultKID, each a UUID string
     * (§4.1.1.2); and every refID is unique across the array (§4.1.1.1, no
     * duplicates). UUID syntax is the "8-4-4-4-12" hex form (case-insensitive).
     * (This is deliberately STRICTER than the receive parser, which accepts an
     * empty defaultKID array on an inbound catalog: a sender refuses to AUTHOR
     * an incomplete/useless DRM entry.) Any OPTIONAL drmSystem field that is
     * marked present must also carry non-empty data: a present
     * laURL/certURL/authURL needs a url (and a non-empty type when has_type),
     * and has_pssh / has_robustness need their bytes. DRM-system SEMANTICS are
     * NOT interpreted -- the UUID's registry MEANING, URL reachability, PSSH box
     * internals, and robustness meaning are carried as opaque metadata, not
     * checked.
     * A malformed entry fails create/attach with MOQ_ERR_INVAL. Borrowed for
     * the call. */
    const moq_cmsf_content_protection_t *content_protections;
    size_t                               content_protection_count;

    /* OPTIONAL demand-visibility callbacks (subscriber joined/left). APPENDED
     * after contentProtections: an older caller's struct_size prefix never
     * reaches these bytes, so they default to none (set callbacks.struct_size
     * via moq_media_sender_callbacks_init). The struct is copied at
     * create/attach; ctx and the function pointers are retained for the
     * sender's life. Leave zeroed for no callbacks (queries still work). */
    moq_media_sender_callbacks_t         callbacks;

    /* false: advertise + answer SUBSCRIBE (pull).
     * true: also PUBLISH every track and emit the catalog live so a relay
     * caches it without a FETCH (push). Default to false. */
    bool                                 publish_tracks;

    /* Media written while its track has no demand: false (default) holds it
     * queued until demand appears; true drops it to stay at the live edge. */
    bool                                 drop_without_demand;
} moq_media_sender_cfg_t;

/* Plain init leaves backpressure UNSET on purpose -- the choice is forced,
 * not defaulted (symmetric with the receiver's overflow policy). Use the
 * presets:
 *   _init_live      -> DROP_TO_KEYFRAME (live: never block the encoder).
 *   _init_lossless  -> BLOCK_TIMEOUT    (the service never drops on its own;
 *                      under sustained congestion write() blocks, then
 *                      surfaces MOQ_ERR_WOULD_BLOCK so the drop decision is
 *                      the app's). The asymmetry with the receiver's
 *                      _init_flow_control is deliberate: the sender's
 *                      guarantee ends at the relay handoff, which it
 *                      controls; a receiver cannot make a relay retain. */
MOQ_API void moq_media_sender_cfg_init(moq_media_sender_cfg_t *cfg);
MOQ_API void moq_media_sender_cfg_init_live(moq_media_sender_cfg_t *cfg);
MOQ_API void moq_media_sender_cfg_init_lossless(moq_media_sender_cfg_t *cfg);

/* -- Track configuration (§7.1) --------------------------------------- *
 * Carries enough to derive the track's MSF catalog entry and to package
 * its media. The service builds and publishes the catalog; apps
 * never hand-build catalog JSON. */

typedef struct moq_media_track_cfg {
    uint32_t struct_size;
    moq_bytes_t name;                  /* REQUIRED: catalog track name */
    moq_media_type_t      media_type;  /* REQUIRED: VIDEO | AUDIO */
    moq_media_packaging_t packaging;   /* REQUIRED: RAW | CMAF */
    moq_bytes_t codec;                 /* codec string, e.g. "avc1.64001f".
                                          MSF-01 5.2.18: REQUIRED for audio and
                                          video tracks (add_track returns
                                          MOQ_ERR_INVAL if empty). */
    uint32_t    timescale;             /* 0 = 1000000 (microseconds) */
    moq_bytes_t init_data;             /* OPTIONAL codec/decoder init config,
                                          carried in the catalog for ANY
                                          packaging when the codec needs it:
                                          a CMAF init segment, H.264/HEVC
                                          SPS/PPS/VPS, AAC AudioSpecificConfig,
                                          etc. -- i.e. the encoder's extradata.
                                          The service base64-encodes it into the
                                          published catalog; receivers see it at
                                          TRACK_ADDED as desc.init_data. This is
                                          NOT per-object LOC metadata (that is
                                          generated per object from the typed
                                          fields). Borrowed: the service copies
                                          it during add_track, so the caller's
                                          buffer need not outlive the call.
                                          Empty when the codec carries its
                                          parameter sets in-band. */
    moq_bytes_t role;                  /* empty = derived from media_type */
    moq_bytes_t lang;
    bool        is_live;               /* catalog isLive (presets: true) */

    /* Video (zero when absent). */
    uint32_t width;
    uint32_t height;
    uint64_t framerate_millis;         /* fps * 1000 */

    /* Audio. MSF-01 5.2.28 (samplerate) and 5.2.29 (channelConfig) are both
     * REQUIRED for audio tracks (add_track returns MOQ_ERR_INVAL if a track
     * with media_type AUDIO omits either); ignored for video. */
    uint32_t    samplerate;
    moq_bytes_t channel_config;        /* e.g. "2" */

    uint64_t    bitrate;               /* maximum bitrate (bits/s). MSF-01
                                          5.2.22: REQUIRED for audio and video
                                          tracks (add_track returns
                                          MOQ_ERR_INVAL if 0). */

    /* CMSF §3.5.2 maximum SAP starting types -- optional advisory hints carried
     * in the catalog; the service does NOT derive or enforce them. has_* false
     * (and value 0) means absent, the default. */
    bool        has_max_grp_sap;       /* maxGrpSapStartingType */
    uint32_t    max_grp_sap;
    bool        has_max_obj_sap;       /* maxObjSapStartingType */
    uint32_t    max_obj_sap;

    /* CMSF §4.1.2 contentProtectionRefIDs: ids referencing root
     * contentProtections entries declared on moq_media_sender_cfg_t. The
     * service copies each id at add_track (your buffer need not outlive the
     * call). Each id MUST resolve to a configured root entry, otherwise
     * add_track returns MOQ_ERR_INVAL (catalog coherence, not DRM semantics).
     * For a CMAF track, declaring these ids means the track is CENC-encrypted
     * (§4.1.2), so add_track ALSO requires a non-empty init_data that parses as
     * protected CMAF init carrying the §4.2 CENC boxes (sinf/schm/schi/tenc) --
     * otherwise it returns MOQ_ERR_INVAL. (Always-on catalog coherence; no
     * defaultKID<->tenc comparison and no DRM/UUID/URL/PSSH semantics.)
     * Empty = unprotected (the default). Authoring metadata only -- no
     * encryption is performed. Borrowed for the call. */
    const moq_bytes_t *content_protection_ref_ids;
    size_t             content_protection_ref_id_count;

    /* CMSF SAP event timeline (CMSF §3.6.1, MSF §8.2/§8.3). When true, the
     * service synthesizes a generated eventtimeline track named "<name>.sap"
     * that 'depends' on this track and conveys its per-Object SAP records,
     * sourced from the app's per-object sap_type declarations (see
     * moq_media_send_object_t.sap_type). With this enabled, every group-start
     * object written to THIS track MUST declare sap_type TYPE_1 or TYPE_2 --
     * write() returns MOQ_ERR_INVAL for a missing declaration or NONE / UNKNOWN
     * / TYPE_3 at a group start. Mid-group declared TYPE_1/2/3 produce records;
     * mid-group NONE is an ordinary frame (no record). Default false => no
     * timeline track and a byte-identical catalog + wire to today. The
     * generated track is published with its own MOQT track and is NOT a media
     * track: apps never write to it (write() rejects its handle). One timeline
     * per opted-in track in v1; multi-track 'depends' is a future extension. */
    bool        emit_sap_timeline;
    /* History depth for the generated timeline: the independent object at each
     * timeline group boundary carries all retained records over the last N
     * media groups (MSF §8.3 "accumulated and accessible up to that point").
     * 0 = default (8). Ignored unless emit_sap_timeline is set. */
    uint32_t    sap_timeline_history_groups;

    /* MSF §5.2.35 track duration in integer milliseconds, for a VOD/non-live
     * track. MUST NOT be set together with is_live=true: add_track returns
     * MOQ_ERR_INVAL otherwise. has_track_duration false (the default) means
     * absent. Carried verbatim in every catalog build; surfaced on the receiver
     * desc as track_duration_ms. (This field is for VOD-from-start; to convert a
     * track that started live to VOD in place, use
     * moq_media_sender_convert_to_vod().) */
    bool        has_track_duration;
    uint64_t    track_duration_ms;

    /* MSF §7 media timeline (§7.1.1/§7.2/§7.3). When true, the service
     * synthesizes a generated media-timeline track named "<name>.timeline" that
     * 'depends' on this track: packaging "mediatimeline", mimeType
     * "application/json", NO eventType. Its objects carry explicit §7.1.1 records
     * [mediaTime, [group,object], wallclock] describing this track's published
     * groups. In this slice the service records ONE record per media group, at
     * the group-start object (Location (group, 0)): mediaTime is that object's
     * presentation time in ms, and wallclock is the emit time in ms for a live
     * track or 0 for a non-live/VOD track. Per-object records are a future
     * extension. The generated track is service-owned: apps never write to it
     * (write() rejects its handle), and it is removed with its media track.
     * Default false => no timeline track and a byte-identical catalog + wire.
     *
     * APPENDED AFTER the V3 trackDuration fields: keep new cfg fields at the end
     * so an older caller's struct_size prefix never reinterprets these bytes. */
    bool        emit_media_timeline;
    /* History depth for the generated media timeline: the independent object at
     * each timeline group boundary carries all retained records over the last N
     * media groups (MSF §7.3 "accumulated and accessible up to that point").
     * 0 = default (8). Ignored unless emit_media_timeline is set. */
    uint32_t    media_timeline_history_groups;

    /* CMSF §3.2 alternate group (altGroup): every CMAF track of a switching set
     * MUST carry a common altGroup value, so a player treats them as
     * bitrate/quality alternatives of one another. Authoring-only: the service
     * emits the value verbatim into this track's catalog entry; it does not
     * infer switching sets or enforce a common value across tracks (that is the
     * caller's responsibility). has_alt_group=false (the default) means absent,
     * for a byte-identical catalog to today.
     *
     * APPENDED AFTER the media-timeline fields: keep new cfg fields at the end so
     * an older caller's struct_size prefix never reinterprets these bytes. */
    bool        has_alt_group;
    int         alt_group;
} moq_media_track_cfg_t;

MOQ_API void moq_media_track_cfg_init(moq_media_track_cfg_t *cfg);

/* Add a track. Legal both before and after readiness: a post-ready add
 * registers the track and triggers an independent catalog republish (a new
 * catalog group at object 0; the network pump coalesces concurrent add/remove
 * mutations into one new generation). The handle is stable for the sender's
 * life. All pre-ready validation still applies (duplicate name, SAP-timeline
 * name collision, content-protection ref resolution, protected-CMAF init); a
 * failed add leaves the sender unchanged and does not dirty the catalog.
 * Returns MOQ_ERR_CLOSED if terminal, MOQ_ERR_INVAL for malformed cfg,
 * MOQ_ERR_NOMEM on exhaustion. Returns MOQ_ERR_WOULD_BLOCK (retryable) for a
 * post-ready add whose name (or a generated "<name>.sap" / "<name>.timeline") is
 * still in use by a just-removed track whose teardown has not completed -- retry
 * later. Any
 * successful replacement publication is ordered after the removal generation,
 * so the receiver sees remove-then-add rather than a changed tuple.
 *
 * The cfg is read prefix-safely by cfg->struct_size: a caller built against an
 * older header (a smaller cfg) is accepted and its missing appended fields
 * (e.g. trackDuration) default to absent; a caller with a newer/larger cfg has
 * its unknown trailing fields ignored. A struct_size below the required prefix
 * is MOQ_ERR_INVAL.
 *
 * MSF-01 required catalog fields are enforced so the sender never authors a
 * track that is invalid by construction: an audio or video track MUST carry a
 * non-empty codec (5.2.18) and a non-zero bitrate (5.2.22), and an audio track
 * MUST additionally carry a non-zero samplerate (5.2.28) and a non-empty
 * channel_config (5.2.29). A track missing a required field is MOQ_ERR_INVAL. */
MOQ_API moq_result_t moq_media_sender_add_track(
    moq_media_sender_t *s, const moq_media_track_cfg_t *cfg,
    moq_media_track_t **out);

/* Remove a previously added media track from the catalog (post-ready: triggers
 * an independent catalog republish without the track; pre-ready: it is simply
 * kept out of the initial catalog). The track is dropped from future catalog
 * builds, its queued objects are discarded, future moq_media_sender_write() on
 * it returns MOQ_ERR_WRONG_STATE, and its underlying MOQT track is ended. A
 * media track's generated SAP-timeline and media-timeline siblings are removed
 * with it. The handle stays valid but inert until the sender is destroyed.
 *
 * Returns MOQ_ERR_INVAL for a NULL/foreign handle or a generated/internal track
 * (e.g. an event/media timeline track -- remove its media track instead),
 * MOQ_ERR_WRONG_STATE for an already-removed handle, MOQ_ERR_CLOSED if terminal,
 * MOQ_ERR_INTERRUPTED if the latch is set. */
MOQ_API moq_result_t moq_media_sender_remove_track(
    moq_media_sender_t *s, moq_media_track_t *track);

/* -- Send objects (§7.3) ---------------------------------------------- *
 * write() ownership is transfer-on-success: on MOQ_OK the caller's refs on
 * payload/properties pass to the service (single-owner confinement -- never
 * incref-share, the refcounts are non-atomic); on ANY non-OK return no
 * transfer occurred and the caller still owns its refs. Per track, write()
 * order IS decode order. */

typedef struct moq_media_send_object {
    uint32_t struct_size;
    moq_rcbuf_t *payload;        /* required */
    moq_rcbuf_t *properties;     /* CMAF: the object's complete property
                                    block, passed through. RAW/LOC: must be
                                    NULL in v0 -- the service generates the
                                    whole LOC block from the typed fields,
                                    and merging app extras into the
                                    delta-coded KVP sequence is a follow-up
                                    (a non-NULL value returns INVAL). */
    bool is_sync;                /* random-access point; drives the drop and
                                    pre-ready sync-anchor policies */
    bool starts_group;           /* opens a new group (closing any open one) */
    bool ends_group;             /* closes the group after this object */
    uint64_t decode_time_us;     /* advisory in v0 (order conveys decode seq) */
    /* RAW/LOC timing. The LOC Capture Timestamp (LOC-01 2.3.1.1, wall-clock
     * microseconds since the Unix epoch) is taken from capture_time_us when
     * has_capture_time is true (emitted verbatim, no clamp/scale); otherwise it
     * falls back to presentation_time_us. presentation_time_us also drives the
     * generated SAP/media timelines. These fields are RAW/LOC-only -- for CMAF
     * the object's timing and properties live in the passed-through `properties`
     * block and these are ignored. For a RAW/LOC track the emitted timestamp
     * (capture_time_us, or presentation_time_us when has_capture_time is false)
     * MUST fit the QUIC varint / LOC encoding range (<= MOQ_QUIC_VARINT_MAX);
     * moq_media_sender_write() rejects an out-of-range value with MOQ_ERR_INVAL
     * (synchronously, without taking payload ownership). */
    uint64_t presentation_time_us;
    bool has_capture_time;       /* true: emit capture_time_us as the LOC ts */
    uint64_t capture_time_us;    /* wall-clock us since epoch (LOC Capture Timestamp) */
    /* OPTIONAL app-declared CMAF SAP type for this object (CMSF §3.6.1). The
     * exact SAP type 1/2/3 cannot be derived from CMAF sample flags alone
     * (it needs codec-bitstream knowledge), so the encoder declares it here.
     * When has_sap_type is true, sap_type MUST be a concrete value:
     * MOQ_SAP_NONE (0, not a SAP) or MOQ_SAP_TYPE_1/2/3 -- MOQ_SAP_UNKNOWN is
     * an internal sentinel and is rejected (MOQ_ERR_INVAL). With a declaration
     * and validate_cmaf (strict by default), a group-start object is required to
     * be SAP type 1 or 2 (§3.4). With no declaration, behavior is unchanged. */
    bool has_sap_type;
    moq_sap_type_t sap_type;
} moq_media_send_object_t;

/* Submit one media object in decode order.
 *
 * Objects are enqueued in decode order into one bounded queue (the
 * pre-ready bound applies before ready, the send-queue bound after) and
 * drained to the session on the network thread once ready. The queue is
 * sync-anchored per track: each track's retained suffix begins at a sync
 * point. On overflow the configured backpressure policy applies:
 *   DROP_TO_KEYFRAME / DROP_GROUP -- evict whole groups from the oldest
 *     end (abandoning the open subgroup on the wire if it was mid-send),
 *     always retaining the most recent sync point; then enqueue.
 *   BLOCK_TIMEOUT -- block until the drain frees space, then enqueue;
 *     MOQ_ERR_WOULD_BLOCK on timeout (no drop, no transfer).
 *   RETURN_WOULD_BLOCK -- MOQ_ERR_WOULD_BLOCK immediately when full.
 * A non-anchor object for a track with no queued anchor, or a GOP larger
 * than the bound under a drop policy, returns MOQ_ERR_WOULD_BLOCK with no
 * ownership taken.
 *
 * Terminal: MOQ_ERR_CLOSED once the endpoint/sender is terminal,
 * MOQ_ERR_INTERRUPTED while the endpoint interrupt latch is set; the
 * caller retains ownership in both cases. MOQ_ERR_INVAL for NULL/foreign
 * track or for non-NULL `properties` on a RAW track (§7.4 -- the service
 * owns the RAW/LOC property block in v0). */
MOQ_API moq_result_t moq_media_sender_write(
    moq_media_sender_t *s, moq_media_track_t *track,
    const moq_media_send_object_t *obj);

/* End a finite track: after this track's already-queued objects drain, the
 * service emits a reliable END_OF_TRACK status object (a subgroup-stream object,
 * NOT a datagram), which surfaces on receivers as MOQ_MEDIA_TRACK_ENDED. Ending
 * one track does NOT end other tracks or close the session/endpoint.
 *
 * After end_track, moq_media_sender_write() for this track returns
 * MOQ_ERR_WRONG_STATE. end_track is IDEMPOTENT: a second call returns MOQ_OK and
 * does nothing. Returns MOQ_OK (queued), MOQ_ERR_INVAL (NULL/foreign track),
 * MOQ_ERR_CLOSED (terminal), MOQ_ERR_INTERRUPTED (latch set), or
 * MOQ_ERR_WOULD_BLOCK when the send queue is momentarily full (retry). */
MOQ_API moq_result_t moq_media_sender_end_track(
    moq_media_sender_t *s, moq_media_track_t *track);

/* Permanently terminate the broadcast (MSF §11.3). The service ends every
 * active publisher track reliably (END_OF_TRACK -> receivers see
 * MOQ_MEDIA_TRACK_ENDED) and their generated SAP-timeline siblings with them,
 * then publishes a terminal independent catalog generation -- a new group at
 * object 0 carrying isComplete:true (MSF §5.1.3) and an empty tracks array --
 * live-written to active subscribers and retained (for a Joining FETCH) so a
 * later joiner still learns the broadcast completed. The namespace/endpoint stay up (this is
 * not destroy()): call moq_media_sender_destroy() when finished.
 *
 * Legal only after readiness; pre-ready returns MOQ_ERR_WRONG_STATE. IDEMPOTENT:
 * a second call returns MOQ_OK and does nothing. After completion, add_track,
 * write, remove_track, and end_track all return MOQ_ERR_WRONG_STATE; track
 * handles stay valid but inert until destroy. Returns MOQ_OK, MOQ_ERR_INVAL for
 * a NULL sender, MOQ_ERR_WRONG_STATE pre-ready, MOQ_ERR_CLOSED if terminal, or
 * MOQ_ERR_INTERRUPTED if the endpoint interrupt latch is set. */
MOQ_API moq_result_t moq_media_sender_complete(moq_media_sender_t *s);

/* Convert one or more LIVE tracks to VOD in place -- the CATALOG step of an MSF
 * §11.3 live->VOD conversion: each listed track flips isLive true->false and
 * gains a trackDuration. The service performs the two conversion steps in strict
 * order: (1) Track Ended -- each track's active subscribers are completed with
 * the "Track Ended" status (step 1) on the network pump; then (2) Catalog -- a
 * single new INDEPENDENT catalog generation is published carrying all converted
 * tracks with the SAME tuples. The Track Ended signal is emitted before the
 * conversion catalog; the catalog republish is gated until every converting
 * track's active subscribers have been finished. This is the one
 * sanctioned mutation of a surviving tuple (§5.2.7) -- a receiver applies it as
 * MOQ_MEDIA_TRACK_UPDATED on the existing handle, not a remove/add. The whole
 * batch publishes together (one generation), so subscribers never see a
 * partially-converted catalog.
 *
 * The tracks are NOT terminated: their publisher tracks stay registered and
 * retained, so a player that saw Track Ended can re-subscribe (or Joining-FETCH)
 * and keep reading the now-VOD content -- the conversion ends the LIVE phase, not
 * the track. This is distinct from END_OF_TRACK; if you additionally want active
 * subscribers to see a terminal END_OF_TRACK, call moq_media_sender_end_track()
 * on each converted track yourself.
 *
 * Each item's track MUST be currently live (isLive true), owned by this sender,
 * not the catalog/generated/internal track, not removed or ended, and not
 * already carrying a duration -- otherwise NOTHING is converted and the call
 * returns MOQ_ERR_INVAL (atomic). After conversion a track is VOD-complete:
 * moq_media_sender_write() on it returns MOQ_ERR_WRONG_STATE (no new content);
 * remove_track / end_track stay available. A generated SAP-timeline sibling is
 * left as-is -- it is not in the batch and its subscribers are not finished by
 * this step. Legal only after readiness; this is not permanent termination --
 * complete() remains separate and isComplete is NOT set.
 *
 * Returns MOQ_OK, MOQ_ERR_INVAL (NULL/empty, or any item invalid -> none
 * converted), MOQ_ERR_WRONG_STATE pre-ready or after complete(), MOQ_ERR_CLOSED
 * if terminal, MOQ_ERR_INTERRUPTED if the interrupt latch is set. */
typedef struct moq_media_vod_track {
    moq_media_track_t *track;
    uint64_t           duration_ms;   /* MSF §5.2.35; integer milliseconds */
} moq_media_vod_track_t;

MOQ_API moq_result_t moq_media_sender_convert_to_vod(
    moq_media_sender_t *s, const moq_media_vod_track_t *items, size_t count);

/* -- Stats (§7.5: every drop is counted, never silent) ---------------- */

typedef struct moq_media_sender_stats {
    uint32_t struct_size;
    uint64_t objects_written;     /* accepted by write() (queued) */
    uint64_t objects_sent;        /* emitted to the session */
    uint64_t objects_queued;      /* currently in the queue */
    uint64_t bytes_queued;        /* payload+properties bytes queued */
    uint64_t objects_dropped;     /* discarded by a drop policy */
    uint64_t groups_dropped;      /* whole groups discarded */
    uint64_t keyframes_dropped;
    uint64_t groups_abandoned;    /* open subgroups RESET on the wire */
    uint64_t backpressure_stalls; /* BLOCK_TIMEOUT waits entered */
    moq_result_t last_error;      /* last non-OK write() return (0 = none) */
    /* Generated SAP event-timeline records aged out of the bounded history
     * (see moq_media_track_cfg_t.sap_timeline_history_groups). Appended after
     * last_error so the frozen v0 stats size is unchanged. */
    uint64_t sap_records_evicted;
} moq_media_sender_stats_t;

/* Snapshot stats into *out. Pass `out_size = sizeof(*out)` as YOU compiled it.
 * The library requires at least the frozen v0 base size, writes at most
 * min(out_size, its own struct size), and stamps out->struct_size with the
 * number of bytes written -- so a newer caller can detect truncation by an older
 * library (struct_size < its sizeof) and a newer library never writes past an
 * older caller's struct. MOQ_ERR_INVAL for NULL args or out_size below the v0
 * base size. */
MOQ_API moq_result_t moq_media_sender_get_stats(
    const moq_media_sender_t *s, moq_media_sender_stats_t *out,
    size_t out_size);

/* -- Lifecycle (§5.5 ownership) --------------------------------------- */

/* Borrow an existing endpoint (cfg->endpoint MUST be NULL). The endpoint
 * must outlive the sender; moq_endpoint_stop() refuses with
 * MOQ_ERR_WRONG_STATE while the sender is attached. v0 attachment
 * contract: at most one sender per endpoint (a second attach returns
 * MOQ_ERR_WRONG_STATE); a sender and a receiver MAY share one endpoint
 * (one attachment slot per kind), with independent lifecycles -- stop()
 * stays gated while EITHER is attached, and destroying one never detaches
 * the other. Blocking waits share the endpoint's single coalesced activity
 * wake, so multiplex them from one app thread rather than parking one
 * thread per attachment. */
MOQ_API moq_result_t moq_media_sender_attach(moq_endpoint_t *ep,
                                             const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);

/* Own a private endpoint built from cfg->endpoint (REQUIRED here). */
MOQ_API moq_result_t moq_media_sender_create(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);

/* Detaches from the endpoint and frees everything the sender retained
 * (track handles, queued objects' refs). A sender that owns its endpoint
 * stops and destroys it too. */
MOQ_API void moq_media_sender_destroy(moq_media_sender_t *s);

/* is_ready (§7.2): the namespace has been accepted AND the catalog is
 * published -- the publish path is ready to accept/send media. It does NOT
 * mean a subscriber exists (pure publish often has no downstream
 * visibility). To observe DEMAND -- whether a relay/player has actually
 * subscribed -- use the callbacks on moq_media_sender_cfg_t or the demand
 * queries below (moq_media_sender_has_media_subscriber et al.). */
MOQ_API bool moq_media_sender_is_ready(const moq_media_sender_t *s);

/* Block (app thread) until the sender's WRITE LEVEL holds, the endpoint
 * wakes, or timeout_us elapses. The level is: ready (see is_ready) AND the
 * active send queue has headroom for at least one more object under the
 * configured object/byte bounds. Level-triggered: a level that already
 * holds returns MOQ_OK immediately, before and after the underlying
 * endpoint wait, so a write -> WOULD_BLOCK -> wait -> retry loop is
 * race-free.
 *
 * MOQ_OK means "attempt (another) write now" -- it does NOT promise the
 * next arbitrary write succeeds: another thread may fill the queue first,
 * a single write may exceed the byte budget, and drop-policy senders can
 * still report per-object outcomes. Treat MOQ_OK as advisory readiness,
 * exactly like moq_media_receiver_wait()'s "poll now".
 *
 * Returns MOQ_OK (level holds / endpoint woke), MOQ_DONE (timeout with the
 * level not holding), MOQ_ERR_INTERRUPTED (endpoint latch set),
 * MOQ_ERR_CLOSED (sender or endpoint terminal), MOQ_ERR_INVAL (NULL).
 * PRIORITY: the latch, then terminal state, WIN over the level -- while
 * latched wait() returns MOQ_ERR_INTERRUPTED (§5.3) and once terminal it
 * returns MOQ_ERR_CLOSED, even when ready+space still holds, because
 * write() refuses those same states and MOQ_OK would be a false
 * "write now". */
MOQ_API moq_result_t moq_media_sender_wait(moq_media_sender_t *s,
                                           uint64_t timeout_us);

/* -- Demand queries (§7.2) -------------------------------------------- *
 * App-thread-safe snapshots of subscriber/demand state. They read mirrored
 * counts under the sender's lock and never call into the core publisher, so
 * they are safe to call from any thread, including from within a demand
 * callback. All return 0/false for a NULL sender or a NULL/foreign track. */

/* Active subscriptions for one track (the exact handle, including a generated
 * track if a caller somehow holds it). 0 for an unknown/removed-and-drained
 * track. */
MOQ_API size_t moq_media_sender_track_subscriptions(
    const moq_media_sender_t *s, const moq_media_track_t *track);

/* Convenience: moq_media_sender_track_subscriptions(s, track) > 0. */
MOQ_API bool moq_media_sender_track_has_subscriber(
    const moq_media_sender_t *s, const moq_media_track_t *track);

/* True if ANY app-visible media track has a subscriber -- the "should I encode
 * media?" helper. Counts user media tracks only: the internal catalog track and
 * generated SAP/media-timeline tracks are excluded, as are removed tracks. So a
 * catalog-only subscription (a receiver that fetched the catalog but subscribed
 * to no media) does NOT make this true. */
MOQ_API bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s);

/* is_closed: clean close / drained. is_fatal: connect, certificate,
 * protocol, transport, or sender failure (catalog encode, namespace
 * rejected, setup). fatal_code: the sender's own code when sender-fatal,
 * otherwise the endpoint's. */
MOQ_API bool     moq_media_sender_is_closed(const moq_media_sender_t *s);
MOQ_API bool     moq_media_sender_is_fatal(const moq_media_sender_t *s);
MOQ_API uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s);

/* Sender-fatal codes (moq_media_sender_fatal_code). */
#define MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE   0x1u /* MSF encode failed */
#define MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED     0x2u /* publisher / track /
                                                        retained setup failed */
#define MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED 0x3u /* peer refused the
                                                        namespace */

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_SENDER_H */
