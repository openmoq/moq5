#ifndef MOQ_MEDIA_RECEIVER_H
#define MOQ_MEDIA_RECEIVER_H

/*
 * moq_media_receiver_t — the receive-side media facade over an endpoint
 * (MEDIA_SERVICE_DESIGN.md §6). Owns the catalog track subscription on the
 * endpoint's network thread, parses the MSF catalog, and surfaces track
 * discovery as a bounded event queue the application drains from its own
 * thread.
 *
 * Current status: discovery, the object data path, and per-track
 * subscription control are live. Discovery: catalog subscribe + parse,
 * TRACK_ADDED per catalog track with structured codec information,
 * CATALOG_READY exactly once. Objects are parsed, queued, and handed to the
 * application with exclusive ownership via moq_media_receiver_poll_object().
 *
 * Subscription modes:
 *  - auto_subscribe = true: every ELIGIBLE media track is subscribed after
 *    discovery (simple-player default). Not eligible: eventtimeline tracks
 *    (e.g. a CMSF SAP timeline -- subscribe explicitly and drain poll_sap),
 *    mediatimeline tracks (MSF §7 -- discovered but not auto-subscribed;
 *    subscribe explicitly to decode their payload via poll_media_timeline) and
 *    MSF §5.2.2 namespace-override tracks
 *    (subscribe_track returns MOQ_ERR_UNSUPPORTED, as the MSF namespace has no
 *    defined MoQT mapping).
 *  - auto_subscribe = false: discovery-only until the application selects
 *    tracks with moq_media_receiver_subscribe_track() and later
 *    moq_media_receiver_unsubscribe_track() -- the model FFmpeg/VLC/GStreamer
 *    integrations use to discover tracks, build streams/pads, then subscribe
 *    only the selected ones.
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/endpoint.h>
#include <moq/media_object.h>
#include <moq/cmaf.h>
#include <moq/msf.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_media_receiver moq_media_receiver_t;

/* Shared opaque media-track handle (see media_sender.h): each facade
 * completes the struct privately, so including both headers is safe. */
#ifndef MOQ_MEDIA_TRACK_T_DEFINED
#define MOQ_MEDIA_TRACK_T_DEFINED
typedef struct moq_media_track moq_media_track_t;
#endif

/* -- Timestamps (§6.4 tail) ------------------------------------------ */

typedef enum moq_media_time_mode {
    MOQ_MEDIA_TIME_RAW = 0,        /* pass wire timestamps through (default) */
    MOQ_MEDIA_TIME_SHARED_EPOCH    /* rebase reported timestamp fields against
                                      one shared epoch; payload bytes are
                                      never modified */
} moq_media_time_mode_t;

/* -- Overflow policy (§6.4, normative: no silent drop) ---------------- *
 * The receiver owns a bounded object queue; when the application stops
 * polling, the network keeps delivering and blocking the network thread is
 * never an option. The policy is a REQUIRED choice: zero/unset fails
 * creation with MOQ_ERR_INVAL. Plain drop-oldest is deliberately absent
 * (evicting a keyframe while keeping its deltas corrupts the stream). */

typedef enum moq_media_overflow_policy {
    MOQ_MEDIA_OVERFLOW_UNSET = 0,        /* creation fails: MOQ_ERR_INVAL */
    MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, /* live renderers */
    MOQ_MEDIA_OVERFLOW_DROP_GROUP,
    MOQ_MEDIA_OVERFLOW_FLOW_CONTROL      /* pause the media subscriptions
                                            upstream when full; gaps can still
                                            occur from relay live-edge behavior
                                            or budget exhaustion (counted) --
                                            the mechanism is a pause, never a
                                            silent discard */
} moq_media_overflow_policy_t;

typedef struct moq_media_overflow_cfg {
    moq_media_overflow_policy_t policy;  /* REQUIRED: non-zero */
    uint32_t max_objects;                /* object-queue depth; 0 = 256 */
    uint64_t max_bytes;                  /* queued payload byte budget;
                                            0 = 32 MiB */
} moq_media_overflow_cfg_t;

/* -- Configuration (§6.1) --------------------------------------------- */

typedef struct moq_media_receiver_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint; /* REQUIRED by *_create() (the sugar
                                           path owns a private endpoint built
                                           from it); MUST be NULL for
                                           *_attach() -- pointer rather than
                                           embedded struct so "ignored" is
                                           structural, not documented */
    moq_namespace_t namespace_;        /* REQUIRED: what to receive */
    moq_bytes_t catalog_track;         /* empty = "catalog" */
    bool auto_subscribe;               /* true: subscribe every ELIGIBLE media
                                          track after discovery and deliver
                                          objects via poll_object (skips
                                          eventtimeline, mediatimeline and MSF
                                          namespace-override tracks). false:
                                          discover only, then
                                          the app chooses tracks with
                                          moq_media_receiver_subscribe_track() /
                                          _unsubscribe_track() (see below) */
    moq_media_time_mode_t time_mode;   /* RAW (default) | SHARED_EPOCH */
    moq_media_overflow_cfg_t overflow; /* REQUIRED: policy must be set */
    uint32_t max_track_events;         /* track-event queue depth; 0 = 64.
                                          Overflow is terminal (§6.3): losing
                                          a discovery event is unrecoverable
                                          miswiring, never a silent drop. */
} moq_media_receiver_cfg_t;

/* Plain init leaves overflow.policy UNSET on purpose -- a deliberate
 * departure from the everywhere-else convention that zero-init works:
 * the overflow choice is forced, not defaulted. Use the presets. */
MOQ_API void moq_media_receiver_cfg_init(moq_media_receiver_cfg_t *cfg);
MOQ_API void moq_media_receiver_cfg_init_live(moq_media_receiver_cfg_t *cfg);
MOQ_API void moq_media_receiver_cfg_init_flow_control(
    moq_media_receiver_cfg_t *cfg);

/* -- Track description ------------------------------------------------ *
 * Lifetimes (§6.2): every span and array in the description (name, codec, the
 * init buffers, depends[], content_protection_ref_ids[], the timeline
 * metadata) is OWNED by the track handle and stays valid until the receiver is
 * destroyed -- it does NOT borrow from the current effective catalog, so a
 * catalog update that replaces the catalog does not invalidate a handle's desc.
 * A handle's metadata is immutable for its lifetime with ONE exception: a §11.3
 * live->VOD conversion updates the surviving handle's is_live/track_duration in
 * place and fires MOQ_MEDIA_TRACK_UPDATED (per MSF-01 §5.3 every OTHER attribute
 * of a declared Namespace|Name tuple stays fixed). A handle removed from the
 * catalog stays allocated (and its desc readable) until the receiver is
 * destroyed; it just transitions to a terminal state.
 *
 * Because handles (and the content-protection snapshots that keep previously
 * returned find_content_protection() pointers valid) live until the receiver is
 * destroyed, the receiver enforces internal, conservative caps on how much such
 * retained catalog-update state it will accumulate, so a peer streaming endless
 * valid catalog generations cannot exhaust memory. Once a catalog is ready, an
 * update that would exceed a cap is dropped (counted in stats.catalog_drops) and
 * the current effective catalog and all existing handles/pointers are preserved
 * unchanged; the very first catalog exceeding a cap terminalizes the receiver
 * with MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE. The caps are generous relative
 * to real catalogs and are not configurable. */

typedef struct moq_media_track_desc {
    uint32_t    struct_size;
    moq_bytes_t name;             /* catalog track name */
    moq_bytes_t role;             /* empty when absent */
    moq_bytes_t codec;            /* codec string, e.g. "avc1.64001f" */
    moq_bytes_t lang;
    moq_bytes_t label;

    /* Structured info derived via the MSF/media helpers. When derivation
     * was not possible for this track (unknown packaging, missing init
     * data), `info` holds zeroed/unknown values and has_init is false --
     * the track is still announced with its catalog strings. */
    moq_media_track_info_t info;  /* media type, packaging, timescale */
    bool                   has_init;
    /* Parsed decoder config. init.codec_config is the DECODER EXTRADATA
     * (H.264/HEVC SPS/PPS/VPS, AAC AudioSpecificConfig, ...) -- this is what a
     * decoder usually wants to be configured with. Its bytes point into the
     * handle-owned init buffer and stay valid for the handle's lifetime (until
     * the receiver is destroyed), like the other desc fields. For a CENC
     * protected CMAF track (CMSF §4.2) init.has_cenc is true and init.scheme /
     * default_kid / default_is_protected / default_per_sample_iv_size carry the
     * tenc parameters for license acquisition (libmoq does not decrypt). */
    moq_cmaf_init_info_t   init;  /* codec kind, dimensions, sample rate,
                                     channels, codec_config (decoder extradata),
                                     CENC protection params */
    /* The track's full container/init segment from the catalog, verbatim: for
     * CMAF the initialization segment (ftyp + moov); for RAW/LOC whatever the
     * sender supplied as init_data. Muxers/recorders that reproduce the
     * container want this; decoders usually want init.codec_config above.
     * Surfaced at TRACK_ADDED; empty when the track carries none. Points into
     * the handle-owned init buffer and follows the handle lifetime. */
    moq_bytes_t init_data;

    /* Catalog passthroughs for fields the structured info may not carry
     * (zero / false when absent) -- the non-CMAF path needs these to build
     * caps/stream parameters. */
    bool        has_width;
    uint32_t    width;
    bool        has_height;
    uint32_t    height;
    bool        has_samplerate;
    uint32_t    samplerate;
    moq_bytes_t channel_config;   /* catalog string, e.g. "2"; empty when
                                     absent */
    bool        has_framerate;
    uint64_t    framerate_millis; /* frames per second * 1000 */
    bool        has_bitrate;
    uint64_t    bitrate;

    /* CMSF §3.5.2: maximum SAP starting types (0 / has_* false when absent).
     * uint32_t to match the catalog model's lenient, range-unenforced value. */
    bool        has_max_grp_sap;
    uint32_t    max_grp_sap;
    bool        has_max_obj_sap;
    uint32_t    max_obj_sap;

    /* CMSF §4.1.2: this track's content-protection reference ids. Owned by the
     * handle (valid for the handle's lifetime, like the other desc spans);
     * empty when the track is unprotected. Resolve each id to its DRM entry
     * with moq_media_receiver_find_content_protection(). libmoq carries this
     * metadata only -- decryption is the app/CDM's job. */
    const moq_bytes_t *content_protection_ref_ids;
    size_t             content_protection_ref_id_count;

    /* Raw catalog packaging string, verbatim (e.g. "cmaf", "loc",
     * "eventtimeline", "mediatimeline"). info.packaging only distinguishes media
     * RAW/CMAF, so this is how a consumer recognizes a non-media track such as a
     * CMSF SAP event timeline or an MSF §7 media timeline. Owned by the handle. */
    moq_bytes_t packaging;
    /* Timeline catalog metadata (all owned by the handle; empty when absent):
     *   - event_type: MSF §5.2.5 eventType -- ONLY for packaging
     *     "eventtimeline" (e.g. a CMSF SAP timeline, "org.ietf.moq.cmsf.sap").
     *   - mime_type: §5.2.19 mimeType -- surfaced for "eventtimeline" and
     *     "mediatimeline" tracks ("application/json"), and any other track that
     *     carries one.
     *   - depends: §5.2.14 applicable media track names -- surfaced for
     *     "eventtimeline" and "mediatimeline" tracks, and any track that
     *     declares one.
     * Use these to identify a SAP timeline track (then poll_sap), or a media
     * timeline track (MSF §7; subscribe explicitly, then poll_media_timeline). */
    moq_bytes_t event_type;
    moq_bytes_t mime_type;
    const moq_bytes_t *depends;
    size_t             depends_count;

    /* MSF §7.4 inline media timeline template (5.2.15), valid when has_template.
     * A fixed-duration-segment timeline carried on a media track; copied
     * verbatim from the catalog (the eight decoded values). Owned by the handle
     * (a plain value, no backing allocation). */
    bool                     has_template;
    moq_msf_media_template_t template_;

    /* MSF §5.2.35 track duration in integer milliseconds, valid when
     * has_track_duration. Present only on a non-live (VOD/ended) track; a plain
     * value, no backing allocation. */
    bool                     has_track_duration;
    uint64_t                 track_duration_ms;

    /* MSF §5.2.7 isLive. A live→VOD conversion (§11.3) is the one sanctioned
     * mutation of a surviving tuple: is_live flips true→false (and
     * track_duration may appear) on the SAME handle, which fires a
     * MOQ_MEDIA_TRACK_UPDATED event. A plain value, no backing allocation. */
    bool                     is_live;
} moq_media_track_desc_t;

/* -- Track discovery events (§6.2) ------------------------------------ */

typedef enum moq_media_track_event_kind {
    MOQ_MEDIA_TRACK_ADDED = 1,   /* new tuple discovered (any catalog generation) */
    MOQ_MEDIA_TRACK_UPDATED,     /* a surviving tuple converted live->VOD (§11.3):
                                    is_live flipped true->false and/or
                                    track_duration appeared on the SAME handle.
                                    This is the ONLY sanctioned mutation of a
                                    declared tuple (§5.2.7); all other metadata
                                    changes remain immutable-tuple violations
                                    (§5.3) and are dropped. The handle's desc is
                                    updated in place before this fires. */
    MOQ_MEDIA_TRACK_REMOVED,     /* tuple dropped by a later catalog generation */
    MOQ_MEDIA_TRACK_ENDED,       /* peer end-of-track / rejection */
    MOQ_MEDIA_CATALOG_READY      /* first (independent) catalog enumerated; fires
                                    exactly once per connection (later generations
                                    emit only ADDED/REMOVED) */
} moq_media_track_event_kind_t;

typedef struct moq_media_track_event {
    uint32_t                      struct_size; /* ABI guard: poll_track stamps the
                                                  bytes written -- min(your sizeof,
                                                  the library's). May be < your
                                                  sizeof when an older library
                                                  fills a newer caller's struct. */
    moq_media_track_event_kind_t  kind;
    moq_media_track_t            *track; /* stable handle; NULL for
                                            CATALOG_READY. Valid from TRACK_ADDED
                                            until the receiver is destroyed
                                            (handles are never recycled). After a
                                            track ENDs OR is REMOVED from the
                                            catalog the handle stays valid but
                                            inert -- subscribe_track() /
                                            unsubscribe_track() / track_state()
                                            refuse it with MOQ_ERR_WRONG_STATE. */
    const moq_media_track_desc_t *desc;  /* handle-owned (see lifetime note
                                            above); NULL for CATALOG_READY */
    uint32_t config_generation;          /* reserved for dynamic config;
                                            always 0 today */
} moq_media_track_event_t;

/* The description for a track handle (handle-owned; same lifetime as the
 * TRACK_ADDED event's desc). NULL for a NULL track. */
MOQ_API const moq_media_track_desc_t *moq_media_track_desc_get(
    const moq_media_track_t *track);

/* Resolve a content-protection reference id (from
 * desc.content_protection_ref_ids, CMSF §4.1.2) to its root content protection
 * entry (§4.1.1). Resolution is against the CURRENT effective catalog, so after
 * a catalog update it reflects the latest contentProtections; NULL if r is
 * NULL or id has no match in the current catalog. Any entry pointer this
 * returned previously stays valid until the receiver is destroyed -- a CP-
 * bearing effective catalog is retained as a snapshot when replaced. libmoq
 * carries this DRM metadata only -- decryption is the app/CDM. */
MOQ_API const moq_cmsf_content_protection_t *
moq_media_receiver_find_content_protection(
    const moq_media_receiver_t *r, moq_bytes_t ref_id);

/* -- Per-track subscription control (§6.2) ---------------------------- *
 * With auto_subscribe = false the receiver discovers tracks but subscribes
 * none until the application selects them. This is the integration model for
 * FFmpeg/VLC/GStreamer: poll TRACK_ADDED, build streams/pads, then subscribe
 * only the chosen tracks, and enable/disable later. (auto_subscribe = true
 * subscribes every eligible media track after discovery -- skipping
 * eventtimeline, mediatimeline and namespace-override tracks -- and is the
 * simple-player default.)
 *
 * Both calls are ASYNCHRONOUS commands: MOQ_OK means the request was
 * validated and recorded as desired state, NOT that the peer has accepted.
 * Reconciliation (issuing the SUBSCRIBE, or pausing/resuming delivery) happens
 * on the endpoint's network thread. Both are valid once a track's TRACK_ADDED
 * has been observed (they do not require CATALOG_READY) and are idempotent.
 *
 * Disabling a track pauses its delivery rather than tearing the subscription
 * down, so enable/disable is a cheap, repeatable toggle -- a consumer may flip
 * a track on and off arbitrarily many times without exhausting any resource.
 *
 * This is a delivery PAUSE/RESUME on the existing subscription, NOT a seek or
 * reconfiguration: the start mode and priority are applied only when the
 * underlying subscription is first issued. A later subscribe_track (resume) does
 * not reapply cfg and does not rejoin at a new requested point -- to change the
 * start point or priority the subscription would have to end first.
 *
 * Naming: the verbs are subscribe/unsubscribe because they express the
 * application's INTENT ("I want / don't want this track"); the implementation
 * realizes a disable as a delivery pause (above). moq_media_receiver_track_state
 * reports the resulting state, distinguishing an app pause (PAUSED_APP) from
 * automatic FLOW_CONTROL backpressure (PAUSED_FLOW). */

typedef enum moq_media_start_mode {
    MOQ_MEDIA_START_CURRENT = 0,   /* live edge: the current/largest object
                                      (default) */
    MOQ_MEDIA_START_NEXT_GROUP     /* the next group boundary (clean future
                                      join) */
} moq_media_start_mode_t;

typedef struct moq_media_receiver_track_subscribe_cfg {
    uint32_t               struct_size;
    moq_media_start_mode_t  start;          /* CURRENT (default) | NEXT_GROUP */
    bool                    has_priority;   /* set the subscriber priority */
    uint8_t                 priority;       /* used when has_priority */
} moq_media_receiver_track_subscribe_cfg_t;

MOQ_API void moq_media_receiver_track_subscribe_cfg_init(
    moq_media_receiver_track_subscribe_cfg_t *cfg);

/* Schedule a subscription for `track` (a handle from a polled TRACK_ADDED).
 * cfg NULL == defaults (CURRENT start, no priority).
 *
 * The call is atomic: it records desired state and schedules reconciliation
 * with a non-allocating wake, so MOQ_OK means the change is committed (it never
 * partially succeeds).
 *
 * Returns:
 *   MOQ_OK            desired state recorded and reconciliation scheduled
 *   MOQ_ERR_INVAL     NULL r/track, a foreign track, or a malformed cfg
 *                     (struct_size too small, or an unknown `start` value)
 *   MOQ_ERR_WRONG_STATE  the track has already ended (TRACK_ENDED): a
 *                     rejected/finished track cannot be (re)subscribed
 *   MOQ_ERR_CLOSED    the receiver is terminal
 *
 * Objects for the track surface via poll_object after the peer accepts; a peer
 * rejection surfaces MOQ_MEDIA_TRACK_ENDED for that track and does not fail the
 * receiver.
 *
 * cfg lifetime: the start mode and priority are applied ONLY when the underlying
 * subscription is first issued for this track. Any later subscribe_track call --
 * while the first subscription is still pending, already active, or paused by a
 * prior unsubscribe_track -- is an idempotent enable/resume command: it (re)sets
 * the track to "wanted" but does NOT change the start mode or priority of the
 * existing subscription (a non-NULL cfg is validated but otherwise has no effect
 * there). Changing those would require the subscription to end first. */
MOQ_API moq_result_t moq_media_receiver_subscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_media_receiver_track_subscribe_cfg_t *cfg);

/* Schedule an unsubscribe (disable) for `track`: stops future delivery and
 * PURGES any already-queued objects for it (so a consumer can disable a stream
 * without draining stale packets). Delivery is paused at the subscription level,
 * not torn down, so a later moq_media_receiver_subscribe_track resumes it
 * cheaply -- repeated enable/disable never accumulates state. The catalog track
 * and the track's TRACK_ADDED handle remain valid -- only delivery stops.
 * Atomic like subscribe_track: MOQ_OK / MOQ_ERR_INVAL / MOQ_ERR_CLOSED; a no-op
 * (and MOQ_OK) for an already-unsubscribed track. */
MOQ_API moq_result_t moq_media_receiver_unsubscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track);

/* A track's current delivery state, for UI/status without inferring it from the
 * event stream. The two pause states are distinct ON PURPOSE: PAUSED_APP is the
 * application's own unsubscribe_track (it stays paused until the app re-enables);
 * PAUSED_FLOW is automatic FLOW_CONTROL backpressure (it resumes on its own once
 * the queue drains). Collapsing them would hide that difference from a consumer
 * and from resume logic. */
typedef enum moq_media_track_state {
    MOQ_MEDIA_TRACK_STATE_DISCOVERED = 0, /* in the catalog, never subscribed */
    MOQ_MEDIA_TRACK_STATE_PENDING,        /* subscribe issued, not yet active */
    MOQ_MEDIA_TRACK_STATE_ACTIVE,         /* subscribed and delivering */
    MOQ_MEDIA_TRACK_STATE_PAUSED_APP,     /* disabled by unsubscribe_track */
    MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW,    /* paused by FLOW_CONTROL backpressure */
    MOQ_MEDIA_TRACK_STATE_ENDED           /* ended (peer reject / end-of-track) */
} moq_media_track_state_t;

/* Write `track`'s current state to *out (a known handle from TRACK_ADDED).
 * Returns MOQ_ERR_INVAL for a NULL r/track/out or a foreign track (these are
 * programmer errors, surfaced rather than hidden); MOQ_OK otherwise, with *out
 * set (a terminal receiver or an ended track yields MOQ_MEDIA_TRACK_STATE_ENDED).
 * The value is a point-in-time snapshot: reconciliation runs on the network
 * thread, so PENDING may advance to ACTIVE (etc.) by the time the caller acts. */
MOQ_API moq_result_t moq_media_receiver_track_state(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_state_t *out);

/* -- Objects (§6.3) ---------------------------------------------------- *
 * The validated parsed shape (moq_media_parsed_object_t fields) plus
 * exactly two additions: the TRACK_ADDED handle and the config
 * generation. poll_object returning MOQ_OK TRANSFERS EXCLUSIVE OWNERSHIP
 * of the object's backing buffers to the caller: the payload/fragment
 * spans stay valid until moq_media_object_cleanup(&obj), which releases
 * them on the calling thread (legal because the handoff is a shard
 * transfer -- the service retains zero references once an object is
 * dequeued). Holding several polled objects simultaneously is legal. On
 * any non-OK return nothing was transferred and there is nothing to
 * clean up.
 *
 * Zero-copy posture (v0): cleanup runs on the polling thread and moq_rcbuf_t
 * refcounts are non-atomic, so the transferred buffers must NOT be handed to a
 * framework buffer whose finalizer runs on another thread (GstBuffer
 * destroy-notify, FFmpeg AVBufferRef free). Copy payload/fragment into a
 * framework-owned buffer for downstream. True cross-thread zero-copy ingest
 * would require a future, separate thread-safe media-buffer API -- not
 * moq_rcbuf_t (see BACKLOG: "Thread-safe zero-copy receive export"). */

typedef struct moq_media_object {
    uint32_t              struct_size;        /* ABI guard: poll_object stamps the
                                                 bytes written -- min(your sizeof,
                                                 the library's). May be < your
                                                 sizeof when an older library
                                                 fills a newer caller's struct. */
    moq_media_track_t    *track;             /* the TRACK_ADDED handle */
    uint32_t              config_generation; /* reserved; always 0 today */

    moq_media_packaging_t packaging;
    moq_object_status_t   status;
    bool                  end_of_group;
    bool                  datagram;
    bool                  keyframe;

    bool                  has_capture_time;
    uint64_t              capture_time_us;
    uint64_t              decode_time_us;
    int64_t               composition_offset_us;
    uint64_t              presentation_time_us;

    /* Where the media bytes live depends on `packaging`:
     *   - RAW / LOC / simple objects: the media is `payload` (fragment is
     *     empty). Decode from `payload`.
     *   - CMAF objects: `payload` is empty; the full CMAF fragment (the
     *     container framing) is preserved in `fragment`. The sample/media
     *     bytes are the mdat box contents: `fragment.data + mdat_offset` for
     *     `mdat_len` bytes. Decoders/muxers that need the framing keep
     *     `fragment`; samplers read the mdat slice. Do NOT decode from
     *     `payload` for CMAF -- it is empty. */
    moq_bytes_t           payload;     /* RAW/LOC media bytes (empty for CMAF) */
    moq_bytes_t           fragment;    /* CMAF: full fragment incl. container
                                          framing (empty for RAW/LOC) */
    size_t                mdat_offset; /* CMAF: media (mdat) byte offset in fragment */
    size_t                mdat_len;    /* CMAF: media (mdat) byte length in fragment */

    size_t                   sample_count;
    const moq_cmaf_sample_t *samples;

    /* Owned by the object until cleanup; treat as opaque. */
    moq_rcbuf_t          *payload_ref;
    moq_rcbuf_t          *properties_ref;
    moq_cmaf_sample_t    *samples_owned;
} moq_media_object_t;

/* Release the buffers a successful poll transferred. Safe on a zeroed
 * struct; idempotent. Bounds its work by the obj's stamped struct_size, so it
 * never touches past the caller's struct. */
MOQ_API void moq_media_object_cleanup(moq_media_object_t *obj);

/* Dequeue the next media object into *obj. Pass `obj_size = sizeof(*obj)` as YOU
 * compiled it; the library requires at least the frozen v0 base size, writes at
 * most min(obj_size, its own struct size), and stamps obj->struct_size with the
 * bytes written -- so a newer caller can detect truncation by an older library
 * and a newer library never writes past an older caller's struct. MOQ_OK
 * transfers ownership (see above), MOQ_DONE = nothing now, MOQ_ERR_INTERRUPTED
 * while the endpoint latch is set, MOQ_ERR_CLOSED when empty AND terminal,
 * MOQ_ERR_INVAL for NULL args or obj_size below the v0 base size. An object for a
 * track is never pollable before that track's TRACK_ADDED has been queued --
 * drain track events before objects on each wakeup and every handle is known
 * before its first object. */
MOQ_API moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r,
                                                    moq_media_object_t *obj,
                                                    size_t obj_size);

/* -- SAP event-timeline records (CMSF §3.6.1) ------------------------- *
 * A CMSF SAP-type timeline track (packaging "eventtimeline", eventType
 * "org.ietf.moq.cmsf.sap") carries a JSON timeline of Stream Access Points,
 * NOT media. Such tracks are NOT auto-subscribed; an application that wants the
 * timeline identifies the track from its TRACK_ADDED desc (packaging /
 * event_type) and subscribes it explicitly with
 * moq_media_receiver_subscribe_track(). Its objects never surface via
 * poll_object -- the receiver parses the JSON payload and delivers typed
 * records here instead. Records are plain values (no owned buffers): nothing to
 * clean up. The receiver de-duplicates the independent-object re-sends (MSF
 * §8.3), so each (track, group, object) SAP surfaces once. */

typedef struct moq_media_sap_record {
    uint32_t           struct_size;
    moq_media_track_t *track;     /* the eventtimeline TRACK_ADDED handle */
    uint64_t           group;     /* media Location group  (record index "l"[0]) */
    uint64_t           object;    /* media Location object (record index "l"[1]) */
    moq_sap_type_t     sap_type;  /* CMSF §3.6.1 data[0]: NONE/1/2/3 */
    uint64_t           ept_ms;    /* earliest presentation time, ms (data[1]) */
} moq_media_sap_record_t;

/* Dequeue the next SAP timeline record into *out. Pass `out_size =
 * sizeof(*out)`; same size contract as poll_object (requires the v0 base,
 * writes min(out_size, current), stamps the bytes written). Returns MOQ_OK with
 * *out filled, MOQ_DONE when none queued, MOQ_ERR_CLOSED when empty AND
 * terminal, MOQ_ERR_INVAL for NULL args or out_size below the v0 base. A
 * malformed timeline payload is counted in stats.parse_drops and is never
 * fatal. The bounded record queue drops oldest under sustained non-polling. */
MOQ_API moq_result_t moq_media_receiver_poll_sap(moq_media_receiver_t *r,
                                                 moq_media_sap_record_t *out,
                                                 size_t out_size);

/* -- Media timeline records (MSF §7.1.1) ------------------------------ *
 * An MSF media-timeline track (packaging "mediatimeline") carries a JSON
 * timeline relating MOQT Locations to media and wallclock time, NOT media. Such
 * tracks are discovered (their packaging/mimeType/depends surface on the desc)
 * but are NOT auto-subscribed. An application that wants the timeline subscribes
 * the track explicitly with moq_media_receiver_subscribe_track(); its objects
 * never surface via poll_object -- the receiver decodes each §7.1.1 explicit
 * payload and delivers typed records here instead. Records are plain values (no
 * owned buffers): nothing to clean up. The receiver de-duplicates the §7.3
 * independent-object re-sends by media Location, so each (track, record.group,
 * record.object) surfaces once. Sender-side generation of these tracks is not
 * implemented yet. */

typedef struct moq_media_timeline_record {
    uint32_t           struct_size;
    moq_media_track_t *track;          /* the mediatimeline TRACK_ADDED handle */
    uint64_t           media_time_ms;  /* §7.1.1 item 0: presentation time (ms) */
    uint64_t           group;          /* §7.1.1 item 1[0]: MOQT Group ID */
    uint64_t           object;         /* §7.1.1 item 1[1]: MOQT Object ID */
    uint64_t           wallclock_ms;   /* §7.1.1 item 2: encode wallclock (ms;
                                          0 = VOD/unknown) */
} moq_media_timeline_record_t;

/* Dequeue the next media-timeline record into *out. Same size/return contract as
 * poll_sap: pass `out_size = sizeof(*out)`; requires the v0 base, writes
 * min(out_size, current), stamps the bytes written. Returns MOQ_OK with *out
 * filled, MOQ_DONE when none queued, MOQ_ERR_CLOSED when empty AND terminal,
 * MOQ_ERR_INVAL for NULL args or out_size below the v0 base. A malformed timeline
 * payload is counted in stats.parse_drops and is never fatal. The bounded record
 * queue drops oldest under sustained non-polling. */
MOQ_API moq_result_t moq_media_receiver_poll_media_timeline(
    moq_media_receiver_t *r,
    moq_media_timeline_record_t *out,
    size_t out_size);

/* -- Stats (§6.4: every drop is counted, never silent) ----------------- */

typedef struct moq_media_receiver_stats {
    uint32_t struct_size;
    uint64_t objects_received;    /* media objects accepted for a known track,
                                     queued or dropped (malformed objects are
                                     counted in parse_drops instead) */
    uint64_t objects_queued;      /* currently in the queue */
    uint64_t bytes_queued;        /* payload bytes currently queued */
    uint64_t objects_dropped;     /* received objects dropped (policy eviction or
                                     a disabled/unsubscribed track); a subset of
                                     objects_received */
    uint64_t groups_dropped;      /* whole groups discarded */
    uint64_t keyframes_dropped;
    uint64_t parse_drops;         /* media-level parse failures (counted,
                                     per-object, never terminal) */
    uint64_t overflow_events;     /* times a policy had to act */
    uint64_t pause_transitions;   /* FLOW_CONTROL pause/resume edges */
    bool     paused;              /* FLOW_CONTROL currently pausing */
    /* Appended after v0: non-fatal catalog-update failures -- a malformed or
     * unusable later catalog object, or an immutable-tuple violation (§5.3) --
     * each dropped without disturbing the current effective catalog. */
    uint64_t catalog_drops;
    /* Appended: true once an effective independent catalog with isComplete
     * (MSF §5.1.3) has been adopted -- the broadcast is terminally complete (no
     * new tracks/content). Latches true and stays true; any later catalog
     * object is then dropped (counted in catalog_drops). Lets an app observe
     * completion beyond the per-track TRACK_REMOVED events a terminating
     * (empty-tracks) catalog emits. */
    bool     catalog_complete;
} moq_media_receiver_stats_t;

/* Snapshot stats into *out. Pass `out_size = sizeof(*out)`; same size contract as
 * poll_object (requires the v0 base, writes min(out_size, current), stamps the
 * bytes written). MOQ_ERR_INVAL for NULL args or out_size below the v0 base.
 *
 * ABI generations (the v0 prefix is frozen at `paused`):
 *   - a v0 caller (out_size through `paused`) gets the fields up to `paused`;
 *   - a caller built against newer headers and passing
 *     sizeof(moq_media_receiver_stats_t) gets the appended `catalog_drops` and
 *     `catalog_complete` too;
 *   - any in-between out_size is clamped by prefix (you get a field only if the
 *     full field fits in out_size). */
MOQ_API moq_result_t moq_media_receiver_get_stats(
    const moq_media_receiver_t *r, moq_media_receiver_stats_t *out,
    size_t out_size);

/* -- Lifecycle (§5.5 ownership) --------------------------------------- */

/* Borrow an existing endpoint (cfg->endpoint MUST be NULL). The endpoint
 * must outlive the receiver; moq_endpoint_stop() refuses with
 * MOQ_ERR_WRONG_STATE while the receiver is attached. v0 attachment
 * contract: at most one receiver per endpoint (a second attach returns
 * MOQ_ERR_WRONG_STATE); a receiver and a sender MAY share one endpoint
 * (one attachment slot per kind). Blocking waits share the endpoint's
 * single coalesced activity wake, so multiplex them from one app thread
 * rather than parking one thread per attachment. */
MOQ_API moq_result_t moq_media_receiver_attach(moq_endpoint_t *ep,
                                               const moq_media_receiver_cfg_t *cfg,
                                               moq_media_receiver_t **out);

/* Own a private endpoint built from cfg->endpoint (REQUIRED here). */
MOQ_API moq_result_t moq_media_receiver_create(const moq_media_receiver_cfg_t *cfg,
                                               moq_media_receiver_t **out);

/* Detaches from the endpoint (decrementing its attachment count) and frees
 * everything the receiver retained -- all track handles and borrowed
 * description spans die here. A receiver that owns its endpoint stops and
 * destroys it too. void, like every libmoq destroy. */
MOQ_API void moq_media_receiver_destroy(moq_media_receiver_t *r);

/* Block until something is pollable, the receiver wakes or turns terminal,
 * or timeout_us elapses. Level-triggered on ALL four receive queues: a
 * queued track event, media object, SAP record, or media-timeline record
 * returns MOQ_OK immediately (before and after the underlying wait), so a
 * check-then-wait loop over any poll_* surface is race-free. Returns MOQ_OK
 * on wake/available, MOQ_DONE on timeout, MOQ_ERR_INTERRUPTED while the
 * endpoint latch is set, MOQ_ERR_CLOSED once terminal (queued items still
 * drain via poll). Reflects the underlying endpoint's terminal state
 * identically in attach and create modes. */
MOQ_API moq_result_t moq_media_receiver_wait(moq_media_receiver_t *r,
                                             uint64_t timeout_us);

/* Drain the track-event queue into *ev. Pass `ev_size = sizeof(*ev)`; same size
 * contract as poll_object (requires the v0 base, writes min(ev_size, current),
 * stamps the bytes written). Returns MOQ_OK with *ev filled, MOQ_DONE when empty,
 * MOQ_ERR_CLOSED when empty AND terminal, MOQ_ERR_INVAL for NULL args or ev_size
 * below the v0 base. Events queued before a terminal transition remain pollable. */
MOQ_API moq_result_t moq_media_receiver_poll_track(moq_media_receiver_t *r,
                                                   moq_media_track_event_t *ev,
                                                   size_t ev_size);

/* is_closed: clean close / drained. is_fatal: connect, certificate,
 * protocol, transport, or receiver failure (catalog unusable, track-event
 * overflow). fatal_code: the receiver's own code when receiver-fatal,
 * otherwise the endpoint's. */
MOQ_API bool     moq_media_receiver_is_closed(const moq_media_receiver_t *r);
MOQ_API bool     moq_media_receiver_is_fatal(const moq_media_receiver_t *r);
MOQ_API uint64_t moq_media_receiver_fatal_code(const moq_media_receiver_t *r);

/* Receiver-fatal codes (moq_media_receiver_fatal_code). */
#define MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE 0x1u /* parse/derive failed */
#define MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW   0x2u /* track-event queue
                                                          overflow (§6.3) */
#define MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED     0x3u /* catalog subscription
                                                          could not be issued */
#define MOQ_MEDIA_RECEIVER_FATAL_CATALOG_REJECTED 0x4u /* the publisher refused
                                                          the catalog
                                                          subscription */

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_RECEIVER_H */
