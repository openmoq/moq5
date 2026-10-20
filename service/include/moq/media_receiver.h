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
 * With the D22 companion, initial discovery waits for the retained
 * catalog fill to complete: its independent base and all deltas through the
 * initial ACK's Largest must be present. Live catalog updates are staged in
 * the existing bounded object ring, not exposed as media or used to rescue
 * failed history. A failed/incomplete fill or staging-capacity refusal latches
 * CATALOG_UNUSABLE on this receiver, not its shared endpoint. An ACK without
 * Largest followed by NO_STREAM instead waits for the first live independent
 * catalog. Draft-16/18 retain the Joining FETCH bootstrap.
 *
 * Subscription modes. One rule decides what can be subscribed: the receiver
 * has a DELIVERY PATH for a track when it is a media track whose structured
 * info derived from the catalog and whose packaging this build parses
 * (poll_object), or a CMSF SAP timeline (poll_sap) or MSF §7 media timeline
 * (poll_media_timeline). subscribe_track refuses any other track with
 * MOQ_ERR_UNSUPPORTED: a track whose info did not derive (e.g. an unknown
 * packaging), an MPEG2TS track in a build without moq::mpeg2ts, an
 * eventtimeline of another kind, and an MSF §5.2.2 namespace-override track
 * (the MSF namespace has no defined MoQT mapping).
 *  - auto_subscribe = true: every media track with a delivery path is
 *    subscribed after discovery (simple-player default); timeline tracks are
 *    not -- subscribe them explicitly.
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
    uint64_t max_bytes;                  /* 0 = 32 MiB. Policy threshold B:
                                         * payload bytes, plus each D22 object's
                                         * private track-properties bytes,
                                         * reserved before copy allocation.
                                         * FLOW_CONTROL absorbs through
                                         * B + floor(B/2); other policies cap B.
                                         * D16/18 remains payload-only. */
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
    bool auto_subscribe;               /* true: subscribe every media track
                                          with a delivery path (see the top of
                                          this header) after discovery and
                                          deliver objects via poll_object.
                                          MPEG2TS es-packets tracks are media
                                          tracks too: each carries one PID, and
                                          combining them with the PAT/PMT
                                          tracks is the app's job. false:
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
    /* Permanently reserved, never read: covers the tail padding the struct
     * had after max_track_events on targets where it had any, so no appended
     * field can be read from an older caller's uninitialized padding. */
    uint8_t  _reserved_cfg[4];
    /* Appended after max_track_events. MPEG2TS tracks only: each subscribed
     * TS track releases its Objects in Location order through a reorder
     * window (see moq_media_object_t). Slots held per track (0 = 64) and how
     * long an Object may wait for those before it (0 = 300 ms). */
    uint32_t mpeg2ts_reorder_slots;
    uint64_t mpeg2ts_reorder_window_us;
    /* Remove the four-octet prefix of 192-octet source packets, so every
     * delivered payload of such a track is 188-octet TS packets
     * (draft-gregoire-moq-msfts, Timestamp Mode: a receiver expecting 188
     * MUST remove it). The catalog's packet size on the desc is unchanged. */
    bool     mpeg2ts_strip_prefix;
    /* Prefix the first Object delivered for each subscription -- LIVE, or
     * from one of the subscription's fills, whichever comes first -- with
     * the catalog's PAT/PMT packets (the track's init_data), their
     * continuity counters aligned to that Object (Use of MSF Initialization
     * Data). Delivered means returned by poll_object: an Object refused,
     * dropped or evicted before it is polled never takes it. No later Object
     * of the subscription carries it. */
    bool     mpeg2ts_join_psi;
    /* Media companion. AUTO is the default: an absent or partially covered
     * field, and every _sized initializer that covers it, select it. A zeroed
     * field is LEGACY_MSF01_LOC01, not AUTO. AUTO resolves once, when the
     * receiver first observes a draft-16/18/22 session and before anything
     * is subscribed, to the matching companion (LEGACY_MSF01_LOC01 on D16/18,
     * D22_MSF01_LOC04_PUBLIC on D22) and then behaves exactly as it does,
     * including poll_object's draft-22 size floor. With SHARED_EPOCH, AUTO
     * has no draft-22 match. Explicit LEGACY_MSF01_LOC01 requires negotiated
     * D16/18; explicit D22_MSF01_LOC04_PUBLIC requires negotiated D22 and RAW
     * time mode (SHARED_EPOCH is UNSUPPORTED at creation without an explicit
     * clock mapping). A negotiated mismatch, or an unmatched AUTO, reports
     * receiver-local FATAL_SETUP_FAILED; it does not close the
     * endpoint/session. Unknown values are MOQ_ERR_INVAL. Aligned as uint64_t (the
     * struct's own alignment), so it starts at the complete sizeof of the
     * layout that ends with mpeg2ts_join_psi on every target: a caller
     * passing that sizeof never covers it with its tail padding. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    moq_media_companion_profile_t companion_profile;
} moq_media_receiver_cfg_t;

/* Frozen v0 sizeof includes tail padding. Use the original embedded
 * aggregate's alignment: standalone uint64_t can be more strongly aligned
 * than its containing struct on i686. Future tails must not change it. */
#ifdef __cplusplus
#define MOQ_MEDIA_RECEIVER_CFG_V0_ALIGNMENT \
    (alignof(void *) > alignof(moq_media_overflow_cfg_t) \
        ? alignof(void *) : alignof(moq_media_overflow_cfg_t))
#else
#define MOQ_MEDIA_RECEIVER_CFG_V0_ALIGNMENT \
    (_Alignof(void *) > _Alignof(moq_media_overflow_cfg_t) \
        ? _Alignof(void *) : _Alignof(moq_media_overflow_cfg_t))
#endif
/* The receiver
 * accepts any struct_size at or above it and reads an appended field only
 * when struct_size covers the whole field. */
#define MOQ_MEDIA_RECEIVER_CFG_V0_SIZE \
    ((offsetof(moq_media_receiver_cfg_t, max_track_events) + sizeof(uint32_t) + \
      MOQ_MEDIA_RECEIVER_CFG_V0_ALIGNMENT - 1) & \
     ~(size_t)(MOQ_MEDIA_RECEIVER_CFG_V0_ALIGNMENT - 1))

/* Plain init leaves overflow.policy UNSET on purpose -- a deliberate
 * departure from the everywhere-else convention that zero-init works:
 * the overflow choice is forced, not defaulted. Use the presets.
 *
 * These pointer-only initializers clear and stamp ONLY the frozen v0 prefix
 * (MOQ_MEDIA_RECEIVER_CFG_V0_SIZE, the complete frozen old config), so the
 * appended mpeg2ts_ fields and companion_profile stay at their defaults
 * (companion_profile: AUTO). To set any of them, use the _sized forms below.
 * Every initializer leaves the companion AUTO. No initializer writes the new
 * tail or beyond an old caller's allocation. */
MOQ_API void moq_media_receiver_cfg_init(moq_media_receiver_cfg_t *cfg);
MOQ_API void moq_media_receiver_cfg_init_live(moq_media_receiver_cfg_t *cfg);
MOQ_API void moq_media_receiver_cfg_init_flow_control(
    moq_media_receiver_cfg_t *cfg);

/* Sized forms: clear and stamp min(cfg_size, this library's struct size) and
 * apply the preset only where the prefix covers it (each preset field is
 * assigned only when completely covered). Safe for a caller compiled against
 * an older, smaller header too. No-op if cfg is NULL or cfg_size cannot hold
 * struct_size. Bytes beyond the library's known record stay untouched. */
MOQ_API void moq_media_receiver_cfg_init_sized(moq_media_receiver_cfg_t *cfg,
                                               size_t cfg_size);
MOQ_API void moq_media_receiver_cfg_init_live_sized(
    moq_media_receiver_cfg_t *cfg, size_t cfg_size);
MOQ_API void moq_media_receiver_cfg_init_flow_control_sized(
    moq_media_receiver_cfg_t *cfg, size_t cfg_size);

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
     * data, an mpeg2ts track that fails moq_msf_track_mpeg2ts_check), `info`
     * holds zeroed/unknown values and has_init is false -- the track is
     * still announced with its catalog strings, and has no delivery path.
     * An mpeg2ts track derives packaging MPEG2TS; its packet size is in the
     * typed extensions below. Its init_data (below) is the catalog's PSI
     * source packets, and init.codec_config stays empty. An es-packets track
     * carries the packets of one PID. */
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

    /* Never read: reserves the original tail padding on ABIs whose uint64_t
     * alignment exceeds pointer alignment. Appended data must not occupy it. */
    uint32_t _reserved_desc;
    /* Appended after is_live; read only when the stamped struct_size covers
     * them. Both are immutable for the handle's life.
     *
     * The catalog's packaging-extension records (see moq_msf_track_ext_t);
     * read the MPEG-2 TS record with
     * moq_msf_ext_mpeg2ts(extensions, extension_count). Handle-owned: the
     * entries, the records and their spans and arrays stay valid until the
     * receiver is destroyed. Empty when the track carries none. */
    const moq_msf_track_ext_t *extensions;
    size_t                     extension_count;
    /* MSF altGroup, for every packaging (0 / false when absent). */
    bool                     has_alt_group;
    int                      alt_group;
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
    MOQ_MEDIA_CATALOG_READY,     /* first (independent) catalog enumerated; fires
                                    exactly once per connection (later generations
                                    emit only ADDED/REMOVED) */
    MOQ_MEDIA_TRACK_UPDATE_OK,   /* the peer acknowledged an APP-initiated
                                    pause/resume for this track . Fires only
                                    for wire acknowledgments attributed to a
                                    subscribe_track/unsubscribe_track intent
                                    change; internal flow-control reconciliation
                                    is never surfaced. The appended
                                    has_largest/expires fields below are
                                    OBSERVATIONAL (where delivery resumed
                                    relative to what was missed); the receiver
                                    issues no catch-up FETCH. */
    MOQ_MEDIA_TRACK_PARSE_DROP,  /* one or more media objects for this track
                                    failed to parse and were dropped. NON-FATAL:
                                    a malformed object never tears down the
                                    receiver. Coalesced -- at most one parse-drop
                                    diagnostic is pending per track at a time, so
                                    a peer flooding malformed objects cannot flood
                                    the app or overflow any queue. Read
                                    parse_drop_class / parse_drops_total /
                                    parse_drops_delta (below); `track` is always
                                    non-NULL (every parse drop is track-scoped)
                                    and `desc` is the handle's description. */
    MOQ_MEDIA_TRACK_LOCAL_FAILURE = 8, /* initial ACK metadata could not be
                                    retained locally. Exactly once per failed
                                    track generation, even without media objects.
                                    Read local_result only when wholly covered by
                                    struct_size. This is not a peer rejection or
                                    END_OF_TRACK; unrelated tracks continue. */
    MOQ_MEDIA_TRACK_FILL_TERMINAL = 9, /* one child ended; parent track state
                                    is unchanged. Read the stamped plain fill
                                    tail below. Catalog children are private.
                                    For a fill of an MPEG2TS media track it
                                    follows every Object of that fill: it is
                                    queued once each of them has been polled
                                    or dropped, and carries the fill's
                                    unreported-loss summary (below). Other
                                    tracks get it when the fill ends. */
    MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE = 10, /* local delivery capacity
                                    refusal, distinct from initial ACK loss.
                                    LIVE permanently fails this generation;
                                    FILL fails only the identified child.
                                    Accepted objects retain ownership. */
    MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE = 11 /* a local UPDATE admission
                                    failed permanently. Exactly once per failed
                                    track generation; read the sized local_result.
                                    Not a peer UPDATE error or END_OF_TRACK.
                                    Unrelated tracks and the endpoint continue. */
} moq_media_track_event_kind_t;

/* Which receive path a coalesced MOQ_MEDIA_TRACK_PARSE_DROP came from. */
typedef enum moq_media_parse_drop_class {
    MOQ_MEDIA_PARSE_DROP_MEDIA = 1,       /* a normal media object (poll_object) */
    MOQ_MEDIA_PARSE_DROP_SAP,             /* a CMSF SAP timeline object (poll_sap) */
    MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE,  /* an MSF media-timeline object
                                             (poll_media_timeline) */
} moq_media_parse_drop_class_t;

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
                                            (handles are never recycled). An
                                            ENDED track stays valid: subscribe_track()
                                            refuses it with MOQ_ERR_WRONG_STATE,
                                            unsubscribe_track() still succeeds
                                            (disable), track_state() reports
                                            ENDED. A locally FAILED track has
                                            the same handle lifetime and control
                                            behavior, but reports FAILED and
                                            cannot rearm through live-to-VOD.
                                            A track REMOVED from the
                                            catalog stays valid but inert: all
                                            three refuse it with
                                            MOQ_ERR_WRONG_STATE. */
    const moq_media_track_desc_t *desc;  /* handle-owned (see lifetime note
                                            above); NULL for CATALOG_READY */
    uint32_t config_generation;          /* reserved for dynamic config;
                                            always 0 today */
    /* Permanently reserved, never read: the v0 struct ended in four
     * bytes of trailing padding after config_generation; the appended
     * fields below start at the old sizeof so an old caller's storage is
     * never written past its struct_size (poll_track stamps
     * min(your sizeof, the library's) and copies only that much). */
    uint8_t  _reserved_track_ev[4];
    /* Appended , valid for MOQ_MEDIA_TRACK_UPDATE_OK only; zero for
     * every other kind. Copied per event at queue time (never derived from
     * later track state). Presence gate: an OLDER library stamps its own
     * (v0) sizeof, so read this block only when the stamped
     * struct_size >= offsetof(has_largest) + sizeof(has_largest) -- the
     * reserved spacer sits INSIDE the old sizeof, so comparing against its
     * offset would misread uninitialized memory as present. */
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;                /* draft-18 peers only */
    uint64_t expires_ms;
    /* Appended (second generation), valid for MOQ_MEDIA_TRACK_PARSE_DROP only;
     * zero for every other kind. Snapshotted at poll time from the track's
     * coalesced per-track counters. Presence gate: an OLDER library stamps its
     * own sizeof, so read this block only when the stamped struct_size >=
     * offsetof(parse_drop_class) + sizeof(parse_drop_class). */
    moq_media_parse_drop_class_t parse_drop_class; /* path/class of the last drop
                                                      in this coalesced run */
    uint64_t parse_drops_total;          /* cumulative parse drops for this track */
    uint64_t parse_drops_delta;          /* drops since the previous drained
                                            parse-drop event for this track */
    /* Appended after the complete preceding allocation, never in its padding.
     * LOCAL_FAILURE carries the exact signed MOQ_ERR_NOMEM or MOQ_ERR_BUFFER
     * from initial ACK retention. DELIVERY_LOCAL_FAILURE carries MOQ_ERR_BUFFER
     * from facade delivery admission. UPDATE_LOCAL_FAILURE carries the exact
     * local UPDATE admission error, not a peer response. None is a wire error
     * code. Read
     * only when struct_size >= offsetof(local_result) + sizeof(local_result).
     * Older/short callers still receive the event's supported prefix but must
     * not infer an error value from an absent or partial field. Zero for every
     * other event kind. */
    int32_t  local_result;
    uint32_t _reserved_local_failure;   /* zero; never read */
    /* Appended after the complete LOCAL_FAILURE allocation. Valid only for
     * FILL_TERMINAL, with the exact initiating SUBSCRIBE/UPDATE ID (zero is
     * valid), MOQ_FILL_* reason and MOQ_FILL_ERROR_* domain from session.h.
     * A STREAM code of zero is still a failure, not COMPLETE. Each field is
     * observable only when wholly covered by struct_size. Short/old outputs
     * consume the event's supported prefix; an absent tail is unobserved.
     * These are plain values, with no cleanup and no borrowed child handle. */
    uint64_t initiating_request_id;
    uint32_t fill_reason;
    uint32_t fill_error_domain;
    uint64_t fill_error_code;
    /* Appended after the complete FILL_TERMINAL allocation. Valid only for
     * DELIVERY_LOCAL_FAILURE: LIVE includes whole objects and chunks; FILL
     * identifies a child without ending its parent. The initiating ID is valid
     * only for FILL (zero is valid); LIVE leaves it zero. This does not change
     * the earlier FILL_TERMINAL initiating_request_id field. Read each field
     * only when its complete extent is covered by struct_size. No owned refs. */
    uint32_t delivery_origin;           /* MOQ_MEDIA_DELIVERY_* */
    uint32_t _reserved_delivery;        /* zero; never read */
    uint64_t delivery_initiating_request_id;
    /* Appended after the complete DELIVERY_LOCAL_FAILURE allocation. Valid
     * only for FILL_TERMINAL of a fill of an MPEG2TS media track; zero for
     * every other event and track. Written and readable only when
     * struct_size covers the whole block, through fill_unreported_loss_object;
     * a shorter stamp leaves it unobserved.
     *
     * A coalesced summary of the fill's loss that no delivered Object of the
     * fill reported: Objects of the fill dropped locally after the last one
     * the application received (including drops after the fill's own end),
     * and an Unknown or Timed-Out range the fill ended with. It is not a list
     * of gaps, and carries no range or duration. Non-Existent ranges, and
     * Location jumps a fill without Range Filters leaves unmarked, are not
     * loss and never count.
     *
     *   fill_unreported_loss_causes  0 when there is none; else the OR of
     *                        MOQ_MEDIA_FILL_LOSS_* for every contribution,
     *                        mixed causes reported together;
     *   fill_unreported_loss_group / fill_unreported_loss_object
     *                        where the contribution earliest in the fill's
     *                        delivery order starts: a dropped Object's own
     *                        Location, or the start of the loss it reported;
     *                        for a range, the Location after the fill's
     *                        previous Object or range end, or the range's own
     *                        end Location when nothing preceded it. A loss
     *                        found later at an earlier delivery position
     *                        replaces it.
     *
     * Fill Objects keep the fill stream's transport order. A descending fill
     * is not a reconstruction of the packet stream: the position is by
     * delivery order, never re-sorted by Location, and a Group reversal is
     * not loss. Sort such Objects by Location (or request an ascending fill)
     * before decoding. The unknown cause is distinct from an Object's
     * gap_tail_unknown. */
    uint32_t fill_unreported_loss_causes; /* MOQ_MEDIA_FILL_LOSS_* bits */
    uint32_t _reserved_fill_loss;         /* zero; never read */
    uint64_t fill_unreported_loss_group;
    uint64_t fill_unreported_loss_object;
} moq_media_track_event_t;

/* Causes in fill_unreported_loss_causes. */
#define MOQ_MEDIA_FILL_LOSS_LOCAL_DROP 0x1u /* this receiver dropped Objects */
#define MOQ_MEDIA_FILL_LOSS_UNKNOWN    0x2u /* an End of Unknown Range */
#define MOQ_MEDIA_FILL_LOSS_TIMED_OUT  0x4u /* an End of Timed-Out Range */

/* The description for a track handle (handle-owned; same lifetime as the
 * TRACK_ADDED event's desc). NULL for a NULL track.
 *
 * Borrowed access is lock-free. Immutable fields (spans, arrays, structured
 * info, catalog passthroughs) may be read through this pointer for the
 * handle's lifetime. The mutable fields -- is_live, has_track_duration,
 * track_duration_ms -- are rewritten in place by the service thread on a
 * live-to-VOD conversion (MSF §11.3) and MUST NOT be read through this
 * pointer concurrently with service: that is a data race. Read them with
 * moq_media_receiver_track_desc_copy() instead. */
MOQ_API const moq_media_track_desc_t *moq_media_track_desc_get(
    const moq_media_track_t *track);

/* Frozen v0 prefix of moq_media_receiver_track_desc_copy()'s output: the
 * description through is_live, the last field the copy operation guarantees.
 * Callers pass sizeof(moq_media_track_desc_t) or any size >= this value. */
#define MOQ_MEDIA_TRACK_DESC_V0_SIZE \
    (offsetof(moq_media_track_desc_t, is_live) + sizeof(bool))

/* Copy a track's CURRENT description into `out` under the receiver mutex --
 * the same mutex the live-to-VOD writer holds -- so the mutable triple is
 * read coherently. This is a snapshot of the present state, not the image
 * the TRACK_ADDED/UPDATED event carried.
 *
 * `track` must be a handle of `r`: membership is checked against the
 * receiver's own handle table without dereferencing an unowned pointer.
 * Removed, ended and locally failed tracks, and a terminal (fatal/closed)
 * receiver, remain readable until moq_media_receiver_destroy(). Spans and arrays
 * in the copy stay BORROWED from the handle (handle lifetime), exactly as through
 * moq_media_track_desc_get(); nested struct_size stamps (info, init) are
 * carried verbatim and are meaningful only inside the outer prefix.
 *
 * Writes min(out_size, sizeof(moq_media_track_desc_t)) bytes and stamps
 * out->struct_size with that size; bytes beyond it are untouched. Refusals
 * write nothing: MOQ_ERR_INVAL for a NULL receiver/track/out, an out_size
 * below MOQ_MEDIA_TRACK_DESC_V0_SIZE, or a handle `r` does not own. Never
 * allocates. Callable from any thread. */
MOQ_API moq_result_t moq_media_receiver_track_desc_copy(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_desc_t *out, size_t out_size);

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
    /* Permanently reserved, never read: the two bytes of tail padding the
     * struct had after priority, so no appended field can be read from an
     * older caller's uninitialized padding. */
    uint8_t                 _reserved_sub[2];
    /* Appended after priority. MPEG2TS only: the program whose PMT gives the
     * random access and PCR PIDs the track's reorder window measures with
     * (it matters on an unmodified-multiplex track). 0 = the catalog's
     * mpeg2tsProgramNumber, else the first program of the PAT. Auto-subscribe
     * uses 0. */
    uint32_t                mpeg2ts_program_number;
    /* Appended beyond the complete old allocation. NULL leaves the existing
     * start/join policy unchanged. A selected
     * location supersedes start (which is then unread). Any explicit option
     * suppresses the implicit current-group fill; only non-NULL fill requests
     * a child. NONE is an explicit unconstrained location, not omission.
     * Both records, including fill->location, borrow only for this call.
     * Selected values are copied before returning; no caller storage is
     * retained. D16/18 accept NONE, NEXT_OBJECT, RELATIVE_GROUP with offset 0,
     * ABSOLUTE_START and GROUP_END. D16 locations must fit 62 bits; D18 accepts
     * full uint64 values. Fill and the other locations require the D22
     * companion (explicit, or AUTO resolved) with negotiated draft22. */
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t            *fill;
} moq_media_receiver_track_subscribe_cfg_t;

/* Frozen v0 sizeof: through priority, rounded to the original uint32_t
 * alignment, including tail padding. subscribe_track accepts any
 * struct_size at or above it and reads an appended field only when
 * struct_size covers the whole field. */
#ifdef __cplusplus
#define MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_ALIGNMENT alignof(uint32_t)
#else
#define MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_ALIGNMENT _Alignof(uint32_t)
#endif
#define MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE \
    ((offsetof(moq_media_receiver_track_subscribe_cfg_t, priority) + \
      sizeof(uint8_t) + MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_ALIGNMENT - 1) & \
     ~(size_t)(MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_ALIGNMENT - 1))

/* Pointer-only initializer: clears and stamps ONLY the frozen v0 prefix (the
 * complete original allocation), so the appended fields keep their defaults.
 * To set any of them, use
 * moq_media_receiver_track_subscribe_cfg_init_sized(cfg, sizeof *cfg)
 * before selecting appended options. */
MOQ_API void moq_media_receiver_track_subscribe_cfg_init(
    moq_media_receiver_track_subscribe_cfg_t *cfg);
/* Sized initializer: clears and stamps min(cfg_size, this library's struct
 * size). No-op if cfg is NULL or cfg_size cannot hold struct_size. Partial
 * option pointers are absent, not inspected. */
MOQ_API void moq_media_receiver_track_subscribe_cfg_init_sized(
    moq_media_receiver_track_subscribe_cfg_t *cfg, size_t cfg_size);

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
 *                     (struct_size below
 *                     MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE, an
 *                     unknown `start` or other selected value, or a selected
 *                     location beyond D16's 62-bit range)
 *   MOQ_ERR_ABI_MISMATCH  a selected nested option is below its frozen floor
 *   MOQ_ERR_WRONG_STATE  the track has ended or failed locally: a
 *                     rejected/finished/failed track cannot be (re)subscribed
 *   MOQ_ERR_UNSUPPORTED  the receiver has no delivery path for this track
 *                     (info not derived, packaging not parseable in this
 *                     build, a timeline of a kind it does not decode, or a
 *                     namespace override), or a location/fill is not
 *                     representable by the negotiated draft and selected
 *                     companion; nothing is recorded
 *   MOQ_ERR_CLOSED    the receiver is terminal
 *
 * Objects for the track surface via poll_object after the peer accepts; a peer
 * rejection surfaces MOQ_MEDIA_TRACK_ENDED for that track and does not fail the
 * receiver. Failure to retain initial ACK metadata instead surfaces
 * MOQ_MEDIA_TRACK_LOCAL_FAILURE and permanently fails only that track generation.
 * A permanent local UPDATE admission failure likewise surfaces
 * MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE with its exact local_result; capacity
 * pressure and an outstanding response remain retryable. Neither local
 * failure reports a peer rejection or closes the shared endpoint.
 *
 * cfg lifetime: the start mode, priority, mpeg2ts_program_number and options apply ONLY when the underlying
 * subscription is first issued for this track. Any later subscribe_track call --
 * while the first subscription is still pending, already active, or paused by a
 * prior unsubscribe_track -- is an idempotent enable/resume command: it (re)sets
 * the track to "wanted" but does NOT change the start mode, priority or options of the
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
 * (and MOQ_OK) for an already-unsubscribed or locally failed track. */
MOQ_API moq_result_t moq_media_receiver_unsubscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track);

/* Cancel one owned D22 fill, selected by its initiating SUBSCRIBE/UPDATE ID.
 * Zero is a valid ID. Records bounded intent and wakes the endpoint executor;
 * only that executor calls the facade and supplies time. No application session
 * access, task allocation or receiver pointer retained by a posted callback.
 * MOQ_OK means scheduled (repeated pending cancellation is idempotent), not
 * that LOCAL_CANCELLED has won a race with a peer terminal. The original
 * winning terminal is observed exactly once through poll_track. Cancellation
 * leaves the parent and other children active; already-owned objects stay valid.
 *
 * Returns INVAL for NULL/foreign track, UNSUPPORTED outside the D22 companion,
 * STALE_HANDLE for unknown/completed keys, CLOSED for a terminal receiver or
 * endpoint. Like subscribe_track, the nonblocking command is accepted while
 * wait's interrupt latch is set; interruption does not cancel recorded intent.
 * Receiver destruction follows the existing executor detach/teardown contract. */
MOQ_API moq_result_t moq_media_receiver_cancel_fill(
    moq_media_receiver_t *r, moq_media_track_t *track,
    uint64_t initiating_request_id);

/* A track's current delivery state, for UI/status without inferring it from the
 * event stream. The two pause states are distinct ON PURPOSE: PAUSED_APP is the
 * application's own unsubscribe_track (it stays paused until the app re-enables);
 * PAUSED_FLOW is automatic FLOW_CONTROL backpressure. D22 resumes when object
 * depth and combined charged bytes (including an owned conversion) are both at
 * or below floor-half of their effective max_objects/max_bytes thresholds.
 * D16/18 retains object-depth-only resume, with no byte low-water condition.
 * Collapsing them would hide that difference from a consumer
 * and from resume logic. */
typedef enum moq_media_track_state {
    MOQ_MEDIA_TRACK_STATE_DISCOVERED = 0, /* in the catalog, never subscribed */
    MOQ_MEDIA_TRACK_STATE_PENDING,        /* subscribe issued, not yet active */
    MOQ_MEDIA_TRACK_STATE_ACTIVE,         /* subscribed and delivering */
    MOQ_MEDIA_TRACK_STATE_PAUSED_APP,     /* disabled by unsubscribe_track */
    MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW,    /* paused by FLOW_CONTROL backpressure */
    MOQ_MEDIA_TRACK_STATE_ENDED,          /* ended (peer reject / end-of-track) */
    MOQ_MEDIA_TRACK_STATE_FAILED = 6      /* local ACK retention, LIVE delivery
                                            or UPDATE admission failure;
                                            remains failed, never rearmed */
} moq_media_track_state_t;

/* Write `track`'s current state to *out (a known handle from TRACK_ADDED).
 * Returns MOQ_ERR_INVAL for a NULL r/track/out or a foreign track (these are
 * programmer errors, surfaced rather than hidden); MOQ_OK otherwise, with *out
 * set (a locally failed track stays FAILED; otherwise a terminal receiver or an
 * ended track yields MOQ_MEDIA_TRACK_STATE_ENDED).
 * The value is a point-in-time snapshot: reconciliation runs on the network
 * thread, so PENDING may advance to ACTIVE (etc.) by the time the caller acts. */
MOQ_API moq_result_t moq_media_receiver_track_state(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_state_t *out);

/* -- Objects (§6.3) ---------------------------------------------------- *
 * The validated parsed shape (moq_media_parsed_object_t fields) plus the
 * track and wire metadata: the TRACK_ADDED handle, the config generation,
 * and the object's wire identity. Objects surface in arrival order, except
 * that an MPEG2TS track's Objects surface in Location order: they pass
 * through that track's reorder window (mpeg2ts_reorder_slots /
 * mpeg2ts_reorder_window_us), which also fills the loss report below. The
 * track's END_OF_TRACK takes its Location in that window too, so
 * TRACK_ENDED follows every earlier Object that arrives in time, even one
 * whose stream the END_OF_TRACK overtook.
 * poll_object returning MOQ_OK TRANSFERS EXCLUSIVE OWNERSHIP
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
 * moq_rcbuf_t (a thread-safe zero-copy receive export is not offered). */

/* Fixed-width delivery_origin values, available only in a complete stamped
 * origin observation. FILL's initiating request ID may be zero. */
#define MOQ_MEDIA_DELIVERY_LIVE 1u
#define MOQ_MEDIA_DELIVERY_FILL 2u

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

    /* Appended after the v0 prefix: the object's wire identity, as the session
     * delivered it. All three ids are full width and zero is a valid value.
     * The identity is available only when the returned prefix covers the
     * complete record, that is when the stamped struct_size reaches the end of
     * has_subgroup. A shorter stamp, whether the caller's buffer was smaller or
     * the library older, leaves the earlier fields as they are and says nothing
     * about any id: it never means an id is absent. Nothing here is owned.
     *
     *   group_id / object_id   the object's location in its track;
     *   has_subgroup           the wire carried a subgroup: true for an object
     *                          from a subgroup stream (subgroup_id is the
     *                          SUBGROUP_HEADER's value, resolved to the first
     *                          object's id or zero per its mode), false for a
     *                          datagram, which has no subgroup on the wire.
     *                          subgroup_id is then zero, and only this flag
     *                          says so: zero is also a real subgroup id;
     *   publisher_priority     the effective priority the session delivered:
     *                          the header or datagram byte when present, else
     *                          the session's default for an inherited priority.
     *                          It is not a claim that a byte was on the wire. */
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority;
    bool                  has_subgroup;
    /* Permanently reserved, never read: the six bytes of tail padding the
     * struct had after has_subgroup, so the stamped size of an older
     * library can never appear to cover the loss report. */
    uint8_t               _reserved_obj[6];

    /* Loss report, appended after has_subgroup; available when the stamped
     * struct_size reaches the end of discontinuous. has_loss_report is true
     * when the receiver tracks Location continuity for this track's
     * packaging (MPEG2TS today) and the fields below are meaningful; false
     * means not tracked, and says nothing about loss.
     *
     *   gap                  a run of missing, rejected or locally dropped
     *                        Objects precedes this one; gap_group /
     *                        gap_object is the Location of its first Object;
     *   gap_tail_unknown     the run is only the unconfirmed tail of the
     *                        previous Group (its final Object was never
     *                        confirmed): not a known loss;
     *   gap_duration_known / gap_duration_27mhz
     *                        the run's PCR span, when the packaging has an
     *                        in-band clock;
     *   discontinuous        informational: no random access point on the
     *                        selected stream since the gap.
     *
     * Each delivery source reports its own loss: LIVE Objects of one
     * subscription, and each fill separately. A fill Object's gap is an
     * Unknown or Timed-Out range, or a local drop, since the fill's previous
     * Object reached the application; gap_group / gap_object is where the
     * earliest of them in the fill's delivery order starts, its duration is
     * not known, and gap_tail_unknown is false. Loss with no later Object of
     * the fill is reported on the fill's FILL_TERMINAL. */
    bool                  has_loss_report;
    bool                  gap;
    uint64_t              gap_group;
    uint64_t              gap_object;
    bool                  gap_tail_unknown;
    bool                  gap_duration_known;
    uint64_t              gap_duration_27mhz;
    bool                  discontinuous;

    /* D22's effective object metadata; complete old object layout preserved.
     * Read only when struct_size covers this entire field. Object config
     * spans borrow properties_ref and remain valid until object cleanup,
     * including after receiver destruction (track itself does not survive).
     * Inherited config spans borrow this object's private track_properties_ref;
     * present object fields (including empty config) override the track block.
     * Raw Track and Object Properties remain separate and unchanged.
     * Legacy objects leave this metadata zero. No clock domain is inferred. */
    moq_media_metadata_t   metadata;
    /* Exact independent copy of this subscription generation's initial ACK
     * block, NULL for an empty block or D16/18. Poll transfers its sole ref:
     * neither the facade, receiver nor another object shares ownership. Valid
     * through cleanup even after receiver/session destruction. Allocation or
     * conversion failure drops/counts this object as PARSE_DROP, not a permanent
     * track LOCAL_FAILURE. Cleanup reads/releases only a fully stamped field. */
    moq_rcbuf_t          *track_properties_ref;

    /* Appended after the complete metadata/ref allocation, including padding.
     * Available only when struct_size covers initiating_request_id completely;
     * a shorter stamp means unobserved, not LIVE or a missing fill. This is the
     * source of the whole object (and every CMAF sample it contains), separate
     * from wire location. LIVE has no valid initiator and stores zero. FILL
     * carries the actual initiating SUBSCRIBE/UPDATE request ID; zero is valid.
     * Plain values survive receiver destruction along with the owned object;
     * the borrowed track handle does not. No ownership or thread rule changes. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t              delivery_origin; /* MOQ_MEDIA_DELIVERY_* */
    uint64_t              initiating_request_id;
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
 * MOQ_ERR_INVAL for NULL args or a legacy obj_size below the v0 base size. An object for a
 * track is never pollable before that track's TRACK_ADDED has been queued --
 * drain track events before objects on each wakeup and every handle is known
 * before its first object. For the D22 companion (explicit, or AUTO resolved on
 * draft 22), obj_size must cover
 * the whole metadata/ref and delivery-origin observation through the end of
 * initiating_request_id; otherwise
 * MOQ_ERR_UNSUPPORTED leaves output and queue untouched, with no transfer.
 * Legacy D16/18 retains the v0 minimum. */
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
                                     per-object, never terminal). Also surfaced
                                     per track as a coalesced
                                     MOQ_MEDIA_TRACK_PARSE_DROP event via
                                     poll_track -- no longer counter-only. */
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
    /* Appended after catalog_complete: the loss reports poll_object handed
     * out with gap set, the subset with gap_tail_unknown, and the Objects a
     * reorder window refused as late or duplicated. */
    uint64_t gaps_reported;
    uint64_t gap_tails_unknown;
    uint64_t reorder_refused;
    /* Appended: media tracks whose subscription ended because the Track was
     * malformed (draft-22 §12.1); each also ends (TRACK_ENDED). */
    uint64_t tracks_malformed;
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
#define MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE 0x1u /* parse/derive, ACK retention
                                                          or catalog delivery failed;
                                                          receiver-local only */
#define MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW   0x2u /* track-event queue
                                                          overflow (§6.3) */
#define MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED     0x3u /* catalog subscription
                                                          could not be issued,
                                                          or companion/version mismatch */
#define MOQ_MEDIA_RECEIVER_FATAL_CATALOG_REJECTED 0x4u /* the publisher refused
                                                          the catalog
                                                          subscription */

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_RECEIVER_H */
