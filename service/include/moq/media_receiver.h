#ifndef MOQ_MEDIA_RECEIVER_H
#define MOQ_MEDIA_RECEIVER_H

/*
 * moq_media_receiver_t — the receive-side media facade over an endpoint.
 * Owns the catalog track subscription on the endpoint's network thread,
 * parses the MSF catalog, and surfaces track discovery as a bounded event
 * queue the application drains from its own thread.
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
 * Catalog revisions. Both MSF-01 and MSF-02 (draft-ietf-moq-msf-02)
 * catalogs are read; the revision is the independent catalog's "version",
 * and a delta follows the catalog it applies to (stats.catalog_revision).
 * Under MSF-01 a declared track's attributes are immutable except the
 * live-to-VOD conversion. Under MSF-02 (5.1.6, 5.3) a catalog may change a
 * declared track in place: an advisory change (bitrate, framerate,
 * dimensions, label, lang, targetLatency, renderGroup, altGroup, depends,
 * the SAP maxima, keyframeSpacing, accessibleWindowTime, mimeType,
 * cipherSuite, keyId, trackBaseKey, packagingMetadata keys other than
 * streamMapping, a template added) keeps the handle and publishes a new
 * description generation (MOQ_MEDIA_TRACK_UPDATED); a change that decides
 * how Objects are parsed or delivered (packaging, role, codec, timescale,
 * samplerate, channelConfig, eventType, the resolved init, the content
 * protection, the MPEG-2 TS record, the stream mapping, encryptionScheme,
 * or anything that changes the receiver's delivery decision) REPLACES the
 * handle: TRACK_REMOVED names the successor (replaced_by), and the
 * successor's TRACK_ADDED, immediately after it, names its predecessor
 * (replaces). The predecessor's selection moves to the successor whole, and
 * a command later issued on the predecessor acts on the successor. A change
 * MSF-02 forbids (isLive false to true, a present template changing) drops
 * the catalog (stats.catalog_drops). Every catalog is adopted whole or not
 * at all: one that cannot be (an allocation failure, a retained-state cap,
 * too little room in the track-event queue for its events once a catalog
 * is ready) changes nothing and is dropped.
 *
 * What a replacement guarantees: a new handle and decoder lifecycle, and
 * that no Object the predecessor's subscription received is delivered on
 * the successor (its queue is purged, its late Objects are dropped, and the
 * successor subscribes only once the predecessor's subscription is
 * released). It does not guarantee that the successor's Objects use the new
 * configuration: MSF gives a catalog change no synchronization point with
 * the media, and a relay may serve the new subscription a Group produced
 * before the change. Where the packaging carries the configuration it is
 * checked: a LOC-05 Codec String or in-band VIDEO/AUDIO_CONFIG on an Object
 * governs that Object, and an MSF-02 CMAF fragment whose tfhd track_ID
 * differs from the init's is dropped (MOQ_MEDIA_PARSE_DROP_MEDIA). A LOC
 * Object without such properties, or a CMAF fragment with the init's
 * track_ID, reaches the new decoder unflagged.
 *
 * Subscription modes. One rule decides what can be subscribed: the receiver
 * has a DELIVERY PATH for a track when it is a media track whose structured
 * info derived from the catalog and whose packaging this build parses
 * (poll_object), or a CMSF SAP timeline (poll_sap) or MSF §7 media timeline
 * (poll_media_timeline). subscribe_track refuses any other track with
 * MOQ_ERR_UNSUPPORTED: a track whose info did not derive (e.g. an unknown
 * packaging), an MPEG2TS track in a build without moq::mpeg2ts, an
 * eventtimeline of another kind, an MSF-01 namespace-override track (the
 * MSF-01 namespace has no defined MoQT mapping; an MSF-02 track's
 * "namespace" names its Track Namespace fields, MSF-02 5.2.2, and is
 * subscribed under them), and these MSF policies:
 *  - an encrypted track (encryptionScheme, MSF 4.4.1) of any packaging but
 *    "loc": the receiver interprets CMAF, MPEG-2 TS and timeline payloads,
 *    which it cannot decrypt. An encrypted "loc" track IS delivered, with
 *    its payload as received (ciphertext) and its LOC properties, which stay
 *    in plaintext (MSF-02 4.4.4); the key fields are on the description's
 *    supplement record (moq_msf_ext_supplement(desc->extensions, ...)), and
 *    the application decrypts;
 *  - an MSF-02 track whose decoder configuration the catalog places in band
 *    (an initDataList "track-property" or "object-property" entry) unless it
 *    is a "loc" track with VIDEO_CONFIG (0x0D) on role "video" or
 *    AUDIO_CONFIG (0x0F) on role "audio" on a draft-22 companion: the legacy
 *    companion carries no per-Object configuration, so a decoder would get
 *    media without it (desc.init_source says where it comes from);
 *  - an MSF-02 "loc" audio or video track without a codec;
 *  - a LOCMAF track (packaging "locmaf", draft-einarsson-moq-locmaf-01)
 *    whose locmafVersion this build does not support, or is missing or
 *    malformed; whose CMAF Header is not inline, does not parse, does not
 *    have exactly one trak or has no trex; or in a build without
 *    moq::locmaf.
 *
 * LOCMAF. A LOCMAF track's description has info.packaging
 * MOQ_MEDIA_PACKAGING_LOCMAF and its CMAF Header record in desc.cmaf. Each
 * of its Objects is decoded against the earlier Objects of its Group and
 * reaches poll_object as the canonical CMAF chunk it encodes (section 15):
 * packaging MOQ_MEDIA_PACKAGING_CMAF, the chunk as `fragment`, `payload`
 * empty, with a loss report. Within a Group Objects surface in Object
 * order; across Groups, in arrival order. Groups decode independently, up
 * to two at once per track, so the tail of a Group that arrives after the
 * next Group began is still delivered; a third concurrent Group closes the
 * lowest open one (or is itself refused when it is the lowest), and the
 * Objects of a closed Group that arrive later are dropped. An Object that
 * cannot be decoded is dropped, reported as a gap on the next delivered
 * Object of its delivery source: after any loss in a Group -- an Object ID
 * gap, its stream reset, a datagram or a second subgroup, which the
 * publisher must not use (section 3), or a refused Object -- deltas are
 * dropped until a full header resumes the Group, or the next Group starts;
 * a rawBoxes Object (section 9) is delivered as its boxes but leaves no
 * reference, so a delta after it is refused. Joining mid-Group (drafts 16
 * and 18 always, START_CURRENT) therefore drops the deltas until the next
 * Group; START_NEXT_GROUP avoids it. On draft 22 the live join at the
 * current Group comes with a fill: the live deltas of that first Group are
 * held (at most 64, each for at most 300 ms) until the fill has decoded the
 * Group's Objects before them, from a full header, and then decode in
 * Location order after the fill's. A fill's Objects are decoded by a
 * decoder of the fill's own and delivered as FILL, and a standalone
 * fetch's by a decoder of the fetch's own, delivered as FETCH; nothing is
 * shared across origins otherwise, and a Location both deliver is
 * delivered by both. The track's END_OF_TRACK, and an END_OF_GROUP status
 * Object, follow the Objects held before them. The counters are in the
 * stats.
 *
 * A LOCMAF Object carries no configuration identity: after a replacement a
 * Group encoded under the previous CMAF Header is rebuilt with the new
 * Header's facts, undetected (the limitation kept for LOC-01 above). A
 * LOCMAF track with encryptionScheme, or whose initialization data is a
 * track or object property, has no delivery path (see above).
 *
 * Compression (MSF-02 12.1). A catalog or timeline Object whose origin's
 * Track Properties or own Object Properties carry MSF_COMPRESSION (0x78)
 * with a value other than 0 (uncompressed) is never processed as if it were
 * uncompressed. This build does not decompress yet, GZIP (1) included, so
 * such an Object is refused as an algorithm it does not support: a catalog
 * Object fails as a malformed one does (the first catalog's failure is
 * CATALOG_UNUSABLE), a timeline whose Track Properties say so fails locally
 * (MOQ_MEDIA_TRACK_LOCAL_FAILURE, MOQ_ERR_UNSUPPORTED) and one Object that
 * says so is a parse drop. An Object carrying the Object property where the
 * Track property is set is dropped (a publisher may not use both). Under an
 * MSF-01 catalog the property is not interpreted on timelines. See the
 * stats for the counters.
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
#include <moq/media_auth.h>
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
     * D22_MSF01_LOC05_PUBLIC on D22) and then behaves exactly as it does,
     * except that it keeps the draft-22 poll_object size floor through
     * initiating_request_id (an older caller's object then simply does not
     * cover the codec_string block). AUTO on D22 therefore reports companion
     * D22_MSF01_LOC05_PUBLIC, and a malformed or repeated LOC-05 Codec String
     * is a parse drop where LOC-04 skipped it as unknown. With SHARED_EPOCH,
     * AUTO has no draft-22 match. Explicit LEGACY_MSF01_LOC01 requires
     * negotiated D16/18; explicit D22_MSF01_LOC04_PUBLIC or
     * D22_MSF01_LOC05_PUBLIC requires negotiated D22 and RAW
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
    /* Permanently reserved, never read: covers the tail padding after
     * companion_profile, so the block below starts at the complete sizeof
     * of the layout that ends with it on every target. */
    uint8_t  _reserved_companion[4];
    /* Appended: the stream-terminal lane (moq_media_receiver_poll_stream_
     * terminal), max_stream_terminals records deep (0 = 64). false (the
     * default) keeps the lane, its intake rule and its records off; nothing
     * else changes. Read as one block, only when struct_size covers
     * surface_stream_terminals. */
    uint32_t max_stream_terminals;
    bool     surface_stream_terminals;
    /* Preserve the complete preceding allocation; never read. */
    uint8_t  _reserved_stream_terminals[3];
    /* Optional resource credentials, independent of endpoint SETUP tokens.
     * Construction copies the source and static bytes. Selection/outcomes
     * follow media_auth.h; absent/partial/NULL means anonymous. A configured
     * source never falls back to anonymous access. A D22 SUBSCRIBE plus fill
     * selects once for SUBSCRIBE, not a separate FETCH. Use a sized init. */
    const moq_media_auth_cfg_t *resource_auth;
    /* Permanently reserved, never read: covers the tail padding after
     * resource_auth on targets where it has any (32-bit targets that align
     * uint64_t to 8, where an older caller's sizeof includes four bytes of
     * padding after the pointer), so the field below starts at or past the
     * complete sizeof of the layout that ends with resource_auth. */
    uint8_t  _reserved_auth[4];
    /* Appended: how many standalone fetches of one track
     * (moq_media_receiver_fetch_track) may be open at once, counted from
     * MOQ_MEDIA_TRACK_FETCH_CREATED until the fetch's
     * MOQ_MEDIA_TRACK_FETCH_TERMINAL is polled; past it fetch_track returns
     * MOQ_ERR_CAPACITY. 0 = 4. Read only when struct_size covers the whole
     * field. */
    uint32_t max_fetches_per_track;
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
 * A published description is immutable with ONE exception: a live->VOD
 * conversion (MSF-01 11.3, MSF-02 11.4) updates the current description's
 * is_live/track_duration in place and fires MOQ_MEDIA_TRACK_UPDATED (per
 * MSF-01 §5.3 every OTHER attribute of a declared Namespace|Name tuple stays
 * fixed). An MSF-02 advisory change publishes a new description generation
 * for the same handle instead (see the top of this header); the earlier
 * ones stay valid and unchanged. A handle removed from the catalog stays
 * allocated (and its desc readable) until the receiver is destroyed; it just
 * transitions to a terminal state. Generations count against the caps below
 * like handles do.
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
     * was not possible for this track (unknown packaging, malformed init
     * data, an mpeg2ts track that fails moq_msf_track_mpeg2ts_check), `info`
     * holds zeroed/unknown values and codec_config, init_data and cmaf below
     * are empty -- the track is still announced with its catalog strings,
     * and has no delivery path. An mpeg2ts track derives packaging MPEG2TS;
     * its packet size is in the typed extensions below. An es-packets track
     * carries the packets of one PID. */
    moq_media_track_info_t info;  /* media type, packaging, timescale */
    /* The DECODER EXTRADATA (H.264/HEVC SPS/PPS/VPS, AAC AudioSpecificConfig,
     * ...), whatever the packaging: for CMAF the sample entry's configuration
     * box (the same span as cmaf->codec_config), for RAW/LOC the catalog's
     * initData verbatim, for MPEG-2 TS empty. This is what a decoder usually
     * wants to be configured with. Empty when the track carries none. */
    moq_bytes_t codec_config;
    /* The track's full init data from the catalog, resolved and verbatim: for
     * CMAF the initialization segment (ftyp + moov); for RAW/LOC whatever the
     * sender supplied as initData; for MPEG-2 TS the PSI source packets.
     * Muxers/recorders that reproduce the container want this; decoders
     * usually want codec_config above. Empty when the track carries none. */
    moq_bytes_t init_data;
    /* The record parsed from the track's CMAF header: its first trak's codec
     * kind, dimensions, sample rate, channels, sample entry and, for a CENC
     * protected track (CMSF §4.2), the tenc parameters for license
     * acquisition (libmoq does not decrypt). Set for a CMAF or LOCMAF track
     * whose CMAF header parsed; NULL for every other track: RAW/LOC, MPEG-2
     * TS, a CMAF track without init data or with a malformed one. Its fields are header
     * observations; the catalog's own declarations (width, height,
     * samplerate, ...) are separate fields below and may differ. A sized
     * record: read a field only when cmaf->struct_size covers all of it.
     *
     * codec_config, init_data, the record and every span in it are
     * handle-owned and immutable: they stay valid, unchanged, until the
     * receiver is destroyed, including after a catalog update replaces the
     * catalog the handle was built from. A copy of the description borrows
     * the same record; it never owns it. */
    const moq_cmaf_init_info_t *cmaf;

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
     * MOQ_MEDIA_TRACK_UPDATED event. Once false, it cannot become true in
     * a later catalog (MSF-01 section 5.2.7). A plain value, no backing
     * allocation. */
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
    /* Appended after alt_group, at the complete sizeof of the layout that
     * ends with it; read only when struct_size covers them. Immutable.
     * MSF renderGroup (0 / false when absent; zero is a value when present),
     * and whether the catalog carried a label at all: `label` is empty both
     * when it was absent and when it was empty. */
    bool                     has_render_group;
    int                      render_group;
    bool                     has_label;
    /* Appended after has_label, at the complete sizeof of the layout that
     * ends with it; one block, read only when struct_size covers
     * config_generation.
     *
     * init_source: where the track's decoder configuration comes from
     * (MOQ_MEDIA_INIT_SOURCE_*). INLINE: from the catalog (init_data,
     * codec_config above). TRACK_PROPERTY / OBJECT_PROPERTY: in band, as the
     * catalog's initDataList entry names it (MSF-02 5.1.7, 12.2), observable
     * on each Object's metadata (video_config / audio_config) from the first
     * Object on, not at TRACK_ADDED; only the draft-22 companions deliver
     * such tracks (see the top of this header). NONE: the catalog names none.
     * init_property_type: the property type for the two in-band kinds (0x0D
     * VIDEO_CONFIG, 0x0F AUDIO_CONFIG), zero otherwise.
     *
     * config_generation: this description's generation. 0 for the
     * description the track was discovered with; n for the one the n-th
     * MSF-02 advisory change of the track published (MOQ_MEDIA_TRACK_UPDATED
     * carries the same number). A description never changes once published,
     * apart from the live-to-VOD triple; a later generation is a new
     * description. */
    uint64_t                 init_property_type;
    uint32_t                 init_source;
    uint32_t                 config_generation;
} moq_media_track_desc_t;

/* moq_media_track_desc_t.init_source. Fixed-width for ABI stability; values
 * are never renumbered. Unknown values must be ignored. */
#define MOQ_MEDIA_INIT_SOURCE_NONE            UINT32_C(0)
#define MOQ_MEDIA_INIT_SOURCE_INLINE          UINT32_C(1)
#define MOQ_MEDIA_INIT_SOURCE_TRACK_PROPERTY  UINT32_C(2)
#define MOQ_MEDIA_INIT_SOURCE_OBJECT_PROPERTY UINT32_C(3)

/* -- Track discovery events (§6.2) ------------------------------------ */

typedef enum moq_media_track_event_kind {
    MOQ_MEDIA_TRACK_ADDED = 1,   /* new tuple discovered (any catalog generation) */
    MOQ_MEDIA_TRACK_UPDATED,     /* the description of the SAME handle
                                    changed. Either a live->VOD conversion
                                    (MSF-01 11.3, MSF-02 11.4): is_live
                                    flipped true->false and/or track_duration
                                    appeared, written in place on the
                                    current description before this fires
                                    (the one in-place change; read it with
                                    moq_media_receiver_track_desc_copy); or an
                                    MSF-02 advisory change (see the top of
                                    this header): a new description
                                    generation, desc, with config_generation
                                    its number; earlier generations stay
                                    valid and unchanged. Under MSF-01 every
                                    other change is an immutable-tuple
                                    violation (5.3) and is dropped. */
    MOQ_MEDIA_TRACK_REMOVED,     /* tuple dropped by a later catalog
                                    generation, or replaced by a new handle
                                    (replaced_by, an MSF-02 critical change) */
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
    MOQ_MEDIA_TRACK_LOCAL_FAILURE = 8, /* the track's subscription cannot be
                                    served locally: its initial ACK metadata
                                    could not be retained (MOQ_ERR_NOMEM or
                                    MOQ_ERR_BUFFER), or a timeline's Track
                                    Properties name a compression this build
                                    does not decompress (MOQ_ERR_UNSUPPORTED,
                                    MSF-02 12.1.1; the subscription ends).
                                    Exactly once per failed track generation,
                                    even without media objects. Read
                                    local_result only when wholly covered by
                                    struct_size. This is not a peer rejection
                                    or END_OF_TRACK; unrelated tracks
                                    continue. */
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
    MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE = 11, /* a local UPDATE admission
                                    failed permanently. Exactly once per failed
                                    track generation; read the sized local_result.
                                    Not a peer UPDATE error or END_OF_TRACK.
                                    Unrelated tracks and the endpoint continue. */
    MOQ_MEDIA_TRACK_FILL_CREATED = 12, /* the fill recorded by
                                    moq_media_receiver_fill_track() was admitted
                                    locally: its UPDATE was sent and the child
                                    exists. Read its initiating_request_id below
                                    (zero is valid) and cancel the child with
                                    moq_media_receiver_cancel_fill(). Local
                                    admission only: not the peer's acceptance,
                                    an opened stream or a live child at poll
                                    time. Queued before that child's
                                    FILL_TERMINAL. Fills requested by
                                    subscribe_track options and the catalog's
                                    fills never produce it. */
    MOQ_MEDIA_TRACK_FETCH_CREATED = 13, /* the fetch recorded by
                                    moq_media_receiver_fetch_track() was sent:
                                    its FETCH was admitted by the session.
                                    initiating_request_id is the FETCH's
                                    request ID, which its Objects carry and
                                    moq_media_receiver_cancel_fetch() takes.
                                    Local admission only, not the peer's
                                    acceptance. Queued before that fetch's
                                    FETCH_TERMINAL. Polling it (by any output
                                    size) releases the track's command slot. */
    MOQ_MEDIA_TRACK_FETCH_TERMINAL = 14 /* one standalone fetch ended, or its
                                    command was refused before it was sent;
                                    the track's state is unchanged. Read
                                    initiating_request_id, fill_reason
                                    (MOQ_MEDIA_FETCH_* reason),
                                    fill_error_domain and fill_error_code,
                                    local_result, and, for an MPEG2TS track,
                                    the fill_unreported_loss_* block (see the
                                    fields below). It follows every Object of
                                    the fetch: it is queued once each of them
                                    has been polled or dropped (and, with the
                                    stream-terminal lane, once the fetch's
                                    RECEIVER_DROP records are polled). Exactly
                                    one per FETCH_CREATED. */
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
                                            above); NULL for CATALOG_READY. The
                                            description generation the event
                                            announces: a later generation never
                                            changes what an earlier event says */
    uint32_t config_generation;          /* desc's generation: 0 for the one
                                            the track was discovered with, n for
                                            the n-th MSF-02 advisory change */
    /* Permanently reserved, never read. On LP64 the v0 struct ended in
     * four bytes of trailing padding after config_generation, which this
     * spacer occupies, so the appended fields below start at the old
     * sizeof; on 32-bit targets v0 ended at config_generation and they
     * start four bytes past it. Either way an old caller's storage is never
     * written past its struct_size (poll_track stamps min(your sizeof, the
     * library's) and copies only that much). */
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
     * local UPDATE admission error, not a peer response. FETCH_TERMINAL
     * carries it for the two local reasons (MOQ_MEDIA_FETCH_LOCAL_FAILURE:
     * MOQ_ERR_BUFFER; MOQ_MEDIA_FETCH_NOT_SENT: the admission error, or
     * MOQ_DONE when the resource credential policy denied the FETCH) and
     * zero for the others. None is a wire error code. Read
     * only when struct_size >= offsetof(local_result) + sizeof(local_result).
     * Older/short callers still receive the event's supported prefix but must
     * not infer an error value from an absent or partial field. Zero for every
     * other event kind. */
    int32_t  local_result;
    uint32_t _reserved_local_failure;   /* zero; never read */
    /* Appended after the complete LOCAL_FAILURE allocation. Valid only for
     * FILL_TERMINAL, with the exact initiating SUBSCRIBE/UPDATE ID (zero is
     * valid), MOQ_FILL_* reason and MOQ_FILL_ERROR_* domain from session.h.
     * A STREAM code of zero is still a failure, not COMPLETE. FILL_CREATED
     * uses initiating_request_id alone, for the UPDATE that requested the
     * fill (zero is valid); its reason, domain and code are zero. Each field
     * is observable only when wholly covered by struct_size. Short/old outputs
     * consume the event's supported prefix; an absent tail is unobserved, and
     * a FILL_CREATED polled that way still releases its command slot.
     * These are plain values, with no cleanup and no borrowed child handle.
     *
     * FETCH_CREATED and FETCH_TERMINAL use the same fields for a standalone
     * fetch: initiating_request_id is the FETCH's request ID (zero is valid;
     * zero with MOQ_MEDIA_FETCH_NOT_SENT, which no FETCH_CREATED precedes).
     * On FETCH_TERMINAL, fill_reason is a MOQ_MEDIA_FETCH_* reason,
     * fill_error_domain a MOQ_MEDIA_FETCH_ERROR_* domain and fill_error_code
     * the code in that domain (an error code of zero is valid when the
     * domain is not NONE). FETCH_CREATED leaves the three zero. */
    uint64_t initiating_request_id;
    uint32_t fill_reason;
    uint32_t fill_error_domain;
    uint64_t fill_error_code;
    /* Appended after the complete FILL_TERMINAL allocation. Valid only for
     * DELIVERY_LOCAL_FAILURE: LIVE includes whole objects and chunks; FILL
     * identifies a child without ending its parent; FETCH identifies one
     * standalone fetch, which then ends with FETCH_TERMINAL
     * (MOQ_MEDIA_FETCH_LOCAL_FAILURE). The initiating ID is valid only for
     * FILL and FETCH (zero is valid); LIVE leaves it zero. This does not change
     * the earlier FILL_TERMINAL initiating_request_id field. Read each field
     * only when its complete extent is covered by struct_size. No owned refs. */
    uint32_t delivery_origin;           /* MOQ_MEDIA_DELIVERY_* */
    uint32_t _reserved_delivery;        /* zero; never read */
    uint64_t delivery_initiating_request_id;
    /* Appended after the complete DELIVERY_LOCAL_FAILURE allocation. Valid
     * only for FILL_TERMINAL of a fill, and FETCH_TERMINAL of a standalone
     * fetch, of an MPEG2TS media track; zero for every other event and
     * track. A fetch's summary follows the fill rules below, with "fill"
     * read as "fetch": its End of Unknown or Timed-Out ranges and its local
     * drops count. Written and readable only when
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
    /* Appended after the complete fill-loss allocation. Valid only for
     * TRACK_ENDED; zero for every other event. Written and readable only
     * when struct_size covers the whole block, through done_status.
     * has_done_status: the track ended because its subscription completed
     * (PUBLISH_DONE, or SUBSCRIBE_DONE on drafts 16/18); done_status is
     * that message's status code. TRACK_ENDED (0x2) and, on drafts 16/18,
     * SUBSCRIPTION_ENDED (0x3) report a normal end; any other code an
     * error. Only a track declared live by its creation catalog is eligible
     * for a one-shot VOD re-arm on its first TRACK_ENDED, with no event.
     * The replacement SUBSCRIBE waits for both the original subscription's
     * drain and a validated conversion catalog (isLive:false with
     * trackDuration), in either order. A terminal empty catalog before a
     * replacement is issued instead reports the drained original done,
     * then TRACK_REMOVED; queued Objects stay pollable. A never-live track
     * ends on its first TRACK_ENDED. Ended or removed tracks cannot re-arm.
     * false: the track ended with no done status -- at its END_OF_TRACK,
     * or because the peer refused its subscription (a VOD
     * re-subscription included). Neither value says whether the
     * END_OF_TRACK arrived: that comes on the stream-terminal lane. */
    bool     has_done_status;
    uint8_t  _reserved_done[7];           /* zero; never read */
    uint64_t done_status;
    /* Appended after done_status; one block, written and readable only when
     * struct_size covers replaced_by. The replacement of a declared track by
     * an MSF-02 catalog that changed it in a decode- or delivery-critical way
     * (see MOQ_MEDIA_TRACK_UPDATED): on that TRACK_REMOVED, replaced_by is
     * the new handle and replaces is NULL; on the TRACK_ADDED that
     * immediately follows it, replaces is the old handle and replaced_by is
     * NULL. NULL on every other event. The REMOVED alone names the successor,
     * so a consumer can act on it without waiting for the ADDED. Both handles
     * stay valid until the receiver is destroyed. */
    moq_media_track_t *replaces;
    moq_media_track_t *replaced_by;
} moq_media_track_event_t;

/* Causes in fill_unreported_loss_causes. */
#define MOQ_MEDIA_FILL_LOSS_LOCAL_DROP 0x1u /* this receiver dropped Objects */
#define MOQ_MEDIA_FILL_LOSS_UNKNOWN    0x2u /* an End of Unknown Range */
#define MOQ_MEDIA_FILL_LOSS_TIMED_OUT  0x4u /* an End of Timed-Out Range */

/* Why a standalone fetch ended: MOQ_MEDIA_TRACK_FETCH_TERMINAL's fill_reason,
 * with the domain of fill_error_code in fill_error_domain. */
#define MOQ_MEDIA_FETCH_COMPLETE       1u /* the peer ended the fetch's data
                                             normally; domain NONE */
#define MOQ_MEDIA_FETCH_PEER_ERROR     2u /* the peer refused the FETCH; domain
                                             REQUEST, the request error code */
#define MOQ_MEDIA_FETCH_RESET          3u /* the peer reset a stream of the
                                             fetch; domain STREAM, its code */
#define MOQ_MEDIA_FETCH_CANCELLED      4u /* moq_media_receiver_cancel_fetch()
                                             won; domain NONE */
#define MOQ_MEDIA_FETCH_SESSION_CLOSED 5u /* the session closed with the fetch
                                             open; domain NONE */
#define MOQ_MEDIA_FETCH_LOCAL_FAILURE  6u /* the fetch's Objects exceeded the
                                             subscriber's queued-byte budget;
                                             a DELIVERY_LOCAL_FAILURE with
                                             origin FETCH precedes it; domain
                                             NONE, local_result MOQ_ERR_BUFFER */
#define MOQ_MEDIA_FETCH_NOT_SENT       7u /* the command was refused before
                                             any FETCH went out (no
                                             FETCH_CREATED precedes it);
                                             domain NONE, local_result the
                                             cause; it releases the command
                                             slot */
#define MOQ_MEDIA_FETCH_ERROR_NONE     0u
#define MOQ_MEDIA_FETCH_ERROR_STREAM   1u /* = MOQ_FILL_ERROR_STREAM */
#define MOQ_MEDIA_FETCH_ERROR_REQUEST  2u

/* The CURRENT description generation of a track handle (handle-owned; it
 * and every earlier generation stay valid until the receiver is
 * destroyed). NULL for a NULL track.
 *
 * Borrowed access is lock-free: the network thread publishes each new
 * generation complete before this can return it. A generation's immutable
 * fields (spans, arrays, structured info, catalog passthroughs) may be read
 * through the pointer for the receiver's lifetime; a later MSF-02 advisory
 * change publishes a NEW generation and never rewrites one. The mutable
 * fields -- is_live, has_track_duration, track_duration_ms -- are rewritten
 * in place on the current generation by the service thread on a live-to-VOD
 * conversion (MSF-01 11.3, MSF-02 11.4) and MUST NOT be read through this
 * pointer concurrently with service: that is a data race. Read them with
 * moq_media_receiver_track_desc_copy() instead. */
MOQ_API const moq_media_track_desc_t *moq_media_track_desc_get(
    const moq_media_track_t *track);

/* Frozen v0 prefix of moq_media_receiver_track_desc_copy()'s output: the
 * description through is_live, the last field the copy operation guarantees.
 * Callers pass sizeof(moq_media_track_desc_t) or any size >= this value. */
#define MOQ_MEDIA_TRACK_DESC_V0_SIZE \
    (offsetof(moq_media_track_desc_t, is_live) + sizeof(bool))

/* Copy a track's CURRENT description generation into `out` under the
 * receiver mutex -- the same mutex the live-to-VOD writer and the
 * generation publisher hold -- so the mutable triple is read coherently.
 * This is a snapshot of the present state, not the image the
 * TRACK_ADDED/UPDATED event carried; its config_generation says which
 * generation it is.
 *
 * `track` must be a handle of `r`: membership is checked against the
 * receiver's own handle table without dereferencing an unowned pointer.
 * Removed, ended and locally failed tracks, and a terminal (fatal/closed)
 * receiver, remain readable until moq_media_receiver_destroy(). Spans and arrays
 * in the copy stay BORROWED from the handle (handle lifetime), exactly as through
 * moq_media_track_desc_get(): the cmaf record pointer is copied, not the
 * record, which stays handle-owned. The nested info stamp is carried
 * verbatim and is meaningful only inside the outer prefix.
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
    /* Appended after fill: the OBJECT_DELIVERY_TIMEOUT and
     * SUBGROUP_DELIVERY_TIMEOUT that every SUBSCRIBE for the track carries,
     * in milliseconds, as moq_sub_track_cfg_t takes them. NULL leaves one
     * absent. The values are copied before returning. Draft 16 carries one
     * shared value: a selected value must be nonzero, and two selected
     * values must be equal; otherwise MOQ_ERR_INVAL. */
    const uint64_t                  *object_delivery_timeout_ms;
    const uint64_t                  *subgroup_delivery_timeout_ms;
    /* Appended: the subscription's initial Forward State, borrowed for the
     * call (read only when struct_size covers the whole pointer). NULL keeps
     * Forward 1. Non-NULL false subscribes with Forward 0: the track is
     * wanted and known but passive, the publisher sends it no Objects, and
     * the receiver never raises its Forward State itself (flow control and
     * subscribe_track resumes leave it at 0). It applies only when the
     * subscription is first issued, as the other options do. A passive track
     * takes no implicit current-group join fill on draft 22. A passive track
     * that stays silent is not an error and raises no event. The app can
     * raise and lower its Forward State later with
     * moq_media_receiver_set_track_forward(). */
    const bool                      *forward;
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
 * Replaced handles: subscribe_track, unsubscribe_track and fill_track on a
 * handle an MSF-02 replacement removed (TRACK_REMOVED with replaced_by) act
 * on the newest handle of its replacement chain and return that call's
 * result, so the latest intent reaches the live handle even when it was
 * issued before the application saw the replacement events. A selection
 * made before the replacement moved with it; a fill command recorded before
 * it belonged to the old subscription and was dropped. Other removed handles
 * keep refusing (MOQ_ERR_WRONG_STATE).
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
 *                     build, a timeline of a kind it does not decode, an
 *                     MSF-01 namespace override, or a receiver policy: see
 *                     the top of this header), or a location/fill is not
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
 * (and MOQ_OK) for an already-unsubscribed or locally failed track.
 *
 * A track a later catalog removed is MOQ_ERR_WRONG_STATE, with one exception:
 * a track a terminal catalog (isComplete) removed while its subscription was
 * still delivering -- it stays wanted, and its TRACK_REMOVED waits for the
 * subscription's done (its END_OF_TRACK alone can overtake its last Objects).
 * Unsubscribing such a track is the same disable: its queued objects
 * are purged, nothing more of it is delivered, and the receiver then retires
 * its subscription and emits TRACK_REMOVED. Cancellation does not synthesize
 * TRACK_ENDED or retract an end already reported. It cannot be subscribed
 * again. */
MOQ_API moq_result_t moq_media_receiver_unsubscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track);

/* Raise (forward = true) or lower (false) the Forward State of a track the
 * app subscribed passively (track subscribe cfg forward = &false). Records the
 * intent and wakes the executor, which sends one REQUEST_UPDATE with the
 * Forward State once the subscription is established and no other update of
 * the track is in flight, as for flow control: intents that cancel out before
 * the update goes out send nothing. Flow control still applies to a raised
 * track (a FLOW_CONTROL pause sends Forward 0 until resumed). Raising issues no
 * join fill on any draft; Objects the publisher sends after the update reach
 * the track as live deliveries. The intent survives a re-subscription of the
 * same track within one receiver (unsubscribe_track then subscribe_track):
 * the new SUBSCRIBE goes out with Forward 0, as the track's options ask, and
 * an update to Forward 1 follows once it is established. A new receiver
 * starts passive.
 *
 * Returns:
 *   MOQ_OK            recorded (also when the Forward State already matches)
 *   MOQ_ERR_INVAL     NULL r or track, or a foreign track
 *   MOQ_ERR_WRONG_STATE  not subscribed passively, disabled by
 *                     unsubscribe_track, ended, failed or removed
 *   MOQ_ERR_CLOSED    the receiver or its endpoint is terminal */
MOQ_API moq_result_t moq_media_receiver_set_track_forward(
    moq_media_receiver_t *r, moq_media_track_t *track, bool forward);

/* Request a D22 fill on `track`'s existing subscription: an UPDATE carrying
 * `fill` (draft-22 sections 3.4 and 9.5). The subscription, its filter and
 * the track's other fills are unchanged; a new fill never cancels one. `fill`
 * is required (moq_fill_cfg_init gives the defaults); it and fill->location
 * borrow only for this call, and their selected values are copied before
 * returning.
 *
 * Nonblocking and callable from any thread: it records one command and wakes
 * the endpoint executor, which sends the UPDATE. MOQ_OK means recorded, not
 * sent or accepted by the peer. The interrupt latch of wait() neither refuses
 * nor withdraws it. The executor sends it only while the track is wanted, its
 * subscription is established and delivering (not paused by FLOW_CONTROL),
 * and no other UPDATE of the track awaits its response: a resume is sent and
 * acknowledged first, and a pause takes precedence over a queued fill.
 *
 * Once the UPDATE is admitted the track gets MOQ_MEDIA_TRACK_FILL_CREATED,
 * with the request ID the fill is identified by; cancel it with
 * moq_media_receiver_cancel_fill(). Its objects carry that ID, and its
 * FILL_TERMINAL ends it. A track holds one unobserved command: until its
 * FILL_CREATED is polled (by any output size), a second call returns
 * MOQ_ERR_WOULD_BLOCK with no effect. Several admitted fills may coexist.
 *
 * unsubscribe_track withdraws a command not yet taken by the executor, and
 * no FILL_CREATED follows. Once taken, its admission may still win: it keeps
 * the slot until its FILL_CREATED is polled, and the admitted fill is then
 * cancelled only by its ID. A disable never cancels admitted fills. A command
 * is also withdrawn, with no event, when its track ends, fails or is removed,
 * when the receiver closes, and when a live-to-VOD re-subscription replaces
 * the subscription it was recorded for. A permanent local UPDATE failure is
 * reported as MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE, as for subscribe_track.
 *
 * Returns:
 *   MOQ_OK            recorded and scheduled
 *   MOQ_ERR_INVAL     NULL r/track/fill, a foreign track, or a malformed
 *                     selected value (an unknown group order or location kind,
 *                     or a location ending before its start)
 *   MOQ_ERR_ABI_MISMATCH  fill or fill->location below its frozen floor
 *   MOQ_ERR_UNSUPPORTED   not draft-22 with the D22 companion profile (drafts
 *                     16 and 18 have no fills); nothing is recorded or sent
 *   MOQ_ERR_WRONG_STATE   the track is not subscribed (never subscribed, or
 *                     disabled by unsubscribe_track), has ended, failed
 *                     locally or been removed
 *   MOQ_ERR_WOULD_BLOCK   the track's previous command is not yet observed
 *   MOQ_ERR_LIMIT     the receiver's command identities are exhausted
 *   MOQ_ERR_CLOSED    the receiver or its endpoint is terminal */
MOQ_API moq_result_t moq_media_receiver_fill_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_fill_cfg_t *fill);

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

/* The range of a standalone fetch of a media track:
 * [start_group:start_object, end_group:end_object), an exclusive end;
 * end_object 0 requests the whole end_group, as moq_fetch_cfg_t. The fetch
 * carries the subscriber priority when has_subscriber_priority is set. */
typedef struct moq_media_fetch_cfg {
    uint32_t struct_size;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
} moq_media_fetch_cfg_t;

/* Frozen v0 floor: the layout through subscriber_priority, ending at that
 * field (its tail padding depends on the data model). fetch_track accepts
 * any struct_size at or above it and reads an appended field only when
 * struct_size covers the whole field. */
#define MOQ_MEDIA_FETCH_CFG_V0_SIZE \
    (offsetof(moq_media_fetch_cfg_t, subscriber_priority) + sizeof(uint8_t))

/* Pointer-only initializer: clears and stamps ONLY the frozen v0 prefix.
 * The sized initializer clears and stamps min(cfg_size, this library's
 * struct size); no-op if cfg is NULL or cfg_size cannot hold struct_size. */
MOQ_API void moq_media_fetch_cfg_init(moq_media_fetch_cfg_t *cfg);
MOQ_API void moq_media_fetch_cfg_init_sized(moq_media_fetch_cfg_t *cfg,
                                            size_t cfg_size);

/* Request a standalone FETCH of `track`'s Objects in `cfg`'s range, on the
 * session the receiver subscribes on, with the track's namespace and name.
 * Every draft the receiver supports carries it (16, 18 and 22). `cfg` is
 * required and borrowed only for this call; its values are copied before
 * returning.
 *
 * The track must be subscribed (subscribe_track, not disabled since) and
 * not ended, failed or removed; a passive track (subscribed with Forward 0)
 * is the expected caller. The executor sends the FETCH once the track's
 * subscription is established.
 *
 * Nonblocking and callable from any thread: it records one command and wakes
 * the endpoint executor, which sends the FETCH. MOQ_OK means recorded, not
 * sent or accepted by the peer. The interrupt latch of wait() neither refuses
 * nor withdraws it. Once the FETCH is sent the track gets
 * MOQ_MEDIA_TRACK_FETCH_CREATED with the fetch's request ID; a command
 * refused before anything is sent gets MOQ_MEDIA_TRACK_FETCH_TERMINAL with
 * MOQ_MEDIA_FETCH_NOT_SENT instead. A track holds one unobserved command:
 * until that event is polled (by any output size), a second call returns
 * MOQ_ERR_WOULD_BLOCK with no effect. Several sent fetches of a track may be
 * open at once, up to moq_media_receiver_cfg_t.max_fetches_per_track; each
 * stays open until its FETCH_TERMINAL is polled.
 *
 * The fetch's Objects come out of poll_object with delivery_origin
 * MOQ_MEDIA_DELIVERY_FETCH and the fetch's request ID in
 * initiating_request_id, in the fetch stream's order. They count against
 * the receiver's queue limits like any Object, and the overflow policy
 * drops them like any Object. An MPEG2TS track's fetched Objects bypass its
 * reorder window and take no join PSI (mpeg2ts_join_psi); they are
 * validated as whole packets, mpeg2ts_strip_prefix applies to them, and each
 * reports the fetch's own loss (ranges the peer marked Unknown or
 * Timed-Out, and local drops) as its gap. A LOCMAF track's fetched Objects
 * are decoded by a decoder of the fetch's own, as a fill's are (see
 * LOCMAF above), and come out as the CMAF chunks they encode; a range that
 * starts after a Group's full header gets none of that Group's deltas
 * before its next full header. The application merges them with
 * the live Objects by Location. Each fetch ends with exactly one
 * FETCH_TERMINAL, queued after every Object of the fetch has been polled or
 * dropped; a fetch open when the session closes ends with
 * MOQ_MEDIA_FETCH_SESSION_CLOSED. A receiver that turns terminal (fatal)
 * ends open fetches as it ends open fills: it synthesizes no FETCH_TERMINAL
 * (one already pollable stays pollable), except after these catalog
 * failures: the catalog failing while it is still being bootstrapped, a
 * catalog ACK or delivery loss, or a credential failure at service scope.
 * After one of those, each open fetch still ends: its Objects not yet taken
 * from the session are discarded, and it ends with the peer's terminal, or
 * with MOQ_MEDIA_FETCH_SESSION_CLOSED if the session closes first. A refusal
 * of the catalog subscription after the catalog took effect is an ordinary
 * fatal, with no terminal. When moq_media_receiver_is_fatal() reports true
 * and no FETCH_TERMINAL is pollable for a fetch, the application treats that
 * fetch as ended.
 *
 * unsubscribe_track withdraws a command not yet taken by the executor, with
 * no event, as does the track's end, failure or removal and the receiver's
 * close; it does not cancel a sent fetch, whose Objects are then dropped as
 * the track's are. See moq_media_receiver_withdraw_fetch() and
 * moq_media_receiver_cancel_fetch().
 *
 * Returns:
 *   MOQ_OK            recorded and scheduled
 *   MOQ_ERR_INVAL     NULL r/track/cfg, a foreign track, a range that ends
 *                     at or before its start, or, on draft 16, a value
 *                     beyond 62 bits
 *   MOQ_ERR_ABI_MISMATCH  cfg->struct_size below MOQ_MEDIA_FETCH_CFG_V0_SIZE
 *   MOQ_ERR_UNSUPPORTED   the receiver has no Object path for this track
 *                     (not a media track with a delivery path)
 *   MOQ_ERR_WRONG_STATE   the track is not subscribed (never subscribed, or
 *                     disabled by unsubscribe_track), has ended, failed
 *                     locally or been removed
 *   MOQ_ERR_WOULD_BLOCK   the track's previous command is not yet observed
 *   MOQ_ERR_CAPACITY  max_fetches_per_track fetches of the track are open;
 *                     polling one's FETCH_TERMINAL frees room
 *   MOQ_ERR_LIMIT     the receiver's command identities are exhausted
 *   MOQ_ERR_CLOSED    the receiver or its endpoint is terminal */
MOQ_API moq_result_t moq_media_receiver_fetch_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_media_fetch_cfg_t *cfg);

/* Cancel one open standalone fetch of `track`, selected by the request ID
 * its FETCH_CREATED carried. Zero is a valid ID. Records bounded intent and
 * wakes the endpoint executor, which cancels the fetch on the session.
 * MOQ_OK means scheduled (repeating it while it is pending is idempotent),
 * not that the cancellation has won a race with the peer's end of the
 * fetch: the fetch's single FETCH_TERMINAL reports whichever won
 * (MOQ_MEDIA_FETCH_CANCELLED when the cancellation did). Objects already
 * queued stay pollable and FETCH_TERMINAL follows them. The receiver
 * cancels a fetch only on this call; it never cancels one by itself.
 *
 * Returns INVAL for NULL r/track or a foreign track, STALE_HANDLE when no
 * open fetch of the track has that ID (never created, or already ended),
 * CLOSED for a terminal receiver or endpoint. */
MOQ_API moq_result_t moq_media_receiver_cancel_fetch(
    moq_media_receiver_t *r, moq_media_track_t *track,
    uint64_t fetch_request_id);

/* Withdraw `track`'s fetch command that the executor has not taken yet.
 * Nonblocking and callable from any thread.
 *
 * Returns:
 *   MOQ_OK            the command was withdrawn: no FETCH went out, no
 *                     FETCH_CREATED or FETCH_TERMINAL follows, and the
 *                     command slot is free
 *   MOQ_DONE          the track has no recorded command
 *   MOQ_ERR_WRONG_STATE  the executor already took the command: its
 *                     admission completes and its FETCH_CREATED (or a
 *                     FETCH_TERMINAL with MOQ_MEDIA_FETCH_NOT_SENT) follows,
 *                     or is queued; cancel a created fetch by its request ID
 *   MOQ_ERR_INVAL     NULL r/track, or a foreign track
 *   MOQ_ERR_CLOSED    the receiver or its endpoint is terminal */
MOQ_API moq_result_t moq_media_receiver_withdraw_fetch(
    moq_media_receiver_t *r, moq_media_track_t *track);

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
    MOQ_MEDIA_TRACK_STATE_FAILED = 6      /* local ACK retention, LIVE delivery,
                                            UPDATE admission or credential failure;
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
 * origin observation. FILL's and FETCH's initiating request ID may be zero.
 * FETCH: an Object of a standalone fetch (moq_media_receiver_fetch_track). */
#define MOQ_MEDIA_DELIVERY_LIVE  1u
#define MOQ_MEDIA_DELIVERY_FILL  2u
#define MOQ_MEDIA_DELIVERY_FETCH 3u

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
    /* RAW (LOC): decode_time_us copies the LOC media time, as does
     * presentation_time_us (composition_offset_us 0). It may decrease in
     * decode order (object order) with frame reordering and is not evidence
     * of a known decode clock. CMAF: the tfdt/trun decode time. */
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
     *                          datagram, which has no subgroup on the wire,
     *                          and for an Object of a standalone fetch
     *                          (MOQ_MEDIA_DELIVERY_FETCH), whose subgroup the
     *                          receiver is not told. subgroup_id is then
     *                          zero, and only this flag says so: zero is also
     *                          a real subgroup id;
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
     * when the receiver tracks Location continuity for the Object's track
     * (MPEG2TS; a LOCMAF track; and a "loc" track whose MSF-02 streamMapping
     * is "object" or a value not recognized, on its live subscription) and
     * the fields below are meaningful; false means not tracked, and says
     * nothing about loss. On a LOCMAF track a gap is a LOCMAF Object
     * dropped before delivery (one that could not be decoded, held past
     * its deadline, or of a closed Group), or an Object ID gap within its
     * Group.
     * For such a LOC track the receiver restores (Group, Object) order, which
     * separate subgroup streams do not keep, through a bounded stage: up to
     * four open Groups and 64 held Objects, each stall waiting at most
     * 300 ms. A position given up by that window or by its capacity is
     * reported as a gap (a policy, not proof the network lost it), and a
     * Group the window retires without a known end reports the next Group's
     * first Object with gap_tail_unknown. A publisher that signals no Group
     * end (no END_OF_GROUP subgroup, no END_OF_GROUP status Object) delays
     * each Group boundary by up to the window; an END_OF_GROUP status Object
     * when the next Group starts removes the wait. A fill is one ordered
     * stream and bypasses the stage.
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
     * On LOC-05 an object naming a codec other than the track's inherits no
     * configuration (see codec_string below).
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
     * FETCH carries the request ID of the standalone FETCH, as its
     * FETCH_CREATED reported it.
     * Plain values survive receiver destruction along with the owned object;
     * the borrowed track handle does not. No ownership or thread rule changes. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t              delivery_origin; /* MOQ_MEDIA_DELIVERY_* */
    uint64_t              initiating_request_id;

    /* Appended after the complete layout through initiating_request_id on
     * every target. Available only when struct_size covers codec_string
     * entirely; zero on every companion but D22_MSF01_LOC05_PUBLIC. The
     * effective LOC-05 properties (MOQ_LOC_EXT_HAS_* bits in loc_ext_present,
     * see moq_loc_ext_t): codec_string is the object's own Codec String, else
     * its track's. It borrows properties_ref or this object's private
     * track_properties_ref and stays valid until cleanup, like the config
     * spans in metadata. It adds no owned reference and no charge. Raw Track
     * and Object Properties stay separate and unchanged, and the catalog codec
     * stays in the track descriptor. A Codec String and a configuration are
     * the two halves of one decoder configuration, so an object that names
     * its own codec carries the track's video/audio configuration in metadata
     * only when that codec is the track's: its Track Properties' Codec
     * String, else the catalog codec (moq_media_object_parse_sized). */
    uint32_t              loc_ext_present;
    uint32_t              _reserved_loc_ext; /* never read */
    moq_bytes_t           codec_string;
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
 * before its first object. For a D22 companion (explicit, or AUTO resolved on
 * draft 22), obj_size must cover
 * the whole metadata/ref and delivery-origin observation through the end of
 * initiating_request_id, and for explicit D22_MSF01_LOC05_PUBLIC through the
 * end of codec_string; otherwise
 * MOQ_ERR_UNSUPPORTED leaves output and queue untouched, with no transfer.
 * Legacy D16/18 retains the v0 minimum. */
MOQ_API moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r,
                                                    moq_media_object_t *obj,
                                                    size_t obj_size);

/* Poll the next Object only when everything it carries fits in
 * `max_charge` bytes: its payload, properties and private track-properties
 * buffers and its owned sample table, as moq_media_object_charge measures
 * them. The check and the transfer are one step: no other Object can
 * replace the head between them. `charge` (optional) receives that
 * Object's charge on MOQ_OK and on MOQ_ERR_BUFFER. MOQ_ERR_BUFFER: the next
 * Object is larger than `max_charge`; nothing is dequeued and nothing
 * changes. Every other result is poll_object's. */
MOQ_API moq_result_t moq_media_receiver_poll_object_within(
    moq_media_receiver_t *r, uint64_t max_charge, uint64_t *charge,
    moq_media_object_t *obj, size_t obj_size);

/* The bytes a polled object holds until cleanup, as poll_object_within
 * charges them; 0 for NULL. Reads only what its stamped struct_size
 * covers. */
MOQ_API uint64_t moq_media_object_charge(const moq_media_object_t *obj);

/* -- Stream terminals -------------------------------------------------- *
 * With moq_media_receiver_cfg_t.surface_stream_terminals, the receiver keeps
 * an ordered lane of delivery facts for its media tracks, of every
 * packaging, so a consumer can tell where delivery ended, what was lost and
 * where the track ends, relative to the objects it polls:
 *
 *   - FIN_END_OF_GROUP / FIN / RESET: how the stream carrying a subgroup
 *     ended, with the last Object it carried (completely, for RESET), or
 *     for a RESET that cut an Object short, that Object;
 *   - RECEIVER_DROP: the receiver's overflow policy dropped Objects of the
 *     track in groups group_id..last_group_id (its safety loss, beyond the
 *     drop counters), from one delivery domain: the LIVE subscription, one
 *     fill, named by its initiating request ID, or one standalone fetch,
 *     named by its request ID. The drops of one
 *     admission are one record per track and domain;
 *   - END_OF_TRACK: the track's END_OF_TRACK, at its Location.
 *
 * Ordering: every terminal is one record, never coalesced, in the order the
 * receiver learned of it. A record is pollable once every Object of its
 * track the receiver took in before it has left the receiver (polled,
 * dropped, evicted or purged), including Objects an MPEG2TS reorder window
 * still holds, and the lane is read in order. To apply each fact in order,
 * poll terminals after each polled Object (or poll Objects until none is
 * queued, then terminals); a record never comes back ahead of an Object it
 * follows. A consumer that moves polled Objects into its own pending
 * storage must settle those before applying a later terminal.
 *
 * Fill terminals: with the lane on, a fill's MOQ_MEDIA_TRACK_FILL_TERMINAL
 * is pollable only once every Object of the fill has left the receiver and
 * every RECEIVER_DROP of the fill has been polled, so a fill does not
 * report completion while its Objects can still be lost or before its loss.
 * A standalone fetch's MOQ_MEDIA_TRACK_FETCH_TERMINAL follows the same rule.
 *
 * Exact debt: the receiver takes in an Object only while the lane has room
 * for every record that admission can produce: a RECEIVER_DROP per
 * delivery domain with queued Objects, and one for the Object itself. It
 * takes in a stream terminal only while it has room for that record.
 * Otherwise intake stops, under every overflow policy: the next Object
 * waits upstream, so the subscription backs up as it does under
 * FLOW_CONTROL, and no record is ever lost or merged with another
 * admission's.
 *
 * MPEG2TS: an Object, status Object or END_OF_TRACK that a reorder window
 * holds keeps the lane slot its intake reserved for its own record, so a
 * record waiting for it never takes the room its release needs. A release
 * that fits the queue destroys nothing and needs no more; one that would
 * evict also needs a RECEIVER_DROP per delivery domain with queued Objects.
 * Until the lane has that, the window waits, reports no deadline, and the
 * application's next poll wakes the network thread to resume it; a
 * subscription that completes meanwhile is retired once its window has
 * released everything.
 *
 * Retirement: a track removed from the catalog keeps reporting the stream
 * terminals already received for it, under its removed handle, until its
 * subscription is released; a track added back is a new handle. A receiver
 * whose catalog failed discards its media, with the stream terminals it has
 * not yet put on the lane and what its MPEG2TS reorder windows hold; the
 * records already on the lane stay, and become pollable as the media they
 * follow leaves.
 *
 * Capacity: a lane of one record works, taking in one Object at a time;
 * every reserved slot counts against max_stream_terminals, so a deeper lane
 * lets more Objects be queued or held at once. Polling Objects and records
 * always lets intake resume. A consumer that enables the lane polls it.
 *
 * Records name the stable track handle and carry plain values only. */
typedef uint32_t moq_media_stream_terminal_kind_t;
#define MOQ_MEDIA_STREAM_FIN_END_OF_GROUP 1u
#define MOQ_MEDIA_STREAM_FIN              2u
#define MOQ_MEDIA_STREAM_RESET            3u
#define MOQ_MEDIA_STREAM_RECEIVER_DROP    4u
#define MOQ_MEDIA_STREAM_END_OF_TRACK     5u

typedef struct moq_media_stream_terminal {
    uint32_t                         struct_size;
    moq_media_stream_terminal_kind_t kind;
    moq_media_track_t               *track;
    /* FIN_END_OF_GROUP, FIN, RESET: the subgroup. END_OF_TRACK: its
     * Location (group_id, object_id). RECEIVER_DROP: the first group. */
    uint64_t                         group_id;
    uint64_t                         subgroup_id;
    uint64_t                         object_id;
    /* RECEIVER_DROP: the last group of the range. */
    uint64_t                         last_group_id;
    /* RESET: the peer's RESET_STREAM code. */
    uint64_t                         error_code;
    /* FIN_END_OF_GROUP, FIN, RESET: as moq_sub_stream_terminal_t. */
    bool                             has_last_object;
    bool                             has_cut_object;
    uint8_t                          _reserved[2];
    /* RECEIVER_DROP: whose Objects were dropped, a MOQ_MEDIA_DELIVERY_*
     * value, and for FILL the fill's initiating request ID, for FETCH the
     * fetch's request ID (zero is a valid ID). Drops of one admission are
     * one record per track and
     * delivery domain. */
    uint32_t                         delivery_origin;
    uint64_t                         last_object_id;
    uint64_t                         cut_object_id;
    uint64_t                         initiating_request_id;
} moq_media_stream_terminal_t;

/* Frozen v0 floor: the complete layout through initiating_request_id. */
#define MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE \
    (offsetof(moq_media_stream_terminal_t, initiating_request_id) + \
     sizeof(uint64_t))

/* Poll the next stream terminal. `out_size` is the caller's sizeof; one
 * below the v0 floor is MOQ_ERR_ABI_MISMATCH and dequeues nothing. MOQ_OK
 * fills out; MOQ_DONE when no record is pollable now (none queued, the lane
 * is disabled, or the head record still waits for its track's Objects);
 * MOQ_ERR_INVAL for a NULL argument. moq_media_receiver_wait() returns when
 * a record is pollable. */
MOQ_API moq_result_t moq_media_receiver_poll_stream_terminal(
    moq_media_receiver_t *r, moq_media_stream_terminal_t *out, size_t out_size);

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
    /* Appended after tracks_malformed; one block, read only when struct_size
     * covers loc_order_forced.
     *
     * catalog_revision: the MOQ_MSF_REVISION_* of the effective catalog,
     * valid once CATALOG_READY was queued.
     *
     * MSF_COMPRESSION (MSF-02 12.1), per catalog or media-timeline Object:
     * objects_decompressed and decompress_failures (malformed compressed
     * data) and decompress_limited (over the decompressed-size cap) count
     * decompression; compression_refused counts Objects and subscriptions
     * refused for an algorithm this build does not decompress (GZIP
     * included: decompression is not implemented yet, so no Object is
     * decompressed and the first three stay zero), a repeated or malformed
     * property, or a compressed MSF-01 catalog; compression_conflicts counts
     * Objects carrying the Object property where the Track property is set;
     * compression_ignored counts timeline Objects whose property was not
     * interpreted because the effective catalog is MSF-01.
     *
     * event_timeline_headers_ignored: SAP-timeline Objects whose origin
     * carried MSF_EVENT_TIMELINE_HEADER, which CMSF-01 defines no meaning for;
     * the records are parsed as without it.
     *
     * loc_order_gaps: Objects the in-group ordering stage of an "object"-
     * mapped LOC track gave up on (the window expired or its capacity forced
     * it; a policy, not proof the network lost them); loc_order_forced: the
     * advances its capacity policy forced. */
    uint32_t catalog_revision;
    uint32_t _reserved_catalog_revision;   /* zero; never read */
    uint64_t objects_decompressed;
    uint64_t decompress_failures;
    uint64_t decompress_limited;
    uint64_t compression_refused;
    uint64_t compression_conflicts;
    uint64_t compression_ignored;
    uint64_t event_timeline_headers_ignored;
    uint64_t loc_order_gaps;
    uint64_t loc_order_forced;
    /* Appended after loc_order_forced; one block, written only when out_size
     * covers locmaf_resets (MOQ_MEDIA_RECEIVER_STATS_SIZE_THROUGH_LOCMAF).
     * LOCMAF tracks (draft-einarsson-moq-locmaf-01), whose Objects arrive as
     * canonical CMAF chunks. Every LOCMAF Object received on a track
     * subscription is counted once: rebuilt (decoded and handed to delivery;
     * the queue may still drop it, counted as for any Object), undecodable
     * (malformed, unsupported, without its reference, or refused by the
     * per-group rules below), expired (held for a fill hand-off that did not
     * come in time, or past the hold's capacity) or late (its group already
     * closed). So, on the subscription, received = rebuilt + undecodable +
     * expired + late. Objects of a fill or a standalone fetch are counted
     * as rebuilt or undecodable. locmaf_groups_evicted: groups closed
     * because a third group opened while two were in flight; locmaf_resets:
     * decoder resets for a reset stream, a datagram, a second subgroup or a
     * disabled track. */
    uint64_t locmaf_objects_rebuilt;
    uint64_t locmaf_objects_undecodable;
    uint64_t locmaf_objects_expired;
    uint64_t locmaf_objects_late;
    uint64_t locmaf_groups_evicted;
    uint64_t locmaf_resets;
} moq_media_receiver_stats_t;

/* The stats through the LOCMAF block: the out_size that receives it. */
#define MOQ_MEDIA_RECEIVER_STATS_SIZE_THROUGH_LOCMAF \
    (offsetof(moq_media_receiver_stats_t, locmaf_resets) + sizeof(uint64_t))

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
 *     full field fits in out_size), except the LOCMAF block, which is written
 *     whole or not at all: an out_size that ends inside it gets the fields
 *     through loc_order_forced. */
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
/* -- Receiver information --------------------------------------------- *
 * What a component built on a receiver needs to know about it, as one
 * sized snapshot. Configuration values are the effective ones: a defaulted
 * zero is reported as the value it selected. */
typedef struct moq_media_receiver_info {
    uint32_t                      struct_size;
    uint32_t                      time_mode;        /* moq_media_time_mode_t */
    uint32_t                      overflow_policy;  /* moq_media_overflow_policy_t */
    uint32_t                      max_objects;
    uint64_t                      max_bytes;
    bool                          auto_subscribe;
    bool                          surface_stream_terminals;
    uint8_t                       _reserved_flags[2];   /* zero; never read */
    /* MOQ_MEDIA_COMPANION_AUTO until the first session resolves it. */
    moq_media_companion_profile_t companion_profile;
    /* The negotiated draft (a moq_version_t value); 0 until a session has
     * been observed. */
    uint32_t                      transport_version;
    uint32_t                      max_stream_terminals; /* 0 with the lane off */
    /* Track events poll_track has returned so far. */
    uint64_t                      track_events_polled;
} moq_media_receiver_info_t;

/* Frozen v0 floor: the complete layout through track_events_polled. */
#define MOQ_MEDIA_RECEIVER_INFO_V0_SIZE \
    (offsetof(moq_media_receiver_info_t, track_events_polled) + sizeof(uint64_t))

/* Fill `out` with min(out_size, sizeof) bytes and stamp struct_size with
 * that count. MOQ_ERR_INVAL for a NULL argument; MOQ_ERR_ABI_MISMATCH below
 * the v0 floor (nothing written). Observes only; any thread. */
MOQ_API moq_result_t moq_media_receiver_get_info(
    const moq_media_receiver_t *r, moq_media_receiver_info_t *out,
    size_t out_size);

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
