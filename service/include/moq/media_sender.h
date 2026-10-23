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
 * on the network thread, generating the standard LOC properties from the
 * typed fields and abandoning open subgroups wire-correctly under
 * backpressure. The media companion defaults to AUTO, which follows the
 * negotiated draft (legacy LOC-01 on drafts 16/18, LOC-04 on draft 22); an
 * explicit companion pins one and refuses the other drafts.
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/endpoint.h>
#include <moq/media_object.h>
#include <moq/media_auth.h>
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

/* Pointer-only initializer: clears and stamps ONLY the frozen v0 prefix
 * (through on_subscriber_left). It cannot know the caller's storage size, so
 * it never touches the appended on_ready/on_closed/on_track_closed fields --
 * they stay disabled (struct_size == the v0 prefix). To SET any appended
 * callback, initialize with moq_media_sender_callbacks_init_sized(cb,
 * sizeof *cb) instead. */
MOQ_API void moq_media_sender_callbacks_init(moq_media_sender_callbacks_t *cb);

/* Sized initializer: clears and stamps min(cb_size, this library's struct
 * size). Pass sizeof(*cb) to enable every callback your build knows about.
 * Safe for a caller compiled against an older, smaller header too. */
MOQ_API void moq_media_sender_callbacks_init_sized(
    moq_media_sender_callbacks_t *cb, size_t cb_size);

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
     * queued until demand appears; true drops it to stay at the live edge.
     * After a no-demand drop, the remainder of that group is also discarded,
     * even if demand returns. Media resumes at the next sync-anchored group
     * (starts_group, or the first object after ends_group), not at a sync
     * object within the same group. Other tracks and END_OF_TRACK still drain. */
    bool                                 drop_without_demand;

    /* OPT-IN interval between automatic independent catalog refreshes, in
     * microseconds. Default OFF: MSF-01 §5 says a catalog object SHOULD be
     * published only when the availability of tracks changes, or after a
     * period of time such that the object might fall out of cache in a
     * delivery network. The sender always republishes on a real track
     * add/remove/conversion; it does NOT republish on a timer unless asked.
     *   0 (or an old-size caller whose struct_size predates this field) or
     *      UINT64_MAX: no timed refresh (the default).
     *   a finite nonzero value: while the catalog track has demand,
     *      republish the current catalog every interval as a new
     *      independent group (object 0 = the complete catalog, never a
     *      delta). Use this only for the cache-staleness case the draft
     *      allows, e.g. a late viewer joining through a relay that resolves
     *      Joining FETCHes locally and neither proxies nor caches unresolved
     *      ones upstream (there only the FIRST catalog joiner per publisher
     *      session obtains the catalog), and pick an interval on the order
     *      of the relay's cache lifetime rather than seconds.
     * A real mutation generation always takes precedence and resets the
     * refresh cadence; no demand means no refresh. */
    uint64_t                             catalog_refresh_interval_us;
    /* Media companion. AUTO is the default: an absent or partially covered
     * field, and every initializer that covers it (the _sized forms), select
     * it. A zeroed field is LEGACY_MSF01_LOC01, not AUTO.
     *
     * Explicit companions are fixed at creation: LEGACY_MSF01_LOC01 requires
     * negotiated D16/18 and D22_MSF01_LOC04_PUBLIC negotiated D22. A known
     * mismatch reports UNSUPPORTED through write/wait and stats.last_error,
     * never endpoint/session termination.
     *
     * AUTO resolves once, when the sender first observes a draft-16/18/22
     * session, to the matching companion, and then behaves exactly as that
     * companion (any other draft is UNSUPPORTED). Nothing is published before
     * then, so the catalog and every object follow the resolved companion.
     * Until it resolves, AUTO differs from an explicit companion in these
     * ways only:
     *   - Creation reads the wallclock Group-ID base described below, and
     *     fails as D22_MSF01_LOC04_PUBLIC does when it cannot. Objects written
     *     before resolution are numbered from it; resolving to legacy renumbers
     *     them from 0, before any is sent.
     *   - add_track accepts a CMAF track without init_data. If the sender then
     *     resolves to draft 22 with such a track still registered, it reports
     *     UNSUPPORTED (write/wait, stats.last_error) and publishes nothing
     *     until those tracks are removed.
     *   - write() accepts RAW objects (explicit D22 refuses writes before
     *     D22 is observed). A RAW object's metadata tail, when present, is
     *     validated and kept for draft 22 (an invalid one is INVAL); its
     *     legacy timestamp must fit the narrowest draft (see
     *     presentation_time_us).
     * On draft 22, an AUTO sender's RAW object written without the metadata
     * tail is not refused: it carries LOC-04 metadata generated from the typed
     * fields. With has_capture_time, capture_time_us is an unscaled Timestamp
     * (Unix-epoch microseconds, LOC-04 2.3.1.1). Otherwise
     * presentation_time_us is media time: the Timestamp goes with a Timescale
     * of 1000000 (2.3.1.2), so a receiver reads it as application media time,
     * not a capture clock. On video a one-byte frame marking whose I bit is
     * is_sync is added. A track whose track_properties carry a LOC-04
     * Timescale (or do not parse as LOC-04 metadata) would make a receiver
     * read every object's Timestamp at that scale, so on such a track a
     * write with has_capture_time and no metadata tail is MOQ_ERR_INVAL
     * whenever the object may go out as draft 22: once resolved to it, or
     * before resolution when the endpoint offers draft 22. Explicit
     * D22_MSF01_LOC04_PUBLIC still requires the tail. On drafts 16/18 the
     * tail is ignored, as with LEGACY.
     *
     * D22_MSF01_LOC04_PUBLIC also selects its MSF-01 catalog: version
     * "draft-01"; every track's init_data in the root initDataList,
     * referenced by initRef (no per-track initData); a CMAF track without
     * init_data refused by add_track (CMSF-01 3.1); an added track with
     * init_data published as a full catalog, not a delta; and each track's
     * Group IDs (media, generated timeline and catalog) starting at the
     * wallclock milliseconds read when the sender is created, the
     * best-effort approach MSF-01 6.1 suggests. It keeps a restarted
     * publisher's IDs above its predecessor's only while the clock moves
     * forward and no instance numbers groups faster than the clock; restart
     * identity otherwise remains the application's responsibility. Creation
     * fails with MOQ_ERR_INTERNAL when the clock cannot be read and with
     * MOQ_ERR_LIMIT when the reading is above the largest Group ID the
     * library writes (2^62 - 1, its object write-path bound on every
     * profile). Group IDs never wrap: a write that would open a group past
     * that ID, and an add_track / remove_track / convert_to_vod / complete
     * whose catalog change would need one, return MOQ_ERR_LIMIT without
     * taking ownership or changing state. The legacy companion keeps its
     * catalog and Group IDs from 0. Unknown values fail creation with
     * MOQ_ERR_INVAL. */
    moq_media_companion_profile_t         companion_profile;
    /* Preserve the complete preceding allocation; never read. */
    uint8_t                             _reserved_companion_profile[4];
    /* Optional resource credentials, independent of endpoint SETUP tokens.
     * Copied at construction, including all static token bytes. Selectors
     * and local outcomes follow media_auth.h. Absent/partial/NULL means
     * anonymous; a configured source never falls back to anonymous access.
     * Use a sized initializer to enable this complete pointer field. */
    const moq_media_auth_cfg_t          *resource_auth;
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

/* Sized forms of the three initializers above. The pointer-only forms clear
 * and stamp ONLY the frozen v0 prefix (through the v0 nested callbacks), so
 * they never enable the appended publish_tracks / drop_without_demand /
 * catalog_refresh_interval_us fields or the appended callbacks. To set ANY
 * appended field -- e.g. a live-preset push sender, or an opt-in timed
 * catalog refresh interval -- initialize with the _sized form and sizeof(*cfg):
 *
 *     moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
 *     cfg.publish_tracks = true;
 *
 * Each clears and stamps min(cfg_size, this library's struct size) and applies
 * its preset defaults (companion_profile = AUTO among them) only to fields
 * that lie within that prefix; the nested
 * callbacks struct is initialized to the portion the caller's storage covers.
 * Safe for a caller compiled against an older, smaller header too. */
MOQ_API void moq_media_sender_cfg_init_sized(moq_media_sender_cfg_t *cfg,
                                             size_t cfg_size);
MOQ_API void moq_media_sender_cfg_init_live_sized(moq_media_sender_cfg_t *cfg,
                                                  size_t cfg_size);
MOQ_API void moq_media_sender_cfg_init_lossless_sized(
    moq_media_sender_cfg_t *cfg, size_t cfg_size);

/* -- Track configuration (§7.1) --------------------------------------- *
 * Carries enough to derive the track's MSF catalog entry and to package
 * its media. The service builds and publishes the catalog; apps
 * never hand-build catalog JSON. */

typedef struct moq_media_track_cfg {
    uint32_t struct_size;
    moq_bytes_t name;                  /* REQUIRED: catalog track name */
    moq_media_type_t      media_type;  /* REQUIRED: VIDEO | AUDIO */
    moq_media_packaging_t packaging;   /* REQUIRED: RAW | CMAF | MPEG2TS */
    moq_bytes_t codec;                 /* codec string, e.g. "avc1.64001f".
                                          MSF-01 5.2.18: REQUIRED for audio and
                                          video tracks (add_track returns
                                          MOQ_ERR_INVAL if empty), except on an
                                          MPEG2TS track: a multiplex has no
                                          inherent codec, so there it is
                                          optional. */
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
     * groups. The service records ONE record per media group, at
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

    /* Publisher scheduling preference (0 is highest, 255 lowest). Subscriber
     * priority takes precedence; cross-stream arrival order is not promised.
     * has_publisher_priority=false keeps the default 128, regardless of the
     * value field. Generated timeline tracks always keep that default.
     * Read only when struct_size covers the complete presence/value pair. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    bool        has_publisher_priority;
    uint8_t     publisher_priority;

    /* Packaging-extension records for this track's catalog entry, one per
     * kind (see moq_msf_track_ext_t). An MPEG2TS track carries exactly one
     * MOQ_MSF_EXT_MPEG2TS record (draft-gregoire-moq-msfts: mode, packet
     * size, program number, ...); read it back with
     * moq_msf_ext_mpeg2ts(extensions, extension_count). Borrowed for the
     * call: add_track deep-copies the entry array, each record (up to its own
     * struct_size), its spans and its si_pids. add_track returns
     * MOQ_ERR_INVAL for a count without an array, an unknown or NONE kind, a
     * kind listed twice, a NULL record, a record below its first published
     * size, a kind that does not belong to the track's packaging, an MPEG2TS
     * track without its record, or a record that fails
     * moq_msf_track_mpeg2ts_check(). Empty (the default) for other
     * packagings.
     *
     * Appended after publisher_priority, aligned as uint64_t (the ABI's own
     * alignment for it, as on the priority pair), so the block starts at or
     * past the struct size of every older layout. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    const moq_msf_track_ext_t *extensions;
    size_t                     extension_count;

    /* MPEG2TS only. false (the default): the app hands the service TS bytes
     * with moq_media_sender_write_mpeg2ts(), and the service forms Objects
     * and Groups with the <moq/mpeg2ts.h> packetizer (and, for a per-program
     * record with a program number, the program filter ahead of it). true:
     * the app forms Objects itself and submits each with
     * moq_media_sender_write(), which then checks every Object (whole
     * source packets with sync bytes, and a random access point at each
     * Group start when the record declares mpeg2tsRandomAccess). Each
     * track takes one of the two calls; the other returns
     * MOQ_ERR_WRONG_STATE. */
    bool        mpeg2ts_app_objects;
    /* MPEG2TS byte-stream mode only (ignored with mpeg2ts_app_objects).
     * Source packets per Object at most, 0 = 64; the shortest Group in PCR
     * time, 0 = a new Group at every random access point; and whether to
     * forward the packets before the first random access point (false, the
     * default, discards them so every Group starts at one -- which is also
     * what makes the service declare mpeg2tsRandomAccess). */
    uint32_t    mpeg2ts_object_packets;
    uint32_t    mpeg2ts_min_group_ms;
    bool        mpeg2ts_forward_before_random_access;

    /* The track's Track Properties (draft-22 §10), stated on every answer
     * the sender gives for the track (SUBSCRIBE_OK, TRACK_STATUS_OK, the
     * PUBLISH of a publish_tracks sender). Opaque and in the negotiated
     * draft's encoding; a property whose type and value are both below 64
     * encodes identically on drafts 16, 18 and 22. add_track checks them,
     * with DYNAMIC_GROUPS added when dynamic_groups is set, before it
     * registers the track, as moq_track_properties_validate() does: against
     * the negotiated draft once known, before that against every draft the
     * endpoint offers. A set that fails returns MOQ_ERR_INVAL (MOQ_ERR_NOMEM
     * when the check cannot allocate) and changes nothing; otherwise
     * add_track copies it. Empty states none. Aligned past the MPEG2TS block
     * on every target, so a caller's struct_size that ends at or before that
     * block never covers it. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    moq_bytes_t track_properties;
    /* Advertise DYNAMIC_GROUPS = 1 (draft-22 §10.6), added to
     * track_properties: subscribers may then ask the sender to start a new
     * group, which moq_media_sender_take_new_group_request() reports. */
    bool        dynamic_groups;

    /* Preserve the tail padding of the layout through dynamic_groups. This
     * storage is reserved and is never read. */
    uint8_t     _reserved_dynamic_groups[7];

    /* MSF-01 section 5.2.11 renderGroup: tracks sharing a value are intended
     * to render together. Independent of alt_group; emitted verbatim without
     * inferring groups or synchronizing tracks. Every int value is accepted.
     * Appended after the complete layout through dynamic_groups. Read only
     * when struct_size covers the whole presence/value pair; false means
     * absent and the value is ignored. */
    bool        has_render_group;
    int         render_group;

    /* MSF-01 section 5.2.10 label: human-readable track-selection text.
     * Borrowed for the call; add_track deep-copies the bytes. Read only when
     * struct_size covers this entire presence/span block. With has_label
     * false the span is ignored. With it true, an empty span emits "", and
     * nonempty text must have a non-NULL pointer and well-formed UTF-8 or
     * add_track returns MOQ_ERR_INVAL. Embedded NUL bytes are preserved. */
    bool        has_label;
    moq_bytes_t label;

    /* The track's group order (MOQT 7.1), ASCENDING or DESCENDING,
     * advertised as DEFAULT_PUBLISHER_GROUP_ORDER and added to
     * track_properties as dynamic_groups is; a subscriber's own order takes
     * precedence. DEFAULT (the default) advertises none. Other values:
     * add_track fails MOQ_ERR_INVAL. The catalog and timeline tracks always
     * keep DEFAULT. Aligned past label on every target, so a caller's
     * struct_size that ends there never covers it. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    moq_group_order_t group_order;
} moq_media_track_cfg_t;

#define MOQ_MEDIA_TRACK_CFG_V0_SIZE \
    offsetof(moq_media_track_cfg_t, has_track_duration)

/* The exported pointer-only symbol clears and stamps the complete historical
 * layout through alt_group. Source calls use the caller-sized macro below.
 * Initializers set is_live when its complete field fits. */
MOQ_API void moq_media_track_cfg_init(moq_media_track_cfg_t *cfg);
/* Clears and stamps min(cfg_size, library size); leaves future trailing storage
 * untouched. NULL or storage too small for struct_size is a no-op. */
MOQ_API void moq_media_track_cfg_init_sized(moq_media_track_cfg_t *cfg,
                                          size_t cfg_size);
#define moq_media_track_cfg_init(cfg) \
    moq_media_track_cfg_init_sized((cfg), sizeof(*(cfg)))

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
 * channel_config (5.2.29). A track missing a required field is MOQ_ERR_INVAL.
 *
 * MPEG2TS tracks (draft-gregoire-moq-msfts). The catalog entry is
 * packaging "mpeg2ts", mimeType "video/mp2t", the record's mpeg2ts* fields,
 * and init_data (when given: whole source packets of the record's packet
 * size, normally the PAT and PMT) as an "inline" initDataList entry
 * referenced by initRef. bitrate stays required; codec, samplerate and
 * channel_config are not (they describe one elementary stream, not a
 * multiplex). In byte-stream mode the service declares mpeg2tsRandomAccess
 * itself -- true unless mpeg2ts_forward_before_random_access, absent for an
 * unmodified-multiplex record without a program number -- and a record value
 * that contradicts it is MOQ_ERR_INVAL; in app-Object mode the record's value
 * is used as given. MOQ_ERR_INVAL also for an es-packets record (authoring
 * es-packets tracks is helper-level), a per-program record without a program
 * number (the service runs the program filter for it), emit_sap_timeline or
 * emit_media_timeline (TS keeps presentation times inside the packets), and
 * init_data that is not whole source packets. A build without moq::mpeg2ts
 * returns MOQ_ERR_UNSUPPORTED for an MPEG2TS track. */
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
    /* Legacy RAW/LOC timing. Every legacy RAW/LOC object carries one timestamp:
     *   drafts 16 and 18: the LOC-01 Capture Timestamp (ID 0x02, LOC-01
     *     2.3.1.1), wall-clock microseconds since the Unix epoch;
     * The value is capture_time_us when has_capture_time is true (emitted
     * verbatim, no clamp/scale); otherwise it falls back to
     * presentation_time_us. presentation_time_us also drives the generated
     * SAP/media timelines. These fields are RAW/LOC-only -- for CMAF the
     * object's timing and properties live in the passed-through `properties`
     * block and these are ignored. moq_media_sender_write() rejects, with
     * MOQ_ERR_INVAL (synchronously, without taking payload ownership), a
     * RAW/LOC timestamp above what the session's draft can encode
     * (moq_loc_value_max_for_version): MOQ_QUIC_VARINT_MAX on draft 16 and
     * before the draft is known; none on draft 18 (vi64 carries uint64).
     * Explicit D22 RAW uses the metadata tail instead, not a capture/PTS
     * fallback; an AUTO sender on draft 22 generates LOC-04 from these fields
     * when the tail is absent (capture time as Unix microseconds, else
     * presentation time with Timescale 1000000; see
     * moq_media_sender_cfg_t.companion_profile). presentation_time_us still
     * drives generated timelines. */
    uint64_t presentation_time_us;
    bool has_capture_time;       /* true: emit capture_time_us as the LOC
                                    timestamp property (an AUTO sender that
                                    may send draft 22 refuses it on a track
                                    with a Track Properties Timescale; see
                                    companion_profile) */
    uint64_t capture_time_us;    /* legacy wall-clock us since epoch */
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
    /* Beyond the COMPLETE old extent, including short-enum/ARM padding.
     * D22 RAW requires a borrowed, full-sized metadata record. loc is the
     * authoritative wire input; derived time/independence observations are
     * not serialized. Timestamp is required; zero scale or conversion overflow
     * is INVAL. Scale presence means application time, including scale1e6;
     * absence means UNIX microseconds. Config spans/marking are copied into
     * owned generated properties before transfer and charged to the queue.
     * No caller metadata span is retained. Legacy RAW and CMAF ignore this
     * tail. Before negotiated D22 is observed, explicit D22 writes are
     * UNSUPPORTED; an AUTO sender's handling is described at
     * moq_media_sender_cfg_t.companion_profile. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    const moq_media_metadata_t *metadata;
} moq_media_send_object_t;

/* Complete frozen pre-metadata input extent; old-sized writes remain legal. */
#define MOQ_MEDIA_SEND_OBJECT_V0_SIZE offsetof(moq_media_send_object_t, metadata)

/* Submit one media object in decode order. On an MPEG2TS track this is the
 * app-Object mode (mpeg2ts_app_objects): the payload must be whole source
 * packets with their sync bytes, and with mpeg2tsRandomAccess declared a
 * starts_group Object must hold a random access point; a failing Object is
 * MOQ_ERR_PROTO with no ownership taken. On a byte-stream MPEG2TS track it
 * returns MOQ_ERR_WRONG_STATE.
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
 * ownership taken. A Group that loses queued media to a drop policy is
 * closed for writes: a non-sync object that would continue it returns
 * MOQ_ERR_WOULD_BLOCK, and the next sync point opens a new Group, so no
 * Location is ever sent twice. (A Group that loses its head to
 * drop_without_demand stays writable and the rest of it is discarded; see
 * that field.) An object that continues its track's open Group never evicts
 * that Group to make room for itself.
 *
 * Terminal: MOQ_ERR_CLOSED once the endpoint/sender is terminal,
 * MOQ_ERR_INTERRUPTED while the endpoint interrupt latch is set; the
 * caller retains ownership in both cases. MOQ_ERR_INVAL for NULL/foreign
 * track or for non-NULL `properties` on a RAW track (§7.4 -- the service
 * owns the RAW/LOC property block in v0). Explicit D22 RAW also requires valid
 * complete metadata (AUTO generates it from the typed fields when absent,
 * and refuses with INVAL a capture time it cannot send as Unix time because
 * the track's properties carry a Timescale; see companion_profile);
 * unusable timing or invalid LOC input is INVAL, generated-property
 * allocation failure is NOMEM. A companion/negotiated-version mismatch, or an
 * AUTO sender blocked on draft 22 by a CMAF track without init_data, is
 * UNSUPPORTED without transfer or endpoint/session termination. An object
 * that would open a group after Group ID 2^62 - 1 (the library's write-path
 * bound) is MOQ_ERR_LIMIT, without transfer; the track's open group still
 * accepts objects. */
MOQ_API moq_result_t moq_media_sender_write(
    moq_media_sender_t *s, moq_media_track_t *track,
    const moq_media_send_object_t *obj);

/* -- MPEG-2 TS byte stream (draft-gregoire-moq-msfts) ----------------- */

/* One write_mpeg2ts() input. A sized input: the library reads it only when
 * struct_size covers end_object. */
typedef struct moq_media_send_mpeg2ts {
    uint32_t    struct_size;
    moq_bytes_t data;        /* source packets of the track's packet size, in
                                any chunking; borrowed for the call */
    bool        end_object;  /* after data, end the Object being filled so
                                this call's last packet closes an Object */
} moq_media_send_mpeg2ts_t;

/* Hand TS bytes to a byte-stream MPEG2TS track. The service runs the
 * program filter (per-program record with a program number) and the
 * packetizer: Groups open at random access points (with the PAT/PMT run
 * that precedes them), Objects hold at most mpeg2ts_object_packets source
 * packets, and every Object goes through the same queue, backpressure
 * policy and drain as write(). PTS, DTS and PCR stay inside the packets.
 *
 * *consumed (required) is always written, on success and on every error:
 * the number of bytes of in->data the service now owns. A packet is taken
 * whole or not at all; bytes that do not complete a packet are kept (at
 * most packet_size - 1 of them) and count as consumed.
 *
 * Writes can be short, as with POSIX write(). When an Object cannot be
 * queued under the backpressure policy (RETURN_WOULD_BLOCK, or a
 * BLOCK_TIMEOUT expiry) it is held by the track (at most two are, the
 * per-track stat mpeg2ts_objects_parked) and the call stops after the packet
 * that completed it. Held Objects are queued first by the next
 * write_mpeg2ts() or end_track(); a caller with no more input passes empty
 * data until mpeg2ts_objects_parked is 0.
 *   MOQ_OK                the call made progress: it took *consumed bytes
 *                         (less than in->data.len for a short write, which
 *                         may also be the whole input with Objects held),
 *                         or queued held Objects, or ended an Object.
 *   MOQ_ERR_WOULD_BLOCK   no progress: the first held Object still cannot be
 *                         queued, *consumed is 0 and nothing changed. Retry
 *                         after moq_media_sender_wait().
 *   MOQ_ERR_NOMEM         an Object could not be copied: it is lost, and so
 *                         is the rest of its Group. Later Objects of that
 *                         Group are discarded and counted as below. When it
 *                         was not the Group's first Object, what is still
 *                         queued of the Group is dropped (objects_dropped)
 *                         and its subgroup, if already open on the wire, is
 *                         reset (groups_abandoned), so the truncated Group
 *                         is never finished as complete. A lost first Object
 *                         takes its Group ID with it, so the next Group
 *                         follows a hole a subscriber can see. *consumed
 *                         covers the packet that completed it.
 *   MOQ_ERR_PROTO         a packet lacks its sync byte (offset 0, or 4 for
 *                         192-octet packets): *consumed stops before that
 *                         packet's bytes in this call and nothing else
 *                         changes. The service does not resync. Bytes of an
 *                         unfinished packet kept by an earlier call are
 *                         discarded with it and counted
 *                         (mpeg2ts_partial_bytes_discarded): the next call
 *                         starts at a packet boundary.
 *   MOQ_ERR_WRONG_STATE   an app-Object track, another write_mpeg2ts() or
 *                         end_track() in progress on the same track (one
 *                         writer per track; writes to different tracks stay
 *                         concurrent), or a track that is ended, removed or
 *                         converted to VOD.
 *   MOQ_ERR_INVAL         NULL s/track/in/consumed, a foreign track, a track
 *                         that is not MPEG2TS, in->struct_size below its
 *                         first published size, or data.len > 0 with a NULL
 *                         data pointer.
 *   MOQ_ERR_CLOSED / MOQ_ERR_INTERRUPTED  as for write(). An interrupt
 *                         that stops a call after it took packets reports
 *                         them in *consumed, and the Object that could not
 *                         be queued stays held, as above, for the next call
 *                         once the latch clears.
 *   MOQ_ERR_UNSUPPORTED   a build without moq::mpeg2ts, or the companion
 *                         refusal write() applies (a companion/negotiated-
 *                         version mismatch, an explicit D22 companion before
 *                         draft 22 is observed, or an AUTO sender blocked on
 *                         draft 22; also stats.last_error). Checked before
 *                         any byte is taken: *consumed is 0 and no kept,
 *                         held or queued state changes. Should the session
 *                         latch such a draft while the call has released the
 *                         sender between Objects, the call stops there and
 *                         *consumed reports the bytes already taken.
 *
 * Under a drop policy, when an open Group has lost queued media, the
 * packetizer's later Objects of that Group are discarded and counted
 * (mpeg2ts_continuation_objects_discarded) until its next Group starts. */
MOQ_API moq_result_t moq_media_sender_write_mpeg2ts(
    moq_media_sender_t *s, moq_media_track_t *track,
    const moq_media_send_mpeg2ts_t *in, size_t *consumed);

/* End a finite track: after this track's already-queued objects drain, the
 * service emits a reliable END_OF_TRACK status object (a subgroup-stream object,
 * NOT a datagram), which surfaces on receivers as MOQ_MEDIA_TRACK_ENDED. A
 * published track (publish_tracks) carries it on its publication before the
 * publication's terminal done, so a relay forwards and retains the end. Two
 * cases end by control message alone, with no status object: a destination
 * that is not receiving the track (Forward 0, or a window that excludes the
 * terminal Location), and a track whose last group is at the group-id ceiling
 * (see moq_pub_end_track). Ending one track does NOT end other tracks or
 * close the session/endpoint.
 *
 * After end_track, moq_media_sender_write() for this track returns
 * MOQ_ERR_WRONG_STATE. end_track is IDEMPOTENT: a second call returns MOQ_OK and
 * does nothing. Returns MOQ_OK (queued), MOQ_ERR_INVAL (NULL/foreign track),
 * MOQ_ERR_CLOSED (terminal), MOQ_ERR_INTERRUPTED (latch set), or
 * MOQ_ERR_WOULD_BLOCK when the send queue is momentarily full (retry).
 *
 * On a byte-stream MPEG2TS track, end_track first queues the Objects
 * write_mpeg2ts() held, then the packetizer's last Objects (the end of the
 * source), and only then the END_OF_TRACK; a MOQ_ERR_WOULD_BLOCK on the way
 * is retried by calling end_track again. MOQ_ERR_NOMEM means one of those
 * last Objects could not be copied and is lost (the rest of its Group is
 * discarded); the track is not ended yet, and calling end_track again queues
 * the END_OF_TRACK. Bytes of an incomplete packet are discarded and counted
 * (mpeg2ts_partial_bytes_discarded). On such a track end_track belongs to
 * the track's writer thread, the one that calls write_mpeg2ts(): from any
 * other thread, while a write_mpeg2ts() on the same track is in progress, it
 * returns MOQ_ERR_WRONG_STATE, the same code as for a removed or converted
 * track, so the two cannot be told apart there. */
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
 * The completion is asynchronous: complete() freezes new input and returns,
 * and the network thread then sends the accepted media, finishes a byte-stream
 * MPEG2TS track's source, ends each track after its own media and each
 * generated timeline after its records, and only then publishes the terminal
 * catalog. Media retained for a track nobody has subscribed to, or a
 * timeline's records kept for a timeline nobody has subscribed to, hold the
 * completion until a subscriber takes them. An Object complete() owed that is
 * lost (a TS track's last Objects that could not be copied) fails it: no
 * further completion end and no terminal catalog follow, and the failure is
 * recorded, sticky, in stats.completion_error (and, until a later failure,
 * stats.last_error). Output the sender had already accepted and
 * that is still sendable -- queued media with demand, a generated timeline's
 * records for its subscriber, an end_track marker -- is still sent and still
 * owed to moq_endpoint_drain(). A successful drain says that owed output was
 * handed over; it is NOT proof that the broadcast completed: a held
 * completion owes nothing more yet, and a failed one never will.
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

/* -- Stats (§7.5: every drop is counted, never silent) ---------------- *
 * FOUR of the counters below -- objects_written, objects_sent, objects_queued,
 * objects_dropped -- plus bytes_queued form ONE population: MEDIA objects
 * accepted by write(), and the bytes they hold. The remaining counters
 * (groups_dropped, keyframes_dropped, groups_abandoned, backpressure_stalls,
 * last_error, sap_records_evicted) describe other facts and are not part of it.
 *
 * Private lifecycle work the service queues on your behalf -- the END_OF_TRACK
 * terminal marker end_track() queues -- is NOT media and appears in none of the
 * five. In particular objects_queued == 0 means NO MEDIA IS PENDING; it does
 * not mean every queued lifecycle item has completed, so a terminal may still
 * be in flight. Within that population conservation holds unconditionally:
 *   objects_written == objects_sent + objects_queued + objects_dropped */

typedef struct moq_media_sender_stats {
    uint32_t struct_size;
    uint64_t objects_written;     /* accepted by write() (queued) */
    uint64_t objects_sent;        /* emitted to the session */
    uint64_t objects_queued;      /* media accepted by write() and still
                                     queued; excludes the private terminal
                                     marker (see the note above) */
    uint64_t bytes_queued;        /* payload+properties bytes queued */
    uint64_t objects_dropped;     /* queued media discarded: by a drop policy,
                                     or by explicit lifecycle removal --
                                     remove_track() / complete() */
    uint64_t groups_dropped;      /* distinct (track, group) values from which
                                     at least ONE queued media object was
                                     discarded: several objects of one group
                                     count once, two groups twice */
    uint64_t keyframes_dropped;
    uint64_t groups_abandoned;    /* open wire subgroups actually driven through
                                     the RESET lifecycle. A partially emitted
                                     group can contribute to BOTH this and
                                     groups_dropped -- they are two different
                                     facts about it */
    uint64_t backpressure_stalls; /* BLOCK_TIMEOUT waits entered */
    moq_result_t last_error;      /* the most recent failure the sender
                                     recorded (0 = none): a non-OK write() or
                                     write_mpeg2ts() return, an unusable draft
                                     for the companion (UNSUPPORTED), or the
                                     failure that stopped a graceful
                                     completion (moq_media_sender_complete).
                                     A later one overwrites it; the
                                     completion's own failure is kept in
                                     completion_error */
    /* Generated SAP event-timeline records aged out of the bounded history
     * (see moq_media_track_cfg_t.sap_timeline_history_groups). Appended after
     * last_error so the frozen v0 stats size is unchanged. */
    uint64_t sap_records_evicted;
    /* The first failure that stopped a graceful completion
     * (moq_media_sender_complete) before its terminal catalog was committed
     * -- an Object it owed that was lost, an end, or the terminal catalog
     * that could not be built, written or retained -- exactly as it
     * occurred, and sticky:
     * nothing later -- a repeated complete(), a refused operation, a later
     * last_error -- changes it. Failure before terminal publication prevents
     * that publication; a later retention failure cannot retract a live
     * catalog already accepted by the publisher. Successful completion
     * requires both steps. MOQ_OK (0) means no such failure was recorded; it does
     * NOT mean a completion succeeded, or that complete() was called. A
     * completion waiting for room or for a subscriber has not failed, and a
     * failure after the terminal catalog committed is not the completion's.
     * Appended after sap_records_evicted. */
    moq_result_t completion_error;
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

/* -- Per-track stats --------------------------------------------------- *
 * The generic counters are the per-track slice of the sender-wide stats
 * above, with the same meanings: summed over every track they equal the
 * sender-wide values, and objects_written == objects_sent + objects_queued +
 * objects_dropped holds per track. The mpeg2ts_ counters follow, zero for a
 * track of any other packaging; the packetizer ones (groups through
 * source_pcr_discontinuities) are counted only in byte-stream mode. */
typedef struct moq_media_sender_track_stats {
    uint32_t struct_size;
    uint64_t objects_written;
    uint64_t objects_sent;
    uint64_t objects_queued;
    uint64_t objects_dropped;
    uint64_t bytes_queued;
    uint64_t groups_dropped;
    uint64_t keyframes_dropped;
    uint64_t groups_abandoned;
    /* MPEG2TS. */
    uint64_t mpeg2ts_groups;              /* Groups the packetizer opened */
    uint64_t mpeg2ts_objects;             /* Objects the packetizer emitted */
    uint64_t mpeg2ts_packets_in;          /* source packets it was given */
    uint64_t mpeg2ts_packets_dropped_before_random_access;
    uint64_t mpeg2ts_max_group_27mhz;     /* longest Group, PCR time */
    uint64_t mpeg2ts_groups_over_2s;      /* Groups longer than 2 s */
    uint64_t mpeg2ts_peak_rate_bps;       /* peak rate between PCRs */
    uint64_t mpeg2ts_source_pcr_discontinuities;
    /* The record declares mpeg2tsMuxRate below mpeg2ts_peak_rate_bps (MSF
     * does not let a published track's fields change, so the service only
     * reports it). */
    bool     mpeg2ts_mux_rate_below_peak;
    /* The carried program left the PAT (per-program): MSFTS says the
     * publisher SHOULD then end the track, which is left to the app. */
    bool     mpeg2ts_program_gone;
    uint64_t mpeg2ts_objects_parked;      /* Objects held now (0..2) */
    uint64_t mpeg2ts_partial_bytes_discarded;
    uint64_t mpeg2ts_continuation_objects_discarded;
} moq_media_sender_track_stats_t;

/* Snapshot one track's counters. Pass `out_size = sizeof(*out)`; same size
 * contract as moq_media_sender_get_stats() (requires the first published
 * size, writes min(out_size, current), stamps out->struct_size with the
 * bytes written). MOQ_ERR_INVAL for NULL arguments, out_size below the first
 * published size, or a track that is not one of this sender's media tracks.
 * Callable from any thread. */
MOQ_API moq_result_t moq_media_sender_get_track_stats(
    const moq_media_sender_t *s, const moq_media_track_t *track,
    moq_media_sender_track_stats_t *out, size_t out_size);

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

/* is_ready (§7.2): the catalog is published AND the peer acknowledged the
 * announcement -- the namespace accepted (pull), or, in push mode
 * (publish_tracks), the catalog track's PUBLISH accepted. The publish path
 * is then ready to accept/send media. It does NOT
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
 * MOQ_ERR_CLOSED (sender or endpoint terminal), MOQ_ERR_INVAL (NULL),
 * MOQ_ERR_UNSUPPORTED (known companion/negotiated-version mismatch, or an
 * AUTO sender blocked on draft 22; see companion_profile).
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

/* Take the new-group request pending on a track added with dynamic_groups:
 * a subscriber asked for a new group (NEW_GROUP_REQUEST, draft-22 §9.20.19)
 * with a value that warrants one -- 0, or larger than the track's current
 * group. The Original Publisher "SHOULD end the current Group and begin a new
 * Group as soon as practical", so write the next object as the start of a
 * group (a sync/keyframe object with starts_group). MOQ_OK clears the request
 * and stores its value (the largest group the subscriber knows, plus one; 0
 * when it knows none) in *requested_group when that is non-NULL; requests
 * that arrive before it is taken coalesce into the latest. MOQ_DONE when none
 * is pending; MOQ_ERR_INVAL for a NULL sender or a NULL/foreign track. Safe
 * from any thread, like the queries above. */
MOQ_API moq_result_t moq_media_sender_take_new_group_request(
    moq_media_sender_t *s, moq_media_track_t *track, uint64_t *requested_group);

/* True if ANY app-visible media track has a subscriber -- the "should I encode
 * media?" helper. Counts user media tracks only: the internal catalog track and
 * generated SAP/media-timeline tracks are excluded, as are removed tracks. So a
 * catalog-only subscription (a receiver that fetched the catalog but subscribed
 * to no media) does NOT make this true. */
MOQ_API bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s);

/* is_closed: clean close / drained. is_fatal: connect, certificate,
 * protocol, transport, or sender failure (catalog encode, setup, namespace
 * rejected, cancelled or terminated, PUBLISH rejected). fatal_code: the sender's own code
 * when sender-fatal, otherwise the endpoint's. */
MOQ_API bool     moq_media_sender_is_closed(const moq_media_sender_t *s);
MOQ_API bool     moq_media_sender_is_fatal(const moq_media_sender_t *s);
MOQ_API uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s);

/* Sender-fatal codes (moq_media_sender_fatal_code). */
#define MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE   0x1u /* MSF encode failed */
#define MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED     0x2u /* publisher / track /
                                                        retained setup failed */
#define MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED 0x3u /* peer refused the
                                                        namespace */
#define MOQ_MEDIA_SENDER_FATAL_NAMESPACE_CANCELLED 0x4u /* peer withdrew a
                                                        previously accepted
                                                        namespace */
#define MOQ_MEDIA_SENDER_FATAL_PUBLISH_REJECTED   0x5u /* peer rejected a
                                                        catalog or media-track
                                                        PUBLISH */
/* D22 namespace operation ended without a peer rejection/cancellation.
 * Sender-local: does not close its shared endpoint or another sender. */
#define MOQ_MEDIA_SENDER_FATAL_NAMESPACE_TERMINATED 0x6u

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_SENDER_H */
