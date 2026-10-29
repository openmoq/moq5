#ifndef MOQ_MSF_H
#define MOQ_MSF_H

/*
 * MSF (MOQT Streaming Format) catalog parser and encoder.
 *
 * Converts between MSF catalog JSON documents and typed C structs.
 * No network, no sessions.
 *
 * Link against moq::msf (depends on moq::core; when built with
 * moq::media-object and moq::cmaf, also provides the
 * moq_msf_track_to_media_info_sized() helper via <moq/msf_media.h>).
 *
 * Lifetime: string fields in the parsed catalog borrow from the
 * internal JSON DOM, which itself is allocated via the caller's
 * moq_alloc_t. All borrows are valid until moq_msf_catalog_cleanup()
 * is called. The original JSON input need not be kept alive after
 * moq_msf_catalog_parse() returns.
 *
 * @see draft-ietf-moq-msf-01
 */

#include <moq/types.h>
#include <moq/rcbuf.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_MSF_VERSION 1

/* How the root "version" (MSF-01 5.1.1) is spelled on the wire. Fixed-width
 * for ABI stability. Values are never renumbered.
 *   NUMERAL     (0) the JSON String "1": a compatibility form, not the
 *               MSF-01 draft convention -- the form this encoder has always
 *               emitted and that existing receivers accept. Zero, so an
 *               absent or zeroed field selects it.
 *   DRAFT_NAME  (1) the JSON String "draft-01": 5.1.1's convention for
 *               catalogs against an Internet-Draft release.
 * The parser accepts both, and for compatibility the MSF-00 numeric 1
 * (reported as NUMERAL), and reports which one it read; a reader of a parsed
 * catalog must tolerate forms it does not know. The encoder accepts only the forms defined here and
 * rejects any other value with MOQ_ERR_INVAL. */
typedef uint32_t moq_msf_version_form_t;
#define MOQ_MSF_VERSION_FORM_NUMERAL    UINT32_C(0)
#define MOQ_MSF_VERSION_FORM_DRAFT_NAME UINT32_C(1)

#define MOQ_MSF_CATALOG_TRACK_NAME     "catalog"
#define MOQ_MSF_CATALOG_TRACK_NAME_LEN 7

/*
 * MSF-01 media timeline template (§5.2.15 / §7.4): a fixed-duration-segment
 * media timeline expressed inline on a media track, as the JSON array
 *   [startMediaTime, deltaMediaTime, [startGroup,startObject],
 *    [deltaGroup,deltaObject], startWallclock, deltaWallclock]
 * All six values are non-negative integers: media/wallclock times are
 * milliseconds and the Location pairs are MOQT Group/Object IDs (and deltas).
 * The entry n is computed as mediaTime[n]=start+(n*delta), location[n]=
 * [startGroup+(n*deltaGroup), startObject+(n*deltaObject)], likewise wallclock.
 */
typedef struct moq_msf_media_template {
    uint64_t start_media_ms;      /* element 0: startMediaTime (ms) */
    uint64_t delta_media_ms;      /* element 1: deltaMediaTime (ms) */
    uint64_t start_group;         /* element 2: startLocation Group ID */
    uint64_t start_object;        /* element 2: startLocation Object ID */
    uint64_t delta_group;         /* element 3: deltaLocation Group delta */
    uint64_t delta_object;        /* element 3: deltaLocation Object delta */
    uint64_t start_wallclock_ms;  /* element 4: startWallclock (ms; 0 for VOD) */
    uint64_t delta_wallclock_ms;  /* element 5: deltaWallclock (ms; 0 for VOD) */
} moq_msf_media_template_t;

/*
 * MSF-01 explicit media-timeline payload record (§7.1.1). The payload of a
 * "mediatimeline" track object is a JSON array of these records, each itself a
 * 3-item array [mediaTime, [group, object], wallclock]:
 *   - media_time_ms: media presentation timestamp of the entry's first sample,
 *     floored to integral milliseconds.
 *   - group / object: the entry's MOQT Location (Group ID, Object ID).
 *   - wallclock_ms: encode wallclock, ms since the Unix epoch; 0 for VOD or
 *     when unknown.
 * All four are non-negative integers on the wire. This codec models ONLY the
 * explicit array form; the §7.4 template form and combined documents are not
 * handled here.
 */
typedef struct moq_msf_media_timeline_record {
    uint64_t media_time_ms;
    uint64_t group;
    uint64_t object;
    uint64_t wallclock_ms;   /* 0 = VOD/unknown */
} moq_msf_media_timeline_record_t;

/*
 * MPEG-2 Transport Stream track fields (draft-gregoire-moq-msfts, Catalog):
 * the "mpeg2ts*" keys of a track whose packaging is "mpeg2ts". They form one
 * record, moq_msf_mpeg2ts_t, carried in the track's extension list under
 * MOQ_MSF_EXT_MPEG2TS (see moq_msf_track_ext_t). Each key has its own has_
 * flag.
 *
 * The parser records every key it finds, on any track, without judging it
 * (Catalog: a parser MUST ignore fields it does not understand, so one bad
 * mpeg2ts track never fails the catalog). A key whose value has the wrong
 * JSON type, or a number that is negative, not integral, or too large for
 * its field, is recorded as present with its bit set in `malformed`. Any
 * JSON spelling of an integral value is accepted (188, 188.0, 1.88e2).
 * moq_msf_track_mpeg2ts_check() is the subscriber's verdict on the track;
 * the encoder applies the same rules strictly.
 */
typedef enum moq_msf_mpeg2ts_mode {
    MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED         = 0,
    MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM   = 1, /* "unmodified-program" */
    MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX = 2, /* "unmodified-multiplex" */
    MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM          = 3, /* "per-program" */
    MOQ_MSF_MPEG2TS_MODE_ES_PACKETS           = 4, /* "es-packets" */
} moq_msf_mpeg2ts_mode_t;

typedef enum moq_msf_mpeg2ts_timestamp_mode {
    MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED = 0,
    MOQ_MSF_MPEG2TS_TIMESTAMP_ARRIVAL_TIME = 1,   /* "arrival-time" */
    MOQ_MSF_MPEG2TS_TIMESTAMP_OPAQUE       = 2,   /* "opaque" */
} moq_msf_mpeg2ts_timestamp_mode_t;

/* Bits of moq_msf_mpeg2ts_t.malformed, one per key. */
#define MOQ_MSF_MPEG2TS_F_MODE           (1u << 0)
#define MOQ_MSF_MPEG2TS_F_PACKET_SIZE    (1u << 1)
#define MOQ_MSF_MPEG2TS_F_ES_PID         (1u << 2)
#define MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER (1u << 3)
#define MOQ_MSF_MPEG2TS_F_PCR_PID        (1u << 4)
#define MOQ_MSF_MPEG2TS_F_MUX_RATE       (1u << 5)
#define MOQ_MSF_MPEG2TS_F_SI_PIDS        (1u << 6)
#define MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS  (1u << 7)
#define MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE (1u << 8)
#define MOQ_MSF_MPEG2TS_F_SCTE35_PID     (1u << 9)

typedef struct moq_msf_mpeg2ts {
    /* Sized record: moq_msf_mpeg2ts_init() stamps it. A reader refuses a
     * record smaller than MOQ_MSF_MPEG2TS_V0_SIZE and reads a later field
     * only when struct_size covers it. */
    uint32_t               struct_size;

    /* mpeg2tsMode. On encode the enum is authoritative; mode_name is what a
     * parse found, and is re-emitted only for an unrecognized value on the
     * receive-path re-encode. */
    bool                   has_mode;
    moq_msf_mpeg2ts_mode_t mode;
    moq_bytes_t            mode_name;          /* BORROWED */

    bool        has_packet_size;     /* mpeg2tsPacketSize: 188 or 192 */
    uint32_t    packet_size;
    bool        has_es_pid;          /* mpeg2tsEsPid */
    uint32_t    es_pid;
    bool        has_program_number;  /* mpeg2tsProgramNumber */
    uint32_t    program_number;
    bool        has_pcr_pid;         /* mpeg2tsPcrPid */
    uint32_t    pcr_pid;
    bool        has_mux_rate;        /* mpeg2tsMuxRate, bit/s */
    uint64_t    mux_rate;
    /* mpeg2tsSiPids. A parsed array belongs to the catalog and is freed by
     * cleanup with its record; on encode it is borrowed for the call. A
     * malformed element is stored as 0 with the bit set. */
    bool        has_si_pids;
    uint32_t   *si_pids;
    size_t      si_pid_count;
    bool        has_random_access;   /* mpeg2tsRandomAccess */
    bool        random_access;
    bool                             has_timestamp_mode; /* mpeg2tsTimestampMode */
    moq_msf_mpeg2ts_timestamp_mode_t timestamp_mode;
    moq_bytes_t                      timestamp_mode_name; /* BORROWED */
    bool        has_scte35_pid;      /* mpeg2tsScte35Pid */
    uint32_t    scte35_pid;

    uint32_t    malformed;           /* MOQ_MSF_MPEG2TS_F_* */
} moq_msf_mpeg2ts_t;

/* The first published size: the layout through `malformed`. */
#define MOQ_MSF_MPEG2TS_V0_SIZE \
    (offsetof(moq_msf_mpeg2ts_t, malformed) + sizeof(uint32_t))

/* Zero and stamp min(m_size, this build's size); nothing is written when
 * m_size cannot hold struct_size. The macro passes the caller's sizeof. */
MOQ_API void moq_msf_mpeg2ts_init_sized(moq_msf_mpeg2ts_t *m, size_t m_size);
#define moq_msf_mpeg2ts_init(m) moq_msf_mpeg2ts_init_sized((m), sizeof(*(m)))

/* -- Packaging extensions ----------------------------------------------
 * Where a packaging or extension draft's catalog fields go:
 *   - a few independent scalar keys sit flat on moq_msf_track_t as has_X /
 *     X pairs, like CMSF's maxGrpSapStartingType / maxObjSapStartingType;
 *   - a cohesive record -- several keys validated together, with their own
 *     enums, arrays, malformed bits and a _check function -- is a sized
 *     struct of its own (leading struct_size, its own _init and _check)
 *     carried in the track's extension list, at most one entry per kind.
 *
 * Parsed: the list and its records are catalog-owned and freed by cleanup;
 * spans inside a record borrow from the DOM. Encode: the list and the
 * records are borrowed for the call. A clone op inherits the parent's
 * entries and merges, by kind, the ones it carries.
 *
 * The encoder refuses a list with a NULL array, an unknown kind, a kind
 * listed twice, a NULL record, a record whose struct_size is below its first
 * published size, or a kind on a track whose packaging it does not belong
 * to (MOQ_MSF_EXT_MPEG2TS: packaging "mpeg2ts"). The parser records a kind
 * on any track; the kind's own _check judges it. */
/* Extension kind. Fixed-width uint32_t for ABI stability. Values are never
 * renumbered. Unknown kinds must be ignored by a reader (the encoder
 * refuses them). */
typedef uint32_t moq_msf_ext_kind_t;

#define MOQ_MSF_EXT_NONE    0u
#define MOQ_MSF_EXT_MPEG2TS 1u   /* draft-gregoire-moq-msfts: u.mpeg2ts */

typedef struct moq_msf_track_ext {
    moq_msf_ext_kind_t kind;
    union {                                 /* select by kind */
        const moq_msf_mpeg2ts_t *mpeg2ts;
    } u;
} moq_msf_track_ext_t;

/* The entry of `kind` in a list, or NULL. Works on any extension list. */
MOQ_API const moq_msf_track_ext_t *moq_msf_ext_find(
    const moq_msf_track_ext_t *list, size_t count, moq_msf_ext_kind_t kind);

/* Typed lookups, one per kind: NULL when the list carries no such record. */
static inline const moq_msf_mpeg2ts_t *moq_msf_ext_mpeg2ts(
    const moq_msf_track_ext_t *list, size_t count)
{
    const moq_msf_track_ext_t *e =
        moq_msf_ext_find(list, count, MOQ_MSF_EXT_MPEG2TS);
    return e ? e->u.mpeg2ts : NULL;
}

typedef struct moq_msf_track {
    uint32_t    struct_size;

    moq_bytes_t name;          /* required */
    moq_bytes_t packaging;     /* required */
    bool        is_live;       /* required */

    bool        has_namespace;
    moq_bytes_t namespace_;

    bool        has_role;
    moq_bytes_t role;

    bool        has_codec;
    moq_bytes_t codec;

    bool        has_init_data;
    moq_bytes_t init_data;     /* base64-encoded */

    bool        has_init_track;
    moq_bytes_t init_track;

    bool        has_width;
    uint32_t    width;
    bool        has_height;
    uint32_t    height;

    bool        has_samplerate;
    uint32_t    samplerate;
    bool        has_bitrate;
    uint64_t    bitrate;
    bool        has_framerate;
    uint64_t    framerate_millis; /* framerate * 1000 to avoid float */

    bool        has_render_group;
    int         render_group;
    bool        has_alt_group;
    int         alt_group;

    bool        has_timescale;
    uint64_t    timescale;
    bool        has_target_latency;
    uint64_t    target_latency;

    bool        has_channel_config;
    moq_bytes_t channel_config;
    bool        has_lang;
    moq_bytes_t lang;
    bool        has_label;
    moq_bytes_t label;

    /* MSF-01 / CMSF. */
    bool        has_init_ref;     /* initRef -> initDataList[].id (MSF 5.2.13) */
    moq_bytes_t init_ref;
    bool        has_event_type;   /* eventType for eventtimeline tracks (MSF 5.2.5) */
    moq_bytes_t event_type;
    bool        has_mime_type;    /* mimeType (MSF 5.2.19); JSON key "mimeType" */
    moq_bytes_t mime_type;
    /* depends (MSF 5.2.14): track names this track applies to / depends on.
     * JSON key "depends". Borrows from the DOM; the array is freed by cleanup. */
    moq_bytes_t *depends;
    size_t       depends_count;

    /* CMSF content protection / SAP (string fields borrow from the DOM; the
     * cp_ref_ids array is freed by cleanup). */
    moq_bytes_t *cp_ref_ids;      /* contentProtectionRefIDs[] (CMSF 4.1.2) */
    size_t       cp_ref_id_count;
    bool        has_max_grp_sap;  /* maxGrpSapStartingType (CMSF 3.5.2.1) */
    uint32_t    max_grp_sap;
    bool        has_max_obj_sap;  /* maxObjSapStartingType (CMSF 3.5.2.2) */
    uint32_t    max_obj_sap;

    /* MSF-01 deltaUpdate clone operation (5.2.33/5.2.34). parentName/
     * parentNamespace are only meaningful on a track object inside a "clone"
     * op; they are absent on independent/add/remove tracks. */
    bool        has_parent_name;
    moq_bytes_t parent_name;
    bool        has_parent_namespace;
    moq_bytes_t parent_namespace;

    /* Presence of the otherwise-required packaging / isLive keys. True for an
     * independent track or a delta "add" track; on a "clone" track they are set
     * only when the clone explicitly overrides the inherited value. The encoder
     * always emits packaging/isLive for a normal/add track regardless of these
     * (they exist for clone-override fidelity, not to gate normal output). */
    bool        has_packaging;
    bool        has_is_live;

    /* MSF-01 track duration (5.2.35): the track length in integer milliseconds.
     * MUST NOT be present when is_live is true (a VOD/ended track only). */
    bool        has_track_duration;
    uint64_t    track_duration_ms;

    /* MSF-01 media timeline template (5.2.15 / §7.4): an inline timeline on a
     * media track. Set has_template when present; the value is a fully decoded
     * 6-tuple. A clone op inherits the parent's template unless it carries its
     * own. (template_ avoids the C++ keyword in extern "C" consumers.) */
    bool                     has_template;
    moq_msf_media_template_t template_;

    /* Packaging-extension records (see moq_msf_track_ext_t). */
    const moq_msf_track_ext_t *extensions;
    size_t                     extension_count;
} moq_msf_track_t;

/* The draft-gregoire-moq-msfts record of a track, or NULL. */
MOQ_API const moq_msf_mpeg2ts_t *moq_msf_track_mpeg2ts(
    const moq_msf_track_t *track);

/* MSF-01 delta update operation kind (5.1.6). */
typedef enum moq_msf_delta_op_kind {
    MOQ_MSF_DELTA_OP_ADD    = 1,  /* add new tracks */
    MOQ_MSF_DELTA_OP_REMOVE = 2,  /* remove tracks (track objects: name[,namespace]) */
    MOQ_MSF_DELTA_OP_CLONE  = 3,  /* clone from a parent track, with overrides */
} moq_msf_delta_op_kind_t;

/* One delta update operation: an op kind plus its track objects (5.1.6). */
typedef struct moq_msf_delta_op {
    moq_msf_delta_op_kind_t op;
    moq_msf_track_t        *tracks;       /* freed by cleanup */
    size_t                  track_count;
} moq_msf_delta_op_t;

/*
 * A root-level Initialization Data List entry (MSF 5.1.7). The header for a
 * MOQT track is referenced from the track's initRef (5.2.13).
 */
typedef struct moq_msf_init_data_entry {
    moq_bytes_t id;     /* required: unique within the catalog */
    moq_bytes_t type;   /* required: "inline" in this MSF version */
    moq_bytes_t data;   /* the init payload (base64 for type "inline") */
} moq_msf_init_data_entry_t;

/*
 * CMSF content protection (CMSF §4.1.1). Parse/decode is intentionally LENIENT
 * for interop: it carries the string fields as-is (borrowed from the DOM) and
 * checks only required keys and JSON shape, NOT their values. Encode/authoring
 * (moq_msf_catalog_encode) is STRICTER and enforces the concrete §4 SYNTAX:
 * scheme is the "cenc"/"cbcs" enum (§4.1.1.3), drmSystem.systemID and every
 * defaultKID are UUID strings (§4.1.1.4.1/§4.1.1.2, "8-4-4-4-12" hex,
 * case-insensitive), and refIDs are unique across the array (§4.1.1.1).
 * DRM-system SEMANTICS are never interpreted on either path -- the UUID's
 * registry meaning, license-URL reachability, PSSH box internals, and
 * robustness meaning are carried as opaque metadata.
 */
typedef struct moq_cmsf_url {
    bool        present;
    moq_bytes_t url;       /* required when present */
    bool        has_type;
    moq_bytes_t type;
} moq_cmsf_url_t;

typedef struct moq_cmsf_drm_system {
    moq_bytes_t    system_id;   /* systemID (required; UUID string -- syntax
                                   validated on encode, value not interpreted) */
    moq_cmsf_url_t la_url;       /* laURL */
    moq_cmsf_url_t cert_url;     /* certURL */
    moq_cmsf_url_t auth_url;     /* authURL */
    bool        has_pssh;
    moq_bytes_t pssh;           /* base64 PSSH (not validated) */
    bool        has_robustness;
    moq_bytes_t robustness;
} moq_cmsf_drm_system_t;

typedef struct moq_cmsf_content_protection {
    moq_bytes_t  ref_id;            /* refID (required) */
    moq_bytes_t *default_kids;      /* defaultKID[] (required array; freed by cleanup) */
    size_t       default_kid_count;
    moq_bytes_t  scheme;            /* "cenc" | "cbcs" enum (validated on encode) */
    moq_cmsf_drm_system_t drm_system;   /* drmSystem (required) */
} moq_cmsf_content_protection_t;

typedef struct moq_msf_catalog {
    uint32_t          struct_size;
    int               version;
    moq_msf_track_t  *tracks;
    size_t            track_count;
    bool              has_generated_at;
    uint64_t          generated_at;

    /* MSF-01 broadcast completion (5.1.3): a root-level commitment that the
     * broadcast is complete (no new tracks, no new content). Only the TRUE state
     * is meaningful -- the spec forbids emitting isComplete:false, so this is a
     * plain bool: false means "absent". Valid on an independent catalog only
     * (a delta MUST NOT carry it). */
    bool              is_complete;

    /* MSF-01: root Initialization Data List (5.1.7). Entries borrow from the
     * DOM; the array itself is freed by cleanup. */
    moq_msf_init_data_entry_t *init_data_list;
    size_t                     init_data_count;

    /* CMSF: root content protections (§4.1.1). Entries borrow from the DOM; the
     * array and each entry's default_kids array are freed by cleanup. */
    moq_cmsf_content_protection_t *content_protections;
    size_t                         content_protection_count;

    /* MSF-01 delta update (5.1.6). When non-NULL this catalog is a DELTA
     * (partial) update: version is 0 and tracks is empty, and these ops
     * describe changes to apply to a prior independent catalog (see
     * moq_msf_catalog_apply_delta). Ops borrow from the DOM; the op array and
     * each op's track array are freed by cleanup. NULL for an independent
     * catalog. */
    moq_msf_delta_op_t *delta_update;
    size_t              delta_update_count;

    /* Internal: freed by cleanup. */
    void             *_dom;
    size_t            _dom_size;

    /* Appended after `_dom_size`; read by the encoder only when struct_size
     * covers it (absent = NUMERAL). A value other than NUMERAL or DRAFT_NAME
     * is rejected with MOQ_ERR_INVAL. The parser sets it to the form it read
     * (NUMERAL for the MSF-00 numeric 1); apply_delta carries the base's form
     * to the applied catalog. Not written for a delta, which has no version. */
    moq_msf_version_form_t version_form;
} moq_msf_catalog_t;

/*
 * Parse an MSF catalog from JSON.
 *
 * Allocates the JSON DOM and track array via alloc. All string
 * fields in the result borrow from the DOM. Call
 * moq_msf_catalog_cleanup() to release.
 *
 * Optional scalar fields with malformed values (negative unsigned integers,
 * fractional pixel counts, out-of-range numbers) are silently treated
 * as absent rather than failing the parse. A present but malformed
 * STRUCTURED field is an exception: a "template" that is not a well-formed
 * 6-tuple (5.2.15 / §7.4) is a PROTO error, not silently dropped. The
 * mpeg2ts* keys are recorded as present-but-malformed instead (see
 * moq_msf_mpeg2ts_t), so that the track check can reject the track; a
 * "remove" delta track carrying one is a PROTO error like any other field. Only
 * missing required fields (version, tracks, track name/packaging/isLive)
 * and structurally invalid JSON cause parse failure.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL if alloc, json.data, or out is NULL.
 * Returns MOQ_ERR_PROTO on malformed JSON or missing required fields.
 * Returns MOQ_ERR_NOMEM on allocation failure.
 */
MOQ_API moq_result_t moq_msf_catalog_parse(const moq_alloc_t *alloc,
                                             moq_bytes_t json,
                                             moq_msf_catalog_t *out);

MOQ_API void moq_msf_catalog_cleanup(const moq_alloc_t *alloc,
                                      moq_msf_catalog_t *cat);

/*
 * Apply a delta catalog (5.1.6) to a base independent catalog, producing a new
 * independent catalog in *out. Operations are applied in order: "add" appends
 * new tracks, "remove" drops tracks matched by name (and namespace when the
 * remove track specifies one), and "clone" appends a copy of a matched parent
 * track with the clone's fields overriding the inherited values.
 *
 * Track matching uses the Track Name, additionally requiring the Track
 * Namespace to match when the op track carries one (catalog-level namespace
 * inheritance is not resolved). The Namespace|Name tuple is immutable once
 * declared (5.3), so the ops enforce: "add" fails if the tuple already exists,
 * "clone" fails if the parent is absent or the new tuple already exists, and
 * "remove" fails if the target tuple is not present.
 *
 * *out is fully owned (like a parsed catalog) and MUST be released with
 * moq_msf_catalog_cleanup. The effective catalog must be encodable
 * (every resulting track passes the encoder's validation), otherwise the
 * call fails and *out is left zeroed.
 *
 * A catalog with is_complete (5.1.3) is terminal: it admits no further tracks
 * or content, so applying any delta to such a base is rejected (MOQ_ERR_PROTO).
 *
 * Returns MOQ_ERR_INVAL if any argument is NULL, base is itself a delta, or
 *   delta is not a delta (delta_update == NULL).
 * Returns MOQ_ERR_PROTO on a complete (terminal) base, a tuple-semantics
 *   violation (duplicate add, clone parent absent, clone tuple collision,
 *   remove target absent), or an invalid effective catalog.
 * Returns MOQ_ERR_NOMEM on allocation failure.
 */
MOQ_API moq_result_t moq_msf_catalog_apply_delta(
    const moq_alloc_t *alloc,
    const moq_msf_catalog_t *base,
    const moq_msf_catalog_t *delta,
    moq_msf_catalog_t *out);

/*
 * Bounds for moq_msf_catalog_apply_delta_ex, so a caller (e.g. a media
 * receiver) can refuse an over-large delta BEFORE building the effective
 * catalog or scanning identities. A zero field means "no limit". ABI-safe:
 * set struct_size = sizeof and zero the rest; new trailing fields default off.
 */
typedef struct moq_msf_apply_limits {
    uint32_t struct_size;
    size_t   max_delta_ops;        /* reject if delta_update_count exceeds it */
    size_t   max_effective_tracks; /* reject if base + adds/clones exceeds it */
} moq_msf_apply_limits_t;

/*
 * Like moq_msf_catalog_apply_delta, but enforces `limits` before any expensive
 * work. moq_msf_catalog_apply_delta is the unlimited convenience wrapper
 * (limits == NULL). Identity lookups are resolved through a temporary index
 * built over the effective tracks, so apply is O(base + delta) rather than
 * O(n^2). limits == NULL or an all-zero struct is unlimited.
 *
 * Returns MOQ_ERR_PROTO when a limit is exceeded (in addition to the tuple /
 * terminal-base violations moq_msf_catalog_apply_delta returns), and
 * MOQ_ERR_INVAL for malformed caller-provided catalog storage.
 */
MOQ_API moq_result_t moq_msf_catalog_apply_delta_ex(
    const moq_alloc_t *alloc,
    const moq_msf_catalog_t *base,
    const moq_msf_catalog_t *delta,
    const moq_msf_apply_limits_t *limits,
    moq_msf_catalog_t *out);

/*
 * Inbound semantic validation of an INDEPENDENT catalog's identity spaces,
 * intended for a receiver to run before adopting a peer-supplied catalog. The
 * raw parser is deliberately lenient (fuzz/raw callers); this is the strict
 * inbound gate. Checks, in O(n) via a temporary index:
 *   - track Namespace|Name tuples are unique (§5.2.3);
 *   - initDataList ids are unique (§5.1.7);
 *   - contentProtection refIDs are unique (CMSF §4.1.1.1).
 * Returns MOQ_OK if all identity spaces are unique, MOQ_ERR_PROTO on the first
 * duplicate, MOQ_ERR_INVAL if cat is NULL, is a delta, or has malformed
 * caller-provided identity storage, MOQ_ERR_NOMEM on allocation failure. Does
 * not mutate cat.
 */
MOQ_API moq_result_t moq_msf_catalog_validate_identities(
    const moq_alloc_t *alloc,
    const moq_msf_catalog_t *cat);

MOQ_API const moq_msf_track_t *moq_msf_catalog_find_role(
    const moq_msf_catalog_t *cat, const char *role);

/*
 * Resolve a track's initRef (MSF 5.2.13) to its Initialization Data List entry
 * (5.1.7). Returns the matching entry, or NULL if id has no match.
 */
MOQ_API const moq_msf_init_data_entry_t *moq_msf_catalog_find_init_data(
    const moq_msf_catalog_t *cat, moq_bytes_t id);

/*
 * Resolve a track's contentProtectionRefIDs entry (CMSF 4.1.2) to its root
 * content protection (4.1.1). Returns the matching entry, or NULL.
 */
MOQ_API const moq_cmsf_content_protection_t *moq_msf_catalog_find_content_protection(
    const moq_msf_catalog_t *cat, moq_bytes_t ref_id);

/*
 * Why a track is not a valid mpeg2ts track (draft-gregoire-moq-msfts).
 */
typedef enum moq_msf_mpeg2ts_error {
    MOQ_MSF_MPEG2TS_OK = 0,
    MOQ_MSF_MPEG2TS_ERR_NOT_MPEG2TS = 1,             /* packaging is not "mpeg2ts" */
    MOQ_MSF_MPEG2TS_ERR_MALFORMED_FIELD = 2,         /* wrong JSON type or range */
    MOQ_MSF_MPEG2TS_ERR_MODE_MISSING = 3,            /* Mode: required */
    MOQ_MSF_MPEG2TS_ERR_MODE_UNRECOGNIZED = 4,       /* Mode: MUST reject the track */
    MOQ_MSF_MPEG2TS_ERR_PACKET_SIZE_MISSING = 5,     /* Packet Size: required */
    MOQ_MSF_MPEG2TS_ERR_PACKET_SIZE = 6,             /* Packet Size: 188 or 192 */
    MOQ_MSF_MPEG2TS_ERR_TIMESTAMP_MODE_188 = 7,      /* Timestamp Mode: not at 188 */
    MOQ_MSF_MPEG2TS_ERR_ES_PID_MISSING = 8,          /* ES PID: required in es-packets */
    MOQ_MSF_MPEG2TS_ERR_ES_PID_FORBIDDEN = 9,        /* ES PID: absent in other modes */
    MOQ_MSF_MPEG2TS_ERR_PID_RANGE = 10,              /* a PID above 0x1FFF */
    MOQ_MSF_MPEG2TS_ERR_PROGRAM_NUMBER_RANGE = 11,   /* not 1..65535 */
    MOQ_MSF_MPEG2TS_ERR_MUX_RATE_RANGE = 12,         /* 0 bit/s */
    MOQ_MSF_MPEG2TS_ERR_MUX_RATE_FORBIDDEN = 13,     /* Mux Rate: unmodified-multiplex */
    MOQ_MSF_MPEG2TS_ERR_SI_PIDS_FORBIDDEN = 14,      /* SI PIDs: with ES PID / multiplex */
    MOQ_MSF_MPEG2TS_ERR_SI_PIDS_DUPLICATE = 15,      /* SI PIDs: each PID listed once */
    MOQ_MSF_MPEG2TS_ERR_SCTE35_PID_FORBIDDEN = 16,   /* SCTE-35 PID: same rule */
    MOQ_MSF_MPEG2TS_ERR_RANDOM_ACCESS_FORBIDDEN = 17, /* multiplex, no reference */
    MOQ_MSF_MPEG2TS_ERR_ALT_GROUP_FORBIDDEN = 18,    /* Switching: multiplex in altGroup */
    MOQ_MSF_MPEG2TS_ERR_INIT_REF_UNRESOLVED = 19,    /* initRef names no entry */
    MOQ_MSF_MPEG2TS_ERR_INIT_DATA_TYPE = 20,         /* entry type is not "inline" */
    MOQ_MSF_MPEG2TS_ERR_INIT_DATA_BASE64 = 21,       /* data is not valid Base64 */
    MOQ_MSF_MPEG2TS_ERR_INIT_DATA_PACKETS = 22,      /* not whole source packets */
} moq_msf_mpeg2ts_error_t;

/*
 * The subscriber's check of one mpeg2ts track (its common fields and its
 * mpeg2ts record; a track without the record has neither Mode nor Packet
 * Size), against every catalog MUST of
 * draft-gregoire-moq-msfts that one track can break:
 *   - Mode and Packet Size are required; the mode is one of the four names,
 *     and an unrecognized one rejects the track;
 *   - the packet size is 188 or 192, and mpeg2tsTimestampMode is absent at
 *     188;
 *   - mpeg2tsEsPid is present in es-packets mode and absent otherwise;
 *   - mpeg2tsMuxRate is absent in unmodified-multiplex;
 *   - mpeg2tsSiPids and mpeg2tsScte35Pid are absent with mpeg2tsEsPid and in
 *     unmodified-multiplex; SI PIDs are listed once each;
 *   - mpeg2tsRandomAccess is absent in unmodified-multiplex when no
 *     mpeg2tsProgramNumber names a reference program;
 *   - an unmodified-multiplex track is not in an altGroup;
 *   - every value is in range before use (Security Considerations): PIDs
 *     0..0x1FFF, program numbers 1..65535, a mux rate above 0, and no
 *     malformed value.
 * An unrecognized mpeg2tsTimestampMode is not a rejection: a 192-octet
 * subscriber then treats the prefix as opaque, as for an absent one.
 *
 * Returns MOQ_OK for a valid track, MOQ_ERR_PROTO with *reason set when the
 * track must be rejected, MOQ_ERR_INVAL for a NULL track, a record whose
 * struct_size is below its first published size, or packaging other than
 * "mpeg2ts" (*reason MOQ_MSF_MPEG2TS_ERR_NOT_MPEG2TS). reason may be NULL.
 */
MOQ_API moq_result_t moq_msf_track_mpeg2ts_check(const moq_msf_track_t *track,
                                                 moq_msf_mpeg2ts_error_t *reason);

/*
 * Resolve and check an mpeg2ts track's initialization data (Use of MSF
 * Initialization Data): initRef names an initDataList entry whose type MUST
 * be "inline", and whose Base64-decoded data MUST be whole source packets of
 * mpeg2tsPacketSize octets (each with the sync byte at offset 0, or 4 for
 * 192); an empty entry is refused as a local policy. The track must pass
 * moq_msf_track_mpeg2ts_check(). Whether the tables are still current is
 * the caller's to judge: the draft has the PSI in the Objects take
 * precedence once its version_number changes.
 *
 * On MOQ_OK, *out_packets (when out_packets is non-NULL) owns the decoded
 * packets; release it with moq_rcbuf_decref().
 *
 * Returns MOQ_OK, MOQ_ERR_PROTO with *reason set, MOQ_ERR_INVAL for NULL
 * arguments, a track without initRef, or one that is not an mpeg2ts track,
 * and MOQ_ERR_NOMEM.
 */
MOQ_API moq_result_t moq_msf_track_mpeg2ts_init_data(
    const moq_alloc_t *alloc,
    const moq_msf_catalog_t *cat,
    const moq_msf_track_t *track,
    moq_rcbuf_t **out_packets,
    moq_msf_mpeg2ts_error_t *reason);

/* Canonical JSON names of the enums ("" for UNRECOGNIZED). */
MOQ_API const char *moq_msf_mpeg2ts_mode_name(moq_msf_mpeg2ts_mode_t mode);
MOQ_API const char *moq_msf_mpeg2ts_timestamp_mode_name(
    moq_msf_mpeg2ts_timestamp_mode_t mode);

/*
 * Encode an MSF catalog to JSON.
 *
 * Produces a UTF-8 JSON string in an moq_rcbuf_t.
 * String fields in the catalog are borrowed moq_bytes_t and must
 * remain valid for the duration of this call.
 *
 * Returns MOQ_OK on success with *out_json set.
 * Returns MOQ_ERR_INVAL if alloc, cat, or out_json is NULL, if
 *   version != MOQ_MSF_VERSION, or if a string field has data==NULL
 *   with len > 0, or if an mpeg2ts rule below is broken.
 * Returns MOQ_ERR_NOMEM on allocation failure (*out_json is NULL).
 *
 * mpeg2ts tracks (draft-gregoire-moq-msfts) are authored strictly, from each
 * track's MOQ_MSF_EXT_MPEG2TS record (the extension-list rules are under
 * moq_msf_track_ext_t):
 *   - an independent track with packaging "mpeg2ts" must pass
 *     moq_msf_track_mpeg2ts_check(), carry a timestamp mode only of a
 *     recognized value, and, when it has initRef, reference an "inline"
 *     initDataList entry whose Base64 data decodes to whole source packets
 *     (Use of MSF Initialization Data);
 *   - a delta "add" track carrying mpeg2ts fields must have packaging
 *     "mpeg2ts" and pass the same check;
 *   - a delta "clone" track's mpeg2ts overrides must each be valid on their
 *     own (in range, not malformed, a recognized mode, 188 or 192, and no
 *     pair of overrides that the rules forbid together); the fields it
 *     inherits are judged when the delta is applied;
 *   - mpeg2ts fields on any other packaging are refused.
 */
MOQ_API moq_result_t moq_msf_catalog_encode(const moq_alloc_t *alloc,
                                              const moq_msf_catalog_t *cat,
                                              moq_rcbuf_t **out_json);

/*
 * Decode base64-encoded init data from a catalog track.
 *
 * Standard RFC 4648 base64 with = padding.
 * Empty input returns MOQ_OK with a valid empty rcbuf.
 * Returns MOQ_ERR_PROTO on malformed base64.
 * Returns MOQ_ERR_INVAL if alloc, out_data, or data==NULL with len>0.
 * Returns MOQ_ERR_NOMEM on allocation failure.
 */
MOQ_API moq_result_t moq_msf_decode_init_data(const moq_alloc_t *alloc,
                                                moq_bytes_t init_data_b64,
                                                moq_rcbuf_t **out_data);

/*
 * Encode raw bytes to base64 for catalog init data.
 *
 * Standard RFC 4648 base64 with = padding.
 * Empty input returns MOQ_OK with a valid empty rcbuf.
 */
MOQ_API moq_result_t moq_msf_encode_init_data(const moq_alloc_t *alloc,
                                                moq_bytes_t init_data,
                                                moq_rcbuf_t **out_b64);

/*
 * Decode an explicit media-timeline payload (§7.1.1) into caller storage.
 *
 * Sans-I/O, no allocation: parses `json` directly into the caller's `out`
 * array. Strict explicit-array form only: the document MUST be a JSON array of
 * 3-item records [mediaTime, [group, object], wallclock] with non-negative
 * integral JSON numbers (no sign, fraction, exponent, or leading zeros); the
 * empty array [] is valid. A NULL json.data, or a combined/template document
 * (a non-array root), is rejected with MOQ_ERR_PROTO.
 *
 * *out_count is always written (it is the number of records the document holds,
 * regardless of `cap`). Up to `cap` records are written to `out`.
 *
 *   - out_count MUST be non-NULL (else MOQ_ERR_INVAL).
 *   - out == NULL with cap == 0 is allowed as a count probe.
 *   - out == NULL with cap  > 0 is MOQ_ERR_INVAL.
 *   - MOQ_OK: the whole document fit; *out_count records written to `out`.
 *   - MOQ_ERR_BUFFER: more records than `cap`; *out_count = the count needed;
 *     the caller MUST ignore the partial `out` and retry with a larger buffer.
 *   - MOQ_ERR_PROTO: malformed JSON/shape, non-array root, non-integral or
 *     negative number, wrong record/location arity, or trailing garbage;
 *     *out_count = 0.
 */
MOQ_API moq_result_t moq_msf_media_timeline_decode(
    moq_bytes_t json,
    moq_msf_media_timeline_record_t *out,
    size_t cap,
    size_t *out_count);

/*
 * Encode media-timeline records as a compact explicit-array payload (§7.1.1)
 * into the caller's `buf`. Sans-I/O, no allocation. Output is compact JSON
 * (no insignificant whitespace) and is NOT NUL-terminated.
 *
 *   - out_len MUST be non-NULL (else MOQ_ERR_INVAL).
 *   - records == NULL with count > 0 is MOQ_ERR_INVAL.
 *   - MOQ_OK: *out_len = bytes written to `buf`.
 *   - MOQ_ERR_BUFFER: `buf` is NULL, `cap` is 0, or `cap` is too small;
 *     *out_len = the number of bytes the output needs.
 */
MOQ_API moq_result_t moq_msf_media_timeline_encode(
    const moq_msf_media_timeline_record_t *records,
    size_t count,
    uint8_t *buf,
    size_t cap,
    size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MSF_H */
