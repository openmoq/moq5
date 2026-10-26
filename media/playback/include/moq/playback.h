#ifndef MOQ_PLAYBACK_H
#define MOQ_PLAYBACK_H

/*
 * Sans-I/O media playback pipeline.
 *
 * Accepts MoQ objects, parses LOC/CMAF metadata internally, buffers
 * in decode order, detects gaps, and emits abstract decoder commands
 * and playback events. No sockets, no decoder, no threads, no clock.
 *
 * Link against moq::playback (depends on moq::core, moq::loc, moq::cmaf).
 *
 * Ownership: push_object borrows payload/properties for the call.
 * On success, payload is retained with one rcbuf incref (zero-copy).
 * Properties are parsed and not retained. Commands own payload refs
 * and require moq_playback_cmd_cleanup. Events are scalar-only.
 *
 * Every caller-filled struct begins with struct_size; the library reads a
 * field beyond the struct's frozen v0 floor only when struct_size covers the
 * whole field. Initialize them with the _init macros, which pass the
 * caller's sizeof to the exported _init_sized functions.
 */

#include <stddef.h>
#include <stdint.h>

#include <moq/types.h>
#include <moq/session.h>
#include <moq/rcbuf.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Opaque handles -------------------------------------------------- */

typedef struct moq_playback moq_playback_t;

typedef struct moq_playback_track {
    uint64_t _opaque;
} moq_playback_track_t;

#ifdef __cplusplus
#define MOQ_PLAYBACK_TRACK_INVALID (moq_playback_track_t{0})
#else
#define MOQ_PLAYBACK_TRACK_INVALID ((moq_playback_track_t){ 0 })
#endif

/* A command hold (moq_playback_poll_command_held). A value: copies name
 * the same one hold, and releasing any copy ends it. */
typedef struct moq_playback_command_hold {
    uint64_t _opaque;
} moq_playback_command_hold_t;

#ifdef __cplusplus
#define MOQ_PLAYBACK_COMMAND_HOLD_INVALID (moq_playback_command_hold_t{0})
#else
#define MOQ_PLAYBACK_COMMAND_HOLD_INVALID ((moq_playback_command_hold_t){ 0 })
#endif

/* -- Kinds and reasons ---------------------------------------------- */

/* Each set below is fixed-width uint32_t for ABI stability. Values are never
 * renumbered. A consumer must ignore a kind or reason it does not know. */

typedef uint32_t moq_playback_media_type_t;
#define MOQ_PLAYBACK_MEDIA_VIDEO 1u
#define MOQ_PLAYBACK_MEDIA_AUDIO 2u

typedef uint32_t moq_playback_packaging_t;
#define MOQ_PLAYBACK_PACKAGING_RAW  1u
#define MOQ_PLAYBACK_PACKAGING_CMAF 2u

typedef uint32_t moq_playback_cmd_kind_t;
#define MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO 1u
#define MOQ_PLAYBACK_CMD_DECODE_VIDEO    2u
#define MOQ_PLAYBACK_CMD_DECODE_CMAF     3u
#define MOQ_PLAYBACK_CMD_RESET           4u
#define MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO 5u
#define MOQ_PLAYBACK_CMD_DECODE_AUDIO    6u

typedef uint32_t moq_playback_reset_reason_t;
#define MOQ_PLAYBACK_RESET_GAP           1u
#define MOQ_PLAYBACK_RESET_DECODE_ERROR  2u
#define MOQ_PLAYBACK_RESET_TRACK_SWITCH  3u
#define MOQ_PLAYBACK_RESET_CONFIG_CHANGE 4u

typedef uint32_t moq_playback_event_kind_t;
#define MOQ_PLAYBACK_EVENT_GAP_DETECTED            1u
#define MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING        2u
#define MOQ_PLAYBACK_EVENT_SKIP_FORWARD            3u
#define MOQ_PLAYBACK_EVENT_OBJECT_DROPPED          4u
#define MOQ_PLAYBACK_EVENT_TRACK_ENDED             5u
#define MOQ_PLAYBACK_EVENT_BACKLOG_SHED            6u
#define MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED 7u
/* A gap_policy CONTINUE track passed a hole without a decoder reset: the
 * media time the missing units would have covered, for concealment. */
#define MOQ_PLAYBACK_EVENT_GAP_CONTINUED           8u
/* The bounded knowledge of a track ran out of room, and two ranges were
 * coalesced into one unresolved range that demoted established absence:
 * the engine may now wait for ids it once knew do not exist. The event
 * names the coalesced range's groups. */
#define MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN     9u
/* A missing unit was given up inside its group: release resumed at the
 * group's next independent unit, behind a RESET (GAP). The units from
 * from_object_id up to, not including, to_object_id were given up; the
 * group is not. Committed with that step, never with the wait's
 * GAP_DETECTED announcement: a missing unit that arrives first is played,
 * and no such event follows. */
#define MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP   10u

/* Why the engine waited on a missing id (gap_detected.cause). UNKNOWN:
 * nothing said why it is missing. STREAM_RESET: a stream reset left it
 * undelivered. RECEIVER_DROP: the receiver's overflow policy dropped its
 * group. LOCAL_REFUSAL: the consumer refused that one object
 * (MOQ_PLAYBACK_STREAM_OBJECT_REFUSED). Each may still arrive another way,
 * so the wait is the same; only the report differs. */
typedef uint32_t moq_playback_gap_cause_t;
#define MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN       1u
#define MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET  2u
#define MOQ_PLAYBACK_GAP_CAUSE_RECEIVER_DROP 3u
#define MOQ_PLAYBACK_GAP_CAUSE_LOCAL_REFUSAL 4u

/* What a track does when it gives up waiting on a missing unit. Supplied by
 * whoever verified the codec's dependency semantics; the engine parses no
 * codec. DISCONTINUITY (the default) resets the decoder and resumes only at
 * an independent unit. CONTINUE keeps the decoder state across the hole,
 * reports it (GAP_CONTINUED) and resumes at the next unit: for a codec whose
 * units do not depend on earlier ones. */
typedef uint32_t moq_playback_gap_policy_t;
#define MOQ_PLAYBACK_GAP_DISCONTINUITY 1u
#define MOQ_PLAYBACK_GAP_CONTINUE      2u

/* Why an object was dropped. Values 1-7 name the same conditions as the
 * moq_media_drop_reason_t values of the same number; the engine maps parser
 * reasons explicitly, never by cast. STALE is an object below the release
 * cursor that the engine never held: it arrived after the engine had moved
 * past its location. NON_MONOTONIC_DTS applies to CMAF only, whose decode
 * time is a decode clock; a RAW object's decode order is its object order. */
typedef uint32_t moq_playback_drop_reason_t;
#define MOQ_PLAYBACK_DROP_MALFORMED_LOC            1u
#define MOQ_PLAYBACK_DROP_MALFORMED_CMAF           2u
#define MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP        3u
#define MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS        4u
#define MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE 5u
#define MOQ_PLAYBACK_DROP_STALE                    6u
#define MOQ_PLAYBACK_DROP_KEYFRAME_WAIT            7u
/* A CMAF object of more than one chunk. The fragment parser describes only
 * an object's first chunk, so the object is refused rather than played
 * with its later chunks dropped. No moq_media_drop_reason_t counterpart. */
#define MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK  8u

typedef uint32_t moq_playback_feedback_kind_t;
#define MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE 1u
#define MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR   2u

/* -- Config ---------------------------------------------------------- */

typedef struct moq_playback_cfg {
    uint32_t struct_size;
    uint32_t max_tracks;             /* default 4 */
    uint32_t max_buffered_objects;   /* jitter buffer entry cap; default 256 */
    uint64_t max_buffered_bytes;     /* bytes the engine keeps for buffered
                                      * media: payloads, CMAF sample tables,
                                      * configuration copies, including those
                                      * of queued commands until they are
                                      * polled, and of held commands until
                                      * their hold is released; default
                                      * 16 MiB */
    uint32_t max_backlog_groups;     /* live shedding threshold; default 3 */
    uint32_t max_commands;           /* command queue capacity; default 64 */
    uint32_t max_events;             /* event queue capacity; default 16 */
    uint64_t gap_timeout_us;         /* gap wait before skip; default 500000 */
    uint32_t max_release_per_tick;   /* bounded drain; 0 = unlimited; default 8 */
    uint32_t max_track_config_bytes; /* per-track codec+init cap; default 4096 */
    uint32_t max_samples_per_object; /* CMAF sample scratch cap; default 16 */

    /* When true, the subgroup-header end_of_group bit on normal
     * objects does not close the group. Group close requires an
     * explicit MOQ_OBJECT_END_OF_GROUP status marker or arrival
     * of any object from group N+1 (when
     * infer_end_of_group_from_next_group is also set).
     * Useful for live CMAF where publishers set eog on every
     * subgroup header. Default false. */
    bool     ignore_eog_bit;

    /* If true, the arrival of any object from group N+1
     * implies group N is complete when no more objects from
     * group N are buffered. Default false. */
    bool     infer_end_of_group_from_next_group;

    /* Appended at the complete sizeof through
     * infer_end_of_group_from_next_group. Ranges of known absence and of
     * unresolved delivery kept per track (see moq_playback_on_stream_
     * terminal); 0 selects 64. */
    uint32_t max_known_ranges;

    /* Appended after max_known_ranges. Commands that may be held at once
     * (moq_playback_poll_command_held), at most 65536; 0, and a
     * struct_size that does not cover the field, disable held polls. */
    uint32_t max_held_commands;
} moq_playback_cfg_t;

/* Frozen v0 floor: struct_size through max_track_config_bytes. The fields
 * after it are read only when struct_size covers the whole field; an
 * uncovered field takes its default. */
#define MOQ_PLAYBACK_CFG_V0_SIZE \
    (offsetof(moq_playback_cfg_t, max_track_config_bytes) + \
     sizeof(((moq_playback_cfg_t *)0)->max_track_config_bytes))

/* Clears min(cfg_size, sizeof(moq_playback_cfg_t)) bytes, stamps
 * struct_size with that count and sets the default of every field wholly
 * inside it. A cfg_size below sizeof(uint32_t), or a NULL cfg, writes
 * nothing. */
MOQ_API void moq_playback_cfg_init_sized(moq_playback_cfg_t *cfg,
                                         size_t cfg_size);
#define moq_playback_cfg_init(cfg) \
    moq_playback_cfg_init_sized((cfg), sizeof(*(cfg)))

/* -- Track config ---------------------------------------------------- */

typedef struct moq_playback_track_cfg {
    uint32_t                   struct_size;
    moq_playback_media_type_t  media_type;
    moq_playback_packaging_t   packaging;
    moq_bytes_t                codec;       /* borrowed; copied internally */
    moq_bytes_t                init_data;   /* borrowed; copied internally */
    uint32_t                   timescale;   /* caller hint; 0 = derive from init_data */
    uint32_t                   width;       /* caller hint; 0 = derive from init_data */
    uint32_t                   height;      /* caller hint; 0 = derive from init_data */
    uint32_t                   samplerate;  /* caller hint; 0 = derive from init_data */
    uint32_t                   channel_count; /* caller hint; 0 = derive from init_data */
    uint64_t                   target_latency_us;
    bool                       is_live;

    /* Appended after is_live. The spacer covers the v0 tail padding and is
     * never read, so the first appended field starts past the complete v0
     * sizeof on every target. */
    uint8_t                    _reserved_v0_tail[7];

    /* The negotiated MoQ transport draft, a moq_version_t value: 16, 18 or
     * 22. It selects the LOC companion and its property codec: LOC-01 on
     * drafts 16 and 18, LOC-04 on draft 22. A caller whose struct_size does
     * not cover it gets draft 16. A covered value must be one of the three;
     * zero is refused rather than read as draft 16. */
    uint32_t                   transport_version;

    /* Draft 22 only: the track's LOC-04 properties, as carried in the
     * catalog or the track's request parameters. Borrowed for the call and
     * copied. An object's own LOC fields win; only an absent timescale or
     * video/audio configuration is inherited from here. Counts against
     * max_track_config_bytes together with codec and init_data. Must be
     * empty on drafts 16 and 18. */
    moq_bytes_t                track_properties;

    /* Appended after track_properties. What the track does when it gives
     * up on a missing unit, a moq_playback_gap_policy_t. A caller whose
     * struct_size does not cover it gets DISCONTINUITY; a covered value
     * must be one of the two. */
    moq_playback_gap_policy_t  gap_policy;
} moq_playback_track_cfg_t;

/* Frozen v0 floor: struct_size through is_live. */
#define MOQ_PLAYBACK_TRACK_CFG_V0_SIZE \
    (offsetof(moq_playback_track_cfg_t, is_live) + \
     sizeof(((moq_playback_track_cfg_t *)0)->is_live))

/* Clears min(cfg_size, sizeof) bytes and stamps struct_size with that
 * count. A covered transport_version is set to MOQ_VERSION_DRAFT_16; a
 * caller on a draft-18 or draft-22 session must assign the negotiated
 * version. A cfg_size below sizeof(uint32_t) writes nothing. */
MOQ_API void moq_playback_track_cfg_init_sized(moq_playback_track_cfg_t *cfg,
                                               size_t cfg_size);
#define moq_playback_track_cfg_init(cfg) \
    moq_playback_track_cfg_init_sized((cfg), sizeof(*(cfg)))

/* -- Input object ---------------------------------------------------- */

typedef struct moq_playback_object {
    uint32_t              struct_size;
    moq_playback_track_t  track;
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority;
    moq_object_status_t   status;
    bool                  end_of_group;
    bool                  datagram;
    moq_rcbuf_t          *payload;     /* borrowed for call */
    moq_rcbuf_t          *properties;  /* borrowed for call */
} moq_playback_object_t;

/* Frozen v0 floor: the complete layout through properties. */
#define MOQ_PLAYBACK_OBJECT_V0_SIZE \
    (offsetof(moq_playback_object_t, properties) + sizeof(moq_rcbuf_t *))

/* Clears min(obj_size, sizeof) bytes and stamps struct_size with that
 * count. An obj_size below sizeof(uint32_t) writes nothing. */
MOQ_API void moq_playback_object_init_sized(moq_playback_object_t *obj,
                                            size_t obj_size);
#define moq_playback_object_init(obj) \
    moq_playback_object_init_sized((obj), sizeof(*(obj)))

/* -- Parsed input ---------------------------------------------------- */

struct moq_media_parsed_object;

/*
 * An object the caller already parsed with the media-object parser, for
 * push_parsed. The engine trusts the record as the parser's output for
 * `payload` and re-checks what it relies on (see push_parsed).
 */
typedef struct moq_playback_parsed_input {
    uint32_t              struct_size;
    moq_playback_track_t  track;
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority;
    /* BORROWED for the call: status, framing, timing and media ranges are
     * read from it; its spans are not retained. A draft-22 track needs
     * the record's struct_size to cover its metadata. */
    const struct moq_media_parsed_object *parsed;
    /* BORROWED for the call; one ref is taken when the object is kept.
     * NULL exactly for a status marker. */
    moq_rcbuf_t          *payload;
    /* The timescale the caller parsed a CMAF object with
     * (moq_media_track_info_t.timescale). For CMAF it must be non-zero and
     * equal to the track's timescale, or the record is refused. Ignored
     * for RAW. */
    uint32_t              timescale;
} moq_playback_parsed_input_t;

/* Frozen v0 floor: the complete layout through timescale. */
#define MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE \
    (offsetof(moq_playback_parsed_input_t, timescale) + sizeof(uint32_t))

MOQ_API void moq_playback_parsed_input_init_sized(
    moq_playback_parsed_input_t *in, size_t in_size);
#define moq_playback_parsed_input_init(in) \
    moq_playback_parsed_input_init_sized((in), sizeof(*(in)))

/* -- Commands -------------------------------------------------------- *
 * DECODE commands come in decode order, per track. Their decode_time_us is
 * the decode clock on CMAF (tfdt/trun), never decreasing within a timeline.
 * On RAW (LOC) it is a copy of the object's LOC media time, equal to
 * presentation_time_us: LOC carries no decode timestamp and decode order is
 * object order, so with frame reordering (B-frames) it can decrease from
 * one command to the next. It is not evidence of a known decode clock, and
 * a zero composition_offset_us does not mean the codec has no reordered
 * frames. */

typedef struct moq_playback_cmd {
    uint32_t                  struct_size;
    moq_playback_cmd_kind_t   kind;
    moq_playback_track_t      track;

    union {
        struct {
            moq_rcbuf_t *codec;        /* OWNED */
            moq_rcbuf_t *codec_config; /* OWNED */
            uint32_t     width;
            uint32_t     height;
        } configure_video;

        struct {
            uint64_t     group_id;
            uint64_t     object_id;
            uint64_t     decode_time_us;
            int64_t      composition_offset_us;
            uint64_t     presentation_time_us;
            bool         has_capture_time;
            uint64_t     capture_time_us;
            bool         keyframe;
            moq_rcbuf_t *payload;      /* OWNED */
        } decode_video;

        struct {
            uint64_t     group_id;
            uint64_t     object_id;
            uint64_t     decode_time_us;
            int64_t      composition_offset_us;
            uint64_t     presentation_time_us;
            bool         has_capture_time;
            uint64_t     capture_time_us;
            bool         keyframe;
            uint32_t     sample_duration_us;
            moq_rcbuf_t *fragment;     /* OWNED: entire moof+mdat */
            size_t       mdat_offset;
            size_t       mdat_len;
        } decode_cmaf;

        struct {
            moq_playback_reset_reason_t reason;
        } reset;

        struct {
            moq_rcbuf_t *codec;        /* OWNED */
            moq_rcbuf_t *codec_config; /* OWNED */
            uint32_t     samplerate;
            uint32_t     channel_count;
        } configure_audio;

        struct {
            uint64_t     group_id;
            uint64_t     object_id;
            uint64_t     decode_time_us;
            int64_t      composition_offset_us;
            uint64_t     presentation_time_us;
            bool         has_capture_time;
            uint64_t     capture_time_us;
            uint32_t     sample_duration_us;
            moq_rcbuf_t *payload;      /* OWNED: RAW payload or CMAF fragment */
            size_t       mdat_offset;  /* 0 for RAW */
            size_t       mdat_len;     /* 0 for RAW */
        } decode_audio;
    } u;
} moq_playback_cmd_t;

/* Frozen v0 size of a command record: through decode_cmaf, the largest
 * union member, rounded up to the record's alignment. A poll needs room for
 * at least this many bytes. */
#ifdef __cplusplus
#define MOQ_PLAYBACK_RECORD_ALIGNMENT \
    (alignof(void *) > alignof(uint64_t) ? alignof(void *) : alignof(uint64_t))
#else
#define MOQ_PLAYBACK_RECORD_ALIGNMENT \
    (_Alignof(void *) > _Alignof(uint64_t) ? _Alignof(void *) : _Alignof(uint64_t))
#endif
#define MOQ_PLAYBACK_CMD_V0_SIZE \
    ((offsetof(moq_playback_cmd_t, u.decode_cmaf.mdat_len) + sizeof(size_t) + \
      MOQ_PLAYBACK_RECORD_ALIGNMENT - 1) & \
     ~(size_t)(MOQ_PLAYBACK_RECORD_ALIGNMENT - 1))

/* Releases the refs a polled command owns and clears those pointers.
 * Every polled command goes through here, whatever its kind: the library
 * knows which kinds own refs, the caller need not. Reads only what the
 * record's stamped struct_size covers, so a record below the v0 size (one
 * this library never stamped) is left untouched. Idempotent and NULL-safe,
 * and legal after the engine that produced the command was destroyed: a
 * command holds rcbuf refs only, never a pointer into the engine. */
MOQ_API void moq_playback_cmd_cleanup(moq_playback_cmd_t *cmd);

/* -- Events ---------------------------------------------------------- */

typedef struct moq_playback_event {
    uint32_t                    struct_size;
    moq_playback_event_kind_t   kind;
    moq_playback_track_t        track;

    union {
        struct {
            uint64_t group_id;
            /* Appended within the record: the missing object and why the
             * engine waited on it, a moq_playback_gap_cause_t. */
            uint64_t object_id;
            moq_playback_gap_cause_t cause;
        } gap_detected;

        struct {
            uint64_t from_group_id;
            uint64_t to_group_id;
        } skip_forward;

        struct {
            moq_playback_drop_reason_t reason;
            uint64_t group_id;
            uint64_t object_id;
        } object_dropped;

        struct {
            uint64_t from_group_id;
            uint64_t to_group_id;
        } partial_group_abandoned;

        struct {
            uint32_t dropped_groups;
            uint32_t remaining_groups;
        } backlog_shed;

        /* Appended members fit within object_dropped on every target, so
         * the record keeps its v0 size. */
        struct {
            uint64_t hole_start_us;    /* where released media stopped */
            uint64_t hole_duration_us; /* to the resumed unit's decode time */
        } gap_continued;

        struct {
            uint64_t first_group_id;   /* the groups now only unresolved */
            uint64_t last_group_id;
        } knowledge_forgotten;

        struct {
            uint64_t group_id;
            uint64_t from_object_id;   /* first unit given up */
            uint64_t to_object_id;     /* where release resumed (exclusive) */
        } gap_skipped_in_group;
    } u;
} moq_playback_event_t;

/* Frozen v0 size of an event record: through object_dropped, the largest
 * union member, rounded up to the record's alignment. */
#define MOQ_PLAYBACK_EVENT_V0_SIZE \
    ((offsetof(moq_playback_event_t, u.object_dropped.object_id) + \
      sizeof(uint64_t) + MOQ_PLAYBACK_RECORD_ALIGNMENT - 1) & \
     ~(size_t)(MOQ_PLAYBACK_RECORD_ALIGNMENT - 1))

/* -- Feedback -------------------------------------------------------- */

typedef struct moq_playback_feedback {
    uint32_t                      struct_size;
    moq_playback_feedback_kind_t  kind;
    moq_playback_track_t          track;

    union {
        struct {
            uint32_t depth;
            uint32_t max_recommended;
        } queue_pressure;

        struct {
            moq_bytes_t message; /* borrowed for call */
        } decode_error;
    } u;
} moq_playback_feedback_t;

/* Frozen v0 floor: the complete layout through the decode_error message. */
#define MOQ_PLAYBACK_FEEDBACK_V0_SIZE \
    (offsetof(moq_playback_feedback_t, u.decode_error.message) + \
     sizeof(moq_bytes_t))

/* Clears min(fb_size, sizeof) bytes and stamps struct_size with that
 * count. An fb_size below sizeof(uint32_t) writes nothing. */
MOQ_API void moq_playback_feedback_init_sized(moq_playback_feedback_t *fb,
                                              size_t fb_size);
#define moq_playback_feedback_init(fb) \
    moq_playback_feedback_init_sized((fb), sizeof(*(fb)))

/* -- Lifecycle ------------------------------------------------------- */

MOQ_API moq_result_t moq_playback_create(
    const moq_alloc_t *alloc,
    const moq_playback_cfg_t *cfg,
    moq_playback_t **out);

MOQ_API void moq_playback_destroy(moq_playback_t *pb);

/* -- Track management ------------------------------------------------ */

/*
 * Returns MOQ_OK; MOQ_ERR_INVAL on a NULL argument, a struct_size below the
 * v0 floor, an unknown media type or packaging, a covered transport_version
 * that is not 16, 18 or 22, non-empty track_properties on drafts 16/18,
 * a span with NULL data and non-zero length, or configuration bytes over
 * max_track_config_bytes; MOQ_ERR_PROTO when a CMAF init segment or the
 * draft-22 track_properties do not parse; MOQ_ERR_NOMEM; MOQ_ERR_WOULD_BLOCK
 * when every track slot is in use. A failed call consumes no slot and
 * retains nothing.
 */
MOQ_API moq_result_t moq_playback_add_track(
    moq_playback_t *pb,
    const moq_playback_track_cfg_t *cfg,
    moq_playback_track_t *out);

MOQ_API moq_result_t moq_playback_remove_track(
    moq_playback_t *pb,
    moq_playback_track_t track);

/* -- Object input ---------------------------------------------------- */

/*
 * Admit one object.
 *
 * An object is placed by its (group_id, object_id) location relative to the
 * track's release cursor, the next location the engine will release:
 *  - At or above the cursor it is buffered; a duplicate of a buffered
 *    location is absorbed (MOQ_OK, nothing retained).
 *  - Below the cursor, once the track has released anything, it can never
 *    be played. A duplicate of an object the engine released or dropped is
 *    absorbed silently. A status marker carries no media and is absorbed
 *    silently. Any other object is reported as OBJECT_DROPPED with reason
 *    STALE and is not retained. The engine remembers the last
 *    max_buffered_objects released locations; an older duplicate is
 *    reported STALE.
 *  - Before the track has released anything, the cursor is set by an
 *    entry: a video object with independence evidence (LOC frame marking,
 *    or a CMAF sync sample), or any audio object. The first entry sets it
 *    wherever it lies; a later entry below it moves it back. An
 *    END_OF_TRACK marker sets the cursor of a track that has no entry yet.
 *    Other objects are buffered meanwhile; a video track holding objects
 *    but no entry reports KEYFRAME_WAITING once from tick.
 *  - Release starts with the first action that commits at the cursor (a
 *    tick refused for queue capacity starts nothing). From then on, objects
 *    still buffered below the cursor are dropped STALE, one event each.
 *
 * Decoder configuration: on draft 22 the object's effective LOC-04 Video
 * Config (video track) or Audio Config (audio track), as the parser
 * resolves it from the object and the track properties, is copied and kept
 * with the object. When the object is released, a CONFIGURE carrying it
 * precedes the object's decode if the decoder is unconfigured or the bytes
 * differ from the configuration last emitted. A present but empty
 * configuration is a change to an empty one (CONFIGURE with no config
 * bytes); an absent one changes nothing. Without any, the first CONFIGURE
 * carries the catalog configuration (init_data or the CMAF init's codec
 * configuration). Drafts 16 and 18 carry no configuration the parser
 * reports. A multi-sample CMAF object's sample table is copied too.
 *
 * Results: MOQ_OK when the object was buffered, absorbed or dropped with
 * its event; MOQ_ERR_LIMIT when what it would keep (payload, sample
 * table, configuration copy) is larger than the whole max_buffered_bytes
 * budget, or its configuration is larger than max_track_config_bytes, so
 * it can never be admitted (nothing is retained or reported, the cursor
 * does not move, and the caller must not retry it). MOQ_ERR_LIMIT also
 * when no object is buffered and the object does not fit beside the
 * configuration copies the tracks keep as their decoder and release
 * configurations: nothing the engine does can free those. Copies owned
 * only by queued CONFIGURE commands do not count there, since polling
 * frees them; that pressure is MOQ_ERR_WOULD_BLOCK. MOQ_ERR_NOMEM when an
 * owned copy cannot be allocated (nothing retained); MOQ_ERR_WOULD_BLOCK
 * when the buffer is full or a drop event has no room. A refused push retains nothing and changes no buffered
 * object, ref, history entry or event; for a full buffer it notes the
 * refused object so the next tick can make room (see tick). Retry after
 * draining and ticking. MOQ_ERR_INVAL on bad arguments, struct_size below
 * the v0 floor, or an unknown status; MOQ_ERR_STALE_HANDLE;
 * MOQ_ERR_WRONG_STATE after the track ended.
 */
MOQ_API moq_result_t moq_playback_push_object(
    moq_playback_t *pb,
    const moq_playback_object_t *obj,
    uint64_t now_us);

/*
 * Admit an object the caller already parsed. The media-object parser is
 * not run again.
 *
 * Placement, absorption, drops and results are push_object's, including
 * MOQ_ERR_LIMIT, MOQ_ERR_NOMEM and the refusal contract. In addition,
 * MOQ_ERR_INVAL when `parsed` is NULL or its struct_size is below
 * MOQ_MEDIA_PARSED_OBJECT_V0_SIZE (or below
 * MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE on a draft-22 track, whatever its
 * status), when its packaging is not the track's, when its status
 * contradicts `payload`, when a RAW record's payload span is not exactly
 * `payload`'s bytes, or when a CMAF record has no samples, a media range
 * outside `payload`, or a timing context other than the track's (below).
 * A CMAF record with more samples than max_samples_per_object is dropped
 * UNSUPPORTED_MULTI_SAMPLE, and one of several chunks
 * UNSUPPORTED_MULTI_CHUNK, as push_object does. Every check on the record
 * comes before any admission effect: an invalid record is refused even
 * where a valid one would be absorbed or dropped.
 *
 * CMAF timing context: the record's decode time is in microseconds, but
 * exact per-sample timing needs the fragment's base decode time in ticks,
 * which the record does not carry. The engine therefore reads the
 * fragment's index once, at admission, and admits the record only if it
 * was produced from this payload with the track's timescale: the same
 * sample table, and a decode time equal to the base tick at that scale. A
 * track without a timescale cannot check this and refuses CMAF records.
 *
 * Ownership: on admission the engine takes one ref on `payload`, copies
 * the record's scalars, and keeps owned copies of the multi-sample CMAF
 * sample table with its base tick and of a draft-22 effective decoder
 * configuration (see push_object). The byte charge is all of it. Release
 * reads only those copies, so the caller's record, sample array and
 * property bytes may go away as soon as the call returns. On every other
 * result nothing is retained and no ref changes.
 */
MOQ_API moq_result_t moq_playback_push_parsed(
    moq_playback_t *pb,
    const moq_playback_parsed_input_t *in,
    uint64_t now_us);

/* -- Admission observation ------------------------------------------ */

/* What stands between an object and its admission: a capacity
 * classification, not a prediction that a push succeeds (a push can still
 * need event room or fail allocation). */
typedef uint32_t moq_playback_admission_t;
/* No capacity obstacle: a push now would place it, or needs no placement
 * (already buffered, stale, or an object the engine would drop). */
#define MOQ_PLAYBACK_ADMISSION_FITS        1u
/* The engine's own relief can make room (it drops waiting objects of
 * tracks without an entry). */
#define MOQ_PLAYBACK_ADMISSION_RELIEF      2u
/* Room exists only once the application releases what it holds up: the
 * buffered media of tracks paused by queue-pressure feedback, and the
 * commands and command holds it has not polled or released. A
 * counterfactual over those resources: resuming a decoder does not by
 * itself release every buffered object, and normal gap and release
 * policy still applies. Ask again after feedback or progress. */
#define MOQ_PLAYBACK_ADMISSION_APPLICATION 3u
/* Room waits on the engine's own progress: release of tracks that are not
 * paused, or a gap settling. */
#define MOQ_PLAYBACK_ADMISSION_ENGINE      4u
/* Placement's LIMIT: the object cannot fit the configured budgets as they
 * stand (it says nothing about a later track or configuration change). */
#define MOQ_PLAYBACK_ADMISSION_NEVER       5u
/* Any other value is a classification of a later release; a caller treats
 * it as no capacity obstacle it knows how to wait on. */

typedef struct moq_playback_admission_info {
    uint32_t                 struct_size;  /* stamped: the bytes written */
    moq_playback_admission_t kind;
    /* What placing it now would charge the byte budget; 0 when it needs no
     * placement. Beyond the v0 floor: written only when out_size covers it. */
    uint64_t                 charge_bytes;
} moq_playback_admission_info_t;

/* The v0 floor of moq_playback_admission_info_t: struct_size and kind. */
#define MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE 8u

/* Classify what stands between `in` and its admission, with the engine as
 * it is now, without admitting it. `in` is what moq_playback_push_parsed
 * takes, validated exactly as it would be: its refusals (MOQ_ERR_INVAL,
 * MOQ_ERR_STALE_HANDLE, MOQ_ERR_WRONG_STATE for an ended track) are returned
 * the same, and nothing is written then.
 *
 * On MOQ_OK writes min(out_size, sizeof(*out)) bytes of `out`, with
 * struct_size the number written. MOQ_ERR_INVAL, writing nothing, when pb,
 * in or out is NULL or out_size is below MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE.
 *
 * Observing: it places, drops, relieves and queues nothing, raises no event,
 * moves no cursor, allocates nothing and leaves any refused push's note as
 * it was. Caller-serialized, like every call on the engine. Its cost is
 * bounded by the configuration: O(max_buffered_objects * (copies + 1)) for
 * the release accounting, plus the queued commands for configuration reuse. */
MOQ_API moq_result_t moq_playback_get_admission(
    const moq_playback_t *pb,
    const moq_playback_parsed_input_t *in,
    moq_playback_admission_info_t *out,
    size_t out_size);

/* -- Stream facts ---------------------------------------------------- */

/* How delivery of a track's objects ended somewhere, as the receiver's
 * stream-terminal lane reports it (moq_media_stream_terminal_t), fed in the
 * lane's order relative to the track's objects. */
typedef uint32_t moq_playback_stream_terminal_kind_t;
/* A subgroup's stream ended cleanly and its header carried END_OF_GROUP:
 * no object of the group above last_object_id exists. */
#define MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP 1u
/* A subgroup's stream ended cleanly: nothing follows about the group. */
#define MOQ_PLAYBACK_STREAM_FIN              2u
/* A subgroup's stream was reset: the group's objects after the last one it
 * completed (or from the one it cut short) were not delivered by it, and
 * may still arrive another way. */
#define MOQ_PLAYBACK_STREAM_RESET            3u
/* The receiver dropped objects of groups group_id..last_group_id; they may
 * be missing. */
#define MOQ_PLAYBACK_STREAM_RECEIVER_DROP    4u
/* The track ends at (group_id, object_id): no object exists there or after. */
#define MOQ_PLAYBACK_STREAM_END_OF_TRACK     5u
/* The consumer refused the object at (group_id, object_id) before pushing
 * it: that one id is unresolved, with cause LOCAL_REFUSAL. Not a transport
 * fact: it says nothing of other ids, of absence or of the group, and moves
 * no cursor. object_id is a real object id (not UINT64_MAX). Once
 * accepted, it also settles a push of that object left waiting for room:
 * nothing is evicted for it any more. A later copy is still accepted. */
#define MOQ_PLAYBACK_STREAM_OBJECT_REFUSED   6u

typedef struct moq_playback_stream_terminal {
    uint32_t                            struct_size;
    moq_playback_stream_terminal_kind_t kind;
    uint64_t                            group_id;
    uint64_t                            subgroup_id;
    uint64_t                            object_id;      /* END_OF_TRACK,
                                                           OBJECT_REFUSED */
    uint64_t                            last_group_id;  /* RECEIVER_DROP */
    uint64_t                            error_code;     /* RESET */
    bool                                has_last_object;
    bool                                has_cut_object; /* RESET */
    uint8_t                             _reserved[6];
    uint64_t                            last_object_id;
    uint64_t                            cut_object_id;
} moq_playback_stream_terminal_t;

/* Frozen v0 floor: the complete layout through cut_object_id. */
#define MOQ_PLAYBACK_STREAM_TERMINAL_V0_SIZE \
    (offsetof(moq_playback_stream_terminal_t, cut_object_id) + sizeof(uint64_t))

/*
 * Feed one stream fact for `track`. The engine keeps three things apart per
 * track: authoritative absence (ids that do not exist: above an
 * END_OF_GROUP stream's last object, and from an END_OF_TRACK on),
 * unresolved delivery (ids a reset or a receiver drop left undelivered,
 * which may still arrive) and its own release cursor. A FIN says nothing
 * about existence. Every fact is also evidence that the track's delivery
 * moved past the cursor, which starts the wait on a missing id (see
 * moq_playback_tick).
 *
 * Absence is skipped without waiting and without a decoder reset: there is
 * nothing to miss. An END_OF_TRACK ends the track (TRACK_ENDED) once the
 * cursor reaches it. Unresolved ids are waited on like unknown ones; only
 * the GAP_DETECTED cause differs.
 *
 * Knowledge is bounded by max_known_ranges per track. Ranges of one kind
 * that touch merge; ranges behind the cursor are dropped. When the ranges
 * ahead of the cursor fill the bound, the two closest are coalesced into
 * one unresolved range covering both; when that demotes absence, one
 * KNOWLEDGE_FORGOTTEN reports it, never a silent merge.
 *
 * Each fact keeps its own arrival time and reach (the end of its group, or
 * for a receiver drop its last group): a wait counts from the earliest fact
 * that reached the missing id, never from an earlier fact that did not.
 * The facts kept are also bounded by max_known_ranges; one that must give
 * way hands its ids to the next fact, so a wait can lengthen but never
 * start before its evidence came.
 *
 * An OBJECT_REFUSED fact reaches only its own id: it is evidence for a
 * wait on that id or before it, not for the rest of its group.
 *
 * Returns MOQ_OK; MOQ_ERR_INVAL for a NULL argument, a struct_size below
 * the v0 floor, an unknown kind, a RECEIVER_DROP range that ends before
 * it starts or an OBJECT_REFUSED object_id of UINT64_MAX; MOQ_ERR_STALE_HANDLE; MOQ_ERR_WOULD_BLOCK when the fact would
 * demote an absence range and the event queue is full. A refusal changes
 * nothing. A fact for an ended track, or wholly behind the cursor, is
 * accepted and changes nothing.
 */
MOQ_API moq_result_t moq_playback_on_stream_terminal(
    moq_playback_t *pb,
    moq_playback_track_t track,
    const moq_playback_stream_terminal_t *rec,
    uint64_t now_us);

/*
 * Push a subscriber object into playback.
 *
 * Convenience for integrations using moq_sub_poll_object(). Maps
 * subscriber object fields to moq_playback_object_t and calls
 * push_object. Payload and properties remain borrowed for the call;
 * no extra allocation or rcbuf incref beyond what push_object does.
 */
struct moq_sub_object;
MOQ_API moq_result_t moq_playback_push_sub_object(
    moq_playback_t *pb,
    moq_playback_track_t track,
    const struct moq_sub_object *src,
    uint64_t now_us);

/*
 * No more input will come for `track`: release what it holds, in decode
 * order, and end it. From the next tick on a missing unit is not waited for:
 * the step the track's gap_policy takes once a wait is over is taken at
 * once, reported with GAP_DETECTED as usual. A track with no entry yet drops
 * what it holds, each object OBJECT_DROPPED with reason KEYFRAME_WAIT. Once
 * nothing is buffered past the cursor, TRACK_ENDED is reported and the track
 * releases nothing more. QUEUE_PRESSURE feedback still pauses it. Objects
 * pushed meanwhile are placed as usual.
 *
 * Returns MOQ_OK, also for a track that is draining or has ended already;
 * MOQ_ERR_INVAL for a NULL engine; MOQ_ERR_STALE_HANDLE.
 */
MOQ_API moq_result_t moq_playback_drain_track(
    moq_playback_t *pb,
    moq_playback_track_t track);

/*
 * Stop waiting for `track`'s missing units without ending it: from the next
 * tick on, the step its gap_policy takes once a wait is over is taken at
 * once, as for a draining track, and objects pushed meanwhile are placed
 * and released as usual. It ends only by moq_playback_drain_track. For a
 * consumer that must settle what it already holds before it can say no
 * more input will come.
 *
 * Returns as moq_playback_drain_track.
 */
MOQ_API moq_result_t moq_playback_settle_track(
    moq_playback_t *pb,
    moq_playback_track_t track);

/* -- Tick ------------------------------------------------------------ */

/*
 * Advance the engine: release objects at each track's cursor, run gap
 * timers, shed backlog. A known-absent id is passed without a wait or a
 * reset. A missing id that is not known absent is waited on for
 * gap_timeout_us, measured from the first evidence that delivery moved
 * past it (a later object's arrival, or a stream fact reaching it); with no
 * such evidence there is nothing to time out. When the wait ends,
 * GAP_DETECTED names the id and its cause, and the track's gap_policy
 * decides the rest: DISCONTINUITY resumes within the group only at an
 * independent unit, behind a RESET, and otherwise abandons the group;
 * CONTINUE resumes at the next unit without a reset and reports the hole
 * (GAP_CONTINUED). GAP_DETECTED commits first; the step it announces
 * commits when its own event has room, on that tick or a later one, so a
 * one-slot event queue progresses. After that, when the last refused push
 * was for a full buffer and that request is still pending (its track is live and
 * has not ended, and the object is neither buffered nor below a committed
 * cursor), tick makes room for it from the buffered objects of tracks that
 * have no entry yet, oldest first from the track holding the most, each
 * dropped KEYFRAME_WAIT with an event naming its own track. A settled or
 * orphaned request is retired and costs nothing.
 * A track that has an entry is never dropped from, and nothing is dropped
 * unless dropping every eligible object would make room: an object that
 * can never fit costs nothing. Room is priced as the retry will need it:
 * a configuration the refused object would have reused from an object
 * that is dropped is counted again once it is gone. Dropping stops once
 * the retry fits; the drops are enough, not necessarily the fewest. These
 * drops are local buffer policy and record nothing about the objects'
 * existence.
 *
 * A track being drained (moq_playback_drain_track) waits for nothing.
 *
 * Returns MOQ_OK; MOQ_ERR_WOULD_BLOCK when a command or event it must
 * emit has no room. Work committed before that point stands; drain the
 * queues and tick again.
 */
MOQ_API moq_result_t moq_playback_tick(
    moq_playback_t *pb,
    uint64_t now_us);

/*
 * The earliest now_us at which tick has time-driven work: the expiry of a
 * running gap timer, as the last tick started it. A track paused by
 * QUEUE_PRESSURE feedback has none.
 * Work that is ready regardless of time is not a deadline; tick reports it
 * through the queues. Returns UINT64_MAX when no timer runs, for a NULL
 * engine, and for a timer whose expiry cannot be represented. Observes
 * only; changes no state.
 */
MOQ_API uint64_t moq_playback_next_deadline_us(const moq_playback_t *pb);

/* -- Output polling -------------------------------------------------- */

/*
 * Dequeue the oldest command into `out`. out_size must be at least
 * MOQ_PLAYBACK_CMD_V0_SIZE; the library writes min(out_size, its own
 * sizeof) bytes and stamps out->struct_size with that count. The command's
 * refs transfer to the caller, who passes it to moq_playback_cmd_cleanup.
 *
 * Returns MOQ_OK; MOQ_DONE when the queue is empty; MOQ_ERR_INVAL on a NULL
 * argument; MOQ_ERR_ABI_MISMATCH when out_size is below the v0 size. A
 * refused poll dequeues nothing and transfers no ref: a retry with a
 * large enough record returns the same command.
 */
MOQ_API moq_result_t moq_playback_poll_command(
    moq_playback_t *pb,
    moq_playback_cmd_t *out,
    size_t out_size);

/*
 * poll_command for a consumer that queues commands for a later reader. On
 * MOQ_OK the command is dequeued exactly as by poll_command, and *hold
 * names a new hold that keeps what the command was charged with charged
 * to max_buffered_bytes until moq_playback_release_command_hold: the
 * unit's payload and sample table when this is its object's last command,
 * and its configuration copy. The hold keeps its own refs on those, so the
 * command's cleanup and the hold's release may come in either order. Every
 * successful held poll takes a hold, a command with no bytes included.
 *
 * Results, in this order; each refusal dequeues nothing, transfers no ref,
 * takes no hold and leaves *out and *hold untouched:
 *   MOQ_ERR_INVAL         a NULL argument
 *   MOQ_ERR_ABI_MISMATCH  out_size below MOQ_PLAYBACK_CMD_V0_SIZE
 *   MOQ_ERR_UNSUPPORTED   held polls disabled (max_held_commands 0)
 *   MOQ_DONE              no command is queued
 *   MOQ_ERR_UNSUPPORTED   the next command is a sample of an object that
 *                         releases as several commands (multi-sample
 *                         CMAF); poll_command takes it, unchanged
 *   MOQ_ERR_WOULD_BLOCK   max_held_commands holds are active
 *   MOQ_ERR_LIMIT         no hold can ever be taken again: this copy of
 *                         the library has issued every hold identity it can
 *                         (65535 * 2^27 across all its engines)
 *
 * A hold is released only into the engine that made it, while it is
 * alive. Overwriting the only copy of an active hold leaks its charge
 * until the engine is destroyed, which ends every hold.
 */
MOQ_API moq_result_t moq_playback_poll_command_held(
    moq_playback_t *pb,
    moq_playback_cmd_t *out,
    size_t out_size,
    moq_playback_command_hold_t *hold);

/* End a hold: its bytes leave the budget, and its configuration copy loses
 * this holder (its charge ends with its last holder of any kind). A NULL
 * engine, an invalid handle, another engine's, and one already released
 * (through any copy) are no-ops: no later hold is ever released by an old
 * handle. Hold identities are issued once each by this copy of the
 * library, across all its engines, so this holds across engine teardown
 * and recreation; a separately loaded copy of the library, or another
 * process, is a separate domain. Taking a hold is safe across threads for
 * different engines, but not from a signal handler. */
MOQ_API void moq_playback_release_command_hold(moq_playback_t *pb,
                                               moq_playback_command_hold_t hold);

/* Bytes active holds keep charged: their payload and sample-table bytes,
 * plus each configuration copy a hold holds, counted once however many
 * hold it and whatever else (a track, a queued command) holds it too.
 * Budget accounting, not allocator usage. 0 for NULL. Observes only. */
MOQ_API uint64_t moq_playback_command_hold_bytes(const moq_playback_t *pb);

/* Structural helpers only: validity does not imply a live hold. */
MOQ_API bool     moq_playback_command_hold_is_valid(moq_playback_command_hold_t h);
MOQ_API bool     moq_playback_command_hold_eq(moq_playback_command_hold_t a,
                                              moq_playback_command_hold_t b);
MOQ_API uint64_t moq_playback_command_hold_hash(moq_playback_command_hold_t h);
MOQ_API uint64_t moq_playback_command_hold_id_for_trace(moq_playback_command_hold_t h);

/*
 * Dequeue the oldest event, with the same size contract as
 * poll_command against MOQ_PLAYBACK_EVENT_V0_SIZE. Events own nothing.
 */
MOQ_API moq_result_t moq_playback_poll_event(
    moq_playback_t *pb,
    moq_playback_event_t *out,
    size_t out_size);

/* -- Decoder feedback ------------------------------------------------ */

MOQ_API moq_result_t moq_playback_handle_feedback(
    moq_playback_t *pb,
    const moq_playback_feedback_t *feedback,
    uint64_t now_us);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PLAYBACK_H */
