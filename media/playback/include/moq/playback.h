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
 */

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

/* -- Enums ----------------------------------------------------------- */

typedef enum moq_playback_media_type {
    MOQ_PLAYBACK_MEDIA_VIDEO = 1,
    MOQ_PLAYBACK_MEDIA_AUDIO = 2,
} moq_playback_media_type_t;

typedef enum moq_playback_packaging {
    MOQ_PLAYBACK_PACKAGING_RAW  = 1,
    MOQ_PLAYBACK_PACKAGING_CMAF = 2,
} moq_playback_packaging_t;

typedef enum moq_playback_cmd_kind {
    MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO = 1,
    MOQ_PLAYBACK_CMD_DECODE_VIDEO    = 2,
    MOQ_PLAYBACK_CMD_DECODE_CMAF     = 3,
    MOQ_PLAYBACK_CMD_RESET           = 4,
    MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO = 5,
    MOQ_PLAYBACK_CMD_DECODE_AUDIO    = 6,
} moq_playback_cmd_kind_t;

typedef enum moq_playback_reset_reason {
    MOQ_PLAYBACK_RESET_GAP           = 1,
    MOQ_PLAYBACK_RESET_DECODE_ERROR  = 2,
    MOQ_PLAYBACK_RESET_TRACK_SWITCH  = 3,
    MOQ_PLAYBACK_RESET_CONFIG_CHANGE = 4,
} moq_playback_reset_reason_t;

typedef enum moq_playback_event_kind {
    MOQ_PLAYBACK_EVENT_GAP_DETECTED           = 1,
    MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING       = 2,
    MOQ_PLAYBACK_EVENT_SKIP_FORWARD           = 3,
    MOQ_PLAYBACK_EVENT_OBJECT_DROPPED         = 4,
    MOQ_PLAYBACK_EVENT_TRACK_ENDED            = 5,
    MOQ_PLAYBACK_EVENT_BACKLOG_SHED           = 6,
    MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED = 7,
} moq_playback_event_kind_t;

typedef enum moq_playback_drop_reason {
    MOQ_PLAYBACK_DROP_MALFORMED_LOC            = 1,
    MOQ_PLAYBACK_DROP_MALFORMED_CMAF           = 2,
    MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP        = 3,
    MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS        = 4,
    MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE = 5,
    MOQ_PLAYBACK_DROP_STALE                    = 6,
    MOQ_PLAYBACK_DROP_KEYFRAME_WAIT            = 7,
} moq_playback_drop_reason_t;

typedef enum moq_playback_feedback_kind {
    MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE = 1,
    MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR   = 2,
} moq_playback_feedback_kind_t;

/* -- Config ---------------------------------------------------------- */

typedef struct moq_playback_cfg {
    uint32_t struct_size;
    uint32_t max_tracks;             /* default 4 */
    uint32_t max_buffered_objects;   /* jitter buffer entry cap; default 256 */
    uint64_t max_buffered_bytes;     /* retained payload byte budget; default 16 MiB */
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
} moq_playback_cfg_t;

MOQ_API void moq_playback_cfg_init(moq_playback_cfg_t *cfg);

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
} moq_playback_track_cfg_t;

MOQ_API void moq_playback_track_cfg_init(moq_playback_track_cfg_t *cfg);

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

MOQ_API void moq_playback_object_init(moq_playback_object_t *obj);

/* -- Commands -------------------------------------------------------- */

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

MOQ_API void moq_playback_cmd_cleanup(moq_playback_cmd_t *cmd);

/* -- Events ---------------------------------------------------------- */

typedef struct moq_playback_event {
    uint32_t                    struct_size;
    moq_playback_event_kind_t   kind;
    moq_playback_track_t        track;

    union {
        struct {
            uint64_t group_id;
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
    } u;
} moq_playback_event_t;

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

MOQ_API void moq_playback_feedback_init(moq_playback_feedback_t *fb);

/* -- Lifecycle ------------------------------------------------------- */

MOQ_API moq_result_t moq_playback_create(
    const moq_alloc_t *alloc,
    const moq_playback_cfg_t *cfg,
    moq_playback_t **out);

MOQ_API void moq_playback_destroy(moq_playback_t *pb);

/* -- Track management ------------------------------------------------ */

MOQ_API moq_result_t moq_playback_add_track(
    moq_playback_t *pb,
    const moq_playback_track_cfg_t *cfg,
    moq_playback_track_t *out);

MOQ_API moq_result_t moq_playback_remove_track(
    moq_playback_t *pb,
    moq_playback_track_t track);

/* -- Object input ---------------------------------------------------- */

MOQ_API moq_result_t moq_playback_push_object(
    moq_playback_t *pb,
    const moq_playback_object_t *obj,
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

/* -- Tick ------------------------------------------------------------ */

MOQ_API moq_result_t moq_playback_tick(
    moq_playback_t *pb,
    uint64_t now_us);

/* -- Output polling -------------------------------------------------- */

MOQ_API moq_result_t moq_playback_poll_command(
    moq_playback_t *pb,
    moq_playback_cmd_t *out);

MOQ_API moq_result_t moq_playback_poll_event(
    moq_playback_t *pb,
    moq_playback_event_t *out);

/* -- Decoder feedback ------------------------------------------------ */

MOQ_API moq_result_t moq_playback_handle_feedback(
    moq_playback_t *pb,
    const moq_playback_feedback_t *feedback,
    uint64_t now_us);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PLAYBACK_H */
