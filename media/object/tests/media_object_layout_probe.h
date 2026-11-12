#ifndef MOQ_MEDIA_OBJECT_LAYOUT_PROBE_H
#define MOQ_MEDIA_OBJECT_LAYOUT_PROBE_H

#include <moq/media_object.h>
#ifdef __cplusplus
#define MEDIA_ASSERT static_assert
#define MEDIA_ALIGN(T) alignof(T)
#else
#define MEDIA_ASSERT _Static_assert
#define MEDIA_ALIGN(T) _Alignof(T)
#endif

/* Exact baseline declarations, with no current tail/pointer alignment. */
typedef struct media_old_track {
    uint32_t struct_size;
    moq_media_type_t media_type;
    moq_media_packaging_t packaging;
    uint32_t timescale;
    moq_version_t transport_version;
} media_old_track_t;

typedef struct media_old_parsed {
    uint32_t struct_size;
    moq_media_packaging_t packaging;
    moq_object_status_t status;
    bool end_of_group;
    bool datagram;
    bool has_capture_time;
    uint64_t capture_time_us;
    uint64_t decode_time_us;
    int64_t composition_offset_us;
    uint64_t presentation_time_us;
    bool keyframe;
    moq_bytes_t payload;
    moq_bytes_t fragment;
    size_t mdat_offset;
    size_t mdat_len;
    size_t sample_count;
    const moq_cmaf_sample_t *samples;
    uint32_t sample_duration_us;
    moq_sap_type_t sap_type;
    uint32_t chunk_count;
    uint32_t track_id;
} media_old_parsed_t;

/* The parsed object through metadata and the parser context through
 * mpeg2ts_packet_size, declared on their own: the complete layouts before
 * the LOC-05 tails. */
typedef struct media_through_metadata_parsed {
    media_old_parsed_t base;
    moq_media_metadata_t metadata;
} media_through_metadata_parsed_t;

typedef struct media_old_parse_cfg {
    uint32_t struct_size;
    uint32_t companion_profile;
    const moq_loc_metadata_t *track_loc_metadata;
    uint32_t mpeg2ts_packet_size;
} media_old_parse_cfg_t;

#define MEDIA_OLD_OFFSET(T, OLD, field) \
    MEDIA_ASSERT(offsetof(T, field) == offsetof(OLD, field), "old offset " #field)
#define MEDIA_PIN_TRACK(f) MEDIA_OLD_OFFSET(moq_media_track_info_t, media_old_track_t, f)
MEDIA_PIN_TRACK(struct_size);
MEDIA_PIN_TRACK(media_type);
MEDIA_PIN_TRACK(packaging);
MEDIA_PIN_TRACK(timescale);
MEDIA_PIN_TRACK(transport_version);
#define MEDIA_PIN_PARSED(f) MEDIA_OLD_OFFSET(moq_media_parsed_object_t, media_old_parsed_t, f)
MEDIA_PIN_PARSED(struct_size);
MEDIA_PIN_PARSED(packaging);
MEDIA_PIN_PARSED(status);
MEDIA_PIN_PARSED(end_of_group);
MEDIA_PIN_PARSED(datagram);
MEDIA_PIN_PARSED(has_capture_time);
MEDIA_PIN_PARSED(capture_time_us);
MEDIA_PIN_PARSED(decode_time_us);
MEDIA_PIN_PARSED(composition_offset_us);
MEDIA_PIN_PARSED(presentation_time_us);
MEDIA_PIN_PARSED(keyframe);
MEDIA_PIN_PARSED(payload);
MEDIA_PIN_PARSED(fragment);
MEDIA_PIN_PARSED(mdat_offset);
MEDIA_PIN_PARSED(mdat_len);
MEDIA_PIN_PARSED(sample_count);
MEDIA_PIN_PARSED(samples);
MEDIA_PIN_PARSED(sample_duration_us);
MEDIA_PIN_PARSED(sap_type);
MEDIA_PIN_PARSED(chunk_count);
MEDIA_PIN_PARSED(track_id);
MEDIA_ASSERT(sizeof(media_old_track_t) == sizeof(moq_media_track_info_t),
             "complete old track extent unchanged");
MEDIA_ASSERT(MEDIA_ALIGN(media_old_track_t) == MEDIA_ALIGN(moq_media_track_info_t),
             "old track alignment unchanged");
MEDIA_ASSERT(sizeof(media_old_parsed_t) == MOQ_MEDIA_PARSED_OBJECT_V0_SIZE,
             "complete old parsed floor, including padding");
MEDIA_ASSERT(MEDIA_ALIGN(media_old_parsed_t) == MEDIA_ALIGN(moq_media_parsed_object_t),
             "old parsed alignment unchanged");
MEDIA_ASSERT(sizeof(moq_media_companion_profile_t) == 4 && sizeof(moq_media_time_domain_t) == 4,
             "fixed companion and clock widths");
MEDIA_ASSERT(offsetof(moq_media_object_parse_cfg_t, struct_size) == 0 &&
             offsetof(moq_media_object_parse_cfg_t, companion_profile) == 4 &&
             offsetof(moq_media_object_parse_cfg_t, track_loc_metadata) == 8,
             "parser context fields have fixed offsets");
MEDIA_ASSERT(MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE == 8,
             "parser context floor ends before its optional pointer");
MEDIA_ASSERT(offsetof(moq_media_object_parse_cfg_t, mpeg2ts_packet_size) ==
             (sizeof(void *) == 8 ? 16 : 12),
             "packet size follows the whole metadata pointer");
MEDIA_ASSERT(sizeof(media_old_parse_cfg_t) == (sizeof(void *) == 8 ? 24 : 16),
             "parser context complete extent before track_loc_ext");
MEDIA_ASSERT(offsetof(moq_media_object_parse_cfg_t, track_loc_ext) ==
             sizeof(media_old_parse_cfg_t),
             "track_loc_ext starts at the complete extent through mpeg2ts_packet_size");
MEDIA_ASSERT(offsetof(moq_media_object_parse_cfg_t, catalog_codec) ==
             (sizeof(void *) == 8 ? 32 : 20),
             "catalog_codec follows the whole track_loc_ext pointer");
MEDIA_ASSERT(sizeof(moq_media_object_parse_cfg_t) == (sizeof(void *) == 8 ? 48 : 28),
             "parser context complete extent");
MEDIA_ASSERT(MOQ_MEDIA_METADATA_V0_SIZE == sizeof(moq_media_metadata_t), "full metadata floor");
MEDIA_ASSERT(offsetof(media_through_metadata_parsed_t, metadata) ==
             offsetof(moq_media_parsed_object_t, metadata),
             "metadata keeps its offset");
MEDIA_ASSERT(MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE ==
             sizeof(media_through_metadata_parsed_t),
             "the D22 floor is the complete extent through metadata");
MEDIA_ASSERT(offsetof(moq_media_parsed_object_t, loc_ext_present) ==
             sizeof(media_through_metadata_parsed_t),
             "the LOC-05 block starts at the sizeof through metadata");
MEDIA_ASSERT(MOQ_MEDIA_PARSED_OBJECT_LOC_EXT_SIZE == sizeof(moq_media_parsed_object_t),
             "full current parsed floor");
#endif
