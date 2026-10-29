#ifndef MOQ_TS_DESC_LAYOUT_PROBE_H
#define MOQ_TS_DESC_LAYOUT_PROBE_H
#include <moq/media_receiver.h>
#include "../../media/object/tests/ts_layout_probe.h"
typedef struct ts_old_desc {
    uint32_t    struct_size;
    moq_bytes_t name;
    moq_bytes_t role;
    moq_bytes_t codec;
    moq_bytes_t lang;
    moq_bytes_t label;
    ts_old_track_t info;
    moq_bytes_t codec_config;
    moq_bytes_t init_data;
    const moq_cmaf_init_info_t *cmaf;
    bool        has_width;
    uint32_t    width;
    bool        has_height;
    uint32_t    height;
    bool        has_samplerate;
    uint32_t    samplerate;
    moq_bytes_t channel_config;
    bool        has_framerate;
    uint64_t    framerate_millis;
    bool        has_bitrate;
    uint64_t    bitrate;
    bool        has_max_grp_sap;
    uint32_t    max_grp_sap;
    bool        has_max_obj_sap;
    uint32_t    max_obj_sap;
    const moq_bytes_t *content_protection_ref_ids;
    size_t             content_protection_ref_id_count;
    moq_bytes_t packaging;
    moq_bytes_t event_type;
    moq_bytes_t mime_type;
    const moq_bytes_t *depends;
    size_t             depends_count;
    bool                     has_template;
    moq_msf_media_template_t template_;
    bool                     has_track_duration;
    uint64_t                 track_duration_ms;
    bool                     is_live;
} ts_old_desc_t;
_Static_assert(offsetof(ts_old_desc_t, codec_config) == offsetof(moq_media_track_desc_t, codec_config), "embedded info moved codec_config");
_Static_assert(offsetof(ts_old_desc_t, cmaf) == offsetof(moq_media_track_desc_t, cmaf), "descriptor moved the cmaf record pointer");
_Static_assert(offsetof(ts_old_desc_t, is_live) == offsetof(moq_media_track_desc_t, is_live), "descriptor prefix moved");
_Static_assert(MOQ_MEDIA_TRACK_DESC_V0_SIZE == offsetof(ts_old_desc_t, is_live) + sizeof(bool), "descriptor V0 floor moved");
_Static_assert(offsetof(moq_media_track_desc_t, extensions) >= sizeof(ts_old_desc_t), "extensions overlap original descriptor");
/* Independent frozen LP64 ordinary-enum pins. Short-enum mode
 * has an enum-to-fixed-width migration; it uses consistency mirrors
 * above, not a claim that old short-enum binaries retain their layout. */
_Static_assert(sizeof(void *) != 8 || sizeof(moq_media_type_t) != 4 ||
               (sizeof(ts_old_track_t) == 20 &&
                offsetof(moq_media_track_desc_t, codec_config) == 112 &&
                offsetof(moq_media_track_desc_t, cmaf) == 144 &&
                offsetof(moq_media_track_desc_t, is_live) == 408 &&
                MOQ_MEDIA_TRACK_DESC_V0_SIZE == 409 &&
                sizeof(ts_old_desc_t) == 416), "LP64 descriptor ABI");
/* Independent pre-TS layouts: do not derive the old sizeof from new tails. */
typedef struct {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
} receiver_cfg_before_ts_t;
typedef struct {
    uint32_t struct_size;
    moq_media_start_mode_t start;
    bool has_priority;
    uint8_t priority;
} subscribe_cfg_before_ts_t;
_Static_assert(MOQ_MEDIA_RECEIVER_CFG_V0_SIZE == sizeof(receiver_cfg_before_ts_t),
               "receiver init must retain the original padded sizeof");
_Static_assert(MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE ==
               sizeof(subscribe_cfg_before_ts_t),
               "subscribe init must retain the original padded sizeof");
typedef struct ts_old_object {
    uint32_t              struct_size;
    moq_media_track_t    *track;
    uint32_t              config_generation;
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
    moq_bytes_t           payload;
    moq_bytes_t           fragment;
    size_t                mdat_offset;
    size_t                mdat_len;
    size_t                   sample_count;
    const moq_cmaf_sample_t *samples;
    moq_rcbuf_t          *payload_ref;
    moq_rcbuf_t          *properties_ref;
    moq_cmaf_sample_t    *samples_owned;
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority;
    bool                  has_subgroup;
} ts_old_object_t;
_Static_assert(offsetof(moq_media_object_t, has_loss_report) >= sizeof(ts_old_object_t),
               "loss report overlaps original object padding");
/* The track event through the delivery-failure block, declared on its own:
 * the fill loss summary starts at this layout's complete sizeof, never in
 * its tail padding, on every data model and enum width. */
typedef struct ts_event_before_fill_loss {
    uint32_t                      struct_size;
    moq_media_track_event_kind_t  kind;
    moq_media_track_t            *track;
    const moq_media_track_desc_t *desc;
    uint32_t                      config_generation;
    uint8_t                       reserved_track_ev[4];
    bool                          has_largest;
    uint64_t                      largest_group;
    uint64_t                      largest_object;
    bool                          has_expires;
    uint64_t                      expires_ms;
    moq_media_parse_drop_class_t  parse_drop_class;
    uint64_t                      parse_drops_total;
    uint64_t                      parse_drops_delta;
    int32_t                       local_result;
    uint32_t                      reserved_local_failure;
    uint64_t                      initiating_request_id;
    uint32_t                      fill_reason;
    uint32_t                      fill_error_domain;
    uint64_t                      fill_error_code;
    uint32_t                      delivery_origin;
    uint32_t                      reserved_delivery;
    uint64_t                      delivery_initiating_request_id;
} ts_event_before_fill_loss_t;
_Static_assert(offsetof(moq_media_track_event_t, delivery_initiating_request_id) ==
               offsetof(ts_event_before_fill_loss_t, delivery_initiating_request_id),
               "delivery failure block moved");
_Static_assert(offsetof(moq_media_track_event_t, fill_unreported_loss_causes) ==
               sizeof(ts_event_before_fill_loss_t),
               "fill loss summary must start at the complete preceding sizeof");
_Static_assert(offsetof(moq_media_track_event_t, has_done_status) ==
               sizeof(ts_event_before_fill_loss_t) + 24,
               "fill loss summary is 24 bytes with no padding");
#endif
