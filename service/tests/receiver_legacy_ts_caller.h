#ifndef MOQ_TEST_RECEIVER_LEGACY_TS_CALLER_H
#define MOQ_TEST_RECEIVER_LEGACY_TS_CALLER_H

#include <moq/media_receiver.h>

/* The complete MPEG2TS-era config, including that layout's tail padding. */
typedef struct ts_receiver_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
    uint8_t reserved_cfg[4];
    uint32_t mpeg2ts_reorder_slots;
    uint64_t mpeg2ts_reorder_window_us;
    bool mpeg2ts_strip_prefix;
    bool mpeg2ts_join_psi;
} ts_receiver_cfg_t;

/* Returns caller-side assertion failures; probe owns implementation checks. */
int moq_test_receiver_legacy_ts_caller(moq_namespace_t namespace_,
                                       void (*probe)(const void *));

#endif
