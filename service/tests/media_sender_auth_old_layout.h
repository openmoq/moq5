#ifndef MEDIA_SENDER_AUTH_OLD_LAYOUT_H
#define MEDIA_SENDER_AUTH_OLD_LAYOUT_H

#include <moq/media_sender.h>
#include <stddef.h>

/* Independent pre-auth declarations, including enum width under short-enums.
 * Keep this replica frozen when the public declarations grow. This header is
 * also a compile-only oracle for LP64, ARM32 and i386 target probes. */
typedef enum sender_old_backpressure {
    SENDER_OLD_BP_UNSET = 0,
    SENDER_OLD_BP_DROP_TO_KEYFRAME,
    SENDER_OLD_BP_DROP_GROUP,
    SENDER_OLD_BP_BLOCK_TIMEOUT,
    SENDER_OLD_BP_RETURN_WOULD_BLOCK
} sender_old_backpressure_t;

typedef struct sender_old_callbacks {
    uint32_t struct_size;
    void *ctx;
    void (*on_subscriber_joined)(void *, moq_media_sender_t *, moq_media_track_t *, size_t);
    void (*on_subscriber_left)(void *, moq_media_sender_t *, moq_media_track_t *, size_t);
    void (*on_ready)(void *, moq_media_sender_t *);
    void (*on_closed)(void *, moq_media_sender_t *, bool, uint64_t);
    void (*on_track_closed)(void *, moq_media_sender_t *, moq_media_track_t *);
} sender_old_callbacks_t;

typedef uint32_t sender_old_companion_t;
#define SENDER_OLD_COMPANION_LEGACY 0u
#define SENDER_OLD_COMPANION_D22 1u
#define SENDER_OLD_COMPANION_AUTO 2u

typedef struct sender_old_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    sender_old_backpressure_t backpressure;
    uint64_t block_timeout_us;
    uint32_t queue_max_objects;
    uint32_t queue_max_bytes;
    uint32_t pre_ready_max_objects;
    uint32_t pre_ready_max_bytes;
    bool validate_cmaf;
    const moq_cmsf_content_protection_t *content_protections;
    size_t content_protection_count;
    sender_old_callbacks_t callbacks;
    bool publish_tracks;
    bool drop_without_demand;
    uint64_t catalog_refresh_interval_us;
    sender_old_companion_t companion_profile;
} sender_old_cfg_t;

#define SENDER_OLD_OFFSET(field) \
    _Static_assert(offsetof(sender_old_cfg_t, field) == \
                   offsetof(moq_media_sender_cfg_t, field), \
                   "pre-auth sender offset changed: " #field)
SENDER_OLD_OFFSET(struct_size);
SENDER_OLD_OFFSET(endpoint);
SENDER_OLD_OFFSET(namespace_);
SENDER_OLD_OFFSET(catalog_track);
SENDER_OLD_OFFSET(backpressure);
SENDER_OLD_OFFSET(block_timeout_us);
SENDER_OLD_OFFSET(queue_max_objects);
SENDER_OLD_OFFSET(queue_max_bytes);
SENDER_OLD_OFFSET(pre_ready_max_objects);
SENDER_OLD_OFFSET(pre_ready_max_bytes);
SENDER_OLD_OFFSET(validate_cmaf);
SENDER_OLD_OFFSET(content_protections);
SENDER_OLD_OFFSET(content_protection_count);
SENDER_OLD_OFFSET(callbacks);
SENDER_OLD_OFFSET(publish_tracks);
SENDER_OLD_OFFSET(drop_without_demand);
SENDER_OLD_OFFSET(catalog_refresh_interval_us);
SENDER_OLD_OFFSET(companion_profile);
#undef SENDER_OLD_OFFSET

_Static_assert(sizeof(sender_old_callbacks_t) == sizeof(moq_media_sender_callbacks_t),
               "pre-auth callback allocation changed");
_Static_assert(_Alignof(sender_old_cfg_t) == _Alignof(moq_media_sender_cfg_t),
               "pre-auth sender alignment changed");
_Static_assert(offsetof(moq_media_sender_cfg_t, resource_auth) >= sizeof(sender_old_cfg_t),
               "resource_auth overlaps complete pre-auth sender allocation");

#endif
