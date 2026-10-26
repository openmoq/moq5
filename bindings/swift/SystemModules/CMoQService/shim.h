#ifndef MOQ_SERVICE_SHIM_H
#define MOQ_SERVICE_SHIM_H

/* The installed C service tier (pkg-config: libmoq-service), built with
 * the playback engine (the media player). The service headers pull
 * types.h/session.h/media_object.h/cmaf.h/msf.h/playback.h transitively;
 * rcbuf.h is included for the sender's payload buffers. */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/media_player.h>
#include <moq/rcbuf.h>

/* Swift does not import these UINT32_C macros. Keep their mapping in C so the
 * binding follows the SDK constants without publishing any library symbols. */
static inline moq_media_companion_profile_t moq_swift_media_companion_auto(void)
{
    return MOQ_MEDIA_COMPANION_AUTO;
}

static inline moq_media_companion_profile_t moq_swift_media_companion_legacy(void)
{
    return MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
}

static inline moq_media_companion_profile_t moq_swift_media_companion_d22_public(void)
{
    return MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
}

static inline uint32_t moq_swift_loc_metadata_has_timestamp(void)
{
    return MOQ_LOC_METADATA_HAS_TIMESTAMP;
}

static inline uint32_t moq_swift_loc_metadata_has_timescale(void)
{
    return MOQ_LOC_METADATA_HAS_TIMESCALE;
}

static inline uint32_t moq_swift_loc_metadata_has_marking(void)
{
    return MOQ_LOC_METADATA_HAS_MARKING;
}

static inline uint32_t moq_swift_loc_metadata_has_audio_level(void)
{
    return MOQ_LOC_METADATA_HAS_AUDIO_LEVEL;
}

static inline uint32_t moq_swift_loc_metadata_has_video_config(void)
{
    return MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
}

static inline uint32_t moq_swift_loc_metadata_has_audio_config(void)
{
    return MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
}

static inline moq_media_time_domain_t moq_swift_media_time_domain_none(void)
{
    return MOQ_MEDIA_TIME_DOMAIN_NONE;
}

static inline moq_media_time_domain_t moq_swift_media_time_domain_unix_us(void)
{
    return MOQ_MEDIA_TIME_DOMAIN_UNIX_US;
}

static inline moq_media_time_domain_t moq_swift_media_time_domain_application_media(void)
{
    return MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA;
}

#endif
