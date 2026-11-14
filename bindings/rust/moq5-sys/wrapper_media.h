/* The media tier (`media` feature): the LOC, CMAF, media-object and MSF
 * parsers the safe crates wrap, generated apart from the core. Everything
 * they reference from the core is in the core bindings. As in
 * wrapper_core.h, the values bindgen cannot evaluate are restated as
 * MOQ5_SYSVAL_ statics whose prefix the build script strips. */

#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/media_object.h>
#include <moq/msf.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* Open sets and modes spelled with UINT32_C(). */
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_AUTO);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_NONE);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_UNIX_US);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_TIMESTAMP);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_TIMESCALE);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_MARKING);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_AUDIO_LEVEL);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_VIDEO_CONFIG);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_AUDIO_CONFIG);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_PRESENT_MASK);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_EXT_HAS_CODEC_STRING);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_EXT_PRESENT_MASK);

/* Frozen v0 floors of the sized media records. */
MOQ5_SYSVAL(size_t, MOQ_MEDIA_TRACK_INFO_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_CMAF_INIT_INFO_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_METADATA_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOC_METADATA_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOC_EXT_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MSF_MPEG2TS_V0_SIZE);

#undef MOQ5_SYSVAL
