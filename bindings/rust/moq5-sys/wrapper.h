/* The headers moq5-sys binds, and the values bindgen cannot read from them.
 *
 * bindgen turns `#define NAME 1u` into a constant but cannot evaluate
 * `UINT32_C(n)`, `((uint64_t)INT64_MAX)` or the offsetof-based v0 floors.
 * Each of those is restated here as a `static const` whose initializer libclang
 * evaluates; the build script strips the MOQ5_SYSVAL_ prefix, so the generated
 * crate exposes them under their C names. A name missing from the installed
 * headers fails the build: the SDK is older than this crate requires. */

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/version.h>
#include <moq/endpoint.h>
#include <moq/cmaf.h>
#include <moq/msf.h>
#include <moq/media_object.h>
#include <moq/media_sender.h>
#include <moq/media_receiver.h>
/* Only for the object status values below; its own declarations are left
 * out of the generated crate. */
#include <moq/control.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* The object statuses a received object reports (moq_object_status_t). */
MOQ5_SYSVAL(uint32_t, MOQ_OBJECT_STATUS_NORMAL);
MOQ5_SYSVAL(uint32_t, MOQ_OBJECT_STATUS_END_OF_GROUP);
MOQ5_SYSVAL(uint32_t, MOQ_OBJECT_STATUS_END_OF_TRACK);

/* Open sets and modes spelled with UINT32_C(). */
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_COMPANION_AUTO);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_NONE);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_UNIX_US);
MOQ5_SYSVAL(uint32_t, MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA);
MOQ5_SYSVAL(uint32_t, MOQ_ENDPOINT_URI_LEGACY);
MOQ5_SYSVAL(uint32_t, MOQ_ENDPOINT_URI_STANDARD);
MOQ5_SYSVAL(uint64_t, MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_TIMESTAMP);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_TIMESCALE);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_MARKING);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_AUDIO_LEVEL);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_VIDEO_CONFIG);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_HAS_AUDIO_CONFIG);
MOQ5_SYSVAL(uint32_t, MOQ_LOC_METADATA_PRESENT_MASK);

/* The core publisher-property builder's offsetof-based frozen input floor. */
MOQ5_SYSVAL(size_t, MOQ_TRACK_DELIVERY_TIMEOUTS_CFG_V0_SIZE);

/* Frozen v0 floors of the sized records the service tier reads and writes. */
MOQ5_SYSVAL(size_t, MOQ_ENDPOINT_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_ENDPOINT_TERMINAL_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_AUTH_REQUEST_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_AUTH_OUTCOME_INFO_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_AUTH_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_TRACK_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_SEND_OBJECT_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_RECEIVER_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_TRACK_DESC_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_RECEIVER_INFO_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_TRACK_INFO_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_METADATA_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOC_METADATA_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MSF_MPEG2TS_V0_SIZE);

#undef MOQ5_SYSVAL
