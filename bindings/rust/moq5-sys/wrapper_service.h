/* The service tier (`service` feature): the managed endpoint, media sender
 * and media receiver, generated apart from the core and media tiers.
 * Everything they reference from those tiers is in their bindings. As in
 * wrapper_core.h, the values bindgen cannot evaluate are restated as
 * MOQ5_SYSVAL_ statics whose prefix the build script strips. */

#include <moq/endpoint.h>
#include <moq/media_auth.h>
#include <moq/media_sender.h>
#include <moq/media_receiver.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* Modes and a floor spelled with UINT32_C(), and a bound spelled with a cast. */
MOQ5_SYSVAL(uint32_t, MOQ_ENDPOINT_URI_LEGACY);
MOQ5_SYSVAL(uint32_t, MOQ_ENDPOINT_URI_STANDARD);
MOQ5_SYSVAL(uint64_t, MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US);
MOQ5_SYSVAL(uint32_t, MOQ_ENDPOINT_IDLE_TIMEOUT_MIN_MS);

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

#undef MOQ5_SYSVAL
