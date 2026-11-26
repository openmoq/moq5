/* The LOCMAF tier (`locmaf` feature): the LOCMAF encoder and decoder,
 * generated apart from the core and media tiers. Everything it references
 * from those tiers (the byte span, the result code, the parsed CMAF Header
 * record) is in their bindings. As in wrapper_core.h, the values bindgen
 * cannot evaluate are restated as MOQ5_SYSVAL_ statics whose prefix the
 * build script strips. */

#include <moq/locmaf.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* The offsetof-based extents of the sized LOCMAF records. */
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_TRACK_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_CHUNK_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_DECODER_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_DECODER_STATS_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_LOCMAF_ENCODER_STATS_V0_SIZE);

#undef MOQ5_SYSVAL
