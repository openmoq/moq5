/* The player tier (`player` feature), generated apart from the lower
 * tiers: the player's declarations and the playback engine configuration
 * it takes. Everything they reference is in the lower tiers' bindings. As
 * in wrapper_core.h, the values bindgen cannot evaluate are restated as
 * MOQ5_SYSVAL_ statics whose prefix the build script strips. */

#include <moq/playback.h>
#include <moq/media_player.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* Frozen v0 floors of the sized records the player reads and writes. */
MOQ5_SYSVAL(size_t, MOQ_MEDIA_PLAYER_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_MEDIA_PLAYER_STATS_V0_SIZE);
MOQ5_SYSVAL(size_t, MOQ_PLAYBACK_CFG_V0_SIZE);

#undef MOQ5_SYSVAL
