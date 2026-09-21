/* Independent authority for the sender configuration initializer.
 *
 * This program links the REAL service SDK, so the test fixture's mirror of
 * moq_media_sender_cfg_init_sized can be compared against the implementation
 * the binding actually ships against. Every value is derived from C; nothing
 * about the layout is assumed by the reader. */

#include <moq/media_sender.h>

#include <stddef.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    moq_media_sender_cfg_t cfg;
    memset(&cfg, 0xA5, sizeof(cfg));
    moq_media_sender_cfg_init_sized(&cfg, sizeof(cfg));

    printf("cfg_size %zu\n", sizeof(cfg));
    printf("callbacks_size %zu\n", sizeof(cfg.callbacks));
    printf("callbacks_offset %zu\n", offsetof(moq_media_sender_cfg_t, callbacks));
    printf("cfg_struct_size %lu\n", (unsigned long)cfg.struct_size);
    printf("callbacks_struct_size %lu\n", (unsigned long)cfg.callbacks.struct_size);
    printf("validate_cmaf %d\n", cfg.validate_cmaf ? 1 : 0);
    printf("backpressure %d\n", (int)cfg.backpressure);
    printf("callbacks_pointers_null %d\n",
           (cfg.callbacks.ctx == NULL && cfg.callbacks.on_subscriber_joined == NULL &&
            cfg.callbacks.on_subscriber_left == NULL && cfg.callbacks.on_ready == NULL &&
            cfg.callbacks.on_closed == NULL && cfg.callbacks.on_track_closed == NULL) ? 1 : 0);
    printf("content_protections_null %d\n",
           (cfg.content_protections == NULL && cfg.content_protection_count == 0) ? 1 : 0);

    /* The track configuration the send-track slice declares against. */
    moq_media_track_cfg_t t;
    memset(&t, 0xA5, sizeof(t));
    moq_media_track_cfg_init(&t);
    printf("track_cfg_size %zu\n", sizeof(t));
    printf("track_struct_size %lu\n", (unsigned long)t.struct_size);
    printf("track_is_live %d\n", t.is_live ? 1 : 0);
    printf("track_media_type %d\n", (int)t.media_type);
    printf("track_packaging %d\n", (int)t.packaging);
    printf("track_timescale %lu\n", (unsigned long)t.timescale);
    printf("track_bitrate %llu\n", (unsigned long long)t.bitrate);
    printf("track_has_track_duration %d\n", t.has_track_duration ? 1 : 0);
    printf("track_has_max_grp_sap %d\n", t.has_max_grp_sap ? 1 : 0);
    printf("track_emit_sap_timeline %d\n", t.emit_sap_timeline ? 1 : 0);
    printf("track_emit_media_timeline %d\n", t.emit_media_timeline ? 1 : 0);
    printf("track_has_alt_group %d\n", t.has_alt_group ? 1 : 0);
    printf("track_content_protection_ref_id_count %zu\n",
           t.content_protection_ref_id_count);
    return 0;
}
