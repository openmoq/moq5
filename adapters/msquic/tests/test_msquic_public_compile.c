/* The public headers must stand alone as C. */

#include <stddef.h>
#include <string.h>

#include <moq/msquic.h>
#ifdef MOQ_HAVE_MSQUIC_MANAGED
#include <moq/msquic_managed.h>
#endif

int main(void)
{
    moq_msquic_conn_cfg_t cfg;
    QUIC_SETTINGS settings;

    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    moq_msquic_settings_init(&settings);
#ifdef MOQ_HAVE_MSQUIC_MANAGED
    moq_msquic_managed_cfg_t mcfg;

    /* an older caller's frozen v0 prefix (everything before the
     * appended fields) must initialize: prefix zeroed, struct_size
     * stamped to the prefix size */
    memset(&mcfg, 0xff, sizeof(mcfg));
    moq_msquic_managed_cfg_init_sized(
        &mcfg, offsetof(moq_msquic_managed_cfg_t, version));
    if (mcfg.struct_size !=
        (uint32_t)offsetof(moq_msquic_managed_cfg_t, version))
        return 1;
    if (mcfg.on_lane_pump != NULL) /* prefix field really zeroed */
        return 1;

    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    mcfg.version = MOQ_VERSION_DRAFT_16;
    if (mcfg.struct_size != sizeof(mcfg))
        return 1;
    if (mcfg.version != MOQ_VERSION_DRAFT_16)
        return 1;
    /* appended streaming_objects: a full-size init defaults it false, it is
     * settable, and it follows every previously-appended field (append-only
     * ABI) */
    if (mcfg.streaming_objects)
        return 1;
    mcfg.streaming_objects = true;
    if (offsetof(moq_msquic_managed_cfg_t, streaming_objects) <
        offsetof(moq_msquic_managed_cfg_t, choose_lane_user) + sizeof(void *))
        return 1;
    /* appended session_idle_timeout_us: zero (disabled) after a full-size
     * init, settable, and appended AFTER streaming_objects — the newest
     * field is the struct tail */
    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    if (mcfg.session_idle_timeout_us != 0)
        return 1;
    mcfg.session_idle_timeout_us = 400000;
    if (mcfg.session_idle_timeout_us != 400000)
        return 1;
    if (offsetof(moq_msquic_managed_cfg_t, session_idle_timeout_us) <=
        offsetof(moq_msquic_managed_cfg_t, streaming_objects))
        return 1;
    /* a caller sized to EXCLUDE the appended fields: its prefix is zeroed and
     * stamped, and init never touches bytes past the caller's size */
    memset(&mcfg, 0xff, sizeof(mcfg));
    moq_msquic_managed_cfg_init_sized(
        &mcfg, offsetof(moq_msquic_managed_cfg_t, streaming_objects));
    if (mcfg.struct_size !=
        (uint32_t)offsetof(moq_msquic_managed_cfg_t, streaming_objects))
        return 1;
    if (mcfg.choose_lane_user != NULL) /* pre-boundary field really zeroed */
        return 1;
    /* poison past the caller's size untouched. The excluded field is inspected
     * BYTEWISE: it still holds the 0xff fill, and loading that through _Bool
     * (whose only valid object representations are 0 and 1) is undefined. */
    {
        unsigned char poison[sizeof(mcfg.streaming_objects)];
        memcpy(poison,
               (const unsigned char *)&mcfg +
                   offsetof(moq_msquic_managed_cfg_t, streaming_objects),
               sizeof(poison));
        for (size_t i = 0; i < sizeof(poison); i++)
            if (poison[i] != 0xffu)
                return 1;
    }
    if (mcfg.session_idle_timeout_us == 0) /* poison untouched here too */
        return 1;
    /* a caller sized THROUGH streaming_objects but excluding the newer
     * session_idle_timeout_us: the prior prefix initializes, the excluded
     * field keeps its poison */
    memset(&mcfg, 0xff, sizeof(mcfg));
    moq_msquic_managed_cfg_init_sized(
        &mcfg, offsetof(moq_msquic_managed_cfg_t, session_idle_timeout_us));
    if (mcfg.struct_size !=
        (uint32_t)offsetof(moq_msquic_managed_cfg_t,
                           session_idle_timeout_us))
        return 1;
    if (mcfg.streaming_objects) /* pre-boundary field really zeroed */
        return 1;
    if (mcfg.session_idle_timeout_us == 0) /* poison untouched */
        return 1;
    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    /* the lane-stats type is consumable from the public header, and the
     * NULL-lane getter refuses with MOQ_ERR_INVAL */
    {
        moq_msquic_lane_stats_t lst;

        memset(&lst, 0, sizeof(lst));
        if (moq_msquic_lane_get_stats(NULL, &lst, sizeof(lst)) != MOQ_ERR_INVAL)
            return 1;
    }
    if (moq_msquic_managed_session(NULL) != NULL)
        return 1;
    if (moq_msquic_managed_lane_count(NULL) != 0)
        return 1;
    if (moq_msquic_managed_lane(NULL, 0) != NULL)
        return 1;
    if (moq_msquic_lane_next_conn(NULL, NULL) != NULL)
        return 1;
    if (moq_msquic_lane_index(NULL) != 0)
        return 1;
    if (moq_msquic_lane_wake(NULL) != MOQ_ERR_INVAL)
        return 1;
    if (moq_msquic_managed_conn_user(NULL) != NULL)
        return 1;
    moq_msquic_managed_conn_set_user(NULL, NULL);
    if (moq_msquic_managed_conn_ack_terminal(NULL) != MOQ_ERR_INVAL)
        return 1;
    {
        /* the acknowledgment's exact shape: a token or lane parameter, or a
         * void return, would fail to compile here */
        moq_result_t (*ack)(moq_msquic_managed_conn_t *) =
            moq_msquic_managed_conn_ack_terminal;

        if (ack == NULL)
            return 1;
    }
    moq_msquic_managed_drain(NULL);
    if (moq_msquic_managed_conn_count(NULL) != 0)
        return 1;
    if (moq_msquic_managed_is_fatal(NULL) ||
        moq_msquic_managed_is_closed(NULL))
        return 1;
    /* appended max_open_subgroups: zero (session default) after a full-size
     * init, settable, and appended after the app_deadline block. */
    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    if (mcfg.max_open_subgroups != 0)
        return 1;
    mcfg.max_open_subgroups = 7;
    if (mcfg.max_open_subgroups != 7)
        return 1;
    /* order: the appended field follows the previous tail (app_deadline_ctx),
     * i.e. the app_deadline block is preserved ahead of it */
    if (offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) <
        offsetof(moq_msquic_managed_cfg_t, app_deadline_ctx) + sizeof(void *))
        return 1;
    /* a caller sized THROUGH the app_deadline block but excluding the newer
     * max_open_subgroups: the prior prefix initializes (app_deadline_ctx and
     * session_idle_timeout_us zeroed), the excluded field keeps its poison */
    memset(&mcfg, 0xff, sizeof(mcfg));
    moq_msquic_managed_cfg_init_sized(
        &mcfg, offsetof(moq_msquic_managed_cfg_t, max_open_subgroups));
    if (mcfg.struct_size !=
        (uint32_t)offsetof(moq_msquic_managed_cfg_t, max_open_subgroups))
        return 1;
    if (mcfg.session_idle_timeout_us != 0) /* pre-boundary field zeroed */
        return 1;
    if (mcfg.app_deadline_ctx != NULL) /* the immediate predecessor zeroed */
        return 1;
    if (mcfg.max_open_subgroups == 0) /* poison untouched */
        return 1;
    /* appended versions/version_count: one ABI block at the struct tail. */
    {
        static const moq_version_t drafts[] = {
            MOQ_VERSION_DRAFT_18,
            MOQ_VERSION_DRAFT_16,
        };

        moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
        if (mcfg.versions != NULL || mcfg.version_count != 0)
            return 1;
        mcfg.versions = drafts;
        mcfg.version_count = 2;
        if (mcfg.versions != drafts || mcfg.version_count != 2)
            return 1;
        if (MOQ_MSQUIC_MANAGED_MAX_VERSIONS < 2u)
            return 1;
        if (offsetof(moq_msquic_managed_cfg_t, versions) <
            offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) +
                sizeof(mcfg.max_open_subgroups))
            return 1;
        if (offsetof(moq_msquic_managed_cfg_t, version_count) <=
            offsetof(moq_msquic_managed_cfg_t, versions))
            return 1;
        if (sizeof(moq_msquic_managed_cfg_t) <
            offsetof(moq_msquic_managed_cfg_t, version_count) +
                sizeof(mcfg.version_count))
            return 1;

        memset(&mcfg, 0xff, sizeof(mcfg));
        moq_msquic_managed_cfg_init_sized(
            &mcfg, offsetof(moq_msquic_managed_cfg_t, versions));
        if (mcfg.struct_size !=
            (uint32_t)offsetof(moq_msquic_managed_cfg_t, versions))
            return 1;
        if (mcfg.max_open_subgroups != 0)
            return 1;
        {
            unsigned char poison[sizeof(mcfg.versions)];

            memcpy(poison,
                   (const unsigned char *)&mcfg +
                       offsetof(moq_msquic_managed_cfg_t, versions),
                   sizeof(poison));
            for (size_t i = 0; i < sizeof(poison); i++)
                if (poison[i] != 0xffu)
                    return 1;
        }
    }
    if (moq_msquic_managed_conn_negotiated_version(NULL) != 0)
        return 1;

#endif
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (settings.SendBufferingEnabled)
        return 1;
    /* NULL-safety of the accessors on a NULL conn */
    if (moq_msquic_conn_session(NULL) != NULL)
        return 1;
    if (moq_msquic_conn_is_fatal(NULL) || moq_msquic_conn_is_closed(NULL))
        return 1;
    return 0;
}
