/* The public headers must stand alone as C++. */

#include <moq/msquic.h>
#ifdef MOQ_HAVE_MSQUIC_MANAGED
#include <moq/msquic_managed.h>
#endif

#include <cstddef>

int main()
{
    moq_msquic_conn_cfg_t cfg;
    QUIC_SETTINGS settings;

    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    moq_msquic_settings_init(&settings);
#ifdef MOQ_HAVE_MSQUIC_MANAGED
    moq_msquic_managed_cfg_t mcfg;

    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    mcfg.version = MOQ_VERSION_DRAFT_16;
    if (mcfg.struct_size != sizeof(mcfg))
        return 1;
    if (mcfg.version != MOQ_VERSION_DRAFT_16)
        return 1;
    if (moq_msquic_managed_port(nullptr) != 0)
        return 1;
    /* appended max_open_subgroups: the C++ view agrees — zero after a
     * full-size init, settable, and after the app_deadline block. */
    if (mcfg.max_open_subgroups != 0)
        return 1;
    mcfg.max_open_subgroups = 7;
    if (mcfg.max_open_subgroups != 7)
        return 1;
    static_assert(offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) >=
                      offsetof(moq_msquic_managed_cfg_t, app_deadline_ctx) +
                          sizeof(void *),
                  "max_open_subgroups must follow the app_deadline block");
    static_assert(offsetof(moq_msquic_managed_cfg_t, versions) >=
                      offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) +
                          sizeof(mcfg.max_open_subgroups),
                  "versions must follow max_open_subgroups");
    static_assert(offsetof(moq_msquic_managed_cfg_t, version_count) >
                      offsetof(moq_msquic_managed_cfg_t, versions),
                  "version_count must follow versions");
    static_assert(sizeof(moq_msquic_managed_cfg_t) >=
                      offsetof(moq_msquic_managed_cfg_t, version_count) +
                          sizeof(mcfg.version_count),
                  "version_count must be contained at the struct tail");
    if (mcfg.versions != nullptr || mcfg.version_count != 0)
        return 1;
    if (moq_msquic_managed_conn_negotiated_version(nullptr) != 0)
        return 1;

#endif
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (moq_msquic_conn_callback() == nullptr)
        return 1;
    return 0;
}
