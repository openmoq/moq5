/* The public headers must stand alone as C++. */

#include <moq/msquic.h>
#ifdef MOQ_HAVE_MSQUIC_MANAGED
#include <moq/msquic_managed.h>
#endif

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
#endif
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (moq_msquic_conn_callback() == nullptr)
        return 1;
    return 0;
}
