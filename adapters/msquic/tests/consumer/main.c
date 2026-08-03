/*
 * Consumer smoke: find_package(libmoq COMPONENTS
 * adapter-msquic-managed) must resolve moq::adapter-msquic-managed AND
 * its transitive attach + msquic::msquic dependencies. The references
 * below touch both public headers and force real symbol resolution
 * against both installed libraries — one attach symbol, one managed
 * symbol.
 */

#include <moq/msquic.h>
#include <moq/msquic_managed.h>

int main(void)
{
    moq_msquic_conn_cfg_t cfg;
    moq_msquic_managed_cfg_t mcfg;
    QUIC_SETTINGS settings;

    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    moq_msquic_settings_init(&settings);
    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (mcfg.struct_size != sizeof(mcfg))
        return 1;
    if (settings.SendBufferingEnabled)
        return 1;
    if (moq_msquic_conn_callback() == NULL)
        return 1;
    if (moq_msquic_managed_port(NULL) != 0)
        return 1;
    return 0;
}
