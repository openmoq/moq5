#if defined(COLD_MSQUIC)
#include <moq/msquic.h>
int main(void)
{
    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#else
#include <moq/wtquic_network_managed.h>
int main(void)
{
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#endif
