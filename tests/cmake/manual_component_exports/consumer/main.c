#if defined(SMOKE_service)
#include <moq/media_sender.h>
int main(void)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#elif defined(SMOKE_adapter_picoquic)
#include <moq/picoquic.h>
int main(void)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#elif defined(SMOKE_adapter_picoquic_threaded)
#include <moq/picoquic_threaded.h>
int main(void)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#elif defined(SMOKE_adapter_pico_wt)
#include <moq/pico_wt.h>
int main(void)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#elif defined(SMOKE_adapter_pico_wt_managed)
#include <moq/pico_wt_managed.h>
int main(void)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg);
}
#else
#error missing component selection
#endif
