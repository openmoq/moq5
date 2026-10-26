#include <moq/media_sender.h>
#include <fixture_provider.h>
int main(void)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    return cfg.struct_size != sizeof(cfg) || issue37_provider_value() != 37;
}
