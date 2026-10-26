#include <moq/pico_wt_managed.h>
#include <stdio.h>

/* The WebTransport managed facade, which links the adapter and picoquic-core. */
int main(void)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    if (cfg.struct_size != sizeof(moq_pico_wt_managed_cfg_t))
        return 1;
    printf("PASS: oldpolicy_pico_wt_managed\n");
    return 0;
}
