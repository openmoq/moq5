#include <moq/mvfst.h>
#include <stdio.h>

static int pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 1;
}

int main(void)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.on_pump = pump;
    printf("moq_mvfst C consumer: cfg_init OK\n");
    return 0;
}
