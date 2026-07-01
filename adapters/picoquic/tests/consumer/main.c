#include <moq/picoquic.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));

    if (cfg.struct_size != sizeof(moq_pq_conn_cfg_t))
        return 1;
    if (cfg.session != NULL)
        return 2;

    /* Verify ALPN constant is exposed by installed headers. */
    if (strcmp(MOQ_PQ_ALPN_DEFAULT, "moqt-16") != 0)
        return 3;

    /* Verify user_ctx in config is initialized to NULL. */
    if (cfg.user_ctx != NULL)
        return 4;

    /* Verify user_ctx accessors on NULL conn are safe. */
    if (moq_pq_conn_user_ctx(NULL) != NULL)
        return 5;
    moq_pq_conn_set_user_ctx(NULL, (void *)1);

    printf("PASS: moq_adapter_consumer_test\n");
    return 0;
}
