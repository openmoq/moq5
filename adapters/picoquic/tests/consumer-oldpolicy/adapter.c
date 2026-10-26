#include <moq/picoquic.h>
#include <picoquic_config.h>
#include <stdio.h>

/* A volatile reference to picoquic_create_and_configure keeps core's
 * configuration object in the link, and that object calls picoquic_set_qlog
 * in picoquic-log: the link must carry the mutually dependent pair. */
static picoquic_quic_t *(*volatile keep_configure)(
    picoquic_quic_config_t *, picoquic_stream_data_cb_fn, void *, uint64_t,
    uint64_t *) = picoquic_create_and_configure;

int main(void)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_pq_conn_cfg_t))
        return 1;
    if (keep_configure == NULL)
        return 2;
    printf("PASS: oldpolicy_adapter\n");
    return 0;
}
