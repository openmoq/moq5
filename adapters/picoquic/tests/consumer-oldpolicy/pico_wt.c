#include <moq/pico_wt.h>
#include <picoquic_config.h>
#include <stdio.h>

/* The standalone WebTransport adapter reaches PicoQUIC through picohttp-core.
 * A direct caller of PicoQUIC's core API keeps core's configuration object in
 * the link, and that object calls picoquic_set_qlog in picoquic-log. */
static picoquic_quic_t *(*volatile keep_configure)(
    picoquic_quic_config_t *, picoquic_stream_data_cb_fn, void *, uint64_t,
    uint64_t *) = picoquic_create_and_configure;

int main(void)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_pico_wt_conn_cfg_t))
        return 1;
    if (keep_configure == NULL)
        return 2;
    printf("PASS: oldpolicy_pico_wt\n");
    return 0;
}
