#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>
#include <picoquic_config.h>
#include <stdio.h>

/* The threaded facade over the adapter, with core's configuration object
 * (which calls into picoquic-log) kept in the link. */
static picoquic_quic_t *(*volatile keep_configure)(
    picoquic_quic_config_t *, picoquic_stream_data_cb_fn, void *, uint64_t,
    uint64_t *) = picoquic_create_and_configure;
static int (*volatile keep_verifier)(picoquic_quic_t *, const char *) =
    moq_picoquic_set_cert_verifier;

int main(void)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_pq_threaded_cfg_t))
        return 1;
    if (keep_configure == NULL || keep_verifier == NULL)
        return 2;
    printf("PASS: oldpolicy_threaded\n");
    return 0;
}
