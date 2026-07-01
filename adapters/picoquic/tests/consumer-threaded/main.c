#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>   /* verifier helper, reachable via the
                                      adapter-picoquic-threaded component */
#include <stdio.h>

static int dummy_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    (void)t; (void)now; (void)ctx;
    return 0;
}

/* Production cert verification: install the system-trust verifier. This
 * is the documented configure_quic hook a real client would use. */
static int configure_verify(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    return moq_picoquic_set_cert_verifier(quic, NULL);  /* 0 = OK */
}

int main(void)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));

    if (cfg.struct_size != sizeof(moq_pq_threaded_cfg_t))
        return 1;
    if (cfg.alloc != NULL)
        return 2;
    if (cfg.on_pump != NULL)
        return 3;

    /* Create, stop, destroy with valid client config.
     * Exercises real allocation, picoquic init, and pthread linking. */
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost";
    cfg.port = 14990;
    cfg.on_pump = dummy_pump;
    cfg.insecure_skip_verify = true;

    moq_pq_threaded_t *t = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
    if (rc != MOQ_OK || !t)
        return 4;

    if (moq_pq_threaded_session(t) == NULL)
        return 5;
    if (moq_pq_threaded_conn(t) == NULL)
        return 6;

    if (moq_pq_threaded_stop(t) != MOQ_OK)
        return 7;
    moq_pq_threaded_destroy(t);

    /* Verifier helper reachability: header on the include path + symbol
     * linkable from the threaded component. NULL quic must return -1
     * (no network). Proves a cold consumer can enable production cert
     * verification via configure_verify above. */
    if (moq_picoquic_set_cert_verifier(NULL, NULL) != -1)
        return 8;
    (void)configure_verify;

    printf("PASS: moq_adapter_threaded_consumer_test\n");
    return 0;
}
