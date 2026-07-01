/*
 * Cold-consumer test: pico WT managed facade + the certificate-verifier
 * helper, consumed exactly as a downstream app would.
 *
 * The verifier helper (<moq/picoquic_verify.h>,
 * moq_picoquic_set_cert_verifier) lives in the base picoquic adapter. The
 * managed component now depends on it (the managed client installs a default
 * verifier), so a WT managed consumer gets the helper transitively. This test
 * STILL requests the picoquic adapter explicitly — proving that the (now
 * redundant) explicit cross-link continues to compose cleanly:
 *   CMake:      find_package(libmoq COMPONENTS
 *                 adapter-pico-wt-managed adapter-picoquic)
 *   pkg-config: pkg-config --cflags --libs libmoq-pico-wt-managed libmoq
 *
 * Includes ONLY public headers; links ONLY the public targets/packages.
 * Compile/link/run only: no network, no certs.
 */
#include <moq/pico_wt_managed.h>
#include <moq/picoquic_verify.h>

#include <stddef.h>
#include <stdio.h>

/* The documented configure_quic hook: install system-trust verification.
 * For a real client, set cfg.sni to the server's certificate name. */
static int configure_verify(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    return moq_picoquic_set_cert_verifier(quic, NULL);  /* 0 = OK */
}

int main(void)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);

    if (cfg.struct_size != sizeof(moq_pico_wt_managed_cfg_t))
        return 1;
    if (cfg.alloc != NULL || cfg.on_pump != NULL)
        return 2;

    /* Wire the verifier the way a production WT client would. */
    cfg.insecure_skip_verify = false;
    cfg.configure_quic = configure_verify;
    if (cfg.configure_quic == NULL)
        return 3;

    /* Verifier helper reachability: header on the include path + symbol
     * linkable from the cross-linked picoquic adapter. NULL quic must
     * return -1 (no network). */
    if (moq_picoquic_set_cert_verifier(NULL, NULL) != -1)
        return 4;

    printf("PASS: moq_pico_wt_managed_verify_consumer_test\n");
    return 0;
}
