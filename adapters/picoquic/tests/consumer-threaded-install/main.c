/*
 * Install-tree cold-consumer: raw picoquic managed (moq_pq_threaded) +
 * the certificate-verifier helper.
 *
 * Consumed exactly as a downstream app would against a `make install`
 * prefix — via find_package(libmoq COMPONENTS adapter-picoquic-threaded)
 * (CMake) or `pkg-config --cflags --libs libmoq` (Meson/autotools).
 * Includes ONLY public headers; no source-tree include paths.
 *
 * Compile/link/run only: proves header + symbol resolution without any
 * network. The verifier helper rides in transitively (the threaded
 * component links adapter-picoquic; the libmoq.pc package carries it).
 */
#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>

#include <stddef.h>
#include <stdio.h>

int main(void)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));

    if (cfg.struct_size != sizeof(moq_pq_threaded_cfg_t))
        return 1;
    if (cfg.alloc != NULL || cfg.on_pump != NULL)
        return 2;

    /* Verifier helper: header on the include path + symbol linkable.
     * NULL quic must return -1 (no network, no picoquic context). */
    if (moq_picoquic_set_cert_verifier(NULL, NULL) != -1)
        return 3;

    printf("PASS: moq_adapter_threaded_install_consumer_test\n");
    return 0;
}
