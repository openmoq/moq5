/*
 * Build-tree consumer test for moq::adapter-pico-wt-managed.
 *
 * Consumes the managed facade exactly as a downstream cold consumer
 * would, via find_package(libmoq COMPONENTS adapter-pico-wt-managed).
 * Includes ONLY the public managed header and links ONLY the managed
 * target — proving the package propagates its public include dirs and
 * link interface without any private build-tree paths.
 *
 * Compile/link/run only: no network, no certs. Does NOT include any
 * internal header.
 */
#include <moq/pico_wt_managed.h>

#include <stddef.h>
#include <stdio.h>

int main(void)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);

    /* cfg_init must set the ABI guard and zero the rest. */
    if (cfg.struct_size != sizeof(moq_pico_wt_managed_cfg_t))
        return 1;
    if (cfg.alloc != NULL || cfg.on_pump != NULL)
        return 2;

    /* The handle is opaque to consumers: only a pointer is available. */
    moq_pico_wt_managed_t *handle = (moq_pico_wt_managed_t *)0;
    (void)handle;

    /* Negative: create must reject bad args without touching the
     * network or certs. NULL cfg + NULL out → MOQ_ERR_INVAL. */
    if (moq_pico_wt_managed_create(NULL, NULL) != MOQ_ERR_INVAL)
        return 3;

    /* NULL cfg with a non-NULL out must fail and NULL *out. */
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)0x1;
    if (moq_pico_wt_managed_create(NULL, &m) != MOQ_ERR_INVAL)
        return 4;
    if (m != NULL)
        return 5;

    printf("PASS: moq_pico_wt_managed_consumer_test\n");
    return 0;
}
