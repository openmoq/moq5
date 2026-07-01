/*
 * Build-tree consumer test for moq::adapter-pico-wt.
 *
 * Consumes the adapter exactly as a downstream project would, via
 * find_package(libmoq COMPONENTS adapter-pico-wt). Includes ONLY the
 * public header and links ONLY the adapter target — proving the
 * package propagates its public include dirs (picohttp/h3zero) and
 * link interface.
 *
 * Does NOT include any internal header.
 */
#include <moq/pico_wt.h>

#include <stddef.h>
#include <stdio.h>

int main(void)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));

    /* cfg_init must set the ABI guard and zero the rest. */
    if (cfg.struct_size != sizeof(moq_pico_wt_conn_cfg_t))
        return 1;
    if (cfg.session != NULL || cfg.user_ctx != NULL)
        return 2;

    /* The handle is opaque to consumers: only a pointer is available. */
    moq_pico_wt_conn_t *conn = (moq_pico_wt_conn_t *)0;
    (void)conn;

    /*
     * Negative: a too-small struct_size must be rejected BEFORE any
     * config field beyond struct_size is read. Fake non-null pointers
     * prove validation order — they are never dereferenced because
     * create() fails on the size check alone.
     */
    moq_pico_wt_conn_cfg_t bad;
    bad.struct_size = (uint32_t)sizeof(bad.struct_size);
    bad.session  = (moq_session_t *)0x1;
    bad.cnx      = (picoquic_cnx_t *)0x1;
    bad.h3_ctx   = (h3zero_callback_ctx_t *)0x1;
    bad.ctrl_ctx = (h3zero_stream_ctx_t *)0x1;
    bad.alloc    = (const moq_alloc_t *)0x1;
    bad.user_ctx = (void *)0x1;

    moq_pico_wt_conn_t *bad_conn = (moq_pico_wt_conn_t *)0x1;
    if (moq_pico_wt_conn_create(&bad, &bad_conn) != -1)
        return 3;
    if (bad_conn != NULL)   /* create() must NULL *out on failure */
        return 4;

    printf("PASS: moq_pico_wt_adapter_consumer_test\n");
    return 0;
}
