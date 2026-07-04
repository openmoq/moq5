/*
 * Picoquic WebTransport adapter conformance test.
 *
 * Runs the shared conformance scenarios through the real
 * picoquic + h3zero + picowt WebTransport stack.
 */

#include "pico_wt_conformance_pair.h"
#include "conformance_scenarios.h"
#include <moq/pico_wt.h>
#include <moq/types.h>
#include <stdio.h>

static int run_count = 0;
static int skip_count = 0;
static int fail_count = 0;

#define RUN_SCENARIO(name, required_caps) do { \
    moq_adapter_pair_t p = pico_wt_conformance_create(); \
    if (!p.ops) { \
        fprintf(stderr, "FAIL: create for " #name "\n"); \
        fail_count++; \
    } else if ((required_caps) && \
               (p.capabilities & (required_caps)) != \
                   (uint32_t)(required_caps)) { \
        fprintf(stderr, "SKIP: " #name \
                " (missing capabilities)\n"); \
        skip_count++; \
        moq_pair_destroy(&p); \
    } else { \
        int f = name(&p); \
        if (f > 0) { \
            fprintf(stderr, "FAIL: " #name \
                    " (%d failures)\n", f); \
            fail_count += f; \
        } \
        run_count++; \
        moq_pair_destroy(&p); \
    } \
} while (0)

#define CAP_BIDI (MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS | \
                  MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN)

/* Config validation: moq_pico_wt_conn_create must reject a missing allocator
 * op. The send queue grows its stream table with realloc, so realloc is now
 * required alongside alloc/free -- rejected up front, before any real use of
 * the (here dummy) session/cnx/h3 pointers. */
static int test_conn_create_validation(void)
{
    int failures = 0;
    moq_pico_wt_conn_t *out = NULL;

    if (moq_pico_wt_conn_create(NULL, &out) == 0) failures++;

    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init(&cfg);
    cfg.session = (moq_session_t *)(uintptr_t)0x1;
    cfg.cnx     = (picoquic_cnx_t *)(uintptr_t)0x1;
    cfg.h3_ctx  = (h3zero_callback_ctx_t *)(uintptr_t)0x1;
    cfg.ctrl_ctx = (h3zero_stream_ctx_t *)(uintptr_t)0x1;

    moq_alloc_t good = *moq_alloc_default();
    moq_alloc_t bad;

    bad = good; bad.realloc = NULL; cfg.alloc = &bad;
    if (moq_pico_wt_conn_create(&cfg, &out) == 0) failures++;
    bad = good; bad.alloc = NULL;   cfg.alloc = &bad;
    if (moq_pico_wt_conn_create(&cfg, &out) == 0) failures++;
    bad = good; bad.free = NULL;    cfg.alloc = &bad;
    if (moq_pico_wt_conn_create(&cfg, &out) == 0) failures++;

    if (failures)
        fprintf(stderr, "FAIL: conn_create validation (%d)\n", failures);
    return failures;
}

int main(void)
{
    fail_count += test_conn_create_validation();

    RUN_SCENARIO(conformance_setup_handshake, 0);
    RUN_SCENARIO(conformance_subscribe_and_object, 0);
    RUN_SCENARIO(conformance_datagram_object, 0);
    RUN_SCENARIO(conformance_single_control_stream,
                 MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT);
    RUN_SCENARIO(conformance_close_not_fatal,
                 MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME);
    RUN_SCENARIO(conformance_session_timer_goaway,
                 MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME);
    RUN_SCENARIO(conformance_bidi_halfclose_accept, CAP_BIDI);
    RUN_SCENARIO(conformance_bidi_halfclose_reject_late_fin, CAP_BIDI);
    RUN_SCENARIO(conformance_crossed_cancel,
                 MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS);
    RUN_SCENARIO(conformance_reset_propagation, 0);
    RUN_SCENARIO(conformance_would_block_ordering,
                 MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES);
    RUN_SCENARIO(conformance_dropped_datagram_unblocks,
                 MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS |
                 MOQ_ADAPTER_PAIR_CAP_DATAGRAMS);
    RUN_SCENARIO(conformance_blocked_ns_reject_tombstone,
                 CAP_BIDI | MOQ_ADAPTER_PAIR_CAP_TOMBSTONES |
                 MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES);

    printf("test_pico_wt_conformance: %d run, %d skipped, %d failed\n",
           run_count, skip_count, fail_count);

    return fail_count ? 1 : 0;
}
