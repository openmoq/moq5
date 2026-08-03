/*
 * moq_wtquic_network_managed cfg initializer + whole-block read-gate test.
 *
 * The pointer and sized initializers both stamp the full current struct. The
 * appended app_deadline callback/ctx form ONE ABI block that create() reads
 * only when struct_size covers THROUGH app_deadline_ctx.
 *
 * The gate is tested CAUSALLY through the real create() config-import, with no
 * network: a config allocated to exactly the prefix ending at app_deadline_us
 * (one field short of the block) is driven into create() with an unrecognized
 * wt_protocols token, so create() reaches the app_deadline config-import and
 * then aborts on the offer list — BEFORE any dial. A correct whole-block gate
 * never reads app_deadline_ctx; a gate keyed on app_deadline_us instead would
 * read app_deadline_ctx one field past the allocation, a heap-buffer-overflow
 * under the canonical ASan configuration. A real static callback is used, not
 * an integer-cast pointer.
 */
#include <moq/wtquic_network_managed.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t deadline_cb(void *ctx) { (void)ctx; return UINT64_MAX; }
static int      pump_cb(moq_wtquic_network_managed_t *m,
                        moq_wtquic_network_managed_lane_t *lane,
                        uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; (void)ctx; return 0; }

/* Fill the fields create() validates before the app_deadline block, plus an
 * unrecognized wt_protocols so it aborts (MOQ_ERR_INVAL) after the block read
 * and before dialing. All of these live at offsets below the block, so they
 * fit inside a prefix allocation that stops at app_deadline_us. */
static void fill_common(moq_wtquic_network_managed_cfg_t *cfg)
{
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 443;
    cfg->wt_protocols = "not-a-real-moq-wt-protocol-token";
    cfg->app_deadline_us = deadline_cb;
}

int main(void)
{
    /* Init contract: both initializers stamp the full current struct. */
    moq_wtquic_network_managed_cfg_t a;
    moq_wtquic_network_managed_cfg_init(&a);
    if (a.struct_size != (uint32_t)sizeof(a)) return 1;
    if (a.alloc || a.on_lane_pump || a.app_deadline_us || a.app_deadline_ctx)
        return 2;
    moq_wtquic_network_managed_cfg_init_sized(&a, sizeof(a));
    if (a.struct_size != (uint32_t)sizeof(a)) return 3;

    /* Full-size positive: create() reads the whole block, installs the
     * callback, and aborts on the bogus offer list (never dials). */
    {
        moq_wtquic_network_managed_cfg_t cfg;
        moq_wtquic_network_managed_cfg_init(&cfg);
        fill_common(&cfg);
        cfg.on_lane_pump = pump_cb;
        cfg.app_deadline_ctx = &cfg;    /* the full block is present */
        moq_wtquic_network_managed_t *m = NULL;
        moq_result_t rc = moq_wtquic_network_managed_create(&cfg, &m);
        if (rc != MOQ_ERR_INVAL || m != NULL) return 4;
    }

    /* Whole-block gate, CAUSAL. Allocate exactly the prefix through
     * app_deadline_us; app_deadline_ctx is NOT part of the allocation.
     * struct_size is stamped to that prefix, so the block is not fully
     * covered and create() must NOT read either field. A gate keyed on
     * app_deadline_us would read cfg->app_deadline_ctx past the allocation. */
    size_t prefix = offsetof(moq_wtquic_network_managed_cfg_t, app_deadline_us) +
                    sizeof(((moq_wtquic_network_managed_cfg_t *)0)->app_deadline_us);
    unsigned char *raw = (unsigned char *)malloc(prefix);
    if (!raw) return 5;
    memset(raw, 0, prefix);
    moq_wtquic_network_managed_cfg_t *cfg = (moq_wtquic_network_managed_cfg_t *)raw;
    cfg->struct_size = (uint32_t)prefix;
    fill_common(cfg);                    /* every set field is within the prefix */
    /* on_lane_pump/app_deadline_ctx intentionally left unset; app_deadline_ctx
     * is beyond the allocation. */

    moq_wtquic_network_managed_t *m = NULL;
    moq_result_t rc = moq_wtquic_network_managed_create(cfg, &m);
    /* Correct gate: block skipped, create aborts on the bogus offer list with
     * no out-of-bounds read. We assert the clean abort, not the callback. */
    if (rc != MOQ_ERR_INVAL || m != NULL) { free(raw); return 6; }
    free(raw);

    printf("PASS: wtquic_network_managed_cfg_abi\n");
    return 0;
}
