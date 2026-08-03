/*
 * pico_wt managed cfg initializer + whole-block read-gate test.
 *
 * The pointer and sized initializers both stamp the full current struct. The
 * appended app_deadline callback/ctx form ONE block that create() reads only
 * when struct_size covers THROUGH app_deadline_ctx.
 *
 * The gate is tested CAUSALLY through the real create() config-import: a config
 * allocated to exactly the prefix ending at app_deadline_us (one field short of
 * the block) is driven into create(). A correct whole-block gate never reads
 * app_deadline_ctx; a gate that keyed on app_deadline_us instead would read
 * app_deadline_ctx one field past the allocation -- a heap-buffer-overflow under
 * the canonical ASan configuration. A real static callback is used, not an
 * integer-cast pointer.
 */
#include <moq/pico_wt_managed.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t deadline_cb(void *ctx) { (void)ctx; return UINT64_MAX; }
static int      pump_cb(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx; return 0; }

int main(void)
{
    /* Init contract: both initializers stamp the full current struct. */
    moq_pico_wt_managed_cfg_t a;
    moq_pico_wt_managed_cfg_init(&a);
    if (a.struct_size != (uint32_t)sizeof(a)) return 1;
    if (a.alloc || a.on_pump || a.app_deadline_us || a.app_deadline_ctx) return 2;
    moq_pico_wt_managed_cfg_init_sized(&a, sizeof(a));
    if (a.struct_size != (uint32_t)sizeof(a)) return 3;

    /* Whole-block gate, CAUSAL. Allocate exactly the prefix through
     * app_deadline_us (the block's first field) -- app_deadline_ctx is NOT part
     * of the allocation. struct_size is stamped to that prefix, so the block is
     * not fully covered and create() must NOT read either field. A gate keyed on
     * app_deadline_us would read cfg->app_deadline_ctx past the allocation. */
    size_t prefix = offsetof(moq_pico_wt_managed_cfg_t, app_deadline_us) +
                    sizeof(((moq_pico_wt_managed_cfg_t *)0)->app_deadline_us);
    unsigned char *raw = (unsigned char *)malloc(prefix);
    if (!raw) return 4;
    memset(raw, 0, prefix);
    moq_pico_wt_managed_cfg_t *cfg = (moq_pico_wt_managed_cfg_t *)raw;
    cfg->struct_size = (uint32_t)prefix;
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->port = 443;
    cfg->host = "127.0.0.1";
    cfg->insecure_skip_verify = true;      /* IP host -> "localhost" SNI, past the SNI gate */
    cfg->on_pump = pump_cb;
    cfg->app_deadline_us = deadline_cb;     /* a real callback, within the prefix */
    /* app_deadline_ctx is beyond the allocation -- intentionally not set. */

    /* This config passes every earlier validation and reaches the app_deadline
     * config-import in create(). A correct whole-block gate never touches
     * app_deadline_ctx (its struct_size does not cover it); a gate keyed on
     * app_deadline_us reads cfg->app_deadline_ctx one field past the allocation
     * -- a heap-buffer-overflow under the canonical ASan configuration. We do
     * not care about the return code, only that no out-of-bounds read occurred;
     * if create() started the facade we tear it right back down. */
    moq_pico_wt_managed_t *fac = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(cfg, &fac);
    if (rc == MOQ_OK && fac) {
        moq_pico_wt_managed_stop(fac);
        moq_pico_wt_managed_destroy(fac);
    }
    free(raw);

    printf("PASS: pico_wt_managed_cfg_abi\n");
    return 0;
}
