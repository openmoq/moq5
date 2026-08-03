/*
 * moq_wtquic_msquic_managed cfg whole-block read-gate test.
 *
 * The appended app_deadline callback/ctx form ONE ABI block that create() reads
 * (MM_CFG_HAS(app_deadline_ctx)) only when struct_size covers THROUGH
 * app_deadline_ctx. Tested CAUSALLY, no network: a config allocated to exactly
 * the prefix ending at app_deadline_us (one field short of the block) is driven
 * into create() with an unrecognized/oversized WT-Protocol offer, so create()
 * reaches the app_deadline config-import and then aborts on the offer list
 * BEFORE dialing. A correct whole-block gate never reads app_deadline_ctx; a
 * gate keyed on app_deadline_us instead reads it one field past the allocation
 * -- a heap-buffer-overflow under the canonical ASan configuration. A real
 * static callback is used, not an integer-cast pointer.
 */
#include <moq/wtquic_msquic_managed.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t deadline_cb(void *ctx) { (void)ctx; return UINT64_MAX; }
static int pump_cb(moq_wtquic_msquic_managed_t *m,
                   moq_wtquic_msquic_managed_lane_t *lane, uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; (void)ctx; return 0; }

/* A 600-byte token: wtquic rejects it (too large) after the block import. */
static char g_big[600];
static const char *const g_offer[] = { g_big };

static void fill_common(moq_wtquic_msquic_managed_cfg_t *cfg)
{
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 47811;
    cfg->on_lane_pump = pump_cb;
    cfg->wt_protocols = g_offer;
    cfg->wt_protocol_count = 1;
    cfg->app_deadline_us = deadline_cb;
}

int main(void)
{
    memset(g_big, 'a', sizeof(g_big) - 1);
    g_big[sizeof(g_big) - 1] = '\0';

    /* Init contract: sized init stamps the requested size. */
    moq_wtquic_msquic_managed_cfg_t a;
    moq_wtquic_msquic_managed_cfg_init_sized(&a, sizeof(a));
    if (a.struct_size != (uint32_t)sizeof(a)) return 1;
    if (a.app_deadline_us || a.app_deadline_ctx) return 2;

    /* Full-size positive: create() reads the whole block, then aborts on the
     * oversized offer (never dials). */
    {
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        fill_common(&cfg);
        cfg.app_deadline_ctx = &cfg;    /* the full block is present */
        moq_wtquic_msquic_managed_t *m = NULL;
        if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_ERR_INVAL) return 3;
        if (m != NULL) return 4;
    }

    /* Whole-block gate, CAUSAL. Allocate exactly the prefix through
     * app_deadline_us; app_deadline_ctx is NOT part of the allocation. */
    size_t prefix = offsetof(moq_wtquic_msquic_managed_cfg_t, app_deadline_us) +
                    sizeof(((moq_wtquic_msquic_managed_cfg_t *)0)->app_deadline_us);
    unsigned char *raw = (unsigned char *)malloc(prefix);
    if (!raw) return 5;
    memset(raw, 0, prefix);
    moq_wtquic_msquic_managed_cfg_t *cfg = (moq_wtquic_msquic_managed_cfg_t *)raw;
    cfg->struct_size = (uint32_t)prefix;
    fill_common(cfg);                    /* every set field is within the prefix */
    /* app_deadline_ctx is beyond the allocation -- intentionally unset. */

    moq_wtquic_msquic_managed_t *m = NULL;
    moq_result_t rc = moq_wtquic_msquic_managed_create(cfg, &m);
    if (rc != MOQ_ERR_INVAL || m != NULL) { free(raw); return 6; }
    free(raw);

    printf("PASS: wtquic_msquic_managed_cfg_abi\n");
    return 0;
}
