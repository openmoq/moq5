/*
 * pico_wt managed cfg initializer + whole-block read-gate test.
 *
 * The pointer initializer only stamps the original prefix through
 * wt_protocols; the sized initializer stamps the caller's declared size. The
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

/* The terminal-wait fields are appended after `target`: the first starts at
 * or past the sizeof of the layout that ended with `target` (that layout's
 * last field is a pointer, so its sizeof is the end of `target` rounded to
 * the struct alignment), and the flag follows it. An older caller's
 * struct_size therefore never covers either. */
#define WT_CFG_ALIGN _Alignof(moq_pico_wt_managed_cfg_t)
#define WT_CFG_THROUGH_TARGET                                                     \
    ((offsetof(moq_pico_wt_managed_cfg_t, target) + sizeof(void *) + WT_CFG_ALIGN - 1) \
     / WT_CFG_ALIGN * WT_CFG_ALIGN)
_Static_assert(offsetof(moq_pico_wt_managed_cfg_t, done_wait_timeout_us) == WT_CFG_THROUGH_TARGET,
               "done_wait_timeout_us starts at the sizeof through target");
_Static_assert(offsetof(moq_pico_wt_managed_cfg_t, done_wait_exact) ==
                   offsetof(moq_pico_wt_managed_cfg_t, done_wait_timeout_us) + sizeof(uint64_t),
               "done_wait_exact follows done_wait_timeout_us");
#if UINTPTR_MAX == UINT64_MAX
_Static_assert(sizeof(moq_pico_wt_managed_cfg_t) ==
                   offsetof(moq_pico_wt_managed_cfg_t, done_wait_exact) + 8,
               "LP64: the flag's byte padded to 8");
#endif

int main(void)
{
    /* An old caller has storage only through wt_protocols. The canary
     * represents bytes owned by whatever follows that old allocation. */
    const size_t v0 = offsetof(moq_pico_wt_managed_cfg_t, wt_protocols) +
                      sizeof(((moq_pico_wt_managed_cfg_t *)0)->wt_protocols);
    union {
        moq_pico_wt_managed_cfg_t cfg;
        unsigned char bytes[sizeof(moq_pico_wt_managed_cfg_t) + 16];
    } old;
    memset(old.bytes, 0xa5, sizeof(old.bytes));
    moq_pico_wt_managed_cfg_init(&old.cfg);
    if (old.cfg.struct_size != v0) {
        fprintf(stderr, "FAIL: pointer init stamped %u, v0 prefix %zu\n",
                old.cfg.struct_size, v0);
        return 1;
    }
    for (size_t i = v0; i < sizeof(old.bytes); i++) {
        if (old.bytes[i] != 0xa5) {
            fprintf(stderr, "FAIL: pointer init wrote past v0 at %zu\n", i);
            return 2;
        }
    }

    moq_pico_wt_managed_cfg_t a;
    moq_pico_wt_managed_cfg_init_sized(&a, sizeof(a));
    if (a.struct_size != (uint32_t)sizeof(a)) return 3;
    if (a.alloc || a.on_pump || a.app_deadline_us || a.app_deadline_ctx ||
        a.keep_alive_interval_ms) return 2;

    /* Whole-block gate, CAUSAL. Allocate exactly the prefix through
     * app_deadline_us (the block's first field) -- app_deadline_ctx is NOT part
     * of the allocation. struct_size is stamped to that prefix, so the block is
     * not fully covered and create() must NOT read either field. A gate keyed on
     * app_deadline_us would read cfg->app_deadline_ctx past the allocation. */
    size_t prefix = offsetof(moq_pico_wt_managed_cfg_t, app_deadline_us) +
                    sizeof(((moq_pico_wt_managed_cfg_t *)0)->app_deadline_us);
    /* The fields are set on a full, properly typed local template and only
     * the prefix bytes are copied into the short block, so no member is
     * written through a struct lvalue larger than its storage. */
    moq_pico_wt_managed_cfg_t tmpl;
    memset(&tmpl, 0, sizeof(tmpl));
    tmpl.struct_size = (uint32_t)prefix;
    tmpl.alloc = moq_alloc_default();
    tmpl.perspective = MOQ_PERSPECTIVE_CLIENT;
    tmpl.port = 443;
    tmpl.host = "127.0.0.1";
    tmpl.insecure_skip_verify = true;      /* IP host -> "localhost" SNI, past the SNI gate */
    tmpl.on_pump = pump_cb;
    tmpl.app_deadline_us = deadline_cb;     /* a real callback, within the prefix */
    /* app_deadline_ctx is beyond the prefix: not copied, not in the block. */
    unsigned char *raw = (unsigned char *)malloc(prefix);
    if (!raw) return 4;
    memcpy(raw, &tmpl, prefix);
    moq_pico_wt_managed_cfg_t *cfg = (moq_pico_wt_managed_cfg_t *)raw;

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

    /* A caller built before `target` was appended: its struct ends at
     * keep_alive_interval_ms and struct_size is that layout's sizeof. Driven
     * through create() twice: as an exact heap block (a target read is out of
     * bounds under ASan) and inside current storage whose bytes past
     * keep_alive_interval_ms are poisoned (a target read is a wild pointer,
     * and a malformed target is refused at create). create() reads neither. */
    {
        const size_t end = offsetof(moq_pico_wt_managed_cfg_t, keep_alive_interval_ms) +
                           sizeof(uint32_t);
        const size_t align = _Alignof(moq_pico_wt_managed_cfg_t);
        const size_t old_size = (end + align - 1) / align * align;
        if (offsetof(moq_pico_wt_managed_cfg_t, target) < old_size) {
            fprintf(stderr, "FAIL: target starts inside the old sizeof\n");
            return 5;
        }
        moq_pico_wt_managed_cfg_t full;
        moq_pico_wt_managed_cfg_init_sized(&full, sizeof(full));
        full.alloc = moq_alloc_default();
        full.perspective = MOQ_PERSPECTIVE_CLIENT;
        full.port = 443;
        full.host = "127.0.0.1";
        full.insecure_skip_verify = true;
        full.on_pump = pump_cb;
        full.struct_size = (uint32_t)old_size;
        for (int view = 0; view < 2; view++) {
            unsigned char *blk = NULL;
            moq_pico_wt_managed_cfg_t poisoned = full;
            const moq_pico_wt_managed_cfg_t *c = &poisoned;
            if (view == 0) {
                blk = (unsigned char *)malloc(old_size);
                if (!blk) return 6;
                memcpy(blk, &full, end);
                memset(blk + end, 0xa5, old_size - end);
                c = (const moq_pico_wt_managed_cfg_t *)(const void *)blk;
            } else {
                memset((unsigned char *)&poisoned + end, 0xa5, sizeof(poisoned) - end);
            }
            moq_pico_wt_managed_t *f = NULL;
            moq_result_t r = moq_pico_wt_managed_create(c, &f);
            if (r == MOQ_ERR_INVAL) {
                fprintf(stderr, "FAIL: old-size create refused (target read?)\n");
                free(blk);
                return 7;
            }
            if (r == MOQ_OK && f) {
                moq_pico_wt_managed_stop(f);
                moq_pico_wt_managed_destroy(f);
            }
            free(blk);
        }
    }

    printf("PASS: pico_wt_managed_cfg_abi\n");
    return 0;
}
