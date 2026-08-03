/*
 * Linked C consumer smoke: <moq/wtquic_msquic_managed.h> is usable by a program
 * linking moq::adapter-wtquic-msquic-managed. Drives a minimal create -> stop ->
 * destroy lifecycle (no transport in this slice) and the NULL-reject path.
 */
#include <moq/wtquic_msquic_managed.h>

#include <stddef.h>
#include <stdint.h>

static int pump(moq_wtquic_msquic_managed_t *m,
                moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    return 0;
}

int main(void)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != (uint32_t)sizeof(cfg))
        return 1;

    /* NULL cfg rejected before any allocation */
    moq_wtquic_msquic_managed_t *bad = (moq_wtquic_msquic_managed_t *)0x1;
    if (moq_wtquic_msquic_managed_create(NULL, &bad) >= 0 || bad != NULL)
        return 1;

    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = pump;

    moq_wtquic_msquic_managed_t *m = NULL;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_OK || m == NULL)
        return 1;
    if (moq_wtquic_msquic_managed_lane_count(m) != 1)
        return 1;
    if (moq_wtquic_msquic_managed_stop(m) != MOQ_OK)
        return 1;
    moq_wtquic_msquic_managed_destroy(m);
    return 0;
}
