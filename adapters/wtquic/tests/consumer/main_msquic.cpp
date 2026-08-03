/*
 * C++17 installed-consumer smoke for adapter-wtquic-msquic-managed: the header
 * is includable/linkable from C++ (extern "C" intact) against the installed
 * archives.
 */
#include <moq/wtquic_msquic_managed.h>

static int pump(moq_wtquic_msquic_managed_t *m,
                moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m; (void)l; (void)t; (void)u;
    return 0;
}

int main()
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = pump;
    moq_wtquic_msquic_managed_t *m = nullptr;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_OK || m == nullptr)
        return 1;
    if (moq_wtquic_msquic_managed_stop(m) != MOQ_OK)
        return 1;
    moq_wtquic_msquic_managed_destroy(m);
    return 0;
}
