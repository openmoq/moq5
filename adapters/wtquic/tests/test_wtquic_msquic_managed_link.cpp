/*
 * Linked C++17 consumer smoke: <moq/wtquic_msquic_managed.h> is includable and
 * linkable from C++ (extern "C" intact). Same minimal lifecycle as the C smoke.
 */
#include <moq/wtquic_msquic_managed.h>

#include <cstddef>
#include <cstdint>

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

int main()
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != static_cast<uint32_t>(sizeof(cfg)))
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
