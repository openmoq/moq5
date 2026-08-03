/*
 * Installed-consumer smoke for the managed wtquic-over-MsQuic facade:
 * find_package(libmoq COMPONENTS adapter-wtquic-msquic-managed) must resolve
 * the facade AND its transitive deps (moq::adapter-wtquic, wtq::msquic).
 * Linking forces real symbol resolution across the installed archives; the
 * calls exercise the facade without dialing.
 */
#include <moq/wtquic_msquic_managed.h>

static int pump(moq_wtquic_msquic_managed_t *m,
                moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m; (void)l; (void)t; (void)u;
    return 0;
}

int main(void)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    /* invalid on purpose (no host): must refuse without side effects */
    moq_wtquic_msquic_managed_t *m = NULL;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_ERR_INVAL || m != NULL)
        return 1;
    /* a valid client comes up and tears down */
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = pump;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_OK || m == NULL)
        return 1;
    if (moq_wtquic_msquic_managed_lane_count(m) != 1)
        return 1;
    if (moq_wtquic_msquic_managed_stop(m) != MOQ_OK)
        return 1;
    moq_wtquic_msquic_managed_destroy(m);
    /* NULL-safe facade observations resolve */
    if (moq_wtquic_msquic_managed_is_fatal(NULL) ||
        moq_wtquic_msquic_managed_negotiated_version(NULL) != 0 ||
        moq_wtquic_msquic_managed_lane_count(NULL) != 0)
        return 1;
    return 0;
}
