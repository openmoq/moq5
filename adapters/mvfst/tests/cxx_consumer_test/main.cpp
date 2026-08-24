#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <cstdio>

/* Installed-consumer link proof for the adapter-mvfst packaging contract.
 *
 * Naming only moq::adapter-mvfst (never fmt), this consumer must LINK. A
 * cfg_init-only consumer does not pull the mvfst/Folly objects that carry the
 * unresolved fmt::v12 references (libmvfst.a(QuicException.cpp.o),
 * libfolly.a(json.cpp.o)); calling moq_mvfst_managed_create() does. The call is
 * session-only (host == NULL, no network), but referencing create() forces the
 * linker to pull the managed adapter's transport object code, which is where
 * the fmt symbols live -- so if the exported target does not carry fmt::fmt,
 * this fails to link, not at runtime. */
static int pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                uint64_t, void *)
{
    return 0;
}

int main()
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = nullptr;          /* session-only: no transport, no network */
    cfg.port = 0;
    cfg.on_lane_pump = pump;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    std::printf("moq_mvfst C++ consumer: create rc=%d\n", (int)rc);
    if (rc == MOQ_OK && m) {
        moq_mvfst_managed_stop(m);
        moq_mvfst_managed_destroy(m);
    }
    return 0;
}
