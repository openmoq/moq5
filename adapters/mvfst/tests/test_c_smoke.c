/*
 * C include smoke test for <moq/mvfst.h>.
 *
 * Proves the header is C-compatible: no C++ types leak through,
 * managed cfg init links, create/destroy lifecycle links.
 */

#include <moq/mvfst.h>
#include <stdio.h>

static int pump_noop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 1;
}

int main(void)
{
    int failures = 0;

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_mvfst_managed_cfg_t)) {
        fprintf(stderr, "FAIL: managed cfg struct_size\n");
        failures++;
    }

    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = NULL;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    if (rc != MOQ_OK || !m) {
        fprintf(stderr, "FAIL: managed create rc=%d\n", rc);
        failures++;
    } else {
        if (moq_mvfst_managed_is_fatal(m)) {
            fprintf(stderr, "FAIL: unexpected fatal\n");
            failures++;
        }
        moq_mvfst_managed_destroy(m);
    }

    moq_mvfst_managed_cfg_init(NULL);
    moq_mvfst_managed_destroy(NULL);

    if (failures == 0) printf("PASS: test_mvfst_c_smoke\n");
    return failures;
}
