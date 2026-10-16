/* Observe the real endpoint connect path at adapter creation, without
 * starting a transport. Query bytes belong to the CONNECT target; URI
 * fragments do not (transport-18 sections 3.1.2 and 3.1.3). */
#include "endpoint_internal.h"
#include "test_support.h"
#include "../../tests/support/failpoint.h"

#include <moq/pico_wt_managed.h>

#include <string.h>

static int failures;
static unsigned calls;
static const char *expected_path;
static const char *expected_host;
static const fp_alloc_state_t *active_alloc;

moq_result_t moq_test_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out);
moq_result_t moq_test_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out)
{
    calls++;
    MOQ_TEST_CHECK(cfg->path != NULL);
    MOQ_TEST_CHECK(cfg->host != NULL);
    if (cfg->path) MOQ_TEST_CHECK(strcmp(cfg->path, expected_path) == 0);
    if (cfg->host) MOQ_TEST_CHECK(strcmp(cfg->host, expected_host) == 0);
    int path_slot = fp_table_find(active_alloc, cfg->path);
    MOQ_TEST_CHECK(path_slot >= 0);
    if (path_slot >= 0)
        MOQ_TEST_CHECK_EQ_U64(active_alloc->table[path_slot].size,
                              strlen(expected_path) + 1);
    *out = NULL;
    return MOQ_ERR_UNSUPPORTED; /* Deliberate stop before any socket/thread. */
}

static uint64_t target_case(const char *url, size_t url_len,
                            const char *override_path, const char *host,
                            const char *target, uint64_t fail_at)
{
    fp_alloc_state_t state = {0};
    state.fail_at = fail_at;
    moq_alloc_t alloc = fp_allocator(&state);
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.url = (moq_bytes_t){ (const uint8_t *)url, url_len };
    cfg.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
    cfg.insecure_skip_verify = true;
    cfg.alloc = &alloc;
    if (override_path)
        cfg.wt_path = (moq_bytes_t){ (const uint8_t *)override_path,
                                     strlen(override_path) };
    calls = 0;
    expected_host = host;
    expected_path = target;
    active_alloc = &state;
    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&cfg, &ep);
    active_alloc = NULL;
    MOQ_TEST_CHECK_EQ_INT(rc, fail_at ? MOQ_ERR_NOMEM : MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_U64(calls, fail_at ? 0 : 1);
    MOQ_TEST_CHECK(ep == NULL);
    if (ep) {
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }
    MOQ_TEST_CHECK(state.balance == 0 && state.live_bytes == 0);
    MOQ_TEST_CHECK(state.sticky_log_overflow == 0);
    MOQ_TEST_CHECK(state.sticky_table_overflow == 0);
    MOQ_TEST_CHECK(state.sticky_unknown_ptr == 0);
    MOQ_TEST_CHECK(state.sticky_size_mismatch == 0);
    return state.call_count;
}

int main(void)
{
    static const struct {
        const char *url;
        const char *override_path;
        const char *host;
        const char *target;
    } cases[] = {
        { "https://relay.example/live", NULL, "relay.example", "/live" },
        { "https://relay.example/moq?jwt=abc", NULL, "relay.example", "/moq?jwt=abc" },
        { "https://relay.example/moq?", NULL, "relay.example", "/moq?" },
        { "https://relay.example", NULL, "relay.example", "/moq" },
        { "https://relay.example?jwt=abc", NULL, "relay.example", "/moq?jwt=abc" },
        { "https://relay.example?", NULL, "relay.example", "/moq?" },
        { "https://relay.example/?x=a%2Fb%23c&x=+?", NULL, "relay.example", "/?x=a%2Fb%23c&x=+?" },
        { "https://relay.example/moq?url=yes", "/custom?own=yes", "relay.example", "/custom?own=yes" },
        { "https://relay.example?url=yes", "/custom", "relay.example", "/custom" },
        { "https://[::1]:4433/moq?x=1", NULL, "::1", "/moq?x=1" },
        { "moqt://relay.example/moq?x=1", NULL, "relay.example", "/moq?x=1" },
        { "https://relay.example/moq?x=1#local:part", NULL, "relay.example", "/moq?x=1" },
        { "https://relay.example/moq#local:part?not-a-query", NULL, "relay.example", "/moq" },
        { "https://relay.example:443#local:part", NULL, "relay.example", "/moq" },
        { "https://relay.example?x=1#local:part", NULL, "relay.example", "/moq?x=1" },
        { "https://relay.example?#local:part", NULL, "relay.example", "/moq?" },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        printf("target case %zu\n", i);
        target_case(cases[i].url, strlen(cases[i].url), cases[i].override_path,
                    cases[i].host, cases[i].target, 0);
    }
    const char bounded[] = "https://relay.example/moq?x=1DO_NOT_SEND";
    target_case(bounded, strlen(bounded) - strlen("DO_NOT_SEND"), NULL,
                "relay.example", "/moq?x=1", 0);

    const char *url = "https://relay.example?jwt=abc";
    uint64_t count = target_case(url, strlen(url), NULL, "relay.example",
                                 "/moq?jwt=abc", 0);
    MOQ_TEST_CHECK(count > 0 && count < FP_LOG_CAP);
    for (uint64_t n = 1; n <= count && n < FP_LOG_CAP; n++)
        target_case(url, strlen(url), NULL, "relay.example", "/moq?jwt=abc", n);

    if (failures) return 1;
    MOQ_TEST_PASS("endpoint_wt_target");
    return 0;
}
