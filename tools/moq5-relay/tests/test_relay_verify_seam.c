/*
 * The blocked-scenario constraint seam (moq-relay-verify only): strict
 * environment parsing fails CLOSED on junk, the bind override reaches the
 * ONE shard-config builder both describe and serve consume (so the described
 * bind ceiling shrinks with the pool — capacity/create parity), and the
 * session value never leaks into the bind config. Pure logic — this binary
 * compiles cli/config.c WITH MOQR_BIND_TESTING; the production moq5-relay
 * compiles the same source without it and carries none of these symbols.
 */

#include "../cli/config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

#define ENV_BIND "MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS"
#define ENV_SESS "MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS"

static moqr_result_t
load(void)
{
    char err[192];
    return moqr_cli_verify_env_load(err, sizeof(err));
}

static int
test_env_strict_parse(void)
{
    int failures = 0;
    unsetenv(ENV_BIND);
    unsetenv(ENV_SESS);

    /* unset = defaults (0) */
    MOQ_TEST_CHECK(load() == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_bind_max_sgs() == 0);
    MOQ_TEST_CHECK(moqr_cli_verify_session_max_sgs() == 0);

    /* boundary values accepted */
    setenv(ENV_BIND, "1", 1);
    setenv(ENV_SESS, "65535", 1);
    MOQ_TEST_CHECK(load() == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_bind_max_sgs() == 1);
    MOQ_TEST_CHECK(moqr_cli_verify_session_max_sgs() == 65535);

    /* every refusal shape, on either variable; a failed load resets BOTH
     * cached values so no stale constraint survives the refusal */
    static const char *bad[] = { "0",     "08",   "1x",    "x1", "",
                                 "65536", "-1",   " 1",    "1 ", "4294967296",
                                 "99999999999999999999" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        setenv(ENV_BIND, "4", 1);   /* a valid sibling must not mask junk */
        setenv(ENV_SESS, bad[i], 1);
        if (load() != MOQR_ERR_INVAL) {
            fprintf(stderr, "FAIL: session junk accepted: \"%s\"\n", bad[i]);
            failures++;
        }
        MOQ_TEST_CHECK(moqr_cli_verify_bind_max_sgs() == 0);
        MOQ_TEST_CHECK(moqr_cli_verify_session_max_sgs() == 0);
        setenv(ENV_BIND, bad[i], 1);
        unsetenv(ENV_SESS);
        if (load() != MOQR_ERR_INVAL) {
            fprintf(stderr, "FAIL: bind junk accepted: \"%s\"\n", bad[i]);
            failures++;
        }
    }
    /* the error message names the offending variable */
    {
        char err[192];
        setenv(ENV_BIND, "junk", 1);
        unsetenv(ENV_SESS);
        MOQ_TEST_CHECK(moqr_cli_verify_env_load(err, sizeof(err)) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(strstr(err, ENV_BIND) != NULL);
        MOQ_TEST_CHECK(strstr(err, "junk") != NULL);
    }
    unsetenv(ENV_BIND);
    unsetenv(ENV_SESS);
    MOQ_TEST_PASS("env_strict_parse");
    return failures;
}

static int
test_bind_override_reaches_builder(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[128];
    const char *json = "{\"listener\":{\"port\":1,\"lanes\":2}}";
    MOQ_TEST_CHECK(moqr_cli_config_parse(json, strlen(json), &cfg, err,
                                         sizeof(err)) == MOQR_OK);

    /* default: builder leaves the template value (0 = library default) */
    unsetenv(ENV_BIND);
    unsetenv(ENV_SESS);
    MOQ_TEST_CHECK(load() == MOQR_OK);
    moqr_shards_cfg_t scfg;
    moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
    MOQ_TEST_CHECK(scfg.bind_cfg.max_open_subgroups == 0);
    moqr_cli_capacity_t cap_default;
    MOQ_TEST_CHECK(moqr_cli_describe_capacity(&cfg, moq_alloc_default(), 0,
                                              &cap_default) == MOQR_OK);

    /* constrained: the value lands on the bind template, and the described
     * bind ceiling SHRINKS (subgroup slot tables are per-conn eager
     * allocations the capacity model counts) — the transport-free proof the
     * constraint reaches the resolver serve consumes. */
    setenv(ENV_BIND, "1", 1);
    MOQ_TEST_CHECK(load() == MOQR_OK);
    moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
    MOQ_TEST_CHECK(scfg.bind_cfg.max_open_subgroups == 1);
    moqr_cli_capacity_t cap_small;
    MOQ_TEST_CHECK(moqr_cli_describe_capacity(&cfg, moq_alloc_default(), 0,
                                              &cap_small) == MOQR_OK);
    MOQ_TEST_CHECK(cap_small.bind_structure_bytes <
                   cap_default.bind_structure_bytes);
    MOQ_TEST_CHECK(cap_small.total_bytes < cap_default.total_bytes);

    /* the SESSION value must NOT touch the bind template */
    unsetenv(ENV_BIND);
    setenv(ENV_SESS, "1", 1);
    MOQ_TEST_CHECK(load() == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_session_max_sgs() == 1);
    moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
    MOQ_TEST_CHECK(scfg.bind_cfg.max_open_subgroups == 0);

    unsetenv(ENV_BIND);
    unsetenv(ENV_SESS);
    MOQ_TEST_PASS("bind_override_reaches_builder");
    return failures;
}

/* The verify and measure builds cannot promise a JSON-only stdout (their seam
 * and RELAY_BLOCKED_V0 rows print straight to it), so a serve with
 * logging.format=json is refused by name; text passes untouched. */
static int
test_json_logging_refused(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[128];
    const char *json = "{\"listener\":{\"port\":1},\"logging\":{\"format\":\"json\"}}";
    const char *text = "{\"listener\":{\"port\":1},\"logging\":{\"format\":\"text\"}}";
    const char *none = "{\"listener\":{\"port\":1}}";

    MOQ_TEST_CHECK(moqr_cli_config_parse(json, strlen(json), &cfg, err,
                                         sizeof(err)) == MOQR_OK);
    err[0] = '\0';
    MOQ_TEST_CHECK(moqr_cli_verify_refuse_json_logging(&cfg, err, sizeof(err)) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strcmp(err, "logging.format: json is not available in the "
                               "verify and measure builds") == 0);
    MOQ_TEST_CHECK(moqr_cli_config_parse(text, strlen(text), &cfg, err,
                                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_refuse_json_logging(&cfg, err, sizeof(err)) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_config_parse(none, strlen(none), &cfg, err,
                                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_refuse_json_logging(&cfg, err, sizeof(err)) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_verify_refuse_json_logging(NULL, err, sizeof(err)) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_PASS("json_logging_refused");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_env_strict_parse();
    failures += test_bind_override_reaches_builder();
    failures += test_json_logging_refused();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
