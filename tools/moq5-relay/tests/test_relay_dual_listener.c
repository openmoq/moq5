/*
 * Dual-listener relay: one process, one shard runtime, two transports.
 *
 * The relay's raw listener speaks MoQ directly over MsQuic; the optional
 * WebTransport listener speaks MoQ over wtquic's MsQuic backend so a browser
 * can reach the same relay. Both feed ONE logical runtime -- a publisher on
 * either transport must be able to serve a subscriber on the other -- so the
 * shard plan below is the load-bearing contract: each facade owns a disjoint
 * range of global shards, and nothing may map a lane of one facade onto a
 * shard owned by the other.
 *
 * These cases are pure: config text in, plan or refusal out. No transport, no
 * socket, no clock.
 */

#include "../cli/config.h"
#include <moq/relay/moqr_shards.h>

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

static moqr_result_t
parse(const char *json, moqr_cli_config_t *out, char *err, size_t errlen)
{
    return moqr_cli_config_parse(json, strlen(json), out, err, errlen);
}

/* A raw-only config, exactly what today's deployments ship. */
static const char *const kLegacy =
    "{\"listener\":{\"host\":\"0.0.0.0\",\"port\":4433,"
    "\"cert\":\"c.pem\",\"key\":\"k.pem\",\"lanes\":2}}";

/* The same, plus an explicit WebTransport listener. */
static const char *const kDual =
    "{\"listener\":{\"host\":\"0.0.0.0\",\"port\":4433,"
    "\"cert\":\"c.pem\",\"key\":\"k.pem\",\"lanes\":2},"
    "\"webtransport\":{\"host\":\"0.0.0.0\",\"port\":4443,"
    "\"cert\":\"c.pem\",\"key\":\"k.pem\",\"lanes\":2,"
    "\"path\":\"/moq\",\"versions\":[18,16],\"profile\":\"current\"}}";

/* -- legacy compatibility -------------------------------------------------- */

static int
t_legacy_raw_only_unchanged(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[256] = { 0 };

    MOQ_TEST_CHECK(parse(kLegacy, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cfg.lanes, 2);
    MOQ_TEST_CHECK_EQ_U64(cfg.port, 4433);
    /* a config that never mentions WebTransport must not acquire a WT
     * listener by default: raw-only stays raw-only */
    MOQ_TEST_CHECK(!moqr_cli_config_has_webtransport(&cfg));
    return failures;
}

/* -- the dual listener itself ---------------------------------------------- */

static int
t_dual_config_accepted(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[256] = { 0 };

    MOQ_TEST_CHECK(parse(kDual, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_config_has_webtransport(&cfg));
    MOQ_TEST_CHECK_EQ_U64(cfg.lanes, 2);
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.lanes, 2);
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.port, 4443);
    MOQ_TEST_CHECK(strcmp(cfg.wt.path, "/moq") == 0);
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.profile, MOQR_CLI_WT_PROFILE_CURRENT);
    /* ordered preference is preserved exactly as written */
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.version_count, 2);
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.versions[0], MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK_EQ_U64(cfg.wt.versions[1], MOQ_VERSION_DRAFT_16);
    return failures;
}

/* -- fail-closed on every WebTransport key --------------------------------- */

static int
t_wt_malformed_fails_closed(void)
{
    int failures = 0;
    static const char *const bad[] = {
        /* unknown key inside the object */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"nonsense\":1}}",
        /* unknown wire profile */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"profile\":\"tomorrow\"}}",
        /* profile of the wrong JSON type */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"profile\":3}}",
        /* a path that is not a path */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"path\":\"moq\"}}",
        /* an unsupported MoQ draft */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[17]}}",
        /* an empty version list is not "the default", it is a mistake */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[]}}",
        /* a WebTransport listener with no TLS material of its own */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443}}",
        /* zero lanes cannot serve anything */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"lanes\":0}}",
        /* the object must be an object */
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
        "\"webtransport\":true}",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };

        if (parse(bad[i], &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("FAIL: webtransport case %zu was accepted: %s\n", i, bad[i]);
            failures++;
            continue;
        }
        /* a refusal has to say which key it refused */
        MOQ_TEST_CHECK(err[0] != '\0');
    }
    return failures;
}

/* -- the shard plan -------------------------------------------------------- */

static int
t_shard_plan_disjoint(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[256] = { 0 };
    moqr_cli_shard_plan_t plan;

    MOQ_TEST_CHECK(parse(kDual, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_shard_plan(&cfg, &plan, err, sizeof(err)) ==
                   MOQR_OK);
    /* raw first, WebTransport after it, no gap and no overlap */
    MOQ_TEST_CHECK_EQ_U64(plan.total_shards, 4);
    MOQ_TEST_CHECK_EQ_U64(plan.raw_first, 0);
    MOQ_TEST_CHECK_EQ_U64(plan.raw_count, 2);
    MOQ_TEST_CHECK_EQ_U64(plan.wt_first, 2);
    MOQ_TEST_CHECK_EQ_U64(plan.wt_count, 2);
    /* every lane maps to exactly one global shard, and the two ranges never
     * touch: this is what keeps one facade's callback off the other's session */
    for (uint32_t l = 0; l < plan.raw_count; l++) {
        MOQ_TEST_CHECK_EQ_U64(moqr_cli_shard_of_raw_lane(&plan, l), l);
    }
    for (uint32_t l = 0; l < plan.wt_count; l++) {
        uint32_t s = moqr_cli_shard_of_wt_lane(&plan, l);
        MOQ_TEST_CHECK_EQ_U64(s, plan.wt_first + l);
        MOQ_TEST_CHECK(s >= plan.raw_first + plan.raw_count);
    }
    return failures;
}

static int
t_shard_plan_raw_only(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[256] = { 0 };
    moqr_cli_shard_plan_t plan;

    MOQ_TEST_CHECK(parse(kLegacy, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_cli_shard_plan(&cfg, &plan, err, sizeof(err)) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.total_shards, 2);
    MOQ_TEST_CHECK_EQ_U64(plan.raw_count, 2);
    MOQ_TEST_CHECK_EQ_U64(plan.wt_count, 0);
    return failures;
}

/* The combined lane count is what the shard runtime must allocate, so the
 * refusal belongs here -- before any allocation or listener exists. */
static int
t_shard_plan_overflow_refuses(void)
{
    int failures = 0;
    char json[512];
    moqr_cli_config_t cfg;
    char err[256] = { 0 };
    moqr_cli_shard_plan_t plan;

    snprintf(json, sizeof(json),
             "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\","
             "\"lanes\":%u},"
             "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
             "\"lanes\":2}}",
             MOQR_CLI_MAX_LANES);
    /* each listener is individually legal; only the SUM is not */
    MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);
    err[0] = '\0';
    MOQ_TEST_CHECK(moqr_cli_shard_plan(&cfg, &plan, err, sizeof(err)) !=
                   MOQR_OK);
    MOQ_TEST_CHECK(err[0] != '\0');
    return failures;
}

/* -- per-facade admission ------------------------------------------------- */

/*
 * The reported ceiling is sized for the shards the runtime actually
 * allocates, so the two listeners together must not be able to admit more
 * than that. A cap handed whole to each facade would let the pair admit twice
 * the capacity whose backing shards were sized and reported.
 */
static int
t_facade_caps_partition_the_total(void)
{
    int failures = 0;
    static const char *const shapes[][2] = {
        { "2", "2" }, { "1", "1" }, { "3", "1" }, { "1", "5" },
    };
    for (size_t i = 0; i < sizeof(shapes) / sizeof(shapes[0]); i++) {
        char json[512];
        moqr_cli_config_t cfg;
        char err[256] = { 0 };

        snprintf(json, sizeof(json),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\","
                 "\"lanes\":%s},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"lanes\":%s}}",
                 shapes[i][0], shapes[i][1]);
        MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);

        moqr_shards_cfg_t scfg;
        uint32_t combined = 0;
        MOQ_TEST_CHECK(moqr_cli_serve_compose(&cfg, moq_alloc_default(), &scfg,
                                              &combined) == MOQR_OK);
        uint32_t raw_cap = 0, wt_cap = 0;
        MOQ_TEST_CHECK(moqr_cli_facade_caps(&cfg, moq_alloc_default(),
                                            &raw_cap, &wt_cap) == MOQR_OK);
        /* the split is exact: neither facade is short-changed, and the two
         * together never exceed what was reported */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)raw_cap + wt_cap, combined);
        MOQ_TEST_CHECK(raw_cap > 0);
        MOQ_TEST_CHECK(wt_cap > 0);
    }
    /* raw-only: the whole cap belongs to the one facade */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        MOQ_TEST_CHECK(parse(kLegacy, &cfg, err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t scfg;
        uint32_t combined = 0, raw_cap = 0, wt_cap = 0;
        MOQ_TEST_CHECK(moqr_cli_serve_compose(&cfg, moq_alloc_default(), &scfg,
                                              &combined) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_facade_caps(&cfg, moq_alloc_default(),
                                            &raw_cap, &wt_cap) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(raw_cap, combined);
        MOQ_TEST_CHECK_EQ_U64(wt_cap, 0);
    }
    return failures;
}

/* -- the WebTransport version label --------------------------------------- */

/*
 * webtransport.versions is independently configurable, so a diagnostic row
 * from a WebTransport shard must name the set THAT listener offers. Reusing
 * the raw listener's label would attribute the wrong negotiated set to every
 * WebTransport row.
 */
static int
t_wt_version_label_is_its_own(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[256] = { 0 };
    static const char *const kSplit =
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[18]},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[16,18]}}";

    MOQ_TEST_CHECK(parse(kSplit, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(strcmp(cfg.alpn_set, "moqt-18") == 0);
    /* ordered, and its own: preference order is preserved verbatim */
    MOQ_TEST_CHECK(strcmp(cfg.wt.alpn_set, "moqt-16+moqt-18") == 0);
    MOQ_TEST_CHECK(strcmp(cfg.wt.alpn_set, cfg.alpn_set) != 0);

    /* When both listeners spell out the SAME ordered set, the labels agree --
     * so a difference always reflects configuration, never the label. */
    static const char *const kAgree =
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[18,16]},"
        "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
        "\"versions\":[18,16]}}";
    moqr_cli_config_t same;
    MOQ_TEST_CHECK(parse(kAgree, &same, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(strcmp(same.wt.alpn_set, same.alpn_set) == 0);
    /* and kDual's raw listener takes the default set while its WebTransport
     * block names one explicitly, so those two legitimately differ */
    moqr_cli_config_t dual;
    MOQ_TEST_CHECK(parse(kDual, &dual, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(strcmp(dual.wt.alpn_set, "moqt-18+moqt-16") == 0);
    return failures;
}

/* -- the WebTransport wire profile ---------------------------------------- */

/*
 * Three dialects, three tokens, and no silent narrowing.
 *
 * A parser that fell back to the default on an unrecognised token, or that
 * collapsed one dialect into another, would leave the relay serving a wire
 * profile the operator did not ask for and does not know it has chosen. No
 * claim is made here about which profile any particular peer selects.
 */
static int
t_wt_profile_tokens(void)
{
    int failures = 0;
    static const struct { const char *token; moqr_cli_wt_profile_t want; }
    ok[] = {
        { "current",        MOQR_CLI_WT_PROFILE_CURRENT },
        { "d13_14_compat",  MOQR_CLI_WT_PROFILE_D13_14_COMPAT },
        { "d02_rfc9297_compat",  MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT },
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        char json[512];
        moqr_cli_config_t cfg;
        char err[256] = { 0 };

        /* d02_rfc9297_compat requires a policy, so every row carries the same
         * one and the accepted profile is what this loop is measuring */
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"origin_policy\":\"allow_any_non_opaque\","
                 "\"profile\":\"%s\"}}", ok[i].token);
        MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.profile, (uint64_t)ok[i].want);
    }
    /* The maximum-width renderers size themselves against one assumed longest
     * name. If a dialect with a longer name is added and that assumption is
     * not updated with it, every bound computed from it is short and the
     * longest line the relay can emit no longer fits. */
    if (!moqr_cli_wt_profile_name_max_is_longest()) {
        printf("FAIL: %s is no longer the longest profile name\n",
               MOQR_CLI_WT_PROFILE_NAME_MAX);
        failures++;
    }
    /* the wire values are the dependency's, and are pinned */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_WT_PROFILE_CURRENT, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_WT_PROFILE_D13_14_COMPAT, 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT, 2);

    /* an omitted profile is still CURRENT: adding a dialect changes no default */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        static const char *const kNoProfile =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\"}}";
        MOQ_TEST_CHECK(parse(kNoProfile, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.profile,
                              (uint64_t)MOQR_CLI_WT_PROFILE_CURRENT);
    }
    /* near-misses and wrong types fail closed rather than defaulting */
    static const char *const bad[] = {
        /* the withdrawn token is refused by the ordinary unknown-value rule,
         * so a configuration written against it fails loudly instead of
         * quietly selecting something else */
        "chrome_legacy", "chrome-legacy", "chromelegacy", "Chrome_Legacy",
        "d02_rfc9297_compat ", " d02_rfc9297_compat", "d02_rfc9297",
        "current ", "CURRENT", "chrome", "legacy", "2", "",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char json[512];
        moqr_cli_config_t cfg;
        char err[256] = { 0 };

        snprintf(json, sizeof(json),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"profile\":\"%s\"}}", bad[i]);
        if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("FAIL: profile token %s was accepted\n", bad[i]);
            failures++;
        }
    }
    /* a numeric profile is not a token */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        static const char *const kNum =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"profile\":2}}";
        MOQ_TEST_CHECK(parse(kNum, &cfg, err, sizeof(err)) != MOQR_OK);
    }
    return failures;
}

/* -- the webtransport object's key boundary -------------------------------- */

/*
 * A key is the whole decoded key, and it appears at most once.
 *
 * The JSON document decides the bytes of a key, not just its printable prefix,
 * so "profile\u0000x" is not "profile" and must not select the profile field.
 * The same holds for the root key that reaches this object at all: a second
 * `webtransport` object, or a duplicate key inside one, would let a later value
 * silently overwrite an earlier one and leave the operator reading a
 * configuration the relay is not running.
 */
static int
t_wt_key_boundary(void)
{
    int failures = 0;
    static const struct { const char *what; const char *json; } bad[] = {
        { "a NUL-suffixed profile value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"current\\u0000x\"}}" },
        { "a NUL-suffixed profile KEY",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\\u0000x\":\"d13_14_compat\"}}" },
        { "a NUL-suffixed root webtransport KEY",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\\u0000x\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\"}}" },
        { "two root webtransport objects",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"d13_14_compat\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\"}}" },
        { "a duplicate profile key",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"current\",\"profile\":\"d13_14_compat\"}}" },
        { "a duplicate profile key with the SAME value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"current\",\"profile\":\"current\"}}" },
        { "the same key spelled with an escape",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"current\",\"\\u0070rofile\":\"d13_14_compat\"}}" },
        { "a duplicate port key",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"port\":4444,"
          "\"cert\":\"c\",\"key\":\"k\"}}" },
        { "a duplicate allowed_origins key",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"allowlist\","
          "\"allowed_origins\":[\"https://a.example\"],"
          "\"allowed_origins\":[\"https://b.example\"]}}" },
        { "a key that is only a prefix of a known one",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"prof\":\"current\"}}" },
        { "a NUL inside the host value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"host\":\"127.0.0.1\\u0000evil\"}}" },
        { "a NUL inside the path value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"path\":\"/moq\\u0000/other\"}}" },
        { "a NUL inside the cert value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"key\":\"k\","
          "\"cert\":\"/real.pem\\u0000/decoy.pem\"}}" },
        { "a NUL inside the key value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
          "\"key\":\"/real.key\\u0000/decoy.key\"}}" },
        { "a NUL inside an origin entry",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"allowlist\","
          "\"allowed_origins\":[\"https://a.example\\u0000https://b.example\"]}}" },
        { "a NUL-suffixed origin_policy value",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"allowlist\\u0000x\","
          "\"allowed_origins\":[\"https://a.example\"]}}" },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        if (parse(bad[i].json, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("FAIL: %s was accepted\n", bad[i].what);
            failures++;
        }
    }
    /* the ordinary spelling of every one of those keys still works */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        static const char *const kOk =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":4443,"
            "\"cert\":\"/c.pem\",\"key\":\"/k.pem\",\"lanes\":2,"
            "\"path\":\"/moq\",\"versions\":[18],"
            "\"profile\":\"d13_14_compat\"}}";
        MOQ_TEST_CHECK(parse(kOk, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.profile,
                              (uint64_t)MOQR_CLI_WT_PROFILE_D13_14_COMPAT);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.lanes, 2);
    }
    return failures;
}

/* -- Origin policy and the allowlist --------------------------------------- */

/*
 * Which policy is in force, and whether a list may accompany it.
 *
 * The rules are cross-field, so they are decided only once the whole object has
 * been read: writing the policy after the list must give exactly the same
 * verdict as writing it before. A list under a policy that cannot use one is
 * refused rather than ignored, because an ignored allowlist reads like
 * enforcement that is not happening.
 */
static int
t_wt_origin_policy(void)
{
    int failures = 0;
    static const char *const kSentinel = "https://sentinel-9f3a.example:8443";

    /* absent policy and absent list: today's behaviour, and no list at all */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        static const char *const j =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\"}}";
        MOQ_TEST_CHECK(parse(j, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_policy,
                              (uint64_t)MOQR_CLI_ORIGIN_POLICY_UNSET);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count, 0);
    }
    /* every policy token maps to its own value, on every profile */
    {
        static const struct { const char *tok; uint32_t want; } pol[] = {
            { "unset",                    MOQR_CLI_ORIGIN_POLICY_UNSET },
            { "allow_any_non_opaque",
              MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE },
            { "allow_any_including_null",
              MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL },
        };
        static const char *const prof[] = { "current", "d13_14_compat",
                                            "d02_rfc9297_compat" };
        for (size_t i = 0; i < sizeof(pol) / sizeof(pol[0]); i++) {
            for (size_t p = 0; p < sizeof(prof) / sizeof(prof[0]); p++) {
                char json[512];
                moqr_cli_config_t cfg;
                char err[256] = { 0 };
                bool want_ok =
                    !(pol[i].want == MOQR_CLI_ORIGIN_POLICY_UNSET && p == 2);

                snprintf(json, sizeof(json),
                         "{\"listener\":{\"port\":4433,\"cert\":\"c\","
                         "\"key\":\"k\"},\"webtransport\":{\"port\":4443,"
                         "\"cert\":\"c\",\"key\":\"k\",\"profile\":\"%s\","
                         "\"origin_policy\":\"%s\"}}", prof[p], pol[i].tok);
                if (want_ok) {
                    MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err))
                                   == MOQR_OK);
                    MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_policy,
                                          (uint64_t)pol[i].want);
                    MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count, 0);
                } else if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                    printf("FAIL: %s without a policy was accepted\n", prof[p]);
                    failures++;
                }
            }
        }
    }
    /* an allowlist, on every profile, and in both field orders */
    {
        static const char *const prof[] = { "current", "d13_14_compat",
                                            "d02_rfc9297_compat" };
        for (size_t p = 0; p < sizeof(prof) / sizeof(prof[0]); p++) {
            static const char *const shape[] = {
                /* policy, then list, then profile */
                "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                "\"origin_policy\":\"allowlist\","
                "\"allowed_origins\":[\"%s\"],\"profile\":\"%s\"}}",
                /* list, then profile, then policy */
                "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                "\"allowed_origins\":[\"%s\"],\"profile\":\"%s\","
                "\"origin_policy\":\"allowlist\"}}",
            };
            for (size_t o = 0; o < sizeof(shape) / sizeof(shape[0]); o++) {
                char json[640];
                moqr_cli_config_t cfg;
                char err[256] = { 0 };
                snprintf(json, sizeof(json), shape[o], kSentinel, prof[p]);
                MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);
                MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_policy,
                                      (uint64_t)MOQR_CLI_ORIGIN_POLICY_ALLOWLIST);
                MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count, 1);
                MOQ_TEST_CHECK(cfg.wt.origins[0] != NULL &&
                               strcmp(cfg.wt.origins[0], kSentinel) == 0);
            }
        }
    }
    /* a list is refused under every policy that cannot use one, an explicitly
     * empty list included, and in both field orders */
    {
        static const char *const pol[] = { "unset", "allow_any_non_opaque",
                                           "allow_any_including_null" };
        static const char *const list[] = { "[\"https://a.example\"]", "[]" };
        for (size_t i = 0; i < sizeof(pol) / sizeof(pol[0]); i++) {
            for (size_t l = 0; l < sizeof(list) / sizeof(list[0]); l++) {
                char json[640];
                moqr_cli_config_t cfg;
                char err[256] = { 0 };

                snprintf(json, sizeof(json),
                         "{\"listener\":{\"port\":4433,\"cert\":\"c\","
                         "\"key\":\"k\"},\"webtransport\":{\"port\":4443,"
                         "\"cert\":\"c\",\"key\":\"k\",\"origin_policy\":"
                         "\"%s\",\"allowed_origins\":%s}}", pol[i], list[l]);
                if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                    printf("FAIL: a list under %s %s was accepted\n",
                           pol[i], list[l]);
                    failures++;
                }
                /* the reverse order gives the same verdict */
                snprintf(json, sizeof(json),
                         "{\"listener\":{\"port\":4433,\"cert\":\"c\","
                         "\"key\":\"k\"},\"webtransport\":{\"port\":4443,"
                         "\"cert\":\"c\",\"key\":\"k\",\"allowed_origins\":%s,"
                         "\"origin_policy\":\"%s\"}}", list[l], pol[i]);
                if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                    printf("FAIL: %s before %s was accepted\n",
                           list[l], pol[i]);
                    failures++;
                }
            }
        }
        /* and with no policy key at all */
        {
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            static const char *const j =
                "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                "\"allowed_origins\":[\"https://a.example\"]}}";
            MOQ_TEST_CHECK(parse(j, &cfg, err, sizeof(err)) != MOQR_OK);
        }
    }
    /* allowlist without a usable list is refused, in every shape */
    {
        static const char *const j[] = {
            /* no list at all */
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"origin_policy\":\"allowlist\"}}",
            /* an explicitly empty list */
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"origin_policy\":\"allowlist\",\"allowed_origins\":[]}}",
            /* the empty list written first */
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"allowed_origins\":[],\"origin_policy\":\"allowlist\"}}",
        };
        for (size_t i = 0; i < sizeof(j) / sizeof(j[0]); i++) {
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            if (parse(j[i], &cfg, err, sizeof(err)) == MOQR_OK) {
                printf("FAIL: allowlist shape %zu was accepted\n", i);
                failures++;
            }
        }
    }
    /* unknown policy tokens and near-misses fail closed */
    {
        static const char *const bad[] = {
            "allow_any", "allowlist ", "Allowlist", "ALLOWLIST", "allow",
            "allow_any_non_opaque_", "deny", "2", "",
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            char json[512];
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\",\"origin_policy\":\"%s\"}}", bad[i]);
            if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                printf("FAIL: policy token %s was accepted\n", bad[i]);
                failures++;
            }
        }
        /* a numeric policy is not a token */
        {
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            static const char *const j =
                "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                "\"origin_policy\":2}}";
            MOQ_TEST_CHECK(parse(j, &cfg, err, sizeof(err)) != MOQR_OK);
        }
        /* a list that is not an array is not a list */
        {
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            static const char *const j =
                "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                "\"origin_policy\":\"allowlist\","
                "\"allowed_origins\":\"https://a.example\"}}";
            MOQ_TEST_CHECK(parse(j, &cfg, err, sizeof(err)) != MOQR_OK);
        }
    }
    /* the closed numeric vocabulary is the dependency's and is pinned */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_ORIGIN_POLICY_UNSET, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE, 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQR_CLI_ORIGIN_POLICY_ALLOWLIST, 2);
    MOQ_TEST_CHECK_EQ_U64(
        (uint64_t)MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL, 3);
    return failures;
}

/* -- allowlist bounds and stored ownership --------------------------------- */

/*
 * Every entry is owned, and every limit is one the facade also enforces.
 *
 * The entries are copied into the config itself, so the document they came from
 * may be freed or reused the moment the parse returns. This overwrites the
 * caller's buffer before reading a single byte back: if the config were
 * borrowing, the comparison below would read the overwriting pattern.
 */
static int
t_wt_origin_storage(void)
{
    int failures = 0;

    /* the copied bytes survive the destruction of the document */
    {
        static const char *const kA = "https://alpha-7c21.example";
        static const char *const kB = "https://bravo-5e08.example:9443";
        char doc[512];
        moqr_cli_config_t cfg;
        char err[256] = { 0 };

        snprintf(doc, sizeof(doc),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"origin_policy\":\"allowlist\","
                 "\"allowed_origins\":[\"%s\",\"%s\"]}}", kA, kB);
        MOQ_TEST_CHECK(parse(doc, &cfg, err, sizeof(err)) == MOQR_OK);
        memset(doc, 'Z', sizeof(doc));

        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count, 2);
        MOQ_TEST_CHECK(cfg.wt.origins[0] != NULL &&
                       strcmp(cfg.wt.origins[0], kA) == 0);
        MOQ_TEST_CHECK(cfg.wt.origins[1] != NULL &&
                       strcmp(cfg.wt.origins[1], kB) == 0);
        /* every stored pointer addresses this config's own buffer, in order,
         * NUL-terminated, and nothing past the last entry is claimed */
        for (size_t i = 0; i < cfg.wt.origin_count; i++) {
            const char *p = cfg.wt.origins[i];
            MOQ_TEST_CHECK(p >= cfg.wt.origin_buf &&
                           p < cfg.wt.origin_buf + sizeof(cfg.wt.origin_buf));
            MOQ_TEST_CHECK(p + strlen(p) <
                           cfg.wt.origin_buf + sizeof(cfg.wt.origin_buf));
            if (i > 0) {
                MOQ_TEST_CHECK(p == cfg.wt.origins[i - 1] +
                                    strlen(cfg.wt.origins[i - 1]) + 1);
            }
        }
        MOQ_TEST_CHECK(cfg.wt.origins[0] == cfg.wt.origin_buf);
        for (size_t i = cfg.wt.origin_count; i < MOQR_CLI_MAX_ORIGINS; i++) {
            MOQ_TEST_CHECK(cfg.wt.origins[i] == NULL);
        }
        /* byte for byte, including both terminators */
        MOQ_TEST_CHECK(memcmp(cfg.wt.origin_buf, kA, strlen(kA) + 1) == 0);
        MOQ_TEST_CHECK(memcmp(cfg.wt.origin_buf + strlen(kA) + 1, kB,
                              strlen(kB) + 1) == 0);
    }
    /* eight entries are accepted; nine are not */
    {
        for (size_t n = 1; n <= MOQR_CLI_MAX_ORIGINS + 1; n++) {
            char json[1024];
            char list[768];
            size_t off = 0;
            moqr_cli_config_t cfg;
            char err[256] = { 0 };

            list[off++] = '[';
            for (size_t i = 0; i < n; i++) {
                off += (size_t)snprintf(list + off, sizeof(list) - off,
                                        "%s\"https://e%zu.example\"",
                                        i ? "," : "", i);
            }
            off += (size_t)snprintf(list + off, sizeof(list) - off, "]");
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\",\"origin_policy\":\"allowlist\","
                     "\"allowed_origins\":%s}}", list);
            if (n <= MOQR_CLI_MAX_ORIGINS) {
                MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);
                MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count,
                                      (uint64_t)n);
            } else if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                printf("FAIL: %zu origin entries were accepted\n", n);
                failures++;
            }
        }
    }
    /* 320 bytes is the longest entry; 321 is not; 0 is not either */
    {
        static const size_t kLens[] = { 1, MOQR_CLI_MAX_ORIGIN_BYTES,
                                        MOQR_CLI_MAX_ORIGIN_BYTES + 1, 0 };
        for (size_t i = 0; i < sizeof(kLens) / sizeof(kLens[0]); i++) {
            char json[1024];
            char entry[MOQR_CLI_MAX_ORIGIN_BYTES + 2];
            moqr_cli_config_t cfg;
            char err[256] = { 0 };
            bool want_ok = kLens[i] >= 1 &&
                           kLens[i] <= MOQR_CLI_MAX_ORIGIN_BYTES;

            memset(entry, 'a', kLens[i]);
            entry[kLens[i]] = '\0';
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\",\"origin_policy\":\"allowlist\","
                     "\"allowed_origins\":[\"%s\"]}}", entry);
            if (want_ok) {
                if (parse(json, &cfg, err, sizeof(err)) != MOQR_OK ||
                    cfg.wt.origins[0] == NULL) {
                    printf("FAIL: a %zu-byte entry was refused: %s\n",
                           kLens[i], err);
                    failures++;
                } else {
                    MOQ_TEST_CHECK_EQ_U64((uint64_t)strlen(cfg.wt.origins[0]),
                                          (uint64_t)kLens[i]);
                }
            } else if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                printf("FAIL: an entry of %zu bytes was accepted\n", kLens[i]);
                failures++;
            }
        }
    }
    /* the total copy budget is 512 bytes INCLUDING every terminator: two
     * entries of 255 bytes fit exactly, and one more byte does not */
    {
        static const struct { size_t a; size_t b; bool ok; } kCases[] = {
            { 255, 255, true },   /* 256 + 256 == 512 */
            { 255, 256, false },  /* 256 + 257 == 513 */
        };
        for (size_t i = 0; i < sizeof(kCases) / sizeof(kCases[0]); i++) {
            char json[1400];
            char a[MOQR_CLI_ORIGIN_COPY_BUDGET];
            char b[MOQR_CLI_ORIGIN_COPY_BUDGET];
            moqr_cli_config_t cfg;
            char err[256] = { 0 };

            memset(a, 'a', kCases[i].a); a[kCases[i].a] = '\0';
            memset(b, 'b', kCases[i].b); b[kCases[i].b] = '\0';
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\",\"origin_policy\":\"allowlist\","
                     "\"allowed_origins\":[\"%s\",\"%s\"]}}", a, b);
            if (kCases[i].ok) {
                if (parse(json, &cfg, err, sizeof(err)) != MOQR_OK ||
                    cfg.wt.origin_count != 2 || cfg.wt.origins[1] == NULL) {
                    printf("FAIL: %zu + %zu bytes did not fit the budget: %s\n",
                           kCases[i].a, kCases[i].b, err);
                    failures++;
                } else {
                    MOQ_TEST_CHECK(strlen(cfg.wt.origins[1]) == kCases[i].b);
                    /* the last terminator is the last byte of the buffer */
                    MOQ_TEST_CHECK(cfg.wt.origins[1] + kCases[i].b ==
                                   cfg.wt.origin_buf +
                                   sizeof(cfg.wt.origin_buf) - 1);
                }
            } else if (parse(json, &cfg, err, sizeof(err)) == MOQR_OK) {
                printf("FAIL: %zu + %zu bytes fit the budget\n",
                       kCases[i].a, kCases[i].b);
                failures++;
            }
        }
    }
    /* duplicate entries are refused by exact bytes, and a near-miss is not a
     * duplicate */
    {
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        static const char *const kDup =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"origin_policy\":\"allowlist\",\"allowed_origins\":"
            "[\"https://a.example\",\"https://a.example\"]}}";
        static const char *const kNear =
            "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
            "\"origin_policy\":\"allowlist\",\"allowed_origins\":"
            "[\"https://a.example\",\"https://a.example:443\"]}}";
        MOQ_TEST_CHECK(parse(kDup, &cfg, err, sizeof(err)) != MOQR_OK);
        MOQ_TEST_CHECK(parse(kNear, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.origin_count, 2);
    }
    /* nothing normalizes an entry: exact bytes go in and exact bytes come out */
    {
        static const char *const kExact = "HTTPS://Mixed.Case.Example:443/";
        char json[512];
        moqr_cli_config_t cfg;
        char err[256] = { 0 };
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"origin_policy\":\"allowlist\",\"allowed_origins\":[\"%s\"]}}",
                 kExact);
        MOQ_TEST_CHECK(parse(json, &cfg, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(strcmp(cfg.wt.origins[0], kExact) == 0);
    }
    return failures;
}

/* -- refusals never quote what the operator wrote -------------------------- */

/*
 * A refusal names the key, the closed vocabulary, the index and the limit. It
 * never echoes the value it rejected: an error line is copied into tickets,
 * logs and chat, and a rejected Origin or credential path is exactly the thing
 * that must not travel there.
 */
static int
t_wt_error_omission(void)
{
    int failures = 0;
    static const struct { const char *what; const char *json;
                          const char *secret; } kCases[] = {
        { "an unknown profile token",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"profile\":\"zqx-secret-token-4417\"}}", "zqx-secret-token-4417" },
        { "an unknown policy token",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"zqx-policy-8823\"}}", "zqx-policy-8823" },
        { "an origin under a policy that takes no list",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"allow_any_non_opaque\",\"allowed_origins\":"
          "[\"https://zqx-origin-9931.example\"]}}",
          "zqx-origin-9931" },
        { "a duplicated origin entry",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"origin_policy\":\"allowlist\",\"allowed_origins\":"
          "[\"https://zqx-dup-2277.example\",\"https://zqx-dup-2277.example\"]}}",
          "zqx-dup-2277" },
        { "an unknown webtransport key",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
          "\"zqx-unknown-3355\":1}}", "zqx-unknown-3355" },
        { "a credential path with an embedded NUL",
          "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
          "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
          "\"key\":\"/zqx-key-6644.pem\\u0000/x\"}}", "zqx-key-6644" },
    };
    for (size_t i = 0; i < sizeof(kCases) / sizeof(kCases[0]); i++) {
        moqr_cli_config_t cfg;
        char err[512] = { 0 };
        if (parse(kCases[i].json, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("FAIL: %s was accepted\n", kCases[i].what);
            failures++;
            continue;
        }
        if (err[0] == '\0') {
            printf("FAIL: %s produced no message\n", kCases[i].what);
            failures++;
            continue;
        }
        if (strstr(err, kCases[i].secret) != NULL) {
            printf("FAIL: the message for %s quoted the rejected value\n",
                   kCases[i].what);
            failures++;
        }
        /* it does name the key it refused, so the message is still useful */
        if (strstr(err, "webtransport") == NULL) {
            printf("FAIL: the message for %s names no key\n", kCases[i].what);
            failures++;
        }
    }
    return failures;
}

int
main(void)
{
    int rc = 0;

    rc |= t_legacy_raw_only_unchanged();
    rc |= t_dual_config_accepted();
    rc |= t_wt_malformed_fails_closed();
    rc |= t_shard_plan_disjoint();
    rc |= t_shard_plan_raw_only();
    rc |= t_shard_plan_overflow_refuses();
    rc |= t_facade_caps_partition_the_total();
    rc |= t_wt_version_label_is_its_own();
    rc |= t_wt_profile_tokens();
    rc |= t_wt_key_boundary();
    rc |= t_wt_origin_policy();
    rc |= t_wt_origin_storage();
    rc |= t_wt_error_omission();
    if (rc == 0) {
        printf("PASS: relay_dual_listener\n");
    }
    return rc;
}
