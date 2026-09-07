/*
 * What the WebTransport transport is actually handed.
 *
 * The relay's second listener is created from the resolved configuration, and
 * every field of that hand-off is a promise: the wrong path serves the wrong
 * URL, the wrong cap admits the wrong number of connections, and a dropped
 * Origin policy turns an allowlist into an open door. None of that is visible
 * from a running relay, and none of it needs a transport to check -- so the two
 * transport callees are replaced at compile time and the configuration is
 * captured at the exact call the real facade would have received.
 */
#include <stdio.h>
#include <string.h>

#include "wtcfg_substitute.h"

#include "config.h"
#include "wtcfg.h"

#include "../../../tests/unit/test_support.h"

relay_wt_capture_t g_relay_wt_capture;

void
relay_test_wt_cfg_init_sized(moq_wtquic_msquic_managed_cfg_t *cfg, size_t size)
{
    g_relay_wt_capture.init_called = true;
    g_relay_wt_capture.init_size = size;
    g_relay_wt_capture.init_pending = cfg;
    /* the real initializer zeroes the caller's prefix; matching that keeps
     * every field this test reads deterministic */
    memset(cfg, 0, size);
    cfg->struct_size = (uint32_t)size;
}

moq_result_t
relay_test_wt_create(const moq_wtquic_msquic_managed_cfg_t *cfg,
                     moq_wtquic_msquic_managed_t **out)
{
    g_relay_wt_capture.create_called = true;
    g_relay_wt_capture.create_calls++;
    /* Decided here, while the step's automatic configuration is still alive:
     * create must be handed the object the initializer prepared. Comparing the
     * two addresses after the step returns would compare pointers into a dead
     * frame. */
    g_relay_wt_capture.init_same_object =
        g_relay_wt_capture.init_pending == cfg;
    g_relay_wt_capture.init_pending = NULL;
    g_relay_wt_capture.cfg = *cfg;
    g_relay_wt_capture.out = out;
    return g_relay_wt_capture.result;
}

static moqr_result_t
parse(const char *json, moqr_cli_config_t *out, char *err, size_t errlen)
{
    return moqr_cli_config_parse(json, strlen(json), out, err, errlen);
}

/* a pump of the production signature; only its identity is under test */
static int
test_pump(moq_wtquic_msquic_managed_t *m,
          moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us, void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    return 0;
}

static const char *const kAllowlistJson =
    "{\"listener\":{\"port\":4433,\"cert\":\"/raw.pem\",\"key\":\"/raw.key\"},"
    "\"webtransport\":{\"host\":\"127.0.0.2\",\"port\":4711,"
    "\"cert\":\"/wt-cert-7a19.pem\",\"key\":\"/wt-key-3c52.key\","
    "\"path\":\"/relay\",\"lanes\":3,\"versions\":[16,18],"
    "\"profile\":\"d02_rfc9297_compat\","
    "\"origin_policy\":\"allowlist\","
    "\"allowed_origins\":[\"https://first-8821.example\","
    "\"https://second-4407.example:9443\"]}}";

static int
build(const char *json, moqr_cli_config_t *cfg, uint32_t lanes, uint32_t caps,
      void *user, moq_wtquic_msquic_managed_t **out, moq_result_t want)
{
    char err[256] = { 0 };
    moq_result_t rc;

    memset(&g_relay_wt_capture, 0, sizeof(g_relay_wt_capture));
    g_relay_wt_capture.result = want;
    if (parse(json, cfg, err, sizeof(err)) != MOQR_OK) {
        printf("FAIL: the fixture was rejected: %s\n", err);
        return 1;
    }
    rc = moqr_cli_wt_listener_create(cfg, lanes, caps, test_pump, user, out);
    if (rc != want) {
        printf("FAIL: create returned %d, not the transport's %d\n",
               (int)rc, (int)want);
        return 1;
    }
    return 0;
}

/* -- the complete configuration, at the create call ------------------------ */
static int
t_full_configuration(void)
{
    int failures = 0;
    static moqr_cli_config_t cfg;
    moq_wtquic_msquic_managed_t *m = NULL;
    void *const user = (void *)&cfg;

    failures += build(kAllowlistJson, &cfg, 3, 4096, user, &m, MOQ_OK);
    if (failures != 0) {
        return failures;
    }
    /* the sized initializer ran, with the REAL full size of the struct the
     * relay compiled against -- not a V0 floor and not a guess */
    MOQ_TEST_CHECK(g_relay_wt_capture.init_called);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.init_size,
                          (uint64_t)sizeof(moq_wtquic_msquic_managed_cfg_t));
    /* and it initialized the very object create was then handed, decided
     * inside the create call while that object was still alive */
    MOQ_TEST_CHECK(g_relay_wt_capture.init_same_object);
    MOQ_TEST_CHECK(g_relay_wt_capture.create_called);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.create_calls, 1);
    MOQ_TEST_CHECK(g_relay_wt_capture.out == &m);

    {
        const moq_wtquic_msquic_managed_cfg_t *c = &g_relay_wt_capture.cfg;

        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->struct_size,
                              (uint64_t)sizeof(*c));
        MOQ_TEST_CHECK(c->alloc == moq_alloc_default());
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->perspective,
                              (uint64_t)MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(c->host != NULL && strcmp(c->host, "127.0.0.2") == 0);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->port, 4711);
        MOQ_TEST_CHECK(c->cert_path != NULL &&
                       strcmp(c->cert_path, "/wt-cert-7a19.pem") == 0);
        MOQ_TEST_CHECK(c->key_path != NULL &&
                       strcmp(c->key_path, "/wt-key-3c52.key") == 0);
        MOQ_TEST_CHECK(c->send_request_capacity);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->initial_request_capacity, 1024);
        MOQ_TEST_CHECK(c->streaming_objects);
        MOQ_TEST_CHECK(c->wt_path != NULL && strcmp(c->wt_path, "/relay") == 0);
        /* the offered subprotocols, in the order the operator wrote them */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->wt_protocol_count, 2);
        MOQ_TEST_CHECK(c->wt_protocols ==
                       (const char *const *)cfg.wt.subprotos);
        MOQ_TEST_CHECK(strcmp(c->wt_protocols[0], "moqt-16") == 0);
        MOQ_TEST_CHECK(strcmp(c->wt_protocols[1], "moqt-18") == 0);
        /* one profile, carried through unnarrowed */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->webtransport_profile,
                              (uint64_t)MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT);
        /* this listener's own share of the plan, not the process totals */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->lane_count, 3);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->max_connections, 4096);
        /* the pump and its context by identity */
        MOQ_TEST_CHECK(c->on_lane_pump == test_pump);
        MOQ_TEST_CHECK(c->on_lane_pump_user == user);
        /* the Origin policy and its list */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->origin_policy,
                              (uint64_t)MOQR_CLI_ORIGIN_POLICY_ALLOWLIST);
        MOQ_TEST_CHECK(c->allowed_origins ==
                       (const char *const *)cfg.wt.origins);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)c->allowed_origin_count, 2);
        if (c->allowed_origins == NULL || c->allowed_origin_count != 2) {
            printf("FAIL: the allowlist did not reach the transport\n");
            failures++;
        } else {
            MOQ_TEST_CHECK(strcmp(c->allowed_origins[0],
                                  "https://first-8821.example") == 0);
            MOQ_TEST_CHECK(strcmp(c->allowed_origins[1],
                                  "https://second-4407.example:9443") == 0);
        }
        /* a server offers no Origin of its own */
        MOQ_TEST_CHECK(c->origin == NULL);
    }
    return failures;
}

/* -- a list travels with ALLOWLIST and with nothing else ------------------- */
/*
 * The config's Origin array is embedded, so its address is never NULL. Handing
 * that address over under a policy that takes no list is the difference between
 * a relay that starts and one the facade refuses, which is why NULL here is a
 * value and not an absence.
 */
static int
t_policy_list_pairing(void)
{
    int failures = 0;
    static const struct { const char *pol; uint32_t want; } kRows[] = {
        { NULL,                       MOQR_CLI_ORIGIN_POLICY_UNSET },
        { "unset",                    MOQR_CLI_ORIGIN_POLICY_UNSET },
        { "allow_any_non_opaque",
          MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE },
        { "allow_any_including_null",
          MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL },
    };
    for (size_t i = 0; i < sizeof(kRows) / sizeof(kRows[0]); i++) {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        char json[640];

        if (kRows[i].pol == NULL) {
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\"}}");
        } else {
            snprintf(json, sizeof(json),
                     "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                     "\"webtransport\":{\"port\":4443,\"cert\":\"c\","
                     "\"key\":\"k\",\"origin_policy\":\"%s\"}}", kRows[i].pol);
        }
        failures += build(json, &cfg, 1, 8, NULL, &m, MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.cfg.origin_policy,
                              (uint64_t)kRows[i].want);
        if (g_relay_wt_capture.cfg.allowed_origins != NULL) {
            printf("FAIL: policy %s forwarded a non-NULL list\n",
                   kRows[i].pol ? kRows[i].pol : "(absent)");
            failures++;
        }
        MOQ_TEST_CHECK_EQ_U64(
            (uint64_t)g_relay_wt_capture.cfg.allowed_origin_count, 0);
    }
    /* and the allowlist case does forward exactly the owned array */
    {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        failures += build(kAllowlistJson, &cfg, 1, 8, NULL, &m, MOQ_OK);
        MOQ_TEST_CHECK(g_relay_wt_capture.cfg.allowed_origins ==
                       (const char *const *)cfg.wt.origins);
        MOQ_TEST_CHECK_EQ_U64(
            (uint64_t)g_relay_wt_capture.cfg.allowed_origin_count, 2);
    }
    return failures;
}

/* -- every profile reaches the transport as itself ------------------------- */
static int
t_profile_pass_through(void)
{
    int failures = 0;
    static const struct { const char *tok; uint32_t want; } kRows[] = {
        { "current",            MOQR_CLI_WT_PROFILE_CURRENT },
        { "d13_14_compat",      MOQR_CLI_WT_PROFILE_D13_14_COMPAT },
        { "d02_rfc9297_compat", MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT },
    };
    for (size_t i = 0; i < sizeof(kRows) / sizeof(kRows[0]); i++) {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        char json[640];

        snprintf(json, sizeof(json),
                 "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"},"
                 "\"webtransport\":{\"port\":4443,\"cert\":\"c\",\"key\":\"k\","
                 "\"origin_policy\":\"allow_any_non_opaque\","
                 "\"profile\":\"%s\"}}", kRows[i].tok);
        failures += build(json, &cfg, 1, 8, NULL, &m, MOQ_OK);
        if (g_relay_wt_capture.cfg.webtransport_profile != kRows[i].want) {
            printf("FAIL: profile %s reached the transport as %u\n",
                   kRows[i].tok,
                   (unsigned)g_relay_wt_capture.cfg.webtransport_profile);
            failures++;
        }
    }
    return failures;
}

/* -- the transport's result is the caller's result ------------------------- */
/*
 * Nothing here maps, softens or retries a native result: a wrapper that always
 * succeeded would let the relay announce readiness over a listener that never
 * came up.
 */
static int
t_result_propagation(void)
{
    int failures = 0;
    static const moq_result_t kResults[] = {
        MOQ_OK, MOQ_ERR_INVAL, MOQ_ERR_NOMEM, MOQ_ERR_WRONG_STATE,
    };
    for (size_t i = 0; i < sizeof(kResults) / sizeof(kResults[0]); i++) {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        /* build() already refuses any result other than the one it armed */
        failures += build(kAllowlistJson, &cfg, 2, 64, NULL, &m, kResults[i]);
        MOQ_TEST_CHECK(g_relay_wt_capture.create_called);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.create_calls, 1);
    }
    return failures;
}

/* -- the lane and cap partition is this listener's own --------------------- */
static int
t_partition(void)
{
    int failures = 0;
    static const struct { uint32_t lanes; uint32_t caps; } kRows[] = {
        { 1, 1 }, { 2, 64 }, { 7, 4096 }, { 64, UINT32_MAX },
    };
    for (size_t i = 0; i < sizeof(kRows) / sizeof(kRows[0]); i++) {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        failures += build(kAllowlistJson, &cfg, kRows[i].lanes, kRows[i].caps,
                          NULL, &m, MOQ_OK);
        /* the config's own `lanes` is NOT what the facade is told: the plan is */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.cfg.lane_count,
                              (uint64_t)kRows[i].lanes);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.cfg.max_connections,
                              (uint64_t)kRows[i].caps);
    }
    /* the fixture asks for 3 lanes, so a step that forwarded cfg->wt.lanes
     * instead of the plan would have passed every row above by accident */
    {
        static moqr_cli_config_t cfg;
        moq_wtquic_msquic_managed_t *m = NULL;
        failures += build(kAllowlistJson, &cfg, 5, 11, NULL, &m, MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.wt.lanes, 3);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_relay_wt_capture.cfg.lane_count, 5);
    }
    return failures;
}

int
main(void)
{
    int rc = 0;
    rc |= t_full_configuration();
    rc |= t_policy_list_pairing();
    rc |= t_profile_pass_through();
    rc |= t_result_propagation();
    rc |= t_partition();
    if (rc == 0) {
        printf("PASS: relay_wt_cfg\n");
    }
    return rc == 0 ? 0 : 1;
}
