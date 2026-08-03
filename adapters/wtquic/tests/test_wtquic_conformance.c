/*
 * Production-contract conformance for the wtquic adapter, over real
 * MsQuic loopback. Each scenario loads per-side scripts, brings the
 * pair up, waits for observable progress, tears down, and asserts the
 * observations. All MoQ session calls run inside the adapter hook on
 * the transport worker — the mode applications actually run.
 *
 * Semantically aligned with the shared conformance scenarios
 * (tests/conformance/conformance_scenarios.c) where the production
 * threading contract allows. Deliberately not covered here:
 *   - datagram scenarios: the adapter does not advertise MoQ datagram
 *     publishing yet, so there is nothing truthful to run;
 *   - timer/virtual-time scenarios (GOAWAY drain timeout): the adapter
 *     has no idle deadline tick integration;
 *   - fault-injection and stream/tombstone-count scenarios: they need
 *     introspection and write-blocking hooks a real transport rail
 *     cannot express (the fake rails cover them);
 *   - bidi half-close injection: needs raw FIN injection below the
 *     adapter (fake rails only);
 *   - single-control-stream accounting: needs adapter-internal stream
 *     counters; the WebTransport layer's single-control invariant is
 *     pinned by wtquic's own engine suite.
 */

#include <stdio.h>
#include <string.h>

#include "wtquic_contract_pair.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static const char *cert_path;
static const char *key_path;

static void default_cfg(wtqc_pair_cfg_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->cert = cert_path;
    cfg->key = key_path;
    cfg->client_offer = "moqt-16";
    cfg->server_serve = "moqt-16";
}

/* Session setup completes on both sides over the negotiated
 * subprotocol; the clean close that follows is terminal-not-fatal. */
static void t_setup_handshake(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 1;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.setup, 1));
    CHECK(wtqc_wait_count(&p.server, &p.server.obs.setup, 1));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.setup == 1);
    CHECK(p.server.obs.setup == 1);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: setup_handshake\n");
}

/* Subscribe, accept, one verified object, clean close — and the dead
 * session refuses further requests after its terminal. */
static void t_subscribe_and_object(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS,
                                       .when_arg = 1,
                                       .act = WTQC_DO_WT_CLOSE,
                                       .a1 = 4 };
    p.client.steps[2] =
        (wtqc_step_t){ .when = WTQC_ON_CLOSED, .act = WTQC_DO_PROBE_DEAD };
    p.client.step_count = 3;
    p.client.expect_objects = 1;
    p.client.expect_size = 64;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 64 };
    p.server.step_count = 2;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.sub_ok, 1));
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects, 1));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.server.obs.sub_request == 1);
    CHECK(p.client.obs.sub_ok == 1);
    CHECK(p.client.obs.objects == 1);
    CHECK(p.client.obs.object_bytes == 64);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    /* dead-session behavior: the probe ran after the terminal and was
     * refused */
    CHECK(p.client.obs.probe_ran);
    CHECK(p.client.obs.probe_rc < 0);
    if (failures == before)
        printf("PASS: subscribe_and_object\n");
}

/* A clean WebTransport close is terminal on both sides without ever
 * being fatal, and the session-closed event reaches the application. */
static void t_clean_close_not_fatal(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SETUP,
                                       .act = WTQC_DO_WT_CLOSE,
                                       .a1 = 9 };
    p.client.step_count = 1;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    CHECK(p.client.obs.session_closed >= 1);
    CHECK(p.server.obs.session_closed >= 1);
    if (failures == before)
        printf("PASS: clean_close_not_fatal\n");
}

/* A client requiring a version the server does not serve is refused at
 * the WebTransport layer: fatal terminal, no setup, no clean close. */
static void t_version_refusal_fatal(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    cfg.client_offer = "moqt-18";

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.fatal));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.setup == 0);
    CHECK(p.client.obs.fatal);
    if (failures == before)
        printf("PASS: version_refusal_fatal\n");
}

/*
 * Resetting a subgroup mid-track propagates without poisoning the
 * session: a follow-up publish on a fresh subgroup still delivers and
 * the close stays clean. The pre-reset object itself is deliberately
 * NOT asserted — without RESET_STREAM_AT (which MsQuic cannot
 * negotiate) an abort may legally discard bytes that were queued but
 * not yet transmitted, so its delivery is a race, not a contract.
 */
static void t_reset_propagation(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    /* close only once the FRESH group's object arrived — the raced
     * pre-reset object must not end the run early */
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS_G1,
                                       .when_arg = 1,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;

    /* both publishes fire in the same batch, well before any close can
     * arrive back from the client */
    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH_RESET,
                                       .a1 = 0x77,
                                       .a2 = 32 };
    p.server.steps[2] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 48 };
    p.server.step_count = 3;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects_g1, 1));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    /* the fresh-group follow-up demonstrably arrived (plus at most the
     * raced pre-reset object) */
    CHECK(p.client.obs.objects_g1 == 1);
    CHECK(p.client.obs.objects >= 1 && p.client.obs.objects <= 2);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: reset_propagation\n");
}

/* Sustained multi-object delivery: a 40-object burst drives the
 * bridge's outbound machinery without a single sleep. Every object
 * must arrive intact. (Budget parking is exercised separately by
 * parked_publish_retry — this schedule stays under the transport's
 * 1 MiB per-stream floor.) */
static void t_bulk_delivery_pressure(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;
    enum { OBJECTS = 40, SIZE = 8192 };

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS,
                                       .when_arg = OBJECTS,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;
    p.client.expect_objects = OBJECTS;
    p.client.expect_size = SIZE;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.step_count = 2;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects, OBJECTS));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == OBJECTS);
    CHECK(p.client.obs.object_bytes == (uint64_t)OBJECTS * SIZE);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: bulk_delivery_pressure\n");
}

/* Parked-publish progress with NO application wakes anywhere: three
 * 600 KiB objects queued in one pump batch park in the bridge behind
 * the transport's 1 MiB per-stream budget. This rail is attach-style —
 * the adapter hook runs only on wtquic transport events — so all three
 * objects arriving intact proves the parked sends were retried purely
 * from transport signals (send completions and the stream-writable
 * edge), never from a timer or an app-side wake. */
static void t_parked_publish_retry(void)
{
    int before = failures;
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;
    enum { OBJECTS = 3 };
    const uint64_t SIZE = 600 * 1024;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS,
                                       .when_arg = OBJECTS,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;
    p.client.expect_objects = OBJECTS;
    p.client.expect_size = SIZE;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.step_count = 2;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects, OBJECTS));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == OBJECTS);
    CHECK(p.client.obs.object_bytes == (uint64_t)OBJECTS * SIZE);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: parked_publish_retry\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];

    t_setup_handshake();
    t_subscribe_and_object();
    t_clean_close_not_fatal();
    t_version_refusal_fatal();
    t_reset_propagation();
    t_bulk_delivery_pressure();
    t_parked_publish_retry();

    /* intentional gaps (see the header comment for the reasoning) */
    printf("SKIP: datagram publishing (capability not advertised)\n");
    printf("SKIP: timer/virtual-time scenarios (no deadline tick)\n");
    printf("SKIP: fault-injection and count-introspection scenarios "
           "(fake rails only)\n");

    if (failures == 0)
        printf("PASS: wtquic_conformance\n");
    else
        fprintf(stderr, "FAIL: wtquic_conformance (%d)\n", failures);
    return failures;
}
