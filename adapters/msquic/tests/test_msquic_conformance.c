/*
 * Production-contract conformance for the direct MsQuic adapter, over
 * real MsQuic loopback through the managed facade. Each scenario loads
 * per-side scripts, brings the pair up, waits for observable progress,
 * tears down, and asserts the observations. All MoQ session calls run
 * inside on_lane_pump — the mode applications actually run.
 *
 * Semantically aligned with the shared conformance scenarios
 * (tests/conformance/conformance_scenarios.c) where the production
 * threading contract and current adapter scope allow. Deliberately not
 * covered here (runtime SKIPs name each):
 *   - timer/virtual-time scenarios (GOAWAY drain timeout): only real
 *     time exists here; the managed doorbell services deadlines but
 *     scenarios cannot fast-forward them;
 *   - fault-injection and stream/tombstone-count scenarios: they need
 *     introspection and write-blocking hooks a real transport rail
 *     cannot express (the fake rails cover them);
 *   - bidi half-close injection: needs raw FIN injection below the
 *     adapter (fake rails only).
 */

#include <stdio.h>
#include <string.h>

#include "msquic_contract_pair.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static const char *cert_path;
static const char *key_path;

static void default_cfg(msqc_pair_cfg_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->cert = cert_path;
    cfg->key = key_path;
}

/* Session setup completes on both sides; the clean close that follows
 * is terminal-not-fatal. */
static void t_setup_handshake(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_CLOSE };
    p.client.step_count = 1;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.setup, 1));
    CHECK(msqc_wait_count(&p.server, &p.server.obs.setup, 1));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

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
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = 1,
                                       .act = MSQC_DO_CLOSE,
                                       .a1 = 4 };
    p.client.steps[2] =
        (msqc_step_t){ .when = MSQC_ON_CLOSED, .act = MSQC_DO_PROBE_DEAD };
    p.client.step_count = 3;
    p.client.expect_size = 64;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 64 };
    p.server.step_count = 2;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.sub_ok, 1));
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, 1));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(p.server.obs.sub_request == 1);
    CHECK(p.client.obs.sub_ok == 1);
    CHECK(p.client.obs.objects == 1);
    CHECK(p.client.obs.object_bytes == 64);
    CHECK(p.client.obs.seen_g0 == 0x1); /* exactly object id 0 */
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

/* A clean session close is terminal on both sides without ever being
 * fatal, and the session-closed event reaches the application. */
static void t_clean_close_not_fatal(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] = (msqc_step_t){ .when = MSQC_ON_SETUP,
                                       .act = MSQC_DO_CLOSE,
                                       .a1 = 9 };
    p.client.step_count = 1;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    CHECK(p.client.obs.session_closed >= 1);
    CHECK(p.server.obs.session_closed >= 1);
    if (failures == before)
        printf("PASS: clean_close_not_fatal\n");
}

/*
 * Resetting a subgroup mid-track propagates without poisoning the
 * session: a follow-up publish on a fresh subgroup still delivers and
 * the close stays clean. The pre-reset object itself is deliberately
 * NOT asserted — an abort may legally discard bytes that were queued
 * but not yet transmitted, so its delivery is a race, not a contract.
 */
static void t_reset_propagation(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    /* close only once the FRESH group's object arrived — the raced
     * pre-reset object must not end the run early */
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS_G1,
                                       .when_arg = 1,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 2;
    p.client.expect_size = 32;    /* raced pre-reset object (group 0) */
    p.client.expect_size_g1 = 48; /* the fresh-group follow-up */

    /* both publishes fire in the same batch, well before any close can
     * arrive back from the client */
    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH_RESET,
                                       .a1 = 0x77,
                                       .a2 = 32 };
    p.server.steps[2] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 48 };
    p.server.step_count = 3;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects_g1, 1));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    /* the fresh-group follow-up demonstrably arrived — the right
     * object, size and content in group 1 (plus at most the raced
     * pre-reset object in group 0) */
    CHECK(p.client.obs.objects_g1 == 1);
    CHECK(p.client.obs.seen_g1 == 0x1);
    CHECK(p.client.obs.seen_g0 == 0x0 || p.client.obs.seen_g0 == 0x1);
    CHECK(p.client.obs.objects >= 1 && p.client.obs.objects <= 2);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: reset_propagation\n");
}

/* Sustained multi-object delivery: eight objects queued in one publish
 * batch drive the staged-write coalescing path end to end. Every
 * object must arrive intact. (Budget parking is exercised separately
 * by parked_publish_retry — this schedule stays under the 1 MiB
 * per-stream floor.) */
static void t_bulk_delivery_pressure(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;
    enum { OBJECTS = 8, SIZE = 96 * 1024 };

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = OBJECTS,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 2;
    p.client.expect_size = SIZE;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.step_count = 2;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, OBJECTS));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == OBJECTS);
    CHECK(p.client.obs.object_bytes == (uint64_t)OBJECTS * SIZE);
    /* every id 0..N-1 exactly once: duplicates would have flagged an
     * error, a missing id would leave a hole in the mask */
    CHECK(p.client.obs.seen_g0 == (1ull << OBJECTS) - 1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: bulk_delivery_pressure\n");
}

/* Parked-publish retry: three 600 KiB objects are queued in a single
 * pump batch, so the writes behind the 1 MiB per-stream budget floor
 * park in the bridge as WOULD_BLOCK. No later pump action publishes
 * anything — forward progress past the first ~1 MiB exists ONLY if the
 * SEND_COMPLETE-driven service pass retries the parked sends. All
 * three objects arriving intact is that retry, observed on the wire. */
static void t_parked_publish_retry(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;
    enum { OBJECTS = 3 };
    const uint64_t SIZE = 600 * 1024;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = OBJECTS,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 2;
    p.client.expect_size = SIZE;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.step_count = 2;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, OBJECTS));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == OBJECTS);
    CHECK(p.client.obs.object_bytes == (uint64_t)OBJECTS * SIZE);
    CHECK(p.client.obs.seen_g0 == (1ull << OBJECTS) - 1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    /* every parked-then-retried send completed before the terminal */
    CHECK(p.server.obs.pending_sends == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: parked_publish_retry\n");
}

/* A datagram object and a status-only datagram both reach the
 * subscriber with datagram=true — the payload one byte-exact, the
 * status one payload-less — and the close stays clean. Loopback QUIC
 * datagrams are effectively lossless (no path drops), so exact counts
 * are honest here. */
static void t_datagram_object(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = 2,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 2;
    p.client.expect_size = 64; /* the group-0 payload datagram */
    /* the status datagram must be exactly what the server scripted:
     * group 1, object 0, END_OF_GROUP */
    p.client.expect_status_set = true;
    p.client.expect_status_group = 1;
    p.client.expect_status_object = 0;
    p.client.expect_status_value = MOQ_OBJECT_END_OF_GROUP;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH_DGRAM,
                                       .a1 = 0,
                                       .a2 = 64 };
    p.server.steps[2] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH_DGRAM_STATUS,
                                       .a1 = 1 };
    p.server.step_count = 3;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, 2));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == 2);
    CHECK(p.client.obs.dgram_objects == 2);
    CHECK(p.client.obs.status_objects == 1);
    CHECK(p.client.obs.seen_g0 == 0x1); /* the payload datagram, exact */
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: datagram_object\n");
}

/* A client offering a version the server does not serve is refused at
 * the ALPN handshake: the client observes a bounded fatal terminal
 * with no MoQ setup and nothing left in flight; the server never
 * accepts the connection (its session is never even created). */
static void t_version_refusal(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    cfg.client_version = MOQ_VERSION_DRAFT_18;
    cfg.server_version = MOQ_VERSION_DRAFT_16;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.fatal));
    /* the server never saw an acceptable connection (ALPN mismatch is
     * rejected before NEW_CONNECTION, so no child is ever admitted).
     * managed_session() is always NULL on a server, so it proves
     * nothing here — assert the connection count directly. */
    CHECK(moq_msquic_managed_conn_count(p.server.m) == 0);
    msqc_pair_teardown(&p);

    CHECK(p.client.obs.setup == 0);
    CHECK(p.client.obs.fatal);
    CHECK(!p.client.obs.closed);
    CHECK(p.client.obs.pending_sends == 0);
    CHECK(p.client.obs.pending_dgrams == 0);
    CHECK(p.server.obs.setup == 0);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: version_refusal\n");
}

/* Both sides at draft-18 negotiate and run the uni-control profile
 * through the same adapter paths: setup, one verified object, clean
 * close. */
static void t_version_match_d18(void)
{
    int before = failures;
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    cfg.client_version = MOQ_VERSION_DRAFT_18;
    cfg.server_version = MOQ_VERSION_DRAFT_18;
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = 1,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 2;
    p.client.expect_size = 64;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 64 };
    p.server.step_count = 2;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, 1));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    CHECK(p.client.obs.setup == 1);
    CHECK(p.server.obs.setup == 1);
    CHECK(p.client.obs.objects == 1);
    CHECK(p.client.obs.seen_g0 == 0x1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    if (failures == before)
        printf("PASS: version_match_d18\n");
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
    t_reset_propagation();
    t_bulk_delivery_pressure();
    t_parked_publish_retry();
    t_datagram_object();
    t_version_refusal();
    t_version_match_d18();

    /* intentional gaps (see the header comment for the reasoning) */
    printf("SKIP: timer/virtual-time scenarios (real time only; no "
           "fast-forward)\n");
    printf("SKIP: dropped-datagram unblock accounting (needs the fake "
           "rails' write-blocking and drop introspection)\n");
    printf("SKIP: fault-injection and count-introspection scenarios "
           "(fake rails only)\n");

    if (failures == 0)
        printf("PASS: msquic_conformance\n");
    else
        fprintf(stderr, "FAIL: msquic_conformance (%d)\n", failures);
    return failures;
}
