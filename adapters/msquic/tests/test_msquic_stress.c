/*
 * Stress/soak for the direct MsQuic adapter over real loopback through
 * the managed facade.
 *
 * Not a benchmark: no throughput thresholds. The point is lifecycle
 * churn — repeated full connection cycles (fresh managed pair each
 * time: registration, listener on port 0, connect, setup, subscribe,
 * publish, close, teardown) across a deterministic schedule of cycle
 * kinds: single objects from small to oversized (past the 64 KiB
 * send-budget floor, where the second oversized object must park until
 * the first completes), sequential objects on one subgroup, datagram +
 * status-datagram publishing, reset-then-recover pinned to the fresh
 * group, and a clean close racing outbound stream data and a datagram.
 *
 * Confinement matches the conformance rail (msquic_contract_pair):
 * every moq_session_* call runs inside the managed on_lane_pump; the main
 * thread only waits on bounded observation predicates and asserts
 * after teardown.
 *
 * Cycle count defaults to 25; MOQ_MSQUIC_STRESS_LOOPS overrides it for
 * longer manual soaks.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "msquic_contract_pair.h"

static int failures = 0;
static int cur_cycle = -1;
static const char *cur_kind = "";

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: cycle %d (%s): %s\n", __FILE__, \
                __LINE__, cur_cycle, cur_kind, #expr); \
        failures++; \
    } \
} while (0)

static const char *cert_path;
static const char *key_path;

static uint64_t total_objects;
static uint64_t total_bytes;
static msqc_obs_t last_client_obs;
static msqc_obs_t last_server_obs;

static void default_cfg(msqc_pair_cfg_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->cert = cert_path;
    cfg->key = key_path;
}

/* The lifecycle contract every non-fatal cycle shares: setup completed
 * exactly once on both sides, the subscription round-trip happened,
 * the session-closed event reached both applications, the terminal is
 * never fatal, and every accepted send/datagram drained (completed or
 * cleanly canceled) by the terminal. expect_sub_ok is false only for
 * the drain-race cycle, where the server's abortive close may cut the
 * SUBSCRIBE_OK delivery. */
static void check_common(const msqc_pair_t *p, bool expect_sub_ok)
{
    last_client_obs = p->client.obs;
    last_server_obs = p->server.obs;
    CHECK(p->client.obs.setup == 1);
    CHECK(p->server.obs.setup == 1);
    CHECK(p->server.obs.sub_request == 1);
    if (expect_sub_ok)
        CHECK(p->client.obs.sub_ok == 1);
    CHECK(p->client.obs.session_closed >= 1);
    CHECK(p->server.obs.session_closed >= 1);
    CHECK(!p->client.obs.fatal);
    CHECK(!p->server.obs.fatal);
    CHECK(p->client.obs.pending_sends == 0);
    CHECK(p->client.obs.pending_dgrams == 0);
    CHECK(p->server.obs.pending_sends == 0);
    CHECK(p->server.obs.pending_dgrams == 0);
}

/* One subgroup, `count` objects of `size` bytes, verified end to end
 * (ids, payloads, exact coverage mask), clean close once everything
 * arrived. */
static void cycle_publish(uint64_t count, uint64_t size)
{
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    /* the close cannot fire before SUBSCRIBE_OK was observed: steps
     * advance in order, so this gate makes sub_ok deterministic */
    p.client.steps[1] =
        (msqc_step_t){ .when = MSQC_ON_SUB_OK, .act = MSQC_DO_NOTHING };
    p.client.steps[2] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = count,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 3;
    p.client.expect_size = size;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = count,
                                       .a2 = size };
    p.server.step_count = 2;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_count(&p.client, &p.client.obs.objects, (int)count));
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    check_common(&p, true);
    CHECK(p.client.obs.objects == (int)count);
    CHECK(p.client.obs.object_bytes == count * size);
    CHECK(p.client.obs.seen_g0 == (count >= 64 ? p.client.obs.seen_g0
                                               : (1ull << count) - 1));
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/* One payload datagram (group 0, byte-exact) plus one status-only
 * datagram (group 1, metadata pinned exactly), then a clean close. */
static void cycle_datagram(void)
{
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] =
        (msqc_step_t){ .when = MSQC_ON_SUB_OK, .act = MSQC_DO_NOTHING };
    p.client.steps[2] = (msqc_step_t){ .when = MSQC_ON_OBJECTS,
                                       .when_arg = 2,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 3;
    p.client.expect_size = 64;
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

    check_common(&p, true);
    CHECK(p.client.obs.objects == 2);
    CHECK(p.client.obs.dgram_objects == 2);
    CHECK(p.client.obs.status_objects == 1);
    CHECK(p.client.obs.seen_g0 == 0x1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/* A mid-track subgroup reset followed by a fresh-group publish that
 * demonstrably delivers, then a clean close. */
static void cycle_reset_recover(void)
{
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.steps[1] =
        (msqc_step_t){ .when = MSQC_ON_SUB_OK, .act = MSQC_DO_NOTHING };
    p.client.steps[2] = (msqc_step_t){ .when = MSQC_ON_OBJECTS_G1,
                                       .when_arg = 1,
                                       .act = MSQC_DO_CLOSE };
    p.client.step_count = 3;
    p.client.expect_size = 32;
    p.client.expect_size_g1 = 48;

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH_RESET,
                                       .a1 = 0x51,
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

    check_common(&p, true);
    CHECK(p.client.obs.objects_g1 == 1);
    CHECK(p.client.obs.seen_g1 == 0x1);
    CHECK(p.client.obs.seen_g0 == 0x0 || p.client.obs.seen_g0 == 0x1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/*
 * The server closes while its output is still draining: the stream
 * publish (group 0), a datagram (group 1) and the session close all
 * fire in the same pump batch. The pinned contract: the close stays
 * clean and non-fatal on both sides — intentionally canceled pending
 * sends and datagram finals release their records without poisoning
 * anything (ASan owns that proof). Delivery of the raced objects is
 * NOT a contract; whatever does arrive must still verify exactly.
 */
static void cycle_close_while_draining(void)
{
    msqc_pair_t p;
    msqc_pair_cfg_t cfg;
    enum { OBJECTS = 6, SIZE = 8192 };

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (msqc_step_t){ .when = MSQC_ON_SETUP, .act = MSQC_DO_SUBSCRIBE };
    p.client.step_count = 1;
    p.client.expect_size = SIZE;
    p.client.expect_size_g1 = 64; /* the raced datagram */

    p.server.steps[0] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_ACCEPT };
    p.server.steps[1] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.steps[2] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_PUBLISH_DGRAM,
                                       .a1 = 1,
                                       .a2 = 64 };
    p.server.steps[3] = (msqc_step_t){ .when = MSQC_ON_SUB_REQUEST,
                                       .act = MSQC_DO_CLOSE,
                                       .a1 = 3 };
    p.server.step_count = 4;

    if (msqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        msqc_pair_teardown(&p);
        return;
    }
    CHECK(msqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(msqc_wait_flag(&p.server, &p.server.obs.closed));
    msqc_pair_teardown(&p);

    check_common(&p, false);
    /* the REAL drain signals: the scripted close code crossed the wire
     * to the client (the terminal came from the close path, not a
     * timeout), and — via check_common's drain probes — every send and
     * datagram the server's batch had accepted was completed or
     * cleanly canceled by the terminal, without the bridge going
     * fatal */
    CHECK(p.client.obs.close_code == 3);
    CHECK(p.client.obs.objects <= OBJECTS + 1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];

    int cycles = 25;
    const char *env = getenv("MOQ_MSQUIC_STRESS_LOOPS");
    if (env != NULL && atoi(env) > 0)
        cycles = atoi(env);

    printf("stress: %d cycles\n", cycles);
    for (int i = 0; i < cycles; i++) {
        cur_cycle = i;
        switch (i % 8) {
        case 0:
            cur_kind = "small";
            cycle_publish(1, 16);
            break;
        case 1:
            cur_kind = "normal";
            cycle_publish(1, 1200);
            break;
        case 2:
            cur_kind = "medium";
            cycle_publish(1, 8192);
            break;
        case 3:
            /* both objects exceed the 64 KiB budget floor: the first
             * is admitted idle, the second parks until the first's
             * completion frees the budget */
            cur_kind = "oversized";
            cycle_publish(2, 96 * 1024);
            break;
        case 4:
            cur_kind = "sequential";
            cycle_publish(12, 1200);
            break;
        case 5:
            cur_kind = "datagram";
            cycle_datagram();
            break;
        case 6:
            cur_kind = "reset_recover";
            cycle_reset_recover();
            break;
        case 7:
            cur_kind = "close_while_draining";
            cycle_close_while_draining();
            break;
        }
        if (failures != 0) {
            fprintf(stderr, "FAIL: stopping at cycle %d (%s)\n", i,
                    cur_kind);
            fprintf(stderr,
                    "  client: fatal=%d code=%llu setup=%d | "
                    "server: fatal=%d code=%llu setup=%d\n",
                    (int)last_client_obs.fatal,
                    (unsigned long long)last_client_obs.fatal_code,
                    last_client_obs.setup, (int)last_server_obs.fatal,
                    (unsigned long long)last_server_obs.fatal_code,
                    last_server_obs.setup);
            break;
        }
    }

    if (failures == 0) {
        printf("stress: %llu objects, %llu bytes verified\n",
               (unsigned long long)total_objects,
               (unsigned long long)total_bytes);
        printf("PASS: msquic_stress\n");
    } else {
        fprintf(stderr, "FAIL: msquic_stress (%d)\n", failures);
    }
    return failures;
}
