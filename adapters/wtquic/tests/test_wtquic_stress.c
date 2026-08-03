/*
 * Stress/soak for the wtquic adapter over real MsQuic loopback.
 *
 * Not a benchmark: no throughput thresholds. The point is lifecycle
 * churn — repeated full connection cycles (env, listener, connect,
 * setup, subscribe, publish, close, teardown), rotating object sizes,
 * stream churn within a connection, a close racing outbound drain, a
 * refusal, and a reset-then-recover — proving the adapter survives
 * production-like repetition without fatals, payload corruption,
 * deadlocks, or callbacks outliving teardown (the pair teardown's
 * env-close quiescence barrier plus ASan cover the latter).
 *
 * Confinement matches the conformance rail: every moq_session_* call
 * runs inside the adapter hook on the transport worker via per-side
 * scripts; the main thread only waits on observations and asserts
 * after teardown.
 *
 * Cycle count defaults to 25; MOQ_WTQUIC_STRESS_LOOPS overrides it for
 * longer manual soak runs.
 *
 * Object sizes rotate through small (16 B), MTU-ish (1200 B), 8 KiB,
 * and a 96 KiB multi-budget case. The transport's per-stream send
 * budget throttles queued depth — an idle stream admits one legal send
 * of any size — so oversized objects serialize one at a time but
 * always make progress.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wtquic_contract_pair.h"

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

static void default_cfg(wtqc_pair_cfg_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->cert = cert_path;
    cfg->key = key_path;
    cfg->client_offer = "moqt-16";
    cfg->server_serve = "moqt-16";
}

/* One subgroup, `count` objects of `size` bytes, verified end to end,
 * clean close once everything arrived. */
static void cycle_publish(uint64_t count, uint64_t size)
{
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS,
                                       .when_arg = count,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;
    p.client.expect_objects = count;
    p.client.expect_size = size;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = count,
                                       .a2 = size };
    p.server.step_count = 2;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects, (int)count));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.setup == 1);
    CHECK(p.server.obs.setup == 1);
    CHECK(p.server.obs.sub_request == 1);
    CHECK(p.client.obs.sub_ok == 1);
    CHECK(p.client.obs.objects == (int)count);
    CHECK(p.client.obs.object_bytes == count * size);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(p.client.obs.session_closed >= 1);
    CHECK(p.server.obs.session_closed >= 1);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/* Stream churn inside one connection: several subgroups (each its own
 * uni stream), all delivered and verified before the clean close. */
static void cycle_multi_subgroup(uint64_t groups, uint64_t per_group,
                                 uint64_t size)
{
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;
    uint64_t total = groups * per_group;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS,
                                       .when_arg = total,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;
    p.client.expect_objects = per_group; /* object ids restart per group */
    p.client.expect_size = size;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    for (uint64_t g = 0; g < groups; g++)
        p.server.steps[1 + g] = (wtqc_step_t){
            .when = WTQC_ON_SUB_REQUEST,
            .act = WTQC_DO_PUBLISH,
            .a1 = per_group,
            .a2 = size,
        };
    p.server.step_count = 1 + (size_t)groups;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_count(&p.client, &p.client.obs.objects, (int)total));
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.objects == (int)total);
    CHECK(p.client.obs.object_bytes == total * size);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/* The server closes while its outbound sends are still draining: the
 * publish and the close fire in the same hook batch. Delivery of the
 * raced objects is not a contract; a clean, non-fatal terminal on both
 * sides is. Whatever objects do arrive must still verify intact. */
static void cycle_close_while_draining(void)
{
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;
    enum { OBJECTS = 6, SIZE = 8192 };

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.step_count = 1;
    p.client.expect_objects = OBJECTS;
    p.client.expect_size = SIZE;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = OBJECTS,
                                       .a2 = SIZE };
    p.server.steps[2] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_WT_CLOSE,
                                       .a1 = 3 };
    p.server.step_count = 3;

    if (wtqc_pair_setup(&p, &cfg) != 0) {
        CHECK(0 && "pair setup");
        wtqc_pair_teardown(&p);
        return;
    }
    CHECK(wtqc_wait_flag(&p.client, &p.client.obs.closed));
    CHECK(wtqc_wait_flag(&p.server, &p.server.obs.closed));
    wtqc_pair_teardown(&p);

    CHECK(p.client.obs.objects <= OBJECTS);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
    total_objects += (uint64_t)p.client.obs.objects;
    total_bytes += p.client.obs.object_bytes;
}

/* A refused connect (subprotocol the server does not serve) is a fatal
 * terminal with no setup on either side — and the next cycle's fresh
 * pair must be unaffected (the loop itself proves that). */
static void cycle_refusal(void)
{
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
    CHECK(p.server.obs.setup == 0);
    CHECK(p.client.obs.fatal);
}

/* A mid-track subgroup reset followed by a fresh-group publish that
 * demonstrably delivers, then a clean close: reset does not poison the
 * session or the connection. */
static void cycle_reset_recover(void)
{
    wtqc_pair_t p;
    wtqc_pair_cfg_t cfg;

    memset(&p, 0, sizeof(p));
    default_cfg(&cfg);
    p.client.steps[0] =
        (wtqc_step_t){ .when = WTQC_ON_SETUP, .act = WTQC_DO_SUBSCRIBE };
    p.client.steps[1] = (wtqc_step_t){ .when = WTQC_ON_OBJECTS_G1,
                                       .when_arg = 1,
                                       .act = WTQC_DO_WT_CLOSE };
    p.client.step_count = 2;

    p.server.steps[0] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_ACCEPT };
    p.server.steps[1] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH_RESET,
                                       .a1 = 0x51,
                                       .a2 = 24 };
    p.server.steps[2] = (wtqc_step_t){ .when = WTQC_ON_SUB_REQUEST,
                                       .act = WTQC_DO_PUBLISH,
                                       .a1 = 1,
                                       .a2 = 40 };
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

    CHECK(p.client.obs.objects_g1 == 1);
    CHECK(p.client.obs.object_errors == 0);
    CHECK(p.server.obs.publish_errors == 0);
    CHECK(!p.client.obs.fatal);
    CHECK(!p.server.obs.fatal);
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
    const char *env = getenv("MOQ_WTQUIC_STRESS_LOOPS");
    if (env != NULL && atoi(env) > 0)
        cycles = atoi(env);

    /* small / MTU-ish / 8 KiB / multi-budget large: 96 KiB exceeds the
     * transport's 64 KiB per-stream budget floor, pinning forward
     * progress for oversized sends (the first is admitted on the idle
     * stream, the second parks until its completion frees the budget) */
    static const struct { uint64_t count, size; } rot[4] = {
        { 12, 16 }, { 8, 1200 }, { 5, 8192 }, { 2, 98304 },
    };

    printf("stress: %d cycles\n", cycles);
    int rot_i = 0;
    for (int i = 0; i < cycles; i++) {
        cur_cycle = i;
        switch (i % 8) {
        case 5:
            cur_kind = "close_while_draining";
            cycle_close_while_draining();
            break;
        case 6:
            cur_kind = "refusal";
            cycle_refusal();
            break;
        case 7:
            cur_kind = "reset_recover";
            cycle_reset_recover();
            break;
        case 1:
        case 3:
            cur_kind = "multi_subgroup";
            cycle_multi_subgroup(3, 4, (i % 8 == 1) ? 1200 : 4096);
            break;
        default:
            /* the publish rotation advances independently of the cycle
             * index so every size — including the multi-budget 96 KiB
             * case — actually runs */
            cur_kind = "publish";
            cycle_publish(rot[rot_i % 4].count, rot[rot_i % 4].size);
            rot_i++;
            break;
        }
        if (failures != 0) {
            fprintf(stderr, "FAIL: stopping at cycle %d (%s)\n", i,
                    cur_kind);
            break;
        }
    }

    if (failures == 0) {
        printf("stress: %llu objects, %llu bytes verified\n",
               (unsigned long long)total_objects,
               (unsigned long long)total_bytes);
        printf("PASS: wtquic_stress\n");
    } else {
        fprintf(stderr, "FAIL: wtquic_stress (%d)\n", failures);
    }
    return failures;
}
