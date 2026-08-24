/*
 * White-box coverage: compiled together with the facade source under
 * MOQ_WTQ_MM_TESTING, so it can drive the private seams and read back internal
 * state. Deterministic: gates and counters only, never timing thresholds.
 *
 * Foundation:
 *   - clean unwind when each pthread_*_init stage fails;
 *   - owned copies actually deep-copy (survive scribble/free of the inputs);
 *   - wt_protocol_count overflow / cap / NULL-array rejection; server lane cap.
 *
 * Client transport (authoritative per the review boundary):
 *   - WT-Protocol negotiation: exact-token validation, offered-token
 *     enforcement, legacy draft-16 fallback, no silent downgrade, and the
 *     materialized default offer;
 *   - first fatal cause preserved (fatal outranks a clean close, first wins);
 *   - the session handle is published under the lane guard, released once;
 *   - connect result mapping, the on_draining NULL-callback guard, env-config
 *     forwarding (allocator + idle timeout), and facade-alloc create-unwind.
 *
 * The client bring-up runs through a fake transport (no real MsQuic dial); the
 * real draft-16/18 loopback establishment is the separate loopback smoke.
 */
#include "wtquic_msquic_managed_test_internal.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_fail;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            g_fail++;                                                       \
        }                                                                   \
    } while (0)

/* thread-safe counting allocator (teardown frees on the coordinator thread);
 * optional budget injects an allocation failure after `budget` successes */
typedef struct {
    pthread_mutex_t mu;
    long live;
    long budget;
    bool limited;
    /* denial accounting: proves an armed zero budget actually refused the
     * first allocation attempted after arming, rather than being inferred
     * from the terminal result */
    long denials;
    long succ_since_arm;
    long first_denial_succ_before;
} acnt_t;
static void acnt_reset_denials(acnt_t *a)
{
    a->denials = 0;
    a->succ_since_arm = 0;
    a->first_denial_succ_before = -1;
}
static void acnt_init(acnt_t *a)
{
    pthread_mutex_init(&a->mu, NULL);
    a->live = 0; a->budget = 0; a->limited = false;
    acnt_reset_denials(a);
}
/* arm a zero budget and start denial accounting from this instant */
static void acnt_arm_zero(acnt_t *a)
{
    pthread_mutex_lock(&a->mu);
    a->limited = true;
    a->budget = 0;
    acnt_reset_denials(a);
    pthread_mutex_unlock(&a->mu);
}
static void acnt_disarm(acnt_t *a)
{
    pthread_mutex_lock(&a->mu);
    a->limited = false;
    pthread_mutex_unlock(&a->mu);
}
static void acnt_denials(acnt_t *a, long *out_denials, long *out_first_after)
{
    pthread_mutex_lock(&a->mu);
    if (out_denials)    *out_denials    = a->denials;
    if (out_first_after) *out_first_after = a->first_denial_succ_before;
    pthread_mutex_unlock(&a->mu);
}
static void acnt_init_budget(acnt_t *a, long budget)
{
    pthread_mutex_init(&a->mu, NULL);
    a->live = 0;
    a->budget = budget;
    a->limited = true;
    acnt_reset_denials(a);
}
static void acnt_destroy(acnt_t *a) { pthread_mutex_destroy(&a->mu); }
static long acnt_live(acnt_t *a)
{
    pthread_mutex_lock(&a->mu);
    long v = a->live;
    pthread_mutex_unlock(&a->mu);
    return v;
}
static void *acnt_alloc(size_t n, void *ctx)
{
    acnt_t *a = ctx;
    pthread_mutex_lock(&a->mu);
    bool deny = a->limited && a->budget-- <= 0;
    if (a->limited) {
        if (deny) {
            if (a->first_denial_succ_before < 0)
                a->first_denial_succ_before = a->succ_since_arm;
            a->denials++;
        } else {
            a->succ_since_arm++;
        }
    }
    if (!deny)
        a->live++;
    pthread_mutex_unlock(&a->mu);
    if (deny)
        return NULL;
    void *p = malloc(n ? n : 1);
    if (p == NULL) {
        pthread_mutex_lock(&a->mu);
        a->live--;
        pthread_mutex_unlock(&a->mu);
    }
    return p;
}
static void *acnt_realloc(void *p, size_t os, size_t ns, void *ctx)
{
    acnt_t *a = ctx;
    (void)os;
    pthread_mutex_lock(&a->mu);
    bool deny = a->limited && a->budget-- <= 0;
    if (a->limited) {
        if (deny) {
            if (a->first_denial_succ_before < 0)
                a->first_denial_succ_before = a->succ_since_arm;
            a->denials++;
        } else {
            a->succ_since_arm++;
        }
    }
    if (!deny && p == NULL)
        a->live++;
    pthread_mutex_unlock(&a->mu);
    if (deny)
        return NULL;
    void *q = realloc(p, ns ? ns : 1);
    if (q == NULL && p == NULL) {
        pthread_mutex_lock(&a->mu);
        a->live--;
        pthread_mutex_unlock(&a->mu);
    }
    return q;
}
static void acnt_free(void *p, size_t n, void *ctx)
{
    acnt_t *a = ctx;
    (void)n;
    if (p == NULL)
        return;
    free(p);
    pthread_mutex_lock(&a->mu);
    a->live--;
    pthread_mutex_unlock(&a->mu);
}

static int pump(moq_wtquic_msquic_managed_t *m,
                moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m;
    (void)l;
    (void)t;
    (void)u;
    return 0;
}

/* A well-behaved application pump: acknowledges terminal for every connection
 * it is presented, from that connection's owning lane callback. Tests whose
 * subject is the transport/traversal side of reaping use this so the session
 * and acknowledgment axes of the gate are satisfied normally. */
static int ack_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m;
    (void)t;
    (void)u;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(l, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(l, c))
        (void)moq_wtquic_msquic_managed_conn_ack_terminal(c);
    return 0;
}

/* --- every pthread stage fails cleanly ----------------------------------- */
static void test_pthread_init_failures(void)
{
    /* A 2-lane SERVER (no real listener, so create runs to completion) exercises
     * EVERY pthread primitive create() spawns, in order:
     *   1 m->mu, 2 m->cv,
     *   3 lane0.mu, 4 lane0.bell_mu, 5 lane0.bell_cv,
     *   6 lane1.mu, 7 lane1.bell_mu, 8 lane1.bell_cv,
     *   9 worker0, 10 worker1,          (partial worker creation)
     *   11 coordinator.
     * Every stage must unwind cleanly: create fails, m is NULL, any already-
     * started worker is joined, and all allocations balance. */
    const uint32_t lanes = 2;
    const int stages = 3 + 4 * (int)lanes; /* = 11 */
    moq_wtquic_msquic_managed_test_no_listener(true);
    for (int stage = 1; stage <= stages; stage++) {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = "c.pem";
        cfg.key_path = "k.pem";
        cfg.on_lane_pump = pump;
        cfg.max_connections = lanes;
        cfg.lane_count = lanes;

        moq_wtquic_msquic_managed_test_fail_pthread(stage);
        moq_wtquic_msquic_managed_t *m = NULL;
        moq_result_t rc = moq_wtquic_msquic_managed_create(&cfg, &m);
        moq_wtquic_msquic_managed_test_fail_pthread(0); /* reset */
        CHECK(rc == MOQ_ERR_INTERNAL);
        CHECK(m == NULL);
        CHECK(acnt_live(&a) == 0); /* clean unwind, no leak, workers joined */
        acnt_destroy(&a);
    }
    /* one past the last stage: no injected failure fires, create SUCCEEDS —
     * proving the stage count above is exact (not silently short) */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = "c.pem";
        cfg.key_path = "k.pem";
        cfg.on_lane_pump = pump;
        cfg.max_connections = lanes;
        cfg.lane_count = lanes;
        moq_wtquic_msquic_managed_test_fail_pthread(stages + 1);
        moq_wtquic_msquic_managed_t *m = NULL;
        moq_result_t rc = moq_wtquic_msquic_managed_create(&cfg, &m);
        moq_wtquic_msquic_managed_test_fail_pthread(0);
        CHECK(rc == MOQ_OK && m != NULL);
        if (m != NULL) {
            CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- owned copies are deep (survive scribble) ----------------- */
static void test_owned_copies_deep(void)
{
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };

    char host[] = "relay.example.net";
    char p0[] = "moqt-18";
    char p1[] = "moqt-16";
    const char *protos[] = { p0, p1 };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = host;
    cfg.port = 443;
    cfg.on_lane_pump = pump;
    cfg.wt_protocols = protos;
    cfg.wt_protocol_count = 2;

    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        /* scribble/repoint every caller input */
        memset(host, 'Z', sizeof(host) - 1);
        memset(p0, 'Z', sizeof(p0) - 1);
        memset(p1, 'Z', sizeof(p1) - 1);
        protos[0] = protos[1] = NULL;
        /* the RETAINED copies must still equal the ORIGINAL content — this is
         * what fails if create() only shallow-copied the pointers */
        CHECK(strcmp(moq_wtquic_msquic_managed_test_host(m),
                     "relay.example.net") == 0);
        CHECK(moq_wtquic_msquic_managed_test_proto_count(m) == 2);
        CHECK(strcmp(moq_wtquic_msquic_managed_test_proto(m, 0), "moqt-18") ==
              0);
        CHECK(strcmp(moq_wtquic_msquic_managed_test_proto(m, 1), "moqt-16") ==
              0);
        moq_wtquic_msquic_managed_stop(m);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
}

/* --- overflowing wt_protocol_count is rejected, not OOB ------- */
static void test_proto_count_overflow(void)
{
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    const char *one[] = { "moqt-18" };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "h";
    cfg.port = 443;
    cfg.on_lane_pump = pump;
    cfg.wt_protocols = one;
    /* a count that would wrap count*sizeof(char*): must be rejected before any
     * allocation or loop, never producing a tiny buffer + OOB write */
    cfg.wt_protocol_count = SIZE_MAX / 2 + 1;

    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);

    /* a merely-too-large (non-overflowing) count is also rejected by the cap */
    acnt_init(&a);
    cfg.wt_protocol_count = 9; /* > MM_MAX_WT_PROTOCOLS */
    m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);

    /* nonzero count with a NULL array is REJECTED, not silently defaulted */
    acnt_init(&a);
    cfg.wt_protocols = NULL;
    cfg.wt_protocol_count = 2;
    m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
}

/* Create a server facade over the no-listener seam (env + child pool, no real
 * listener), one lane per connection for round-robin clarity. */
static moq_wtquic_msquic_managed_t *make_server(const moq_alloc_t *alloc,
                                                uint32_t max_conn)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump;
    cfg.max_connections = max_conn;
    cfg.lane_count = max_conn;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    return m;
}

/* --- server lanes capped to max_connections ------------------- */
static void test_server_lane_cap(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump;
    cfg.max_connections = 2;
    cfg.lane_count = 8; /* capped to max_connections (2) */

    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        CHECK(moq_wtquic_msquic_managed_test_lane_count(m) == 2);
        CHECK(moq_wtquic_msquic_managed_lane_count(m) == 2);
        moq_wtquic_msquic_managed_stop(m);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- server admission: admit / refuse / abandon / count / drain ---------- */
static void test_server_admission(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_t *m = make_server(&alloc, 2);
    if (m != NULL) {
        void *c0 = NULL, *c1 = NULL, *c2 = NULL, *c3 = NULL, *c4 = NULL;

        /* admit up to max_connections; round-robin distinct lanes */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1) && c1 != NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2);
        CHECK(moq_wtquic_msquic_managed_test_conn_lane_index(c0) == 0);
        CHECK(moq_wtquic_msquic_managed_test_conn_lane_index(c1) == 1);

        /* refuse past max_connections; count unchanged */
        CHECK(!moq_wtquic_msquic_managed_test_accept(m, &c2) && c2 == NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2);

        /* abandon frees a slot + decrements; a fresh admission then succeeds */
        moq_wtquic_msquic_managed_test_abandon(m, c0);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c3) && c3 != NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2);

        /* drain refuses new admissions before the transport barrier */
        moq_wtquic_msquic_managed_drain(m);
        moq_wtquic_msquic_managed_test_abandon(m, c1); /* free a slot */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);
        CHECK(!moq_wtquic_msquic_managed_test_accept(m, &c4));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* teardown reaps the remaining admitted child (c3) and zeroes the
         * live count */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- server chooser: placed on the returned lane; out-of-range refuses;
 *     the callback runs with NO facade lock held; a chooser-triggered drain is
 *     honored by the post-callback recheck --------------------------------- */
static uint32_t g_choose_result;
static int g_choose_calls;
static uint32_t g_choose_observed_count; /* conn_count seen DURING the callback */
static bool g_choose_drain;              /* when set, the callback drains m */
static uint32_t choose_fixed(moq_wtquic_msquic_managed_t *m,
                             const moq_wtquic_msquic_accept_info_t *info,
                             void *user)
{
    (void)info;
    (void)user;
    g_choose_calls++;
    /* Call back into the facade from inside the chooser: both of these public
     * any-thread APIs take m->mu, so if accept_prepare invoked the chooser while
     * still holding m->mu (the old, wrong behavior) this would deadlock
     * deterministically. Reaching the assertions at all proves it runs UNLOCKED.
     * conn_count reflects the reservation already taken before the callback. */
    g_choose_observed_count = moq_wtquic_msquic_managed_conn_count(m);
    if (g_choose_drain)
        moq_wtquic_msquic_managed_drain(m);
    return g_choose_result;
}

static void test_server_chooser(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump;
    cfg.max_connections = 4;
    cfg.lane_count = 3;
    cfg.choose_lane = choose_fixed;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c0 = NULL, *c1 = NULL, *c2 = NULL;
        g_choose_drain = false;

        /* the chooser's in-range result is honored exactly (no modulo), and the
         * callback observes the reservation (count 1) WITHOUT deadlocking -
         * proving accept_prepare released m->mu before calling it */
        g_choose_result = 2;
        g_choose_calls = 0;
        g_choose_observed_count = 0;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);
        CHECK(g_choose_calls == 1);
        CHECK(g_choose_observed_count == 1);
        CHECK(moq_wtquic_msquic_managed_test_conn_lane_index(c0) == 2);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* an out-of-range result REFUSES (not modulo) and rolls back the
         * reservation */
        g_choose_result = 3; /* lane_count is 3 -> indices 0..2 */
        CHECK(!moq_wtquic_msquic_managed_test_accept(m, &c1) && c1 == NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* a chooser that DRAINS the facade mid-callback: the post-callback
         * recheck sees refuse_admissions and rolls the reservation back, even
         * though the returned lane index is in range */
        g_choose_result = 0; /* a valid lane */
        g_choose_drain = true;
        CHECK(!moq_wtquic_msquic_managed_test_accept(m, &c2) && c2 == NULL);
        CHECK(g_choose_observed_count == 2); /* the reservation was taken */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1); /* rolled back */
        g_choose_drain = false;

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- server-local establishment failure stays child-local ---------------- */
/* A child whose establishment fails (unrecognized subprotocol) must mark only
 * itself terminal — a server outlives one bad child, so facade-wide fatal/closed
 * state must NOT latch. */
static void test_server_establish_failure(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    /* the default fake transport (installed in main) keeps g_mm_test_release
     * non-NULL, so the failure path's guarded session close and the reap touch
     * no real (sentinel) handle */
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_t *m = make_server(&alloc, 2);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* drive a failing establishment on this child (unrecognized token) */
        moq_wtquic_msquic_managed_test_deliver_established(m, c0, "x-not-moq");

        /* the server stays healthy: no facade-wide terminal latched */
        CHECK(!moq_wtquic_msquic_managed_is_fatal(m));
        CHECK(!moq_wtquic_msquic_managed_is_closed(m));
        /* and it can still admit a fresh connection */
        void *c1 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1) && c1 != NULL);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- a child establishing DURING teardown converges instead of going live -- */
/* The teardown scan can request quiescence of a child that has been admitted
 * but has not established yet (in_use, ws == NULL). Its on_established then runs
 * under the lane guard, observes the request, and converges immediately rather
 * than building a live session the barrier would have to wait on. */
static void test_server_late_establish_converges(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_t *m = make_server(&alloc, 2);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);

        /* teardown requests quiescence while the child is still pre-establish */
        moq_wtquic_msquic_managed_test_quiesce_conn(m, c0);

        /* the late establishment arrives with a would-succeed token; the flag
         * pre-empts the build, so no session is created and the sentinel is
         * never dereferenced */
        moq_wtquic_msquic_managed_test_deliver_established(m, c0, "moqt-18");

        /* converged, not live: no established session, and the facade (a server)
         * stays healthy */
        CHECK(!moq_wtquic_msquic_managed_test_server_child(m, NULL, NULL, NULL));
        CHECK(!moq_wtquic_msquic_managed_is_fatal(m));
        CHECK(!moq_wtquic_msquic_managed_is_closed(m));

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- final pump: window access and iterability ---------------------------- */
/* A pump callback that records, from INSIDE the window, what it iterated and
 * what the pump-confined accessors returned for each connection. */
#define REC_MAX 8
typedef struct {
    int calls;
    uint32_t seen;
    moq_wtquic_msquic_managed_conn_t *conns[REC_MAX];
} pump_rec_t;
static int recording_pump(moq_wtquic_msquic_managed_t *m,
                          moq_wtquic_msquic_managed_lane_t *lane,
                          uint64_t now_us, void *user)
{
    (void)now_us;
    pump_rec_t *r = user;
    r->calls++;
    r->seen = 0;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL && r->seen < REC_MAX;
         c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        r->conns[r->seen++] = c;
        /* inside the window EVERY conn accessor resolves (session is NULL for a
         * child that failed establishment, but the CALL is granted) */
        (void)moq_wtquic_msquic_managed_conn_session(c);
        (void)moq_wtquic_msquic_managed_conn_negotiated_version(c);
        CHECK(moq_wtquic_msquic_managed_conn_lane(c) == lane);
        moq_wtquic_msquic_managed_conn_set_user(c, (void *)0x5c);
        CHECK(moq_wtquic_msquic_managed_conn_user(c) == (void *)0x5c);
    }
    /* lane-stats TLS discrimination: THIS lane reads inline (its guard is held),
     * a DIFFERENT lane's stats are refused (blocking on its guard would invert
     * lock order) */
    moq_wtquic_msquic_lane_stats_t st;
    CHECK(moq_wtquic_msquic_lane_get_stats(lane, &st, sizeof(st)) == MOQ_OK);
    uint32_t li = moq_wtquic_msquic_lane_index(lane);
    moq_wtquic_msquic_managed_lane_t *other =
        moq_wtquic_msquic_managed_lane(m, li == 0 ? 1 : 0);
    CHECK(moq_wtquic_msquic_lane_get_stats(other, &st, sizeof(st)) ==
          MOQ_ERR_WRONG_STATE);
    return 0;
}
static bool rec_saw(const pump_rec_t *r, const void *c)
{
    for (uint32_t i = 0; i < r->seen; i++)
        if (r->conns[i] == c)
            return true;
    return false;
}

static void test_final_pump_window_and_gate(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    pump_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = recording_pump;
    cfg.on_lane_pump_user = &rec;
    cfg.max_connections = 2;
    cfg.lane_count = 2; /* c0 -> lane 0, c1 -> lane 1 (round-robin) */
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        bool lt, tq, sb;
        void *c0 = NULL, *c1 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1) && c1 != NULL);
        /* both reach their transport terminal (failed establishment is a
         * child-local terminal that needs no real session) */
        moq_wtquic_msquic_managed_test_deliver_established(m, c0, "x-not-moq");
        moq_wtquic_msquic_managed_test_deliver_established(m, c1, "x-not-moq");

        /* outside any pump EVERY conn accessor is closed */
        CHECK(moq_wtquic_msquic_managed_conn_session(c0) == NULL);
        CHECK(moq_wtquic_msquic_managed_conn_adapter(c0) == NULL);
        CHECK(moq_wtquic_msquic_managed_conn_negotiated_version(c0) == 0);
        CHECK(moq_wtquic_msquic_managed_conn_lane(c0) == NULL);
        CHECK(moq_wtquic_msquic_managed_conn_user(c0) == NULL);
        moq_wtquic_msquic_managed_conn_set_user(c0, (void *)0x9); /* no-op */
        CHECK(moq_wtquic_msquic_managed_conn_user(c0) == NULL);

        /* --- ordering A: pump BEFORE quiescence (c0, lane 0) --- */
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        /* pre-session: establishment failed before a MoQ session existed */
        CHECK(lt && !tq && !sb);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(rec.calls == 1);
        CHECK(rec_saw(&rec, c0)); /* terminal child is iterable for its pump */
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && !tq && !sb);  /* a pump does not change any gate fact */
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && tq && !sb);   /* pre-session gate now complete */

        /* --- ordering B: quiescence BEFORE the pump (c1, lane 1) --- */
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c1);
        moq_wtquic_msquic_managed_test_conn_gate(c1, &lt, &tq, &sb);
        CHECK(lt && tq && !sb);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 1) == 0);
        CHECK(rec_saw(&rec, c1));
        moq_wtquic_msquic_managed_test_conn_gate(c1, &lt, &tq, &sb);
        CHECK(lt && tq && !sb);   /* same facts, either order */

        /* teardown runs EXACTLY ONE more pump per lane (the final pump), then
         * reaps: the delta proves the per-lane final pump and that nothing
         * pumps after on_stopped */
        int before = rec.calls;
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        CHECK(rec.calls - before == 2); /* one final pump per lane */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0); /* reaped, absent */
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- a terminal that happens DURING the pump needs another pump ----------- */
/* A pump callback that drives its target child terminal from INSIDE the window
 * (a would-succeed establishment aborted by a bad token), so the child is not
 * terminal at the snapshot but is by the time the callback returns. */
static void *g_stp_conn;
static int same_pass_pump(moq_wtquic_msquic_managed_t *m,
                          moq_wtquic_msquic_managed_lane_t *lane,
                          uint64_t now_us, void *user)
{
    (void)lane;
    (void)now_us;
    (void)user;
    if (g_stp_conn != NULL) {
        moq_wtquic_msquic_managed_test_deliver_established(m, g_stp_conn,
                                                           "x-not-moq");
        g_stp_conn = NULL;
    }
    return 0;
}
static void test_same_pass_terminal(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = same_pass_pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        bool lt, tq, sb;
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(!lt && !tq); /* not terminal before any pump */

        /* the callback drives c0 terminal mid-pump: the fact latches, and a
         * pump neither adds nor removes any other gate fact */
        g_stp_conn = c0;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && !tq && !sb);   /* pre-session terminal, not yet quiesced */

        /* a further pump changes nothing while quiescence is missing */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && !tq && !sb);
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0));

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- conn_close latches in the pump; the close is deferred, retained until
 *     established, and then executed once outside the window ---------------- */
static void *g_ccd_conn;
static bool g_ccd_pending_in_cb;
static int close_pump(moq_wtquic_msquic_managed_t *m,
                      moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                      void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    if (g_ccd_conn != NULL) {
        moq_wtquic_msquic_managed_conn_close(g_ccd_conn, 42);
        /* latched during the callback — NOT executed yet (deferred past it) */
        g_ccd_pending_in_cb =
            moq_wtquic_msquic_managed_test_conn_close_pending(g_ccd_conn);
        g_ccd_conn = NULL;
    }
    return 0;
}
/* records the deferred close the pump runs */
static struct {
    int calls;
    uint32_t code;
    wtq_session_t *ws;
    bool in_window;
} g_cop;
static void cop_close(wtq_session_t *ws, uint32_t code)
{
    g_cop.calls++;
    g_cop.code = code;
    g_cop.ws = ws;
    g_cop.in_window = moq_wtquic_msquic_managed_test_in_pump_window();
}
static void test_conn_close_deferred(void)
{
    wtq_session_t *const fake_ws = (wtq_session_t *)0x11;
    void *const fake_session = (void *)0x22;
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = close_pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0) && c0 != NULL);

        /* conn_close OUTSIDE a pump is a no-op — nothing is latched */
        moq_wtquic_msquic_managed_conn_close(c0, 1);
        CHECK(!moq_wtquic_msquic_managed_test_conn_close_pending(c0));

        /* pump 1: latched during the callback. c0 is NOT established, so the
         * request is RETAINED (not silently dropped) for a later pump */
        g_ccd_conn = c0;
        g_ccd_pending_in_cb = false;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(g_ccd_pending_in_cb); /* latched during the callback */
        CHECK(moq_wtquic_msquic_managed_test_conn_close_pending(c0)); /* retained */

        /* establish c0 and install the close-op probe */
        moq_wtquic_msquic_managed_test_set_conn_established(c0, fake_ws,
                                                           fake_session);
        memset(&g_cop, 0, sizeof(g_cop));
        moq_wtquic_msquic_managed_test_set_close_op(cop_close);

        /* pump 2: the retained close now executes — exactly once, with the
         * latched code, outside the TLS window — and is then cleared */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(g_cop.calls == 1);
        CHECK(g_cop.code == 42);
        CHECK(g_cop.ws == fake_ws);
        CHECK(!g_cop.in_window); /* executed after the window closed */
        CHECK(!moq_wtquic_msquic_managed_test_conn_close_pending(c0));

        /* un-establish (sentinels are not real) + drop the probe before reap */
        moq_wtquic_msquic_managed_test_set_close_op(NULL);
        moq_wtquic_msquic_managed_test_set_conn_established(c0, NULL, NULL);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- callback-owned teardown: no pump runs after on_stopped --------------- */
/* g_pas is written only on the coordinator thread (pas_pump during the teardown
 * final pump, then pas_on_stopped). `stopped` is published under the lock so the
 * test can wait for on_stopped (the callback-owned lifecycle: destroy only from
 * or after on_stopped). pumps/pump_after_stopped are then read AFTER destroy()
 * joins the coordinator, so pthread_join provides their happens-before. */
static struct {
    int pumps;
    bool stopped;
    bool pump_after_stopped;
    pthread_mutex_t mu;
    pthread_cond_t cv;
} g_pas;
static int pas_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                    void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    if (g_pas.stopped)
        g_pas.pump_after_stopped = true;
    g_pas.pumps++;
    return 0;
}
static void pas_on_stopped(void *ctx)
{
    (void)ctx;
    pthread_mutex_lock(&g_pas.mu);
    g_pas.stopped = true;
    pthread_cond_broadcast(&g_pas.cv);
    pthread_mutex_unlock(&g_pas.mu);
}
static void test_pump_after_stopped_owned(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    memset(&g_pas, 0, sizeof(g_pas));
    CHECK(pthread_mutex_init(&g_pas.mu, NULL) == 0);
    CHECK(pthread_cond_init(&g_pas.cv, NULL) == 0);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pas_pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    cfg.on_stopped = pas_on_stopped; /* callback-owned teardown (join refused) */
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        /* initiate the coordinator teardown and WAIT for on_stopped (the
         * callback-owned lifecycle: destroy only from or after on_stopped) */
        CHECK(moq_wtquic_msquic_managed_stop_begin(m)); /* first to latch */
        pthread_mutex_lock(&g_pas.mu);
        while (!g_pas.stopped)
            pthread_cond_wait(&g_pas.cv, &g_pas.mu);
        pthread_mutex_unlock(&g_pas.mu);
        /* destroy() joins the coordinator — the barrier after which no pump can
         * run — so the pumps/pump_after_stopped reads below cannot race one */
        moq_wtquic_msquic_managed_destroy(m);
        CHECK(g_pas.pumps >= 1);           /* the final pump ran */
        CHECK(!g_pas.pump_after_stopped);  /* nothing pumped after on_stopped */
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    pthread_cond_destroy(&g_pas.cv);
    pthread_mutex_destroy(&g_pas.mu);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- per-lane doorbell: real workers, coalescing, cross-lane, concurrency -- */
/* A pump that records per-lane + total invocations, can be gated (held in the
 * callback so the test can arm during a pump), and can cross-wake another lane.
 * All coordination is condvar-based — no sleeps, no latency assumptions. */
#define DW_LANES 4
#define DW_SEEN 4
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int pumps[DW_LANES];
    int total;
    int in_pump;   /* pumps currently held at the gate */
    bool gate;     /* hold every pump in its callback until cleared */
    int cross_from; /* lane whose pump cross-wakes cross_to once; -1 = off */
    int cross_to;
    void *seen[DW_LANES][DW_SEEN]; /* conns the most recent pump of a lane saw */
    int seen_n[DW_LANES];
} g_dw = { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
           { 0 }, 0, 0, false, -1, -1, { { 0 } }, { 0 } };

static void dw_reset(void)
{
    pthread_mutex_lock(&g_dw.mu);
    memset(g_dw.pumps, 0, sizeof(g_dw.pumps));
    memset(g_dw.seen, 0, sizeof(g_dw.seen));
    memset(g_dw.seen_n, 0, sizeof(g_dw.seen_n));
    g_dw.total = 0;
    g_dw.in_pump = 0;
    g_dw.gate = false;
    g_dw.cross_from = -1;
    g_dw.cross_to = -1;
    pthread_mutex_unlock(&g_dw.mu);
}
static int dw_pump(moq_wtquic_msquic_managed_t *m,
                   moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                   void *user)
{
    (void)now_us;
    (void)user;
    uint32_t li = moq_wtquic_msquic_lane_index(lane);
    /* capture this pump's stable chain (we are inside the window) */
    void *saw[DW_SEEN];
    int saw_n = 0;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL && saw_n < DW_SEEN;
         c = moq_wtquic_msquic_lane_next_conn(lane, c))
        saw[saw_n++] = c;
    bool do_cross = false;
    uint32_t cross_to = 0;
    pthread_mutex_lock(&g_dw.mu);
    if (li < DW_LANES) {
        g_dw.pumps[li]++;
        g_dw.seen_n[li] = saw_n;
        for (int i = 0; i < saw_n; i++)
            g_dw.seen[li][i] = saw[i];
    }
    g_dw.total++;
    if (g_dw.cross_from == (int)li) {
        do_cross = true;
        cross_to = (uint32_t)g_dw.cross_to;
        g_dw.cross_from = -1; /* once */
    }
    pthread_cond_broadcast(&g_dw.cv);
    if (g_dw.gate) {
        g_dw.in_pump++;
        pthread_cond_broadcast(&g_dw.cv);
        while (g_dw.gate)
            pthread_cond_wait(&g_dw.cv, &g_dw.mu);
        g_dw.in_pump--;
        pthread_cond_broadcast(&g_dw.cv);
    }
    pthread_mutex_unlock(&g_dw.mu);
    /* cross-wake OUTSIDE the harness lock (lane_wake takes m->mu + bell_mu) */
    if (do_cross)
        (void)moq_wtquic_msquic_lane_wake(
            moq_wtquic_msquic_managed_lane(m, cross_to));
    return 0;
}
static void dw_wait_total(int n)
{
    pthread_mutex_lock(&g_dw.mu);
    while (g_dw.total < n)
        pthread_cond_wait(&g_dw.cv, &g_dw.mu);
    pthread_mutex_unlock(&g_dw.mu);
}
static void dw_wait_in_pump(int n)
{
    pthread_mutex_lock(&g_dw.mu);
    while (g_dw.in_pump < n)
        pthread_cond_wait(&g_dw.cv, &g_dw.mu);
    pthread_mutex_unlock(&g_dw.mu);
}
static void dw_wait_lane(int li, int n)
{
    pthread_mutex_lock(&g_dw.mu);
    while (g_dw.pumps[li] < n)
        pthread_cond_wait(&g_dw.cv, &g_dw.mu);
    pthread_mutex_unlock(&g_dw.mu);
}
static void dw_ungate(void)
{
    pthread_mutex_lock(&g_dw.mu);
    g_dw.gate = false;
    pthread_cond_broadcast(&g_dw.cv);
    pthread_mutex_unlock(&g_dw.mu);
}
/* the most recent pump of `li` saw exactly the connections in want[0..n) */
static bool dw_saw_exactly(int li, void **want, int n)
{
    bool ok;
    pthread_mutex_lock(&g_dw.mu);
    ok = (g_dw.seen_n[li] == n);
    for (int i = 0; ok && i < n; i++) {
        bool found = false;
        for (int j = 0; j < g_dw.seen_n[li]; j++)
            if (g_dw.seen[li][j] == want[i])
                found = true;
        ok = found;
    }
    pthread_mutex_unlock(&g_dw.mu);
    return ok;
}
static moq_wtquic_msquic_managed_t *dw_make_server(acnt_t *a, moq_alloc_t *alloc,
                                                   uint32_t lanes,
                                                   uint32_t max_conn)
{
    alloc->ctx = a;
    alloc->alloc = acnt_alloc;
    alloc->realloc = acnt_realloc;
    alloc->free = acnt_free;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = dw_pump;
    cfg.max_connections = max_conn;
    cfg.lane_count = lanes;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    return m;
}

/* a lane_wake on an idle lane delivers exactly one pump via its worker */
static void test_doorbell_wake_delivers(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 0)) == MOQ_OK);
        dw_wait_total(1); /* the worker ran the pump */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a burst of arms DURING a pump coalesces into exactly one rearmed pump (no lost
 * wake, no per-arm pump) */
static void test_doorbell_coalesce(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        moq_wtquic_msquic_managed_lane_t *l0 =
            moq_wtquic_msquic_managed_lane(m, 0);
        pthread_mutex_lock(&g_dw.mu);
        g_dw.gate = true;
        pthread_mutex_unlock(&g_dw.mu);

        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK);
        dw_wait_in_pump(1); /* pump #1 is held in its callback */

        /* five arms while the worker is inside pump #1 — all must coalesce */
        for (int k = 0; k < 5; k++)
            CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK);

        dw_ungate();       /* release pump #1 -> exactly one rearmed pump #2 */
        dw_wait_total(2);
        CHECK(g_dw.total == 2); /* 6 arms -> 2 pumps, not 7 */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a pump can wake ANOTHER lane; that lane's worker then pumps */
static void test_doorbell_cross_lane(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 2, 2);
    if (m != NULL) {
        pthread_mutex_lock(&g_dw.mu);
        g_dw.cross_from = 0; /* lane 0's pump cross-wakes lane 1 */
        g_dw.cross_to = 1;
        pthread_mutex_unlock(&g_dw.mu);

        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 0)) == MOQ_OK);
        dw_wait_lane(1, 1); /* lane 1 pumped as a result of the cross-wake */
        CHECK(g_dw.pumps[1] >= 1);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* two lanes pump SIMULTANEOUSLY, each iterating ONLY its own stable chain */
static void test_doorbell_two_lane_concurrent(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 2, 2);
    if (m != NULL) {
        /* one distinct child per lane (round-robin: c0->lane0, c1->lane1) */
        void *c0 = NULL, *c1 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1));
        CHECK(moq_wtquic_msquic_managed_test_conn_lane_index(c0) == 0);
        CHECK(moq_wtquic_msquic_managed_test_conn_lane_index(c1) == 1);

        pthread_mutex_lock(&g_dw.mu);
        g_dw.gate = true;
        pthread_mutex_unlock(&g_dw.mu);

        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 0)) == MOQ_OK);
        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 1)) == MOQ_OK);
        dw_wait_in_pump(2); /* BOTH workers inside their pumps at once */
        CHECK(g_dw.in_pump == 2);

        /* each concurrent snapshot is independent: lane 0 iterated only c0,
         * lane 1 only c1 — no cross-lane clobber */
        void *w0[] = { c0 }, *w1[] = { c1 };
        CHECK(dw_saw_exactly(0, w0, 1));
        CHECK(dw_saw_exactly(1, w1, 1));

        dw_ungate();
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a child admitted AFTER a snapshot is captured appears only on the NEXT pump */
static void test_doorbell_snapshot_stable(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m =
        dw_make_server(&a, &alloc, 1, 2); /* one lane, two slots */
    if (m != NULL) {
        moq_wtquic_msquic_managed_lane_t *l0 =
            moq_wtquic_msquic_managed_lane(m, 0);
        void *c0 = NULL, *c1 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0)); /* lane 0 */

        pthread_mutex_lock(&g_dw.mu);
        g_dw.gate = true;
        pthread_mutex_unlock(&g_dw.mu);
        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK);
        dw_wait_in_pump(1); /* pump #1's snapshot is now frozen: {c0} */

        /* admit c1 while pump #1 is held — it must NOT enter the frozen chain */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1)); /* lane 0 */
        void *only_c0[] = { c0 };
        CHECK(dw_saw_exactly(0, only_c0, 1)); /* pump #1 saw only c0 */

        /* arm again, release: pump #2 takes a fresh snapshot with BOTH */
        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK);
        dw_ungate();
        dw_wait_lane(0, 2); /* pump #2 ran */
        void *both[] = { c0, c1 };
        CHECK(dw_saw_exactly(0, both, 2)); /* c1 appears only on pump #2 */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* an arm landing EXACTLY at the worker's idle check-to-wait boundary is not
 * lost (the mutex protocol makes the arm block until cond_wait releases bell_mu) */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool at_boundary;
} g_pw = { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, false };
static void pw_hook(void) /* runs on the worker, UNDER bell_mu */
{
    pthread_mutex_lock(&g_pw.mu);
    g_pw.at_boundary = true;
    pthread_cond_broadcast(&g_pw.cv);
    pthread_mutex_unlock(&g_pw.mu);
}
static void test_doorbell_check_to_wait(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    pthread_mutex_lock(&g_pw.mu);
    g_pw.at_boundary = false;
    pthread_mutex_unlock(&g_pw.mu);
    moq_wtquic_msquic_managed_test_set_prewait_hook(pw_hook);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        /* the fresh worker finds no work and reaches the boundary, firing the
         * hook (still holding bell_mu) */
        pthread_mutex_lock(&g_pw.mu);
        while (!g_pw.at_boundary)
            pthread_cond_wait(&g_pw.cv, &g_pw.mu);
        pthread_mutex_unlock(&g_pw.mu);

        /* arm now: this blocks on bell_mu until the worker's cond_wait releases
         * it, so the arm lands precisely at the check-to-wait edge. It must wake
         * the worker (no lost wake) — proven by the pump running. */
        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 0)) == MOQ_OK);
        dw_wait_total(1);
        CHECK(g_dw.total >= 1);

        moq_wtquic_msquic_managed_test_set_prewait_hook(NULL);
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    moq_wtquic_msquic_managed_test_set_prewait_hook(NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a transport-activity hook arms the connection's lane and the worker pumps */
static void test_transport_activity_pumps(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        /* a transport event serviced c0 -> the hook nudges lane 0 -> a pump */
        moq_wtquic_msquic_managed_test_fire_hook(c0);
        dw_wait_total(1);
        CHECK(g_dw.total >= 1);
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* on_activity fires after a pump, signal-only: session/teardown APIs are refused
 * from it, lane_wake is not */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int calls;
    moq_result_t stop_rc, wait_rc, join_rc, lanewake_rc;
    bool stopbegin_ret;
    void *conn_session;
    moq_wtquic_msquic_managed_t *m;
    void *probe;
} g_oa = { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0,
           MOQ_OK, MOQ_OK, MOQ_OK, MOQ_OK, false, (void *)0x1, NULL, NULL };
static void oa_activity(moq_wtquic_msquic_managed_t *m, void *ctx)
{
    (void)ctx;
    /* enforcement (run once): the blocking teardown calls stop/wait/JOIN are all
     * refused; a per-connection accessor is closed (outside the window);
     * lane_wake stays available (signal a waiter); and stop_begin is LEGAL from
     * this context (it only latches + signals, never blocks). stop_begin runs
     * last, initiating teardown, so the test finishes with destroy(). */
    moq_result_t sr = MOQ_OK, wr = MOQ_OK, jr = MOQ_OK, lr = MOQ_OK;
    void *cs = (void *)0x1;
    bool sb = false;
    pthread_mutex_lock(&g_oa.mu);
    bool first = (g_oa.calls == 0);
    pthread_mutex_unlock(&g_oa.mu);
    if (first) {
        sr = moq_wtquic_msquic_managed_stop(m);
        wr = moq_wtquic_msquic_managed_wait(m, 0);
        jr = moq_wtquic_msquic_managed_join(m);
        cs = moq_wtquic_msquic_managed_conn_session(g_oa.probe);
        lr = moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
        sb = moq_wtquic_msquic_managed_stop_begin(m);
    }
    pthread_mutex_lock(&g_oa.mu);
    if (first) {
        g_oa.stop_rc = sr;
        g_oa.wait_rc = wr;
        g_oa.join_rc = jr;
        g_oa.lanewake_rc = lr;
        g_oa.stopbegin_ret = sb;
        g_oa.conn_session = cs;
    }
    g_oa.calls++;
    pthread_cond_broadcast(&g_oa.cv);
    pthread_mutex_unlock(&g_oa.mu);
}
static void test_on_activity_enforcement(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    pthread_mutex_lock(&g_oa.mu);
    g_oa.calls = 0;
    pthread_mutex_unlock(&g_oa.mu);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    alloc.ctx = &a;
    alloc.alloc = acnt_alloc;
    alloc.realloc = acnt_realloc;
    alloc.free = acnt_free;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = dw_pump;
    cfg.on_activity = oa_activity;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        g_oa.probe = c0;

        moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
        pthread_mutex_lock(&g_oa.mu);
        while (g_oa.calls < 1)
            pthread_cond_wait(&g_oa.cv, &g_oa.mu);
        pthread_mutex_unlock(&g_oa.mu);

        CHECK(g_oa.stop_rc == MOQ_ERR_WRONG_STATE); /* blocking teardown refused */
        CHECK(g_oa.wait_rc == MOQ_ERR_WRONG_STATE);
        CHECK(g_oa.join_rc == MOQ_ERR_WRONG_STATE);
        CHECK(g_oa.conn_session == NULL);           /* accessor closed */
        CHECK(g_oa.lanewake_rc == MOQ_OK);          /* signal stays available */
        CHECK(g_oa.stopbegin_ret);                  /* stop_begin is legal here */

        /* stop_begin() (from on_activity) already initiated teardown; destroy
         * joins it to completion */
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a pump satisfies the wait() contract: one pump -> one MOQ_OK, then MOQ_DONE */
static void test_doorbell_pump_wakes_wait(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
        /* wait blocks until the worker's pump latches activity */
        CHECK(moq_wtquic_msquic_managed_wait(m, 5000000) == MOQ_OK);
        /* the latch is level-retained and consumed once */
        CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_DONE);
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a MoQ-session deadline triggers a pump with no arm (a deadline sweep) */
static void test_doorbell_deadline_sweep(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 1, 1);
    if (m != NULL) {
        /* no arm — set a near deadline; the worker converts the monotonic
         * remaining to a realtime cond_timedwait and pumps when it elapses */
        moq_wtquic_msquic_managed_test_set_deadline(
            moq_wtquic_msquic_managed_lane(m, 0), 10000 /* 10 ms */);
        dw_wait_total(1); /* the deadline (not a wake) produced the pump */
        CHECK(g_dw.total >= 1);
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- per-lane wake/pump statistics ---------------------------------------- */
static void st_get(moq_wtquic_msquic_managed_t *m, uint32_t li,
                   moq_wtquic_msquic_lane_stats_t *out)
{
    memset(out, 0, sizeof(*out));
    CHECK(moq_wtquic_msquic_lane_get_stats(
              moq_wtquic_msquic_managed_lane(m, li), out, sizeof(*out)) == MOQ_OK);
}
/* poll a lane's stats until pump_sweeps reaches n (the sweep committed its
 * counters at the end of the pump) */
static void st_wait_sweeps(moq_wtquic_msquic_managed_t *m, uint32_t li,
                           uint64_t n, moq_wtquic_msquic_lane_stats_t *out)
{
    for (;;) {
        st_get(m, li, out);
        if (out->pump_sweeps >= n)
            return;
    }
}
/* coalescing, pump/deadline sweeps, wake latency, service passes, and the two
 * deliberately-zero counters */
static void test_lane_stats_pump(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 2, 2);
    if (m != NULL) {
        moq_wtquic_msquic_managed_lane_t *l0 =
            moq_wtquic_msquic_managed_lane(m, 0);
        pthread_mutex_lock(&g_dw.mu);
        g_dw.gate = true;
        pthread_mutex_unlock(&g_dw.mu);
        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK); /* arm #1 -> pump #1 */
        dw_wait_in_pump(1);
        for (int k = 0; k < 5; k++)                       /* 5 coalesced arms */
            CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK);
        dw_ungate();

        moq_wtquic_msquic_lane_stats_t s;
        st_wait_sweeps(m, 0, 2, &s); /* pump #1 + the coalesced pump #2 */
        CHECK(s.wakes_external == 6);         /* all 6 arms from the test thread */
        /* arm #1 pumps; the worker consumes it (served catches pending) before
         * pump #1 runs, so arm #2 starts a fresh batch and is NOT coalesced —
         * only arms #3..#6 fold into it */
        CHECK(s.wakes_coalesced == 4);
        CHECK(s.pump_sweeps == 2);
        CHECK(s.deadline_sweeps == 0);
        CHECK(s.wake_to_pump_samples == 2);   /* both arm-driven pumps timed */
        CHECK(s.service_passes == 0);         /* empty lane: no connection serviced */
        CHECK(s.idle_cap_wakes == 0);         /* deliberate: no idle-cap mechanism */
        CHECK(s.flush_sends == 0 && s.flush_bytes == 0); /* deliberate: no counts */

        /* a deadline sweep on lane 1: pre + post service, no wake sample */
        moq_wtquic_msquic_managed_test_set_deadline(
            moq_wtquic_msquic_managed_lane(m, 1), 10000);
        moq_wtquic_msquic_lane_stats_t s1;
        st_wait_sweeps(m, 1, 1, &s1);
        CHECK(s1.deadline_sweeps == 1);
        CHECK(s1.wake_to_pump_samples == 0);  /* a sweep is not arm-driven */
        CHECK(s1.service_passes == 0);        /* empty lane: nothing serviced */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* wake classification by the arming thread's context */
static struct {
    moq_wtquic_msquic_managed_t *m;
    bool arm_same, arm_cross;
} g_cls;
static int cls_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                    void *user)
{
    (void)now_us;
    (void)user;
    if (moq_wtquic_msquic_lane_index(lane) == 0) {
        if (g_cls.arm_same) { /* arm THIS lane from inside its own pump */
            g_cls.arm_same = false;
            (void)moq_wtquic_msquic_lane_wake(lane);
        }
        if (g_cls.arm_cross) { /* arm the OTHER lane from this pump */
            g_cls.arm_cross = false;
            (void)moq_wtquic_msquic_lane_wake(
                moq_wtquic_msquic_managed_lane(m, 1));
        }
    }
    return 0;
}
static void test_lane_stats_classification(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    g_cls.arm_same = true;
    g_cls.arm_cross = true;
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = cls_pump;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        g_cls.m = m;
        /* external arm triggers lane 0's pump, which arms itself (same) + lane 1
         * (cross); lane 0 then re-pumps (its same-arm) */
        CHECK(moq_wtquic_msquic_lane_wake(
                  moq_wtquic_msquic_managed_lane(m, 0)) == MOQ_OK);
        moq_wtquic_msquic_lane_stats_t s0, s1;
        st_wait_sweeps(m, 0, 2, &s0); /* external pump + the same-lane re-pump */
        st_wait_sweeps(m, 1, 1, &s1); /* the cross-lane arm's pump */
        CHECK(s0.wakes_external == 1); /* the test thread's arm */
        CHECK(s0.wakes_same_lane == 1); /* armed from inside lane 0's pump */
        CHECK(s1.wakes_cross_lane == 1); /* armed from lane 0's pump */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* service_passes counts PER-CONNECTION service invocations, not lane cycles */
static void sp_service(void *conn, int phase)
{
    (void)conn;
    (void)phase; /* present only so mm_service_pass counts each connection */
}
static void test_stats_service_passes(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    moq_wtquic_msquic_managed_test_set_service_op(sp_service);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump; /* no-op */
    cfg.max_connections = 3;
    cfg.lane_count = 1; /* all children on lane 0 */
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        moq_wtquic_msquic_lane_stats_t st;
        void *c0 = NULL, *c1 = NULL;
        /* empty lane: a (non-sweep) pump services no connection */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        st_get(m, 0, &st);
        CHECK(st.service_passes == 0);
        /* one connection: +1 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        st_get(m, 0, &st);
        CHECK(st.service_passes == 1);
        /* two connections: +2 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1));
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        st_get(m, 0, &st);
        CHECK(st.service_passes == 3); /* 0 + 1 + 2 */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    moq_wtquic_msquic_managed_test_set_service_op(NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* lane_get_stats sized-copy ABI: exact-v0, oversized (zeroed tail), and the
 * INVAL rejections. Driven from an external thread (no pump window). */
static void test_lane_stats_abi(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        moq_wtquic_msquic_managed_lane_t *lane =
            moq_wtquic_msquic_managed_lane(m, 0);
        CHECK(lane != NULL);

        /* exact-v0: out_size == the frozen floor -> full struct, stamped size */
        moq_wtquic_msquic_lane_stats_t st;
        memset(&st, 0xCD, sizeof(st));
        CHECK(moq_wtquic_msquic_lane_get_stats(
                  lane, &st, MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE) == MOQ_OK);
        CHECK(st.struct_size == MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE);
        CHECK(st.struct_size == (uint32_t)sizeof(st));

        /* oversized output (a future caller's larger struct): the getter fills
         * only what it knows and zeroes the unknown tail; struct_size reports
         * the bytes it actually wrote, not the caller's out_size. */
        union {
            moq_wtquic_msquic_lane_stats_t st;
            unsigned char bytes[sizeof(moq_wtquic_msquic_lane_stats_t) + 32];
        } big;
        memset(big.bytes, 0xAB, sizeof(big.bytes));
        CHECK(moq_wtquic_msquic_lane_get_stats(lane, &big.st, sizeof(big.bytes)) ==
              MOQ_OK);
        CHECK(big.st.struct_size == (uint32_t)sizeof(moq_wtquic_msquic_lane_stats_t));
        for (size_t i = sizeof(moq_wtquic_msquic_lane_stats_t);
             i < sizeof(big.bytes); i++)
            CHECK(big.bytes[i] == 0); /* unknown tail zeroed */

        /* out_size below the frozen floor, and NULL args, are all INVAL */
        CHECK(moq_wtquic_msquic_lane_get_stats(
                  lane, &st, MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE - 1) ==
              MOQ_ERR_INVAL);
        CHECK(moq_wtquic_msquic_lane_get_stats(NULL, &st, sizeof(st)) ==
              MOQ_ERR_INVAL);
        CHECK(moq_wtquic_msquic_lane_get_stats(lane, NULL, sizeof(st)) ==
              MOQ_ERR_INVAL);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* wake is honored before stop and refused once stop_begin is observed */
static void test_doorbell_wake_vs_stop(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    dw_reset();
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = dw_make_server(&a, &alloc, 2, 2);
    if (m != NULL) {
        moq_wtquic_msquic_managed_lane_t *l0 =
            moq_wtquic_msquic_managed_lane(m, 0);
        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_OK); /* before stop */

        CHECK(moq_wtquic_msquic_managed_stop_begin(m));
        /* once teardown is latched, every wake is refused (never absorbed as a
         * pump after teardown) */
        CHECK(moq_wtquic_msquic_lane_wake(l0) == MOQ_ERR_CLOSED);
        CHECK(moq_wtquic_msquic_managed_wake(m) == MOQ_ERR_CLOSED);

        moq_wtquic_msquic_managed_destroy(m); /* joins coordinator + workers */
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- fake transport sentinel (shared by the reap + client tests) --------- */
/* A sentinel handle: connect returns it, reap releases it; never dereferenced. */
static wtq_session_t *const FAKE_WS = (wtq_session_t *)0x1;
static long g_release_count;

static wtq_result_t trivial_connect(wtq_msquic_env_t *env,
                                    const wtq_msquic_client_cfg_t *cfg,
                                    wtq_session_t **out)
{
    (void)env;
    (void)cfg;
    *out = FAKE_WS;
    return WTQ_OK;
}
static void trivial_release(wtq_session_t *ws)
{
    CHECK(ws == FAKE_WS);
    g_release_count++;
}
/* a connect that fires the transport-activity hook (arms the lane) DURING the
 * dial — i.e. while create() is still running its fallible bring-up */
static wtq_result_t hook_connect(wtq_msquic_env_t *env,
                                 const wtq_msquic_client_cfg_t *cfg,
                                 wtq_session_t **out)
{
    (void)env;
    *out = FAKE_WS;
    moq_wtquic_msquic_managed_test_fire_hook(cfg->user); /* user is the conn */
    return WTQ_OK;
}

/* a connect that CAPTURES the WebTransport profile the client path forwarded
 * onto the connect config (ccfg->connect->webtransport_profile), proving the
 * client-side forwarding of m->webtransport_profile. */
static uint32_t g_captured_client_profile;
static wtq_result_t capture_connect(wtq_msquic_env_t *env,
                                    const wtq_msquic_client_cfg_t *cfg,
                                    wtq_session_t **out)
{
    (void)env;
    g_captured_client_profile = cfg->connect->webtransport_profile;
    *out = FAKE_WS;
    return WTQ_OK;
}

/* --- worker activation gate ----------------------------------------------- */
/* A hook armed during bring-up must NOT pump if create() then fails. */
static void test_activation_no_pump_on_create_fail(void)
{
    dw_reset();
    moq_wtquic_msquic_managed_test_set_transport(hook_connect, trivial_release);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.on_lane_pump = dw_pump;
    /* 1 lane -> stage 7 (2 + 3*1 + 1 worker + 1) is the coordinator create */
    moq_wtquic_msquic_managed_test_fail_pthread(7);
    moq_wtquic_msquic_managed_t *m = NULL;
    moq_result_t rc = moq_wtquic_msquic_managed_create(&cfg, &m);
    moq_wtquic_msquic_managed_test_fail_pthread(0);
    CHECK(rc == MOQ_ERR_INTERNAL); /* coordinator create failed */
    CHECK(m == NULL);
    /* the hook armed the lane during connect, but the worker was never activated
     * (create failed before publication), so NO pump/on_activity ran */
    CHECK(g_dw.total == 0);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);
}

/* on success, the arm queued during bring-up IS delivered once workers activate */
static void test_activation_delivers_retained_arm(void)
{
    dw_reset();
    moq_wtquic_msquic_managed_test_set_transport(hook_connect, trivial_release);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.on_lane_pump = dw_pump;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        dw_wait_total(1); /* the retained bring-up arm is delivered post-activation */
        CHECK(g_dw.total >= 1);
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);
}

/* --- session-deadline delivery (seam-driven, deterministic) --------------- */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int seq, pre_seq, pump_seq, post_seq;
    int pumps;
    int dl_calls;
    void *dl_last_conn;   /* the conn most recently queried for a deadline */
    void *reaped;         /* a reaped conn must NEVER be deadline-queried */
    bool queried_reaped;
    moq_wtquic_msquic_managed_t *m;
    void *reused;         /* the conn admitted into the reaped slot */
} g_dl = { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
           0, 0, 0, 0, 0, 0, NULL, NULL, false, NULL, NULL };
/* fired right after a slot is published free: reassign it mid-traversal */
static void ar_reuse(void *reaped_conn)
{
    (void)reaped_conn;
    moq_wtquic_msquic_managed_test_set_after_reap(NULL); /* one-shot */
    void *c_new = NULL;
    if (moq_wtquic_msquic_managed_test_accept(g_dl.m, &c_new))
        g_dl.reused = c_new; /* reuses the just-freed slot (memsets it) */
}
static uint64_t dl_mono_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}
static int dl_pump(moq_wtquic_msquic_managed_t *m,
                   moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                   void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    pthread_mutex_lock(&g_dl.mu);
    g_dl.pump_seq = ++g_dl.seq;
    g_dl.pumps++;
    pthread_cond_broadcast(&g_dl.cv);
    pthread_mutex_unlock(&g_dl.mu);
    return 0;
}
static void dl_service(void *conn, int phase)
{
    (void)conn;
    pthread_mutex_lock(&g_dl.mu);
    if (phase == 0)
        g_dl.pre_seq = ++g_dl.seq;
    else
        g_dl.post_seq = ++g_dl.seq;
    pthread_cond_broadcast(&g_dl.cv);
    pthread_mutex_unlock(&g_dl.mu);
}
/* first query arms a near deadline; later queries return none (park) */
static uint64_t dl_next_deadline(void *conn)
{
    pthread_mutex_lock(&g_dl.mu);
    g_dl.dl_calls++;
    g_dl.dl_last_conn = conn;
    if (conn == g_dl.reaped)
        g_dl.queried_reaped = true;
    uint64_t v = (g_dl.dl_calls == 1) ? (dl_mono_now() + 10000u) : UINT64_MAX;
    pthread_cond_broadcast(&g_dl.cv);
    pthread_mutex_unlock(&g_dl.mu);
    return v;
}
/* wait until n deadline recomputes have run — the LAST step of a pump, so on
 * return the nth pump has fully completed (service + callback + recompute) */
static void dl_wait_calls(int n)
{
    pthread_mutex_lock(&g_dl.mu);
    while (g_dl.dl_calls < n)
        pthread_cond_wait(&g_dl.cv, &g_dl.mu);
    pthread_mutex_unlock(&g_dl.mu);
}
/* records the queried conn but NEVER arms (so no worker sweep races the test) */
static uint64_t dl_none(void *conn)
{
    pthread_mutex_lock(&g_dl.mu);
    g_dl.dl_calls++;
    g_dl.dl_last_conn = conn;
    if (conn == g_dl.reaped)
        g_dl.queried_reaped = true;
    pthread_mutex_unlock(&g_dl.mu);
    return UINT64_MAX;
}
/* a session deadline arms the worker; the due path services BEFORE on_lane_pump;
 * the next deadline is recomputed and rearmed */
static void test_deadline_delivery(void)
{
    wtq_session_t *const ws = (wtq_session_t *)0x1;
    void *const sess = (void *)0x2;
    moq_wtquic_msquic_managed_test_no_listener(true);
    memset(&g_dl.seq, 0, sizeof(int) * 6); /* seq..dl_calls */
    g_dl.dl_last_conn = NULL;
    g_dl.reaped = NULL;
    g_dl.queried_reaped = false;
    moq_wtquic_msquic_managed_test_set_service_op(dl_service);
    moq_wtquic_msquic_managed_test_set_next_deadline_op(dl_next_deadline);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = dl_pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        moq_wtquic_msquic_managed_test_set_conn_established(c0, ws, sess);

        /* an initial pump recomputes the deadline -> arms the worker for a sweep */
        moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
        dl_wait_calls(2); /* initial pump + the deadline sweep, both COMPLETE */

        /* snapshot under the lock — the worker still writes g_dl concurrently */
        pthread_mutex_lock(&g_dl.mu);
        int pumps = g_dl.pumps, calls = g_dl.dl_calls, pre = g_dl.pre_seq,
            pump = g_dl.pump_seq, post = g_dl.post_seq;
        pthread_mutex_unlock(&g_dl.mu);
        CHECK(pumps >= 2);           /* next_deadline armed the worker */
        CHECK(calls >= 2);           /* recomputed every pump (rearm) */
        /* the due path serviced BEFORE on_lane_pump, then again after */
        CHECK(pre != 0 && pre < pump);
        CHECK(pump < post);

        /* stop() joins the worker before teardown reaps c0 (its sentinel session
         * needs no destroy under the fake seam) */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    moq_wtquic_msquic_managed_test_set_service_op(NULL);
    moq_wtquic_msquic_managed_test_set_next_deadline_op(NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* an EXPLICIT wake pumps with NO pre-service (only a deadline sweep pre-services);
 * the pump is followed by the post-service only */
static void test_wake_no_preservice(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    memset(&g_dl.seq, 0, sizeof(int) * 6);
    moq_wtquic_msquic_managed_test_set_service_op(dl_service);
    moq_wtquic_msquic_managed_test_set_next_deadline_op(dl_none); /* no deadline */
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = dl_pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        moq_wtquic_msquic_managed_test_set_conn_established(c0, (wtq_session_t *)0x1,
                                                           (void *)0x2);

        moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
        dl_wait_calls(1); /* the wake pump completed (recompute is its last step) */

        pthread_mutex_lock(&g_dl.mu);
        int pre = g_dl.pre_seq, pump = g_dl.pump_seq, post = g_dl.post_seq;
        pthread_mutex_unlock(&g_dl.mu);
        CHECK(pre == 0);                       /* NO pre-service on an explicit wake */
        CHECK(pump != 0 && post != 0 && pump < post); /* pump -> post-service only */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    moq_wtquic_msquic_managed_test_set_service_op(NULL);
    moq_wtquic_msquic_managed_test_set_next_deadline_op(NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* the deadline recompute runs in the SAME traversal as live reap: a reaped
 * child is torn down and NEVER deadline-queried (no use-after-publish); a
 * surviving child's deadline is read while still in use */
static void test_deadline_reap_no_use_after_publish(void)
{
    wtq_session_t *const ws = (wtq_session_t *)0x1;
    void *const sess = (void *)0x2;
    moq_wtquic_msquic_managed_test_no_listener(true);
    memset(&g_dl.seq, 0, sizeof(int) * 6);
    g_dl.dl_last_conn = NULL;
    g_dl.queried_reaped = false;
    g_dl.reused = NULL;
    moq_wtquic_msquic_managed_test_set_next_deadline_op(dl_none);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = ack_pump; /* well-behaved: acknowledges terminal */
    cfg.max_connections = 2;
    cfg.lane_count = 1; /* both children on lane 0 */
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        void *c_reap = NULL, *c_surv = NULL;
        /* accept c_reap FIRST so it PRECEDES c_surv in the chain: c_reap is
         * reaped + its slot reused mid-traversal, and the pump must still reach
         * c_surv via the captured `next` (the old second-traversal code would
         * follow the reused slot's corrupted snap_next and miss the survivor) */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c_reap));
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c_surv));
        g_dl.m = m;
        g_dl.reaped = c_reap;
        /* c_reap: fully gated -> reaped this pump. c_surv: a live session. */
        moq_wtquic_msquic_managed_test_deliver_established(m, c_reap, "x-not-moq");
        moq_wtquic_msquic_managed_test_mark_terminal_observed(m, c_reap);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c_reap);
        moq_wtquic_msquic_managed_test_set_conn_established(c_surv, ws, sess);

        /* reassign c_reap's slot the instant it is published free */
        moq_wtquic_msquic_managed_test_set_after_reap(ar_reuse);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        moq_wtquic_msquic_managed_test_set_after_reap(NULL);

        CHECK(g_dl.reused == c_reap); /* the freed slot WAS reassigned mid-sweep */
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c_surv));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2); /* survivor + reused */
        /* the traversal never re-read the reaped (now reused) slot: */
        CHECK(!g_dl.queried_reaped);        /* its deadline was NOT queried */
        CHECK(g_dl.dl_last_conn == c_surv); /* only the survivor's was */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    moq_wtquic_msquic_managed_test_set_next_deadline_op(NULL);
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- live reap: three-part gate, orderings, iterability, count, reuse ------ */
/* A pump that records the connections it iterated this pass, and can drive one
 * target child terminal FROM INSIDE the callback (after the snapshot), to make a
 * same-pass terminal that must not be reaped until a later pump. */
static struct {
    uint32_t seen;
    void *conns[REC_MAX];
    void *trigger; /* if set, made terminal mid-callback, then cleared */
    bool no_ack;   /* model an app that never acknowledges */
} g_lr;
/* Drive a synthetic child to "session terminal observed": the real path is a
 * poll of MOQ_EVENT_SESSION_CLOSED, which a child with no session cannot do. */
static void mm_test_make_observed(moq_wtquic_msquic_managed_t *m, void *c)
{
    moq_wtquic_msquic_managed_test_mark_terminal_observed(m, c);
}

/*
 * Three fixtures, one per reachable child shape. They are kept distinct so a
 * test always exercises the contract it claims: a pre-session child can never
 * observe or acknowledge anything, so observation is NEVER injected on one.
 */

/* PRE-SESSION terminal: establishment fails before a MoQ session exists.
 * Nothing to observe, nothing to acknowledge; reclaims on F1 + F2 alone. */
static void mm_test_terminal_only(moq_wtquic_msquic_managed_t *m, void *c)
{
    bool lt = false, tq = false, sb = false;
    moq_wtquic_msquic_managed_test_deliver_established(m, c, "x-not-moq");
    moq_wtquic_msquic_managed_test_conn_gate(c, &lt, &tq, &sb);
    CHECK(lt && !sb);   /* terminal, and NOT session-backed */
}

/* SESSION-BACKED, terminal, terminal event NOT observed: the state that must
 * hold its capacity slot. */
static void mm_test_session_terminal_unobserved(moq_wtquic_msquic_managed_t *m,
                                                void *c)
{
    bool lt = false, tq = false, sb = false;
    moq_wtquic_msquic_managed_test_set_conn_established(
        c, (wtq_session_t *)0x1, (void *)0x2);
    moq_wtquic_msquic_managed_test_mark_logical_terminal(m, c);
    moq_wtquic_msquic_managed_test_conn_gate(c, &lt, &tq, &sb);
    CHECK(lt && sb);    /* the predicate branch under test */
    CHECK(!moq_wtquic_msquic_managed_test_conn_terminal_observed(c));
}

/* SESSION-BACKED, terminal, terminal event OBSERVED: ready to acknowledge. */
static void mm_test_make_terminal(moq_wtquic_msquic_managed_t *m, void *c)
{
    mm_test_session_terminal_unobserved(m, c);
    mm_test_make_observed(m, c);
    CHECK(moq_wtquic_msquic_managed_test_conn_terminal_observed(c));
}
static int lr_pump(moq_wtquic_msquic_managed_t *m,
                   moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                   void *user)
{
    (void)now_us;
    (void)user;
    g_lr.seen = 0;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL && g_lr.seen < REC_MAX;
         c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        g_lr.conns[g_lr.seen++] = c;
        /* A well-behaved application acknowledges from the owning lane callback
         * once it has observed the terminal event and finished its terminal
         * processing for the connection. g_lr.no_ack models an app that never
         * does, which must hold the slot indefinitely. */
        if (!g_lr.no_ack)
            (void)moq_wtquic_msquic_managed_conn_ack_terminal(c);
    }
    if (g_lr.trigger != NULL) {
        /* a SESSION-BACKED child going terminal mid-callback: it is subject to
         * the observe+acknowledge contract, and this callback has already
         * iterated past it, so it cannot have been acknowledged here */
        mm_test_make_terminal(m, g_lr.trigger);
        g_lr.trigger = NULL;
    }
    return 0;
}
static bool lr_saw(const void *c)
{
    for (uint32_t i = 0; i < g_lr.seen; i++)
        if (g_lr.conns[i] == c)
            return true;
    return false;
}
static moq_wtquic_msquic_managed_t *lr_make_server(acnt_t *a, moq_alloc_t *alloc,
                                                   uint32_t n)
{
    alloc->ctx = a;
    alloc->alloc = acnt_alloc;
    alloc->realloc = acnt_realloc;
    alloc->free = acnt_free;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = lr_pump;
    cfg.max_connections = n;
    cfg.lane_count = n; /* one lane per child, so each is pumped independently */
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    return m;
}

/* incomplete gates hold the slot; either terminal/quiescence ordering reaps */
static void test_live_reap_gates(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = lr_make_server(&a, &alloc, 4);
    if (m != NULL) {
        void *c0 = NULL, *c1 = NULL, *c2 = NULL, *c3 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0)); /* lane 0 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1)); /* lane 1 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c2)); /* lane 2 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c3)); /* lane 3 */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 4);

        /* Each incomplete gate holds its slot through a pump:
         *  c1 terminal + pumped, NOT quiesced;
         *  c2 quiesced + pumped, NOT terminal;
         *  c3 terminal + quiesced, NOT pumped. */
        mm_test_make_terminal(m, c1);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 1) == 0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c2);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 2) == 0);
        mm_test_make_terminal(m, c3);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c3);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 4); /* none reaped */
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c1));
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c2));
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c3));

        /* c0: full gate, ordering A (terminal -> quiesce -> pump). The reaping
         * pump still presents it (terminal children stay iterable for it), then
         * reaps it. */
        mm_test_make_terminal(m, c0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(lr_saw(c0));                                     /* iterable */
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0)); /* reaped */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 3);   /* decremented */

        /* completing c1's missing gate (quiesce) then pumping reaps it too;
         * ordering here is terminal -> pump -> quiesce -> pump */
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c1);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 1) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c1));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 2);

        /* c2: ordering B (quiesce -> terminal -> pump) — reaches the gate the
         * other way around */
        mm_test_make_terminal(m, c2);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 2) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c2));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* c3 was terminal + quiesced but never pumped — its first pump reaps it */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 3) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c3));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* exactly-once resource reap, clean generation reset, no double release */
static void test_live_reap_reuse(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = lr_make_server(&a, &alloc, 2);
    if (m != NULL) {
        bool lt, tq, sb, qr, cp;
        moq_version_t ver;
        void *usr;
        void *c0 = NULL, *reuse = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0)); /* lane 0 */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* Give the generation a full complement of reusable state AND a retained
         * wtquic ref, so both a stale-state regression and a double release
         * would be caught. */
        moq_wtquic_msquic_managed_test_seed_conn(c0, FAKE_WS,
                                                 MOQ_VERSION_DRAFT_18,
                                                 (void *)0x77, true, true);

        /* full gate -> reap on the next pump of its lane; the retained ref is
         * released exactly once */
        g_release_count = 0;
        mm_test_make_terminal(m, c0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        CHECK(g_release_count == 1); /* retained ref released once at reap */

        /* exactly-once: pumping the lane again neither re-reaps nor re-releases
         * (c0 is no longer in the snapshot) */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        CHECK(g_release_count == 1);

        /* the freed slot is reused by a fresh admission, and EVERY per-generation
         * field is reset — not just the gate flags */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &reuse) && reuse != NULL);
        CHECK(reuse == c0); /* same slot */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(reuse));
        moq_wtquic_msquic_managed_test_conn_gate(reuse, &lt, &tq, &sb);
        CHECK(!lt && !tq && !sb); /* terminal / quiesce / session-backed cleared */
        moq_wtquic_msquic_managed_test_conn_gen_state(reuse, &ver, &usr, &qr, &cp);
        CHECK(ver == 0 && usr == NULL && !qr && !cp); /* version/user/latches */

        /* teardown reaps the reused child once; the old c0 generation was
         * already reaped, so its ref is NOT released a second time here */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        CHECK(g_release_count == 1); /* no second release of the old ref */
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* a child that is quiesced and becomes terminal DURING a pump is not reaped that
 * pump -- the callback had already iterated past it, so it could not have
 * acknowledged -- only the next */
static void test_live_reap_same_pass(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = lr_make_server(&a, &alloc, 1);
    if (m != NULL) {
        bool lt, tq, sb;
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0); /* quiesced first */

        /* pump 1: the callback makes c0 terminal AFTER it had already iterated
         * past it, so c0 cannot have been acknowledged here -- it is NOT
         * reaped, even though it is now terminal + quiesced + observed */
        g_lr.trigger = c0;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && tq && sb);   /* session-backed: needs observe + ack */
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0)); /* not reaped */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* pump 2: c0 is presented terminal from the start, so the callback
         * acknowledges it and it is reaped */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* Create a client facade over the installed fake transport (no real dial). */
static moq_wtquic_msquic_managed_t *make_client(const moq_alloc_t *alloc,
                                                const char *const *protos,
                                                size_t nprotos)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.on_lane_pump = pump;
    if (protos != NULL) {
        cfg.wt_protocols = protos;
        cfg.wt_protocol_count = nprotos;
    }
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    return m;
}

/* --- WT_ALPN_ERROR on the client negotiation-failure route (RED) --------- */
/* draft-ietf-webtrans-http3-15 3.3 + its IANA registry entry: a client that
 * REQUIRES application-protocol negotiation MUST close the WebTransport session
 * with WT_ALPN_ERROR (0x0817b3dd) when a successful response omits WT-Protocol,
 * or carries a token the client never offered. Client-side wire obligation,
 * observed at the establishment close -- not a server-local surrogate.
 *
 * Only the NEGOTIATION-failure class owes this code. Session/adapter
 * construction failures share mm_establish_fail() but are NOT ALPN failures,
 * and their positive close contract is deliberately not asserted here. */
#define WT_ALPN_ERROR_CODE 0x0817b3ddu

/* Each arm is declared, so the two required failure classes cannot collapse
 * into one: the token actually handed to establishment, whether it must be
 * present, and (when present) its exact bytes. */
typedef struct {
    const char *name;          /* arm name, printed in every diagnostic */
    const char *tok;           /* token passed to the establishment callback */
    bool        expect_present;/* must the arm pass a non-NULL token? */
    const char *expect_bytes;  /* exact bytes when present */
} alpn_arm_t;

static const char *g_alpn_arm = "(none)";
#define ACHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL [arm %s] %s:%d: %s\n", \
                g_alpn_arm, __FILE__, __LINE__, #expr); \
        g_fail++; \
    } \
} while (0)

/* --- captured connect image (the client's OUTBOUND offer) ----------------- */
static void       *g_alpn_conn;
static int         g_alpn_connects;
static size_t      g_alpn_offer_count;
static char        g_alpn_offer0[64];
static bool        g_alpn_offer0_null;
static wtq_result_t alpn_capture_connect(wtq_msquic_env_t *env,
                                         const wtq_msquic_client_cfg_t *cfg,
                                         wtq_session_t **out)
{
    (void)env;
    g_alpn_connects++;
    g_alpn_conn = cfg->user;              /* the admitted connection */
    g_alpn_offer_count = 0;
    g_alpn_offer0[0] = '\0';
    g_alpn_offer0_null = true;
    if (cfg->connect != NULL) {
        g_alpn_offer_count = cfg->connect->subprotocol_count;
        if (cfg->connect->subprotocols != NULL &&
            cfg->connect->subprotocol_count > 0 &&
            cfg->connect->subprotocols[0] != NULL) {
            g_alpn_offer0_null = false;
            snprintf(g_alpn_offer0, sizeof(g_alpn_offer0), "%s",
                     cfg->connect->subprotocols[0]);
        }
    }
    *out = FAKE_WS;
    return WTQ_OK;
}

/* --- close observer -------------------------------------------------------- */
static int             g_alpn_close_calls;
static wtq_session_t  *g_alpn_close_ws;
static uint32_t        g_alpn_close_code;
static void alpn_close_op(wtq_session_t *ws, uint32_t code)
{
    g_alpn_close_calls++;
    g_alpn_close_ws = ws;
    g_alpn_close_code = code;
}

/* One arm: fresh fixture, exact draft-18 offer, driven through the REAL
 * establishment callback. Failure-safe: if the preflight says this token no
 * longer rejects, the arm reports that and does NOT drive the (now successful)
 * establishment route. */
static void alpn_run_arm(const alpn_arm_t *arm)
{
    g_alpn_arm = arm->name;
    g_alpn_conn = NULL; g_alpn_connects = 0;
    g_alpn_offer_count = 0; g_alpn_offer0[0] = '\0'; g_alpn_offer0_null = true;
    g_alpn_close_calls = 0; g_alpn_close_ws = NULL; g_alpn_close_code = 0;

    moq_wtquic_msquic_managed_test_set_transport(alpn_capture_connect,
                                                 trivial_release);
    moq_wtquic_msquic_managed_test_set_close_op(alpn_close_op);

    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    const char *only18[] = { "moqt-18" };
    moq_wtquic_msquic_managed_t *m = make_client(&alloc, only18, 1);
    if (m != NULL) {
        /* -- the arm's own token really is of the declared class ---------- */
        ACHECK(arm->expect_present == (arm->tok != NULL));
        if (arm->expect_present) {
            ACHECK(arm->tok != NULL && strlen(arm->tok) > 0);
            ACHECK(arm->tok != NULL && arm->expect_bytes != NULL &&
                   strcmp(arm->tok, arm->expect_bytes) == 0);
        }

        /* -- the OUTBOUND offer materialized as exactly one exact token ---- */
        ACHECK(g_alpn_connects == 1);
        ACHECK(g_alpn_offer_count == 1);
        ACHECK(!g_alpn_offer0_null);
        ACHECK(strcmp(g_alpn_offer0, "moqt-18") == 0);
        ACHECK(g_alpn_conn != NULL);

        /* -- preflight: the REAL negotiator must reject this token --------- */
        moq_version_t nv = 0;
        bool rejects = !moq_wtquic_msquic_managed_test_negotiate(
            m, arm->tok, arm->tok != NULL ? strlen(arm->tok) : 0, &nv);
        ACHECK(rejects);

        if (rejects && g_alpn_conn != NULL) {
            void *conn = g_alpn_conn;
            moq_wtquic_msquic_managed_test_deliver_established(m, conn, arm->tok);

            /* -- the WIRE obligation: this is the RED -- */
            ACHECK(g_alpn_close_calls == 1);
            ACHECK(g_alpn_close_ws == FAKE_WS);
            ACHECK(g_alpn_close_code == WT_ALPN_ERROR_CODE);

            const int      seen_calls = g_alpn_close_calls;
            wtq_session_t *seen_ws    = g_alpn_close_ws;
            const uint32_t seen_code  = g_alpn_close_code;

            /* -- declared pre-session terminal inventory, before teardown -- */
            ACHECK(moq_wtquic_msquic_managed_session(m) == NULL);
            ACHECK(moq_wtquic_msquic_managed_conn_session(conn) == NULL);
            ACHECK(moq_wtquic_msquic_managed_negotiated_version(m) == 0);
            ACHECK(moq_wtquic_msquic_managed_conn_negotiated_version(conn) == 0);
            ACHECK(moq_wtquic_msquic_managed_is_fatal(m));
            ACHECK(moq_wtquic_msquic_managed_fatal_code(m) == 0);
            ACHECK(!moq_wtquic_msquic_managed_is_closed(m));   /* not ALSO clean-closed */
            /* SOURCE-DERIVED, not observed. Both facts are independent:
             * conn_count is incremented only in mm_accept_prepare (the SERVER
             * admission path), and in_use is the server child-slot reservation
             * bit -- mm_client_start() zero-initializes the client connection
             * and never sets it. So a CLIENT facade reports 0 live connections
             * and its connection is NOT in_use. The pointer is nonetheless
             * valid here, which the gate and gen-state reads below evidence. */
            ACHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
            ACHECK(!moq_wtquic_msquic_managed_test_conn_in_use(conn));

            bool lt = false, tq = true, sb = true;
            moq_wtquic_msquic_managed_test_conn_gate(conn, &lt, &tq, &sb);
            ACHECK(lt);        /* logical_terminal latched */
            ACHECK(!tq);       /* transport NOT quiesced on this path */
            ACHECK(!sb);       /* pre-session failure: not session-backed */

            moq_version_t cv = 1; void *cu = (void *)1;
            bool q = true, cp = true;
            moq_wtquic_msquic_managed_test_conn_gen_state(conn, &cv, &cu, &q, &cp);
            ACHECK(cv == 0);   /* internal connection version */
            ACHECK(cu == NULL);
            ACHECK(!q);        /* quiesce_requested */
            ACHECK(!cp);       /* close_pending */

            /* -- lifecycle conservation: the observer stays installed across
             * stop/destroy, so a duplicate or altered close during cleanup is
             * caught rather than happening after the last assertion. */
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
            m = NULL;
            ACHECK(g_alpn_close_calls == seen_calls);   /* still exactly one */
            ACHECK(g_alpn_close_ws == seen_ws);         /* same session */
            ACHECK(g_alpn_close_code == seen_code);     /* unchanged */
        }
        if (m != NULL) {
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
    }
    ACHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);

    moq_wtquic_msquic_managed_test_set_close_op(NULL);
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);
    g_alpn_arm = "(none)";
}

static void test_alpn_error_on_negotiation_failure(void)
{
    static const alpn_arm_t arms[] = {
        { "missing-token",   NULL,      false, NULL },
        { "unoffered-token", "moqt-16", true,  "moqt-16" },
    };
    moq_wtquic_msquic_managed_test_no_listener(true);
    for (size_t i = 0; i < sizeof(arms) / sizeof(arms[0]); i++)
        alpn_run_arm(&arms[i]);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* --- WT-Protocol negotiation (exact token + offered enforcement) --------- */
/* Negative CLASS discriminator: a construction failure is NOT an ALPN failure.
 * The token is offered AND selected, so negotiation SUCCEEDS; the allocator is
 * then limited to zero further allocations immediately before establishment, so
 * the FIRST moq_session_create() allocation fails and the route closes before
 * any adapter can dereference the sentinel session. Asserts only the negative
 * -- it does not freeze 0 as the one correct construction-failure code. */
static void test_alpn_not_used_for_construction_failure(void)
{
    g_alpn_arm = "construction-failure";
    moq_wtquic_msquic_managed_test_no_listener(true);

    g_alpn_conn = NULL; g_alpn_connects = 0;
    g_alpn_close_calls = 0; g_alpn_close_ws = NULL; g_alpn_close_code = 0;
    moq_wtquic_msquic_managed_test_set_transport(alpn_capture_connect,
                                                 trivial_release);
    moq_wtquic_msquic_managed_test_set_close_op(alpn_close_op);

    acnt_t a;
    acnt_init(&a);                     /* UNLIMITED for facade construction */
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    const char *only18[] = { "moqt-18" };
    moq_wtquic_msquic_managed_t *m = make_client(&alloc, only18, 1);
    if (m != NULL) {
        ACHECK(g_alpn_connects == 1);
        ACHECK(g_alpn_offer_count == 1);
        ACHECK(!g_alpn_offer0_null);
        ACHECK(strcmp(g_alpn_offer0, "moqt-18") == 0);
        ACHECK(g_alpn_conn != NULL);
        if (g_alpn_conn != NULL) {
            void *conn = g_alpn_conn;
            /* the arm: this token IS accepted by the real negotiator */
            moq_version_t nv = 0;
            ACHECK(moq_wtquic_msquic_managed_test_negotiate(m, "moqt-18", 7, &nv));
            ACHECK(nv == MOQ_VERSION_DRAFT_18);

            /* starve the allocator only now, so the construction route fails.
             * Denial accounting starts at this instant. */
            acnt_arm_zero(&a);
            long pre_denials = -1, pre_first = -2;
            acnt_denials(&a, &pre_denials, &pre_first);
            ACHECK(pre_denials == 0);      /* armed, nothing refused yet */
            ACHECK(pre_first == -1);

            moq_wtquic_msquic_managed_test_deliver_established(m, conn, "moqt-18");

            /* the denial is MEASURED, not inferred from the terminal result:
             * at least one allocation was refused, and the FIRST allocation
             * attempted after arming is the one that was refused (no armed
             * allocation succeeded before it) */
            long denials = -1, first_after = -2;
            acnt_denials(&a, &denials, &first_after);
            /* EXACT inventory: the failure path performs exactly one
             * allocator call after arming -- moq_session_create()'s first
             * allocation -- so the arm pins the count, not merely ">= 1". */
            ACHECK(denials == 1);
            ACHECK(first_after == 0);

            /* lift the limit so teardown runs outside the fault-injection
             * arm; no claim is made about whether teardown allocates */
            acnt_disarm(&a);

            ACHECK(g_alpn_close_calls == 1);
            ACHECK(g_alpn_close_ws == FAKE_WS);
            ACHECK(g_alpn_close_code != WT_ALPN_ERROR_CODE);   /* the negative */
            ACHECK(moq_wtquic_msquic_managed_conn_session(conn) == NULL);
            ACHECK(moq_wtquic_msquic_managed_conn_negotiated_version(conn) == 0);
            ACHECK(moq_wtquic_msquic_managed_session(m) == NULL);
            ACHECK(moq_wtquic_msquic_managed_negotiated_version(m) == 0);
            ACHECK(moq_wtquic_msquic_managed_is_fatal(m));
            ACHECK(moq_wtquic_msquic_managed_fatal_code(m) == 0);

            const int      seen_calls = g_alpn_close_calls;
            wtq_session_t *seen_ws    = g_alpn_close_ws;
            const uint32_t seen_code  = g_alpn_close_code;
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
            m = NULL;
            ACHECK(g_alpn_close_calls == seen_calls);
            ACHECK(g_alpn_close_ws == seen_ws);
            ACHECK(g_alpn_close_code == seen_code);
        }
        if (m != NULL) {
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
    }
    ACHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_set_close_op(NULL);
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);
    moq_wtquic_msquic_managed_test_no_listener(false);
    g_alpn_arm = "(none)";
}

static void test_negotiation_matrix(void)
{
    moq_version_t v;

    /* explicit offer draft-18 + draft-16 */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        const char *both[] = { "moqt-18", "moqt-16" };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, both, 2);
        if (m != NULL) {
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, "moqt-18", 7, &v) &&
                  v == MOQ_VERSION_DRAFT_18);
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, "moqt-16", 7, &v) &&
                  v == MOQ_VERSION_DRAFT_16);
            /* no token -> legacy draft-16 fallback (draft-16 was offered) */
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, NULL, 0, &v) &&
                  v == MOQ_VERSION_DRAFT_16);
            /* unrecognized token -> no version (build no session) */
            v = 0;
            CHECK(!moq_wtquic_msquic_managed_test_negotiate(m, "moqt-99", 7, &v));
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* explicit offer draft-18 ONLY: draft-16 is recognized but un-offered, so
     * it is fatal, never a silent downgrade; and no-token has no fallback */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        const char *only18[] = { "moqt-18" };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, only18, 1);
        if (m != NULL) {
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, "moqt-18", 7, &v) &&
                  v == MOQ_VERSION_DRAFT_18);
            v = 0;
            CHECK(!moq_wtquic_msquic_managed_test_negotiate(m, "moqt-16", 7, &v));
            v = 0;
            CHECK(!moq_wtquic_msquic_managed_test_negotiate(m, NULL, 0, &v));
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* default (NULL) offer materializes to draft-18 + draft-16 */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            CHECK(moq_wtquic_msquic_managed_test_proto_count(m) == 2);
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, "moqt-18", 7, &v) &&
                  v == MOQ_VERSION_DRAFT_18);
            v = 0;
            CHECK(moq_wtquic_msquic_managed_test_negotiate(m, NULL, 0, &v) &&
                  v == MOQ_VERSION_DRAFT_16);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
}

/* --- first fatal cause is preserved; fatal outranks a clean close -------- */
static void test_first_fatal_cause(void)
{
    /* clean close, then fatal: fatal is recorded and outranks the close */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_latch_closed(m, 7);
            CHECK(moq_wtquic_msquic_managed_is_closed(m) &&
                  moq_wtquic_msquic_managed_close_code(m) == 7 &&
                  !moq_wtquic_msquic_managed_is_fatal(m));
            moq_wtquic_msquic_managed_test_latch_fatal(m, 42);
            CHECK(moq_wtquic_msquic_managed_is_fatal(m) &&
                  moq_wtquic_msquic_managed_fatal_code(m) == 42);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* the FIRST fatal cause wins over a later one */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_latch_fatal(m, 42);
            moq_wtquic_msquic_managed_test_latch_fatal(m, 99);
            CHECK(moq_wtquic_msquic_managed_fatal_code(m) == 42);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* with a fatal latched, a later clean close does NOT overwrite it */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_latch_fatal(m, 42);
            moq_wtquic_msquic_managed_test_latch_closed(m, 7);
            CHECK(moq_wtquic_msquic_managed_is_fatal(m) &&
                  moq_wtquic_msquic_managed_fatal_code(m) == 42 &&
                  !moq_wtquic_msquic_managed_is_closed(m));
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
}

/* --- the session handle is published UNDER the lane guard, released once -- */
/* A worker (modelling a connect-time callback) can enter the lane guard only
 * once the publisher releases it — after connect + publication. When it gets
 * in, the handle must already be published. The guard is held ACROSS connect
 * (wtquic's abandon path makes that deadlock-free), so this is deterministic:
 * condition-variable gated, no timing. The app-owned ref is also released
 * exactly once at reap. */
static struct pubprobe {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    wtq_guard_t guard;
    moq_wtquic_msquic_managed_conn_t *child;
    pthread_t worker;
    bool spawned;
    bool done;
    bool saw_published;
} g_pp;
static long g_pub_release_count;
static void counting_release(wtq_session_t *ws)
{
    CHECK(ws == (void *)FAKE_WS);
    g_pub_release_count++;
}

static void *pub_worker(void *arg)
{
    struct pubprobe *p = arg;
    p->guard.enter(p->guard.ctx); /* blocks until the publisher releases */
    void *ws = moq_wtquic_msquic_managed_test_conn_ws(p->child);
    p->guard.leave(p->guard.ctx);
    pthread_mutex_lock(&p->mu);
    p->saw_published = (ws == (void *)FAKE_WS);
    p->done = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

static wtq_result_t probe_connect(wtq_msquic_env_t *env,
                                  const wtq_msquic_client_cfg_t *cfg,
                                  wtq_session_t **out)
{
    (void)env;
    /* invoked by mm_client_start WHILE HOLDING the lane guard */
    g_pp.guard = cfg->guard;
    g_pp.child = cfg->user;
    if (pthread_create(&g_pp.worker, NULL, pub_worker, &g_pp) != 0)
        return WTQ_ERR_BACKEND; /* fail the connect so create() fails visibly */
    g_pp.spawned = true;
    *out = FAKE_WS; /* mm_client_start publishes then releases the guard */
    return WTQ_OK;
}

static void test_guard_held_publication(void)
{
    memset(&g_pp, 0, sizeof(g_pp));
    pthread_mutex_init(&g_pp.mu, NULL);
    pthread_cond_init(&g_pp.cv, NULL);
    g_pub_release_count = 0;
    moq_wtquic_msquic_managed_test_set_transport(probe_connect, counting_release);

    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
    if (m != NULL) {
        CHECK(g_pp.spawned);
        pthread_mutex_lock(&g_pp.mu);
        while (!g_pp.done)
            pthread_cond_wait(&g_pp.cv, &g_pp.mu);
        pthread_mutex_unlock(&g_pp.mu);
        /* join failure is fatal: the worker cannot be reaped, so its state is
         * unsafe to read (and leaving it running would corrupt later tests) */
        if (pthread_join(g_pp.worker, NULL) != 0)
            abort();
        /* the callback that entered the guard saw the published handle:
         * publication happened before the guard was released */
        CHECK(g_pp.saw_published);
        CHECK(moq_wtquic_msquic_managed_test_client_ws(m) == (void *)FAKE_WS);
        moq_wtquic_msquic_managed_stop(m);
        moq_wtquic_msquic_managed_destroy(m);
        CHECK(g_pub_release_count == 1); /* released exactly once at reap */
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);

    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);
    pthread_cond_destroy(&g_pp.cv);
    pthread_mutex_destroy(&g_pp.mu);
}

/* --- on_draining does not deref the NULL attach-table member ------------- */
static void test_draining_null_callback(void)
{
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
    if (m != NULL) {
        /* the attach table leaves on_draining NULL; forwarding it with a
         * non-NULL adapter must skip it, not crash */
        moq_wtquic_msquic_managed_test_deliver_draining(m);
        CHECK(1); /* reached here => no crash */
        moq_wtquic_msquic_managed_stop(m);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
}

/* --- env config forwards the bridged allocator + idle timeout ------------ */
static void test_env_cfg_forwarding(void)
{
    /* nonzero idle_timeout_ms -> tuning initialized with that override */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = 443;
        cfg.on_lane_pump = pump;
        cfg.idle_timeout_ms = 5000;
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
        if (m != NULL) {
            wtq_msquic_env_cfg_t ecfg;
            moq_wtquic_msquic_managed_test_fill_env_cfg(m, &ecfg);
            CHECK(ecfg.tuning.struct_size != 0); /* tuning applied */
            CHECK(ecfg.tuning.idle_timeout_ms == 5000);
            CHECK(ecfg.alloc != NULL); /* bridged allocator, not the default */
            CHECK(ecfg.alloc->alloc == acnt_alloc);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* zero idle_timeout_ms -> tuning left as defaults (struct_size == 0) */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            wtq_msquic_env_cfg_t ecfg;
            moq_wtquic_msquic_managed_test_fill_env_cfg(m, &ecfg);
            CHECK(ecfg.tuning.struct_size == 0); /* defaults */
            CHECK(ecfg.alloc != NULL && ecfg.alloc->alloc == acnt_alloc);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
}

/* --- create-unwind OOM: facade allocations + real env_open --------------- */
/* The fake connect (installed globally) allocates nothing, so injected OOM
 * exercises the facade's own allocations and the real env_open — never
 * wtquic's connect internals (not OOM-hardened at the pinned revision). */
static void test_create_unwind(void)
{
    bool saw_success = false;
    for (long k = 0; k < 40 && !saw_success; k++) {
        acnt_t a;
        acnt_init_budget(&a, k);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        const char *protos[] = { "moqt-18" };
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = 443;
        cfg.on_lane_pump = pump;
        cfg.wt_protocols = protos;
        cfg.wt_protocol_count = 1;
        moq_wtquic_msquic_managed_t *m = NULL;
        moq_result_t rc = moq_wtquic_msquic_managed_create(&cfg, &m);
        if (rc == MOQ_OK) {
            saw_success = true;
            CHECK(m != NULL);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        } else {
            CHECK(rc == MOQ_ERR_NOMEM);
            CHECK(m == NULL);
        }
        CHECK(acnt_live(&a) == 0); /* balanced either way */
        acnt_destroy(&a);
    }
    CHECK(saw_success);
}

/* --- connect result mapping: each wtquic code -> the right moq code ------- */
static wtq_result_t g_forced_connect_rc;
static wtq_result_t forced_connect(wtq_msquic_env_t *env,
                                   const wtq_msquic_client_cfg_t *cfg,
                                   wtq_session_t **out)
{
    (void)env;
    (void)cfg;
    (void)out;
    return g_forced_connect_rc; /* fail without publishing a handle */
}
static void test_connect_result_mapping(void)
{
    struct {
        wtq_result_t in;
        moq_result_t out;
    } cases[] = {
        { WTQ_ERR_INVALID_ARG, MOQ_ERR_INVAL },
        { WTQ_ERR_TOO_LARGE, MOQ_ERR_INVAL },
        { WTQ_ERR_NOMEM, MOQ_ERR_NOMEM },
        { WTQ_ERR_BACKEND, MOQ_ERR_INTERNAL },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        g_forced_connect_rc = cases[i].in;
        moq_wtquic_msquic_managed_test_set_transport(forced_connect,
                                                     trivial_release);
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = 443;
        cfg.on_lane_pump = pump;
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == cases[i].out);
        CHECK(m == NULL);
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
    /* restore the default fake transport */
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect,
                                                 trivial_release);
}

/* --- wait() state machine: level-retained activity + terminal ------------ */
static void test_wait_state_machine(void)
{
    /* pre-latched activity resolves one wait to MOQ_OK, and is consumed exactly
     * once: the next zero-timeout wait is MOQ_DONE. */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_set_activity(m);
            CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_OK);
            CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_DONE);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* a latched fatal cause resolves wait to MOQ_ERR_CLOSED */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_latch_fatal(m, 7);
            CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_ERR_CLOSED);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* a clean close resolves wait to MOQ_ERR_CLOSED */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            moq_wtquic_msquic_managed_test_latch_closed(m, 3);
            CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_ERR_CLOSED);
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }

    /* a completed stop resolves wait to MOQ_ERR_CLOSED */
    {
        acnt_t a;
        acnt_init(&a);
        moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
        moq_wtquic_msquic_managed_t *m = make_client(&alloc, NULL, 0);
        if (m != NULL) {
            /* stop + join -> m->stopped */
            CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
            CHECK(moq_wtquic_msquic_managed_wait(m, 0) == MOQ_ERR_CLOSED);
            moq_wtquic_msquic_managed_destroy(m);
        }
        CHECK(acnt_live(&a) == 0);
        acnt_destroy(&a);
    }
}

/* --- webtransport_profile: tail-field ABI + client/server forwarding ------ *
 * The profile is a struct_size-gated tail field on the managed cfg, forwarded
 * verbatim onto BOTH the client connect config and the server listener config.
 * This proves: the complete-presence gate (previous-size / partial-field poison
 * treated as absent -> CURRENT), forward-compat (oversized-future struct read
 * correctly, unknown tail ignored), range rejection (invalid -> INVAL, no
 * start), and that BOTH the server path (listener cfg) and the client path
 * (connect cfg) actually carry the configured value. */
static void test_wt_profile_abi_forwarding(void)
{
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    const size_t off =
        offsetof(moq_wtquic_msquic_managed_cfg_t, webtransport_profile);

#define SRV_BASE(c) do {                                                    \
        moq_wtquic_msquic_managed_cfg_init_sized(&(c), sizeof(c));          \
        (c).alloc = &alloc; (c).perspective = MOQ_PERSPECTIVE_SERVER;       \
        (c).cert_path = "c"; (c).key_path = "k"; (c).on_lane_pump = pump;   \
    } while (0)
#define SRV_CREATE_OK(c) do {                                               \
        moq_wtquic_msquic_managed_t *m = NULL;                             \
        CHECK(moq_wtquic_msquic_managed_create(&(c), &m) == MOQ_OK);        \
        if (m != NULL) {                                                    \
            moq_wtquic_msquic_managed_stop(m);                             \
            moq_wtquic_msquic_managed_destroy(m);                         \
        }                                                                  \
    } while (0)

    moq_wtquic_msquic_managed_test_no_listener(true);

    /* SERVER forwarding + default: full-size cfg, unset -> CURRENT on the
     * listener cfg. */
    {
        moq_wtquic_msquic_managed_cfg_t c; SRV_BASE(c);
        SRV_CREATE_OK(c);
        CHECK(moq_wtquic_msquic_managed_test_last_listener_profile() ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT);
    }
    /* SERVER forwarding, explicit compat -> forwarded onto the listener cfg. */
    {
        moq_wtquic_msquic_managed_cfg_t c; SRV_BASE(c);
        c.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        SRV_CREATE_OK(c);
        CHECK(moq_wtquic_msquic_managed_test_last_listener_profile() ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT);
    }
    /* PREVIOUS-SIZE caller: struct_size stops before the field. Poison the
     * field, but the complete-presence gate must treat it as ABSENT -> CURRENT. */
    {
        moq_wtquic_msquic_managed_cfg_t c; SRV_BASE(c);
        c.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        c.struct_size = (uint32_t)off;
        SRV_CREATE_OK(c);
        CHECK(moq_wtquic_msquic_managed_test_last_listener_profile() ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT);
    }
    /* PARTIAL FIELD: struct_size covers only one byte of the four -> absent. */
    {
        moq_wtquic_msquic_managed_cfg_t c; SRV_BASE(c);
        c.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        c.struct_size = (uint32_t)(off + 1);
        SRV_CREATE_OK(c);
        CHECK(moq_wtquic_msquic_managed_test_last_listener_profile() ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT);
    }
    /* OVERSIZED FUTURE caller: struct_size beyond sizeof (padded buffer). The
     * known field IS read; the unknown tail is ignored (never over-read). */
    {
        struct { moq_wtquic_msquic_managed_cfg_t c; uint8_t pad[16]; } big;
        SRV_BASE(big.c);
        memset(big.pad, 0, sizeof(big.pad));
        big.c.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        big.c.struct_size = (uint32_t)(sizeof(big.c) + sizeof(big.pad));
        SRV_CREATE_OK(big.c);
        CHECK(moq_wtquic_msquic_managed_test_last_listener_profile() ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT);
    }
    /* INVALID VALUE: out-of-range -> create rejects with INVAL, never starts. */
    {
        moq_wtquic_msquic_managed_cfg_t c; SRV_BASE(c);
        c.webtransport_profile = 7;
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(&c, &m) == MOQ_ERR_INVAL);
        CHECK(m == NULL);
    }

    /* CLIENT forwarding: a capturing connect reads the profile off the connect
     * config the client path built (ccfg->connect->webtransport_profile). */
    moq_wtquic_msquic_managed_test_no_listener(false);
    g_captured_client_profile = 0xFFFFFFFFu;
    moq_wtquic_msquic_managed_test_set_transport(capture_connect, trivial_release);
    {
        moq_wtquic_msquic_managed_cfg_t c;
        moq_wtquic_msquic_managed_cfg_init_sized(&c, sizeof(c));
        c.alloc = &alloc;
        c.perspective = MOQ_PERSPECTIVE_CLIENT;
        c.host = "127.0.0.1";
        c.port = 443;
        c.on_lane_pump = pump;
        c.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(&c, &m) == MOQ_OK);
        CHECK(g_captured_client_profile ==
              (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT);
        if (m != NULL) {
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
    }
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);

#undef SRV_CREATE_OK
#undef SRV_BASE

    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
}

/* --- app service-deadline fold (whole-block gated) ----------------------- */

/* A pure cached read of a distinctive absolute deadline (like media_sender's
 * refresh_wake_deadline_us). Far in the future, so no worker sweep spins. */
static _Atomic uint64_t g_wmm_app_target = UINT64_MAX;
static uint64_t wmm_app_deadline(void *ctx)
{ (void)ctx; return atomic_load(&g_wmm_app_target); }

/* The pump publishes lane->deadline_us = min(session deadlines, app deadline).
 * With no connections the session side is UINT64_MAX, so the published deadline
 * is exactly the app value when armed and UINT64_MAX when disarmed. */
static void test_app_deadline_fold(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a;
    acnt_init(&a);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = pump;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    cfg.app_deadline_us = wmm_app_deadline;
    cfg.app_deadline_ctx = NULL;

    atomic_store(&g_wmm_app_target, UINT64_MAX);
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        /* disarmed: no session, no app deadline -> UINT64_MAX */
        moq_wtquic_msquic_managed_test_pump_lane(m, 0);
        CHECK(moq_wtquic_msquic_managed_test_lane_deadline_us(m, 0) == UINT64_MAX);
        /* armed with a distinctive finite deadline: the fold must publish it */
        const uint64_t V = ((uint64_t)1 << 60);   /* finite, far future */
        atomic_store(&g_wmm_app_target, V);
        moq_wtquic_msquic_managed_test_pump_lane(m, 0);
        CHECK(moq_wtquic_msquic_managed_test_lane_deadline_us(m, 0) == V);
        atomic_store(&g_wmm_app_target, UINT64_MAX);   /* disarm before teardown */
        moq_wtquic_msquic_managed_stop(m);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* ===================================================================
 * Independent terminal facts and application acknowledgment
 * =================================================================== */

/* F5 (acknowledgment) is refused until F4 (the terminal event was actually
 * POLLED). Transport terminal and quiescence are not enough: the whole point is
 * that reclamation waits for the application, not for pump timing. */
static void *g_ack_probe_conn;
static moq_result_t g_ack_probe_rc;
static bool g_ack_probe_dup_ok;
static int ack_probe_pump(moq_wtquic_msquic_managed_t *m,
                          moq_wtquic_msquic_managed_lane_t *l, uint64_t t,
                          void *u)
{
    (void)m; (void)t; (void)u;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(l, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(l, c)) {
        if ((void *)c != g_ack_probe_conn) continue;
        g_ack_probe_rc = moq_wtquic_msquic_managed_conn_ack_terminal(c);
        /* a repeat in the SAME valid callback is harmless */
        g_ack_probe_dup_ok =
            (moq_wtquic_msquic_managed_conn_ack_terminal(c) == MOQ_OK);
    }
    return 0;
}

static moq_wtquic_msquic_managed_t *mm_test_server_n(acnt_t *a,
                                                moq_alloc_t *alloc,
                                                uint32_t lanes, uint32_t conns,
                                                moq_wtquic_msquic_lane_pump_fn p)
{
    alloc->ctx = a;
    alloc->alloc = acnt_alloc;
    alloc->realloc = acnt_realloc;
    alloc->free = acnt_free;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "c";
    cfg.key_path = "k";
    cfg.on_lane_pump = p;
    cfg.max_connections = conns;
    cfg.lane_count = lanes;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    return m;
}
static moq_wtquic_msquic_managed_t *mm_test_server(acnt_t *a, moq_alloc_t *alloc,
                                                uint32_t n,
                                                moq_wtquic_msquic_lane_pump_fn p)
{
    return mm_test_server_n(a, alloc, n, n, p);
}

/* Acknowledgment before observation is refused; after observation it is
 * accepted, and a duplicate in the same callback is harmless. */
static void test_ack_requires_observed(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, ack_probe_pump);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        g_ack_probe_conn = c0;

        /* session-backed and fully terminal, but the app has NOT polled the
         * terminal event -- so acknowledgment must be refused */
        mm_test_session_terminal_unobserved(m, c0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        g_ack_probe_rc = MOQ_OK;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(g_ack_probe_rc == MOQ_ERR_WRONG_STATE);      /* refused */
        CHECK(!moq_wtquic_msquic_managed_test_conn_acked(c0));
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0)); /* not reaped */

        /* now the app observes it: acknowledgment is accepted, duplicate is OK */
        mm_test_make_observed(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_conn_terminal_observed(c0));
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(g_ack_probe_rc == MOQ_OK);
        CHECK(g_ack_probe_dup_ok);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0)); /* reaped */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    g_ack_probe_conn = NULL;
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* An application that never polls its events is still presented the child by
 * repeated pumps -- but it has observed nothing and acknowledged nothing, so a
 * session-backed child must not be reclaimed. */
static void test_never_polling_app_does_not_reap(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    /* lr_pump acknowledges, but acknowledgment itself is refused while the
     * terminal is unobserved -- so a never-polling app cannot reap. */
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, lr_pump);
    if (m != NULL) {
        void *c0 = NULL;
        bool lt, tq, sb;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        mm_test_session_terminal_unobserved(m, c0);        /* F1, never polled */
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);  /* F2 */

        for (int i = 0; i < 4; i++)
            CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);

        moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
        CHECK(lt && tq && sb);        /* the OLD three-condition gate is MET */
        CHECK(!moq_wtquic_msquic_managed_test_conn_terminal_observed(c0));
        CHECK(!moq_wtquic_msquic_managed_test_conn_acked(c0));
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0)); /* still held */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* F1 + F4 + F5 without F2 must not unlink or release the child; injecting F2
 * then permits reclamation. */
static void test_no_reap_before_quiesced(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, lr_pump);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        /* session-backed, F1 + F4 */
        mm_test_make_terminal(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0); /* F5 */
        CHECK(moq_wtquic_msquic_managed_test_conn_acked(c0));
        {   /* the predicate branch under test really is the session-backed one */
            bool lt = false, tq = false, sb = false;
            moq_wtquic_msquic_managed_test_conn_gate(c0, &lt, &tq, &sb);
            CHECK(lt && sb && !tq);   /* F1 + session-backed, F2 still false */
        }

        /* F2 is still false: linked, iterable, counted, unreleased */
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(lr_saw(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0));

        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);  /* inject F2 */
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* A held terminal child consumes its capacity slot: accepts are refused while
 * it is unacknowledged, the refusal is attributed, and capacity returns after
 * acknowledgment -- with no timeout involved. */
static void test_unacked_holds_capacity(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, lr_pump);
    if (m != NULL) {
        uint64_t tterm = 0, acked = 0, reaped = 0, refused = 0;
        uint32_t held = 0;
        void *c0 = NULL, *c1 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));

        /* session-backed, terminal + quiesced, but NOT observed -> cannot be
         * acknowledged, so the slot stays held */
        mm_test_session_terminal_unobserved(m, c0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1); /* still counted */

        /* The reserve is full so the accept is refused, and because this child
         * is session-backed and terminal-but-unacknowledged the refusal is
         * ATTRIBUTED to that contractual hold. */
        CHECK(!moq_wtquic_msquic_managed_test_accept(m, &c1));
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(tterm == 1 && held == 1);
        CHECK(refused == 1);
        CHECK(acked == 0 && reaped == 0);

        /* acknowledge -> reclaimed -> capacity available again */
        mm_test_make_observed(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c1));

        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(acked == 1 && reaped == 1);   /* conservation: reaped <= acked */
        CHECK(held == 0);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* Forced stop reclaims a terminal child that was never acknowledged: it is the
 * documented forced path, not the normal one. */
static void test_stop_forces_cleanup_without_ack(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, lr_pump);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        mm_test_session_terminal_unobserved(m, c0);
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c0);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_acked(c0));
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);   /* everything released despite no ack */
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* A pre-session failure (refused/failed before establishment) has no session
 * terminal to enqueue, observe or acknowledge, so it must not enter the
 * session-backed conservation counters. */
/* A server child whose negotiation/setup fails before any MoQ session exists has
 * no terminal event to observe and nothing to acknowledge. It must therefore
 * reclaim on transport terminal + quiescence ALONE -- otherwise repeated setup
 * failures would pin max_connections until facade shutdown. It also stays out of
 * the session-backed conservation counters. */
static void test_pre_session_terminal_reclaims_and_frees_capacity(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    /* one slot, so a leaked pre-session child is immediately visible */
    moq_wtquic_msquic_managed_t *m = mm_test_server_n(&a, &alloc, 1, 1, lr_pump);
    if (m != NULL) {
        uint64_t tterm = 0, acked = 0, reaped = 0, refused = 0;
        uint32_t held = 0;

        for (int round = 0; round < 3; round++) {
            void *c = NULL;
            /* capacity must be available on every round -- that is the point */
            CHECK(moq_wtquic_msquic_managed_test_accept(m, &c));
            CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

            /* a real pre-session terminal: the offered subprotocol is not one
             * we recognise, so establishment fails before a session is built */
            mm_test_terminal_only(m, c);
            moq_wtquic_msquic_managed_test_mark_quiesced(m, c);

            /* it was never observed and never acknowledged */
            CHECK(!moq_wtquic_msquic_managed_test_conn_terminal_observed(c));
            CHECK(!moq_wtquic_msquic_managed_test_conn_acked(c));

            /* one ordinary lane cycle reclaims it */
            CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
            CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c));
            CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);
        }

        /* three admissions through a single slot: capacity was genuinely
         * returned each time, not merely on shutdown */
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused,
                                                          &held);
        CHECK(tterm == 0);   /* never entered the session-backed relation */
        CHECK(acked == 0);
        CHECK(held == 0);
        CHECK(reaped == 3);  /* reclaimed exactly once per round */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    /* every slot/resource released exactly once */
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}


/* Concurrent exercise of the terminal accounting's ownership boundary.
 *
 * SCOPE, stated precisely: the concurrency here is CONCURRENT LANE PUMPS. Every
 * child is admitted up front on one thread; the workers then pump their own
 * lanes simultaneously, so several lanes publish terminal and acknowledgment
 * effects into the facade (under m->mu, in lane -> facade order) at the same
 * time. That is exactly the boundary this change introduced.
 *
 * It deliberately does NOT drive admission concurrently. The white-box accept
 * seam calls mm_accept_prepare directly, bypassing the guard handshake wtquic
 * performs before delivering callbacks for a new child, so racing it against
 * pumps exercises the seam rather than the product. See the plan's open items:
 * whether production admission has the same exposure is a separate audit. */
#define CML_LANES 4
#define CML_PER_LANE 6
struct cml {
    moq_wtquic_msquic_managed_t *m;
    uint32_t lane;
};
/* Every per-child fact is written HERE, inside the lane callback, because the
 * lane guard owns them -- mm_pump_lane relies on exactly that ("only mutated
 * under the lane guard"). */
static int cml_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)t; (void)u;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(l, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(l, c)) {
        bool lt = false, tq = false, sb = false;
        moq_wtquic_msquic_managed_test_conn_gate(c, &lt, &tq, &sb);
        if (!lt) {
            moq_wtquic_msquic_managed_test_mark_logical_terminal(m, c);
            moq_wtquic_msquic_managed_test_mark_terminal_observed(m, c);
            moq_wtquic_msquic_managed_test_mark_quiesced(m, c);
        }
        (void)moq_wtquic_msquic_managed_conn_ack_terminal(c);
    }
    return 0;
}
static void *cml_lane_worker(void *arg)
{
    struct cml *w = arg;
    for (int r = 0; r < CML_PER_LANE + 2; r++)
        (void)moq_wtquic_msquic_managed_test_pump_lane(w->m, w->lane);
    return NULL;
}

static void test_terminal_accounting_concurrent_lanes(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    const uint32_t total = CML_LANES * CML_PER_LANE;
    moq_wtquic_msquic_managed_t *m = mm_test_server_n(&a, &alloc, CML_LANES,
                                                      total, cml_pump);
    if (m != NULL) {
        /* admit and make session-backed on ONE thread, before any pump runs */
        for (uint32_t i = 0; i < total; i++) {
            void *c = NULL;
            CHECK(moq_wtquic_msquic_managed_test_accept(m, &c));
            moq_wtquic_msquic_managed_test_set_conn_established(
                c, (wtq_session_t *)0x1, (void *)0x2);
        }
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == total);

        pthread_t th[CML_LANES];
        struct cml w[CML_LANES];
        for (int i = 0; i < CML_LANES; i++) {
            w[i].m = m; w[i].lane = (uint32_t)i;
            CHECK(pthread_create(&th[i], NULL, cml_lane_worker, &w[i]) == 0);
        }
        for (int i = 0; i < CML_LANES; i++)
            CHECK(pthread_join(th[i], NULL) == 0);

        uint64_t tterm = 0, acked = 0, reaped = 0, refused = 0;
        uint32_t held = 0;
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused,
                                                          &held);
        /* every child went terminal -> observed -> acknowledged -> reaped, and
         * no lane withdrew a hold belonging to another */
        CHECK(tterm == total);
        CHECK(acked == total);
        CHECK(reaped == total);
        CHECK(held == 0);
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 0);

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

/* Acknowledgment may legally precede the transport terminal: a locally closed
 * session enqueues and surfaces its terminal event while the transport is still
 * winding down. The published-hold accounting must survive that order, and a
 * child must never withdraw a hold belonging to another child.
 *
 * Pins  terminal_held_count == #{server children with terminal && !acknowledged}
 * at every observable point. */
static void *g_ord_ack_conn;
static int ord_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m; (void)t; (void)u;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(l, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(l, c))
        if ((void *)c == g_ord_ack_conn)
            (void)moq_wtquic_msquic_managed_conn_ack_terminal(c);
    return 0;
}

static void test_ack_before_transport_terminal(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 2, ord_pump);
    if (m != NULL) {
        uint64_t tterm = 0, acked = 0, reaped = 0, refused = 0;
        uint32_t held = 0;
        wtq_session_t *const ws = (wtq_session_t *)0x1;
        void *const sess = (void *)0x2;
        void *c_ord = NULL, *c_held = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c_ord));  /* lane 0 */
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c_held)); /* lane 1 */
        /* both are SESSION-BACKED server children: only those enter the
         * admission-capacity relation */
        moq_wtquic_msquic_managed_test_set_conn_established(c_ord, ws, sess);
        moq_wtquic_msquic_managed_test_set_conn_established(c_held, ws, sess);

        /* c_held reaches its terminal normally and is never acknowledged: it
         * publishes and keeps exactly one hold for the whole test. */
        moq_wtquic_msquic_managed_test_mark_logical_terminal(m, c_held);
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(held == 1);

        /* c_ord is acknowledged BEFORE its transport terminal arrives. It has
         * no hold to withdraw, and it must not consume c_held's. */
        mm_test_make_observed(m, c_ord);
        g_ord_ack_conn = c_ord;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(moq_wtquic_msquic_managed_test_conn_acked(c_ord));
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(acked == 1);
        CHECK(held == 1);   /* still exactly c_held's hold */

        /* c_ord's transport terminal arrives LAST. It is already acknowledged,
         * so it must publish no hold -- the count stays at c_held's one. */
        moq_wtquic_msquic_managed_test_mark_logical_terminal(m, c_ord);
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(held == 1);
        CHECK(tterm == 2);  /* both reached a session-backed transport terminal */

        /* quiesce c_ord: all four conditions hold, so it reclaims, and c_held's
         * hold is untouched */
        moq_wtquic_msquic_managed_test_mark_quiesced(m, c_ord);
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(!moq_wtquic_msquic_managed_test_conn_in_use(c_ord));
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(reaped == 1);
        CHECK(acked == 1);
        CHECK(held == 1);   /* c_held still holds its own slot */
        CHECK(moq_wtquic_msquic_managed_conn_count(m) == 1);

        /* teardown withdraws the surviving hold rather than leaking it */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_test_terminal_counters(m, &tterm, &acked,
                                                          &reaped, &refused, &held);
        CHECK(held == 0);
        moq_wtquic_msquic_managed_destroy(m);
    }
    g_ord_ack_conn = NULL;
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}


/* Argument and context rejection: NULL is invalid input; the client connection
 * is never reaped per-child so it is a state error; a valid server handle used
 * outside its owning callback is a state error. */
static bool g_arg_ran;
static int arg_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *l, uint64_t t, void *u)
{
    (void)m; (void)t; (void)u;
    CHECK(moq_wtquic_msquic_managed_conn_ack_terminal(NULL) == MOQ_ERR_INVAL);
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(l, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(l, c)) {
        /* not terminal-observed yet: a state error, not an argument error */
        CHECK(moq_wtquic_msquic_managed_conn_ack_terminal(c) ==
              MOQ_ERR_WRONG_STATE);
        g_arg_ran = true;
    }
    return 0;
}

static void test_ack_argument_and_context_errors(void)
{
    moq_wtquic_msquic_managed_test_no_listener(true);
    acnt_t a; acnt_init(&a);
    moq_alloc_t alloc;
    moq_wtquic_msquic_managed_t *m = mm_test_server(&a, &alloc, 1, arg_pump);
    if (m != NULL) {
        void *c0 = NULL;
        CHECK(moq_wtquic_msquic_managed_test_accept(m, &c0));
        moq_wtquic_msquic_managed_test_set_conn_established(
            c0, (wtq_session_t *)0x1, (void *)0x2);

        /* outside any callback: a valid handle is still a context error */
        CHECK(moq_wtquic_msquic_managed_conn_ack_terminal(c0) ==
              MOQ_ERR_WRONG_STATE);
        /* NULL is an argument error in every context */
        CHECK(moq_wtquic_msquic_managed_conn_ack_terminal(NULL) ==
              MOQ_ERR_INVAL);

        g_arg_ran = false;
        CHECK(moq_wtquic_msquic_managed_test_pump_lane(m, 0) == 0);
        CHECK(g_arg_ran);   /* the in-callback assertions actually ran */

        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }
    CHECK(acnt_live(&a) == 0);
    acnt_destroy(&a);
    moq_wtquic_msquic_managed_test_no_listener(false);
}

int main(void)
{
    /* Route client bring-up through a fake transport by default so the
     * lifecycle tests never touch the network; individual tests may install a
     * more specific connect probe. */
    moq_wtquic_msquic_managed_test_set_transport(trivial_connect, trivial_release);

    test_pthread_init_failures();
    test_owned_copies_deep();
    test_proto_count_overflow();
    test_server_lane_cap();
    test_server_admission();
    test_server_chooser();
    test_server_establish_failure();
    test_server_late_establish_converges();
    test_final_pump_window_and_gate();
    test_same_pass_terminal();
    test_conn_close_deferred();
    test_pump_after_stopped_owned();
    test_live_reap_gates();
    test_live_reap_reuse();
    test_live_reap_same_pass();
    test_ack_requires_observed();
    test_never_polling_app_does_not_reap();
    test_no_reap_before_quiesced();
    test_unacked_holds_capacity();
    test_stop_forces_cleanup_without_ack();
    test_pre_session_terminal_reclaims_and_frees_capacity();
    test_terminal_accounting_concurrent_lanes();
    test_ack_before_transport_terminal();
    test_ack_argument_and_context_errors();
    test_doorbell_wake_delivers();
    test_doorbell_coalesce();
    test_doorbell_cross_lane();
    test_doorbell_two_lane_concurrent();
    test_doorbell_snapshot_stable();
    test_doorbell_check_to_wait();
    test_lane_stats_pump();
    test_lane_stats_classification();
    test_stats_service_passes();
    test_lane_stats_abi();
    test_transport_activity_pumps();
    test_on_activity_enforcement();
    test_activation_no_pump_on_create_fail();
    test_activation_delivers_retained_arm();
    test_doorbell_pump_wakes_wait();
    test_doorbell_deadline_sweep();
    test_deadline_delivery();
    test_app_deadline_fold();
    test_wake_no_preservice();
    test_deadline_reap_no_use_after_publish();
    test_doorbell_wake_vs_stop();
    test_negotiation_matrix();
    test_alpn_error_on_negotiation_failure();
    test_alpn_not_used_for_construction_failure();
    test_first_fatal_cause();
    test_guard_held_publication();
    test_draining_null_callback();
    test_env_cfg_forwarding();
    test_create_unwind();
    test_connect_result_mapping();
    test_wait_state_machine();
    test_wt_profile_abi_forwarding();

    if (g_fail != 0) {
        fprintf(stderr, "FAILED: test_wtquic_msquic_managed_internal (%d)\n",
                g_fail);
        return 1;
    }
    printf("PASS: test_wtquic_msquic_managed_internal\n");
    return 0;
}
