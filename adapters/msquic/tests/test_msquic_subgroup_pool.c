/*
 * Managed-config max_open_subgroups reaches the LIVE session.
 *
 * Parse/assign/echo tests elsewhere prove the value lands on the managed
 * cfg; this pin proves the forwarding into moq_session_cfg_t changes real
 * session behavior. Two sequential loopback arms, one client each:
 *
 *   constrained (max_open_subgroups = 1): after accept, the server opens
 *     subgroup A, then — with A still open and the action queue NOT full —
 *     a second open returns MOQ_ERR_WOULD_BLOCK. Positive action capacity
 *     at the refusal discriminates the subgroup pool from the queue (the
 *     pool check precedes the queue check in the session).
 *
 *   prefix-sized cfg (excludes the appended field): the same second open
 *     SUCCEEDS — an old caller keeps the session default pool.
 *
 * Neutering the adapter's forwarding assignment must fail the constrained
 * arm. Confinement: every moq_session_* call lives inside on_lane_pump.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdlib.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <moq/msquic_managed.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* --- server observation (written on the lane pump, read after stop) --- */

struct pool_obs {
    moq_subscription_t sub;
    bool have_sub;
    bool accepted;
    bool first_open;             /* subgroup A is open                    */
    moq_subgroup_handle_t sg_a;
    atomic_int probed;           /* the second open was attempted         */
    moq_result_t second_rc;      /* its result                            */
    size_t cap_at_probe;         /* action capacity at that attempt       */
    moq_subgroup_handle_t sg_b;  /* valid when second_rc == 0             */
};

static int pool_server_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane,
                            uint64_t now, void *user)
{
    struct pool_obs *po = user;

    (void)m;
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;

        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !po->have_sub) {
                po->sub = ev.u.subscribe_request.sub;
                po->have_sub = true;
            }
            moq_event_cleanup(&ev);
        }
        if (!po->have_sub || atomic_load(&po->probed))
            continue;
        if (!po->accepted) {
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(s, po->sub, &ac, now) < 0)
                continue; /* retry a later pump */
            po->accepted = true;
        }
        if (!po->first_open) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.subgroup_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, po->sub, &sgc, now,
                                          &po->sg_a) < 0)
                continue; /* retry a later pump */
            po->first_open = true;
        }
        /* Probe only when the action queue demonstrably has room, so a
         * WOULD_BLOCK can only mean the subgroup pool. */
        size_t cap = moq_session_action_capacity(s);
        if (cap == 0)
            continue; /* drain first; probe a later pump */
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 1;
        sgc.publisher_priority = 200;
        po->cap_at_probe = cap;
        po->second_rc = moq_session_open_subgroup(s, po->sub, &sgc, now,
                                                  &po->sg_b);
        atomic_store(&po->probed, 1);
        /* Clean close: the probe is recorded; release what opened. */
        if (po->second_rc == 0)
            (void)moq_session_close_subgroup(s, po->sg_b, now);
        (void)moq_session_close_subgroup(s, po->sg_a, now);
        (void)moq_session_close(s, 0, NULL, now);
    }
    return 0;
}

/* --- client: subscribe once setup completes -------------------------- */

struct pool_client {
    bool subscribed;
    int errors;
};

static int pool_client_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane,
                            uint64_t now, void *user)
{
    struct pool_client *cl = user;
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)lane;
    if (s == NULL)
        return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !cl->subscribed) {
            static const moq_bytes_t ns[] = {
                { (const uint8_t *)"pool", 4 },
            };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;

            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            cl->subscribed = true;
            if (moq_session_subscribe(s, &sc, now, &sub) < 0)
                cl->errors++;
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

/* One arm: run the flow with the given server session pool configuration.
 * constrained == true  -> full-size cfg, max_open_subgroups = 1
 * constrained == false -> PREFIX-SIZED cfg (excludes the appended field):
 *                         the old-caller shape that must keep the default. */
/* Arm selector. FULL honors the field; EXACT_PREFIX is the old-caller shape
 * whose struct_size stops exactly at the field; PARTIAL_FIELD stops INSIDE it,
 * where copying the low bytes would forward poison as if it were a pool size.
 * Both prefix arms explicitly poison the excluded tail rather than relying on
 * uninitialized stack bytes, so the gate is proven, not assumed. */
enum pool_arm { ARM_FULL, ARM_EXACT_PREFIX, ARM_PARTIAL_FIELD };

static void run_arm_sized(const char *cert, const char *key, enum pool_arm arm)
{
    const bool constrained = (arm == ARM_FULL);
    int before = failures;
    struct pool_obs po;
    memset(&po, 0, sizeof(po));

    moq_msquic_managed_cfg_t cfg;
    size_t init_size;
    switch (arm) {
    case ARM_FULL:
        init_size = sizeof(cfg);
        break;
    case ARM_EXACT_PREFIX:
        init_size = offsetof(moq_msquic_managed_cfg_t, max_open_subgroups);
        break;
    default:   /* ARM_PARTIAL_FIELD: land INSIDE the appended field */
        init_size = offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) + 1;
        break;
    }
    /* Poison the WHOLE struct first: the excluded tail then carries a
     * deterministic nonzero pattern instead of whatever the stack held, so a
     * missing gate forwards a visible bogus pool rather than an accidental 0. */
    memset(&cfg, 0xA5, sizeof(cfg));
    moq_msquic_managed_cfg_init_sized(&cfg, init_size);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    if (constrained)
        cfg.max_open_subgroups = 1;
    if (arm == ARM_PARTIAL_FIELD) {
        /* struct_size stops INSIDE the field while its bytes hold a real
         * value: without a full-field gate the truncated copy would take the
         * covered low byte(s) and constrain the pool from a value the caller
         * never fully declared. The value is chosen so its LOW byte is the
         * constraining one on either endianness. */
        cfg.max_open_subgroups = 0x01010101u;
    }
    /* The prefix arms deliberately leave the poisoned tail in place. */
    cfg.on_lane_pump = pool_server_pump;
    cfg.on_lane_pump_user = &po;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);

    struct pool_client cl;
    memset(&cl, 0, sizeof(cl));
    moq_msquic_managed_t *cli = NULL;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.on_lane_pump = pool_client_pump;
    cfg.on_lane_pump_user = &cl;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    for (int iter = 0; iter < 4000 && !atomic_load(&po.probed); iter++) {
        (void)moq_msquic_managed_wake(srv);
        (void)moq_msquic_managed_wake(cli);
        struct timespec ts = { 0, 3 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    if (cli != NULL) {
        (void)moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    CHECK(atomic_load(&po.probed));      /* the flow reached the probe    */
    CHECK(po.cap_at_probe > 0);          /* queue had room: pool verdicts */
    CHECK(cl.errors == 0);
    if (constrained) {
        /* pool 1: subgroup A holds the only slot — B refuses */
        CHECK(po.second_rc == MOQ_ERR_WOULD_BLOCK);
    } else {
        /* Both prefix arms keep the session default and B opens. For
         * ARM_PARTIAL_FIELD this is the load-bearing one: struct_size lands
         * INSIDE the field while its bytes hold 0x01010101, so honoring the
         * covered low byte(s) would constrain the pool and refuse B. */
        CHECK(po.second_rc == 0);
    }

    if (failures == before)
        printf("PASS: subgroup_pool_%s\n",
               arm == ARM_FULL ? "constrained"
                               : (arm == ARM_EXACT_PREFIX ? "prefix_default"
                                                          : "partial_field"));
}

/* A counting allocator: an invalid pool size must be rejected BEFORE any
 * allocation or transport setup happens, on both perspectives. */
static int inval_allocs;
static void *inval_alloc(size_t n, void *ctx)
{ (void)ctx; inval_allocs++; return malloc(n); }
static void *inval_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)ctx; (void)o; inval_allocs++; return realloc(p, n); }
static void inval_free(void *p, size_t n, void *ctx)
{ (void)ctx; (void)n; free(p); }

/* The validation/non-vacuity arms need a pump that is safe with NO context:
 * the legal control really does create a managed connection, so its lane
 * callback can run concurrently with stop/destroy. This no-op pump touches
 * nothing, and records if it were ever handed the observer context it must
 * not receive -- so restoring the observer pump (which dereferences `user`
 * immediately) is caught deterministically instead of by scheduler luck. */
static atomic_int noop_pump_ran;
static atomic_int noop_pump_bad_ctx;
static int pool_noop_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane,
                          uint64_t now, void *user)
{
    (void)m; (void)lane; (void)now;
    atomic_fetch_add(&noop_pump_ran, 1);
    if (user != NULL)
        atomic_fetch_add(&noop_pump_bad_ctx, 1);
    return 0;
}

static void run_inval_arm(const char *cert, const char *key, bool server)
{
    int before = failures;
    moq_alloc_t alloc;
    memset(&alloc, 0, sizeof(alloc));
    alloc.alloc = inval_alloc;
    alloc.realloc = inval_realloc;
    alloc.free = inval_free;
    inval_allocs = 0;

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = server ? 0 : 4433;
    if (server) { cfg.cert_path = cert; cfg.key_path = key; }
    else        { cfg.insecure_skip_verify = true; }
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    /* A no-op pump with an explicitly NULL context: the cfg must be invalid
     * ONLY because of the pool size (a missing pump would reject it for an
     * unrelated reason and prove nothing). */
    cfg.on_lane_pump = pool_noop_pump;
    cfg.on_lane_pump_user = NULL;
    cfg.max_open_subgroups = 0x10000;   /* one past the core's limit */

    moq_msquic_managed_t *m = (moq_msquic_managed_t *)(uintptr_t)0x1;
    moq_result_t rc = moq_msquic_managed_create(&cfg, &m);
    CHECK(rc == MOQ_ERR_INVAL);   /* the core's classification, not INTERNAL */
    CHECK(m == NULL);             /* no handle handed back                  */
    CHECK(inval_allocs == 0);     /* rejected before any allocation         */

    /* Non-vacuity: the identical config with a LEGAL pool size is accepted,
     * so the rejection above is attributable to the pool size alone. */
    inval_allocs = 0;
    /* Per-arm isolation: the invalid create started no manager, and the
     * previous arm already stopped and destroyed its own, so both counters
     * belong solely to the legal manager created below. Without this reset a
     * later arm could satisfy itself with an earlier arm's callback. */
    atomic_store(&noop_pump_ran, 0);
    atomic_store(&noop_pump_bad_ctx, 0);
    cfg.max_open_subgroups = 4;
    moq_msquic_managed_t *ok = NULL;
    moq_result_t okrc = moq_msquic_managed_create(&cfg, &ok);
    CHECK(okrc == MOQ_OK);
    CHECK(ok != NULL);
    CHECK(inval_allocs > 0);
    if (ok) {
        /* Let the lane actually run before teardown, so a context-unsafe
         * pump would be exercised rather than skipped. */
        for (int i = 0; i < 200; i++) {
            (void)moq_msquic_managed_wake(ok);
            struct timespec ts = { 0, 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
            if (atomic_load(&noop_pump_ran) > 0) break;
        }
        moq_msquic_managed_stop(ok);
        moq_msquic_managed_destroy(ok);
    }
    /* THIS arm's callback really ran -- otherwise the context assertion below
     * would be vacuous, and the claim that the unsafe pump is exercised
     * rather than skipped would be unproven. */
    CHECK(atomic_load(&noop_pump_ran) > 0);
    /* The pump must never have been handed a context it would dereference. */
    CHECK(atomic_load(&noop_pump_bad_ctx) == 0);

    if (failures == before)
        printf("PASS: subgroup_pool_inval_%s\n", server ? "server" : "client");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    run_arm_sized(argv[1], argv[2], ARM_FULL);
    run_arm_sized(argv[1], argv[2], ARM_EXACT_PREFIX);
    run_arm_sized(argv[1], argv[2], ARM_PARTIAL_FIELD);
    run_inval_arm(argv[1], argv[2], false);   /* client */
    run_inval_arm(argv[1], argv[2], true);    /* server */
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
