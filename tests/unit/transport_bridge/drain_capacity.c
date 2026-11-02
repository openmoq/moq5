#include "test_cases.h"

int test_hol_drain_budget_is_a_real_backstop(void)
{
    int failures = 0;
    unsigned f64 = 0, o64 = 0, f256 = 0, o256 = 0, f512 = 0, o512 = 0;

    (void)hol_budget_case(64,  &f64,  &o64,  &failures);
    (void)hol_budget_case(256, &f256, &o256, &failures);
    (void)hol_budget_case(512, &f512, &o512, &failures);

    /* Each SEND_DATA action costs two endpoint writes (header, then payload),
     * so the writes-per-action factor is derived from the below-budget case
     * rather than assumed. */
    if (o64 == 0 || f64 == 0 || (f64 % o64) != 0) {
        fprintf(stderr, "BUDGET cap=64: cannot derive writes-per-action from "
                "%u write(s) for %u object(s)\n", f64, o64);
        return failures + 1;
    }
    {
        unsigned per = f64 / o64;
        unsigned want = (unsigned)BRIDGE_DRAIN_BUDGET * per;
        printf("BUDGET writes-per-action=%u; budget in writes=%u\n", per, want);

        /* Below the budget the SESSION cap binds: one pass takes everything. */
        if (f64 != o64 * per) {
            fprintf(stderr, "BUDGET cap=64: one pass took %u write(s) of %u; "
                    "the session cap should bind below the budget\n",
                    f64, o64 * per);
            failures++;
        }
        /* Above it the BRIDGE budget binds: exactly the budget in the first
         * pass, the rest in a later bounded pass. This is the oracle the budget
         * mutant must break. */
        if (o512 <= (unsigned)BRIDGE_DRAIN_BUDGET) {
            fprintf(stderr, "BUDGET cap=512: only %u action(s) offered, not "
                    "more than the budget %d\n", o512,
                    (int)BRIDGE_DRAIN_BUDGET);
            failures++;
        } else if (f512 != want) {
            fprintf(stderr, "BUDGET cap=512: first pass did %u write(s), "
                    "expected exactly the budget's %u (%d actions x %u)\n",
                    f512, want, (int)BRIDGE_DRAIN_BUDGET, per);
            failures++;
        }
        /* at exactly the budget, one pass takes it all and nothing is left */
        if (o256 == (unsigned)BRIDGE_DRAIN_BUDGET && f256 != o256 * per) {
            fprintf(stderr, "BUDGET cap=256: first pass did %u write(s), "
                    "expected %u\n", f256, o256 * per);
            failures++;
        }
    }
    return failures;
}

#ifdef MOQ_BRIDGE_TEST_INJECT


/*
 * A failed retention releases the item's owned payload exactly once, on both
 * the initial-retain and retry-retain paths.
 *
 * The fixture keeps one reference to the buffer and gives the action another.
 * Cleanup must return the count to one while the fixture's reference keeps
 * the buffer valid for inspection.
 */
int test_hol_retention_releases_owned_payload_once(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    for (int phase = 0; phase < 2; phase++) {
        hol_fix_t f;
        moq_subgroup_handle_t sg;
        moq_rcbuf_t *held = NULL;
        moq_action_t action = {0};
        moq_bridge_test_clear_retain_failure();
        g_server_alloc_override = &al;
        if (hol_init(&f) < 0) { g_server_alloc_override = NULL; return 1; }
        g_server_alloc_override = NULL;
        held_bridge_destroy(f.tp.server_bridge);
        moq_transport_bridge_cfg_t cfg;
        moq_transport_bridge_cfg_init(&cfg, &al);
        HELD_REQUIRE(held_bridge_create(&cfg, f.tp.server,
            &f.tp.server_ep.vtable, &f.tp.server_ep, &f.tp.server_bridge) == MOQ_OK);
        MOQ_TEST_CHECK(hol_open_sg(&f, 0, &sg) == MOQ_OK);
        MOQ_TEST_CHECK(hol_write(&f, sg, 0, 32) == MOQ_OK);
        hol_service(&f, 4);
        MOQ_TEST_CHECK(hol_pending(&f) == 0);
        const uint8_t bytes[96] = {0};
        moq_result_t rc = moq_rcbuf_create(&al, bytes, sizeof(bytes), &held);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (held) {
            action.kind = MOQ_ACTION_SEND_DATA;
            action.detail_size = sizeof(action.u.send_data);
            action.u.send_data.stream_ref = moq_transport_bridge_find_ref(
                f.tp.server_bridge, hol_nth_open(&f, 0));
            action.u.send_data.payload = held;
            moq_rcbuf_incref(held);    /* fixture ref plus the action's owned ref */
            MOQ_TEST_CHECK(action.u.send_data.stream_ref._v != 0);
            f.tp.server_ep.block_write = true;
            if (phase == 1) {
                /* Queue one owned action at the actual FIFO boundary. The
                 * ordinary mapped-data pump uses a reserved head instead. */
                MOQ_TEST_CHECK(moq_bridge_test_retain_data_action(
                    f.tp.server_bridge, &action) == MOQ_OK);
                MOQ_TEST_CHECK(hol_pending(&f) == 1);
                MOQ_TEST_CHECK(action.u.send_data.payload == NULL);
            }
            uint32_t before = moq_rcbuf_refcount(held);
            MOQ_TEST_CHECK(before == 2);
            moq_bridge_test_fail_retain_after(0);
            rc = phase == 0
                ? moq_bridge_test_retain_data_action(f.tp.server_bridge, &action)
                : held_bridge_service(f.tp.server_bridge, 0);
            MOQ_TEST_CHECK(rc == MOQ_ERR_INTERNAL);
            MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(f.tp.server_bridge));
            MOQ_TEST_CHECK(f.tp.server_bridge->fatal_code == 0x5);
            MOQ_TEST_CHECK(moq_rcbuf_refcount(held) == before - 1);
            MOQ_TEST_CHECK(action.u.send_data.payload == NULL);
        }
        moq_bridge_test_clear_retain_failure();
        moq_action_cleanup(&action);
        moq_rcbuf_decref(held);
        test_pair_destroy(&f.tp);
        MOQ_TEST_CHECK(acct.live_blocks == 0);
        MOQ_TEST_CHECK(acct.live_bytes == 0);
        MOQ_TEST_CHECK(!acct.negative);
    }
    return failures;
}

/*
 * The first retention of a newly polled action follows the same policy. The
 * drain-capacity guard makes capacity failure here unreachable, so the branch
 * is reached by fault injection -- but an identity or fault failure must still
 * stop the pass rather than clean the action and report it delivered.
 */
int test_hol_initial_retention_failure_is_fatal_not_silent(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t writes_after_breach, opens_after_breach;
    moq_result_t rc;

    memset(&f, 0, sizeof(f));
    /* the server session AND its bridge on the counting allocator, so a
     * retained action that is never released shows up as live memory */
    g_server_alloc_override = &al;
    if (hol_init(&f) < 0) { g_server_alloc_override = NULL; return 1; }
    g_server_alloc_override = NULL;
    held_bridge_destroy(f.tp.server_bridge);
    {
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, &al);
        HELD_REQUIRE(held_bridge_create(&bcfg, f.tp.server,
                           &f.tp.server_ep.vtable, &f.tp.server_ep,
                           &f.tp.server_bridge) == MOQ_OK);
    }

    /* block the OPEN, so the retention that fails is a first-enqueue site
     * reached only by a newly polled action, not the shared retry path */
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 96) == MOQ_OK);

    /* fail the very first retention of that action */
    moq_bridge_test_fail_retain_after(0);
    rc = held_bridge_service(f.tp.server_bridge, 0);

    if (rc == MOQ_OK) {
        fprintf(stderr, "RETAIN0: an initial retention failure was reported "
                "as success\n");
        failures++;
    }
    if (!moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
        fprintf(stderr, "RETAIN0: an initial retention failure left the "
                "bridge usable\n");
        failures++;
    }
    if (f.tp.server_bridge->fatal_code != 0x5) {
        fprintf(stderr, "RETAIN0: fatal code was %llu, not the retention "
                "code\n",
                (unsigned long long)f.tp.server_bridge->fatal_code);
        failures++;
    }
    /* the action was not left queued as though it had been retained */
    if (hol_pending(&f) != 0) {
        fprintf(stderr, "RETAIN0: %u item(s) queued after a failed "
                "retention\n", hol_pending(&f));
        failures++;
    }

    /* the endpoint counts an ACCEPTED open, so unblock first and require that
     * the unblocked pass still delivers nothing */
    writes_after_breach = f.tp.server_ep.write_calls;
    opens_after_breach = f.tp.server_ep.open_uni_calls;
    f.tp.server_ep.block_open_uni = false;
    (void)held_bridge_service(f.tp.server_bridge, 0);
    if (f.tp.server_ep.write_calls != writes_after_breach ||
        f.tp.server_ep.open_uni_calls != opens_after_breach) {
        fprintf(stderr, "RETAIN0: %llu write(s) and %llu open(s) after an "
                "invariant breach\n",
                (unsigned long long)(f.tp.server_ep.write_calls -
                                     writes_after_breach),
                (unsigned long long)(f.tp.server_ep.open_uni_calls -
                                     opens_after_breach));
        failures++;
    }

    held_bridge_destroy(f.tp.server_bridge);
    f.tp.server_bridge = NULL;
    /* Bridges first: each session outlives its bridge. */
    held_bridge_destroy(f.tp.client_bridge);
    f.tp.client_bridge = NULL;
    moq_session_destroy(f.tp.client);
    moq_session_destroy(f.tp.server);

    if (acct.live_blocks != 0 || acct.live_bytes != 0 || acct.negative) {
        fprintf(stderr, "RETAIN0: live=%lld blocks %lld bytes negative=%d\n",
                (long long)acct.live_blocks, (long long)acct.live_bytes,
                (int)acct.negative);
        failures++;
    }
    return failures;
}


/*
 * Re-retaining an item the bridge already owns cannot fail by capacity, so
 * the branch is reachable only by fault injection. It must still be an
 * invariant breach rather than a quiet drop: the pass stops, reports the
 * error, marks the bridge fatal, and releases the item exactly once.
 */
int test_hol_retention_failure_is_fatal_not_silent(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t writes_before;
    moq_result_t rc;
    int i;

    memset(&f, 0, sizeof(f));
    /* the server session AND its bridge on the counting allocator, so a
     * retained action that is never released shows up as live memory */
    g_server_alloc_override = &al;
    if (hol_init(&f) < 0) { g_server_alloc_override = NULL; return 1; }
    g_server_alloc_override = NULL;
    held_bridge_destroy(f.tp.server_bridge);
    {
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, &al);
        HELD_REQUIRE(held_bridge_create(&bcfg, f.tp.server,
                           &f.tp.server_ep.vtable, &f.tp.server_ep,
                           &f.tp.server_bridge) == MOQ_OK);
    }

    moq_bridge_test_clear_retain_failure();
    /* Data stays in its reserved stream queue. Two blocked FIFO control
     * items exercise the first-survivor/second-failure retry boundary. */
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    for (i = 0; i < 4; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, (uint64_t)i, 96) == MOQ_OK);
    f.tp.server_ep.block_write = true;
    hol_service(&f, 6);
    f.tp.server_ep.block_stop = true;
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
        PENDING_STOP_SENDING, 0, 70000));
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
        PENDING_STOP_SENDING, 0, 70001));
    MOQ_TEST_CHECK(hol_pending(&f) == 2);

    /* let the blocked head re-retain, then fail the next survivor */
    moq_bridge_test_fail_retain_after(1);
    rc = held_bridge_service(f.tp.server_bridge, 0);

    if (rc == MOQ_OK) {
        fprintf(stderr, "RETAIN: a retention failure was reported as "
                "success\n");
        failures++;
    }
    if (!moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
        fprintf(stderr, "RETAIN: a retention failure left the bridge "
                "usable\n");
        failures++;
    }
    if (f.tp.server_bridge->fatal_code != 0x5) {
        fprintf(stderr, "RETAIN: fatal code was %llu, not the retention "
                "code\n",
                (unsigned long long)f.tp.server_bridge->fatal_code);
        failures++;
    }

    /* No endpoint operation after the error, even when data becomes writable. */
    writes_before = f.tp.server_ep.write_calls;
    moq_bridge_test_clear_retain_failure();
    f.tp.server_ep.block_write = false;
    (void)held_bridge_service(f.tp.server_bridge, 0);
    if (f.tp.server_ep.write_calls != writes_before) {
        fprintf(stderr, "RETAIN: %llu write(s) were delivered after an "
                "invariant breach\n",
                (unsigned long long)(f.tp.server_ep.write_calls -
                                     writes_before));
        failures++;
    }

    held_bridge_destroy(f.tp.server_bridge);
    f.tp.server_bridge = NULL;
    /* Bridges first: each session outlives its bridge. */
    held_bridge_destroy(f.tp.client_bridge);
    f.tp.client_bridge = NULL;
    moq_session_destroy(f.tp.client);
    moq_session_destroy(f.tp.server);

    /* released exactly once: no leak, and no negative balance */
    if (acct.live_blocks != 0 || acct.live_bytes != 0 || acct.negative) {
        fprintf(stderr, "RETAIN: live=%lld blocks %lld bytes negative=%d\n",
                (long long)acct.live_blocks, (long long)acct.live_bytes,
                (int)acct.negative);
        failures++;
    }
    return failures;
}

#endif /* MOQ_BRIDGE_TEST_INJECT */



int test_hol_owned_payload_conservation(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    hol_fix_t f;
    moq_subgroup_handle_t a;
    int pass;

    /* the fixture's sessions use the default allocator, but the BRIDGE is
     * where retained items live -- give it the counting one */
    memset(&f, 0, sizeof(f));
    if (hol_init(&f) < 0) return 1;
    held_bridge_destroy(f.tp.server_bridge);
    {
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, &al);
        HELD_REQUIRE(held_bridge_create(&bcfg, f.tp.server,
                           &f.tp.server_ep.vtable, &f.tp.server_ep,
                           &f.tp.server_bridge) == MOQ_OK);
    }

    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    /* a real payload, so the retained item owns heap data */
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 96) == MOQ_OK);
    f.tp.server_ep.block_write = true;
    hol_service(&f, 2);
    MOQ_TEST_CHECK(hol_retained(&f) >= 1);
    /* many re-enqueues of the SAME owning item */
    for (pass = 0; pass < 10; pass++)
        (void)held_bridge_service(f.tp.server_bridge, 0);
    /* then let it out, so the item is cleaned exactly once on success */
    f.tp.server_ep.block_write = false;
    hol_service(&f, 16);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);

    /* and a second owning item destroyed while still retained */
    MOQ_TEST_CHECK(hol_write(&f, a, 1, 96) == MOQ_OK);
    f.tp.server_ep.block_write = true;
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_retained(&f) >= 1);

    held_bridge_destroy(f.tp.server_bridge);
    f.tp.server_bridge = NULL;
    /* Bridges first: each session outlives its bridge. */
    held_bridge_destroy(f.tp.client_bridge);
    f.tp.client_bridge = NULL;
    moq_session_destroy(f.tp.client);
    moq_session_destroy(f.tp.server);

    if (acct.live_blocks != 0 || acct.live_bytes != 0 || acct.negative) {
        fprintf(stderr, "HOL owned-payload: live=%lld blocks %lld bytes "
                "negative=%d allocs=%lld frees=%lld\n",
                (long long)acct.live_blocks, (long long)acct.live_bytes,
                (int)acct.negative, (long long)acct.allocs,
                (long long)acct.frees);
        failures++;
    }
    MOQ_TEST_CHECK(acct.allocs > 0);
    return failures;
}


/* ============== retention capacity at the drain boundary =================
 * Without the drain loop's `pending_count < max_pending` stop condition, one
 * pass could retain up to BRIDGE_DRAIN_BUDGET actions into a queue that holds
 * max_pending. Because the domain gate guarantees that every
 * later action for a blocked stream is retained rather than dispatched, the
 * queue fills deterministically -- and every enqueue-failure branch takes the
 * connection fatal. So a blocked stream with more queued work than the pending
 * capacity would destroy the connection and lose its objects.
 *
 * The fixture records both public knobs and asserts their coupling, because
 * the defect only exists when max_actions > max_pending.
 */
typedef struct {
    unsigned  offered;
    unsigned  max_depth;
    int       worst_rc;
    bool      fatal;
    uint64_t  writes_after_unblock;
} f1_result_t;


static int f1_case(uint32_t max_actions, uint32_t max_pending,
                   unsigned objects, f1_result_t *out)
{
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t sid_a, base;
    unsigned i;

    memset(out, 0, sizeof(*out));
    out->worst_rc = 0;

    /* the publisher's action queue and the bridge's retention capacity are
     * both public knobs; this is the pairing that matters */
    if (test_pair_init_full(&f.tp, 1, false, 0, 0, max_actions) < 0) return -1;
    if (!setup_handshake(&f.tp)) { test_pair_destroy(&f.tp); return -1; }
    /* rebuild the publisher bridge with an explicit retention capacity */
    held_bridge_destroy(f.tp.server_bridge);
    {
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        bcfg.max_pending = max_pending;
        if (held_bridge_create(&bcfg, f.tp.server,
                &f.tp.server_ep.vtable, &f.tp.server_ep,
                &f.tp.server_bridge) != MOQ_OK) {
            test_pair_destroy(&f.tp);
            return -1;
        }
    }
    if (hol_subscribe_accept(&f) < 0) { test_pair_destroy(&f.tp); return -1; }

    if (hol_open_sg(&f, 0, &a) != MOQ_OK) { test_pair_destroy(&f.tp); return -1; }
    (void)hol_write(&f, a, 0, 16);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    if (sid_a == UINT64_MAX) { test_pair_destroy(&f.tp); return -1; }

    /* ONE stream blocked at the endpoint. Per-stream, so no control item can
     * mask the effect. */
    fake_block_stream(&f.tp.server_ep, sid_a);
    for (i = 1; i <= objects; i++) {
        if (hol_write(&f, a, i, 16) != MOQ_OK) break;
        out->offered++;
    }
    for (i = 0; i < 8; i++) {
        moq_result_t rc = held_bridge_service(f.tp.server_bridge, 0);
        if ((int)rc < out->worst_rc) out->worst_rc = (int)rc;
        if (hol_pending(&f) > out->max_depth) out->max_depth = hol_pending(&f);
        if (moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
            out->fatal = true;
            break;
        }
    }
    /* unblock and see what survives */
    base = f.tp.server_ep.write_calls;
    fake_unblock_stream(&f.tp.server_ep);
    hol_service(&f, 128);
    out->writes_after_unblock = f.tp.server_ep.write_calls - base;
    test_pair_destroy(&f.tp);
    return 0;
}


int test_hol_retention_capacity_is_not_fatal(void)
{
    int failures = 0;
    f1_result_t below, at, above;
    const uint32_t MA = 4096, MP = 64;

    /* the coupling this defect needs */
    MOQ_TEST_CHECK(MA > MP);

    if (f1_case(MA, MP, 60, &below) < 0) return 1;
    if (f1_case(MA, MP, 64, &at) < 0) return 1;
    if (f1_case(MA, MP, 70, &above) < 0) return 1;

    printf("CAP max_actions=%u max_pending=%u\n", MA, MP);
    printf("CAP objects=60 depth=%u rc=%d fatal=%d writes_after=%llu\n",
           below.max_depth, below.worst_rc, (int)below.fatal,
           (unsigned long long)below.writes_after_unblock);
    printf("CAP objects=64 depth=%u rc=%d fatal=%d writes_after=%llu\n",
           at.max_depth, at.worst_rc, (int)at.fatal,
           (unsigned long long)at.writes_after_unblock);
    printf("CAP objects=70 depth=%u rc=%d fatal=%d writes_after=%llu\n",
           above.max_depth, above.worst_rc, (int)above.fatal,
           (unsigned long long)above.writes_after_unblock);

    /* THE CONTRACT: exceeding retention capacity is backpressure, never a
     * connection kill, and never data loss. */
    if (above.fatal || above.worst_rc < 0) {
        fprintf(stderr, "CAP: %u objects on one blocked stream took the "
                "connection FATAL (rc=%d)\n", above.offered, above.worst_rc);
        failures++;
    }
    /* it must saturate at the capacity, not beyond */
    if (above.max_depth > MP) {
        fprintf(stderr, "CAP: pending reached %u, past the capacity %u\n",
                above.max_depth, MP);
        failures++;
    }
    /* and every offered object must come out after the block lifts */
    if (!above.fatal && above.writes_after_unblock < (uint64_t)above.offered) {
        fprintf(stderr, "CAP: only %llu write(s) after unblock for %u offered "
                "objects: queued work was lost\n",
                (unsigned long long)above.writes_after_unblock, above.offered);
        failures++;
    }
    /* below and at capacity must be unaffected */
    MOQ_TEST_CHECK(!below.fatal);
    MOQ_TEST_CHECK(!at.fatal);
    return failures;
}

#ifdef MOQ_BRIDGE_TEST_INJECT


/* An item that names only a REF, for a stream the map still
 * knows, must have its transport id filled at enqueue -- so a later action
 * naming only that ID conflicts with it. */
int test_hol_canon_fills_sid_from_ref(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t sid_a, cref = 0, csid = 0, live_ref = 0, dummy = 0;
    bool chave = false, have2 = false;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);

    /* learn the live stream's ref by injecting an id-only probe, reading its
     * filled ref, then starting clean */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, sid_a, true));
    moq_bridge_test_pending_identity(f.tp.server_bridge, 0, &live_ref,
                                     &dummy, &have2);
    test_pair_destroy(&f.tp);
    if (live_ref == 0) {
        fprintf(stderr, "CANON: could not learn the live ref for id %llu\n",
                (unsigned long long)sid_a);
        return failures + 1;
    }

    if (hol_init(&f) < 0) return failures + 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);

    /* the ref-only item: enqueue must fill in the transport id */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_HEADER_PAYLOAD, live_ref, 0, false));
    moq_bridge_test_pending_identity(f.tp.server_bridge, 0, &cref, &csid,
                                     &chave);
    if (!chave || csid != sid_a) {
        fprintf(stderr, "CANON: a ref-only item was not given its transport id "
                "(have=%d id=%llu, expected %llu)\n", (int)chave,
                (unsigned long long)csid, (unsigned long long)sid_a);
        failures++;
    }
    /* and an ID-only action for the same stream must therefore conflict */
    if (!moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, sid_a, true)) {
        fprintf(stderr, "CANON: an id-named action bypassed a ref-named "
                "pending item for the same stream\n");
        failures++;
    }
    MOQ_TEST_CHECK(!moq_bridge_test_domain_blocked(f.tp.server_bridge,
                                                    0, sid_a + 991, true));
    test_pair_destroy(&f.tp);
    return failures;
}


/* A candidate that names neither a ref nor a transport id cannot be
 * resolved. It must be refused outright rather than compare equal to nothing
 * and bypass the queue, and the refusal must be visible as a fatal bridge. */
int test_hol_identityless_candidate_fails_closed(void)
{
    int failures = 0;
    hol_fix_t f;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, 0, true));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.server_bridge));

    if (!moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 0, false)) {
        fprintf(stderr, "SHAPE: an identity-less candidate was admitted\n");
        failures++;
    }
    if (!moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
        fprintf(stderr, "SHAPE: an identity-less candidate did not fail "
                "closed\n");
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}


/*
 * Pending identity is completed exactly once, at the enqueue boundary, and a
 * candidate's identity is never completed from the stream map.
 *
 * Completing a candidate would only ever matter for a pending item that is
 * named by one identity while its stream is mapped under the other. No such
 * item can exist: the enqueue boundary fills the missing name for every item
 * whose counterpart is already mapped, and an item injected before the
 * mapping exists does not survive the service passes that create it. This
 * pins the second half of that argument -- the first half is pinned by the
 * mixed-identity case, where enqueue resolves an id-only item's ref.
 */
int test_hol_pending_identity_completed_only_at_enqueue(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;
    uint64_t sid_a, sid_b, ref_b = 0, cref = 0, csid = 0;
    bool chave = false;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);

    /* named by an id the map does not know, so enqueue cannot complete it */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, sid_a + 1, true));
    moq_bridge_test_pending_identity(f.tp.server_bridge, 0, &cref, &csid,
                                     &chave);
    if (cref != 0) {
        fprintf(stderr, "SHAPE: an unmapped id was completed to ref %llu\n",
                (unsigned long long)cref);
        failures++;
    }
    MOQ_TEST_CHECK(chave && csid == sid_a + 1);

    /* that id now becomes a live stream with a ref of its own */
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_b = hol_nth_open(&f, 1);
    MOQ_TEST_CHECK(sid_b == sid_a + 1);
    ref_b = moq_bridge_test_ref_for_id(f.tp.server_bridge, sid_b);
    MOQ_TEST_CHECK(ref_b != 0);

    /* the half-named item did not outlive the mapping it was missing */
    if (moq_bridge_test_pending_depth(f.tp.server_bridge) != 0) {
        fprintf(stderr, "SHAPE: a half-named item survived to see its stream "
                "mapped (id=%llu ref=%llu)\n", (unsigned long long)sid_b,
                (unsigned long long)ref_b);
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}


/* Transport stream id 0 is LEGAL. It must be a real identity that
 * conflicts with itself, must NOT alias "unknown", and must not block an
 * unrelated stream. */
int test_hol_stream_id_zero_is_a_real_identity(void)
{
    int failures = 0;
    hol_fix_t f;

    if (hol_init(&f) < 0) return 1;
    /* a pending item on the legal stream id 0 */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, 0, true));
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == 1);
    {
        uint64_t cref = 0, csid = 0xdead;
        bool chave = false;
        moq_bridge_test_pending_identity(f.tp.server_bridge, 0,
                                         &cref, &csid, &chave);
        /* the id is KNOWN and it is zero */
        if (!chave || csid != 0) {
            fprintf(stderr, "ID0: a legal id 0 was recorded as have=%d id=%llu; "
                    "it aliased 'unknown'\n", (int)chave,
                    (unsigned long long)csid);
            failures++;
        }
    }
    /* it conflicts with itself, by id */
    if (!moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 0, true)) {
        fprintf(stderr, "ID0: stream id 0 did not conflict with itself\n");
        failures++;
    }
    /* and an unrelated stream is free */
    if (moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 77, true)) {
        fprintf(stderr, "ID0: stream id 0 blocked unrelated stream 77\n");
        failures++;
    }
    test_pair_destroy(&f.tp);

    /* the mirror: an item with an UNKNOWN id must not conflict with a
     * candidate naming the legal id 0 */
    if (hol_init(&f) < 0) return failures + 1;
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending_ex(f.tp.server_bridge,
                       PENDING_OPEN_UNI_DATA, 0x4242, 0, false));
    if (moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 0, true)) {
        fprintf(stderr, "ID0: an unknown-id item conflicted with the legal "
                "stream id 0\n");
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}

#endif


/* The soundness precondition for that capacity guard: after it, a drain-side
 * enqueue failure is structurally unreachable, because one polled action
 * consumes at most one retention slot. Registered last so it sees the maximum
 * observed across every fixture that ran before it. */
int test_hol_one_slot_per_action_and_no_drain_overflow(void)
{
    int failures = 0;
#ifdef MOQ_BRIDGE_SCAN_COUNTERS
    extern uint32_t moq_bridge_max_retain_delta;
    printf("SLOTS max pending slots consumed by one drained action = %u\n",
           moq_bridge_max_retain_delta);
    if (moq_bridge_max_retain_delta > 1) {
        fprintf(stderr, "SLOTS: an action consumed %u slots; the capacity "
                "guard's one-slot margin is unsound\n",
                moq_bridge_max_retain_delta);
        failures++;
    }
    if (moq_bridge_max_retain_delta == 0) {
        fprintf(stderr, "SLOTS: no retention was ever observed; the property "
                "is vacuous\n");
        failures++;
    }
#else
    printf("SLOTS not measured (build without MOQ_BRIDGE_SCAN_COUNTERS)\n");
#endif
    return failures;
}
