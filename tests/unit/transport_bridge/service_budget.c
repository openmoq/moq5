#include "test_cases.h"

/*
 * The budgeted-advance context must be left on EVERY exit from the budgeted
 * service entry.
 * If any path returned without leaving it, the next ordinary session call would
 * run inside a budget it never asked for and could observe the private
 * suspension sentinel -- which no application-facing caller may ever see.
 *
 * Each case forces a different exit, then runs an unlimited advancing call.
 */
int test_budget_context_paired_on_every_exit(void)
{
    int failures = 0;

    /* Exit: fatal short-circuit (before the pass is even entered). */
    {
        fake_endpoint_t ep;
        held_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        held_bridge_error(b, 0x1, 1);
        MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(b));
        moq_bridge_budgeted_result_t r;
        (void)held_bridge_service_budgeted(b, 2, 4, &r);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        held_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: ordinary drained pass (the bottom break). */
    {
        fake_endpoint_t ep;
        held_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_bridge_budgeted_result_t r;
        MOQ_TEST_CHECK(held_bridge_service_budgeted(b, 1, 4, &r) ==
                       MOQ_OK);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 2) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        held_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: the pass itself returns non-OK -- a write error makes dispatch
     * fatal inside Step 2, so service returns MOQ_ERR_INTERNAL from within the
     * bracketed region rather than from a pre-enter guard. */
    {
        fake_endpoint_t ep;
        held_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        ep.fail_write = true;
        MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
        moq_bridge_budgeted_result_t r;
        MOQ_TEST_CHECK(held_bridge_service_budgeted(b, 2, 4, &r) ==
                       MOQ_ERR_INTERNAL);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        held_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: outbound blocked, so the pass breaks out of Step 1. */
    {
        fake_endpoint_t ep;
        held_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        ep.block_write = true;      /* force WOULD_BLOCK on the setup write */
        MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
        moq_bridge_budgeted_result_t r;
        (void)held_bridge_service_budgeted(b, 2, 4, &r);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        held_bridge_destroy(b);
        moq_session_destroy(s);
    }

    return failures;
}


/* == Tests ========================================================== */

/*
 * The unlimited service entry must enter NO budget context.
 *
 * Reading budget_active after the call cannot show this -- a paired
 * enter/leave also leaves it false. The entry COUNT is what discriminates:
 * a UINT32_MAX bracket would still be a finite budget, routing every wired
 * session entry point through the budgeted advance and giving the unlimited
 * entry a suspension it has no way to report.
 *
 * Deltas, not absolutes: the counters are gated globals that every test in
 * this binary accumulates into.
 */
int test_unlimited_service_enters_no_budget_context(void)
{
    int failures = 0;

    fake_endpoint_t ep;
    held_endpoint_init(&ep, 100, 200);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                &b) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);

    uint64_t before = session_budget_enter_count;
    MOQ_TEST_CHECK(held_bridge_service(b, 2) == MOQ_OK);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 0);
    MOQ_TEST_CHECK(!s->budget_active);

    before = session_budget_enter_count;
    moq_bridge_budgeted_result_t r;
    MOQ_TEST_CHECK(held_bridge_service_budgeted(b, 3, 8, &r) == MOQ_OK);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 1);
    MOQ_TEST_CHECK(!s->budget_active);

    held_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}


/*
 * The budgeted entry requires an output. A finite budget that can suspend,
 * paired with a discarded outcome, would report a drained pass while a cursor
 * is still live. *out is zeroed before any other argument is validated, so
 * every rejection except the NULL-output one still yields a defined outcome.
 */
int test_budgeted_service_requires_output(void)
{
    int failures = 0;

    fake_endpoint_t ep;
    held_endpoint_init(&ep, 100, 200);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                &b) == MOQ_OK);

    uint64_t before = session_budget_enter_count;
    MOQ_TEST_CHECK(held_bridge_service_budgeted(b, 1, 8, NULL) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 0);

    /* NULL bridge still leaves a defined, non-suspended outcome. */
    moq_bridge_budgeted_result_t r;
    memset(&r, 0xAB, sizeof(r));
    MOQ_TEST_CHECK(held_bridge_service_budgeted(NULL, 1, 8, &r) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(!r.suspended);
    MOQ_TEST_CHECK(r.sweep_spent == 0);

    /* An idle pool completes even at zero budget: no runnable work. */
    MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service_budgeted(b, 2, 0, &r) == MOQ_OK);
    MOQ_TEST_CHECK(!r.suspended);

    held_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}
