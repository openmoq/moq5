/*
 * moq_session_has_pending_output: whether a session still holds outbound
 * work it has not handed over.
 *
 * Pinned:
 *   - NULL is false; an established, drained session is false;
 *   - a queued, unpolled action is true, and polling it away makes it false;
 *   - observation consumes nothing: repeated calls agree, and the action
 *     capacity and queue are unchanged by them;
 *   - retained outbound work with an empty action ring is true (the full
 *     classification, not just the ring);
 *   - a ring holding only a datagram is true -- the reliable-only variant,
 *     which excludes explicit datagrams, would say false.
 * The last two set session state directly through the test internals: they
 * show which classification the public observer uses.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_outbound_internal.h"

#include <string.h>

static int failures = 0;

static void drain_all(moq_simpair_t *sp)
{
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(sp, 64, NULL), MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
}

static void run(moq_version_t version)
{
    int before = failures;
    MOQ_TEST_CHECK(!moq_session_has_pending_output(NULL));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 5;
    cfg.initial_now_us = 1000;
    cfg.version = version;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 8;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_create(&cfg, &sp), MOQ_OK);
    if (!sp) goto done;
    moq_simpair_start(sp);
    drain_all(sp);
    moq_session_t *c = moq_simpair_client(sp);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(c), MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(!moq_session_has_pending_output(c));

    /* A queued SUBSCRIBE, not yet polled. */
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ parts, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("t");
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &sc, moq_simpair_now_us(sp), &sub),
                              MOQ_OK);
    }
    size_t cap = moq_session_action_capacity(c);
    MOQ_TEST_CHECK(moq_session_has_pending_output(c));
    MOQ_TEST_CHECK(moq_session_has_pending_output(c));          /* repeatable */
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(c), cap);  /* consumed nothing */
    MOQ_TEST_CHECK(c->action_head != c->action_tail);
    drain_all(sp);                                              /* polled away */
    MOQ_TEST_CHECK(!moq_session_has_pending_output(c));

    /* Retained work with an empty ring. */
    MOQ_TEST_CHECK(c->action_head == c->action_tail);
    c->noslot_carrier_count = 1;
    MOQ_TEST_CHECK(moq_session_has_pending_output(c));
    c->noslot_carrier_count = 0;
    MOQ_TEST_CHECK(!moq_session_has_pending_output(c));

    /* A ring holding only a datagram. */
    {
        size_t slot = c->action_tail % c->action_cap;
        moq_action_t saved = c->actions[slot];
        memset(&c->actions[slot], 0, sizeof(c->actions[slot]));
        c->actions[slot].kind = MOQ_ACTION_SEND_DATAGRAM;
        c->action_tail++;
        MOQ_TEST_CHECK(moq_session_has_pending_output(c));
        MOQ_TEST_CHECK(!session_has_reliable_outbound_obligation(c));
        c->action_tail--;
        c->actions[slot] = saved;
        MOQ_TEST_CHECK(!moq_session_has_pending_output(c));
    }

    moq_simpair_destroy(sp);
done:
    printf("%s: session pending output draft=%llu\n", failures == before ? "PASS" : "FAIL",
           (unsigned long long)version);
}

int main(void)
{
    run(MOQ_VERSION_DRAFT_16);
    run(MOQ_VERSION_DRAFT_18);
    run(MOQ_VERSION_DRAFT_22);
    return failures ? 1 : 0;
}
