#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

int main(void)
{
    int failures = 0;

    /* == FETCH request → reject lifecycle ============================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client sends FETCH. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("audio");
        fcfg.start_group = 0;
        fcfg.start_object = 0;
        fcfg.end_group = 5;
        fcfg.end_object = 0;
        fcfg.group_order = MOQ_GROUP_ORDER_ASCENDING;

        moq_fetch_t fetch_handle;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000,
            &fetch_handle), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_fetch_is_valid(fetch_handle));

        /* Pump FETCH to server. */
        pump_actions_to_peer(c, sv, 1000);

        /* Server receives FETCH_REQUEST event. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.track_namespace.count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.track_namespace.parts[0].len, 4);
        MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.track_namespace.parts[0].data,
            "live", 4) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.track_name.len, 5);
        MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.track_name.data,
            "audio", 5) == 0);
        MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.start_group, 0);
        MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.end_group, 5);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fetch_request.group_order, (int)MOQ_GROUP_ORDER_ASCENDING);

        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server rejects the fetch. */
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        rej.reason = MOQ_BYTES_LITERAL("no such track");

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sv_fetch,
            &rej, 2000), (int)MOQ_OK);

        /* Handle should now be stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sv_fetch,
            &rej, 2000), (int)MOQ_ERR_STALE_HANDLE);

        /* Pump REQUEST_ERROR to client. */
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_ERROR);
        MOQ_TEST_CHECK(moq_fetch_eq(ev.u.fetch_error.fetch, fetch_handle));
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fetch_error.error_code, (int)MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_error.reason.len, 13);

        /* Client handle is now stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, fetch_handle,
            3000), (int)MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH → cancel lifecycle ==================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 3;

        moq_fetch_t fetch_handle;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000,
            &fetch_handle), (int)MOQ_OK);

        /* Pump FETCH to server. */
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Client cancels. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, fetch_handle,
            2000), (int)MOQ_OK);

        /* Client handle is now stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, fetch_handle,
            2000), (int)MOQ_ERR_STALE_HANDLE);

        /* Pump FETCH_CANCEL to server. */
        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_CANCELLED);
        MOQ_TEST_CHECK(moq_fetch_eq(ev.u.fetch_cancelled.fetch, sv_fetch));

        /* Server handle is now stale. */
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sv_fetch,
            &rej, 3000), (int)MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Event WOULD_BLOCK: no mutation on inbound FETCH ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Setup manually — don't drain server setup event. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        scfg.max_events = 1;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Drain CLIENT setup event but NOT server's — server event queue is full. */
        moq_event_t drain;
        moq_session_poll_events(c, &drain, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("y");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);

        /* Pump FETCH bytes to server — on_control_bytes will WOULD_BLOCK
         * because the event queue is full (setup event still queued). */
        pump_actions_to_peer(c, sv, 1000);

        /* Verify no fetch entry was committed. */
        bool found = false;
        for (size_t i = 0; i < sv->fetch_cap; i++) {
            if (sv->fetches[i].state != MOQ_FETCH_FREE) {
                found = true;
                break;
            }
        }
        MOQ_TEST_CHECK(!found);

        /* Drain events and retry — now it should succeed. */
        moq_session_poll_events(sv, &drain, 1);
        moq_session_process_pending(sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action WOULD_BLOCK: no mutation on outbound FETCH ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("y");
        fcfg.end_group = 1;

        /* Fill the action queue. */
        moq_fetch_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h1), (int)MOQ_OK);

        /* Second fetch should WOULD_BLOCK — action queue full. */
        fcfg.track_name = MOQ_BYTES_LITERAL("z");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000,
            &h2), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_fetch_is_valid(h2));

        /* Verify no second fetch entry was committed. */
        int live_count = 0;
        for (size_t i = 0; i < c->fetch_cap; i++) {
            if (c->fetches[i].state != MOQ_FETCH_FREE)
                live_count++;
        }
        MOQ_TEST_CHECK_EQ_INT(live_count, 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Joining fetch with unknown joining_request_id → error ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Manually craft a joining FETCH wire message and feed to server. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        moq_d16_fetch_t fetch = {
            .request_id = 0,
            .fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN,
            .joining_request_id = 2,
            .joining_start = 10,
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fetch, NULL, 0), (int)MOQ_OK);
        size_t wire_len = moq_buf_writer_offset(&w);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire, wire_len,
            1000), (int)MOQ_OK);

        /* Server should emit REQUEST_ERROR(INVALID_JOINING_REQUEST_ID). */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);

        /* No FETCH_REQUEST event — joining was rejected internally. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 0);

        /* Session should still be ESTABLISHED (not closed). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Inbound joining fetch on a reused pending publisher slot ====== */
    /* Regression for the stale largest-object state on a publisher-side
     * subscription slot reused while still pending app-accept. The freed slot's
     * largest fields are cleared (so the reused pending subscription has no
     * current largest), and the D16 inbound joining-fetch path rejects a pending
     * subscription with no largest via INVALID_RANGE -- it must never surface a
     * FETCH_REQUEST built from a prior occupant's largest. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        /* One subscription slot on the server forces slot reuse. */
        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        /* Subscription #1 (client request_id 0): server accepts with a chosen
         * largest, then the client unsubscribes to free the server's slot. */
        moq_subscription_t sub1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub1) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub1 = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        asub.has_largest = true;
        asub.largest_group = 10;
        asub.largest_object = 5;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub1, &asub, 1000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        { moq_event_t e; while (moq_session_poll_events(c, &e, 1) > 0)
              moq_event_cleanup(&e); }

        MOQ_TEST_CHECK(moq_session_unsubscribe(c, sub1, 1500) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1500);
        { moq_event_t e; while (moq_session_poll_events(sv, &e, 1) > 0)
              moq_event_cleanup(&e); }

        /* Subscription #2 (client request_id 2): reuses the server slot and
         * stays PENDING (the app does not accept it). */
        moq_subscription_t sub2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 2000, &sub2) == MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);
        { moq_action_t a; while (moq_session_poll_actions(sv, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Peer sends a relative joining FETCH (its own request_id 4 = next
         * expected) referencing subscription #2 (joining_request_id 2). */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_fetch_t fetch = {
            .request_id = 4,
            .fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN,
            .joining_request_id = 2,
            .joining_start = 3,
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fetch, NULL, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 2500), (int)MOQ_OK);

        /* No FETCH_REQUEST may be surfaced for the unaccepted subscription. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        /* The fetch (request_id 4) is rejected with REQUEST_ERROR
         * INVALID_RANGE, not surfaced. */
        { moq_action_t acts[4]; size_t na = moq_session_poll_actions(sv, acts, 4);
          bool saw_err = false;
          for (size_t i = 0; i < na; i++) {
              moq_d16_request_error_t err;
              if (decode_action_request_error(&acts[i], &err) == MOQ_OK &&
                  err.request_id == 4 &&
                  err.error_code == MOQ_REQUEST_ERROR_INVALID_RANGE)
                  saw_err = true;
              moq_action_cleanup(&acts[i]);
          }
          MOQ_TEST_CHECK(saw_err); }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == REQUEST_OK for fetch closes session ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        /* Drain server FETCH_REQUEST event. */
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        /* Feed a generic REQUEST_OK for the fetch request_id to the client.
         * Client request IDs start at 0 (even parity). */
        uint8_t ok_buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_request_ok(&w, 0, NULL, 0), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&w), 2000), (int)MOQ_OK);

        /* Client should have closed the session. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_ERROR WB: no mutation until retry ===================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Client with max_events=1; don't drain setup event. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        ccfg.max_events = 1;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        /* Don't drain client setup event — event queue full. */
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &drain, 1);

        /* Server rejects. */
        moq_fetch_t sv_fetch = drain.u.fetch_request.fetch;
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sv_fetch,
            &rej, 2000), (int)MOQ_OK);

        /* Pump REQUEST_ERROR to client — event queue full, WOULD_BLOCK. */
        pump_actions_to_peer(sv, c, 2000);

        /* Fetch entry should still be live (not freed). */
        int slot = fetch_resolve_handle(c, h);
        MOQ_TEST_CHECK(slot >= 0);

        /* Drain events, retry. */
        moq_session_poll_events(c, &drain, 1);
        moq_session_process_pending(c, 2000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_ERROR);

        /* Handle should now be stale. */
        MOQ_TEST_CHECK(fetch_resolve_handle(c, h) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_CANCEL WB: no mutation until retry ==================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Server with max_events=1; don't drain setup event. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        scfg.max_events = 2;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Drain client setup but NOT server's. */
        moq_event_t drain;
        moq_session_poll_events(c, &drain, 1);

        /* Client sends FETCH, pump to server. Server event queue has
         * cap=2: slot 0 = SETUP_COMPLETE (undrained). The FETCH_REQUEST
         * fills slot 1 → queue is now full. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        /* Server fetch entry should be live. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        moq_fetch_t sv_fetch = {0};
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_FETCH_REQUEST)
                sv_fetch = evts[i].u.fetch_request.fetch;
        }
        MOQ_TEST_CHECK(moq_fetch_is_valid(sv_fetch));
        int sv_slot = fetch_resolve_handle(sv, sv_fetch);
        MOQ_TEST_CHECK(sv_slot >= 0);

        /* Client cancels, pump FETCH_CANCEL to server.
         * Server event queue is now empty after our drain above,
         * so we need to fill it first. */

        /* Send another FETCH to fill the server event queue. */
        fcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 2000, &h2), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);
        /* Now server event queue has FETCH_REQUEST for t2 → 1/2.
         * Send a third fetch to fill it. */
        fcfg.track_name = MOQ_BYTES_LITERAL("t3");
        moq_fetch_t h3;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 3000, &h3), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 3000);
        /* Now server event queue has 2 FETCH_REQUEST events → full. */

        /* Client cancels the first fetch. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 4000), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 4000);

        /* Server fetch entry should still be live (WOULD_BLOCK). */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) >= 0);

        /* Drain events, retry. */
        moq_session_poll_events(sv, evts, 4);
        moq_session_process_pending(sv, 4000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_CANCELLED);

        /* Handle should now be stale. */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* FETCH Slice F2: accept + data stream                           */
    /* ============================================================== */

    /* == accept_fetch emits FETCH_OK + data stream header ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("audio");
        fcfg.end_group = 5;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server accepts. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_of_track = false;
        acfg.end_group = 5;
        acfg.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        /* Server should have state ACCEPTED. */
        int sv_slot = fetch_resolve_handle(sv, sv_fetch);
        MOQ_TEST_CHECK(sv_slot >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[sv_slot].state, (int)MOQ_FETCH_ACCEPTED);

        /* Poll server actions: SEND_CONTROL (FETCH_OK) + SEND_DATA (header). */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK_EQ_INT((int)acts[1].kind, (int)MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(acts[1].u.send_data.header_len > 0);
        MOQ_TEST_CHECK(!acts[1].u.send_data.fin);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action queue full during accept — no mutation =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;

        /* Server action queue has cap=1 and is empty after draining.
         * First accept pushes FETCH_OK (fills queue), then tries SEND_DATA
         * which WOULD_BLOCK. State should not have changed. */
        moq_result_t rc = moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000);

        /* May succeed or WOULD_BLOCK depending on timing. If it blocked,
         * verify state unchanged. */
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            int sv_slot = fetch_resolve_handle(sv, sv_fetch);
            MOQ_TEST_CHECK(sv_slot >= 0);
            MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[sv_slot].state, (int)MOQ_FETCH_PENDING_PUBLISHER);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Empty fetch: header + FIN, then entry freed ================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        acfg.end_group = 0;
        acfg.end_object = 0;

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        /* Server handle should be stale (empty fetch frees entry). */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) < 0);

        /* Poll actions: SEND_CONTROL (FETCH_OK) + SEND_DATA (header+FIN). */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK_EQ_INT((int)acts[1].kind, (int)MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(acts[1].u.send_data.fin);

        /* Feed FETCH_OK control bytes to client. */
        if (acts[0].kind == MOQ_ACTION_SEND_CONTROL) {
            moq_session_on_control_bytes(c, acts[0].u.send_control.data,
                acts[0].u.send_control.len, 2000);
        }

        /* Client should receive FETCH_OK event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);
        MOQ_TEST_CHECK(moq_fetch_eq(ev.u.fetch_ok.fetch, h));
        MOQ_TEST_CHECK(ev.u.fetch_ok.end_of_track == true);

        /* Feed data stream to client (header + FIN). */
        if (na >= 2 && acts[1].kind == MOQ_ACTION_SEND_DATA) {
            moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
            moq_session_on_data_bytes(c, dref,
                acts[1].u.send_data.header, acts[1].u.send_data.header_len,
                acts[1].u.send_data.fin, 2000);
        }

        /* Client should receive FETCH_COMPLETE event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_COMPLETE);
        MOQ_TEST_CHECK(moq_fetch_eq(ev.u.fetch_complete.fetch, h));

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Data-before-OK: FETCH_HEADER arrives before FETCH_OK ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server accepts (non-empty). */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed data stream FIRST (before FETCH_OK control). */
        if (acts[1].kind == MOQ_ACTION_SEND_DATA) {
            moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, dref,
                acts[1].u.send_data.header, acts[1].u.send_data.header_len,
                false, 2000), (int)MOQ_OK);
        }

        /* Client fetch entry should have data_stream_started. */
        int c_slot = fetch_resolve_handle(c, h);
        MOQ_TEST_CHECK(c_slot >= 0);
        MOQ_TEST_CHECK(c->fetches[c_slot].data_stream_started);

        /* Session should still be active (not closed). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* Now feed FETCH_OK control bytes. */
        if (acts[0].kind == MOQ_ACTION_SEND_CONTROL) {
            moq_session_on_control_bytes(c, acts[0].u.send_control.data,
                acts[0].u.send_control.len, 2000);
        }
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Duplicate FETCH_HEADER on a second stream must not complete === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);

        /* Server accepts (non-empty) so a real response data stream opens. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);
        MOQ_TEST_CHECK_EQ_INT((int)acts[1].kind, (int)MOQ_ACTION_SEND_DATA);

        /* Feed FETCH_OK so control_ok is set (the completion gate). */
        if (acts[0].kind == MOQ_ACTION_SEND_CONTROL)
            moq_session_on_control_bytes(c, acts[0].u.send_control.data,
                acts[0].u.send_control.len, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);
        moq_event_cleanup(&ev);  /* drain FETCH_OK */

        /* Legitimate FETCH_HEADER on stream A, no FIN: stream binds. */
        moq_stream_ref_t a_ref = acts[1].u.send_data.stream_ref;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, a_ref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            false, 2000), (int)MOQ_OK);

        int c_slot = fetch_resolve_handle(c, h);
        MOQ_TEST_CHECK(c_slot >= 0);
        MOQ_TEST_CHECK(c->fetches[c_slot].data_stream_started);
        MOQ_TEST_CHECK(c->fetches[c_slot].data_stream_ref._v == a_ref._v);

        /* Duplicate FETCH_HEADER (same request_id bytes) on a *different*
         * stream B, with FIN: must NOT overwrite the bound stream or emit
         * FETCH_COMPLETE -- it is a protocol violation (close 0x3). */
        moq_stream_ref_t b_ref = moq_stream_ref_from_u64(a_ref._v + 100);
        moq_session_on_data_bytes(c, b_ref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            true, 2000);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);
        /* No FETCH_COMPLETE was emitted and the fetch was not freed by the
         * duplicate. (Same-stream WB re-drive is covered by the dedicated
         * FETCH_HEADER+FIN retry tests below, which keep the same stream_ref.) */
        size_t ne = moq_session_poll_events(c, &ev, 1);
        if (ne == 1)
            MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_FETCH_COMPLETE);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Unknown request_id in FETCH_HEADER closes session ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a FETCH_HEADER with bogus request_id and feed to client. */
        uint8_t hdr_buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr_buf, sizeof(hdr_buf));
        moq_d16_encode_fetch_header(&w, 999);
        size_t hdr_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t fake_ref = moq_stream_ref_from_u64(777);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, fake_ref,
            hdr_buf, hdr_len, false, 1000), (int)MOQ_OK);

        /* Session should have closed. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == F2 fix: accept_fetch with max_actions=1 → clean WB ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;

        /* max_actions=1: accept needs 2 slots → WOULD_BLOCK. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_ERR_WOULD_BLOCK);

        /* State unchanged. */
        int sv_slot = fetch_resolve_handle(sv, sv_fetch);
        MOQ_TEST_CHECK(sv_slot >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[sv_slot].state, (int)MOQ_FETCH_PENDING_PUBLISHER);

        /* No actions queued. */
        moq_action_t acts[4];
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, acts, 4), 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == F2 fix: FETCH_HEADER+FIN WB retry =========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Client with max_events=1; don't drain setup event. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        ccfg.max_events = 1;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        /* Don't drain client setup event — event queue full. */
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &drain, 1);

        moq_fetch_t sv_fetch = drain.u.fetch_request.fetch;
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);

        /* Feed FETCH_OK control first so control_ok is set. Event queue
         * is full (setup event), so FETCH_OK will WOULD_BLOCK. */
        MOQ_TEST_CHECK(na >= 2);
        moq_result_t crc = moq_session_on_control_bytes(c,
            acts[0].u.send_control.data, acts[0].u.send_control.len, 2000);
        (void)crc;

        /* Drain setup event, retry control to get FETCH_OK through. */
        moq_session_poll_events(c, &drain, 1);
        moq_session_process_pending(c, 2000);
        moq_session_poll_events(c, &drain, 1);
        MOQ_TEST_CHECK_EQ_INT((int)drain.kind, (int)MOQ_EVENT_FETCH_OK);

        /* Now feed FETCH_HEADER+FIN. Client event queue is empty (cap=1).
         * This should produce FETCH_COMPLETE. */
        moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, dref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            acts[1].u.send_data.fin, 2000), (int)MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_COMPLETE);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == F2 fix: duplicate FETCH_OK closes session ==================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_fetch_t sv_fetch = drain.u.fetch_request.fetch;
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);

        /* Feed first FETCH_OK to client. */
        MOQ_TEST_CHECK(na >= 1);
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 2000);
        moq_session_poll_events(c, &drain, 1);
        MOQ_TEST_CHECK_EQ_INT((int)drain.kind, (int)MOQ_EVENT_FETCH_OK);

        /* Feed same FETCH_OK again → session closes. */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 3000);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == F2 fix: fetch object bytes don't emit subgroup events ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_fetch_t sv_fetch = drain.u.fetch_request.fetch;
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);

        /* Feed FETCH_HEADER (no FIN) to client. */
        MOQ_TEST_CHECK(na >= 2);
        moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
        moq_session_on_data_bytes(c, dref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            false, 2000);

        /* Send some fake object-like bytes — session should close. */
        uint8_t fake_data[] = { 0x00, 0x01, 0x02, 0x03, 0x04 };
        moq_session_on_data_bytes(c, dref, fake_data, sizeof(fake_data),
            false, 2000);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_OK + FIN completes lifecycle, handle stales ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, drain.u.fetch_request.fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed FETCH_OK first, then data+FIN. */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 2000);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);

        moq_session_on_data_bytes(c, acts[1].u.send_data.stream_ref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            acts[1].u.send_data.fin, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        /* Handle should now be stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000), (int)MOQ_ERR_STALE_HANDLE);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_ERROR then late data FIN: no FETCH_COMPLETE ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        /* Server accepts (non-empty) to get a data stream. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, drain.u.fetch_request.fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed FETCH_HEADER (no FIN) to client. */
        moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
        moq_session_on_data_bytes(c, dref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            false, 2000);

        /* Now feed REQUEST_ERROR instead of FETCH_OK. Craft it manually. */
        uint8_t err_buf[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        moq_d16_encode_request_error(&ew, 0, 0x10, 0,
            (const uint8_t *)"gone", 4);
        moq_session_on_control_bytes(c, err_buf,
            moq_buf_writer_offset(&ew), 3000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_ERROR);

        /* Client handle is now stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000), (int)MOQ_ERR_STALE_HANDLE);

        /* Late data FIN arrives — no FETCH_COMPLETE, no close. */
        moq_session_on_data_bytes(c, dref, NULL, 0, true, 4000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_HEADER+FIN WB with control_ok: retry emits complete === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        ccfg.max_events = 1;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);
        /* Drain client setup event. */
        moq_session_poll_events(c, &drain, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &drain, 1);

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, drain.u.fetch_request.fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed FETCH_OK to client (event queue empty, cap=1). */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 2000);
        /* FETCH_OK fills the event queue (cap=1). */

        /* Feed FETCH_HEADER+FIN — event queue full, should WB. */
        moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
        moq_result_t drc = moq_session_on_data_bytes(c, dref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            acts[1].u.send_data.fin, 2000);
        MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_ERR_WOULD_BLOCK);

        /* Session still alive. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* Drain FETCH_OK event. */
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);

        /* Retry data stream. */
        moq_session_on_data_bytes(c, dref, NULL, 0, false, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Data-before-OK with FIN: FETCH_OK triggers FETCH_COMPLETE === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv,
            drain.u.fetch_request.fetch, &acfg, 2000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed data stream with FIN FIRST (before FETCH_OK). */
        moq_stream_ref_t dref = acts[1].u.send_data.stream_ref;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, dref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            acts[1].u.send_data.fin, 2000), (int)MOQ_OK);

        /* No FETCH_COMPLETE yet — control_ok is false. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);

        /* Now feed FETCH_OK. */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 3000);

        /* Should get FETCH_OK followed by FETCH_COMPLETE. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(ne, 2);
        MOQ_TEST_CHECK_EQ_INT((int)evts[0].kind, (int)MOQ_EVENT_FETCH_OK);
        MOQ_TEST_CHECK_EQ_INT((int)evts[1].kind, (int)MOQ_EVENT_FETCH_COMPLETE);
        MOQ_TEST_CHECK(moq_fetch_eq(evts[1].u.fetch_complete.fetch, h));

        /* Handle should now be stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 4000),
            (int)MOQ_ERR_STALE_HANDLE);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Slot reuse after completion: second fetch works ============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_fetches = 1;
        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_fetches = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* First fetch: request + reject. */
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t1");
        fcfg.end_group = 1;

        moq_fetch_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h1), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch1 = ev.u.fetch_request.fetch;

        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sv_fetch1,
            &rej, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_ERROR);

        /* Second fetch reusing slot 0. */
        fcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 3000, &h2), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 3000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_fetch_t sv_fetch2 = ev.u.fetch_request.fetch;

        /* Server accepts second fetch. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch2,
            &acfg, 4000), (int)MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);

        /* Feed FETCH_OK + data to client. */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 4000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);

        moq_session_on_data_bytes(c, acts[1].u.send_data.stream_ref,
            acts[1].u.send_data.header, acts[1].u.send_data.header_len,
            acts[1].u.send_data.fin, 4000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        /* Handle stales. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h2, 5000),
            (int)MOQ_ERR_STALE_HANDLE);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* FETCH Slice F3: object/gap parsing + publisher write           */
    /* ============================================================== */

    /* == End-to-end fetch with objects + gap + end_fetch ============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client sends FETCH. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 3;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server accepts. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 3;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        /* Write an object: group=0, subgroup=0, object=0 */
        uint8_t obj_data[] = "hello";
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, obj_data, 5, &payload);

        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.subgroup_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);

        /* Write a gap: group=1 non-existent */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_range(sv, sv_fetch,
            MOQ_FETCH_RANGE_NON_EXISTENT, 1, 0, 3000), (int)MOQ_OK);

        /* Write another object: group=2 */
        moq_rcbuf_create(&alloc, (const uint8_t *)"world", 5, &payload);
        ocfg.group_id = 2;
        ocfg.object_id = 0;
        ocfg.payload = payload;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);

        /* End fetch. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(sv, sv_fetch, 4000),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(sv, sv_fetch, 4000),
            (int)MOQ_ERR_STALE_HANDLE);

        /* Pump all server actions to client. */
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);

        /* Feed FETCH_OK control to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_session_on_control_bytes(c, acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 4000);
            }
        }
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OK);

        /* Feed all data actions to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    acts[i].u.send_data.fin, 4000);
                if (acts[i].u.send_data.payload) {
                    moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 4000);
                }
            }
        }

        /* Client should receive: FETCH_OBJECT, FETCH_GAP, FETCH_OBJECT, FETCH_COMPLETE */
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(c, evts, 8);
        MOQ_TEST_CHECK(ne >= 4);

        MOQ_TEST_CHECK_EQ_INT((int)evts[0].kind, (int)MOQ_EVENT_FETCH_OBJECT);
        MOQ_TEST_CHECK_EQ_U64(evts[0].u.fetch_object.group_id, 0);
        MOQ_TEST_CHECK_EQ_U64(evts[0].u.fetch_object.object_id, 0);
        MOQ_TEST_CHECK(evts[0].u.fetch_object.payload != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(evts[0].u.fetch_object.payload), 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[0].u.fetch_object.payload),
            "hello", 5) == 0);

        MOQ_TEST_CHECK_EQ_INT((int)evts[1].kind, (int)MOQ_EVENT_FETCH_GAP);
        MOQ_TEST_CHECK_EQ_INT((int)evts[1].u.fetch_gap.range_kind,
            (int)MOQ_FETCH_RANGE_NON_EXISTENT);
        MOQ_TEST_CHECK_EQ_U64(evts[1].u.fetch_gap.group_id, 1);

        MOQ_TEST_CHECK_EQ_INT((int)evts[2].kind, (int)MOQ_EVENT_FETCH_OBJECT);
        MOQ_TEST_CHECK_EQ_U64(evts[2].u.fetch_object.group_id, 2);
        MOQ_TEST_CHECK(evts[2].u.fetch_object.payload != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(evts[2].u.fetch_object.payload), 5);

        MOQ_TEST_CHECK_EQ_INT((int)evts[3].kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        /* Handle should be stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 5000),
            (int)MOQ_ERR_STALE_HANDLE);

        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == write_fetch_object WB: no prior mutation ===================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 2;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        moq_session_fetch(c, &fcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        /* Action queue cap=2 — accept used both slots. */
        int sv_slot = fetch_resolve_handle(sv, sv_fetch);
        MOQ_TEST_CHECK(sv_slot >= 0);
        MOQ_TEST_CHECK(!sv->fetches[sv_slot].prior.has_prev);

        uint8_t d[] = "x";
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, d, 1, &payload);

        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_ERR_WOULD_BLOCK);

        /* Prior should NOT have been updated. */
        MOQ_TEST_CHECK(!sv->fetches[sv_slot].prior.has_prev);

        moq_rcbuf_decref(payload);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Late object bytes after REQUEST_ERROR: absorbed silently ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        /* Server accepts to get a data stream. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv,
            drain.u.fetch_request.fetch, &acfg, 2000), (int)MOQ_OK);

        /* Write an object on the server side. */
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"abc", 3, &payload);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        moq_session_write_fetch_object(sv, drain.u.fetch_request.fetch,
            &ocfg, 2000);
        moq_rcbuf_decref(payload);

        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);

        /* Feed FETCH_HEADER (no FIN) to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA && !acts[i].u.send_data.fin) {
                moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    false, 2000);
                if (acts[i].u.send_data.payload)
                    moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 2000);
            }
        }

        /* Feed REQUEST_ERROR to kill the fetch. */
        uint8_t err_buf[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        moq_d16_encode_request_error(&ew, 0, 0x10, 0,
            (const uint8_t *)"gone", 4);
        moq_session_on_control_bytes(c, err_buf,
            moq_buf_writer_offset(&ew), 3000);

        /* Drain: should see FETCH_OBJECT then FETCH_ERROR. */
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(c, evts, 8);
        bool saw_object = false, saw_error = false;
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) saw_object = true;
            if (evts[i].kind == MOQ_EVENT_FETCH_ERROR) saw_error = true;
            moq_event_cleanup(&evts[i]);
        }
        MOQ_TEST_CHECK(saw_object);
        MOQ_TEST_CHECK(saw_error);

        /* Now feed more object bytes on the data stream.
         * Handle is stale — these should be absorbed, not crash.
         * Write a second object on the server side (still accepted). */
        moq_rcbuf_create(&alloc, (const uint8_t *)"xyz", 3, &payload);
        ocfg.group_id = 1;
        ocfg.object_id = 0;
        ocfg.payload = payload;
        moq_session_write_fetch_object(sv, drain.u.fetch_request.fetch,
            &ocfg, 3000);
        moq_rcbuf_decref(payload);

        moq_action_t acts2[8];
        size_t na2 = moq_session_poll_actions(sv, acts2, 8);
        for (size_t i = 0; i < na2; i++) {
            if (acts2[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_session_on_data_bytes(c, acts2[i].u.send_data.stream_ref,
                    acts2[i].u.send_data.header, acts2[i].u.send_data.header_len,
                    false, 4000);
                if (acts2[i].u.send_data.payload)
                    moq_session_on_data_bytes(c, acts2[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts2[i].u.send_data.payload),
                        moq_rcbuf_len(acts2[i].u.send_data.payload),
                        false, 4000);
            }
        }

        /* No new events should be emitted. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        for (size_t i = 0; i < na2; i++) moq_action_cleanup(&acts2[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Polling FETCH_OBJECT releases receive budget ================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 5;

        moq_fetch_t h;
        moq_session_fetch(c, &fcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 5;
        moq_session_accept_fetch(sv, sv_fetch, &acfg, 2000);

        /* Write one object with 16-byte payload. */
        uint8_t data[16];
        memset(data, 'A', sizeof(data));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, data, sizeof(data), &p);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = p;
        moq_session_write_fetch_object(sv, sv_fetch, &ocfg, 3000);
        moq_rcbuf_decref(p);

        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);

        /* Feed FETCH_OK control to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c, acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 3000);
        }
        moq_session_poll_events(c, &ev, 1);

        /* Feed all data to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    acts[i].u.send_data.fin, 3000);
                if (acts[i].u.send_data.payload)
                    moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 3000);
            }
        }

        /* Budget should be charged. */
        MOQ_TEST_CHECK(c->recv_payload_bytes >= 16);

        /* Poll the FETCH_OBJECT event — budget should be released. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_OBJECT);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK_EQ_SIZE(c->recv_payload_bytes, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Late object bytes after REQUEST_ERROR with full event queue == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        ccfg.max_events = 1;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain;
        moq_session_poll_events(c, &drain, 1);
        moq_session_poll_events(sv, &drain, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        moq_session_fetch(c, &fcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &drain, 1);

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        moq_session_accept_fetch(sv, drain.u.fetch_request.fetch, &acfg, 2000);

        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"abc", 3, &payload);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        moq_session_write_fetch_object(sv, drain.u.fetch_request.fetch,
            &ocfg, 2000);
        moq_rcbuf_decref(payload);

        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);

        /* Feed FETCH_HEADER (no FIN, no payload yet) to client. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_session_on_data_bytes(c, acts[i].u.send_data.stream_ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    false, 2000);
                break;
            }
        }

        /* Feed REQUEST_ERROR. Event queue cap=1, currently empty. */
        uint8_t err_buf[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        moq_d16_encode_request_error(&ew, 0, 0x10, 0,
            (const uint8_t *)"gone", 4);
        moq_session_on_control_bytes(c, err_buf,
            moq_buf_writer_offset(&ew), 3000);
        /* Event queue now has FETCH_ERROR → full. */

        /* Feed object header + payload bytes. Handle is stale, event
         * queue full. Should return OK (absorbed), not WOULD_BLOCK. */
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA &&
                acts[i].u.send_data.header_len > 0 &&
                acts[i].u.send_data.payload) {
                moq_session_on_data_bytes(c,
                    acts[i].u.send_data.stream_ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    false, 3000);
                moq_result_t drc = moq_session_on_data_bytes(c,
                    acts[i].u.send_data.stream_ref,
                    moq_rcbuf_data(acts[i].u.send_data.payload),
                    moq_rcbuf_len(acts[i].u.send_data.payload),
                    false, 3000);
                MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_OK);
            }
        }

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        moq_session_poll_events(c, &drain, 1);
        MOQ_TEST_CHECK_EQ_INT((int)drain.kind, (int)MOQ_EVENT_FETCH_ERROR);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Properties+payload round-trip to subscriber ================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch, &acfg, 2000), (int)MOQ_OK);

        /* Write object with properties + payload. */
        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &payload), (int)MOQ_OK);
        moq_rcbuf_t *props = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, (const uint8_t *)"prop", 4, &props), (int)MOQ_OK);

        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        ocfg.properties = props;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(sv, sv_fetch, 3000), (int)MOQ_OK);

        /* Pump all to client. */
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data, acts[i].u.send_control.len, 3000), (int)MOQ_OK);
            else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c,
                    acts[i].u.send_data.stream_ref, acts[i].u.send_data.header,
                    acts[i].u.send_data.header_len, acts[i].u.send_data.fin, 3000), (int)MOQ_OK);
                if (acts[i].u.send_data.payload)
                    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c,
                        acts[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 3000), (int)MOQ_OK);
            }
        }

        /* Drain: FETCH_OK, FETCH_OBJECT (with props+payload), FETCH_COMPLETE. */
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(c, evts, 8);
        MOQ_TEST_CHECK(ne >= 3);
        MOQ_TEST_CHECK_EQ_INT((int)evts[0].kind, (int)MOQ_EVENT_FETCH_OK);
        MOQ_TEST_CHECK_EQ_INT((int)evts[1].kind, (int)MOQ_EVENT_FETCH_OBJECT);
        MOQ_TEST_CHECK(evts[1].u.fetch_object.payload != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(evts[1].u.fetch_object.payload), 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[1].u.fetch_object.payload), "hello", 5) == 0);
        MOQ_TEST_CHECK(evts[1].u.fetch_object.properties != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(evts[1].u.fetch_object.properties), 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[1].u.fetch_object.properties), "prop", 4) == 0);
        MOQ_TEST_CHECK_EQ_INT((int)evts[2].kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Properties WB: insufficient slots leaves prior unchanged ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 3;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch, &acfg, 2000), (int)MOQ_OK);
        /* accept used 2 of 3 action slots; 1 remains. */

        int sv_slot = fetch_resolve_handle(sv, sv_fetch);
        MOQ_TEST_CHECK(sv_slot >= 0);
        MOQ_TEST_CHECK(!sv->fetches[sv_slot].prior.has_prev);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &payload), (int)MOQ_OK);
        moq_rcbuf_t *props = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, (const uint8_t *)"p", 1, &props), (int)MOQ_OK);

        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        ocfg.properties = props;

        /* Properties needs 2 slots, only 1 available → WOULD_BLOCK. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_ERR_WOULD_BLOCK);

        /* Prior unchanged. */
        MOQ_TEST_CHECK(!sv->fetches[sv_slot].prior.has_prev);

        /* No partial actions queued — only the accept's 2 actions. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        MOQ_TEST_CHECK_EQ_SIZE(na, 2);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Properties-only (NULL payload) round-trip ===================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch, &acfg, 2000), (int)MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, (const uint8_t *)"meta", 4, &props), (int)MOQ_OK);

        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.publisher_priority = 128;
        ocfg.payload = NULL;
        ocfg.properties = props;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sv_fetch,
            &ocfg, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(props);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(sv, sv_fetch, 3000), (int)MOQ_OK);

        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(sv, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data, acts[i].u.send_control.len, 3000), (int)MOQ_OK);
            else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c,
                    acts[i].u.send_data.stream_ref, acts[i].u.send_data.header,
                    acts[i].u.send_data.header_len, acts[i].u.send_data.fin, 3000), (int)MOQ_OK);
                if (acts[i].u.send_data.payload)
                    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c,
                        acts[i].u.send_data.stream_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 3000), (int)MOQ_OK);
            }
        }

        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(c, evts, 8);
        MOQ_TEST_CHECK(ne >= 3);
        MOQ_TEST_CHECK_EQ_INT((int)evts[0].kind, (int)MOQ_EVENT_FETCH_OK);
        MOQ_TEST_CHECK_EQ_INT((int)evts[1].kind, (int)MOQ_EVENT_FETCH_OBJECT);
        MOQ_TEST_CHECK(evts[1].u.fetch_object.payload == NULL);
        MOQ_TEST_CHECK(evts[1].u.fetch_object.properties != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(evts[1].u.fetch_object.properties), 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[1].u.fetch_object.properties), "meta", 4) == 0);
        MOQ_TEST_CHECK_EQ_INT((int)evts[2].kind, (int)MOQ_EVENT_FETCH_COMPLETE);

        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* Batch 3: FETCH_CANCEL after acceptance                         */
    /* ============================================================== */

    /* == FETCH_CANCEL after accept emits CANCELLED + RESET_DATA ====== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server accepts (non-empty). */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        /* Drain accept actions (FETCH_OK + FETCH_HEADER). */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Client cancels. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 3000);

        /* Server should get FETCH_CANCELLED event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_CANCELLED);
        MOQ_TEST_CHECK(moq_fetch_eq(ev.u.fetch_cancelled.fetch, sv_fetch));

        /* Server should also have a RESET_DATA action for the fetch stream. */
        na = moq_session_poll_actions(sv, acts, 8);
        bool found_reset = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_RESET_DATA)
                found_reset = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_reset);

        /* Handle is stale. */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action WB on accepted cancel leaves state intact ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 2;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);
        /* Action queue now full (2/2: FETCH_OK + FETCH_HEADER). */

        /* Client cancels, pump to server. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 3000);

        /* Cancel needs 1 action slot for RESET_DATA — should WB. */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[fetch_resolve_handle(sv, sv_fetch)].state,
            (int)MOQ_FETCH_ACCEPTED);

        /* Drain actions, retry. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_process_pending(sv, 3000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_CANCELLED);
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Event WB on cancel leaves state intact and retry works ======= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_events = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Fill event queue: send a second fetch request. */
        fcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 2000, &h2),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);
        /* Event queue now full (1/1: FETCH_REQUEST for t2). */

        /* Client cancels first fetch. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 3000);

        /* Cancel should WB — fetch still alive. */
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) >= 0);

        /* Drain events, retry. */
        moq_session_poll_events(sv, &ev, 1);
        moq_session_process_pending(sv, 3000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_CANCELLED);
        MOQ_TEST_CHECK(fetch_resolve_handle(sv, sv_fetch) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Late fetch data after local cancel: absorbed, no close ======= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

        /* Server accepts and writes an object. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sv_fetch,
            &acfg, 2000), (int)MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc,
            (const uint8_t *)"obj", 3, &payload), (int)MOQ_OK);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.publisher_priority = 128;
        ocfg.payload = payload;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv,
            sv_fetch, &ocfg, 2000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(sv, sv_fetch, 2000),
            (int)MOQ_OK);

        /* Collect server actions BEFORE client cancel. */
        moq_action_t sv_acts[16];
        size_t sv_na = moq_session_poll_actions(sv, sv_acts, 16);

        /* Client cancels locally — handle becomes stale but tombstone
         * remains in the registry. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 3000),
            (int)MOQ_ERR_STALE_HANDLE);

        /* Now feed the server's FETCH_OK + data stream to the client.
         * This is the "late data after cancel" race. */
        for (size_t i = 0; i < sv_na; i++) {
            if (sv_acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    sv_acts[i].u.send_control.data,
                    sv_acts[i].u.send_control.len, 4000);
            else if (sv_acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    sv_acts[i].u.send_data.stream_ref._v + 10000);
                moq_session_on_data_bytes(c, ref,
                    sv_acts[i].u.send_data.header,
                    sv_acts[i].u.send_data.header_len,
                    sv_acts[i].u.send_data.fin, 4000);
                if (sv_acts[i].u.send_data.payload)
                    moq_session_on_data_bytes(c, ref,
                        moq_rcbuf_data(sv_acts[i].u.send_data.payload),
                        moq_rcbuf_len(sv_acts[i].u.send_data.payload),
                        false, 4000);
            }
        }

        /* Session must still be alive — no protocol close. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* No FETCH_OBJECT or FETCH_COMPLETE events should have been emitted. */
        moq_event_t drain[8];
        size_t ne = moq_session_poll_events(c, drain, 8);
        for (size_t i = 0; i < ne; i++) {
            MOQ_TEST_CHECK((int)drain[i].kind != (int)MOQ_EVENT_FETCH_OBJECT);
            MOQ_TEST_CHECK((int)drain[i].kind != (int)MOQ_EVENT_FETCH_COMPLETE);
            moq_event_cleanup(&drain[i]);
        }

        for (size_t i = 0; i < sv_na; i++) moq_action_cleanup(&sv_acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Local cancel frees the fetch slot; late REQUEST_ERROR absorbed == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_fetches = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t1");
        fcfg.end_group = 1;

        moq_fetch_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h1),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Client cancels before server responds. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h1, 2000),
            (int)MOQ_OK);

        /* The fetch slot is freed at cancel: a second fetch succeeds
         * immediately even though the peer has sent no response (the core
         * regression -- a withheld response previously pinned the only slot). */
        fcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 2000, &h2),
            (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Server rejects the original (cancelled) fetch -> the late
         * REQUEST_ERROR is absorbed by the cancel tombstone: no FETCH_ERROR
         * event, session stays established. */
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv,
            ev.u.fetch_request.fetch, &rej, 2000), (int)MOQ_OK);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 3000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Tombstone freed on late data stream after FETCH_OK =========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_fetches = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t1");
        fcfg.end_group = 1;

        moq_fetch_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h1),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Server accepts with empty fetch (header+FIN). */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv,
            ev.u.fetch_request.fetch, &acfg, 2000), (int)MOQ_OK);

        moq_action_t sv_acts[8];
        size_t sv_na = moq_session_poll_actions(sv, sv_acts, 8);

        /* Client cancels before receiving anything. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h1, 2000),
            (int)MOQ_OK);

        /* Slot freed at cancel: a second fetch succeeds immediately, before any
         * of the peer's withheld FETCH_OK / data stream arrives. */
        fcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_fetch_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 3000, &h2),
            (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Feed late FETCH_OK (control) for the cancelled fetch — absorbed by the
         * tombstone (kept so a following data stream is still stopped). */
        for (size_t i = 0; i < sv_na; i++) {
            if (sv_acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    sv_acts[i].u.send_control.data,
                    sv_acts[i].u.send_control.len, 3000);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);

        /* Feed the late data stream for the cancelled fetch — stopped via a
         * STOP_DATA action, tombstone consumed, session stays open. */
        for (size_t i = 0; i < sv_na; i++) {
            if (sv_acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    sv_acts[i].u.send_data.stream_ref._v + 10000);
                moq_session_on_data_bytes(c, ref,
                    sv_acts[i].u.send_data.header,
                    sv_acts[i].u.send_data.header_len,
                    sv_acts[i].u.send_data.fin, 3000);
            }
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
        { moq_action_t acts[8]; size_t na = moq_session_poll_actions(c, acts, 8);
          bool saw_stop = false;
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_STOP_DATA) saw_stop = true;
              moq_action_cleanup(&acts[i]);
          }
          MOQ_TEST_CHECK(saw_stop); }

        for (size_t i = 0; i < sv_na; i++) moq_action_cleanup(&sv_acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Cancel tombstone cache stays bounded across repeated cancels === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_fetches = 2;   /* tombstone cache cap == fetch pool cap */

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;

        /* Cancel far more fetches than the cache can remember; every fetch must
         * still get a slot (the cache drops oldest, never pins the pool). */
        for (int i = 0; i < 4; i++) {   /* 4 cancels, cache cap 2 -> drop-oldest */
            moq_fetch_t h;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000 + i, &h),
                (int)MOQ_OK);
            { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
                  moq_action_cleanup(&a); }
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h, 1000 + i),
                (int)MOQ_OK);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Cancel tombstone consumed on a deferred (retried) STOP ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_fetches = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t1");
        fcfg.end_group = 1;

        moq_fetch_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h1),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Server accepts (empty: header+FIN) — capture the data-stream action. */
        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.empty = true;
        acfg.end_of_track = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv,
            ev.u.fetch_request.fetch, &acfg, 2000), (int)MOQ_OK);
        moq_action_t sv_acts[8];
        size_t sv_na = moq_session_poll_actions(sv, sv_acts, 8);

        /* Client cancels: tombstone recorded, slot freed. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, h1, 2000),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(c->fetch_cancel_tomb_count, (size_t)1);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Fill the client action queue so the late data stream's STOP_DATA
         * cannot be queued immediately (forces the MOQ_RX_NEED_STOP deferral). */
        test_session_fill_action_queue(c);

        /* Feed the late data stream — the stop is deferred (WOULD_BLOCK); the
         * tombstone must be retained, not consumed yet. */
        moq_stream_ref_t dref = {0};
        for (size_t i = 0; i < sv_na; i++) {
            if (sv_acts[i].kind == MOQ_ACTION_SEND_DATA) {
                dref = moq_stream_ref_from_u64(
                    sv_acts[i].u.send_data.stream_ref._v + 10000);
                moq_session_on_data_bytes(c, dref,
                    sv_acts[i].u.send_data.header,
                    sv_acts[i].u.send_data.header_len,
                    sv_acts[i].u.send_data.fin, 3000);
            }
        }
        MOQ_TEST_CHECK_EQ_SIZE(c->fetch_cancel_tomb_count, (size_t)1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* Drain the queue and re-drive the receive: the deferred STOP now queues
         * and the tombstone is consumed on the retry path. */
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, dref, NULL, 0,
            false, 3000), (int)MOQ_OK);

        bool saw_stop = false;
        { moq_action_t acts[8]; size_t na = moq_session_poll_actions(c, acts, 8);
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_STOP_DATA) saw_stop = true;
              moq_action_cleanup(&acts[i]);
          } }
        MOQ_TEST_CHECK(saw_stop);
        MOQ_TEST_CHECK_EQ_SIZE(c->fetch_cancel_tomb_count, (size_t)0);

        for (size_t i = 0; i < sv_na; i++) moq_action_cleanup(&sv_acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH without GROUP_ORDER → ascending ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a standalone FETCH with no GROUP_ORDER param (params_count=0). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_fetch_t fetch = {
            .request_id   = 0,
            .fetch_type   = MOQ_D16_FETCH_TYPE_STANDALONE,
            .track_namespace = ns,
            .track_name   = MOQ_BYTES_LITERAL("t"),
            .start_group  = 0,
            .start_object = 0,
            .end_group    = 5,
            .end_object   = 0,
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fetch, NULL, 0),
            (int)MOQ_OK);
        size_t wire_len = moq_buf_writer_offset(&w);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(sv, wire, wire_len, 1000),
            (int)MOQ_OK);

        /* Server emits FETCH_REQUEST with group_order == ASCENDING. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fetch_request.group_order,
            (int)MOQ_GROUP_ORDER_ASCENDING);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == moq_session_fetch rejects end before start =================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");

        /* end_group < start_group → INVAL */
        fcfg.start_group = 5;
        fcfg.start_object = 0;
        fcfg.end_group = 3;
        fcfg.end_object = 0;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_ERR_INVAL);

        /* end_group == start_group, end_object <= start_object (non-zero end) → INVAL */
        fcfg.start_group = 5;
        fcfg.start_object = 3;
        fcfg.end_group = 5;
        fcfg.end_object = 2;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_ERR_INVAL);

        /* end_object == 0 (open end) in same group → valid */
        fcfg.start_group = 5;
        fcfg.start_object = 3;
        fcfg.end_group = 5;
        fcfg.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_OK: defined wrong-scope param ignored, unknown param closes */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* --- Part A: defined wrong-scope param (FORWARD=0x10) ignored --- */
        {
            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };

            moq_fetch_cfg_t fcfg;
            moq_fetch_cfg_init(&fcfg);
            fcfg.track_namespace = ns;
            fcfg.track_name = MOQ_BYTES_LITERAL("t");
            fcfg.end_group = 5;

            moq_fetch_t h;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
            pump_actions_to_peer(c, sv, 1000);
            moq_event_t ev;
            moq_session_poll_events(sv, &ev, 1);

            /* Build FETCH_OK with FORWARD param (defined, wrong-scope → ignored).
             * FORWARD (0x10) is even → is_varint=true; encode value=1 as varint. */
            uint8_t fwd_val[] = { 0x01 };
            moq_kvp_entry_t bad_params[1] = {{
                .type = MOQ_MSG_PARAM_FORWARD,
                .value = fwd_val, .value_len = 1,
                .is_varint = true, .raw = NULL, .raw_len = 0,
            }};
            uint8_t ok_wire[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, ok_wire, sizeof(ok_wire));
            const uint8_t ext_data[] = "trackprop";
            moq_d16_fetch_ok_t fok = {
                .request_id = 0,
                .end_of_track = 0,
                .end_group = 10,
                .end_object = 0,
                .params = bad_params,
                .params_count = 1,
                .params_cap = 1,
                .track_extensions = ext_data,
                .track_extensions_len = 9,
            };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d16_encode_fetch_ok(&w, &fok), (int)MOQ_OK);
            size_t ok_len = moq_buf_writer_offset(&w);

            /* Feed to client — session must stay alive (param ignored). */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_control_bytes(c, ok_wire, ok_len, 2000),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

            moq_event_t okev;
            MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &okev, 1), 1);
            MOQ_TEST_CHECK_EQ_INT((int)okev.kind, (int)MOQ_EVENT_FETCH_OK);

            /* Track properties should be present and readable. */
            MOQ_TEST_CHECK_EQ_SIZE(okev.u.fetch_ok.track_properties.len, 9);
            MOQ_TEST_CHECK(memcmp(okev.u.fetch_ok.track_properties.data,
                "trackprop", 9) == 0);

            moq_session_destroy(c);
            moq_session_destroy(sv);
        }

        /* --- Part B: truly unknown param (0xFE) closes session ---------- */
        {
            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };

            moq_fetch_cfg_t fcfg;
            moq_fetch_cfg_init(&fcfg);
            fcfg.track_namespace = ns;
            fcfg.track_name = MOQ_BYTES_LITERAL("t");
            fcfg.end_group = 5;

            moq_fetch_t h;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
            pump_actions_to_peer(c, sv, 1000);
            moq_event_t ev;
            moq_session_poll_events(sv, &ev, 1);

            /* Build FETCH_OK with truly unknown param 0xFE.
             * 0xFE is even → is_varint=true; encode value=0 as varint. */
            uint8_t unk_val[] = { 0x00 };
            moq_kvp_entry_t unk_params[1] = {{
                .type = 0xFE, .value = unk_val, .value_len = 1,
                .is_varint = true, .raw = NULL, .raw_len = 0,
            }};
            uint8_t ok_wire[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, ok_wire, sizeof(ok_wire));
            moq_d16_fetch_ok_t fok = {
                .request_id = 0,
                .end_of_track = 0,
                .end_group = 10,
                .end_object = 0,
                .params = unk_params,
                .params_count = 1,
                .params_cap = 1,
            };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d16_encode_fetch_ok(&w, &fok), (int)MOQ_OK);
            size_t ok_len = moq_buf_writer_offset(&w);

            /* Feed to client — session must close. */
            moq_session_on_control_bytes(c, ok_wire, ok_len, 2000);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

            moq_event_t cev;
            moq_session_poll_events(c, &cev, 1);
            MOQ_TEST_CHECK_EQ_INT((int)cev.kind, (int)MOQ_EVENT_SESSION_CLOSED);
            MOQ_TEST_CHECK_EQ_INT((int)cev.u.closed.code, 0x3);

            moq_session_destroy(c);
            moq_session_destroy(sv);
        }

        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == FETCH_OK end before requested start closes session ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = ns;
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.start_group = 5;
        fcfg.start_object = 0;
        fcfg.end_group = 10;
        fcfg.end_object = 0;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(c, &fcfg, 1000, &h), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* Server crafts FETCH_OK with end_group=3, which is before start_group=5. */
        uint8_t ok_wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_wire, sizeof(ok_wire));
        moq_d16_fetch_ok_t fok = {
            .request_id = 0,
            .end_of_track = 0,
            .end_group = 3,
            .end_object = 0,
        };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d16_encode_fetch_ok(&w, &fok), (int)MOQ_OK);
        size_t ok_len = moq_buf_writer_offset(&w);

        /* Feed to client — session must close with PROTOCOL_VIOLATION (0x3). */
        moq_session_on_control_bytes(c, ok_wire, ok_len, 2000);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_event_t cev;
        moq_session_poll_events(c, &cev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)cev.kind, (int)MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK_EQ_INT((int)cev.u.closed.code, 0x3);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE for FETCH → NOT_SUPPORTED, no close ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        fcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 5; fcfg.end_object = 0;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 1000, &fh) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 2, 0, NULL, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 2000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_buf_reader_t cr;
                moq_buf_reader_init(&cr, acts[i].u.send_control.data,
                    acts[i].u.send_control.len);
                moq_control_envelope_t env;
                if (moq_control_decode_envelope(&cr, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D16_REQUEST_ERROR)
                    found_err = true;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* ============================================================== */
    /* Joining FETCH                                                   */
    /* ============================================================== */

    /* == Relative joining fetch: request → accept → OK =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        asub.has_largest = true;
        asub.largest_group = 10;
        asub.largest_object = 5;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub, &asub, 1000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Client sends relative joining fetch: 3 groups back. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = true;
        fcfg.joining_sub = sub;
        fcfg.joining_start = 3;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 2000, &fh) == MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);

        /* Server receives FETCH_REQUEST with calculated range. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK(ev.u.fetch_request.start_group == 7);
        MOQ_TEST_CHECK(ev.u.fetch_request.start_object == 0);
        MOQ_TEST_CHECK(ev.u.fetch_request.end_group == 10);
        MOQ_TEST_CHECK(ev.u.fetch_request.end_object == 6);
        MOQ_TEST_CHECK(moq_subscription_is_valid(
            ev.u.fetch_request.joining_sub));
        moq_fetch_t sv_fh = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);

        /* Server accepts. */
        moq_accept_fetch_cfg_t afcfg; moq_accept_fetch_cfg_init(&afcfg);
        afcfg.end_group = 10; afcfg.end_object = 6;
        MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sv_fh, &afcfg, 3000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 3000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_OK);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch on a reused pending slot rejects stale largest == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        /* One subscription slot, so subscription #2 reuses #1's freed slot. */
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        /* Subscription #1: established with an attacker-chosen largest. */
        moq_subscription_t sub1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub1) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub1 = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub1; moq_accept_subscribe_cfg_init(&asub1);
        asub1.has_largest = true;
        asub1.largest_group = 10;
        asub1.largest_object = 5;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub1, &asub1, 1000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Free subscription #1 so its slot (and stale largest) is reusable. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, sub1, 1500) == MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Subscription #2 reuses the freed slot and stays PENDING (not
         * accepted), so it has no current largest of its own. */
        moq_subscription_t sub2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 2000, &sub2) == MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* A joining fetch against the pending, reused subscription must be
         * rejected locally -- it must not inherit subscription #1's stale
         * largest_group and emit a FETCH with a fabricated range. */
        moq_fetch_cfg_t jfcfg; moq_fetch_cfg_init(&jfcfg);
        jfcfg.is_joining = true;
        jfcfg.joining_relative = true;
        jfcfg.joining_sub = sub2;
        jfcfg.joining_start = 3;
        moq_fetch_t jfh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &jfcfg, 2500, &jfh) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        /* No FETCH was emitted by the rejected joining fetch. */
        { moq_action_t a; size_t na = 0;
          while (moq_session_poll_actions(c, &a, 1) > 0) {
              na++; moq_action_cleanup(&a);
          }
          MOQ_TEST_CHECK(na == 0); }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Absolute joining fetch ====================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        asub.has_largest = true;
        asub.largest_group = 20;
        asub.largest_object = 3;
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Absolute joining: start at group 5. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = false;
        fcfg.joining_sub = sub;
        fcfg.joining_start = 5;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 2000, &fh) == MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK(ev.u.fetch_request.start_group == 5);
        MOQ_TEST_CHECK(ev.u.fetch_request.start_object == 0);
        MOQ_TEST_CHECK(ev.u.fetch_request.end_group == 20);
        MOQ_TEST_CHECK(ev.u.fetch_request.end_object == 4);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch: invalid joining request ID → error =========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Inject a joining fetch wire message with bogus existing_request_id. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_fetch_t fetch = {
            .request_id = 0,
            .fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN,
            .joining_request_id = 99,
            .joining_start = 5,
        };
        moq_d16_encode_fetch(&w, &fetch, NULL, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Should have queued REQUEST_ERROR(INVALID_JOINING_REQUEST_ID). */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        bool found_0x32 = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_buf_reader_t cr;
                moq_buf_reader_init(&cr, acts[i].u.send_control.data,
                    acts[i].u.send_control.len);
                moq_control_envelope_t env;
                if (moq_control_decode_envelope(&cr, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D16_REQUEST_ERROR) {
                    moq_d16_request_error_t err;
                    if (moq_d16_decode_request_error(env.payload,
                            env.payload_len, &err) == MOQ_OK)
                        found_0x32 = (err.error_code == 0x32);
                }
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_0x32);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch: no largest → client rejects locally =========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Accept without LARGEST_OBJECT. */
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Client rejects joining fetch locally (no largest stored). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = true;
        fcfg.joining_sub = sub;
        fcfg.joining_start = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 2000, &fh) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch: non-LARGEST_OBJECT filter → client rejects ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        asub.has_largest = true;
        asub.largest_group = 10;
        asub.largest_object = 5;
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = true;
        fcfg.joining_sub = sub;
        fcfg.joining_start = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 2000, &fh) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch: absolute start > largest → INVALID_RANGE ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        asub.has_largest = true;
        asub.largest_group = 10;
        asub.largest_object = 5;
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = false;
        fcfg.joining_sub = sub;
        fcfg.joining_start = 99;
        moq_fetch_t fh;
        moq_session_fetch(c, &fcfg, 2000, &fh);
        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_ERROR);
        MOQ_TEST_CHECK(ev.u.fetch_error.error_code ==
            MOQ_REQUEST_ERROR_INVALID_RANGE);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Joining fetch: stale subscription handle → STALE_HANDLE ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscription_t bad = { 0xDEADBEEF };
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true;
        fcfg.joining_relative = true;
        fcfg.joining_sub = bad;
        fcfg.joining_start = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(c, &fcfg, 1000, &fh) ==
            MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Namespace parts survive decode boundary (ASan regression) ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("org"),
            MOQ_BYTES_LITERAL("example"),
            MOQ_BYTES_LITERAL("data"),
        };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = (moq_namespace_t){ ns_parts, 3 };
        fcfg.track_name = MOQ_BYTES_LITERAL("audio");
        fcfg.end_group = 1;

        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.track_namespace.count, 3);
        MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.track_namespace.parts[0].data,
            "org", 3) == 0);
        MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.track_namespace.parts[1].data,
            "example", 7) == 0);
        MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.track_namespace.parts[2].data,
            "data", 4) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == FETCH with auth token reaches server event ==================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;
        uint8_t tok_val[] = { 0xBE, 0xEF };
        moq_auth_token_t tok = {
            .token_type = 7,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        fcfg.auth_tokens = &tok;
        fcfg.auth_token_count = 1;

        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_fetch(c, &fcfg, 0, &fh), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.tokens[0].token_type, 7);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.tokens[0].token_value.len, 2);
        MOQ_TEST_CHECK(ev.u.fetch_request.tokens[0].token_value.data[0] == 0xBE);
        MOQ_TEST_CHECK(ev.u.fetch_request.tokens[0].token_value.data[1] == 0xEF);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    MOQ_TEST_PASS("test_session_fetch");
    return failures ? 1 : 0;
}
