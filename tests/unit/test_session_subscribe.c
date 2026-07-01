#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <moq/control_d18.h>
#ifdef MOQ_TEST_SIM
#include <moq/sim.h>
#endif

int main(void)
{
    int failures = 0;
    /* ============================================================== */
    /* SUBSCRIBE / SUBSCRIBE_OK                                       */
    /* ============================================================== */

    /* -- Happy path: subscribe + accept ------------------------------ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

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

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Drain setup events. */
        moq_event_t drain_ev;
        moq_session_poll_events(c, &drain_ev, 1);
        moq_session_poll_events(sv, &drain_ev, 1);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        MOQ_TEST_CHECK(moq_subscription_is_valid(sub_handle));

        /* Pump SUBSCRIBE to server. */
        pump_actions_to_peer(c, sv, 1000);

        /* Server receives SUBSCRIBE_REQUEST event. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.track_namespace.count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.track_name.len == 5);
        MOQ_TEST_CHECK(ev.u.subscribe_request.filter ==
                        MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);

        /* Server accepts. */
        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        accept.has_largest = true;
        accept.largest_group = 5;
        accept.largest_object = 42;

        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &accept, 1000) == MOQ_OK);

        /* Pump SUBSCRIBE_OK to client. */
        pump_actions_to_peer(sv, c, 1000);

        /* Client receives SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.has_largest == true);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.largest_group == 5);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.largest_object == 42);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.track_alias > 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Request blocked: MAX_REQUEST_ID=0 --------------------------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        /* Server does NOT send MAX_REQUEST_ID → default 0 → no requests. */

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain_ev;
        moq_session_poll_events(c, &drain_ev, 1);
        moq_session_poll_events(sv, &drain_ev, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &h) == MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(!moq_subscription_is_valid(h));

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Rejection path with retry translation ----------------------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

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

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain_ev;
        moq_session_poll_events(c, &drain_ev, 1);
        moq_session_poll_events(sv, &drain_ev, 1);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);

        /* Server rejects with retry. */
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        reject_sub(sv, ev.u.subscribe_request.sub,
            MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("not found"),
            true, 500, 1000);

        pump_actions_to_peer(sv, c, 1000);

        /* Client receives SUBSCRIBE_ERROR. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        MOQ_TEST_CHECK(ev.u.subscribe_error.error_code ==
                        MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
        MOQ_TEST_CHECK(ev.u.subscribe_error.can_retry == true);
        MOQ_TEST_CHECK(ev.u.subscribe_error.retry_after_ms == 500);
        MOQ_TEST_CHECK(ev.u.subscribe_error.reason.len == 9);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Unknown message param in SUBSCRIBE → PROTOCOL_VIOLATION ----- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        /* Generate a CLIENT_SETUP to establish the server. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain_ev;
        moq_session_poll_events(sv, &drain_ev, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Craft SUBSCRIBE with unknown param type 0xFF. */
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };
        uint8_t unk_val[] = { 0x00 };
        moq_kvp_entry_t params[1] = {{
            .type = 0xFF, .value = unk_val, .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("t"),
                                  params, 1);

        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Cross-session handle misuse (session tag regression) -------- */
    /*
     * Same now_us, same perspective, same slot, same generation.
     * Difference is ONLY the session tag (derived from allocation address).
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Two server sessions, SAME now_us=0, SAME perspective. */
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;

        moq_session_t *sA = NULL, *sB = NULL;
        moq_session_create(&scfg, 0, &sA);
        moq_session_create(&scfg, 0, &sB);

        /* Establish both with separate client peers. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;

        moq_session_t *cA = NULL, *cB = NULL;
        moq_session_create(&ccfg, 0, &cA);
        moq_session_create(&ccfg, 0, &cB);

        moq_session_start(cA, 0);
        pump_actions_to_peer(cA, sA, 0);
        pump_actions_to_peer(sA, cA, 0);
        moq_session_start(cB, 0);
        pump_actions_to_peer(cB, sB, 0);
        pump_actions_to_peer(sB, cB, 0);

        moq_event_t drain;
        moq_session_poll_events(sA, &drain, 1);
        moq_session_poll_events(sB, &drain, 1);
        moq_session_poll_events(cA, &drain, 1);
        moq_session_poll_events(cB, &drain, 1);

        /* Subscribe client A → server A pending sub in slot 0. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("y");

        moq_subscription_t hA_client;
        moq_session_subscribe(cA, &sub_cfg, 0, &hA_client);
        pump_actions_to_peer(cA, sA, 0);
        moq_event_t evA;
        moq_session_poll_events(sA, &evA, 1);
        MOQ_TEST_CHECK(evA.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        /* Subscribe client B → server B pending sub in slot 0. */
        moq_subscription_t hB_client;
        moq_session_subscribe(cB, &sub_cfg, 0, &hB_client);
        pump_actions_to_peer(cB, sB, 0);
        moq_event_t evB;
        moq_session_poll_events(sB, &evB, 1);
        MOQ_TEST_CHECK(evB.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        /* Verify handles have different session tags. */
        MOQ_TEST_CHECK(moq_handle_session_tag(
            evA.u.subscribe_request.sub._opaque) !=
            moq_handle_session_tag(
            evB.u.subscribe_request.sub._opaque));

        /* Server A handle used against server B → STALE_HANDLE. */
        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sB,
            evA.u.subscribe_request.sub, &accept, 0) ==
            MOQ_ERR_STALE_HANDLE);

        /* Correct accepts work. */
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sA,
            evA.u.subscribe_request.sub, &accept, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sB,
            evB.u.subscribe_request.sub, &accept, 0) == MOQ_OK);

        moq_session_destroy(cA);
        moq_session_destroy(sA);
        moq_session_destroy(cB);
        moq_session_destroy(sB);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Subscribe struct_size too small (Fix 3 regression) ----------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.struct_size = 4;
        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(s, &sub_cfg, 0, &h) ==
                        MOQ_ERR_INVAL);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Duplicate track identity ["ab"]+"c" vs ["a"]+"bc" (Fix 4) --- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

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

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain;
        moq_session_poll_events(c, &drain, 1);
        moq_session_poll_events(sv, &drain, 1);

        /* Subscribe to ["ab"] + "c". */
        moq_bytes_t ns1_parts[] = { MOQ_BYTES_LITERAL("ab") };
        moq_namespace_t ns1 = { ns1_parts, 1 };
        moq_subscribe_cfg_t sub1;
        moq_subscribe_cfg_init(&sub1);
        sub1.track_namespace = ns1;
        sub1.track_name = MOQ_BYTES_LITERAL("c");

        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub1, 0, &h1) == MOQ_OK);

        /* Subscribe to ["a"] + "bc" — must succeed (different track). */
        moq_bytes_t ns2_parts[] = { MOQ_BYTES_LITERAL("a") };
        moq_namespace_t ns2 = { ns2_parts, 1 };
        moq_subscribe_cfg_t sub2;
        moq_subscribe_cfg_init(&sub2);
        sub2.track_namespace = ns2;
        sub2.track_name = MOQ_BYTES_LITERAL("bc");

        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub2, 0, &h2) == MOQ_OK);
        MOQ_TEST_CHECK(!moq_subscription_eq(h1, h2));

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Staged commit: send buffer too small for SUBSCRIBE ---------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        ccfg.send_buffer_size = 16;

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

        /* Any SUBSCRIBE is > 16 bytes. */
        uint8_t big_name[10];
        memset(big_name, 'z', sizeof(big_name));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = (moq_bytes_t){ big_name, sizeof(big_name) };

        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(c, &sub_cfg, 0, &h);
        MOQ_TEST_CHECK(rc == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(!moq_subscription_is_valid(h));
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- accept with bad extensions does not advance alias ------------ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

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
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* Accept with track_properties.len>0 but data=NULL → ERR_INVAL. */
        moq_accept_subscribe_cfg_t bad_accept;
        moq_accept_subscribe_cfg_init(&bad_accept);
        bad_accept.track_properties.len = 5;
        bad_accept.track_properties.data = NULL;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &bad_accept, 0) == MOQ_ERR_INVAL);

        /* Valid accept gets alias 1 (not 2, proving bad accept didn't advance). */
        moq_accept_subscribe_cfg_t good_accept;
        moq_accept_subscribe_cfg_init(&good_accept);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &good_accept, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.track_alias == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- reject with reason.len>0, data=NULL → ERR_INVAL ------------ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

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
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        moq_bytes_t bad_reason = { .data = NULL, .len = 5 };
        MOQ_TEST_CHECK(reject_sub(sv,
            ev.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            bad_reason, false, 0, 0) == MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Invalid cfg + no credit → ERR_INVAL, not REQUESTS_BLOCKED -- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        /* No MAX_REQUEST_ID → no credit. */

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t drain;
        moq_session_poll_events(c, &drain, 1);

        /* Bad cfg: namespace count = 0. Should fail with INVAL before
         * reaching credit check / REQUESTS_BLOCKED side effect. */
        moq_subscribe_cfg_t bad_cfg;
        moq_subscribe_cfg_init(&bad_cfg);
        bad_cfg.track_namespace.count = 0;

        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &bad_cfg, 0, &h) ==
                        MOQ_ERR_INVAL);

        /* No REQUESTS_BLOCKED should have been queued. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(c, &act, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* ============================================================== */
    /* ADVERSARIAL SUBSCRIBE MATRIX                                    */
    /* ============================================================== */

    /* == 1. Request ID accounting ==================================== */

    /* -- No local MAX_REQUEST_ID → TOO_MANY_REQUESTS (0x7) ----------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 0, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x7);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Wrong parity → INVALID_REQUEST_ID (0x4) --------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 1, "ns", "t", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x4);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Non-next request ID → INVALID_REQUEST_ID (0x4) -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 2, "ns", "t", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x4);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Valid below-max + exactly-at-max → first succeeds, second closes */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 2, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.track_name.len == 2);

        feed_subscribe(sv, 2, "ns", "t2", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x7);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUESTS_BLOCKED decode + suppression ----------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 0, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h) ==
                        MOQ_ERR_REQUEST_BLOCKED);

        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(c, &act, 1) == 1);
        MOQ_TEST_CHECK(decode_action_msg_type(&act) == MOQ_D16_REQUESTS_BLOCKED);

        /* Suppression: second attempt produces no action. */
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h) ==
                        MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(moq_session_poll_actions(c, &act, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 2. Message parameter validation ============================= */

    /* -- Duplicate known SUBSCRIBE param → 0x3 ----------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t v[1] = {0x01};
        moq_kvp_entry_t params[2] = {
            { MOQ_MSG_PARAM_FORWARD, .value = v, .value_len = 1,
              .is_varint = true },
            { MOQ_MSG_PARAM_FORWARD, .value = v, .value_len = 1,
              .is_varint = true },
        };
        feed_subscribe(sv, 0, "ns", "t", params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Repeated AUTH_TOKEN tolerated + event delivered -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Two USE_VALUE tokens with different types. */
        moq_d16_auth_token_t tok1 = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 1,
            .token_value = (const uint8_t *)"a",
            .token_value_len = 1,
        };
        moq_d16_auth_token_t tok2 = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 2,
            .token_value = (const uint8_t *)"b",
            .token_value_len = 1,
        };
        uint8_t tb1[32], tb2[32];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &tok1);
        moq_d16_auth_token_encode(&tw2, &tok2);
        moq_kvp_entry_t params[2] = {
            { MOQ_MSG_PARAM_AUTHORIZATION_TOKEN, .value = tb1,
              .value_len = moq_buf_writer_offset(&tw1), .is_varint = false },
            { MOQ_MSG_PARAM_AUTHORIZATION_TOKEN, .value = tb2,
              .value_len = moq_buf_writer_offset(&tw2), .is_varint = false },
        };
        feed_subscribe(sv, 0, "ns", "t", params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 2);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Malformed FORWARD (value=2) → 0x3 ----------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        uint8_t bad[] = {0x02};
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD, .value = bad, .value_len = 1,
            .is_varint = true,
        }};
        feed_subscribe(sv, 0, "ns", "t", p, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Malformed GROUP_ORDER (value=0) → 0x3 ----------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        uint8_t bad[] = {0x00};
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_GROUP_ORDER, .value = bad, .value_len = 1,
            .is_varint = true,
        }};
        feed_subscribe(sv, 0, "ns", "t", p, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- DELIVERY_TIMEOUT=0 → 0x3 ------------------------------------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        uint8_t zero[] = {0x00};
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT, .value = zero,
            .value_len = 1, .is_varint = true,
        }};
        feed_subscribe(sv, 0, "ns", "t", p, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Unknown SUBSCRIBE_OK param → 0x3 (hand-built payload) ------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);

        /* Hand-build: req_id=0, alias=1, 1 param, type 0xFF (odd,
         * delta=0xFF → 2-byte varint 0x40FF), value_len=1, value=0x42. */
        uint8_t payload[] = {
            0x00,               /* request_id = 0 */
            0x01,               /* track_alias = 1 */
            0x01,               /* param count = 1 */
            0x40, 0xFF,         /* delta type = 0xFF (2-byte varint) */
            0x01,               /* value length = 1 */
            0x42,               /* value */
        };
        moq_result_t rc = feed_raw_subscribe_ok(c, payload, sizeof(payload));
        (void)rc;
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Duplicate SUBSCRIBE_OK param → 0x3 (hand-built payload) ----- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);

        /* Hand-build: req_id=0, alias=1, 2 params, both EXPIRES (0x08).
         * First: delta=0x08, varint value 100.
         * Second: delta=0x00 (same type again), varint value 200. */
        uint8_t payload[] = {
            0x00,               /* request_id */
            0x01,               /* track_alias */
            0x02,               /* param count = 2 */
            0x08,               /* delta = 0x08 → type 0x08 = EXPIRES */
            0x40, 0x64,         /* varint value = 100 */
            0x00,               /* delta = 0 → type still 0x08 */
            0x40, 0xC8,         /* varint value = 200 */
        };
        moq_result_t rc = feed_raw_subscribe_ok(c, payload, sizeof(payload));
        (void)rc;
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 3. Track identity and aliasing ============================== */

    /* -- Duplicate outgoing same track → ERR_INVAL ------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t h1, h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) == MOQ_ERR_INVAL);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Duplicate incoming → REQUEST_ERROR DUPLICATE_SUBSCRIPTION ---- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        feed_subscribe(sv, 2, "ns", "t", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(decode_action_request_error(&act, &err) == MOQ_OK);
        MOQ_TEST_CHECK(err.error_code == 0x19);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Duplicate track alias (raw wire) → client closes 0x5 -------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        moq_session_subscribe(c, &sub_cfg, 0, &h1);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = 42;
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        moq_session_subscribe(c, &sub_cfg, 0, &h2);
        feed_subscribe_ok(c, 2, 42, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x5);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Explicit alias collision → ERR_INVAL, then different works -- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t hc;
        moq_session_subscribe(c, &sub_cfg, 0, &hc);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = 42;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0) == MOQ_OK);

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_session_subscribe(c, &sub_cfg, 0, &hc);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);

        moq_accept_subscribe_cfg_t bad;
        moq_accept_subscribe_cfg_init(&bad);
        bad.has_track_alias = true;
        bad.track_alias = 42;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &bad, 0) == MOQ_ERR_INVAL);

        /* Fix with different alias → succeeds. */
        moq_accept_subscribe_cfg_t good;
        moq_accept_subscribe_cfg_init(&good);
        good.has_track_alias = true;
        good.track_alias = 99;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &good, 0) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Pool-full incoming → REQUEST_ERROR with INTERNAL_ERROR ------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
        extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &extra);

        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        feed_subscribe(sv, 2, "ns", "t2", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(decode_action_request_error(&act, &err) == MOQ_OK);
        MOQ_TEST_CHECK(err.error_code == 0x0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4. Pool and buffer limits =================================== */

    /* -- max_subscriptions=1: second outgoing blocked ----------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
        extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h1) == MOQ_OK);

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) ==
                        MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_subscription_is_valid(h2));
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- max_subscriptions=1: pool recovery after reject -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
        extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &extra, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

        /* A fills the single slot. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("a");
        moq_subscription_t ha;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &ha) == MOQ_OK);

        /* B blocked — pool full. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("b");
        moq_subscription_t hb;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &hb) ==
                        MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_subscription_is_valid(hb));

        /* Deliver A to server, server rejects it. */
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(reject_sub(sv, sv_sub,
            MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        /* Client receives SUBSCRIBE_ERROR — slot freed. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        moq_event_cleanup(&ev);

        /* Retry B — should succeed now. */
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &hb) == MOQ_OK);
        MOQ_TEST_CHECK(moq_subscription_is_valid(hb));
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        pump_actions_to_peer(c, sv, 0);
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- send_buffer too small for accept (oversized extensions) ------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.send_buffer_size = 64;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        uint8_t big_ext[128];
        memset(big_ext, 0xAA, sizeof(big_ext));
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.track_properties.data = big_ext;
        acc.track_properties.len = sizeof(big_ext);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Retry with small extensions → succeeds, proving no commit. */
        moq_accept_subscribe_cfg_t small_acc;
        moq_accept_subscribe_cfg_init(&small_acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &small_acc, 0) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- send_buffer too small for reject (oversized reason) ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.send_buffer_size = 64;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        uint8_t big_reason[128];
        memset(big_reason, 'x', sizeof(big_reason));
        moq_bytes_t reason = { big_reason, sizeof(big_reason) };
        MOQ_TEST_CHECK(reject_sub(sv,
            ev.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            reason, false, 0, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Retry with short reason → succeeds, proving no commit. */
        MOQ_TEST_CHECK(reject_sub(sv,
            ev.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 5. Borrow/lifetime ========================================== */

    /* -- SUBSCRIBE_REQUEST borrow invalidated by tick() -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        uint64_t epoch = ev.borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(sv, epoch));
        moq_session_tick(sv, 1);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(sv, epoch));
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBSCRIBE_OK extensions borrow valid until next call -------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        const uint8_t ext[] = {0x01, 0x02, 0x03};
        acc.track_properties.data = ext;
        acc.track_properties.len = 3;
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        uint64_t epoch = ev.borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(c, epoch));
        MOQ_TEST_CHECK(ev.u.subscribe_ok.track_properties.len == 3);

        moq_session_tick(c, 1);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(c, epoch));
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBSCRIBE_ERROR reason borrow valid until next call ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        moq_session_subscribe(c, &sub_cfg, 0, &h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        reject_sub(sv, ev.u.subscribe_request.sub,
            MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("gone"), false, 0, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        uint64_t epoch = ev.borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(c, epoch));
        MOQ_TEST_CHECK(ev.u.subscribe_error.reason.len == 4);

        moq_session_tick(c, 1);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(c, epoch));
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 6. Action queue full: accept ================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        /* Two incoming subscribes. */
        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        moq_event_t ev1;
        moq_session_poll_events(sv, &ev1, 1);
        MOQ_TEST_CHECK(ev1.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        feed_subscribe(sv, 2, "ns", "t2", NULL, 0);
        moq_event_t ev2;
        moq_session_poll_events(sv, &ev2, 1);
        MOQ_TEST_CHECK(ev2.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        /* Accept first → fills action queue (cap=1). */
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev1.u.subscribe_request.sub, &acc, 0) == MOQ_OK);

        /* Accept second → WOULD_BLOCK (queue full). */
        moq_accept_subscribe_cfg_t acc2;
        moq_accept_subscribe_cfg_init(&acc2);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev2.u.subscribe_request.sub, &acc2, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Drain first SUBSCRIBE_OK and assert alias=1. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        moq_d16_subscribe_ok_t ok1 = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(decode_action_subscribe_ok(&act, &ok1) == MOQ_OK);
        MOQ_TEST_CHECK(ok1.track_alias == 1);

        /* Retry → succeeds, proving sub 2 still pending. */
        moq_accept_subscribe_cfg_t acc3;
        moq_accept_subscribe_cfg_init(&acc3);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            ev2.u.subscribe_request.sub, &acc3, 0) == MOQ_OK);

        /* Second SUBSCRIBE_OK should have alias=2 (failed attempt didn't skip). */
        moq_action_t act2;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act2, 1) == 1);
        moq_d16_subscribe_ok_t ok2 = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(decode_action_subscribe_ok(&act2, &ok2) == MOQ_OK);
        MOQ_TEST_CHECK(ok2.track_alias == 2);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 7. Action queue full: reject ================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        moq_event_t ev1;
        moq_session_poll_events(sv, &ev1, 1);

        feed_subscribe(sv, 2, "ns", "t2", NULL, 0);
        moq_event_t ev2;
        moq_session_poll_events(sv, &ev2, 1);

        /* Reject first → fills queue. */
        MOQ_TEST_CHECK(reject_sub(sv,
            ev1.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_OK);

        /* Reject second → WOULD_BLOCK. */
        MOQ_TEST_CHECK(reject_sub(sv,
            ev2.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Drain first REQUEST_ERROR. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(decode_action_request_error(&act, &err) == MOQ_OK);
        MOQ_TEST_CHECK(err.error_code == MOQ_REQUEST_ERROR_DOES_NOT_EXIST);

        /* Retry → succeeds, proving sub 2 still pending. */
        MOQ_TEST_CHECK(reject_sub(sv,
            ev2.u.subscribe_request.sub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_OK);

        /* Decode second REQUEST_ERROR. */
        moq_action_t act2;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act2, 1) == 1);
        moq_d16_request_error_t err2;
        MOQ_TEST_CHECK(decode_action_request_error(&act2, &err2) == MOQ_OK);
        MOQ_TEST_CHECK(err2.error_code == MOQ_REQUEST_ERROR_DOES_NOT_EXIST);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 8. Failed REQUESTS_BLOCKED queueing ========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 2, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");

        /* First subscribe succeeds → fills action queue (cap=1). */
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h1) == MOQ_OK);

        /* Second subscribe: next_local_request_id=2 >= max=2 → blocked.
         * REQUESTS_BLOCKED cannot queue because action queue full. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) ==
                        MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(!moq_subscription_is_valid(h2));

        /* Drain the SUBSCRIBE action. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(c, &act, 1) == 1);
        MOQ_TEST_CHECK(decode_action_msg_type(&act) == MOQ_D16_SUBSCRIBE);

        /* Retry blocked subscribe → REQUESTS_BLOCKED now queued. */
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) ==
                        MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(moq_session_poll_actions(c, &act, 1) == 1);
        MOQ_TEST_CHECK(decode_action_msg_type(&act) == MOQ_D16_REQUESTS_BLOCKED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 9. Output scratch too small for SUBSCRIBE_REQUEST ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.output_scratch_size = 4;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        /* Feed SUBSCRIBE with namespace "ns" + name "track" — needs
         * scratch for parts array + field bytes + name bytes > 4.
         * Use feed_subscribe which calls on_control_bytes internally;
         * capture the result by calling on_control_bytes directly. */
        uint8_t sub_buf[256];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub_buf, sizeof(sub_buf));
        moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_encode_subscribe(&sw, 0, &ns, moq_bytes_cstr("track"),
                                  NULL, 0);
        /* Scratch is permanently too small (4 bytes, scratch was empty) →
         * session closes with INTERNAL_ERROR rather than wedging. */
        moq_result_t rc = moq_session_on_control_bytes(sv, sub_buf,
            moq_buf_writer_offset(&sw), 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 10. MAX_REQUEST_ID handling =================================== */

    /* -- Monotonic increase updates credit ----------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft MAX_REQUEST_ID=20 from server to client. */
        uint8_t mr_buf[32];
        moq_buf_writer_t mrw;
        moq_buf_writer_init(&mrw, mr_buf, sizeof(mr_buf));
        moq_d16_encode_varint_msg(&mrw, MOQ_D16_MAX_REQUEST_ID, 20);
        moq_session_on_control_bytes(c, mr_buf, moq_buf_writer_offset(&mrw), 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 20);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Equal/smaller closes with PROTOCOL_VIOLATION ----------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Send MAX_REQUEST_ID=5 (less than setup's 10). */
        uint8_t mr_buf[32];
        moq_buf_writer_t mrw;
        moq_buf_writer_init(&mrw, mr_buf, sizeof(mr_buf));
        moq_d16_encode_varint_msg(&mrw, MOQ_D16_MAX_REQUEST_ID, 5);
        moq_session_on_control_bytes(c, mr_buf, moq_buf_writer_offset(&mrw), 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Prior blocked + credit increase → REQUEST_READY -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        /* Peer max=2: allows request 0 only. */
        establish_pair(&alloc, 10, 2, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;

        /* First subscribe uses request ID 0 → success. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h1) == MOQ_OK);

        /* Second is blocked (next=2 >= max=2). */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) ==
                        MOQ_ERR_REQUEST_BLOCKED);

        /* Drain SUBSCRIBE + REQUESTS_BLOCKED actions. */
        moq_action_t acts[4];
        moq_session_poll_actions(c, acts, 4);

        /* Peer raises MAX_REQUEST_ID to 10. */
        uint8_t mr_buf[32];
        moq_buf_writer_t mrw;
        moq_buf_writer_init(&mrw, mr_buf, sizeof(mr_buf));
        moq_d16_encode_varint_msg(&mrw, MOQ_D16_MAX_REQUEST_ID, 10);
        moq_session_on_control_bytes(c, mr_buf, moq_buf_writer_offset(&mrw), 0);

        /* Should emit REQUEST_READY. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);
        MOQ_TEST_CHECK(ev.u.request_ready.available_requests > 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Credit increase without prior block → no event --------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Raise from 10 to 20 without any subscribe call. */
        uint8_t mr_buf[32];
        moq_buf_writer_t mrw;
        moq_buf_writer_init(&mrw, mr_buf, sizeof(mr_buf));
        moq_d16_encode_varint_msg(&mrw, MOQ_D16_MAX_REQUEST_ID, 20);
        moq_session_on_control_bytes(c, mr_buf, moq_buf_writer_offset(&mrw), 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 20);

        /* No event emitted. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_READY event-queue-full retry ------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Manual setup to keep SETUP_COMPLETE undrained. */
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
        scfg.initial_request_capacity = 2;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        /* Do NOT drain client events — SETUP_COMPLETE fills queue (1/1). */
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        /* First subscribe succeeds (request 0 < max 2). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        moq_session_subscribe(c, &sub_cfg, 0, &h1);

        /* Second subscribe blocked (request 2 >= max 2). */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &h2) ==
                        MOQ_ERR_REQUEST_BLOCKED);

        /* Drain SUBSCRIBE + REQUESTS_BLOCKED actions. */
        moq_action_t acts[4];
        moq_session_poll_actions(c, acts, 4);

        /* MAX_REQUEST_ID=10 arrives → would emit REQUEST_READY
         * but event queue is full (SETUP_COMPLETE still there). */
        uint8_t mr_buf[32];
        moq_buf_writer_t mrw;
        moq_buf_writer_init(&mrw, mr_buf, sizeof(mr_buf));
        moq_d16_encode_varint_msg(&mrw, MOQ_D16_MAX_REQUEST_ID, 10);
        moq_result_t rc = moq_session_on_control_bytes(c, mr_buf,
            moq_buf_writer_offset(&mrw), 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 2);

        /* Drain SETUP_COMPLETE. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);

        /* Retry → credit committed + REQUEST_READY emitted. */
        MOQ_TEST_CHECK(moq_session_process_pending(c, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 10);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }


    /* -- UNSUBSCRIBE: basic established subscription terminated ------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Client subscribes, server accepts. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Feed UNSUBSCRIBE to server. */
        uint8_t ubuf[16];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0) == MOQ_OK);

        /* Server emits UNSUBSCRIBED, stays open. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- UNSUBSCRIBE: stale handle after unsubscribe ------------------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        uint8_t ubuf[16]; moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        moq_session_on_control_bytes(sv, ubuf, moq_buf_writer_offset(&uw), 0);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Old handle is stale — open_subgroup should fail. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg)
            == MOQ_ERR_STALE_HANDLE);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- UNSUBSCRIBE: unknown request_id closes session --------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        uint8_t ubuf[16]; moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 42);
        moq_session_on_control_bytes(sv, ubuf, moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_SESSION_CLOSED)
                      MOQ_TEST_CHECK(evts[i].u.closed.code == 0x4);
                  moq_event_cleanup(&evts[i]);
              }
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- UNSUBSCRIBE: open subgroup queues RESET_DATA ----------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Open subgroup. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg)
            == MOQ_OK);
        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Feed UNSUBSCRIBE. */
        uint8_t ubuf[16]; moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Should have RESET_DATA action + UNSUBSCRIBED event. */
        bool found_reset = false;
        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) {
                  if (acts[i].kind == MOQ_ACTION_RESET_DATA) {
                      MOQ_TEST_CHECK(acts[i].u.reset_data.error_code == 0x1);
                      found_reset = true;
                  }
                  moq_action_cleanup(&acts[i]);
              }
        }
        MOQ_TEST_CHECK(found_reset);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- UNSUBSCRIBE: pending subscription also terminated ------------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        /* Server has SUBSCRIBE_REQUEST but hasn't accepted yet. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Feed UNSUBSCRIBE before accepting. */
        uint8_t ubuf[16]; moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- UNSUBSCRIBE: WOULD_BLOCK during subgroup reset, retry OK ---- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Open 2 subgroups, draining actions between each. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg, 0, &sg1) == MOQ_OK);
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        moq_subgroup_cfg_t sg_cfg2;
        moq_subgroup_cfg_init(&sg_cfg2);
        sg_cfg2.group_id = 1;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg2, 0, &sg2) == MOQ_OK);
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Feed UNSUBSCRIBE. With max_actions=1, first RESET_DATA fills
         * the queue; second RESET cannot be queued → WOULD_BLOCK. */
        uint8_t ubuf[16]; moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        moq_result_t rc = moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* No UNSUBSCRIBED event before retry. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* Drain exactly one RESET action. */
        {
            moq_action_t acts[4]; size_t na;
            na = moq_session_poll_actions(sv, acts, 4);
            MOQ_TEST_CHECK(na == 1);
            MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_RESET_DATA);
            MOQ_TEST_CHECK(acts[0].u.reset_data.error_code == 0x1);
            moq_action_cleanup(&acts[0]);
        }

        /* Retry via process_pending — second reset + event. */
        rc = moq_session_process_pending(sv, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* Second RESET action queued. */
        {
            moq_action_t acts[4]; size_t na;
            na = moq_session_poll_actions(sv, acts, 4);
            MOQ_TEST_CHECK(na >= 1);
            bool found_reset = false;
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA)
                    found_reset = true;
                moq_action_cleanup(&acts[i]);
            }
            MOQ_TEST_CHECK(found_reset);
        }

        /* UNSUBSCRIBED event emitted. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        /* Handle is stale. */
        moq_subgroup_handle_t sg3;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg, 0, &sg3) == MOQ_ERR_STALE_HANDLE);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(c, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: established round-trip ----------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Client unsubscribes. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server receives UNSUBSCRIBED event. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: pending (before SUBSCRIBE_OK) ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Unsubscribe before receiving SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server should see SUBSCRIBE_REQUEST then UNSUBSCRIBED. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: WOULD_BLOCK retry ---------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);

        /* Action queue has SUBSCRIBE (1/1). Unsubscribe should WOULD_BLOCK. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0)
            == MOQ_ERR_WOULD_BLOCK);
        /* Handle still valid. */
        MOQ_TEST_CHECK(moq_subscription_is_valid(csub));

        /* Drain, retry. */
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);

        pump_actions_to_peer(c, sv, 0);
        /* Drain server events. */
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: stale handle after ---------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        moq_session_unsubscribe(c, csub, 0);
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0)
            == MOQ_ERR_STALE_HANDLE);

        pump_actions_to_peer(c, sv, 0);
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: wrong role rejected -------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Server (publisher role) tries to unsubscribe — wrong role. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(sv, ssub, 0)
            == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: allowed after GOAWAY ------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Server sends GOAWAY. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);

        /* Unsubscribe still works in DRAINING. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);

        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: slot reuse after established unsub ----- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Unsubscribe established subscription. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Handle should be stale now. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0)
            == MOQ_ERR_STALE_HANDLE);

        /* New subscribe should succeed — slot must be free. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t csub2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub2)
            == MOQ_OK);

        pump_actions_to_peer(c, sv, 0);
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: tombstone full returns WOULD_BLOCK ------ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub1) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Fill tombstone ring completely. */
        while (c->unsub_tomb_count < c->unsub_tomb_cap)
            unsub_tomb_add(c, 0xDEAD);

        /* Pending unsub needs tombstone: full → WOULD_BLOCK. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub1, 0)
            == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_subscription_is_valid(csub1));

        /* Free one tombstone slot. */
        unsub_tomb_consume(c, 0xDEAD);

        /* Retry succeeds now. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub1, 0) == MOQ_OK);

        pump_actions_to_peer(c, sv, 0);
        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        /* Drain remaining synthetic tombstones. */
        while (unsub_tomb_consume(c, 0xDEAD)) {}
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: tombstone reuse with max_subscriptions=1 */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* Round 1: subscribe (request_id 0), pending unsub. */
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub1) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub1, 0) == MOQ_OK);

        /* Tombstone full (1/1). Second pending unsub would WOULD_BLOCK.
         * Drain tombstone by delivering late response for round 1. */
        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) {
                  if (evts[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                      moq_accept_subscribe_cfg_t acc;
                      moq_accept_subscribe_cfg_init(&acc);
                      moq_session_accept_subscribe(sv,
                          evts[j].u.subscribe_request.sub, &acc, 0);
                  }
                  moq_event_cleanup(&evts[j]);
              }
        }
        pump_actions_to_peer(sv, c, 0);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);
        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }

        /* Round 2: tombstone consumed, slot free. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t csub2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub2) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub2, 0) == MOQ_OK);

        /* Server accepts request_id 2. */
        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) {
                  if (evts[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                      moq_accept_subscribe_cfg_t acc;
                      moq_accept_subscribe_cfg_init(&acc);
                      moq_session_accept_subscribe(sv,
                          evts[j].u.subscribe_request.sub, &acc, 0);
                  }
                  moq_event_cleanup(&evts[j]);
              }
        }

        /* Deliver late SUBSCRIBE_OK for request_id 2. */
        pump_actions_to_peer(sv, c, 0);

        /* Client must stay open — tombstone for round 2 is present. */
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Deliver UNSUBSCRIBEs to server. */
        pump_actions_to_peer(c, sv, 0);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: 9 pending unsubs, late response OK ----- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 20, 20, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* Subscribe 9, pump each, then unsubscribe all before
         * server responds. */
        moq_subscription_t csubs[9];
        for (int i = 0; i < 9; i++) {
            char name[8];
            snprintf(name, sizeof(name), "t%d", i);
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = moq_bytes_cstr(name);
            sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0,
                &csubs[i]) == MOQ_OK);
            pump_actions_to_peer(c, sv, 0);
        }

        /* Server drains SUBSCRIBE_REQUEST events and accepts first. */
        moq_event_t evts[16]; size_t ne;
        moq_subscription_t first_ssub = {0};
        while ((ne = moq_session_poll_events(sv, evts, 16)) > 0)
            for (size_t j = 0; j < ne; j++) {
                if (evts[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                    first_ssub._opaque == 0)
                    first_ssub = evts[j].u.subscribe_request.sub;
                moq_event_cleanup(&evts[j]);
            }

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, first_ssub,
            &acc, 0) == MOQ_OK);

        /* Client unsubscribes all 9 before receiving SUBSCRIBE_OK.
         * Each pending unsub adds to the tombstone ring. */
        for (int i = 0; i < 9; i++)
            MOQ_TEST_CHECK(moq_session_unsubscribe(c, csubs[i], 0)
                == MOQ_OK);

        /* Deliver SUBSCRIBE_OK to client (late response for
         * request_id 0, which was tombstoned). */
        pump_actions_to_peer(sv, c, 0);

        /* Client must stay open — the tombstone for request_id 0
         * must not have been evicted. */
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Now deliver UNSUBSCRIBEs to server. */
        pump_actions_to_peer(c, sv, 0);

        { while ((ne = moq_session_poll_events(c, evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(sv, evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: late SUBSCRIBE_OK after unsub ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server sees SUBSCRIBE_REQUEST. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Client unsubscribes before server responds. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);

        /* Server accepts (crossed with UNSUBSCRIBE in flight). */
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0)
            == MOQ_OK);

        /* Deliver SUBSCRIBE_OK to client, then UNSUBSCRIBE to server. */
        pump_actions_to_peer(sv, c, 0);
        pump_actions_to_peer(c, sv, 0);

        /* Client must stay open — late SUBSCRIBE_OK for an
         * unsubscribed request must not crash the session. */
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Server sees UNSUBSCRIBED event. */
        bool found_unsub = false;
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_UNSUBSCRIBED)
                      found_unsub = true;
                  moq_event_cleanup(&evts[i]);
              }
        }
        MOQ_TEST_CHECK(found_unsub);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(c, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Outbound UNSUBSCRIBE: late REQUEST_ERROR after unsub --------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Client unsubscribes before server responds. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, csub, 0) == MOQ_OK);

        /* Server rejects (crossed with UNSUBSCRIBE in flight). */
        MOQ_TEST_CHECK(reject_sub(sv, ssub,
            MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0) == MOQ_OK);

        /* Deliver REQUEST_ERROR to client, then UNSUBSCRIBE to server. */
        pump_actions_to_peer(sv, c, 0);
        pump_actions_to_peer(c, sv, 0);

        /* Client must stay open. */
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          while ((ne = moq_session_poll_events(c, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE: basic priority update ------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Feed REQUEST_UPDATE with priority change. */
        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        uint8_t prio_buf[8];
        size_t prio_len = moq_quic_varint_encode(42, prio_buf, sizeof(prio_buf));
        moq_kvp_entry_t upd_params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
              .value = prio_buf, .value_len = prio_len, .is_varint = true },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&uw, 2, 0,
            upd_params, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Server emits SUBSCRIBE_UPDATED + REQUEST_OK action. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.subscriber_priority == 42);
        moq_event_cleanup(&ev);

        /* Decode outbound action: REQUEST_OK with request_id=2. */
        bool found_ok = false;
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) {
                  if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                      uint64_t mt = decode_action_msg_type(&acts[i]);
                      if (mt == MOQ_D16_REQUEST_OK) found_ok = true;
                  }
                  moq_action_cleanup(&acts[i]);
              }
        }
        MOQ_TEST_CHECK(found_ok);

        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE: unsupported param → REQUEST_ERROR ------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Send REQUEST_UPDATE with SUBSCRIPTION_FILTER (unsupported v1). */
        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        uint8_t filter_buf[8];
        size_t filter_len = moq_quic_varint_encode(1, filter_buf, sizeof(filter_buf));
        moq_kvp_entry_t upd_params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
              .value = filter_buf, .value_len = filter_len, .is_varint = false },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&uw, 2, 0,
            upd_params, 1) == MOQ_OK);
        moq_result_t upd_rc = moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(upd_rc == MOQ_OK);

        /* Session stays open — REQUEST_ERROR sent, no session close. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* No SUBSCRIBE_UPDATED event. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* Decode outbound: REQUEST_ERROR with NOT_SUPPORTED. */
        bool found_err = false;
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) {
                  if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                      uint64_t mt = decode_action_msg_type(&acts[i]);
                      if (mt == MOQ_D16_REQUEST_ERROR) {
                          moq_d16_request_error_t rerr;
                          if (decode_action_request_error(&acts[i], &rerr) == MOQ_OK) {
                              MOQ_TEST_CHECK(rerr.error_code ==
                                  MOQ_REQUEST_ERROR_NOT_SUPPORTED);
                              MOQ_TEST_CHECK(rerr.request_id == 2);
                          }
                          found_err = true;
                      }
                  }
                  moq_action_cleanup(&acts[i]);
              }
        }
        MOQ_TEST_CHECK(found_err);

        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE: malformed param closes session ----------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Send FORWARD with invalid value (2 is not 0 or 1). */
        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        uint8_t fwd_buf[8];
        size_t fwd_len = moq_quic_varint_encode(2, fwd_buf, sizeof(fwd_buf));
        moq_kvp_entry_t upd_params[] = {
            { .type = MOQ_MSG_PARAM_FORWARD,
              .value = fwd_buf, .value_len = fwd_len, .is_varint = true },
        };
        moq_d16_encode_request_update(&uw, 2, 0, upd_params, 1);
        moq_session_on_control_bytes(sv, ubuf, moq_buf_writer_offset(&uw), 0);

        /* Should close — malformed param value. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE: WOULD_BLOCK retry with max_actions=1 ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        /* Leave SUBSCRIBE_OK action in queue (1/1 full). */

        /* Feed REQUEST_UPDATE — action queue full → WOULD_BLOCK. */
        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        uint8_t prio_buf[8];
        size_t prio_len = moq_quic_varint_encode(99, prio_buf, sizeof(prio_buf));
        moq_kvp_entry_t upd_params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
              .value = prio_buf, .value_len = prio_len, .is_varint = true },
        };
        moq_d16_encode_request_update(&uw, 2, 0, upd_params, 1);
        moq_result_t upd_rc = moq_session_on_control_bytes(sv, ubuf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(upd_rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* No event yet. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* Drain action queue. */
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Retry via process_pending. */
        MOQ_TEST_CHECK(moq_session_process_pending(sv, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* SUBSCRIBE_UPDATED event emitted. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.subscriber_priority == 99);
        moq_event_cleanup(&ev);

        /* REQUEST_OK action queued — decode and verify request_id. */
        {
            moq_action_t acts[4]; size_t na;
            na = moq_session_poll_actions(sv, acts, 4);
            MOQ_TEST_CHECK(na >= 1);
            bool found_ok = false;
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                    moq_control_envelope_t env;
                    moq_buf_reader_t rr;
                    moq_buf_reader_init(&rr, acts[i].u.send_control.data,
                        acts[i].u.send_control.len);
                    if (moq_control_decode_envelope(&rr, &env) == MOQ_OK &&
                        env.msg_type == MOQ_D16_REQUEST_OK) {
                        moq_kvp_entry_t ok_params[4];
                        moq_d16_request_ok_t ok = {
                            .params = ok_params, .params_cap = 4
                        };
                        if (moq_d16_decode_request_ok(env.payload,
                                env.payload_len, &ok) == MOQ_OK)
                            MOQ_TEST_CHECK(ok.request_id == 2);
                        found_ok = true;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
            MOQ_TEST_CHECK(found_ok);
        }

        /* Next REQUEST_UPDATE with request_id=4 should work. */
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE: unknown existing_request_id → close ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        moq_d16_encode_request_update(&uw, 0, 99, NULL, 0);
        moq_session_on_control_bytes(sv, ubuf, moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Index: multiple subscriptions resolved by request_id ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_subscriptions = 8;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &cextra, NULL);

        /* Client creates 4 outstanding subscriptions. */
        moq_subscription_t subs[4];
        for (int i = 0; i < 4; i++) {
            char name[16];
            snprintf(name, sizeof(name), "idx%d", i);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t cfg;
            moq_subscribe_cfg_init(&cfg);
            cfg.track_namespace = ns;
            cfg.track_name = moq_bytes_cstr(name);
            MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 0, &subs[i]) == MOQ_OK);
        }
        pump_actions_to_peer(c, sv, 0);

        /* Accept all 4 on the server, pump SUBSCRIBE_OK back. Each
         * SUBSCRIBE_OK routes through the request_id index. */
        moq_event_t ev;
        for (int i = 0; i < 4; i++) {
            MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            acc.has_track_alias = true;
            acc.track_alias = (uint64_t)(100 + i);
            moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub,
                &acc, 0);
            moq_event_cleanup(&ev);
        }
        pump_actions_to_peer(sv, c, 0);

        /* Client should get 4 SUBSCRIBE_OK events — each found via
         * request_id index lookup. */
        int ok_count = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok_count++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok_count == 4);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Index: free + reuse subscription slot ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Subscribe → reject → subscribe again (pool recovery). */
        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        reject_sub(sv, ssub, MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
            MOQ_BYTES_LITERAL("no"), false, 0, 0);

        /* Drain to clear the reject action. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Second subscribe should reuse the freed slot. */
        feed_subscribe(sv, 2, "ns", "t2", NULL, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- prepare/commit: WOULD_BLOCK does not advance request ID ------ */
    {
        /* Configure server with max_events=1 so the event queue fills
         * after one subscribe event, forcing WOULD_BLOCK on the second
         * inbound subscribe. The second subscribe must use the same
         * peer request ID after retry. */
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Client subscribes twice. Both SUBSCRIBE messages queued. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 100, &h1) == MOQ_OK);

        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 100, &h2) == MOQ_OK);

        /* Pump both SUBSCRIBEs to server. */
        pump_actions_to_peer(c, sv, 100);

        /* Server: first subscribe event fills the 1-slot queue. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Second subscribe should have returned WOULD_BLOCK internally
         * and the peer_next_request_id should NOT have advanced.
         * Re-process pending → this time the event queue has room. */
        MOQ_TEST_CHECK(moq_session_process_pending(sv, 100) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Session is still active (not closed). */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- outbound: WOULD_BLOCK does not advance local request ID ------ */
    {
        /* Configure client with max_actions=1 so the action queue fills
         * after the first subscribe's SEND_CONTROL, forcing WOULD_BLOCK
         * on the second outbound subscribe. The second subscribe must
         * succeed after draining, using request_id 2 (not 4). */
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        /* First subscribe succeeds (begin_advance resets the empty queue). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 200, &h1) == MOQ_OK);

        /* Second subscribe at the SAME now_us: action queue is full,
         * so it should return WOULD_BLOCK. Use same now_us to prevent
         * begin_advance from resetting the queue. */
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t h2;
        moq_result_t rc2 = moq_session_subscribe(c, &sub_cfg, 200, &h2);
        MOQ_TEST_CHECK(rc2 == MOQ_ERR_WOULD_BLOCK);

        /* Drain the action queue. */
        pump_actions_to_peer(c, sv, 200);

        /* Retry: should succeed with request_id=2 (not 4). */
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 201, &h2) == MOQ_OK);

        /* Pump to server and verify both requests are accepted. */
        pump_actions_to_peer(c, sv, 201);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Session is still active. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- invalid request ID SUBSCRIBE must not process auth ----------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.send_auth_token_cache_size = true;
        svextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Feed SUBSCRIBE with wrong-parity request_id (odd=server parity,
         * but peer is client → expects even). Auth token must not be
         * processed/staged. */
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 99,
            .token_type = 7,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg);
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 1, "ns", "t", p, 1);

        MOQ_TEST_CHECK_EQ_INT(moq_session_state(sv), MOQ_SESS_CLOSED);

        /* Auth alias must NOT have been registered. */
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(&sv->peer_token_cache, 99,
            NULL, NULL, NULL), MOQ_TOKEN_ERR_UNKNOWN);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK_EQ_INT(evts[0].kind, MOQ_EVENT_SESSION_CLOSED);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT((int)as.balance, 0);
    }

    /* -- invalid request ID REQUEST_UPDATE must not queue/emit --------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes, server accepts → ESTABLISHED subscription. */
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        sub_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 100, &h) == MOQ_OK);
        pump_actions_to_peer(c, sv, 100);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 100);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 100);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        /* Feed REQUEST_UPDATE with wrong-parity new request_id (odd=1,
         * but client→server expects even from client). */
        {
            uint8_t buf[128];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_request_update(&w, 1, 0, NULL, 0);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 101);
        }

        MOQ_TEST_CHECK_EQ_INT(moq_session_state(sv), MOQ_SESS_CLOSED);

        /* No SUBSCRIBE_UPDATED event should have been emitted. */
        size_t ne = moq_session_poll_events(sv, &ev, 1);
        if (ne > 0) {
            MOQ_TEST_CHECK_EQ_INT(ev.kind, MOQ_EVENT_SESSION_CLOSED);
            moq_event_cleanup(&ev);
        }

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* Failed subscription REQUEST_UPDATE → ERROR + PUBLISH_DONE     */
    /* ============================================================== */

    /* == Unsupported sub update produces ERROR + PUBLISH_DONE ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t asub;
        moq_accept_subscribe_cfg_init(&asub);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            sv_sub, &asub, 2000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Send unsupported REQUEST_UPDATE (SUBSCRIPTION_FILTER). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t filt_buf[16];
        size_t filt_len = 0;
        moq_d16_subscription_filter_t filt = { .filter_type = 0x2 };
        moq_d16_encode_subscription_filter(filt_buf, sizeof(filt_buf),
            &filt_len, &filt);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
            .value = filt_buf, .value_len = filt_len,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Server should have queued 2 actions:
         * REQUEST_ERROR + PUBLISH_DONE(UPDATE_FAILED). */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        MOQ_TEST_CHECK(na >= 2);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK(acts[1].kind == MOQ_ACTION_SEND_CONTROL);

        /* Decode first: should be REQUEST_ERROR. */
        {
            moq_buf_reader_t cr;
            moq_buf_reader_init(&cr, acts[0].u.send_control.data,
                acts[0].u.send_control.len);
            moq_control_envelope_t env;
            MOQ_TEST_CHECK(moq_control_decode_envelope(&cr, &env) == MOQ_OK);
            MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_ERROR);
        }
        /* Decode second: should be PUBLISH_DONE. */
        {
            moq_buf_reader_t cr;
            moq_buf_reader_init(&cr, acts[1].u.send_control.data,
                acts[1].u.send_control.len);
            moq_control_envelope_t env;
            MOQ_TEST_CHECK(moq_control_decode_envelope(&cr, &env) == MOQ_OK);
            MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH_DONE);

            moq_d16_publish_done_t done;
            MOQ_TEST_CHECK(moq_d16_decode_publish_done(env.payload,
                env.payload_len, &done) == MOQ_OK);
            MOQ_TEST_CHECK(done.status_code == 0x8);
        }
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Subscription should be terminated (freed). */
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Failed sub update resets active subgroups =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub, &asub, 2000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Open a subgroup on the subscription and drain its open SEND_DATA. */
        moq_subgroup_cfg_t sg_cfg; moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 2500, &sg)
            == MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(sv, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Unsupported REQUEST_UPDATE (SUBSCRIPTION_FILTER) terminates the sub. */
        uint8_t wire[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t filt_buf[16]; size_t filt_len = 0;
        moq_d16_subscription_filter_t filt = { .filter_type = 0x2 };
        moq_d16_encode_subscription_filter(filt_buf, sizeof(filt_buf),
            &filt_len, &filt);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
            .value = filt_buf, .value_len = filt_len,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* RESET_DATA for the subgroup plus REQUEST_ERROR + PUBLISH_DONE. */
        bool found_reset = false, found_err = false, found_done = false;
        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) {
                  if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                      acts[i].u.reset_data.error_code == 0x1)
                      found_reset = true;
                  if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                      moq_buf_reader_t cr;
                      moq_buf_reader_init(&cr, acts[i].u.send_control.data,
                          acts[i].u.send_control.len);
                      moq_control_envelope_t env;
                      if (moq_control_decode_envelope(&cr, &env) == MOQ_OK) {
                          if (env.msg_type == MOQ_D16_REQUEST_ERROR)
                              found_err = true;
                          if (env.msg_type == MOQ_D16_PUBLISH_DONE)
                              found_done = true;
                      }
                  }
                  moq_action_cleanup(&acts[i]);
              }
        }
        MOQ_TEST_CHECK(found_reset);
        MOQ_TEST_CHECK(found_err);
        MOQ_TEST_CHECK(found_done);

        /* Subscription terminated. */
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) < 0);

        /* The orphaned subgroup handle no longer accepts writes / emits data. */
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &payload);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, payload, 3500)
            != MOQ_OK);
        moq_rcbuf_decref(payload);
        { moq_action_t a; bool saw_data = false;
          while (moq_session_poll_actions(sv, &a, 1) > 0) {
              if (a.kind == MOQ_ACTION_SEND_DATA) saw_data = true;
              moq_action_cleanup(&a);
          }
          MOQ_TEST_CHECK(!saw_data); }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Action WB on subscription update leaves state intact ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 2;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t asub;
        moq_accept_subscribe_cfg_init(&asub);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            sv_sub, &asub, 2000) == MOQ_OK);
        /* Action queue: 1 of 2 used (SUBSCRIBE_OK). Need 2 for
         * the two-action error response, only 1 slot free → WB. */

        /* Send unsupported REQUEST_UPDATE — needs 2 slots, only 1. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t filt_buf[16];
        size_t filt_len = 0;
        moq_d16_subscription_filter_t filt = { .filter_type = 0x2 };
        moq_d16_encode_subscription_filter(filt_buf, sizeof(filt_buf),
            &filt_len, &filt);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
            .value = filt_buf, .value_len = filt_len,
            .is_varint = false,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        moq_result_t urc = moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000);
        MOQ_TEST_CHECK(urc == MOQ_ERR_WOULD_BLOCK);

        /* Subscription still alive. */
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) >= 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Drain and retry. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_process_pending(sv, 3000);

        /* Now terminated. */
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) < 0);

        na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Send-buf capacity blocks paired response atomically ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Tiny send buffer: fits SUBSCRIBE_OK but not the two-message
         * error pair on top of it. */
        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.send_buffer_size = 19;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &scfg, 1000, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t asub;
        moq_accept_subscribe_cfg_init(&asub);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
            sv_sub, &asub, 2000) == MOQ_OK);
        /* SUBSCRIBE_OK sits in send_buf (not yet polled). */

        /* Send unsupported REQUEST_UPDATE. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t filt_buf[16];
        size_t filt_len = 0;
        moq_d16_subscription_filter_t filt = { .filter_type = 0x2 };
        moq_d16_encode_subscription_filter(filt_buf, sizeof(filt_buf),
            &filt_len, &filt);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
            .value = filt_buf, .value_len = filt_len,
            .is_varint = false,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        moq_result_t urc = moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000);
        MOQ_TEST_CHECK(urc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Subscription still live — no partial output. */
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) >= 0);

        /* Only the SUBSCRIBE_OK action should be queued. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(na, 1);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* After draining, retry succeeds (send_buf reset). */
        moq_session_process_pending(sv, 3000);
        MOQ_TEST_CHECK(sub_resolve_handle(sv, sv_sub) < 0);

        na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 2);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Namespace parts survive decode boundary (ASan regression) ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("org"),
            MOQ_BYTES_LITERAL("example"),
            MOQ_BYTES_LITERAL("track"),
        };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 3 };
        scfg.track_name = MOQ_BYTES_LITERAL("video");

        moq_subscription_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_namespace.count, 3);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_namespace.parts[0].len, 3);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.track_namespace.parts[0].data,
            "org", 3) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_namespace.parts[1].len, 7);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.track_namespace.parts[1].data,
            "example", 7) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_namespace.parts[2].len, 5);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.track_namespace.parts[2].data,
            "track", 5) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* SUBSCRIPTION UPDATE (outbound)                                 */
    /* ============================================================== */

    /* == Successful update: wire content + server event ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        /* Decode the queued action to verify wire content. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(na, 1);
        MOQ_TEST_CHECK_EQ_U64(decode_action_msg_type(&acts[0]),
            MOQ_D16_REQUEST_UPDATE);
        {
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[0].u.send_control.data,
                acts[0].u.send_control.len);
            MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
            moq_kvp_entry_t params[4];
            moq_d16_request_update_t upd = {
                .params = params, .params_cap = 4 };
            MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_request_update(
                env.payload, env.payload_len, &upd), MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(upd.request_id, 2);
            MOQ_TEST_CHECK_EQ_U64(upd.existing_request_id, 0);
            MOQ_TEST_CHECK_EQ_SIZE(upd.params_count, 1);
            MOQ_TEST_CHECK_EQ_U64(upd.params[0].type,
                MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
            uint64_t prio;
            MOQ_TEST_CHECK(moq_quic_varint_decode(upd.params[0].value,
                upd.params[0].value_len, &prio) > 0);
            MOQ_TEST_CHECK_EQ_U64(prio, 200);
        }
        /* Feed to server. */
        moq_session_on_control_bytes(sv, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 1000);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Server sees the update event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.subscriber_priority, 200);
        moq_event_cleanup(&ev);

        /* Server's auto-ack (REQUEST_OK) should reach client cleanly. */
        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Update carries an auth token end to end (D16 wire) ============ *
     *  REQUEST_UPDATE with a USE_VALUE token: AUTH_TOKEN (0x03) rides in
     *  ascending KVP order, the peer decodes it through the real inbound
     *  path, and SUBSCRIBE_UPDATED surfaces the resolved token. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Priority + token: DELIVERY_TIMEOUT(0x02) absent here, so the
         * wire order is AUTH_TOKEN(0x03) then SUBSCRIBER_PRIORITY(0x20). */
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        moq_auth_token_t tok = {
            .token_type = 11,
            .token_value = MOQ_BYTES_LITERAL("fresh"),
        };
        ucfg.auth_tokens = &tok;
        ucfg.auth_token_count = 1;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(na, 1);
        MOQ_TEST_CHECK_EQ_U64(decode_action_msg_type(&acts[0]),
            MOQ_D16_REQUEST_UPDATE);
        {
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[0].u.send_control.data,
                acts[0].u.send_control.len);
            MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
            moq_kvp_entry_t params[4];
            moq_d16_request_update_t upd = {
                .params = params, .params_cap = 4 };
            MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_request_update(
                env.payload, env.payload_len, &upd), MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(upd.params_count, 2);
            MOQ_TEST_CHECK_EQ_U64(upd.params[0].type,
                MOQ_MSG_PARAM_AUTHORIZATION_TOKEN);
            MOQ_TEST_CHECK_EQ_U64(upd.params[1].type,
                MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
        }
        moq_session_on_control_bytes(sv, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 1000);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* The peer surfaces the resolved token on SUBSCRIBE_UPDATED. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.tokens[0].token_type, 11);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.subscribe_updated.tokens[0].token_value.len, 5);
        MOQ_TEST_CHECK(memcmp(
            ev.u.subscribe_updated.tokens[0].token_value.data, "fresh", 5) == 0);
        moq_event_cleanup(&ev);

        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == New-group request end to end (D16 wire) ======================= *
     *  SUBSCRIBE carries it blind; SUBSCRIBE_OK advertises DYNAMIC_GROUPS=1
     *  via the extensions blob ({0x30, 0x01}); the update then carries a
     *  fresh request, gated on the latched support. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.has_new_group_request = true;
        scfg.new_group_request = 0;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.has_new_group_request);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.new_group_request, 0);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        static const uint8_t dyn[] = { 0x30, 0x01 };
        acc.track_properties = (moq_bytes_t){ dyn, 2 };
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.dynamic_groups);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_new_group_request = true;
        ucfg.new_group_request = 21;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_new_group_request);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.new_group_request, 21);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == New-group request without dynamic-group support: INVAL ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);   /* no DYNAMIC_GROUPS */
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(!ev.u.subscribe_ok.dynamic_groups);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_new_group_request = true;
        ucfg.new_group_request = 5;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_ERR_INVAL);
        /* Nothing was mutated: a plain update still works. */
        ucfg.has_new_group_request = false;
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 9;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DYNAMIC_GROUPS above 1 in SUBSCRIBE_OK closes 0x3 ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        static const uint8_t bad[] = { 0x30, 0x02 };   /* value 2: violation */
        acc.track_properties = (moq_bytes_t){ bad, 2 };
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(ev.u.closed.code, 0x3);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Second update while pending returns WRONG_STATE =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        ucfg.subscriber_priority = 100;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_ERR_WRONG_STATE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Empty config returns INVAL ==================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Stale handle returns STALE_HANDLE ============================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        moq_subscription_t bogus = { 0xDEAD };
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, bogus,
            &ucfg, 1000), MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Closed session returns CLOSED ================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_session_destroy(sv);
        sv = NULL;

        moq_session_on_transport_close(c, 0, 1000);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(c, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        { moq_event_t de; while (moq_session_poll_events(c, &de, 1) > 0)
            moq_event_cleanup(&de); }

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 2000), MOQ_ERR_CLOSED);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Action queue full returns WOULD_BLOCK, no side effects ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        test_session_fill_action_queue(c);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_ERR_WOULD_BLOCK);

        /* Drain and retry succeeds. */
        { moq_action_t da[64]; size_t dn;
          while ((dn = moq_session_poll_actions(c, da, 64)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 2000), MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_ERROR for update clears pending, session stays up ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        /* Inject a raw REQUEST_ERROR for the update request_id=2. */
        uint8_t err_buf[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_error(&ew, 2, 0x10,
            0, NULL, 0), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, err_buf,
            moq_buf_writer_offset(&ew), 2000), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* After error, another update succeeds. */
        { moq_action_t da[4]; size_t dn;
          while ((dn = moq_session_poll_actions(c, da, 4)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        ucfg.subscriber_priority = 100;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 3000), MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Update pending cleanup on unsubscribe ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv, ssub,
            &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Send update, then unsubscribe before response. */
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_unsubscribe(c, sub, 1000), MOQ_OK);

        /* Feed BOTH REQUEST_UPDATE and UNSUBSCRIBE to the server so
         * the server's request-id sequence stays intact. */
        {
            moq_action_t da[8];
            size_t dn;
            int saw_update = 0, saw_unsub = 0;
            while ((dn = moq_session_poll_actions(c, da, 8)) > 0) {
                for (size_t i = 0; i < dn; i++) {
                    if (da[i].kind == MOQ_ACTION_SEND_CONTROL) {
                        uint64_t mt = decode_action_msg_type(&da[i]);
                        if (mt == MOQ_D16_REQUEST_UPDATE) saw_update++;
                        else if (mt == MOQ_D16_UNSUBSCRIBE) saw_unsub++;
                        MOQ_TEST_CHECK_EQ_INT(
                            moq_session_on_control_bytes(sv,
                                da[i].u.send_control.data,
                                da[i].u.send_control.len, 1000), MOQ_OK);
                    }
                    moq_action_cleanup(&da[i]);
                }
            }
            MOQ_TEST_CHECK_EQ_INT(saw_update, 1);
            MOQ_TEST_CHECK_EQ_INT(saw_unsub, 1);
        }

        /* Drain server events from old update/unsub path, but leave
         * server actions queued — the old REQUEST_OK for update id=2
         * must reach the client later so the tombstone is exercised. */
        { moq_event_t sev; while (moq_session_poll_events(sv, &sev, 1) > 0)
            moq_event_cleanup(&sev); }

        /* New subscribe reuses the same slot (max_subscriptions=1). */
        scfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_subscription_t sub2;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 2000,
            &sub2), MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);

        /* Accept the new subscribe on the server. */
        {
            moq_event_t sevts[4];
            size_t ne = moq_session_poll_events(sv, sevts, 4);
            moq_subscription_t ssub2 = MOQ_SUBSCRIPTION_INVALID;
            for (size_t i = 0; i < ne; i++) {
                if (sevts[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                    ssub2 = sevts[i].u.subscribe_request.sub;
                moq_event_cleanup(&sevts[i]);
            }
            MOQ_TEST_CHECK(!moq_subscription_eq(ssub2,
                MOQ_SUBSCRIPTION_INVALID));
            moq_accept_subscribe_cfg_t acc2;
            moq_accept_subscribe_cfg_init(&acc2);
            MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
                ssub2, &acc2, 2000), MOQ_OK);
        }

        /* Pump server -> client.  The server has a pending REQUEST_OK
         * for the old update (id=2) which the client's tombstone should
         * absorb, plus a SUBSCRIBE_OK for the new sub. */
        pump_actions_to_peer(sv, c, 2000);

        /* Poll client events until we see SUBSCRIBE_OK for sub2.
         * The tombstoned REQUEST_OK for id=2 is silently consumed. */
        {
            bool got_ok = false;
            moq_event_t cev;
            while (moq_session_poll_events(c, &cev, 1) > 0) {
                if (cev.kind == MOQ_EVENT_SUBSCRIBE_OK) got_ok = true;
                moq_event_cleanup(&cev);
            }
            MOQ_TEST_CHECK(got_ok);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* New subscription on reused slot should accept an update. */
        ucfg.subscriber_priority = 100;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub2,
            &ucfg, 3000), MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_OK for update: defined wrong-scope param ignored ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        /* Inject REQUEST_OK with FORWARD param (defined, wrong scope). */
        uint8_t fwd_val = 0x01;
        moq_kvp_entry_t params[] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = &fwd_val, .value_len = 1,
            .is_varint = true,
        }};
        uint8_t ok_buf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_ok(&ow, 2, params, 1),
            MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_OK for update: unknown param closes session =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 1000), MOQ_OK);

        /* Inject REQUEST_OK with unknown param type 0x40. */
        uint8_t unk_val = 0x00;
        moq_kvp_entry_t params[] = {{
            .type = 0x40,
            .value = &unk_val, .value_len = 1,
            .is_varint = true,
        }};
        uint8_t ok_buf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_ok(&ow, 2, params, 1),
            MOQ_OK);
        moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* AUTH TOKEN outbound on SUBSCRIBE                               */
    /* ============================================================== */

    /* == Subscribe with one auth token ================================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        uint8_t tok_val[] = { 0xCA, 0xFE };
        moq_auth_token_t tok = {
            .token_type = 42,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        scfg.auth_tokens = &tok;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[0].token_type, 42);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.tokens[0].token_value.len, 2);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.data[0] == 0xCA);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.data[1] == 0xFE);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Subscribe with multiple auth tokens ========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        uint8_t val_a[] = { 0x01 };
        uint8_t val_b[] = { 0x02, 0x03 };
        moq_auth_token_t toks[2] = {
            { .token_type = 10, .token_value = { val_a, 1 } },
            { .token_type = 20, .token_value = { val_b, 2 } },
        };
        scfg.auth_tokens = toks;
        scfg.auth_token_count = 2;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 2);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[0].token_type, 10);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.tokens[0].token_value.len, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[1].token_type, 20);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.tokens[1].token_value.len, 2);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Auth token validation: count>0 with NULL pointer -> INVAL ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.auth_tokens = NULL;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Auth token validation: value len>0 with NULL data -> INVAL ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_auth_token_t bad_tok = {
            .token_type = 1,
            .token_value = { NULL, 5 },
        };
        scfg.auth_tokens = &bad_tok;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-length token value is semantically malformed ============= *
     *  A well-formed Token structure whose RESOLVED value fails semantic
     *  validation (zero-length) MUST be rejected with MALFORMED_AUTH_TOKEN
     *  (request error 0x4) -- the request never surfaces and the session
     *  stays alive. The sender side stays permissive: what is semantically
     *  valid is ultimately the receiver's call. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_auth_token_t tok = { .token_type = 99, .token_value = { NULL, 0 } };
        scfg.auth_tokens = &tok;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* The request is auto-rejected: no SUBSCRIBE_REQUEST, session alive. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* The client observes SUBSCRIBE_ERROR with MALFORMED_AUTH_TOKEN. */
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_error.error_code, 0x4);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Tiny send buffer: encode failure with no committed state ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.send_buffer_size = 16;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        uint8_t big_val[64];
        memset(big_val, 0xAB, sizeof(big_val));
        moq_auth_token_t tok = {
            .token_type = 1,
            .token_value = { big_val, sizeof(big_val) },
        };
        scfg.auth_tokens = &tok;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(c, &scfg, 0, &sub);
        MOQ_TEST_CHECK(rc < 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Auth token with invalid token_type -> INVAL ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_auth_token_t bad_tok = {
            .token_type = UINT64_MAX,
            .token_value = { NULL, 0 },
        };
        scfg.auth_tokens = &bad_tok;
        scfg.auth_token_count = 1;

        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Too many auth tokens exceeds D16 param capacity =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_auth_token_t toks[17];
        for (int i = 0; i < 17; i++) {
            toks[i].token_type = (uint64_t)i;
            toks[i].token_value = (moq_bytes_t){ NULL, 0 };
        }
        scfg.auth_tokens = toks;
        scfg.auth_token_count = 17;

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(c, &scfg, 0, &sub);
        MOQ_TEST_CHECK(rc < 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 13 tokens + priority + forward + filter + group_order = 17 -> fail */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.has_subscriber_priority = true;
        scfg.subscriber_priority = 50;
        scfg.has_forward = true;
        scfg.forward = false;
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        scfg.group_order = MOQ_GROUP_ORDER_ASCENDING;
        moq_auth_token_t toks[13];
        static uint8_t tok_vals[13];
        for (int i = 0; i < 13; i++) {
            tok_vals[i] = (uint8_t)('A' + i);   /* semantically valid values */
            toks[i].token_type = (uint64_t)i;
            toks[i].token_value = (moq_bytes_t){ &tok_vals[i], 1 };
        }
        scfg.auth_tokens = toks;
        scfg.auth_token_count = 13;

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(c, &scfg, 0, &sub);
        MOQ_TEST_CHECK(rc < 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* 12 tokens + 4 other params = 16, should succeed and be
         * accepted by a libmoq peer. */
        scfg.auth_token_count = 12;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 12);
        for (size_t ti = 0; ti < 12; ti++)
            MOQ_TEST_CHECK_EQ_U64(
                ev.u.subscribe_request.tokens[ti].token_type, ti);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* DONE SUBSCRIBE / FORWARD ENFORCEMENT                          */
    /* ============================================================== */

    /* == Publisher sends done_subscribe, subscriber gets event ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv, sv_sub,
            &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x4;
        dcfg.stream_count = 7;
        dcfg.reason = MOQ_BYTES_LITERAL("done");
        MOQ_TEST_CHECK_EQ_INT(moq_session_done_subscribe(sv, sv_sub,
            &dcfg, 1000), MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_DONE);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x4);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 7);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_done.reason.len, 4);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_done.reason.data, "done", 4) == 0);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Forward=false blocks open_subgroup ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.has_forward = true;
        scfg.forward = false;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(!ev.u.subscribe_request.forward);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(sv, sv_sub,
            &acc, 0), MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        moq_subgroup_handle_t sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_subgroup(sv, sv_sub,
            &sgcfg, 1000, &sgh), MOQ_ERR_WRONG_STATE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_UPDATE with forward=true re-enables send ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.has_forward = true;
        scfg.forward = false;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_subgroup(sv, sv_sub,
            &sgcfg, 1000, &sgh), MOQ_ERR_WRONG_STATE);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = true;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 2000), MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);
        { moq_event_t sev;
          while (moq_session_poll_events(sv, &sev, 1) > 0)
              moq_event_cleanup(&sev); }
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_INT(moq_session_open_subgroup(sv, sv_sub,
            &sgcfg, 3000, &sgh), MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Update forward=false blocks writes on open subgroup =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 0, &sub),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_subgroup(sv, sv_sub,
            &sgcfg, 1000, &sgh), MOQ_OK);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(c, sub,
            &ucfg, 2000), MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);
        { moq_event_t sev;
          while (moq_session_poll_events(sv, &sev, 1) > 0)
              moq_event_cleanup(&sev); }
        pump_actions_to_peer(sv, c, 2000);

        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &buf);
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_object(sv, sgh, 0, buf,
            3000), MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(buf);

        MOQ_TEST_CHECK_EQ_INT(moq_session_close_subgroup(sv, sgh, 3000),
            MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* REQUEST_UPDATE for non-subscription targets                    */
    /* ============================================================== */

    /* == REQUEST_UPDATE for FETCH → NOT_SUPPORTED, session stays up == */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_fetch_cfg_t fcfg;
        moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        fcfg.track_name = MOQ_BYTES_LITERAL("t");
        fcfg.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_fetch(c, &fcfg, 0, &fh), MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        { moq_event_t ev;
          moq_session_poll_events(sv, &ev, 1);
          moq_event_cleanup(&ev); }

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 2, 0, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          bool found = false;
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                  decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_ERROR) {
                  moq_control_envelope_t env;
                  moq_buf_reader_t r;
                  moq_buf_reader_init(&r, acts[i].u.send_control.data,
                      acts[i].u.send_control.len);
                  moq_control_decode_envelope(&r, &env);
                  moq_d16_request_error_t err;
                  moq_d16_decode_request_error(env.payload,
                      env.payload_len, &err);
                  MOQ_TEST_CHECK_EQ_HEX(err.error_code,
                      (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED);
                  found = true;
              }
              moq_action_cleanup(&acts[i]);
          }
          MOQ_TEST_CHECK(found); }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_UPDATE for PUBLISH → accepted, session stays up ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t ph;
        MOQ_TEST_CHECK_EQ_INT(moq_session_publish(c, &pcfg, 0, &ph),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        { moq_event_t ev;
          moq_session_poll_events(sv, &ev, 1);
          moq_accept_publish_cfg_t acfg;
          moq_accept_publish_cfg_init(&acfg);
          moq_session_accept_publish(sv, ev.u.publish_request.pub,
              &acfg, 1000);
          moq_event_cleanup(&ev); }
        pump_actions_to_peer(sv, c, 1000);
        { moq_event_t ev;
          moq_session_poll_events(c, &ev, 1);
          moq_event_cleanup(&ev); }

        /* Feed into c (publisher). request_id=1 (server's first odd). */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 1, 0, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, wire,
            moq_buf_writer_offset(&w), 2000), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(c, acts, 4);
          bool found = false;
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                  decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_OK)
                  found = true;
              moq_action_cleanup(&acts[i]);
          }
          MOQ_TEST_CHECK(found); }

        /* Drain PUBLISH_UPDATED event. */
        { moq_event_t ev;
          MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
          MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED); }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == queued event spans survive batched inbound messages =========== *
     * Two SUBSCRIBEs delivered back-to-back (two advancing calls, no poll
     * between -- one packet batch). The first queued SUBSCRIBE_REQUEST's
     * borrowed name/namespace live in the event borrow arena and must NOT
     * be clobbered by the second decode (they previously shared the
     * per-advance transient scratch). */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns-one", "track-alpha", NULL, 0);
        feed_subscribe(sv, 2, "ns-two", "track-beta", NULL, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_name.len,
                               strlen("track-alpha"));
        MOQ_TEST_CHECK(ev.u.subscribe_request.track_name.data &&
                       memcmp(ev.u.subscribe_request.track_name.data,
                              "track-alpha", strlen("track-alpha")) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_namespace.count,
                               (size_t)1);
        MOQ_TEST_CHECK(
            ev.u.subscribe_request.track_namespace.parts[0].data &&
            ev.u.subscribe_request.track_namespace.parts[0].len ==
                strlen("ns-one") &&
            memcmp(ev.u.subscribe_request.track_namespace.parts[0].data,
                   "ns-one", strlen("ns-one")) == 0);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_name.len,
                               strlen("track-beta"));
        MOQ_TEST_CHECK(ev.u.subscribe_request.track_name.data &&
                       memcmp(ev.u.subscribe_request.track_name.data,
                              "track-beta", strlen("track-beta")) == 0);
        MOQ_TEST_CHECK(
            ev.u.subscribe_request.track_namespace.parts[0].data &&
            memcmp(ev.u.subscribe_request.track_namespace.parts[0].data,
                   "ns-two", strlen("ns-two")) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Data before SUBSCRIBE_OK is staged, not lost (§11.1, §11.3) ==== *
     * Track Alias is assigned by the PUBLISHER in SUBSCRIBE_OK, never in
     * SUBSCRIBE. Because data and control travel on independent QUIC streams
     * (and a relay may emit a cached object the moment it sees the
     * SUBSCRIBE), an object can arrive before the SUBSCRIBE_OK that
     * establishes its alias. Such an object MUST NOT be delivered before the
     * OK (the subscriber cannot yet know which track the alias names) and
     * MUST NOT be silently lost: it is staged by alias and released once the
     * OK assigns that alias. The early object uses a publisher-chosen alias
     * that is NOT the subscriber's internal default. draft-16. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Publisher-chosen alias the subscriber does not yet know. */
        const uint64_t k_alias = 7;

        /* Early object on that alias, before SUBSCRIBE_OK, injected on the
         * client's data plane (as a relay's cached object would arrive). */
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = k_alias;
        dg.group_id = 0;
        dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_object_datagram(&dw, &dg),
                              (int)MOQ_OK);
        moq_session_on_datagram(c, db, moq_buf_writer_offset(&dw), 1000);

        /* Before the OK: the object must NOT surface and the session lives. */
        int early = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) early++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(early, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                              (int)MOQ_SESS_ESTABLISHED);

        /* Server accepts, assigning the alias the early object already used. */
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = k_alias;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        /* After the OK: SUBSCRIBE_OK first, then the staged object. */
        int ok_idx = -1, obj_idx = -1, idx = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && ok_idx < 0) ok_idx = idx;
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && obj_idx < 0)
                obj_idx = idx;
            moq_event_cleanup(&ev);
            idx++;
        }
        MOQ_TEST_CHECK(ok_idx >= 0);
        MOQ_TEST_CHECK(obj_idx >= 0);
        MOQ_TEST_CHECK(ok_idx < obj_idx);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Subgroup/uni data before SUBSCRIBE_OK is staged, not lost ===== *
     * The moxygen/moqx case: a relay delivers its cached object on a uni
     * subgroup stream the instant it sees the SUBSCRIBE, before SUBSCRIBE_OK
     * establishes the alias. The stream is held (deferred) for control/data
     * reordering and its object surfaces only after the OK assigns the alias.
     * draft-16. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        const uint64_t k_alias = 7;

        /* Hand-craft a subgroup header (type 0x14: id-present, no ext/eog,
         * explicit priority) + one object, on a uni stream, for alias 7. */
        uint8_t sg[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sg, sizeof(sg));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = k_alias;
        hdr.group_id = 0;
        hdr.subgroup_id = 0;
        hdr.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_subgroup_header(&sw, &hdr),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_object_fields(&sw, 0, 5,
                              (const uint8_t *)"hello"), (int)MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
        moq_session_on_data_bytes(c, rx_ref, sg, moq_buf_writer_offset(&sw),
                                  false, 1000);

        /* Before the OK: no object surfaces, session lives. */
        int early = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) early++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(early, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                              (int)MOQ_SESS_ESTABLISHED);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = k_alias;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        /* After the OK: SUBSCRIBE_OK first, then the deferred object. */
        int ok_idx = -1, obj_idx = -1, idx = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && ok_idx < 0) ok_idx = idx;
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && obj_idx < 0)
                obj_idx = idx;
            moq_event_cleanup(&ev);
            idx++;
        }
        MOQ_TEST_CHECK(ok_idx >= 0);
        MOQ_TEST_CHECK(obj_idx >= 0);
        MOQ_TEST_CHECK(ok_idx < obj_idx);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Teardown: held/deferred data discarded, no leak, session alive = *
     * Early datagram + subgroup arrive for a pending subscription; the
     * subscription is then REJECTED. With no forwarding subscription left
     * pending, the held data can never match an alias, so it is discarded
     * (no delivery, no leak), and the session stays up. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        moq_d16_encode_object_datagram(&dw, &dg);
        moq_session_on_datagram(c, db, moq_buf_writer_offset(&dw), 1000);

        uint8_t sg[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sg, sizeof(sg));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 8;
        hdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&sw, &hdr);
        moq_d16_encode_object_fields(&sw, 0, 5, (const uint8_t *)"hello");
        moq_session_on_data_bytes(c, moq_stream_ref_from_u64(77), sg,
                                  moq_buf_writer_offset(&sw), false, 1000);

        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        MOQ_TEST_CHECK(moq_session_reject_subscribe(sv, server_sub, &rej,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        int objs = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) objs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                              (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);   /* held data not leaked */
    }

    /* == forward=false: early data is NOT held (peer must not send) ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        sub_cfg.has_forward = true;
        sub_cfg.forward = false;   /* not forwarding: no staging */
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        moq_d16_encode_object_datagram(&dw, &dg);
        moq_session_on_datagram(c, db, moq_buf_writer_offset(&dw), 1000);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = 7;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        int objs = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) objs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 0);   /* never staged -> never delivered */

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Tiny event queue: deferred subgroup releases on the NEXT poll === *
     * The event queue holds one event, so SUBSCRIBE_OK fills it and the
     * deferred object cannot emit during release. Polling the OK frees a slot
     * and the retry hook delivers the object with NO new network input. This
     * is the staged_count==0 case the poll retry must still handle. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) > 0) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        uint8_t sg[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sg, sizeof(sg));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 7;
        hdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&sw, &hdr);
        moq_d16_encode_object_fields(&sw, 0, 5, (const uint8_t *)"hello");
        moq_session_on_data_bytes(c, moq_stream_ref_from_u64(55), sg,
                                  moq_buf_writer_offset(&sw), false, 1000);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = 7;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        /* Poll #1: only SUBSCRIBE_OK fits (cap 1). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);
        /* Poll #2: retry hook released the deferred object, no new input. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Tiny event queue: a held DATAGRAM for the established alias must
     *    NOT be discarded by the no-pending cleanup while it awaits capacity.
     *    (Regression: discard-on-establish previously cleared it.) ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) > 0) moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        moq_d16_encode_object_datagram(&dw, &dg);
        moq_session_on_datagram(c, db, moq_buf_writer_offset(&dw), 1000);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = 7;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Discard under action-queue pressure leaves no stale stream ===== *
     * A deferred stream is rejected while the client's action queue is full,
     * so rx_try_stop() cannot send STOP_DATA. With no forwarding subscription
     * left to retry, the discard must free the entry directly (no lingering
     * DEFERRED_ALIAS / NEED_STOP stream, no leak). */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000, &sub1) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);     /* drains the SUBSCRIBE action */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Deferred subgroup stream for alias 7 (sub1 is forwarding+pending). */
        uint8_t sg[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sg, sizeof(sg));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 7;
        hdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&sw, &hdr);
        moq_d16_encode_object_fields(&sw, 0, 5, (const uint8_t *)"hello");
        moq_session_on_data_bytes(c, moq_stream_ref_from_u64(55), sg,
                                  moq_buf_writer_offset(&sw), false, 1000);

        /* Fill the client action queue with a NON-forwarding subscribe (so it
         * does not keep a forwarding sub pending), left un-pumped. */
        moq_subscribe_cfg_t sub2_cfg = sub_cfg;
        sub2_cfg.track_name = MOQ_BYTES_LITERAL("audio");
        sub2_cfg.has_forward = true;
        sub2_cfg.forward = false;
        moq_subscription_t sub2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub2_cfg, 1000, &sub2) == MOQ_OK);

        /* Reject sub1: client frees it -> no forwarding pending -> discard the
         * deferred stream while the action queue is full. */
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        MOQ_TEST_CHECK(moq_session_reject_subscribe(sv, server_sub, &rej,
            1000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        /* No lingering deferred / need-stop rx stream. */
        int stale = 0;
        for (size_t i = 0; i < c->rx_cap; i++) {
            if (c->rx_streams[i].active &&
                (c->rx_streams[i].parse_state == MOQ_RX_DEFERRED_ALIAS ||
                 c->rx_streams[i].parse_state == MOQ_RX_NEED_STOP))
                stale++;
        }
        MOQ_TEST_CHECK_EQ_INT(stale, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                              (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);   /* deferred buffer freed */
    }

#ifdef MOQ_TEST_SIM
    /* == draft-18 parity (SimPair): data before SUBSCRIBE_OK is held ==== *
     * draft-18 setup/control routing needs SimPair. Same semantics as D16:
     * an early datagram is not delivered before the OK, then the OK assigns
     * the alias and the held object surfaces (OK first, then object). */
    {
        moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
        spcfg.alloc = moq_alloc_default();
        spcfg.version = MOQ_VERSION_DRAFT_18;
        spcfg.client_send_request_capacity = true;
        spcfg.client_initial_request_capacity = 10;
        spcfg.server_send_request_capacity = true;
        spcfg.server_initial_request_capacity = 10;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_simpair_create(&spcfg, &sp), 0);
        moq_session_t *c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                              (int)MOQ_SESS_ESTABLISHED);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg,
            moq_simpair_now_us(sp), &sub_handle) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
        bool got = false;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                server_sub = ev.u.subscribe_request.sub;
                got = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);

        const uint64_t k_alias = 7;
        moq_d18_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = k_alias;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_object_datagram(&dw, &dg),
                              (int)MOQ_OK);
        moq_session_on_datagram(c, db, moq_buf_writer_offset(&dw),
                                moq_simpair_now_us(sp));

        int early = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) early++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(early, 0);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = k_alias;
        acc.has_largest = true;
        acc.largest_group = 0;
        acc.largest_object = 0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        int ok_idx = -1, obj_idx = -1, idx = 0;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && ok_idx < 0) ok_idx = idx;
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && obj_idx < 0)
                obj_idx = idx;
            moq_event_cleanup(&ev);
            idx++;
        }
        MOQ_TEST_CHECK(ok_idx >= 0);
        MOQ_TEST_CHECK(obj_idx >= 0);
        MOQ_TEST_CHECK(ok_idx < obj_idx);

        moq_simpair_destroy(sp);
    }
#endif /* MOQ_TEST_SIM */

    /* == accept_subscribe rejects an alias used by a local publication == */
    /* Publications and subscriptions share the outbound data-alias namespace:
     * accepting a subscription with an alias already used by a publisher-role
     * publication would make two local data sources emit indistinguishable
     * subgroup headers. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;

        /* c PUBLISHes with track_alias 5 (publisher-role, outbound alias 5). */
        moq_bytes_t pns[] = { MOQ_BYTES_LITERAL("p") };
        moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ pns, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("pt");
        pcfg.has_track_alias = true; pcfg.track_alias = 5;
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub_h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &apc, 1000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 1000);
        { moq_event_t e; while (moq_session_poll_events(c, &e, 1) > 0)
              moq_event_cleanup(&e); }

        /* sv SUBSCRIBEs to c; c must reject accepting it with the pub's alias. */
        moq_bytes_t sns[] = { MOQ_BYTES_LITERAL("s") };
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ sns, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("st");
        moq_subscription_t sv_sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(sv, &scfg, 1500,
            &sv_sub), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1500);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_subscription_t c_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true; acc.track_alias = 5;   /* collides with pub */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(c, c_sub, &acc,
            1500), (int)MOQ_ERR_INVAL);
        /* A non-colliding alias is still accepted. */
        acc.track_alias = 6;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(c, c_sub, &acc,
            1500), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_OK with a publication's alias is rejected ============ */
    /* The inbound (peer-assigned) data-alias namespace is shared with our
     * subscriber-role publications: a SUBSCRIBE_OK reusing such an alias would
     * misattribute the peer's data, so it is a protocol violation. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;

        /* sv PUBLISHes to c with track_alias 7 -> c has a subscriber-role
         * publication holding inbound alias 7. */
        moq_bytes_t pns[] = { MOQ_BYTES_LITERAL("p") };
        moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ pns, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("pt");
        pcfg.has_track_alias = true; pcfg.track_alias = 7;
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(sv, &pcfg, 1000, &pub_h),
            (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        moq_session_accept_publish(c, ev.u.publish_request.pub, &apc, 1000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(c, sv, 1000);
        { moq_event_t e; while (moq_session_poll_events(sv, &e, 1) > 0)
              moq_event_cleanup(&e); }

        /* c SUBSCRIBEs (its first request -> id 0); drain its outbound action. */
        moq_bytes_t sns[] = { MOQ_BYTES_LITERAL("s") };
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ sns, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("st");
        moq_subscription_t c_sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 1500,
            &c_sub), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* A crafted SUBSCRIBE_OK reusing alias 7 must be rejected (close 0x5). */
        uint8_t okbuf[64]; moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, okbuf, sizeof(okbuf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_subscribe_ok(&ow, 0, 7,
            NULL, 0, NULL, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, okbuf,
            moq_buf_writer_offset(&ow), 2000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == accept_subscribe auto-alias scan skips publication aliases ==== */
    /* The auto-alias scan now skips publisher-role publication aliases too, so
     * its attempt bound must allow for both pools -- otherwise enough
     * publications ahead of the next alias would falsely exhaust the scan. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_subscriptions = 2;   /* small sub pool: old bound = 3 */
        c_extra.max_publishes = 8;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 16, 16, &c, &sv, &c_extra, NULL);
        moq_event_t ev;

        /* Occupy the first auto aliases (1..3) with publisher-role publications
         * (explicit aliases do not advance the alias counter). */
        for (uint64_t a = 1; a <= 3; a++) {
            moq_bytes_t pns[] = { MOQ_BYTES_LITERAL("p") };
            moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
            pcfg.track_namespace = (moq_namespace_t){ pns, 1 };
            char nm[2] = { 't', (char)('0' + a) };
            pcfg.track_name = (moq_bytes_t){ (const uint8_t *)nm, 2 };
            pcfg.has_track_alias = true; pcfg.track_alias = a;
            moq_publication_t ph;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &ph),
                (int)MOQ_OK);
        }
        { moq_action_t act; while (moq_session_poll_actions(c, &act, 1) > 0)
              moq_action_cleanup(&act); }

        /* sv SUBSCRIBEs; c auto-allocates an alias that must skip 1..3 and
         * succeed (not falsely return MOQ_ERR_INTERNAL). */
        moq_bytes_t sns[] = { MOQ_BYTES_LITERAL("s") };
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ sns, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("st");
        moq_subscription_t sv_sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(sv, &scfg, 1500,
            &sv_sub), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1500);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_subscription_t c_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(c, c_sub, &acc,
            1500), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_subscribe");
    return failures;
}
