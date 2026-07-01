#include "test_session_support.h"

static int g_wrap_release_count = 0;

static void wrap_release_fn(void *ctx, const uint8_t *data, size_t len)
{
    (void)data; (void)len;
    int *count = (int *)ctx;
    (*count)++;
}

int main(void)
{
    int failures = 0;
    /* ============================================================== */
    /* SUBGROUP DATA PATH                                              */
    /* ============================================================== */

    /* -- open/write/close subgroup happy path ------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe + accept. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        /* Open subgroup on server (publisher side). */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            sv_sub, &sg_cfg, 0, &sg) == MOQ_OK);

        /* Verify SEND_DATA header-only action. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(act.u.send_data.header_len > 0);
        MOQ_TEST_CHECK(act.u.send_data.payload == NULL);
        MOQ_TEST_CHECK(act.u.send_data.fin == false);
        moq_action_cleanup(&act);

        /* Write object. */
        moq_rcbuf_t *payload = NULL;
        const uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
        moq_rcbuf_create(&alloc, data, sizeof(data), &payload);

        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, payload, 0) == MOQ_OK);
        moq_rcbuf_decref(payload); /* caller releases its ref */

        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(act.u.send_data.header_len > 0);
        MOQ_TEST_CHECK(act.u.send_data.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(act.u.send_data.payload) == 4);
        MOQ_TEST_CHECK(act.u.send_data.fin == false);
        moq_action_cleanup(&act);

        /* Close subgroup. */
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(act.u.send_data.fin == true);
        MOQ_TEST_CHECK(act.u.send_data.payload == NULL);
        moq_action_cleanup(&act);

        /* Handle is now stale. */
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) ==
                        MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Multi-object subgroup with gaps ------------------------------ */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 0, &sg);
        moq_action_t act;
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        /* Object 0. */
        moq_rcbuf_t *p1 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"A", 1, &p1);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p1, 0) == MOQ_OK);
        moq_rcbuf_decref(p1);
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        /* Object 5 (gap: 1,2,3,4 skipped). */
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"B", 1, &p2);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 5, p2, 0) == MOQ_OK);
        moq_rcbuf_decref(p2);
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        /* Object 3 (backwards) → rejected. */
        moq_rcbuf_t *p3 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"C", 1, &p3);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 3, p3, 0) ==
                        MOQ_ERR_INVAL);
        moq_rcbuf_decref(p3);

        /* Object 5 (equal) → rejected. */
        moq_rcbuf_t *p4 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"D", 1, &p4);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 5, p4, 0) ==
                        MOQ_ERR_INVAL);
        moq_rcbuf_decref(p4);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Duplicate subgroup open rejected ----------------------------- */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg2) == MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- reset_subgroup emits RESET_DATA and invalidates handle ------- */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);

        /* Drain all pending actions (SUBSCRIBE_OK + subgroup open). */
        moq_action_t drain_acts[8];
        size_t nd = moq_session_poll_actions(sv, drain_acts, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);

        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0x42, 0) == MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_RESET_DATA);
        MOQ_TEST_CHECK(act.u.reset_data.error_code == 0x42);
        moq_action_cleanup(&act);

        /* Handle stale. */
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, NULL, 0) ==
                        MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Session destroy decrefs queued SEND_DATA payloads ------------ */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"XYZ", 3, &payload);
        moq_session_write_object(sv, sg, 0, payload, 0);
        moq_rcbuf_decref(payload);

        /* Do NOT drain actions. Destroy session. */
        moq_session_destroy(sv);
        moq_session_destroy(c);
        /* If payload was not decreffed by destroy, balance != 0. */
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Mixed queue: prefix-sized poll drains control, stops at data -- */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        /* Queue: SEND_CONTROL (SUBSCRIBE_OK) */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 0, &sg);
        /* Queue: SEND_CONTROL, SEND_DATA (header-only) */

        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"AB", 2, &payload);
        moq_session_write_object(sv, sg, 0, payload, 0);
        moq_rcbuf_decref(payload);
        /* Queue: SEND_CONTROL, SEND_DATA, SEND_DATA (with rcbuf) */

        /* Poll with prefix-only element size (too small for SEND_DATA).
         * SEND_CONTROL drains, first SEND_DATA stops (even header-only). */
        size_t prefix_size = offsetof(moq_action_t, u) + sizeof(moq_send_control_action_t);
        uint8_t small_buf[2][256];
        size_t count = 0;
        moq_result_t rc = moq_session_poll_actions_ex(sv, small_buf, 2,
            prefix_size, &count);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(count == 1);

        /* Full-size poll gets both SEND_DATA actions. */
        moq_action_t full_acts[4];
        size_t full_count = moq_session_poll_actions(sv, full_acts, 4);
        MOQ_TEST_CHECK(full_count == 2);
        MOQ_TEST_CHECK(full_acts[0].kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(full_acts[1].kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(full_acts[1].u.send_data.payload != NULL);
        moq_action_cleanup(&full_acts[0]);
        moq_action_cleanup(&full_acts[1]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Subgroup operations after session close rejected ------------- */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 0, &sg);

        /* Force protocol close via garbage. */
        const uint8_t garbage[] = { 0x40, 0xFF, 0x00, 0x00 };
        moq_session_on_control_bytes(sv, garbage, sizeof(garbage), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        /* All subgroup operations rejected. */
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"X", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) ==
                        MOQ_ERR_CLOSED);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) ==
                        MOQ_ERR_CLOSED);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) ==
                        MOQ_ERR_CLOSED);
        moq_rcbuf_decref(p);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Stale subgroup handle with non-NULL payload ------------------ */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);

        /* Close subgroup → handle stale. */
        moq_action_t drain_acts[8];
        size_t nd = moq_session_poll_actions(sv, drain_acts, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);
        moq_session_close_subgroup(sv, sg, 0);
        nd = moq_session_poll_actions(sv, drain_acts, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);

        /* write/close/reset with non-NULL payload all reject. */
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"X", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) ==
                        MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) ==
                        MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) ==
                        MOQ_ERR_STALE_HANDLE);
        moq_rcbuf_decref(p);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Close then immediate reopen same tuple → rejected ------------ */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg1, 0) == MOQ_OK);

        /* Same tuple immediately → rejected (CLOSING, not FREE). */
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg2) == MOQ_ERR_INVAL);

        /* Drain all actions + advancing call → reap CLOSING entries. */
        moq_action_t drain_acts[16];
        size_t nd = moq_session_poll_actions(sv, drain_acts, 16);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);
        moq_session_tick(sv, 1);

        /* Now reopen succeeds. */
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg2) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- max_open_subgroups=1: second open rejected ------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_open_subgroups = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg1) == MOQ_OK);

        moq_subgroup_cfg_t sg_cfg2;
        moq_subgroup_cfg_init(&sg_cfg2);
        sg_cfg2.group_id = 1;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg2, 0, &sg2) ==
            MOQ_ERR_WOULD_BLOCK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- max_open_subgroups=1: pool recovery after close -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_open_subgroups = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        /* Open A fills the single slot. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg_a;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg, 0, &sg_a) == MOQ_OK);

        /* Open B blocked. */
        moq_subgroup_cfg_t sg_cfg2;
        moq_subgroup_cfg_init(&sg_cfg2);
        sg_cfg2.group_id = 1;
        moq_subgroup_handle_t sg_b;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg2, 0, &sg_b) == MOQ_ERR_WOULD_BLOCK);

        /* Close A to free the slot. */
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg_a, 0) == MOQ_OK);

        /* Drain actions so the close is processed. */
        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Retry open B — should succeed. */
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub,
            &sg_cfg2, 0, &sg_b) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Queue full: write_object no object_id advance, no payload leak */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);
        /* Queue now full (cap=2: SUBSCRIBE_OK + subgroup header). */

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"X", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) ==
                        MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(p);

        /* Drain, then write succeeds with object_id=0 (not advanced). */
        moq_action_t drain_acts[4];
        size_t nd = moq_session_poll_actions(sv, drain_acts, 4);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);

        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"Y", 1, &p2);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p2, 0) == MOQ_OK);
        moq_rcbuf_decref(p2);

        nd = moq_session_poll_actions(sv, drain_acts, 4);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_acts[di]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Queue full: open_subgroup returns WOULD_BLOCK, retry works --- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        /* Queue full (cap=1, SUBSCRIBE_OK). */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg) ==
            MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_subgroup_is_valid(sg));

        /* Drain, retry. */
        moq_action_t drain_a[4];
        size_t nd = moq_session_poll_actions(sv, drain_a, 4);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_a[di]);
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv,
            ev.u.subscribe_request.sub, &sg_cfg, 0, &sg) == MOQ_OK);
        MOQ_TEST_CHECK(moq_subgroup_is_valid(sg));

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Queue full: close_subgroup WOULD_BLOCK, handle stays valid --- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);
        /* Queue full (cap=2: SUBSCRIBE_OK + open). */

        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) ==
                        MOQ_ERR_WOULD_BLOCK);

        /* Drain, retry close. */
        moq_action_t drain_a[4];
        size_t nd = moq_session_poll_actions(sv, drain_a, 4);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_a[di]);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Queue full: reset_subgroup WOULD_BLOCK, handle stays valid --- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sv_extra = MOQ_SESSION_CFG_INIT;
        sv_extra.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sv_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);

        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) ==
                        MOQ_ERR_WOULD_BLOCK);

        moq_action_t drain_a[4];
        size_t nd = moq_session_poll_actions(sv, drain_a, 4);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_a[di]);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Subgroup handle helpers -------------------------------------- */
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
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ev.u.subscribe_request.sub, &sg_cfg, 0, &sg);

        /* INVALID is not valid. */
        moq_subgroup_handle_t inv = MOQ_SUBGROUP_INVALID;
        MOQ_TEST_CHECK(!moq_subgroup_is_valid(inv));

        /* Live handle is valid. */
        MOQ_TEST_CHECK(moq_subgroup_is_valid(sg));
        MOQ_TEST_CHECK(moq_subgroup_hash(sg) != 0);
        MOQ_TEST_CHECK(moq_subgroup_id_for_trace(sg) == sg._opaque);
        MOQ_TEST_CHECK(moq_subgroup_eq(sg, sg));
        MOQ_TEST_CHECK(!moq_subgroup_eq(sg, inv));

        /* Close + drain + advance → handle stale for operations. */
        moq_action_t drain_a[8];
        size_t nd = moq_session_poll_actions(sv, drain_a, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_a[di]);
        moq_session_close_subgroup(sv, sg, 0);
        nd = moq_session_poll_actions(sv, drain_a, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&drain_a[di]);
        moq_session_tick(sv, 1); /* reaps CLOSING entry */

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"X", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) ==
                        MOQ_ERR_STALE_HANDLE);
        moq_rcbuf_decref(p);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- action_cleanup idempotent ------------------------------------ */
    {
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.kind = MOQ_ACTION_SEND_CONTROL;
        moq_action_cleanup(&act);

        act.kind = MOQ_ACTION_SEND_DATA;
        act.u.send_data.payload = NULL;
        moq_action_cleanup(&act);
        moq_action_cleanup(&act);
    }


    /* == Streaming send: begin + data + end → whole-object receive ===== */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 10, 0) == MOQ_OK);
        moq_rcbuf_t *d1 = NULL, *d2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &d1);
        moq_rcbuf_create(&alloc, (const uint8_t *)"world", 5, &d2);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d2, 0) == MOQ_OK);
        moq_rcbuf_decref(d1);
        moq_rcbuf_decref(d2);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(300);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 10);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
            "helloworld", 10) == 0);
        moq_event_cleanup(&ev);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: received as OBJECT_CHUNK events =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 8, 0) == MOQ_OK);
        moq_rcbuf_t *d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"streamed", 8, &d);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        moq_rcbuf_decref(d);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(301);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        /* Header and payload arrive as separate SEND_DATA actions, so
         * the receiver may emit begin (0 bytes) then payload chunk. */
        uint8_t reasm[8];
        size_t rlen = 0;
        bool saw_begin = false, saw_end = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
            if (ev.u.object_chunk.begin) saw_begin = true;
            if (ev.u.object_chunk.chunk) {
                size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
                if (rlen + cl <= sizeof(reasm))
                    memcpy(reasm + rlen,
                        moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
                rlen += cl;
            }
            if (ev.u.object_chunk.end) saw_end = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_begin);
        MOQ_TEST_CHECK(saw_end);
        MOQ_TEST_CHECK(rlen == 8);
        MOQ_TEST_CHECK(memcmp(reasm, "streamed", 8) == 0);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: zero-length object ============================ */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(302);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: error cases =================================== */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &d);

        /* write_object_data before begin → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) ==
            MOQ_ERR_WRONG_STATE);

        /* end_object before begin → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) ==
            MOQ_ERR_WRONG_STATE);

        /* begin, then double begin → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 5, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 1, 5, 0) ==
            MOQ_ERR_WRONG_STATE);

        /* write_object while streaming → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 1, d, 0) ==
            MOQ_ERR_WRONG_STATE);

        /* close_subgroup while streaming → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) ==
            MOQ_ERR_WRONG_STATE);

        /* overflow: write more than declared → INVAL, no state change. */
        moq_rcbuf_t *big = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"toolong", 7, &big);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, big, 0) ==
            MOQ_ERR_INVAL);
        moq_rcbuf_decref(big);

        /* end before full payload → INVAL, stays STREAMING. */
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_ERR_INVAL);

        /* Write remaining bytes, then end succeeds. */
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        moq_rcbuf_t *d4 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"rest", 4, &d4);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d4, 0) == MOQ_OK);
        moq_rcbuf_decref(d4);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        moq_rcbuf_decref(d);

        /* zero-length write_object_data → no-op success. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 1, 0, 0) == MOQ_OK);
        moq_rcbuf_t *empty = NULL;
        moq_rcbuf_create(&alloc, NULL, 0, &empty);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, empty, 0) == MOQ_OK);
        moq_rcbuf_decref(empty);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        /* reset while streaming → succeeds. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 2, 100, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0x1, 0) == MOQ_OK);

        moq_action_t acts[32]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 32)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: WOULD_BLOCK on begin, retry succeeds ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        /* Drain SUBSCRIBE_OK so open_subgroup can succeed. */
        { moq_action_t a; moq_session_poll_actions(sv, &a, 1); moq_action_cleanup(&a); }

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        /* Don't drain subgroup header — action queue full (max_actions=1). */

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 5, 0) ==
            MOQ_ERR_WOULD_BLOCK);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 5, 0) == MOQ_OK);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_rcbuf_t *d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"abcde", 5, &d);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        moq_rcbuf_decref(d);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: WOULD_BLOCK on write_object_data, no advance = */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        /* Drain SUBSCRIBE_OK. */
        { moq_action_t a; moq_session_poll_actions(sv, &a, 1); moq_action_cleanup(&a); }

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        /* Drain subgroup header. */
        { moq_action_t a; moq_session_poll_actions(sv, &a, 1); moq_action_cleanup(&a); }

        /* begin uses 1 slot. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 10, 0) == MOQ_OK);

        moq_rcbuf_t *d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &d);

        /* First data uses slot 2 — queue full. */
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) ==
            MOQ_ERR_WOULD_BLOCK);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        moq_rcbuf_decref(d);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: multiple objects on same subgroup ============= */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        for (uint64_t oid = 0; oid < 3; oid++) {
            uint8_t buf[4];
            buf[0] = (uint8_t)('A' + oid);
            buf[1] = buf[2] = buf[3] = buf[0];
            MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, oid, 4, 0) == MOQ_OK);
            moq_rcbuf_t *d = NULL;
            moq_rcbuf_create(&alloc, buf, 4, &d);
            MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
            moq_rcbuf_decref(d);
            MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);
        }

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(303);
        moq_action_t acts[32]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 32)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        for (uint64_t oid = 0; oid < 3; oid++) {
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
            MOQ_TEST_CHECK(ev.u.object_received.object_id == oid);
            MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 4);
            moq_event_cleanup(&ev);
        }

        moq_session_close_subgroup(sv, sg, 0);
        while ((na = moq_session_poll_actions(sv, acts, 32)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming send: on_data_stop while streaming ================== */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Capture the stream_ref from the subgroup header action. */
        moq_stream_ref_t stream_ref = moq_stream_ref_from_u64(0);
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(sv, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                stream_ref = acts[i].u.send_data.stream_ref;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(stream_ref._v != 0);

        /* Begin streaming an object. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 100, 0) == MOQ_OK);
        moq_rcbuf_t *d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"partial", 7, &d);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) == MOQ_OK);
        moq_rcbuf_decref(d);

        /* Drain begin + data actions. */
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Simulate STOP_SENDING from peer. */
        MOQ_TEST_CHECK(moq_session_on_data_stop(sv, stream_ref, 0x1, 0) == MOQ_OK);

        /* Should queue RESET_DATA. */
        na = moq_session_poll_actions(sv, acts, 16);
        bool got_reset = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_RESET_DATA) got_reset = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(got_reset);

        /* Subgroup should now be RESETTING — further writes fail. */
        d = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &d);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, d, 0) ==
            MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 1, 5, 0) ==
            MOQ_ERR_STALE_HANDLE);
        moq_rcbuf_decref(d);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Wrapped rcbuf: write_object release on action cleanup ========= */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        static const uint8_t ext_data[] = "external";
        g_wrap_release_count = 0;
        moq_rcbuf_t *wp = NULL;
        moq_rcbuf_wrap(&alloc, ext_data, 8, wrap_release_fn,
            &g_wrap_release_count, &wp);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, wp, 0) == MOQ_OK);
        moq_rcbuf_decref(wp);
        MOQ_TEST_CHECK(g_wrap_release_count == 0);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(g_wrap_release_count == 1);

        moq_session_close_subgroup(sv, sg, 0);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Wrapped rcbuf: write_object_data release on action cleanup ==== */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 0, 5, 0) == MOQ_OK);

        static const uint8_t ext_data[] = "chunk";
        g_wrap_release_count = 0;
        moq_rcbuf_t *wp = NULL;
        moq_rcbuf_wrap(&alloc, ext_data, 5, wrap_release_fn,
            &g_wrap_release_count, &wp);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, wp, 0) == MOQ_OK);
        moq_rcbuf_decref(wp);
        MOQ_TEST_CHECK(g_wrap_release_count == 0);

        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(g_wrap_release_count == 1);

        moq_session_close_subgroup(sv, sg, 0);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Wrapped rcbuf: destroy releases queued wrapped payloads ======= */
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
        moq_subscription_t csub;
        moq_session_subscribe(c, &sub_cfg, 0, &csub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        static const uint8_t ext_data[] = "destroy";
        g_wrap_release_count = 0;
        moq_rcbuf_t *wp = NULL;
        moq_rcbuf_wrap(&alloc, ext_data, 7, wrap_release_fn,
            &g_wrap_release_count, &wp);
        moq_session_write_object(sv, sg, 0, wp, 0);
        moq_rcbuf_decref(wp);
        MOQ_TEST_CHECK(g_wrap_release_count == 0);

        /* Destroy without draining — session cleanup must release. */
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(g_wrap_release_count == 1);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == write_object_ex with properties on extensions subgroup ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg) == MOQ_OK);

        moq_rcbuf_t *payload = NULL, *props = NULL;
        uint8_t pay_data[] = {0xCA, 0xFE};
        /* Valid KVP: key=1, value_len=1, value=0xAA */
        uint8_t prop_data[] = {0x01, 0x01, 0xAA};
        moq_rcbuf_create(&alloc, pay_data, 2, &payload);
        moq_rcbuf_create(&alloc, prop_data, 3, &props);

        moq_object_cfg_t ocfg; moq_object_cfg_init(&ocfg);
        ocfg.object_id = 0;
        ocfg.payload = payload;
        ocfg.properties = props;
        MOQ_TEST_CHECK(moq_session_write_object_ex(sv, sg, &ocfg, 0) == MOQ_OK);
        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);

        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(500);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        bool got_obj = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                got_obj = true;
                MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 2);
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_received.payload)[0] == 0xCA);
                MOQ_TEST_CHECK(ev.u.object_received.properties != NULL);
                MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.properties) == 3);
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_received.properties)[2] == 0xAA);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_obj);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == write_object_ex without properties on extensions subgroup === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.object_properties = true;
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg);

        moq_rcbuf_t *payload = NULL;
        uint8_t pay_data[] = {0xDE, 0xAD};
        moq_rcbuf_create(&alloc, pay_data, 2, &payload);

        moq_object_cfg_t ocfg; moq_object_cfg_init(&ocfg);
        ocfg.object_id = 0;
        ocfg.payload = payload;
        MOQ_TEST_CHECK(moq_session_write_object_ex(sv, sg, &ocfg, 0) == MOQ_OK);
        moq_rcbuf_decref(payload);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(501);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        bool got_obj = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                got_obj = true;
                MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 2);
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_received.payload)[0] == 0xDE);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_obj);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Properties on non-extensions subgroup returns INVAL ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        /* object_properties=false (default) */
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg);

        moq_rcbuf_t *payload = NULL, *props = NULL;
        uint8_t d[] = {1};
        moq_rcbuf_create(&alloc, d, 1, &payload);
        moq_rcbuf_create(&alloc, d, 1, &props);

        moq_object_cfg_t ocfg; moq_object_cfg_init(&ocfg);
        ocfg.object_id = 0;
        ocfg.payload = payload;
        ocfg.properties = props;
        MOQ_TEST_CHECK(moq_session_write_object_ex(sv, sg, &ocfg, 0) ==
                       MOQ_ERR_INVAL);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == ABI regression: old struct_size must not read object_properties */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        /* Simulate old caller: set struct_size to exclude object_properties.
         * The padding + object_properties may contain garbage. */
        moq_subgroup_cfg_t sgcfg;
        memset(&sgcfg, 0xFF, sizeof(sgcfg));  /* fill with garbage */
        sgcfg.struct_size = offsetof(moq_subgroup_cfg_t, object_properties);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        /* object_properties is in garbage territory (0xFF). */

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg) == MOQ_OK);

        /* Verify no extensions bit on the wire. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x01) == 0);
        moq_action_cleanup(&act);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == ABI regression: old struct_size must not read end_of_group ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.struct_size = offsetof(moq_subgroup_cfg_t, end_of_group);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        sgcfg.end_of_group = true; /* Must be ignored: outside struct_size. */

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg) == MOQ_OK);

        /* EOG bit (0x08) must NOT be set despite garbage padding. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) == 0);
        moq_action_cleanup(&act);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == ABI regression: new struct_size with end_of_group=true ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t2");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg) == MOQ_OK);

        /* EOG bit (0x08) must be set. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) != 0);
        moq_action_cleanup(&act);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == begin_object_ex: streaming with properties =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc; ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true; ccfg.initial_request_capacity = 10;
        ccfg.streaming_objects = true;
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc; scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true; scfg.initial_request_capacity = 10;
        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        { moq_event_t e; moq_session_poll_events(c, &e, 1);
          moq_session_poll_events(sv, &e, 1); }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, sv_sub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.object_properties = true;
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg);

        moq_rcbuf_t *props = NULL;
        uint8_t prop_data[] = {0x01, 0x01, 0xBB};
        moq_rcbuf_create(&alloc, prop_data, 3, &props);

        moq_begin_object_cfg_t bcfg;
        moq_begin_object_cfg_init(&bcfg);
        bcfg.object_id = 0;
        bcfg.payload_length = 4;
        bcfg.properties = props;
        MOQ_TEST_CHECK(moq_session_begin_object_ex(sv, sg, &bcfg, 0) == MOQ_OK);
        moq_rcbuf_decref(props);

        moq_rcbuf_t *chunk = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"abcd", 4, &chunk);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, chunk, 0) == MOQ_OK);
        moq_rcbuf_decref(chunk);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(600);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        bool got_begin = false;
        size_t total_data = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
                if (ev.u.object_chunk.begin) {
                    got_begin = true;
                    MOQ_TEST_CHECK(ev.u.object_chunk.properties != NULL);
                    MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.properties) == 3);
                    MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_chunk.properties)[2] == 0xBB);
                }
                if (ev.u.object_chunk.chunk)
                    total_data += moq_rcbuf_len(ev.u.object_chunk.chunk);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_begin);
        MOQ_TEST_CHECK(total_data == 4);

        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == begin_object_ex: zero-length streaming with properties ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, sv_sub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.object_properties = true;
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg);

        moq_rcbuf_t *props = NULL;
        uint8_t prop_data[] = {0x01, 0x01, 0xCC};
        moq_rcbuf_create(&alloc, prop_data, 3, &props);

        moq_begin_object_cfg_t bcfg;
        moq_begin_object_cfg_init(&bcfg);
        bcfg.object_id = 0;
        bcfg.payload_length = 0;
        bcfg.properties = props;
        MOQ_TEST_CHECK(moq_session_begin_object_ex(sv, sg, &bcfg, 0) == MOQ_OK);
        moq_rcbuf_decref(props);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(601);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        bool got_obj = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                got_obj = true;
                MOQ_TEST_CHECK(ev.u.object_received.properties != NULL);
                MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.properties) == 3);
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_received.properties)[2] == 0xCC);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_obj);

        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == WOULD_BLOCK on data bytes: drain + retry recovers ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc; ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true; ccfg.initial_request_capacity = 10;
        ccfg.max_events = 1;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc; scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true; scfg.initial_request_capacity = 10;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        { moq_event_t e;
          moq_session_poll_events(c, &e, 1);
          moq_session_poll_events(sv, &e, 1); }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns; sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, sv_sub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);  /* drain subscribe_ok; fills event slot */

        /* Server writes two objects. */
        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sgcfg, 0, &sg);
        moq_rcbuf_t *p1 = NULL, *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"AAA", 3, &p1);
        moq_rcbuf_create(&alloc, (const uint8_t *)"BBB", 3, &p2);
        moq_session_write_object(sv, sg, 0, p1, 0);
        moq_session_write_object(sv, sg, 1, p2, 0);
        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_session_close_subgroup(sv, sg, 0);

        /* Feed all data to client. With max_events=1, the first object
         * fills the event queue and the second returns WOULD_BLOCK. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(700);
        moq_action_t acts[16]; size_t na;
        moq_result_t last_rc = MOQ_OK;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        acts[i].u.send_data.fin && !acts[i].u.send_data.payload, 0);
                    if (rc == MOQ_OK && acts[i].u.send_data.payload)
                        rc = moq_session_on_data_bytes(c, rx_ref,
                            moq_rcbuf_data(acts[i].u.send_data.payload),
                            moq_rcbuf_len(acts[i].u.send_data.payload),
                            acts[i].u.send_data.fin, 0);
                    last_rc = rc;
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Session should still be alive (not closed). */
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Drain first object. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 3);
        moq_event_cleanup(&ev);

        /* Retry the blocked data stream. */
        moq_result_t retry_rc = moq_session_on_data_bytes(
            c, rx_ref, NULL, 0, false, 0);
        /* Should succeed or produce the pending object. */

        /* Drain second object. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 3);
        MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.object_received.payload)[0] == 'B');
        moq_event_cleanup(&ev);

        (void)last_rc; (void)retry_rc;
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* reliable terminal status object: a zero-length END_OF_TRACK on a subgroup
     * stream (moq_session_write_status_object). Pins the encoder: the object
     * header carries the status varint (0x4), not a hardcoded NORMAL. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(c, &sub_cfg, 0, &sub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, sv_sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 0, &sg);
        moq_action_t act;
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        /* Object 0: a normal object, so the terminal is not the first. */
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"A", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        /* Terminal END_OF_TRACK object (id 1, delta 0): header is
         * delta(0) + payload_len(0) + status(0x4), no payload. */
        MOQ_TEST_CHECK(moq_session_write_status_object(
            sv, sg, 1, MOQ_OBJECT_END_OF_TRACK, 0) == MOQ_OK);
        moq_action_t sa;
        MOQ_TEST_CHECK(moq_session_poll_actions(sv, &sa, 1) == 1);
        MOQ_TEST_CHECK(sa.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(sa.u.send_data.header_len == 3);
        MOQ_TEST_CHECK(sa.u.send_data.header[0] == 0x00);   /* object id delta */
        MOQ_TEST_CHECK(sa.u.send_data.header[1] == 0x00);   /* payload length 0 */
        MOQ_TEST_CHECK(sa.u.send_data.header[2] == 0x04);   /* END_OF_TRACK */
        MOQ_TEST_CHECK(sa.u.send_data.payload == NULL);
        moq_action_cleanup(&sa);

        /* NORMAL has no zero-length stream meaning -> rejected. */
        MOQ_TEST_CHECK(moq_session_write_status_object(
            sv, sg, 2, MOQ_OBJECT_NORMAL, 0) == MOQ_ERR_INVAL);

        moq_session_close_subgroup(sv, sg, 0);
        moq_session_poll_actions(sv, &act, 1);
        moq_action_cleanup(&act);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_subgroup");
    return failures;
}
