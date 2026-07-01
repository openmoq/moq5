#include "test_session_support.h"

/* -- Feed helpers -------------------------------------------------- */

static moq_result_t feed_publish_namespace(moq_session_t *s,
                                            uint64_t request_id,
                                            const char *ns_field,
                                            const moq_kvp_entry_t *params,
                                            size_t params_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { moq_bytes_cstr(ns_field) };
    moq_namespace_t ns = { parts, 1 };
    moq_result_t rc = moq_d16_encode_publish_namespace(&w, request_id,
                                                        &ns, params,
                                                        params_count);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

static moq_result_t feed_request_ok(moq_session_t *s,
                                     uint64_t request_id,
                                     const moq_kvp_entry_t *params,
                                     size_t params_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_d16_encode_request_ok(&w, request_id,
                                                  params, params_count);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

static moq_result_t feed_request_error(moq_session_t *s,
                                        uint64_t request_id,
                                        uint64_t error_code,
                                        const char *reason)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    size_t rlen = reason ? strlen(reason) : 0;
    moq_result_t rc = moq_d16_encode_request_error(&w, request_id,
        error_code, 0, (const uint8_t *)reason, rlen);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

static moq_result_t feed_publish_namespace_done(moq_session_t *s,
                                                 uint64_t request_id)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_d16_encode_varint_msg(&w,
        MOQ_D16_PUBLISH_NAMESPACE_DONE, request_id);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

static moq_result_t feed_publish_namespace_cancel(moq_session_t *s,
                                                   uint64_t request_id,
                                                   uint64_t error_code,
                                                   const char *reason)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    size_t rlen = reason ? strlen(reason) : 0;
    moq_result_t rc = moq_d16_encode_publish_namespace_cancel(&w,
        request_id, error_code, (const uint8_t *)reason, rlen);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

int main(void)
{
    int failures = 0;

    /* == 1. Happy path: server publishes, client accepts ============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Server publishes namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 1000,
                                                      &ann) == MOQ_OK);
        MOQ_TEST_CHECK(moq_announcement_is_valid(ann));

        /* Pump to client. */
        pump_actions_to_peer(sv, c, 1000);

        /* Client sees NAMESPACE_PUBLISHED. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.track_namespace.count == 1);
        MOQ_TEST_CHECK(ev.u.namespace_published.track_namespace.parts[0].len == 4);
        MOQ_TEST_CHECK(memcmp(ev.u.namespace_published.track_namespace.parts[0].data,
                               "live", 4) == 0);

        /* Client accepts. */
        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_namespace(c,
            ev.u.namespace_published.ann, &acc, 1000) == MOQ_OK);

        /* Pump REQUEST_OK back to server. */
        pump_actions_to_peer(c, sv, 1000);

        /* Server sees NAMESPACE_ACCEPTED. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);
        MOQ_TEST_CHECK(moq_announcement_eq(ev.u.namespace_accepted.ann, ann));

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 2. Reject path: server publishes, client rejects ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        /* Client rejects. */
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rej.reason = MOQ_BYTES_LITERAL("no thanks");
        MOQ_TEST_CHECK(moq_session_reject_namespace(c,
            ev.u.namespace_published.ann, &rej, 1000) == MOQ_OK);

        /* Pump REQUEST_ERROR back to server. */
        pump_actions_to_peer(c, sv, 1000);

        /* Server sees NAMESPACE_REJECTED. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_REJECTED);
        MOQ_TEST_CHECK(ev.u.namespace_rejected.error_code ==
                        MOQ_REQUEST_ERROR_NOT_SUPPORTED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 3. Done path: server publishes, client accepts, server done == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);

        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_session_accept_namespace(c, ev.u.namespace_published.ann,
                                      &acc, 1000);
        pump_actions_to_peer(c, sv, 1000);

        /* Server sees NAMESPACE_ACCEPTED. */
        moq_event_t sv_ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &sv_ev, 1) == 1);
        MOQ_TEST_CHECK(sv_ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);

        /* Server calls publish_namespace_done. */
        MOQ_TEST_CHECK(moq_session_publish_namespace_done(sv, ann,
                                                           2000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);

        /* Client sees NAMESPACE_DONE. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_DONE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 4. Cancel after accept: client cancels established namespace = */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_announcement_t client_ann = ev.u.namespace_published.ann;

        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_session_accept_namespace(c, client_ann, &acc, 1000);
        pump_actions_to_peer(c, sv, 1000);

        /* Drain NAMESPACE_ACCEPTED from server. */
        moq_event_t sv_ev;
        moq_session_poll_events(sv, &sv_ev, 1);

        /* Client cancels established namespace. */
        moq_cancel_namespace_cfg_t can;
        moq_cancel_namespace_cfg_init(&can);
        can.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        MOQ_TEST_CHECK(moq_session_cancel_namespace(c, client_ann,
                                                     &can, 2000) == MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);

        /* Server sees NAMESPACE_CANCELLED (from PUBLISH_NAMESPACE_CANCEL). */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_CANCELLED);
        MOQ_TEST_CHECK(ev.u.namespace_cancelled.error_code ==
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 5. Done after cancel -> STALE_HANDLE ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_announcement_t client_ann = ev.u.namespace_published.ann;

        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_session_accept_namespace(c, client_ann, &acc, 1000);
        pump_actions_to_peer(c, sv, 1000);

        /* Drain NAMESPACE_ACCEPTED. */
        moq_event_t sv_ev;
        moq_session_poll_events(sv, &sv_ev, 1);

        moq_cancel_namespace_cfg_t can;
        moq_cancel_namespace_cfg_init(&can);
        moq_session_cancel_namespace(c, client_ann, &can, 2000);
        pump_actions_to_peer(c, sv, 2000);

        /* Server tries publish_namespace_done -> STALE_HANDLE. */
        MOQ_TEST_CHECK(moq_session_publish_namespace_done(sv, ann,
                                                           3000) == MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 6. Duplicate outgoing: same namespace twice -> INVAL ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann1, ann2;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 1000,
                                                      &ann1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 2000,
                                                      &ann2) == MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 7. Duplicate incoming: two PUBLISH_NAMESPACE same ns ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed two PUBLISH_NAMESPACE with same ns but different request IDs.
         * Server perspective: peer is client, so expect even parity. */
        feed_publish_namespace(sv, 0, "ns", NULL, 0);
        feed_publish_namespace(sv, 2, "ns", NULL, 0);

        moq_event_t ev[2];
        MOQ_TEST_CHECK(moq_session_poll_events(sv, ev, 2) == 2);
        MOQ_TEST_CHECK(ev[0].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev[1].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 8. Request credit blocked: peer_max=0 ======================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Client does not send MAX_REQUEST_ID, so server has no credit. */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 0, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 1000,
            &ann) == MOQ_ERR_REQUEST_BLOCKED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 9. Request ID parity: wrong parity -> close ================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed PUBLISH_NAMESPACE with odd request ID to server
         * (server expects even from client peer). */
        feed_publish_namespace(sv, 1, "ns", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 10. Unknown message param -> close =========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed PUBLISH_NAMESPACE with unknown param type 0x98 (even = varint). */
        uint8_t v1[1] = {0x01};
        moq_kvp_entry_t params[1] = {{
            .type = 0x98,
            .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};
        feed_publish_namespace(sv, 0, "ns", params, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 11. AUTH_TOKEN tolerated ====================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Encode a valid USE_VALUE auth token. */
        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 1,
            .token_value = (const uint8_t *)"tok",
            .token_value_len = 3,
        };
        uint8_t tb[32];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &tok);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_publish_namespace(sv, 0, "ns", params, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.token_count == 1);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens[0].token_type == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 12. REQUEST_OK with params -> close ========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Server publishes namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);

        /* Feed REQUEST_OK with params_count > 0 back to server. */
        /* Server's request_id is 1 (server perspective = odd). */
        uint8_t v1[1] = {0x01};
        moq_kvp_entry_t params[1] = {{
            .type = 0x08, .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};
        feed_request_ok(sv, 1, params, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 13. REQUEST_OK routes to announcement, not subscription ====== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Server subscribes (gets request_id 1). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("track");

        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(sv, &sub_cfg, 1000,
                                              &sub) == MOQ_OK);

        /* Server publishes namespace (gets request_id 3). */
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 2000,
                                                      &ann) == MOQ_OK);

        /* Feed REQUEST_OK for the publish_namespace request_id (3).
         * It must route to announcement, not subscription. */
        feed_request_ok(sv, 3, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Server sees NAMESPACE_ACCEPTED. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 14. Handle stale after error/done/cancel ===================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Test stale after error. */
        {
            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };

            moq_publish_namespace_cfg_t pn_cfg;
            moq_publish_namespace_cfg_init(&pn_cfg);
            pn_cfg.track_namespace = ns;

            moq_announcement_t ann;
            moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);

            /* Feed REQUEST_ERROR for this announcement. */
            feed_request_error(sv, 1, 0x10, "gone");

            moq_event_t ev;
            moq_session_poll_events(sv, &ev, 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_REJECTED);

            /* Handle should be stale now. */
            MOQ_TEST_CHECK(moq_session_publish_namespace_done(sv, ann,
                2000) == MOQ_ERR_STALE_HANDLE);

            moq_session_destroy(c);
            moq_session_destroy(sv);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        /* Test stale after done (receiver side). */
        {
            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };

            moq_publish_namespace_cfg_t pn_cfg;
            moq_publish_namespace_cfg_init(&pn_cfg);
            pn_cfg.track_namespace = ns;

            moq_announcement_t ann;
            moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
            pump_actions_to_peer(sv, c, 1000);

            moq_event_t ev;
            moq_session_poll_events(c, &ev, 1);
            moq_announcement_t client_ann = ev.u.namespace_published.ann;

            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            moq_session_accept_namespace(c, client_ann, &acc, 1000);
            pump_actions_to_peer(c, sv, 1000);

            /* Drain NAMESPACE_ACCEPTED. */
            moq_event_t sv_ev2;
            moq_session_poll_events(sv, &sv_ev2, 1);

            /* Done. */
            moq_session_publish_namespace_done(sv, ann, 2000);
            pump_actions_to_peer(sv, c, 2000);
            moq_session_poll_events(c, &ev, 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_DONE);

            /* Client handle should be stale. */
            moq_cancel_namespace_cfg_t can;
            moq_cancel_namespace_cfg_init(&can);
            MOQ_TEST_CHECK(moq_session_cancel_namespace(c, client_ann,
                &can, 3000) == MOQ_ERR_STALE_HANDLE);

            moq_session_destroy(c);
            moq_session_destroy(sv);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
    }

    /* == 15. Duplicate REQUEST_OK: second closes session ================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);

        /* First REQUEST_OK (request_id=1 for server). */
        feed_request_ok(sv, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Drain NAMESPACE_ACCEPTED event. */
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);

        /* Second REQUEST_OK for same request_id -> closes. */
        feed_request_ok(sv, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 16. REQUEST_ERROR after REQUEST_OK -> close =================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);

        feed_request_ok(sv, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* REQUEST_ERROR for the now-ESTABLISHED announcement -> close. */
        feed_request_error(sv, 1, 0x10, "late error");
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 17. CANCEL while PENDING_ANNOUNCER -> close =================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);

        /* PUBLISH_NAMESPACE_CANCEL while still PENDING_ANNOUNCER -> close. */
        feed_publish_namespace_cancel(sv, 1, 0x0, "nope");
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 18. DONE while PENDING_RECEIVER -> close ====================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed PUBLISH_NAMESPACE from client to server (request_id=0). */
        feed_publish_namespace(sv, 0, "ns", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        /* Feed PUBLISH_NAMESPACE_DONE before server accepts -> close.
         * State is PENDING_RECEIVER, not ESTABLISHED. */
        feed_publish_namespace_done(sv, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 19. DONE after CANCEL -> close (entry freed) ================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_announcement_t client_ann = ev.u.namespace_published.ann;

        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_session_accept_namespace(c, client_ann, &acc, 1000);
        pump_actions_to_peer(c, sv, 1000);

        /* Drain NAMESPACE_ACCEPTED. */
        moq_event_t sv_ev;
        moq_session_poll_events(sv, &sv_ev, 1);

        /* Client cancels established namespace. */
        moq_cancel_namespace_cfg_t can;
        moq_cancel_namespace_cfg_init(&can);
        moq_session_cancel_namespace(c, client_ann, &can, 2000);
        pump_actions_to_peer(c, sv, 2000);

        /* Drain NAMESPACE_CANCELLED. */
        moq_session_poll_events(sv, &sv_ev, 1);

        /* Feed PUBLISH_NAMESPACE_DONE for the freed request_id -> close. */
        feed_publish_namespace_done(sv, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == 20. Two duplicate incoming: DONE one, other still alive ======= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed two PUBLISH_NAMESPACE with same ns, different request IDs. */
        feed_publish_namespace(sv, 0, "ns", NULL, 0);
        feed_publish_namespace(sv, 2, "ns", NULL, 0);

        moq_event_t ev[2];
        MOQ_TEST_CHECK(moq_session_poll_events(sv, ev, 2) == 2);
        MOQ_TEST_CHECK(ev[0].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev[1].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        moq_announcement_t ann0 = ev[0].u.namespace_published.ann;
        moq_announcement_t ann1 = ev[1].u.namespace_published.ann;

        /* Accept both. */
        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_session_accept_namespace(sv, ann0, &acc, 1000);
        moq_session_accept_namespace(sv, ann1, &acc, 2000);

        /* DONE one (request_id=0). */
        feed_publish_namespace_done(sv, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t done_ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &done_ev, 1) == 1);
        MOQ_TEST_CHECK(done_ev.kind == MOQ_EVENT_NAMESPACE_DONE);

        /* The other (request_id=2) is still alive — cancel should work. */
        moq_cancel_namespace_cfg_t can;
        moq_cancel_namespace_cfg_init(&can);
        MOQ_TEST_CHECK(moq_session_cancel_namespace(sv, ann1, &can,
                                                     3000) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == max_announcements=1: pool recovery after done =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_announcements = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* A fills the single server announcement slot. */
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        moq_bytes_t ns_a[] = { MOQ_BYTES_LITERAL("na") };
        pn_cfg.track_namespace.parts = ns_a;
        pn_cfg.track_namespace.count = 1;
        moq_announcement_t ann_a;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 0,
            &ann_a) == MOQ_OK);

        /* B blocked — pool full. */
        moq_bytes_t ns_b[] = { MOQ_BYTES_LITERAL("nb") };
        pn_cfg.track_namespace.parts = ns_b;
        moq_announcement_t ann_b;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 0,
            &ann_b) == MOQ_ERR_WOULD_BLOCK);

        /* Complete A: pump to client, client accepts, pump back,
         * server calls publish_namespace_done. */
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        moq_announcement_t c_ann = ev.u.namespace_published.ann;
        moq_event_cleanup(&ev);

        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        MOQ_TEST_CHECK(moq_session_accept_namespace(c, c_ann, &nacc, 0)
            == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_publish_namespace_done(sv, ann_a, 0)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Retry B — should succeed now. */
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 0,
            &ann_b) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        pump_actions_to_peer(sv, c, 0);
        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(c, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
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
            MOQ_BYTES_LITERAL("live"),
        };
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = (moq_namespace_t){ ns_parts, 3 };

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(sv, &pn_cfg, 1000,
            &ann) == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.track_namespace.count == 3);
        MOQ_TEST_CHECK(memcmp(
            ev.u.namespace_published.track_namespace.parts[0].data,
            "org", 3) == 0);
        MOQ_TEST_CHECK(memcmp(
            ev.u.namespace_published.track_namespace.parts[1].data,
            "example", 7) == 0);
        MOQ_TEST_CHECK(memcmp(
            ev.u.namespace_published.track_namespace.parts[2].data,
            "live", 4) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Outbound PUBLISH_NAMESPACE with one auth token ================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        uint8_t tok_val[] = { 0xDD };
        moq_auth_token_t tok = {
            .token_type = 99,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        pn_cfg.auth_tokens = &tok;
        pn_cfg.auth_token_count = 1;

        moq_announcement_t ann;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_namespace(sv, &pn_cfg,
            1000, &ann), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.namespace_published.token_count, 1);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens != NULL);
        MOQ_TEST_CHECK_EQ_U64(
            ev.u.namespace_published.tokens[0].token_type, 99);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.namespace_published.tokens[0].token_value.len, 1);
        MOQ_TEST_CHECK(
            ev.u.namespace_published.tokens[0].token_value.data[0] == 0xDD);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == PUBLISH_NAMESPACE auth validation: count>0 but tokens==NULL === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pn_cfg.auth_tokens = NULL;
        pn_cfg.auth_token_count = 1;

        moq_announcement_t ann;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_namespace(sv, &pn_cfg,
            1000, &ann), (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == PUBLISH_NAMESPACE old struct_size caller: zero tokens ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pn_cfg.struct_size =
            offsetof(moq_publish_namespace_cfg_t, auth_tokens);

        moq_announcement_t ann;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_namespace(sv, &pn_cfg,
            1000, &ann), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.namespace_published.token_count, 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == queued event spans survive batched inbound messages =========== *
     * Two PUBLISH_NAMESPACE for distinct namespaces in one batch (no poll
     * between the advancing calls): both queued NAMESPACE_PUBLISHED
     * events keep their own namespace bytes (event borrow arena). */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_publish_namespace(sv, 0, "alpha", NULL, 0);
        feed_publish_namespace(sv, 2, "bravo", NULL, 0);

        moq_event_t ev[2];
        MOQ_TEST_CHECK(moq_session_poll_events(sv, ev, 2) == 2);
        MOQ_TEST_CHECK(ev[0].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev[0].u.namespace_published.track_namespace.count == 1);
        MOQ_TEST_CHECK(
            ev[0].u.namespace_published.track_namespace.parts[0].len == 5 &&
            memcmp(ev[0].u.namespace_published.track_namespace.parts[0].data,
                   "alpha", 5) == 0);
        MOQ_TEST_CHECK(ev[1].kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(
            ev[1].u.namespace_published.track_namespace.parts[0].len == 5 &&
            memcmp(ev[1].u.namespace_published.track_namespace.parts[0].data,
                   "bravo", 5) == 0);
        moq_event_cleanup(&ev[0]);
        moq_event_cleanup(&ev[1]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    MOQ_TEST_PASS("test_session_namespace");
    return failures;
}
