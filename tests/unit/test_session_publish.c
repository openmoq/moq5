#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

int main(void)
{
    int failures = 0;

    /* == PUBLISH request → accept → publisher receives OK ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("video");

        moq_publication_t pub_handle;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000,
            &pub_handle), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_publication_is_valid(pub_handle));

        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.track_namespace.count, 1);
        MOQ_TEST_CHECK(memcmp(ev.u.publish_request.track_namespace.parts[0].data,
            "live", 4) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.track_name.len, 5);

        moq_publication_t sv_pub = ev.u.publish_request.pub;

        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), (int)MOQ_OK);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_ok.pub, pub_handle));

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == PUBLISH request → reject → publisher receives error ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;

        moq_reject_publish_cfg_t rej;
        moq_reject_publish_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        rej.reason = MOQ_BYTES_LITERAL("denied");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(sv, sv_pub,
            &rej, 2000), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(sv, sv_pub,
            &rej, 2000), (int)MOQ_ERR_STALE_HANDLE);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_ERROR);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_error.pub, h));
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_error.error_code,
            (int)MOQ_REQUEST_ERROR_UNAUTHORIZED);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_error.reason.len, 6);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Late REQUEST_ERROR must not tear down an established publish == */
    /* REQUEST_ERROR is a terminal response to a still-pending PUBLISH. After
     * PUBLISH_OK has established the publication, a (malicious/duplicate) late
     * REQUEST_ERROR must be a protocol violation, not free the live publication. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub_h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &apc, 2000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(pub_resolve_handle(c, pub_h) >= 0);   /* established */

        /* Late REQUEST_ERROR for the (now established) publish request id 0. */
        uint8_t errbuf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, errbuf, sizeof(errbuf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_request_error(&w, 0,
            MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, errbuf,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);

        /* Protocol violation: session closes, no PUBLISH_ERROR is surfaced. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);
        bool saw_err = false;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_ERROR) saw_err = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Event WB on inbound PUBLISH: auth commit-last (REGISTER token) */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_events = 1;
        s_extra.send_auth_token_cache_size = true;
        s_extra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);
        /* establish_pair already drained both setup events; event queue empty. */

        /* Fill the 1-slot event queue: feed a no-auth PUBLISH_NAMESPACE
         * (request_id=0, client parity even) so sv's event queue is full. */
        {
            uint8_t fill_buf[128];
            moq_buf_writer_t fw;
            moq_buf_writer_init(&fw, fill_buf, sizeof(fill_buf));
            moq_bytes_t fill_parts[] = { MOQ_BYTES_LITERAL("fill") };
            moq_namespace_t fill_ns = { fill_parts, 1 };
            moq_d16_encode_publish_namespace(&fw, 0, &fill_ns, NULL, 0);
            moq_session_on_control_bytes(sv, fill_buf, moq_buf_writer_offset(&fw), 0);
        }
        /* Server queue now full (1/1): NAMESPACE_PUBLISHED for "fill". */

        /* Build an AUTH_TOKEN REGISTER param (alias=1, type=1, value="secret"). */
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 1,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg_tok);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};

        /* Encode PUBLISH with AUTH_TOKEN (request_id=2, next even ID). */
        uint8_t pub_buf[256];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, pub_buf, sizeof(pub_buf));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_publish_t wire_pub = {
            .request_id = 2,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("t"),
            .track_alias = 42,
            .params = params,
            .params_count = 1,
            .params_cap = 1,
            .track_extensions = NULL,
            .track_extensions_len = 0,
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&pw, &wire_pub), (int)MOQ_OK);

        /* Feed the PUBLISH with AUTH_TOKEN — event queue full → WOULD_BLOCK. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, pub_buf,
            moq_buf_writer_offset(&pw), 1000), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        /* Verify no publish entry was committed (pool still all-free). */
        bool found = false;
        for (size_t i = 0; i < sv->pub_cap; i++) {
            if (sv->publishes[i].state != MOQ_PUB_FREE) { found = true; break; }
        }
        MOQ_TEST_CHECK(!found);

        /* Auth alias must NOT be in cache yet (commit-last). */
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            NULL, NULL, NULL), MOQ_TOKEN_ERR_UNKNOWN);

        /* Drain the blocking event, then retry. */
        moq_event_t drain;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &drain, 1), 1);
        moq_event_cleanup(&drain);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        /* PUBLISH_REQUEST event should now be emitted. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        MOQ_TEST_CHECK(ev.u.publish_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.publish_request.tokens != NULL);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_request.tokens[0].token_type, 1);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.tokens[0].token_value.len, 6);
        MOQ_TEST_CHECK(memcmp(ev.u.publish_request.tokens[0].token_value.data,
                              "secret", 6) == 0);

        /* Auth alias committed exactly once — no duplicate-alias close. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
        uint64_t rtype; const uint8_t *rval; size_t rlen;
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            &rtype, &rval, &rlen), MOQ_TOKEN_OK);
        MOQ_TEST_CHECK_EQ_U64(rtype, 1);
        MOQ_TEST_CHECK_EQ_SIZE(rlen, 6);

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action WB on accept: no state mutation ====================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Use max_actions=1 and fill with a client PUBLISH action
         * that hasn't been drained yet. Since the server side is
         * what we test, we use max_actions=1 on the server and
         * pre-fill its queue with a separate publish accept. */
        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* First publish: fills the action queue after accept. */
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t1");

        moq_publication_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h1),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub1 = ev.u.publish_request.pub;

        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub1,
            &acfg, 2000), (int)MOQ_OK);
        /* Action queue now full (1/1). */

        /* Second publish. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 2000, &h2),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub2 = ev.u.publish_request.pub;

        /* Accept should WOULD_BLOCK — action queue full. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub2,
            &acfg, 3000), (int)MOQ_ERR_WOULD_BLOCK);

        int sv_slot = pub_resolve_handle(sv, sv_pub2);
        MOQ_TEST_CHECK(sv_slot >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv->publishes[sv_slot].state,
            (int)MOQ_PUB_PENDING_SUBSCRIBER);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Generic REQUEST_OK for publish closes session =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t drain;
        moq_session_poll_events(sv, &drain, 1);

        uint8_t ok_buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_request_ok(&w, 0, NULL, 0),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&w), 2000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Duplicate track alias from PUBLISH closes session ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes to establish an alias. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("sub_track");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 1000, &sub),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        /* Server accepts with a specific track alias. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t asub;
        moq_accept_subscribe_cfg_init(&asub);
        asub.has_track_alias = true;
        asub.track_alias = 42;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &asub, 1000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);

        /* Now craft a PUBLISH with the same track_alias=42 and feed
         * to the subscriber (server). */
        uint8_t pub_buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pub_buf, sizeof(pub_buf));
        moq_d16_publish_t wire_pub;
        memset(&wire_pub, 0, sizeof(wire_pub));
        wire_pub.request_id = 2;
        wire_pub.track_namespace = ns;
        wire_pub.track_name = MOQ_BYTES_LITERAL("pub_track");
        wire_pub.track_alias = 42;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&w, &wire_pub),
            (int)MOQ_OK);

        moq_session_on_control_bytes(sv, pub_buf,
            moq_buf_writer_offset(&w), 2000);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Explicit alias collision rejected ============================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t1");
        pcfg.has_track_alias = true;
        pcfg.track_alias = 99;

        moq_publication_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h1),
            (int)MOQ_OK);

        /* Second publish with same explicit alias must fail. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h2),
            (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!moq_publication_is_valid(h2));

        /* Session still alive. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Automatic alias skips used aliases ============================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* First publish takes an automatic alias. */
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t1");

        moq_publication_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h1),
            (int)MOQ_OK);
        int s1 = pub_resolve_handle(c, h1);
        MOQ_TEST_CHECK(s1 >= 0);
        uint64_t alias1 = c->publishes[s1].track_alias;

        /* Second publish must get a different automatic alias. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 2000, &h2),
            (int)MOQ_OK);
        int s2 = pub_resolve_handle(c, h2);
        MOQ_TEST_CHECK(s2 >= 0);
        uint64_t alias2 = c->publishes[s2].track_alias;

        MOQ_TEST_CHECK(alias1 != alias2);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == publish → accept → open_pub_subgroup → write_object → OBJECT_RECEIVED */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("track");

        moq_publication_t pub_handle;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub_handle),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_publication_is_valid(pub_handle));

        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t sv_pub = ev.u.publish_request.pub;

        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), (int)MOQ_OK);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_ok.pub, pub_handle));

        /* Client opens a subgroup on the accepted publication. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_handle,
            &sg_cfg, 3000, &sg), (int)MOQ_OK);

        /* Write one object with payload "hello". */
        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc,
            (const uint8_t *)"hello", 5, &payload), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(c, sg, 0, payload, 3000),
            (int)MOQ_OK);
        moq_rcbuf_decref(payload);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sg, 3000),
            (int)MOQ_OK);

        /* Manually pump client → server: SEND_CONTROL (PUBLISH_OK already
         * pumped above) and SEND_DATA actions (subgroup header, object, FIN). */
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(c, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_session_on_control_bytes(sv, acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 3000);
            } else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(
                    acts[i].u.send_data.stream_ref._v + 10000);
                FEED_SEND_DATA(sv, rx_ref, acts[i], 3000);
            }
            moq_action_cleanup(&acts[i]);
        }

        /* Server should receive OBJECT_RECEIVED with group=0, object=0,
         * payload="hello". */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK_EQ_U64(ev.u.object_received.group_id, 0);
        MOQ_TEST_CHECK_EQ_U64(ev.u.object_received.object_id, 0);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(ev.u.object_received.payload), 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
            "hello", 5) == 0);
        MOQ_TEST_CHECK(moq_publication_is_valid(ev.u.object_received.pub));
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action WB on open_pub_subgroup leaves state unchanged ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t pub_handle;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub_handle),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;

        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), (int)MOQ_OK);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);

        /* Drain any pending actions on client so action queue is empty. */
        moq_action_t drain_acts[8];
        size_t nd = moq_session_poll_actions(c, drain_acts, 8);
        for (size_t i = 0; i < nd; i++) moq_action_cleanup(&drain_acts[i]);

        /* First open_pub_subgroup uses the single action slot. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_handle,
            &sg_cfg, 3000, &sg1), (int)MOQ_OK);

        /* Second open_pub_subgroup should WOULD_BLOCK (action queue full). */
        moq_subgroup_cfg_t sg_cfg2;
        moq_subgroup_cfg_init(&sg_cfg2);
        sg_cfg2.group_id = 1;
        sg_cfg2.subgroup_id = 0;
        sg_cfg2.publisher_priority = 128;
        moq_subgroup_handle_t sg2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_handle,
            &sg_cfg2, 3000, &sg2), (int)MOQ_ERR_WOULD_BLOCK);

        /* Verify only one non-free subgroup entry exists. */
        size_t open_count = 0;
        for (size_t i = 0; i < c->sg_cap; i++)
            if (c->subgroups[i].state != MOQ_SG_FREE) open_count++;
        MOQ_TEST_CHECK_EQ_SIZE(open_count, 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Duplicate pub subgroup rejected =============================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv,
            ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 5;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;

        moq_subgroup_handle_t sg1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg1), (int)MOQ_OK);

        moq_subgroup_handle_t sg2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg2), (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Streaming OBJECT_CHUNK.pub set for publisher-initiated data == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.streaming_objects = true;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv,
            ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg), (int)MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc,
            (const uint8_t *)"chunk", 5, &payload), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(c, sg, 0,
            payload, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sg, 3000),
            (int)MOQ_OK);

        /* Pump data to server manually with stream_ref offset. */
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(c, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    acts[i].u.send_data.stream_ref._v + 10000);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(sv, ref,
                    acts[i].u.send_data.header, acts[i].u.send_data.header_len,
                    acts[i].u.send_data.fin, 3000), (int)MOQ_OK);
                if (acts[i].u.send_data.payload)
                    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(sv, ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        false, 3000), (int)MOQ_OK);
            }
        }

        /* Server should get OBJECT_CHUNK with pub set. */
        moq_event_t chunks[8];
        size_t nc = moq_session_poll_events(sv, chunks, 8);
        bool found_chunk = false;
        for (size_t i = 0; i < nc; i++) {
            if (chunks[i].kind == MOQ_EVENT_OBJECT_CHUNK && chunks[i].u.object_chunk.begin) {
                MOQ_TEST_CHECK(moq_publication_is_valid(chunks[i].u.object_chunk.pub));
                MOQ_TEST_CHECK(!moq_subscription_is_valid(chunks[i].u.object_chunk.sub));
                found_chunk = true;
            }
            moq_event_cleanup(&chunks[i]);
        }
        MOQ_TEST_CHECK(found_chunk);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* PUBLISH_DONE completion                                        */
    /* ============================================================== */

    /* == finish_publish → subscriber receives PUBLISH_FINISHED ======= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);

        /* Publisher finishes with Stream Count 0 (no data streams opened), so the
         * subscriber finalizes immediately -- this checks the finish-event
         * plumbing; Stream-Count gating is covered by the deferral test below. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = 0;
        fcfg.stream_count = 0;
        fcfg.reason = MOQ_BYTES_LITERAL("done");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 3000), (int)MOQ_OK);

        /* Handle should now be stale. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 3000), (int)MOQ_ERR_STALE_HANDLE);

        pump_actions_to_peer(c, sv, 3000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_finished.pub, sv_pub));
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.stream_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_finished.reason.len, 4);

        /* Subscriber handle now stale. */
        MOQ_TEST_CHECK(pub_resolve_handle(sv, sv_pub) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == PUBLISH_DONE defers finish until Stream Count streams drain ==== */
    /* PUBLISH_DONE (Stream Count = 1) arrives before the publication's data
     * stream (draft-16 Section 9.15: it is likely to precede late-opening
     * streams). The subscriber must keep the publication live, deliver the late
     * object, and only emit PUBLISH_FINISHED once the stream has been processed. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        /* Publisher opens a subgroup and writes one object; hold its data. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg), (int)MOQ_OK);
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"late", 4, &payload);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(c, sg, 0,
            payload, 3000), (int)MOQ_OK);
        moq_rcbuf_decref(payload);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sg, 3000),
            (int)MOQ_OK);

        /* Capture the held data-stream actions (subgroup header, object, FIN). */
        moq_action_t data_acts[16];
        size_t data_na = moq_session_poll_actions(c, data_acts, 16);

        /* Publisher finishes (Stream Count = 1). */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = 0;
        fcfg.stream_count = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h, &fcfg, 4000),
            (int)MOQ_OK);

        /* Deliver PUBLISH_DONE FIRST (reordered ahead of the data stream). */
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0) {
              if (a.kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(sv, a.u.send_control.data,
                      a.u.send_control.len, 4000);
              moq_action_cleanup(&a);
          } }

        /* PUBLISH_FINISHED is deferred: the stream has not been processed yet, so
         * the publication is still live (no terminal event). */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 0);
        MOQ_TEST_CHECK(pub_resolve_handle(sv, sv_pub) >= 0);

        /* Deliver the late data stream. Its object is delivered under the live
         * publication, then -- once the stream FINs and the processed count
         * reaches Stream Count -- PUBLISH_FINISHED is emitted and the pub freed. */
        for (size_t i = 0; i < data_na; i++) {
            if (data_acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(
                    data_acts[i].u.send_data.stream_ref._v + 10000);
                FEED_SEND_DATA(sv, rx_ref, data_acts[i], 4000);
            }
            moq_action_cleanup(&data_acts[i]);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
            (int)MOQ_SESS_ESTABLISHED);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL &&
            moq_rcbuf_len(ev.u.object_received.payload) == 4 &&
            memcmp(moq_rcbuf_data(ev.u.object_received.payload), "late", 4) == 0);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.stream_count, 1);
        moq_event_cleanup(&ev);

        /* Publication is removed only after the gated finish. */
        MOQ_TEST_CHECK(pub_resolve_handle(sv, sv_pub) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Action WB on finish_publish leaves handle valid =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t1");

        moq_publication_t h1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h1),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Fill the action queue with a second publish. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t h2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 3000, &h2),
            (int)MOQ_OK);
        /* Action queue full (1/1). */

        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h1,
            &fcfg, 3000), (int)MOQ_ERR_WOULD_BLOCK);

        /* Handle still valid. */
        MOQ_TEST_CHECK(pub_resolve_handle(c, h1) >= 0);

        /* Drain action queue and retry — finish should succeed. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h1,
            &fcfg, 4000), (int)MOQ_OK);
        MOQ_TEST_CHECK(pub_resolve_handle(c, h1) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Event WB on inbound PUBLISH_DONE: retry emits exactly once == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_events = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        moq_session_publish(c, &pcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Fill server event queue: send a second PUBLISH. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t h2;
        moq_session_publish(c, &pcfg, 3000, &h2);
        pump_actions_to_peer(c, sv, 3000);
        /* Server event queue: PUBLISH_REQUEST for t2 → full (1/1). */

        /* Publisher finishes first publish. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, h, &fcfg, 4000);
        pump_actions_to_peer(c, sv, 4000);

        /* PUBLISH_DONE should WB. Subscriber pub entry still valid. */
        MOQ_TEST_CHECK(pub_resolve_handle(sv, sv_pub) >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        /* Drain event, retry. */
        moq_session_poll_events(sv, &ev, 1);
        moq_session_process_pending(sv, 4000);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);

        /* Now stale. */
        MOQ_TEST_CHECK(pub_resolve_handle(sv, sv_pub) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Stale handle after finish: open_pub_subgroup fails =========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 3000), (int)MOQ_OK);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg), (int)MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Unknown request_id in PUBLISH_DONE closes session ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t done_buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, done_buf, sizeof(done_buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish_done(&w,
            999, 0, 0, NULL, 0), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, done_buf,
            moq_buf_writer_offset(&w), 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == finish_publish rejected while subgroup is open =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv,
            ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg), (int)MOQ_OK);

        /* finish must fail while subgroup is open. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 3000), (int)MOQ_ERR_WRONG_STATE);

        /* Close subgroup, then finish succeeds. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sg, 3000),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 4000), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == finish_publish on closed session returns error =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Force-close the session by feeding an unknown control message. */
        uint8_t bad[8];
        moq_buf_writer_t bw;
        moq_buf_writer_init(&bw, bad, sizeof(bad));
        moq_buf_write_varint(&bw, 0x7F);
        moq_buf_write_uint16(&bw, 0);
        moq_session_on_control_bytes(c, bad, moq_buf_writer_offset(&bw), 3000);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h,
            &fcfg, 4000), (int)MOQ_ERR_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* D16 message parameter scope semantics                          */
    /* ============================================================== */

    /* == Known-but-wrong-scope param on FETCH does not close ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a FETCH with a FORWARD param (defined in D16 but not
         * allowed on FETCH — should be ignored, not close). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(1, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_v, .value_len = fwd_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_fetch_t fetch = {
            .request_id = 0,
            .fetch_type = MOQ_D16_FETCH_TYPE_STANDALONE,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("t"),
            .end_group = 1,
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fetch, params, 1),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Known-but-wrong-scope param on PUBLISH does not close ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a PUBLISH with DELIVERY_TIMEOUT param (defined in D16
         * but not allowed on PUBLISH — should be ignored). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        uint8_t dt_v[8];
        size_t dt_len = moq_quic_varint_encode(5000, dt_v, sizeof(dt_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .value = dt_v, .value_len = dt_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_publish_t pub;
        memset(&pub, 0, sizeof(pub));
        pub.request_id = 0;
        pub.track_namespace = ns;
        pub.track_name = MOQ_BYTES_LITERAL("t");
        pub.track_alias = 1;
        pub.params = params;
        pub.params_count = 1;
        pub.params_cap = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&w, &pub),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Truly unknown param closes session =========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a PUBLISH with unknown param type 0xFE (even = varint). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        uint8_t dummy_v[8];
        size_t dummy_len = moq_quic_varint_encode(1, dummy_v, sizeof(dummy_v));
        moq_kvp_entry_t params[1] = {{
            .type = 0xFE,
            .value = dummy_v, .value_len = dummy_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_publish_t pub;
        memset(&pub, 0, sizeof(pub));
        pub.request_id = 0;
        pub.track_namespace = ns;
        pub.track_name = MOQ_BYTES_LITERAL("t");
        pub.track_alias = 1;
        pub.params = params;
        pub.params_count = 1;
        pub.params_cap = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&w, &pub),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE with wrong-scope EXPIRES: ignored, no error === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes and server accepts to get an established sub. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 1000, &sub),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_subscribe_cfg_t asub;
        moq_accept_subscribe_cfg_init(&asub);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv,
            ev.u.subscribe_request.sub, &asub, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Craft a REQUEST_UPDATE with only EXPIRES (wrong scope for
         * REQUEST_UPDATE). Should be ignored — no close, no error. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        uint8_t exp_v[8];
        size_t exp_len = moq_quic_varint_encode(5000, exp_v, sizeof(exp_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_EXPIRES,
            .value = exp_v, .value_len = exp_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};
        /* REQUEST_UPDATE: new_request_id=2, existing_request_id=0 */
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        /* Server should emit REQUEST_OK + SUBSCRIBE_UPDATED.
         * SUBSCRIBE_UPDATED only fires on the REQUEST_OK path —
         * if this were REQUEST_ERROR, we'd see no SUBSCRIBE_UPDATED. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);

        /* Decode the first byte of the control message to verify
         * it is REQUEST_OK (0x07), not REQUEST_ERROR (0x05). */
        if (acts[0].u.send_control.len > 0) {
            moq_buf_reader_t cr;
            moq_buf_reader_init(&cr, acts[0].u.send_control.data,
                acts[0].u.send_control.len);
            moq_control_envelope_t env;
            MOQ_TEST_CHECK_EQ_INT((int)moq_control_decode_envelope(&cr, &env),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)env.msg_type, (int)MOQ_D16_REQUEST_OK);
        }
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_event_t ev2;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev2, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev2.kind, (int)MOQ_EVENT_SUBSCRIBE_UPDATED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == Duplicate allowed param still closes ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Craft a PUBLISH with two FORWARD params (duplicate). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(1, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[2] = {
            { .type = MOQ_MSG_PARAM_FORWARD, .value = fwd_v, .value_len = fwd_len,
              .is_varint = true, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_FORWARD, .value = fwd_v, .value_len = fwd_len,
              .is_varint = true, .raw = NULL, .raw_len = 0 },
        };

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_publish_t pub;
        memset(&pub, 0, sizeof(pub));
        pub.request_id = 0;
        pub.track_namespace = ns;
        pub.track_name = MOQ_BYTES_LITERAL("t");
        pub.track_alias = 1;
        pub.params = params;
        pub.params_count = 2;
        pub.params_cap = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&w, &pub),
            (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* Batch 2: PUBLISH_OK semantics preservation                     */
    /* ============================================================== */

    /* == PUBLISH_OK with FORWARD=0 blocks open_pub_subgroup ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Subscriber accepts with FORWARD=0 via raw PUBLISH_OK. */
        uint8_t ok_buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(0, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t ok_params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_v, .value_len = fwd_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish_ok(&w,
            sv->publishes[0].request_id, ok_params, 1), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&w), 2000), (int)MOQ_OK);

        /* Client should get PUBLISH_OK with send_allowed=false. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);
        MOQ_TEST_CHECK(!ev.u.publish_ok.send_allowed);

        /* open_pub_subgroup must fail. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h,
            &sg_cfg, 3000, &sg), (int)MOQ_ERR_WRONG_STATE);

        /* No SEND_DATA should have been queued. */
        moq_action_t acts[4];
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(c, acts, 4), 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == PUBLISH_OK with params surfaced in event ===================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Build PUBLISH_OK with SUBSCRIBER_PRIORITY=64, GROUP_ORDER=2,
         * DELIVERY_TIMEOUT=5000, EXPIRES=30000. */
        uint8_t ok_buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));

        uint8_t pri_v[8], go_v[8], dt_v[8], exp_v[8];
        size_t pri_len = moq_quic_varint_encode(64, pri_v, sizeof(pri_v));
        size_t go_len = moq_quic_varint_encode(2, go_v, sizeof(go_v));
        size_t dt_len = moq_quic_varint_encode(5000, dt_v, sizeof(dt_v));
        size_t exp_len = moq_quic_varint_encode(30000, exp_v, sizeof(exp_v));
        moq_kvp_entry_t ok_params[4] = {
            { .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT, .value = dt_v,
              .value_len = dt_len, .is_varint = true },
            { .type = MOQ_MSG_PARAM_EXPIRES, .value = exp_v,
              .value_len = exp_len, .is_varint = true },
            { .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY, .value = pri_v,
              .value_len = pri_len, .is_varint = true },
            { .type = MOQ_MSG_PARAM_GROUP_ORDER, .value = go_v,
              .value_len = go_len, .is_varint = true },
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish_ok(&w,
            sv->publishes[0].request_id, ok_params, 4), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&w), 2000), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_OK);
        MOQ_TEST_CHECK(ev.u.publish_ok.send_allowed);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_ok.subscriber_priority, 64);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_ok.group_order,
            (int)MOQ_GROUP_ORDER_DESCENDING);
        MOQ_TEST_CHECK(ev.u.publish_ok.has_delivery_timeout);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_ok.delivery_timeout_ms, 5000);
        MOQ_TEST_CHECK(ev.u.publish_ok.has_expires);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_ok.expires_ms, 30000);

        /* Verify params persisted on pub entry after event is polled. */
        int ps = pub_resolve_handle(c, h);
        MOQ_TEST_CHECK(ps >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)c->publishes[ps].subscriber_priority, 64);
        MOQ_TEST_CHECK_EQ_INT((int)c->publishes[ps].group_order,
            (int)MOQ_GROUP_ORDER_DESCENDING);
        MOQ_TEST_CHECK(c->publishes[ps].has_delivery_timeout);
        MOQ_TEST_CHECK_EQ_U64(c->publishes[ps].delivery_timeout_ms, 5000);
        MOQ_TEST_CHECK(c->publishes[ps].has_expires);
        MOQ_TEST_CHECK_EQ_U64(c->publishes[ps].expires_ms, 30000);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == PUBLISH_OK with GROUP_ORDER=0 closes session ================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        uint8_t ok_buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));
        uint8_t go_v[8];
        size_t go_len = moq_quic_varint_encode(0, go_v, sizeof(go_v));
        moq_kvp_entry_t ok_params[1] = {{
            .type = MOQ_MSG_PARAM_GROUP_ORDER,
            .value = go_v, .value_len = go_len,
            .is_varint = true,
        }};
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish_ok(&w,
            sv->publishes[0].request_id, ok_params, 1), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&w), 2000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* ============================================================== */
    /* Batch 5: REQUEST_UPDATE routing across request families        */
    /* ============================================================== */

    /* == REQUEST_UPDATE for PUBLISH with priority+forward: accepted ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client publishes and server accepts. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub_h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv,
            ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Subscriber (sv) sends REQUEST_UPDATE to publisher (c).
         * request_id=1 (server's first odd ID), existing=0 (PUBLISH). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t pri_v[8];
        size_t pri_len = moq_quic_varint_encode(64, pri_v, sizeof(pri_v));
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(0, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[2] = {
            { .type = MOQ_MSG_PARAM_FORWARD,
              .value = fwd_v, .value_len = fwd_len, .is_varint = true },
            { .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
              .value = pri_v, .value_len = pri_len, .is_varint = true },
        };
        moq_d16_encode_request_update(&w, 1, 0, params, 2);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* Publisher should have sent REQUEST_OK and emitted PUBLISH_UPDATED. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);
        if (acts[0].u.send_control.len > 0) {
            moq_buf_reader_t cr;
            moq_buf_reader_init(&cr, acts[0].u.send_control.data,
                acts[0].u.send_control.len);
            moq_control_envelope_t env;
            MOQ_TEST_CHECK_EQ_INT((int)moq_control_decode_envelope(&cr, &env),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)env.msg_type, (int)MOQ_D16_REQUEST_OK);
        }
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_updated.pub, pub_h));
        MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_INT(ev.u.publish_updated.subscriber_priority, 64);
        MOQ_TEST_CHECK(ev.u.publish_updated.has_forward);
        MOQ_TEST_CHECK(!ev.u.publish_updated.forward);
        MOQ_TEST_CHECK(!ev.u.publish_updated.has_delivery_timeout);

        /* Verify state was committed on the publisher entry. */
        MOQ_TEST_CHECK_EQ_INT(c->publishes[0].subscriber_priority, 64);
        MOQ_TEST_CHECK(!c->publishes[0].send_allowed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE for FETCH returns NOT_SUPPORTED, no close ==== */
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

        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &fh),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Send REQUEST_UPDATE targeting the fetch request ID (0). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t pri_v[8];
        size_t pri_len = moq_quic_varint_encode(64, pri_v, sizeof(pri_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = pri_v, .value_len = pri_len,
            .is_varint = true,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE for PUBLISH: malformed FORWARD closes ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = ns;
        pcfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv,
            ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* FORWARD=99 is malformed (not 0 or 1) → protocol close.
         * Feed into c (publisher), request_id=1 (server's first). */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(99, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_v, .value_len = fwd_len,
            .is_varint = true,
        }};
        moq_d16_encode_request_update(&w, 1, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE for FETCH: malformed SUBSCRIBER_PRIORITY closes */
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

        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &fh),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* REQUEST_UPDATE targeting fetch with malformed SUBSCRIBER_PRIORITY
         * (value 999, outside 0-255). SUBSCRIBER_PRIORITY is in-scope for
         * FETCH targets, so value validation runs and closes. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t pri_v[8];
        size_t pri_len = moq_quic_varint_encode(999, pri_v, sizeof(pri_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = pri_v, .value_len = pri_len,
            .is_varint = true,
        }};
        moq_d16_encode_request_update(&w, 2, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    /* == REQUEST_UPDATE for PUBLISH_NAMESPACE returns NOT_SUPPORTED == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client announces a namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_namespace_cfg_t ncfg;
        moq_publish_namespace_cfg_init(&ncfg);
        ncfg.track_namespace = ns;
        moq_announcement_t ann;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_namespace(c, &ncfg,
            1000, &ann), (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);

        /* Send REQUEST_UPDATE targeting the namespace request ID. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 2, 0, NULL, 0);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
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
            MOQ_BYTES_LITERAL("media"),
        };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 3 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");

        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.track_namespace.count, 3);
        MOQ_TEST_CHECK(memcmp(ev.u.publish_request.track_namespace.parts[0].data,
            "org", 3) == 0);
        MOQ_TEST_CHECK(memcmp(ev.u.publish_request.track_namespace.parts[1].data,
            "example", 7) == 0);
        MOQ_TEST_CHECK(memcmp(ev.u.publish_request.track_namespace.parts[2].data,
            "media", 5) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Peer UNSUBSCRIBE for PUBLISH-initiated subscription ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT(moq_session_publish(c, &pcfg, 1000,
            &pub_h), MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_publish(sv, sv_pub,
            &acfg, 2000), MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_OK);
        moq_event_cleanup(&ev);

        /* Server (subscriber) sends UNSUBSCRIBE for the publish
         * request_id. Inject raw wire message. */
        {
            uint8_t buf[32];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 0);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), 3000), MOQ_OK);
        }

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_unsubscribed.pub,
            pub_h));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Peer UNSUBSCRIBE must not reset an unrelated reused subgroup === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_event_t ev;

        /* c PUBLISHes (client request_id 0); sv accepts -> c has an
         * established publisher-role publication. */
        moq_bytes_t pub_ns[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ pub_ns, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT(moq_session_publish(c, &pcfg, 1000, &pub_h),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t apcfg;
        moq_accept_publish_cfg_init(&apcfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &apcfg, 1000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 1000);
        { moq_event_t e; while (moq_session_poll_events(c, &e, 1) > 0)
              moq_event_cleanup(&e); }

        /* sv SUBSCRIBEs to c (server request_id 1); c accepts -> c also has a
         * publisher-role subscription, so it can open a subscription-backed
         * subgroup that shares the subgroup pool with the publication. */
        moq_bytes_t sub_ns[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ sub_ns, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("audio");
        moq_subscription_t sv_sub_out;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(sv, &scfg, 1500,
            &sv_sub_out), MOQ_OK);
        pump_actions_to_peer(sv, c, 1500);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t c_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ascfg;
        moq_accept_subscribe_cfg_init(&ascfg);
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(c, c_sub, &ascfg,
            1500), MOQ_OK);
        pump_actions_to_peer(c, sv, 1500);
        { moq_event_t e; while (moq_session_poll_events(sv, &e, 1) > 0)
              moq_event_cleanup(&e); }

        /* Open a publication subgroup (takes a subgroup slot), then close it
         * and drain its FIN so the slot is reaped to FREE. */
        moq_subgroup_cfg_t pub_sgcfg;
        moq_subgroup_cfg_init(&pub_sgcfg);
        pub_sgcfg.group_id = 0;
        pub_sgcfg.subgroup_id = 0;
        pub_sgcfg.publisher_priority = 128;
        moq_subgroup_handle_t pub_sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_pub_subgroup(c, pub_h,
            &pub_sgcfg, 2000, &pub_sgh), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_close_subgroup(c, pub_sgh, 2000),
            MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Open a subscription-backed subgroup. The freed publication slot is
         * reaped and reused, so it must not still carry the stale publication
         * handle. */
        moq_subgroup_cfg_t sub_sgcfg;
        moq_subgroup_cfg_init(&sub_sgcfg);
        sub_sgcfg.group_id = 0;
        sub_sgcfg.subgroup_id = 0;
        sub_sgcfg.publisher_priority = 128;
        moq_subgroup_handle_t sub_sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_subgroup(c, c_sub, &sub_sgcfg,
            2000, &sub_sgh), MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* Peer UNSUBSCRIBEs the publication (request_id 0). The publication is
         * cleaned up, but the unrelated subscription subgroup must NOT be
         * reset. */
        {
            uint8_t buf[32];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 0);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), 3000), MOQ_OK);
        }

        bool saw_unsub = false, saw_reset = false;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED) saw_unsub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_unsub);
        { moq_action_t acts[8];
          size_t na = moq_session_poll_actions(c, acts, 8);
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_RESET_DATA) saw_reset = true;
              moq_action_cleanup(&acts[i]);
          } }
        MOQ_TEST_CHECK(!saw_reset);

        /* The subscription subgroup remains usable (not RESETTING). */
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &payload);
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_object(c, sub_sgh, 0,
            payload, 3000), MOQ_OK);
        moq_rcbuf_decref(payload);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Peer UNSUBSCRIBE resets open publication subgroups ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 1000, &pub_h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub,
            &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Open a subgroup. */
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        moq_subgroup_handle_t sgh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_open_pub_subgroup(c, pub_h,
            &sgcfg, 3000, &sgh), MOQ_OK);

        /* Peer UNSUBSCRIBE while subgroup is open. */
        {
            uint8_t buf[32];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 0);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), 4000), MOQ_OK);
        }

        /* Should get PUBLISH_UNSUBSCRIBED event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED);
        moq_event_cleanup(&ev);

        /* Subgroup write should fail (handle stale after cleanup). */
        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &buf);
        MOQ_TEST_CHECK(moq_session_write_object(c, sgh, 0, buf, 4000)
            != MOQ_OK);
        moq_rcbuf_decref(buf);

        /* Check for RESET_DATA action from subgroup cleanup. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(c, acts, 8);
        bool found_reset = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_RESET_DATA) found_reset = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_reset);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == REQUEST_UPDATE targeting PUBLISH → accepted, id accounting === */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_publication_t pub_h;
        MOQ_TEST_CHECK_EQ_INT(moq_session_publish(c, &pcfg, 0, &pub_h),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub,
            &acfg, 1000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_OK);
        moq_event_cleanup(&ev);

        /* Inject REQUEST_UPDATE into c (publisher).
         * request_id=1 (server's first odd), existing=0 (PUBLISH). */
        {
            uint8_t wire[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, wire, sizeof(wire));
            moq_d16_encode_request_update(&w, 1, 0, NULL, 0);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(c, wire,
                moq_buf_writer_offset(&w), 2000), MOQ_OK);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Decode the REQUEST_OK response. */
        {
            moq_action_t acts[4];
            size_t na = moq_session_poll_actions(c, acts, 4);
            bool found = false;
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                    decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_OK)
                    found = true;
                moq_action_cleanup(&acts[i]);
            }
            MOQ_TEST_CHECK(found);
        }

        /* Drain PUBLISH_UPDATED event. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);

        /* Next injected request (id=3) is accepted — no gap close.
         * This proves request_id=1 was committed correctly. */
        feed_subscribe(c, 3, "live", "audio", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        {
            moq_event_t sev;
            MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &sev, 1), 1);
            MOQ_TEST_CHECK(sev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
            moq_event_cleanup(&sev);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Outbound PUBLISH with one auth token ========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        uint8_t tok_val[] = { 0xBB, 0xCC };
        moq_auth_token_t tok = {
            .token_type = 42,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        pcfg.auth_tokens = &tok;
        pcfg.auth_token_count = 1;

        moq_publication_t pub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.token_count, 1);
        MOQ_TEST_CHECK(ev.u.publish_request.tokens != NULL);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_request.tokens[0].token_type, 42);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.publish_request.tokens[0].token_value.len, 2);
        MOQ_TEST_CHECK(
            ev.u.publish_request.tokens[0].token_value.data[0] == 0xBB);
        MOQ_TEST_CHECK(
            ev.u.publish_request.tokens[0].token_value.data[1] == 0xCC);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == PUBLISH auth validation: count>0 but tokens==NULL ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        pcfg.auth_tokens = NULL;
        pcfg.auth_token_count = 1;

        moq_publication_t pub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub),
            (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == PUBLISH auth validation: token_value.len>0 but data==NULL ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_auth_token_t tok = {
            .token_type = 1,
            .token_value = { NULL, 5 },
        };
        pcfg.auth_tokens = &tok;
        pcfg.auth_token_count = 1;

        moq_publication_t pub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub),
            (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == PUBLISH old struct_size caller works with zero tokens ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("video");
        pcfg.struct_size = offsetof(moq_publish_cfg_t, auth_tokens);

        moq_publication_t pub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &pub),
            (int)MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_request.token_count, 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == REQUEST_UPDATE for PUBLISH: auth token applies + surfaces ===== *
     *  Auth on a publication update is supported: a structurally valid
     *  USE_VALUE token resolves and rides PUBLISH_UPDATED; the update is
     *  acknowledged with REQUEST_OK. (A structurally truncated token still
     *  closes 0x6 -- second half.) */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t h;
        moq_session_publish(c, &pcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* Feed into c (publisher), request_id=1 (server's first):
         * USE_VALUE(3), token_type 7, value "pk". */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t tok_v[] = { 0x03, 0x07, 'p', 'k' };
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_v, .value_len = sizeof(tok_v),
            .is_varint = false,
        }};
        moq_d16_encode_request_update(&w, 1, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
            (int)MOQ_SESS_ESTABLISHED);

        bool updated = false;
        while (moq_session_poll_events(c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED &&
                ev.u.publish_updated.token_count == 1 &&
                ev.u.publish_updated.tokens[0].token_type == 7 &&
                ev.u.publish_updated.tokens[0].token_value.len == 2 &&
                memcmp(ev.u.publish_updated.tokens[0].token_value.data,
                       "pk", 2) == 0)
                updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        bool found_ok = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_OK)
                found_ok = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_ok);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == REQUEST_UPDATE for PUBLISH: truncated token closes 0x6 ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t h;
        moq_session_publish(c, &pcfg, 1000, &h);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        /* REGISTER(1), alias 2, then a truncated multi-byte varint. */
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        uint8_t tok_v[] = { 0x01, 0x02, 0xAA };
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_v, .value_len = sizeof(tok_v),
            .is_varint = false,
        }};
        moq_d16_encode_request_update(&w, 1, 0, params, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(c, wire,
            moq_buf_writer_offset(&w), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
            (int)MOQ_SESS_CLOSED);
        bool saw_close = false;
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_CLOSE_SESSION) {
                saw_close = true;
                MOQ_TEST_CHECK_EQ_U64(acts[i].u.close_session.code, 0x6);
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(saw_close);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == REQUEST_UPDATE for PUBLISH: WOULD_BLOCK no-mutation retry ==== */
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
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Fill c's action queue: subscribe from c fills 1/1. */
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("x");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t csub;
        moq_session_subscribe(c, &scfg, 0, &csub);
        /* SUBSCRIBE action is queued (1/1 full). */

        /* Feed REQUEST_UPDATE into c — action queue full → WOULD_BLOCK.
         * request_id=1 (server's first odd). */
        uint8_t ubuf[128];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, ubuf, sizeof(ubuf));
        uint8_t prio_buf[8];
        size_t prio_len = moq_quic_varint_encode(50, prio_buf,
            sizeof(prio_buf));
        moq_kvp_entry_t upd_params[] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = prio_buf, .value_len = prio_len, .is_varint = true,
        }};
        moq_d16_encode_request_update(&uw, 1, 0, upd_params, 1);
        moq_result_t upd_rc = moq_session_on_control_bytes(c, ubuf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK_EQ_INT((int)upd_rc, (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* No PUBLISH_UPDATED event yet. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);

        /* Drain action queue. */
        {
            moq_action_t acts[4]; size_t na;
            while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
                for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }

        /* Retry via process_pending. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(c, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* PUBLISH_UPDATED event emitted. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_updated.pub, pub_h));
        MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_INT(ev.u.publish_updated.subscriber_priority, 50);
        moq_event_cleanup(&ev);

        /* REQUEST_OK action should be present. */
        {
            moq_action_t acts[4];
            size_t na = moq_session_poll_actions(c, acts, 4);
            MOQ_TEST_CHECK(na >= 1);
            bool found_ok = false;
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                    decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_OK)
                    found_ok = true;
                moq_action_cleanup(&acts[i]);
            }
            MOQ_TEST_CHECK(found_ok);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* PU-2: moq_session_update_publication outbound tests              */
    /* ================================================================ */

    /* == Successful outbound update: wire content + peer receives ===== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Subscriber (sv) sends update to publisher (c). */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);

        /* Decode the queued REQUEST_UPDATE action on sv. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
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
            MOQ_TEST_CHECK_EQ_U64(upd.request_id, 1);
            MOQ_TEST_CHECK_EQ_U64(upd.existing_request_id, 0);
            MOQ_TEST_CHECK_EQ_SIZE(upd.params_count, 1);
            MOQ_TEST_CHECK_EQ_U64(upd.params[0].type,
                MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
        }
        /* Feed to publisher — PU-1 inbound path handles it. */
        moq_session_on_control_bytes(c, acts[0].u.send_control.data,
            acts[0].u.send_control.len, 1000);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.subscriber_priority, 200);
        moq_event_cleanup(&ev);

        /* Feed publisher's REQUEST_OK back to subscriber. */
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Second update while pending returns WRONG_STATE ============== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);

        ucfg.subscriber_priority = 100;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_ERR_WRONG_STATE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_OK clears pending; subsequent update succeeds ======== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);

        /* Pump update to publisher and REQUEST_OK back. */
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(c, sv, 1000);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Second update should succeed now. */
        ucfg.forward = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 2000), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_ERROR clears pending; subsequent update succeeds ===== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 50;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);

        /* Drain action, manually feed REQUEST_ERROR back to sv. */
        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        uint8_t errbuf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, errbuf, sizeof(errbuf));
        moq_d16_encode_request_error(&ew, 1, MOQ_REQUEST_ERROR_NOT_SUPPORTED,
            0, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, errbuf,
            moq_buf_writer_offset(&ew), 2000), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Second update should succeed. */
        ucfg.subscriber_priority = 100;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 3000), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Empty cfg returns INVAL ====================================== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Bad delivery_timeout_us returns INVAL ======================== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_delivery_timeout = true;
        ucfg.delivery_timeout_us = 500;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_ERR_INVAL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Stale handle returns STALE_HANDLE ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv,
            MOQ_PUBLICATION_INVALID, &ucfg, 0), (int)MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == WOULD_BLOCK: no update_pending set, no registry insert ======= */
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
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        /* PUBLISH_OK action fills sv's queue (1/1). Don't drain. */

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 77;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 2000), (int)MOQ_ERR_WOULD_BLOCK);

        /* update_pending must NOT be set. */
        MOQ_TEST_CHECK(!sv->publishes[0].update_pending);

        /* Retry after drain should succeed. */
        { moq_action_t acts[4]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 3000), (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* PU-3: publication-update tombstones across termination           */
    /* ================================================================ */

    /* == Pending update + PUBLISH_DONE: late REQUEST_OK absorbed ====== */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Subscriber sends update (request_id=1). */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 42;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);
        /* Drain update action but don't deliver response. */
        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        /* Publisher finishes. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, pub_h,
            &fcfg, 2000), (int)MOQ_OK);

        /* Deliver PUBLISH_DONE to subscriber. */
        pump_actions_to_peer(c, sv, 2000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
        moq_event_cleanup(&ev);

        /* Feed late REQUEST_OK for update request_id=1. */
        uint8_t okbuf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, okbuf, sizeof(okbuf));
        moq_d16_encode_request_ok(&ow, 1, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, okbuf,
            moq_buf_writer_offset(&ow), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
            (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Pending update + PUBLISH_DONE: late REQUEST_ERROR absorbed === */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 42;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub,
            &ucfg, 1000), (int)MOQ_OK);
        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, pub_h, &fcfg, 2000);
        pump_actions_to_peer(c, sv, 2000);
        moq_session_poll_events(sv, &ev, 1);
        moq_event_cleanup(&ev);

        /* Feed late REQUEST_ERROR for update request_id=1. */
        uint8_t errbuf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, errbuf, sizeof(errbuf));
        moq_d16_encode_request_error(&ew, 1,
            MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, errbuf,
            moq_buf_writer_offset(&ew), 3000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
            (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Tombstone capacity full: PUBLISH_DONE fails closed ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Subscriber sends update. */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 42;
        moq_session_update_publication(sv, sv_pub, &ucfg, 1000);
        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        /* Fill tombstone ring so there's no capacity for the update ID. */
        while (sv->unsub_tomb_count < sv->unsub_tomb_cap)
            unsub_tomb_add(sv, 0xDEAD);

        /* Publisher finishes and delivers PUBLISH_DONE to subscriber. The
         * subscriber needs an update tombstone but the array is full. This is
         * NOT recoverable backpressure: PUBLISH_DONE is at the head of the
         * ordered control stream and the only messages that free tombstones
         * (REQUEST_OK/ERROR) are queued behind it -- a WOULD_BLOCK here would
         * stall the control stream forever. The session must fail closed. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, pub_h, &fcfg, 2000);
        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        bool saw_closed = false;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                saw_closed = true;
                MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_closed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Slot reuse: late response for old update does not affect new = */
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
        moq_publication_t pub_h1;
        moq_session_publish(c, &pcfg, 0, &pub_h1);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub1 = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub1, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Subscriber sends update (request_id=1). */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 10;
        moq_session_update_publication(sv, sv_pub1, &ucfg, 1000);
        { moq_action_t acts[4]; size_t na;
          na = moq_session_poll_actions(sv, acts, 4);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        /* Publisher finishes → PUBLISH_DONE to subscriber. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, pub_h1, &fcfg, 2000);
        pump_actions_to_peer(c, sv, 2000);
        moq_session_poll_events(sv, &ev, 1);
        moq_event_cleanup(&ev);

        /* Now create a second PUBLISH that reuses the same slot. */
        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t pub_h2;
        moq_session_publish(c, &pcfg, 3000, &pub_h2);
        pump_actions_to_peer(c, sv, 3000);
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub2 = ev.u.publish_request.pub;
        moq_session_accept_publish(sv, sv_pub2, &acfg, 3000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 3000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Feed late REQUEST_OK for the OLD update (request_id=1). */
        uint8_t okbuf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, okbuf, sizeof(okbuf));
        moq_d16_encode_request_ok(&ow, 1, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, okbuf,
            moq_buf_writer_offset(&ow), 4000), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
            (int)MOQ_SESS_ESTABLISHED);

        /* New publication must be unaffected. */
        MOQ_TEST_CHECK(!sv->publishes[0].update_pending);
        MOQ_TEST_CHECK(sv->publishes[0].state == MOQ_PUB_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Two pending pub updates + PUBLISH_DONE: both tombstoned ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_subscriptions = 1;
        sextra.max_publishes = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };

        /* Create two PUBLISH-created subscriptions on sv. */
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_publication_t pub_h1;
        moq_session_publish(c, &pcfg, 0, &pub_h1);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub1 = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub1, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        pcfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_publication_t pub_h2;
        moq_session_publish(c, &pcfg, 1000, &pub_h2);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub2 = ev.u.publish_request.pub;
        moq_session_accept_publish(sv, sv_pub2, &acfg, 1000);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Subscriber sends updates for both. */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 10;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub1,
            &ucfg, 2000), (int)MOQ_OK);
        ucfg.subscriber_priority = 20;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sv_pub2,
            &ucfg, 2000), (int)MOQ_OK);
        { moq_action_t acts[8]; size_t na;
          na = moq_session_poll_actions(sv, acts, 8);
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

        /* Publisher finishes both. */
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, pub_h1, &fcfg, 3000);
        moq_session_finish_publish(c, pub_h2, &fcfg, 3000);

        /* Deliver both PUBLISH_DONEs to subscriber. */
        pump_actions_to_peer(c, sv, 3000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
        moq_event_cleanup(&ev);

        /* Both late responses should be absorbed. */
        uint8_t okbuf[64];
        moq_buf_writer_t ow;

        moq_buf_writer_init(&ow, okbuf, sizeof(okbuf));
        moq_d16_encode_request_ok(&ow, 1, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, okbuf,
            moq_buf_writer_offset(&ow), 4000), (int)MOQ_OK);

        moq_buf_writer_init(&ow, okbuf, sizeof(okbuf));
        moq_d16_encode_request_ok(&ow, 3, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, okbuf,
            moq_buf_writer_offset(&ow), 4000), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
            (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* PU-4: forward enforcement for PUBLISH-created subscriptions     */
    /* ================================================================ */

    /* == Update forward=false blocks open/datagram/status; true restores */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.u.publish_ok.send_allowed);
        moq_event_cleanup(&ev);

        /* Subscriber sends forward=false update. */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        moq_session_update_publication(sv, sv_pub, &ucfg, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(ev.u.publish_updated.has_forward);
        MOQ_TEST_CHECK(!ev.u.publish_updated.forward);
        moq_event_cleanup(&ev);

        /* Datagrams blocked. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_send_pub_object_datagram(c,
            pub_h, 0, 0, 128, false, NULL, NULL, 0, 2000),
            (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_send_pub_status_datagram(c,
            pub_h, 0, 0, 128, MOQ_OBJECT_END_OF_GROUP, 2000),
            (int)MOQ_ERR_WRONG_STATE);

        /* Pump REQUEST_OK back to subscriber. */
        pump_actions_to_peer(c, sv, 2000);

        /* Subscriber sends forward=true update. */
        ucfg.forward = true;
        moq_session_update_publication(sv, sv_pub, &ucfg, 3000);
        pump_actions_to_peer(sv, c, 3000);
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(ev.u.publish_updated.forward);
        moq_event_cleanup(&ev);

        /* Status datagram re-enabled. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_send_pub_status_datagram(c,
            pub_h, 0, 0, 128, MOQ_OBJECT_END_OF_GROUP, 4000),
            (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Existing subgroup after forward=false: writes blocked, close ok */
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
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, sv_pub, &acfg, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Open a subgroup while forward is true. */
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_h,
            &sgcfg, 1000, &sgh), (int)MOQ_OK);

        /* Subscriber sends forward=false. */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        moq_session_update_publication(sv, sv_pub, &ucfg, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* write_object on existing subgroup: blocked. */
        moq_rcbuf_t *wbuf = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &wbuf);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(c, sgh,
            0, wbuf, 3000), (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(wbuf);

        /* New open_pub_subgroup: blocked. */
        moq_subgroup_handle_t sgh2;
        sgcfg.group_id = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_h,
            &sgcfg, 3000, &sgh2), (int)MOQ_ERR_WRONG_STATE);

        /* close_subgroup and reset_subgroup: still allowed. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sgh, 3000),
            (int)MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == ABI: old struct_size must not read end_of_group in pub subgroup */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("eog_abi");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg; moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.struct_size = offsetof(moq_subgroup_cfg_t, end_of_group);
        sgcfg.group_id = 0; sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        sgcfg.end_of_group = true; /* Must be ignored: outside struct_size. */

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_h,
            &sgcfg, 0, &sg), (int)MOQ_OK);

        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(c, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_INT(act.kind, MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) == 0);
        moq_action_cleanup(&act);

        moq_session_close_subgroup(c, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == pub subgroup with end_of_group=true sets EOG bit ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg; moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("eog_set");
        moq_publication_t pub_h;
        moq_session_publish(c, &pcfg, 0, &pub_h);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_publish_cfg_t acfg; moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(sv, ev.u.publish_request.pub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0; sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = 128;
        sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, pub_h,
            &sgcfg, 0, &sg), (int)MOQ_OK);

        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(c, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_INT(act.kind, MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) != 0);
        moq_action_cleanup(&act);

        moq_session_close_subgroup(c, sg, 0);
        moq_session_destroy(c); moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Objects before PUBLISH_OK (§9.4) surface on draft-16 too ====== *
     * draft-16 §9.4 has the identical rule (a forward=1 PUBLISH may send objects
     * before PUBLISH_OK); this is a draft-neutral receiver correction, not a wire
     * change. An early object on a still-pending forward=1 publication surfaces. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Inbound PUBLISH (FORWARD omitted -> 1), left unaccepted. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        uint8_t pub_buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pub_buf, sizeof(pub_buf));
        moq_d16_publish_t wire_pub;
        memset(&wire_pub, 0, sizeof(wire_pub));
        wire_pub.request_id = 0;
        wire_pub.track_namespace = ns;
        wire_pub.track_name = MOQ_BYTES_LITERAL("t");
        wire_pub.track_alias = 50;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_publish(&w, &wire_pub),
                              (int)MOQ_OK);
        moq_session_on_control_bytes(sv, pub_buf, moq_buf_writer_offset(&w), 1000);
        moq_event_t ev; bool got_req = false;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) got_req = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_req);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        /* Early datagram (before PUBLISH_OK) for the alias -> surfaces. */
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 50; dg.group_id = 0; dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello"; dg.payload_len = 5;
        uint8_t db[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, db, sizeof(db));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_object_datagram(&dw, &dg),
                              (int)MOQ_OK);
        moq_session_on_datagram(sv, db, moq_buf_writer_offset(&dw), 1000);
        int objs = 0;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) objs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT(alloc_state.balance, 0);
    }

    MOQ_TEST_PASS("test_session_publish");
    return failures ? 1 : 0;
}
