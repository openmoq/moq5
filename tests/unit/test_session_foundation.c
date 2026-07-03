#include "test_session_support.h"
#include "test_deadline_support.h"

int main(void)
{
    int failures = 0;
    /* == Client/server setup handshake ============================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t client_cfg = MOQ_SESSION_CFG_INIT;
        client_cfg.alloc = &alloc;
        client_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_cfg_t server_cfg = MOQ_SESSION_CFG_INIT;
        server_cfg.alloc = &alloc;
        server_cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *client = NULL;
        moq_session_t *server = NULL;

        MOQ_TEST_CHECK(moq_session_create(&client_cfg, 0, &client) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&server_cfg, 0, &server) == MOQ_OK);

        /* Client starts: emits CLIENT_SETUP. */
        MOQ_TEST_CHECK(moq_session_start(client, 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_SETUP_SENT);

        /* Server is ready immediately after create - no start needed. */
        MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_IDLE);

        /* Pump CLIENT_SETUP to server. Server becomes ESTABLISHED. */
        pump_actions_to_peer(client, server, 1000);
        MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);

        /* Check server event. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(server, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.local_perspective == MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.peer_perspective == MOQ_PERSPECTIVE_CLIENT);

        /* Pump SERVER_SETUP back to client. */
        pump_actions_to_peer(server, client, 1000);
        MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);

        ne = moq_session_poll_events(client, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.local_perspective == MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.peer_perspective == MOQ_PERSPECTIVE_SERVER);

        moq_session_destroy(client);
        moq_session_destroy(server);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Unknown control message closes session ==================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);

        uint8_t msg[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_buf_write_varint(&w, 0xFF);
        moq_buf_write_uint16(&w, 0);

        MOQ_TEST_CHECK(moq_session_on_control_bytes(s, msg,
            moq_buf_writer_offset(&w), 100) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(s, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);
        MOQ_TEST_CHECK(evts[0].u.closed.reason.len > 0);

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(s, acts, 8);
        MOQ_TEST_CHECK(na == 1);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.len > 0);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.data != NULL);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.len ==
                        evts[0].u.closed.reason.len);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.data ==
                        evts[0].u.closed.reason.data);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Malformed setup closes session ============================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

        uint8_t msg[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_buf_write_varint(&w, MOQ_D16_CLIENT_SETUP);
        moq_buf_write_uint16(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"\xFF\xFF", 2);

        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Legacy version-list CLIENT_SETUP -> ALPN diagnostic ======== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

        /*
         * Construct a legacy (draft <= 14) CLIENT_SETUP payload shape:
         *   [Number of Supported Versions = 2]
         *   [version 0xff000001][version 0xff00000e]   (drafts 1 and 14)
         *   [Number of Parameters = 0]
         * This is the shape a legacy moq-00 peer (e.g. moxygen) sends. It
         * is GUARANTEED malformed under D16's counted-params parser (the
         * 0xff0000xx version varints cannot form two valid setup-param
         * KVPs that exactly consume the buffer), so it is never accepted
         * as an unknown D16 parameter. The advisory diagnostic must fire.
         */
        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 2) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0xff000001u) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0xff00000eu) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0) == MOQ_OK);
        size_t payload_len = moq_buf_writer_offset(&pw);

        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, MOQ_D16_CLIENT_SETUP) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_uint16(&w, (uint16_t)payload_len) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, payload, payload_len) == MOQ_OK);

        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        const char *expect =
            "CLIENT_SETUP looks like a legacy version-list SETUP; "
            "D16 negotiates version via ALPN (expected moqt-16)";

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(s, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);
        MOQ_TEST_CHECK(evts[0].u.closed.reason.len == strlen(expect));
        MOQ_TEST_CHECK(evts[0].u.closed.reason.data != NULL &&
            memcmp(evts[0].u.closed.reason.data, expect,
                   strlen(expect)) == 0);

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(s, acts, 8);
        MOQ_TEST_CHECK(na == 1);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);
        MOQ_TEST_CHECK(acts[0].u.close_session.code == 0x3);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.len == strlen(expect));
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.data != NULL &&
            memcmp(acts[0].u.close_session.reason.data, expect,
                   strlen(expect)) == 0);

        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Bounded polling ============================================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        moq_session_start(s, 0);

        moq_action_t one_act;
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &one_act, 1) == 1);
        MOQ_TEST_CHECK(one_act.kind == MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &one_act, 1) == 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Client start: wrong state ================================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_ERR_WRONG_STATE);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Server start is rejected (client-only) ==================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_ERR_WRONG_STATE);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Destroy without close ===================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        moq_session_start(s, 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Destroy NULL ============================================== */
    moq_session_destroy(NULL);

    /* == Closed session rejects bytes =============================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        uint8_t msg[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_buf_write_varint(&w, 0xFF);
        moq_buf_write_uint16(&w, 0);
        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(s, msg, 1, 0) == MOQ_ERR_CLOSED);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Borrow validation ========================================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        moq_session_start(s, 0);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(s, acts, 4);
        MOQ_TEST_CHECK(na >= 1);

        uint64_t epoch = acts[0].borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(s, epoch));

        /* Advancing call invalidates. */
        moq_session_tick(s, 1);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(s, epoch));

        MOQ_TEST_CHECK(!moq_session_borrow_valid(NULL, 0));

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Config struct_size compatibility ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        cfg.struct_size = (uint32_t)offsetof(moq_session_cfg_t,
                                             send_request_capacity);
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_session_destroy(s);

        cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.send_request_capacity = true;
        cfg.struct_size = (uint32_t)offsetof(moq_session_cfg_t,
                                             initial_request_capacity);
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_ERR_INVAL);

        cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.struct_size = (uint32_t)offsetof(moq_session_cfg_t,
                                             perspective);
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_ERR_INVAL);

        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == version sits past the pre-version tail padding (ABI) ========== *
     * A caller compiled before `version` existed ends its struct at
     * max_namespace_subscriptions, 8-byte-aligned. `version` must start at/after
     * that old sizeof so such a caller (passing its sizeof, with garbage tail
     * padding) does not have those bytes read as `version`. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        size_t pad_start = offsetof(moq_session_cfg_t,
                                    max_namespace_subscriptions) + sizeof(uint32_t);
        size_t old_sizeof = (pad_start + 7u) & ~(size_t)7u;

        moq_session_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        /* Dirty the pre-version tail padding (what an old caller leaves
         * uninitialized). With version inside that padding this read back as an
         * unsupported version and failed create; with version moved past it,
         * create must default to D16 and succeed. */
        memset((unsigned char *)&cfg + pad_start, 0xA5, old_sizeof - pad_start);
        cfg.struct_size = (uint32_t)old_sizeof;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == pointer-only init must not write past the frozen v0 prefix ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Frozen v0 prefix: struct_size + alloc + perspective. */
        enum { V0 = (int)offsetof(moq_session_cfg_t, send_request_capacity) };
        /* Old caller's storage is exactly V0 bytes; a canary sits right after. */
        _Alignas(moq_session_cfg_t) unsigned char buf[V0 + sizeof(uint64_t)];
        uint64_t canary = 0xDEADBEEFCAFEF00DULL;
        memcpy(buf + V0, &canary, sizeof(canary));

        moq_session_cfg_init((moq_session_cfg_t *)buf, &alloc,
                             MOQ_PERSPECTIVE_CLIENT);

        uint64_t after = 0;
        memcpy(&after, buf + V0, sizeof(after));
        MOQ_TEST_CHECK(after == canary);   /* nothing written past the prefix */
        uint32_t ss = 0;
        memcpy(&ss, buf, sizeof(ss));
        MOQ_TEST_CHECK(ss == (uint32_t)V0); /* stamped exactly the v0 prefix */
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == sized init stamps the full struct and honors appended fields == */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg;
        memset(&cfg, 0xAB, sizeof(cfg));   /* dirty: sized init must clear it */
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc,
                                   MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(cfg.struct_size == sizeof(cfg));
        MOQ_TEST_CHECK(cfg.send_request_capacity == false); /* cleared */
        cfg.version = MOQ_VERSION_DRAFT_18;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Allocator missing realloc is rejected ========================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        alloc.realloc = NULL;
        moq_session_t *s = NULL;

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
    }

    /* == Version / profile selection ================================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *s = NULL;

        /* Explicit D16 version. */
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.version = MOQ_VERSION_DRAFT_16;
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&cfg, 0, &s), MOQ_OK);
        moq_session_destroy(s);

        /* Default version (0) selects D16. */
        cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.version = 0;
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&cfg, 0, &s), MOQ_OK);
        moq_session_destroy(s);

        /* Old struct_size that doesn't include version field → D16. */
        cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.struct_size = (uint32_t)offsetof(moq_session_cfg_t, version);
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&cfg, 0, &s), MOQ_OK);
        moq_session_destroy(s);

        /* Unknown nonzero version → ERR_INVAL. */
        cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.version = (moq_version_t)9999;
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);

        MOQ_TEST_CHECK_EQ_INT((int)alloc_state.balance, 0);
    }

    /* == NULL arg guards =========================================== */
    {
        moq_session_t *s = (moq_session_t *)1;
        MOQ_TEST_CHECK(moq_session_create(NULL, 0, &s) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
        MOQ_TEST_CHECK(moq_session_create(NULL, 0, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_start(NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(NULL, NULL, 0, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_tick(NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(NULL) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(NULL) == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_poll_actions(NULL, NULL, 0) == 0);
        MOQ_TEST_CHECK(moq_session_poll_events(NULL, NULL, 0) == 0);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(NULL, 0));
    }

    /* == Tick on closed session ==================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        uint8_t msg[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_buf_write_varint(&w, 0xFF);
        moq_buf_write_uint16(&w, 0);
        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_tick(s, 1) == MOQ_ERR_CLOSED);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Close clears stale actions and events ======================== */
    /*
     * After a successful handshake, the server has undrained outputs:
     * SEND_CONTROL(SERVER_SETUP) action and SETUP_COMPLETE event.
     * A subsequent protocol violation must clear both queues and
     * deliver only CLOSE_SESSION + SESSION_CLOSED.
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);

        /* Pump CLIENT_SETUP to server. Server becomes ESTABLISHED.
         * Do NOT drain server actions or events. */
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Inject garbage to trigger protocol violation on server. */
        const uint8_t garbage[] = { 0x40, 0xFF, 0x00, 0x00 };
        moq_session_on_control_bytes(sv, garbage, sizeof(garbage), 100);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        /* Only CLOSE_SESSION action — no stale SEND_CONTROL. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        MOQ_TEST_CHECK(na == 1);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);

        /* Only SESSION_CLOSED event — no stale SETUP_COMPLETE. */
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(sv, evts, 8);
        MOQ_TEST_CHECK(ne == 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Queues drainable after close ================================= */
    /*
     * After close, polling returns the close outputs once, then
     * returns 0 on subsequent polls. Confirms no zombie data.
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        const uint8_t garbage[] = { 0x40, 0xFF, 0x00, 0x00 };
        moq_session_on_control_bytes(s, garbage, sizeof(garbage), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        /* First drain: close outputs. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_CLOSE_SESSION);

        moq_event_t evt;
        MOQ_TEST_CHECK(moq_session_poll_events(s, &evt, 1) == 1);
        MOQ_TEST_CHECK(evt.kind == MOQ_EVENT_SESSION_CLOSED);

        /* Second drain: empty. */
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &act, 1) == 0);
        MOQ_TEST_CHECK(moq_session_poll_events(s, &evt, 1) == 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Setup parameter negotiation ================================= */

    /* -- Default MAX_REQUEST_ID is 0 when absent -------------------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        /* send_request_capacity = false (default) */

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Neither side sent MAX_REQUEST_ID, so peer defaults to 0. */
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 0);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(sv) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Nonzero MAX_REQUEST_ID roundtrips through setup ------------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 42;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 100;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Server saw client's MAX_REQUEST_ID=42. */
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(sv) == 42);
        /* Client saw server's MAX_REQUEST_ID=100. */
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 100);

        /* Local values match what was configured. */
        MOQ_TEST_CHECK(moq_session_local_max_request_id(c) == 42);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 100);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Server-sent PATH closes client with INVALID_PATH ----------- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);

        /* Drain CLIENT_SETUP action. */
        moq_action_t drain[4];
        moq_session_poll_actions(c, drain, 4);

        /* Craft a SERVER_SETUP with PATH param (server MUST NOT send it). */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));

        moq_kvp_entry_t params[1];
        params[0].type      = MOQ_SETUP_PARAM_PATH;
        params[0].value     = (const uint8_t *)"/test";
        params[0].value_len = 5;
        params[0].is_varint = false;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;
        moq_d16_encode_server_setup(&w, params, 1);

        moq_session_on_control_bytes(c, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x8); /* INVALID_PATH */

        moq_session_destroy(c);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Server-sent AUTHORITY closes client with INVALID_AUTHORITY -- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);
        moq_action_t drain[4];
        moq_session_poll_actions(c, drain, 4);

        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_kvp_entry_t params[1];
        params[0].type      = MOQ_SETUP_PARAM_AUTHORITY;
        params[0].value     = (const uint8_t *)"example.com";
        params[0].value_len = 11;
        params[0].is_varint = false;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;
        moq_d16_encode_server_setup(&w, params, 1);

        moq_session_on_control_bytes(c, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x19); /* INVALID_AUTHORITY */

        moq_session_destroy(c);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Duplicate known setup param closes with PROTOCOL_VIOLATION -- */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        /* Craft CLIENT_SETUP with duplicate MAX_REQUEST_ID. */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));

        uint8_t v1[1]; moq_quic_varint_encode(10, v1, 1);
        uint8_t v2[1]; moq_quic_varint_encode(20, v2, 1);
        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_REQUEST_ID,
            .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        /* Second MAX_REQUEST_ID — same type, duplicate. */
        params[1] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_REQUEST_ID,
            .value = v2, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        moq_d16_encode_client_setup(&w, params, 2);

        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3); /* PROTOCOL_VIOLATION */

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* -- Duplicate unknown setup param is tolerated ------------------ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        /* CLIENT_SETUP with two unknown params of the same type (0xFF).
         * KVP delta encoding makes this: first delta=0xFF, second delta=0. */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));

        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = 0xFF, .value = (const uint8_t *)"a", .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = 0xFF, .value = (const uint8_t *)"b", .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0
        };
        moq_d16_encode_client_setup(&w, params, 2);

        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);
        /* Session should NOT close — unknown duplicates are allowed. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Failed validation does not leak peer params ================= */
    /*
     * Craft CLIENT_SETUP with MAX_REQUEST_ID=42 then a duplicate
     * MAX_REQUEST_ID. Validation fails → close. Verify the server
     * never committed the peer's MAX_REQUEST_ID value.
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));

        uint8_t v1[1]; moq_quic_varint_encode(42, v1, 1);
        uint8_t v2[1]; moq_quic_varint_encode(99, v2, 1);
        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_REQUEST_ID,
            .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_REQUEST_ID,
            .value = v2, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        moq_d16_encode_client_setup(&w, params, 2);
        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(sv) == 0);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Zero-byte on_control_bytes processes buffered input ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        /* Build a valid CLIENT_SETUP. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);

        moq_action_t act;
        moq_session_poll_actions(c, &act, 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_CONTROL);

        /* Feed the CLIENT_SETUP bytes to the server. */
        moq_session_on_control_bytes(sv, act.u.send_control.data,
                                      act.u.send_control.len, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Now call with NULL/0 — should succeed without crash. */
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, NULL, 0, 100) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Partial input remains buffered =============================== */
    /*
     * Feed half a CLIENT_SETUP, then the rest. Session should
     * establish only after the complete message arrives.
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        /* Generate valid CLIENT_SETUP bytes. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);

        moq_action_t act;
        moq_session_poll_actions(c, &act, 1);
        const uint8_t *data = act.u.send_control.data;
        size_t len = act.u.send_control.len;
        MOQ_TEST_CHECK(len > 2);

        /* Feed first half. */
        size_t half = len / 2;
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, data, half, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_IDLE);

        /* Feed second half. */
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, data + half,
                                                     len - half, 100) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Resource limits: zero means default ========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.max_actions = 0;
        cfg.max_events = 0;
        cfg.send_buffer_size = 0;
        cfg.recv_buffer_size = 0;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Send buffer too small -> MOQ_ERR_BUFFER ====================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.send_buffer_size = 1;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 42;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_ERR_BUFFER);

        /* State and local_setup not committed after failed start. */
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_IDLE);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(s) == 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Recv buffer too small -> MOQ_ERR_BUFFER ====================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.recv_buffer_size = 1;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

        /* CLIENT_SETUP is more than 1 byte. */
        const uint8_t fake_msg[] = { 0x20, 0x04, 0x01, 0x02, 0x00, 0x00 };
        MOQ_TEST_CHECK(moq_session_on_control_bytes(s, fake_msg,
            sizeof(fake_msg), 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_IDLE);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Failed setup receive: state not committed ==================== */
    /*
     * Server with send_buffer_size=1: can receive CLIENT_SETUP but
     * cannot encode SERVER_SETUP -> MOQ_ERR_BUFFER. State stays IDLE,
     * peer_setup not committed.
     */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Client with default buffers to generate valid CLIENT_SETUP. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 77;

        moq_session_t *c = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_start(c, 0);
        moq_action_t act;
        moq_session_poll_actions(c, &act, 1);

        /* Server with tiny send buffer. */
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_buffer_size = 1;

        moq_session_t *sv = NULL;
        moq_session_create(&scfg, 0, &sv);

        moq_result_t rc = moq_session_on_control_bytes(sv,
            act.u.send_control.data, act.u.send_control.len, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_IDLE);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(sv) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Tiny capacities are honored ================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.max_actions = 1;
        ccfg.max_events = 1;
        ccfg.send_buffer_size = 64;
        ccfg.recv_buffer_size = 64;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.max_actions = 1;
        scfg.max_events = 1;
        scfg.send_buffer_size = 64;
        scfg.recv_buffer_size = 64;

        moq_session_t *c = NULL, *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Drained action borrow invalidated by next advancing call ===== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.send_buffer_size = 64;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        /* start() fills send_buf with CLIENT_SETUP. */
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);

        /* Drain the action. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &act, 1) == 1);
        uint64_t epoch = act.borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(s, epoch));

        /* Next advancing call invalidates the polled action borrow.
         * send_buf reclamation is internal; with the setup-only surface
         * there is no second non-close send path that can observe reuse. */
        const uint8_t garbage[] = { 0x40, 0xFF, 0x00, 0x00 };
        moq_result_t rc = moq_session_on_control_bytes(s, garbage,
            sizeof(garbage), 100);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(!moq_session_borrow_valid(s, epoch));
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Overflow in allocation size -> MOQ_ERR_INVAL ================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.max_actions = UINT32_MAX;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Close overrides stale outputs with tiny queues =============== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.max_actions = 1;
        ccfg.max_events = 1;
        ccfg.send_buffer_size = 64;
        ccfg.recv_buffer_size = 64;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.max_actions = 1;
        scfg.max_events = 1;
        scfg.send_buffer_size = 64;
        scfg.recv_buffer_size = 64;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Don't drain server actions or events. Queues are full (1/1). */
        const uint8_t garbage[] = { 0x40, 0xFF, 0x00, 0x00 };
        moq_session_on_control_bytes(sv, garbage, sizeof(garbage), 100);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na == 1);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne == 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Size-aware action poll ABI =================================== */
    {
        typedef struct action_prefix {
            moq_action_kind_t kind;
            uint32_t          detail_size;
            uint64_t          borrow_epoch;
        } action_prefix_t;

        typedef struct action_large {
            moq_action_t action;
            uint8_t      tail[16];
        } action_large_t;

        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);

        action_prefix_t prefix;
        memset(&prefix, 0xA5, sizeof(prefix));
        size_t count = 99;
        MOQ_TEST_CHECK(sizeof(prefix) == offsetof(moq_action_t, u));
        MOQ_TEST_CHECK(moq_session_poll_actions_ex(s, &prefix, 1,
            sizeof(prefix), &count) == MOQ_OK);
        MOQ_TEST_CHECK(count == 1);
        MOQ_TEST_CHECK(prefix.kind == MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK(prefix.detail_size == sizeof(moq_send_control_action_t));
        MOQ_TEST_CHECK(moq_session_borrow_valid(s, prefix.borrow_epoch));
        MOQ_TEST_CHECK(moq_session_poll_actions(s, NULL, 0) == 0);

        moq_session_destroy(s);

        /* Too-small caller stride must not drain the queued action. */
        s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);

        uint8_t too_small[sizeof(action_prefix_t) - 1];
        count = 99;
        MOQ_TEST_CHECK(moq_session_poll_actions_ex(s, too_small, 1,
            sizeof(too_small), &count) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(count == 0);

        moq_action_t action;
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &action, 1) == 1);
        MOQ_TEST_CHECK(action.kind == MOQ_ACTION_SEND_CONTROL);

        moq_session_destroy(s);

        /* Oversized caller stride is zero-filled past the library record. */
        s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);

        action_large_t large;
        memset(&large, 0xA5, sizeof(large));
        count = 99;
        MOQ_TEST_CHECK(moq_session_poll_actions_ex(s, &large, 1,
            sizeof(large), &count) == MOQ_OK);
        MOQ_TEST_CHECK(count == 1);
        MOQ_TEST_CHECK(large.action.kind == MOQ_ACTION_SEND_CONTROL);
        for (size_t i = 0; i < sizeof(large.tail); i++)
            MOQ_TEST_CHECK(large.tail[i] == 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Size-aware event poll ABI ==================================== */
    {
        typedef struct event_prefix {
            moq_event_kind_t kind;
            uint32_t         detail_size;
            uint64_t         borrow_epoch;
        } event_prefix_t;

        typedef struct event_large {
            moq_event_t event;
            uint8_t     tail[16];
        } event_large_t;

        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *c = NULL;
        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        event_prefix_t prefix;
        memset(&prefix, 0xA5, sizeof(prefix));
        size_t count = 99;
        MOQ_TEST_CHECK(sizeof(prefix) == offsetof(moq_event_t, u));
        MOQ_TEST_CHECK(moq_session_poll_events_ex(sv, &prefix, 1,
            sizeof(prefix), &count) == MOQ_OK);
        MOQ_TEST_CHECK(count == 1);
        MOQ_TEST_CHECK(prefix.kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(prefix.detail_size == sizeof(moq_setup_complete_event_t));
        MOQ_TEST_CHECK(moq_session_borrow_valid(sv, prefix.borrow_epoch));

        moq_session_destroy(c);
        moq_session_destroy(sv);

        /* Too-small caller stride must not drain the queued event. */
        c = NULL;
        sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        uint8_t too_small[sizeof(event_prefix_t) - 1];
        count = 99;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(sv, too_small, 1,
            sizeof(too_small), &count) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(count == 0);

        moq_event_t event;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &event, 1) == 1);
        MOQ_TEST_CHECK(event.kind == MOQ_EVENT_SETUP_COMPLETE);

        moq_session_destroy(c);
        moq_session_destroy(sv);

        /* Oversized caller stride is zero-filled past the library record. */
        c = NULL;
        sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        event_large_t large;
        memset(&large, 0xA5, sizeof(large));
        count = 99;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(sv, &large, 1,
            sizeof(large), &count) == MOQ_OK);
        MOQ_TEST_CHECK(count == 1);
        MOQ_TEST_CHECK(large.event.kind == MOQ_EVENT_SETUP_COMPLETE);
        for (size_t i = 0; i < sizeof(large.tail); i++)
            MOQ_TEST_CHECK(large.tail[i] == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }


    /* == GOAWAY: receive with empty URI ============================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Server sends GOAWAY with empty URI. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        /* Pump to client. */
        pump_actions_to_peer(sv, c, 0);

        /* Client receives GOAWAY event. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        MOQ_TEST_CHECK(ev.u.goaway.new_session_uri.len == 0);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);

        /* Client subscribe after GOAWAY → MOQ_ERR_GOAWAY. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub)
            == MOQ_ERR_GOAWAY);

        /* Duplicate GOAWAY send → WRONG_STATE. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0)
            == MOQ_ERR_WRONG_STATE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: peer SUBSCRIBE after local GOAWAY is refused ========= */
    {
        /* After we send GOAWAY (§10.4) the peer must not open new requests.
         * session_is_active() still includes DRAINING, so the inbound
         * request initiators need an explicit local-GOAWAY gate -- otherwise
         * a fresh SUBSCRIBE still allocates request state and emits an app
         * event. */
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns,
            MOQ_BYTES_LITERAL("t"), NULL, 0) == MOQ_OK);
        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

        /* Refused as a protocol violation: no SUBSCRIBE_REQUEST surfaced. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        bool saw_sub_req = false, saw_closed = false;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) saw_sub_req = true;
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                saw_closed = true;
                MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_sub_req);
        MOQ_TEST_CHECK(saw_closed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: peer PUBLISH_NAMESPACE after local GOAWAY refused ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns, NULL, 0)
            == MOQ_OK);
        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        bool saw_ns_pub = false, saw_closed = false;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) saw_ns_pub = true;
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                saw_closed = true;
                MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_ns_pub);
        MOQ_TEST_CHECK(saw_closed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: peer FETCH / PUBLISH / TRACK_STATUS after GOAWAY ===== */
    {
        /* The whole D16 control-channel request-initiator class must reject
         * new peer requests after our local GOAWAY -- not just SUBSCRIBE and
         * PUBLISH_NAMESPACE. */
        for (int which = 0; which < 3; which++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

            MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

            uint8_t msg[128];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, msg, sizeof(msg));
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            uint32_t refused_event;

            if (which == 0) {           /* FETCH (standalone) */
                moq_d16_fetch_t fetch = {
                    .request_id = 0,
                    .fetch_type = MOQ_D16_FETCH_TYPE_STANDALONE,
                    .track_namespace = ns,
                    .track_name = MOQ_BYTES_LITERAL("t"),
                    .end_group = 1,
                };
                MOQ_TEST_CHECK(moq_d16_encode_fetch(&w, &fetch, NULL, 0) == MOQ_OK);
                refused_event = MOQ_EVENT_FETCH_REQUEST;
            } else if (which == 1) {    /* PUBLISH (peer offers a track) */
                moq_d16_publish_t pub = {
                    .request_id = 0,
                    .track_namespace = ns,
                    .track_name = MOQ_BYTES_LITERAL("t"),
                    .track_alias = 42,
                };
                MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);
                refused_event = MOQ_EVENT_PUBLISH_REQUEST;
            } else {                    /* TRACK_STATUS request */
                MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, &ns,
                    MOQ_BYTES_LITERAL("t"), NULL, 0) == MOQ_OK);
                refused_event = MOQ_EVENT_TRACK_STATUS_REQUEST;
            }

            moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

            MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
            moq_event_t ev;
            bool saw_req = false, saw_closed = false;
            while (moq_session_poll_events(sv, &ev, 1) == 1) {
                if (ev.kind == refused_event) saw_req = true;
                if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                    saw_closed = true;
                    MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(!saw_req);
            MOQ_TEST_CHECK(saw_closed);

            moq_session_destroy(c);
            moq_session_destroy(sv);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
    }

    /* == GOAWAY: existing request can still complete during draining == */
    {
        /* Positive control: a request that arrived BEFORE our GOAWAY can
         * still be answered while DRAINING (the gate is only for NEW peer
         * requests, not in-flight ones). */
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Now drain: existing in-flight SUBSCRIBE must still be acceptable. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        pump_actions_to_peer(sv, c, 0);
        bool saw_sub_ok = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) saw_sub_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_sub_ok);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: receive with URI ==================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        const char *uri = "wss://new.example.com";
        MOQ_TEST_CHECK(moq_session_goaway(sv,
            (const uint8_t *)uri, strlen(uri), 0) == MOQ_OK);

        pump_actions_to_peer(sv, c, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        MOQ_TEST_CHECK(ev.u.goaway.new_session_uri.len == strlen(uri));
        MOQ_TEST_CHECK(memcmp(ev.u.goaway.new_session_uri.data,
            uri, strlen(uri)) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: duplicate receive → PROTOCOL_VIOLATION ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode two GOAWAY messages back-to-back. */
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_goaway(&w, NULL, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_d16_encode_goaway(&w, NULL, 0) == MOQ_OK);

        moq_session_on_control_bytes(c, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        /* close_with_error clears the event queue, so the first
         * GOAWAY event is wiped. Only SESSION_CLOSED survives. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        bool found_closed = false;
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_SESSION_CLOSED) {
                MOQ_TEST_CHECK(evts[i].u.closed.code == 0x3);
                found_closed = true;
            }
            moq_event_cleanup(&evts[i]);
        }
        MOQ_TEST_CHECK(found_closed);

        { moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: server receives URI → PROTOCOL_VIOLATION =========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client sends GOAWAY with URI (invalid). */
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_encode_goaway(&w, (const uint8_t *)"bad", 3);
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        { moq_event_t evts[4]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: client cannot send URI ============================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_goaway(c,
            (const uint8_t *)"uri", 3, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Empty URI from client is fine. */
        MOQ_TEST_CHECK(moq_session_goaway(c, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: publish_namespace after GOAWAY → MOQ_ERR_GOAWAY ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        pn_cfg.track_namespace.parts = ns_parts;
        pn_cfg.track_namespace.count = 1;
        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(c,
            &pn_cfg, 0, &ann) == MOQ_ERR_GOAWAY);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: existing subscription survives ====================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe before GOAWAY. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub)
            == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub,
            &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Server sends GOAWAY. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);

        /* Server can still write objects on existing subscription. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub,
            &sg_cfg, 0, &sg) == MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"data", 4, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, payload, 0)
            == MOQ_OK);
        moq_rcbuf_decref(payload);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
          while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == GOAWAY: REQUEST_ERROR for pending sub after GOAWAY ========== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub)
            == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server sends GOAWAY. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        moq_event_t ev;
        /* Drain SUBSCRIBE_REQUEST on server. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Drain GOAWAY on client. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);

        /* Server rejects the pending subscribe with REQUEST_ERROR. */
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = 1;
        rej.reason = MOQ_BYTES_LITERAL("going away");
        MOQ_TEST_CHECK(moq_session_reject_subscribe(sv, server_sub,
            &rej, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        /* Client should get SUBSCRIBE_ERROR, NOT a protocol close. */
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        MOQ_TEST_CHECK(ev.u.subscribe_error.error_code == 1);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Scratch exhaustion: GOAWAY URI too large =================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.output_scratch_size = 8;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Server sends GOAWAY with URI larger than client scratch. */
        const char *big_uri = "wss://very-long-uri.example.com";
        MOQ_TEST_CHECK(moq_session_goaway(sv,
            (const uint8_t *)big_uri, strlen(big_uri), 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        /* Client should detect permanently-too-small scratch and close
         * with error code 0x1, not return WOULD_BLOCK forever. */
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        {
            bool found_closed = false;
            moq_event_t evts[8]; size_t ne;
            while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
                for (size_t i = 0; i < ne; i++) {
                    if (evts[i].kind == MOQ_EVENT_SESSION_CLOSED) {
                        MOQ_TEST_CHECK(evts[i].u.closed.code == 0x1);
                        found_closed = true;
                    }
                    moq_event_cleanup(&evts[i]);
                }
            MOQ_TEST_CHECK(found_closed);
            moq_action_t acts[8]; size_t na;
            while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
                for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Scratch exhaustion: SUBSCRIBE namespace too large ============ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.output_scratch_size = 8;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Client subscribes with a namespace larger than server scratch. */
        char big_ns[64];
        memset(big_ns, 'N', sizeof(big_ns));
        moq_bytes_t ns_parts[] = { { (const uint8_t *)big_ns, sizeof(big_ns) } };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server should detect permanently-too-small scratch and close. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

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
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Scratch exhaustion: temporary, retryable after advance ====== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        /* Server scratch fits one SUBSCRIBE_REQUEST event but not two.
         * Each event needs ~24 bytes of scratch (namespace parts array +
         * namespace data + track name). 32 bytes fits one, not two. */
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.output_scratch_size = 32;
        sextra.max_events = 4;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sextra);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Encode two SUBSCRIBE messages back-to-back. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub1;
        moq_subscribe_cfg_init(&sub1);
        sub1.track_namespace = ns;
        sub1.track_name = MOQ_BYTES_LITERAL("t1");
        sub1.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t h1;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub1, 0, &h1) == MOQ_OK);

        moq_subscribe_cfg_t sub2;
        moq_subscribe_cfg_init(&sub2);
        sub2.track_namespace = ns;
        sub2.track_name = MOQ_BYTES_LITERAL("t2");
        sub2.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub2, 0, &h2) == MOQ_OK);

        /* Feed both SUBSCRIBE actions to server in one on_control_bytes
         * call by collecting all client actions first. */
        uint8_t buf[512];
        size_t buf_len = 0;
        moq_action_t acts[8]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                    size_t al = acts[i].u.send_control.len;
                    if (buf_len + al <= sizeof(buf)) {
                        memcpy(buf + buf_len,
                            acts[i].u.send_control.data, al);
                        buf_len += al;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed to server: first SUBSCRIBE succeeds (scratch fits),
         * second SUBSCRIBE returns retryable error (scratch consumed). */
        moq_result_t rc = moq_session_on_control_bytes(sv, buf,
            buf_len, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* First SUBSCRIBE_REQUEST event should be emitted. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Retry via process_pending: scratch resets, second
         * SUBSCRIBE is re-parsed and event emitted. */
        rc = moq_session_process_pending(sv, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
          moq_action_t a[8]; size_t naa;
          while ((naa = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < naa; i++) moq_action_cleanup(&a[i]);
          while ((naa = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < naa; i++) moq_action_cleanup(&a[i]);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Scratch exhaustion: GOAWAY empty URI with tiny scratch OK ==== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.output_scratch_size = 8;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(sv, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* GOAWAY with empty URI should succeed with tiny scratch. */
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        MOQ_TEST_CHECK(ev.u.goaway.new_session_uri.len == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Timer: no deadline returns UINT64_MAX ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(c) == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: outbound GOAWAY arms deadline ========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = 5000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 6000);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: inbound GOAWAY arms deadline =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.goaway_timeout_us = 3000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        MOQ_TEST_CHECK(moq_session_next_deadline_us(c) == UINT64_MAX);
        moq_session_goaway(sv, NULL, 0, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(c) == 5000);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: tick before deadline keeps DRAINING ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = 5000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_session_goaway(sv, NULL, 0, 1000);
        MOQ_TEST_CHECK(moq_session_tick(sv, 4999) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 6000);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: tick at deadline closes with GOAWAY_TIMEOUT ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = 5000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_session_goaway(sv, NULL, 0, 1000);
        MOQ_TEST_CHECK(moq_session_tick(sv, 6000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        /* Subsequent tick returns CLOSED. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 7000) == MOQ_ERR_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT);
        moq_event_cleanup(&ev);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: disabled timeout does not arm ========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_session_goaway(sv, NULL, 0, 1000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_tick(sv, 999999) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: old struct_size cfg compatible ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.struct_size = offsetof(moq_session_cfg_t, goaway_timeout_us);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(s) == UINT64_MAX);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: protocol close clears deadline ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = 5000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_session_goaway(sv, NULL, 0, 1000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 6000);

        /* Force a protocol close: client sends GOAWAY twice to server.
         * First is accepted (goaway_received becomes true), second
         * triggers "duplicate GOAWAY" close_with_error. */
        moq_session_goaway(c, NULL, 0, 2000);
        pump_actions_to_peer(c, sv, 2000);
        /* Drain GOAWAY event so the queue can accept the close event. */
        moq_event_t gev;
        if (moq_session_poll_events(sv, &gev, 1) == 1) moq_event_cleanup(&gev);
        /* Send the raw GOAWAY bytes again to trigger duplicate close. */
        {
            uint8_t goaway_msg[8];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, goaway_msg, sizeof(goaway_msg));
            moq_d16_encode_goaway(&w, NULL, 0);
            moq_session_on_control_bytes(sv, goaway_msg,
                moq_buf_writer_offset(&w), 3000);
        }
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Timer: deadline overflow saturates ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = UINT64_MAX;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        moq_session_goaway(sv, NULL, 0, 1000);
        uint64_t dl = moq_session_next_deadline_us(sv);
        MOQ_TEST_CHECK(dl != UINT64_MAX);
        MOQ_TEST_CHECK(dl == UINT64_MAX - 1);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: SUBSCRIBE stores delivery_timeout_us ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed SUBSCRIBE with DELIVERY_TIMEOUT = 500ms (wire). */
        uint8_t timeout_param_val[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, timeout_param_val, sizeof(timeout_param_val));
        moq_buf_write_varint(&pw, 500);
        moq_kvp_entry_t params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = timeout_param_val,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.delivery_timeout_us == 500000);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Verify internal storage via white-box helper. */
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 500000);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: SUBSCRIBE without DELIVERY_TIMEOUT stores 0 ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 0);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: REQUEST_UPDATE stores delivery_timeout_us ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe with initial timeout 500ms. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 500);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 500000);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        /* Drain SUBSCRIBE_OK locally (raw feed has no matching client sub). */
        moq_action_t drain_acts[16]; size_t dn;
        while ((dn = moq_session_poll_actions(sv, drain_acts, 16)) > 0)
            for (size_t i = 0; i < dn; i++) moq_action_cleanup(&drain_acts[i]);

        /* Send REQUEST_UPDATE with new DELIVERY_TIMEOUT 1000ms. */
        uint8_t tv2[8];
        moq_buf_writer_init(&pw, tv2, sizeof(tv2));
        moq_buf_write_varint(&pw, 1000);
        moq_kvp_entry_t upd_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv2,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};

        uint8_t upd_buf[512];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, upd_buf, sizeof(upd_buf));
        moq_d16_encode_request_update(&uw, 2, 0, upd_params, 1);
        moq_session_on_control_bytes(sv, upd_buf,
            moq_buf_writer_offset(&uw), 0);

        /* Drain REQUEST_OK action and verify SUBSCRIBE_UPDATED event. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_delivery_timeout == true);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.delivery_timeout_us == 1000000);
        moq_event_cleanup(&ev);

        /* Verify internal storage updated. */
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 1000000);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: REQUEST_UPDATE WB leaves old timeout ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Subscribe with initial timeout 500ms via raw feed. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 500);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 500000);

        /* Accept — queues SUBSCRIBE_OK action, filling the 1-slot
         * action queue. Do NOT pump to client (raw feed has no
         * matching client subscription). */
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        /* Feed REQUEST_UPDATE with DELIVERY_TIMEOUT 2000ms.
         * action_queue_full precheck should return WOULD_BLOCK. */
        uint8_t tv2[8];
        moq_buf_writer_init(&pw, tv2, sizeof(tv2));
        moq_buf_write_varint(&pw, 2000);
        moq_kvp_entry_t upd_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv2,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        uint8_t upd_buf[512];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, upd_buf, sizeof(upd_buf));
        moq_d16_encode_request_update(&uw, 2, 0, upd_params, 1);
        moq_result_t rc = moq_session_on_control_bytes(sv, upd_buf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Old timeout must be unchanged. */
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 500000);

        /* Drain SUBSCRIBE_OK action to free the queue. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Retry via process_pending. */
        rc = moq_session_process_pending(sv, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        /* Now timeout should be updated. */
        MOQ_TEST_CHECK(test_get_sub_delivery_timeout(sv, ssub) == 2000000);

        /* Drain remaining events and actions. */
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: next_deadline_us min(goaway, subgroup) ============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.goaway_timeout_us = 10000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Create a subscription + subgroup BEFORE draining: new inbound peer
         * requests are refused after a local GOAWAY, so this in-flight setup
         * must happen first. */
        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* No deadlines armed yet. */
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        /* Arm GOAWAY → deadline at 1000 + 10000 = 11000. */
        moq_session_goaway(sv, NULL, 0, 1000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 11000);

        /* Set subgroup deadline earlier → min should update. */
        test_set_sg_deadline(sv, sg, 5000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 5000);

        /* Set subgroup deadline later than GOAWAY → GOAWAY wins. */
        test_set_sg_deadline(sv, sg, 20000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 11000);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: close_with_error clears subgroup_deadline_us ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        test_set_sg_deadline(sv, sg, 5000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 5000);

        /* Force protocol close. */
        uint8_t goaway_msg[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, goaway_msg, sizeof(goaway_msg));
        moq_d16_encode_goaway(&w, NULL, 0);
        moq_session_on_control_bytes(sv, goaway_msg,
            moq_buf_writer_offset(&w), 0);
        /* Send duplicate to trigger close. */
        moq_event_t gev;
        if (moq_session_poll_events(sv, &gev, 1) == 1) moq_event_cleanup(&gev);
        moq_session_on_control_bytes(sv, goaway_msg,
            moq_buf_writer_offset(&w), 0);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: subgroup deadline cleared on close/reuse ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        test_set_sg_deadline(sv, sg, 3000);
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == 3000);

        /* Close the subgroup → terminal state → reap clears deadline. */
        moq_session_close_subgroup(sv, sg, 0);

        /* Drain actions to trigger sg_reap_terminal via next
         * advancing call. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Trigger reap via an advancing call. */
        moq_session_tick(sv, 0);
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == UINT64_MAX);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: tick fires subgroup timeout and emits RESET_DATA ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        test_set_sg_deadline(sv, sg, 1000);

        /* Tick before deadline — subgroup stays alive. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 999) == MOQ_OK);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg) == 1000);

        /* Tick at deadline — fires timeout reset. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_tick(sv, 1000) == MOQ_OK);
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Should have RESET_DATA action with timeout error code. */
        bool got_reset = false;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                    acts[i].u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
                    got_reset = true;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(got_reset);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: multiple subgroups, earliest fires first ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        sg_cfg.group_id = 0;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg1);
        sg_cfg.group_id = 1;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg2);
        test_set_sg_deadline(sv, sg1, 5000);
        test_set_sg_deadline(sv, sg2, 3000);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 3000);

        /* Drain queued actions. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Tick at 3000 — sg2 fires, sg1 stays. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 3000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 5000);

        int reset_count = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                    acts[i].u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
                    reset_count++;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(reset_count == 1);

        /* Tick at 5000 — sg1 fires. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 5000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        reset_count = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                    acts[i].u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
                    reset_count++;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(reset_count == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: WOULD_BLOCK on timeout reset, retry succeeds ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        /* SUBSCRIBE_OK fills the 1-slot action queue. Don't drain. */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        /* open_subgroup needs an action slot — drain first. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        /* Subgroup header fills queue again. Don't drain. */

        test_set_sg_deadline(sv, sg, 1000);

        /* Tick past deadline — action queue full → WOULD_BLOCK. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 2000) == MOQ_ERR_WOULD_BLOCK);
        /* Pending reset retained: raw subgroup deadline stays expired. */
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg) == 1000);
        /* But the *reported* next deadline must be deferred past now so a
         * timer driver sleeps instead of busy-spinning a zero wait on the
         * still-expired deadline (the actual bug — neuter the backoff and
         * this returns 1000, the expired raw deadline). */
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) > 2000);

        /* Drain and retry. */
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_tick(sv, 2000) == MOQ_OK);

        bool got_reset = false;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                    acts[i].u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
                    got_reset = true;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(got_reset);
        /* Reset emitted → deadline fully cleared, no lingering backoff that
         * would keep waking the driver. */
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: blocked retry must not mask a nearer subgroup ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        feed_subscribe(sv, 0, "ns", "t", NULL, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Open two subgroups. Drain sg1's header so sg2 can open; leave
         * sg2's header pending → the 1-slot action queue is full. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        sg_cfg.group_id = 1;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg1)
                       == MOQ_OK);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        sg_cfg.group_id = 2;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg2)
                       == MOQ_OK);
        /* sg2 header now fills the queue. Don't drain. */

        /* sg1 expired (deadline 1000); sg2 still in the future at 3000,
         * which is < the retry backoff (now 2000 + 5000 = 7000). */
        test_set_sg_deadline(sv, sg1, 1000);
        test_set_sg_deadline(sv, sg2, 3000);

        /* Tick past sg1's deadline → queue full → WOULD_BLOCK, retry armed. */
        MOQ_TEST_CHECK(moq_session_tick(sv, 2000) == MOQ_ERR_WOULD_BLOCK);

        /* The retry backoff defers only sg1's expired reset. sg2's nearer
         * future deadline must still be reported — the backoff must not mask
         * it (reporting 7000 would sleep past sg2's 3000). */
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == 3000);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: REQUEST_UPDATE affects only future subgroups ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe with 500ms timeout. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 500);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        /* Open first subgroup at t=1000. Deadline = 1000 + 500000 = 501000. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1;
        sg_cfg.group_id = 0;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 1000, &sg1);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg1) == 501000);

        /* REQUEST_UPDATE changes timeout to 2000ms. */
        uint8_t tv2[8];
        moq_buf_writer_init(&pw, tv2, sizeof(tv2));
        moq_buf_write_varint(&pw, 2000);
        moq_kvp_entry_t upd_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv2,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        uint8_t upd_buf[512];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, upd_buf, sizeof(upd_buf));
        moq_d16_encode_request_update(&uw, 2, 0, upd_params, 1);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_on_control_bytes(sv, upd_buf,
            moq_buf_writer_offset(&uw), 2000);
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* First subgroup keeps old deadline. */
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg1) == 501000);

        /* Open second subgroup at t=2000. Should use new 2000ms timeout.
         * Deadline = 2000 + 2000000 = 2002000. */
        moq_subgroup_handle_t sg2;
        sg_cfg.group_id = 1;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 2000, &sg2);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg2) == 2002000);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: close_subgroup before timeout prevents reset ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe with 1000ms timeout. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 1000);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg) != UINT64_MAX);

        /* Close before timeout fires. */
        moq_session_close_subgroup(sv, sg, 500);
        MOQ_TEST_CHECK(moq_session_next_deadline_us(sv) == UINT64_MAX);

        /* Tick past would-have-been deadline — no reset. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_tick(sv, 9999999) == MOQ_OK);

        bool got_reset = false;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA) got_reset = true;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(!got_reset);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: unsubscribe clears subgroup deadline ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Subscribe with 1000ms timeout. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 1000);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg) != UINT64_MAX);
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) != UINT64_MAX);

        /* Drain actions. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Feed UNSUBSCRIBE for request_id 0. */
        {
            uint8_t unsub_buf[64];
            moq_buf_writer_t uw;
            moq_buf_writer_init(&uw, unsub_buf, sizeof(unsub_buf));
            moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
            moq_session_on_control_bytes(sv, unsub_buf,
                moq_buf_writer_offset(&uw), 0);
        }

        /* Subgroup deadline must be cleared. */
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == UINT64_MAX);

        /* Tick past deadline — no timeout reset (already cancelled). */
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_tick(sv, 9999999) == MOQ_OK);

        bool got_timeout_reset = false;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA &&
                    acts[i].u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
                    got_timeout_reset = true;
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(!got_timeout_reset);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Deadline: unsubscribe partial WB recomputes deadline ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Subscribe with 1000ms timeout. */
        uint8_t tv[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, tv, sizeof(tv));
        moq_buf_write_varint(&pw, 1000);
        moq_kvp_entry_t sub_params[] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .is_varint = true,
            .value = tv,
            .value_len = (uint16_t)moq_buf_writer_offset(&pw),
        }};
        feed_subscribe(sv, 0, "ns", "t", sub_params, 1);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        /* Drain SUBSCRIBE_OK. */
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Open two subgroups at different times → different deadlines. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg1, sg2;
        sg_cfg.group_id = 0;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 100, &sg1);
        /* Drain sg1 header action. */
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        sg_cfg.group_id = 1;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 200, &sg2);
        /* sg2 header sits in the queue (1 of 2 slots used). */

        /* sg1 deadline = 100 + 1000000 = 1000100
         * sg2 deadline = 200 + 1000000 = 1000200 */
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg1) == 1000100);
        MOQ_TEST_CHECK(test_get_sg_deadline(sv, sg2) == 1000200);
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == 1000100);

        /* Feed UNSUBSCRIBE. With max_actions=2 and 1 slot used (sg2
         * header), the first RESET_DATA fits (slot 2). Then the
         * second RESET_DATA hits action_queue_full → WOULD_BLOCK. */
        uint8_t unsub_buf[64];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, unsub_buf, sizeof(unsub_buf));
        moq_d16_encode_varint_msg(&uw, MOQ_D16_UNSUBSCRIBE, 0);
        moq_result_t rc = moq_session_on_control_bytes(sv, unsub_buf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* sg1 was reset and its deadline cleared. sg2 still armed.
         * Session deadline should reflect sg2 only. */
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == 1000200);

        /* Drain and retry. */
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        rc = moq_session_process_pending(sv, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        /* Both cleared now. */
        MOQ_TEST_CHECK(test_get_session_sg_deadline(sv) == UINT64_MAX);

        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Registry: mixed sub + announcement IDs cannot cross-route ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes (request_id=0) and publishes namespace
         * (request_id=2). Both go through the registry. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t csub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &csub) == MOQ_OK);

        moq_publish_namespace_cfg_t pub_cfg;
        moq_publish_namespace_cfg_init(&pub_cfg);
        pub_cfg.track_namespace = ns;
        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(c, &pub_cfg, 0, &ann) == MOQ_OK);

        pump_actions_to_peer(c, sv, 0);

        /* Server accepts subscription (request_id=0). */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);

        /* Server accepts namespace (request_id=2). */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        moq_session_accept_namespace(sv, ev.u.namespace_published.ann,
            &nacc, 0);
        moq_event_cleanup(&ev);

        /* Pump responses back. Client should get:
         * - SUBSCRIBE_OK for request_id=0
         * - NAMESPACE_ACCEPTED for request_id=2
         * Both routed through the registry. */
        pump_actions_to_peer(sv, c, 0);

        int sub_ok = 0, ns_acc = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok++;
            else if (ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) ns_acc++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(sub_ok == 1);
        MOQ_TEST_CHECK(ns_acc == 1);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Registry: REQUEST_OK for subscription closes session ========== */
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

        /* Session should be ESTABLISHED with a pending subscription. */
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Feed a generic REQUEST_OK for subscription request_id=0.
         * This is a protocol violation — subscriptions use SUBSCRIBE_OK. */
        {
            uint8_t buf[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_request_ok(&w, 0, NULL, 0);
            moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), 0);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Registry: REQUEST_UPDATE for announcement request_id closes == */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Feed raw PUBLISH_NAMESPACE to server (request_id=0).
         * This creates an announcement-kind registry entry. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_namespace_cfg_t pub_cfg;
        moq_publish_namespace_cfg_init(&pub_cfg);
        pub_cfg.track_namespace = ns;
        moq_announcement_t ann;
        moq_session_publish_namespace(c, &pub_cfg, 0, &ann);
        pump_actions_to_peer(c, sv, 0);

        /* Server accepts the namespace. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        moq_session_accept_namespace(sv, ev.u.namespace_published.ann,
            &nacc, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);
        while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Feed REQUEST_UPDATE with existing_request_id=0 (the announcement).
         * REQUEST_UPDATE routes through the request registry, finds the
         * announcement, and returns NOT_SUPPORTED (no session close). */
        uint8_t upd_buf[512];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, upd_buf, sizeof(upd_buf));
        moq_d16_encode_request_update(&uw, 2, 0, NULL, 0);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_on_control_bytes(sv, upd_buf,
            moq_buf_writer_offset(&uw), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Registry: terminal cleanup removes entry ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client publishes namespace → request_id=0. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_publish_namespace_cfg_t pub_cfg;
        moq_publish_namespace_cfg_init(&pub_cfg);
        pub_cfg.track_namespace = ns;
        moq_announcement_t ann;
        moq_session_publish_namespace(c, &pub_cfg, 0, &ann);
        pump_actions_to_peer(c, sv, 0);

        /* Server rejects namespace. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        moq_session_reject_namespace(sv, ev.u.namespace_published.ann,
            &rej, 0);
        moq_event_cleanup(&ev);
        pump_actions_to_peer(sv, c, 0);

        /* Client gets NAMESPACE_REJECTED, entry freed. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_REJECTED);
        moq_event_cleanup(&ev);

        /* Now feed a late REQUEST_OK for the same request_id=0.
         * Entry was cleaned up → should close (unknown request). */
        {
            uint8_t buf[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_request_ok(&w, 0, NULL, 0);
            moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), 0);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Registry: tombstone consumes late SUBSCRIBE_OK after unsub === */
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

        /* Unsubscribe before server responds — tombstones request_id=0,
         * removes registry entry. */
        moq_session_unsubscribe(c, csub, 0);

        /* Feed late SUBSCRIBE_OK for request_id=0. Tombstone consumes
         * it silently: no close, no SUBSCRIBE_OK event emitted. */
        feed_subscribe_ok(c, 0, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_event_t ev;
        bool got_sub_ok = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) got_sub_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!got_sub_ok);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUESTS_BLOCKED: accepted while ESTABLISHED ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_encode_varint_msg(&w, MOQ_D16_REQUESTS_BLOCKED, 9);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, buf,
            moq_buf_writer_offset(&w), 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUESTS_BLOCKED: rejected before ESTABLISHED ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);

        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_encode_varint_msg(&w, MOQ_D16_REQUESTS_BLOCKED, 0);
        moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUESTS_BLOCKED: malformed closes =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode envelope with truncated payload. */
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_control_encode_envelope(&w, MOQ_D16_REQUESTS_BLOCKED,
            NULL, 0);
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE codec round-trip =========================== */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("example.com"),
                                 MOQ_BYTES_LITERAL("meeting=123") };
        moq_namespace_t prefix = { parts, 2 };
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_namespace(&w, 42, &prefix,
            1, NULL, 0) == MOQ_OK);

        size_t len = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(len > 0);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, len);
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SUBSCRIBE_NAMESPACE);

        moq_bytes_t dec_parts[8];
        moq_d16_subscribe_namespace_t sn = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_namespace(
            env.payload, env.payload_len, dec_parts, 8, &sn) == MOQ_OK);
        MOQ_TEST_CHECK(sn.request_id == 42);
        MOQ_TEST_CHECK(sn.track_namespace_prefix.count == 2);
        MOQ_TEST_CHECK(sn.subscribe_options == 1);
    }

    /* == SUBSCRIBE_NAMESPACE: empty prefix round-trip =================== */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_namespace_t empty_prefix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_namespace(&w, 7,
            &empty_prefix, 2, NULL, 0) == MOQ_OK);

        size_t len = moq_buf_writer_offset(&w);
        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, len);
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_bytes_t dec_parts[8];
        moq_d16_subscribe_namespace_t sn = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_namespace(
            env.payload, env.payload_len, dec_parts, 8, &sn) == MOQ_OK);
        MOQ_TEST_CHECK(sn.request_id == 7);
        MOQ_TEST_CHECK(sn.track_namespace_prefix.count == 0);
        MOQ_TEST_CHECK(sn.subscribe_options == 2);
    }

    /* == SUBSCRIBE_NAMESPACE: invalid subscribe_options rejected ======= */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t prefix = { parts, 1 };
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_namespace(&w, 0, &prefix,
            3, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* == SUBSCRIBE_NAMESPACE on control stream closes ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t prefix = { parts, 1 };
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 0, NULL, 0);
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == NAMESPACE on control stream closes ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode a minimal NAMESPACE envelope. */
        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("track1") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        moq_buf_write_namespace(&pw, &suffix);
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_control_encode_envelope(&w, MOQ_D16_NAMESPACE,
            payload, (uint16_t)moq_buf_writer_offset(&pw));
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == NAMESPACE_DONE on control stream closes ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("gone") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        moq_buf_write_namespace(&pw, &suffix);
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_control_encode_envelope(&w, MOQ_D16_NAMESPACE_DONE,
            payload, (uint16_t)moq_buf_writer_offset(&pw));
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == update_max_request_id: grants credit and unlocks peer ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 2, 2, &c, &sv, NULL, NULL);

        /* Client starts with peer_max=2 → credit for request ID 0 only. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("rc") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = MOQ_BYTES_LITERAL("a");
        cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t h;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 0, &h) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        { moq_event_t d[8]; size_t ne;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Second subscribe → blocked. */
        cfg.track_name = MOQ_BYTES_LITERAL("b");
        moq_subscription_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 0, &h2) == MOQ_ERR_REQUEST_BLOCKED);

        /* Server grants more credit (test both old and semantic APIs). */
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 2);
        MOQ_TEST_CHECK(moq_session_local_request_capacity(sv) == 2);
        MOQ_TEST_CHECK(moq_session_grant_request_capacity(sv, 4, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 4);
        MOQ_TEST_CHECK(moq_session_local_request_capacity(sv) == 4);

        /* Pump grant to client. */
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_peer_max_request_id(c) == 4);
        MOQ_TEST_CHECK(moq_session_peer_request_capacity(c) == 4);

        /* Client gets REQUEST_READY. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);
        moq_event_cleanup(&ev);

        /* Retry subscribe → OK. */
        MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 0, &h2) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        /* Server accepts retry → proves request ID 2 is valid after grant. */
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            MOQ_TEST_CHECK(moq_session_accept_subscribe(sv,
                ev.u.subscribe_request.sub, &acc, 0) == MOQ_OK);
            moq_event_cleanup(&ev);
        }
        pump_actions_to_peer(sv, c, 0);
        { moq_event_t d[8]; size_t ne;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
          while ((na = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == update_max_request_id: rejects non-increasing value =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_update_max_request_id(sv, 10, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_update_max_request_id(sv, 5, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_update_max_request_id(sv, 12, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 12);
        MOQ_TEST_CHECK(moq_session_update_max_request_id(sv, 12, 0) == MOQ_ERR_INVAL);

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
          while ((na = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == update_max_request_id: WB does not advance local max ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 2, 2, &c, &sv, NULL, &tiny);

        /* Fill server action queue. */
        moq_session_goaway(sv, NULL, 0, 0);

        /* Update should WB; local max stays at 2. */
        moq_result_t rc = moq_session_update_max_request_id(sv, 4, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 2);

        /* Drain, retry → OK. */
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_update_max_request_id(sv, 4, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_local_max_request_id(sv) == 4);

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
          while ((na = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Property fields are the primary API names ===================== */
    {
        moq_subscribe_ok_event_t ok_ev;
        memset(&ok_ev, 0, sizeof(ok_ev));
        ok_ev.track_properties.data = (const uint8_t *)"test";
        ok_ev.track_properties.len = 4;
        MOQ_TEST_CHECK(ok_ev.track_properties.len == 4);

        moq_object_received_event_t obj_ev;
        memset(&obj_ev, 0, sizeof(obj_ev));
        obj_ev.properties = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(obj_ev.properties == (moq_rcbuf_t *)0xDEAD);

        moq_object_chunk_event_t chunk_ev;
        memset(&chunk_ev, 0, sizeof(chunk_ev));
        chunk_ev.properties = (moq_rcbuf_t *)0xBEEF;
        MOQ_TEST_CHECK(chunk_ev.properties == (moq_rcbuf_t *)0xBEEF);

        moq_accept_subscribe_cfg_t acc_cfg;
        memset(&acc_cfg, 0, sizeof(acc_cfg));
        acc_cfg.track_properties.data = (const uint8_t *)"prop";
        acc_cfg.track_properties.len = 4;
        MOQ_TEST_CHECK(acc_cfg.track_properties.len == 4);
    }

    /* ============================================================== */
    /* Wrong-state session delivery tests                             */
    /*                                                                */
    /* Feed valid D16 messages to sessions in wrong states and verify  */
    /* clean close behavior (state -> CLOSED, close code 0x3).        */
    /* ============================================================== */

    /* -- Helper: build a valid SUBSCRIBE message ---------------------- */
    #define BUILD_SUBSCRIBE(outbuf, outlen) do { \
        moq_buf_writer_t _bsw; \
        moq_buf_writer_init(&_bsw, (outbuf), sizeof(outbuf)); \
        moq_bytes_t _ns[] = { MOQ_BYTES_LITERAL("test") }; \
        moq_namespace_t _nst = { _ns, 1 }; \
        moq_d16_encode_subscribe(&_bsw, 0, &_nst, \
            MOQ_BYTES_LITERAL("t"), NULL, 0); \
        (outlen) = moq_buf_writer_offset(&_bsw); \
    } while (0)

    /* -- Helper: verify session closed with code 0x3 ------------------ */
    #define CHECK_CLOSED_0x3(session, label) do { \
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(session), \
                              (int)MOQ_SESS_CLOSED); \
        moq_event_t _ce[4]; \
        size_t _cne = moq_session_poll_events((session), _ce, 4); \
        MOQ_TEST_CHECK(_cne >= 1); \
        if (_cne >= 1) { \
            MOQ_TEST_CHECK_EQ_INT((int)_ce[0].kind, \
                                  (int)MOQ_EVENT_SESSION_CLOSED); \
            MOQ_TEST_CHECK_EQ_HEX(_ce[0].u.closed.code, 0x3); \
        } \
        for (size_t _ci = 0; _ci < _cne; _ci++) moq_event_cleanup(&_ce[_ci]); \
        moq_action_t _ca[8]; size_t _cna; \
        while ((_cna = moq_session_poll_actions((session), _ca, 8)) > 0) \
            for (size_t _ci = 0; _ci < _cna; _ci++) moq_action_cleanup(&_ca[_ci]); \
    } while (0)

    /* -- SUBSCRIBE before ESTABLISHED --------------------------------- */
    /* Feed a valid SUBSCRIBE to a server in IDLE state. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_IDLE);

        uint8_t msg[512]; size_t mlen;
        BUILD_SUBSCRIBE(msg, mlen);
        moq_session_on_control_bytes(s, msg, mlen, 0);

        CHECK_CLOSED_0x3(s, "SUBSCRIBE before ESTABLISHED");

        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBSCRIBE_OK without pending subscription -------------------- */
    /* Feed SUBSCRIBE_OK to established client with no pending subscribe. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);

        /* Feed a SUBSCRIBE_OK for request_id=0 but no subscription is pending. */
        feed_subscribe_ok(c, 0, 1, NULL, 0);

        CHECK_CLOSED_0x3(c, "SUBSCRIBE_OK without pending subscription");

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- REQUEST_UPDATE without established subscription --------------- */
    /* Feed REQUEST_UPDATE to an established server with no active sub. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_request_update(&w, 0, 0, NULL, 0);
        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

        CHECK_CLOSED_0x3(sv, "REQUEST_UPDATE without subscription");

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- PUBLISH_NAMESPACE before ESTABLISHED ------------------------- */
    /* Feed to IDLE server. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_IDLE);

        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("test") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_encode_publish_namespace(&w, 0, &ns, NULL, 0);
        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);

        CHECK_CLOSED_0x3(s, "PUBLISH_NAMESPACE before ESTABLISHED");

        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- GOAWAY before ESTABLISHED ------------------------------------ */
    /* Feed to IDLE client. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_IDLE);

        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_goaway(&w, NULL, 0);
        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);

        CHECK_CLOSED_0x3(s, "GOAWAY before ESTABLISHED");

        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- CLIENT_SETUP to client --------------------------------------- */
    /* Feed CLIENT_SETUP to a client session (should only go to server). */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(s, 0) == MOQ_OK);
        /* Drain CLIENT_SETUP action. */
        {
            moq_action_t a[8];
            size_t na = moq_session_poll_actions(s, a, 8);
            for (size_t j = 0; j < na; j++) moq_action_cleanup(&a[j]);
        }

        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_client_setup(&w, NULL, 0);
        moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);

        CHECK_CLOSED_0x3(s, "CLIENT_SETUP to client");

        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SERVER_SETUP to established server --------------------------- */
    /* Feed SERVER_SETUP to an already-established server. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_server_setup(&w, NULL, 0);
        moq_session_on_control_bytes(sv, msg, moq_buf_writer_offset(&w), 0);

        CHECK_CLOSED_0x3(sv, "SERVER_SETUP to established server");

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(c, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Duplicate GOAWAY --------------------------------------------- */
    /* Send GOAWAY twice; second should close session. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Build a GOAWAY message. */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_goaway(&w, NULL, 0);
        size_t msg_len = moq_buf_writer_offset(&w);

        /* First GOAWAY → session drains or stays established. */
        moq_session_on_control_bytes(c, msg, msg_len, 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Drain events from first GOAWAY. */
        { moq_event_t d[8]; size_t ne;
          while ((ne = moq_session_poll_events(c, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Second GOAWAY → session closes. */
        moq_session_on_control_bytes(c, msg, msg_len, 0);
        CHECK_CLOSED_0x3(c, "Duplicate GOAWAY");

        { moq_event_t d[8]; moq_action_t a[8]; size_t ne, na;
          while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((na = moq_session_poll_actions(sv, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    #undef BUILD_SUBSCRIBE
    #undef CHECK_CLOSED_0x3

    /* == Undersized poll does not pop OBJECT_RECEIVED ================== */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx = moq_stream_ref_from_u64(99);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        size_t too_small = offsetof(moq_event_t, u) + 1;
        uint8_t buf[sizeof(moq_event_t)];
        size_t count = 99;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(c, buf, 1,
            too_small, &count) == MOQ_ERR_ABI_MISMATCH);
        MOQ_TEST_CHECK(count == 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 4);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Undersized poll does not pop FETCH_OBJECT ===================== */
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
        moq_session_fetch(c, &fcfg, 0, &fh);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t sfh = ev.u.fetch_request.fetch;

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        moq_session_accept_fetch(sv, sfh, &acfg, 0);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"fetch", 5, &p);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.payload = p;
        moq_session_write_fetch_object(sv, sfh, &ocfg, 0);
        moq_rcbuf_decref(p);
        moq_session_end_fetch(sv, sfh, 0);

        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                    moq_session_on_control_bytes(c,
                        acts[i].u.send_control.data,
                        acts[i].u.send_control.len, 0);
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, acts[i].u.send_data.stream_ref,
                        acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        /* Drain FETCH_OK first. */
        moq_session_poll_events(c, &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_OK);
        moq_event_cleanup(&ev);

        size_t too_small = offsetof(moq_event_t, u) + 1;
        uint8_t buf[sizeof(moq_event_t)];
        size_t count = 99;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(c, buf, 1,
            too_small, &count) == MOQ_ERR_ABI_MISMATCH);
        MOQ_TEST_CHECK(count == 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_OBJECT);
        MOQ_TEST_CHECK(ev.u.fetch_object.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fetch_object.payload) == 5);
        moq_event_cleanup(&ev);

        while (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Undersized poll does not pop OBJECT_CHUNK ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"chunk", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx = moq_stream_ref_from_u64(77);
        moq_action_t acts[16];
        size_t na;
        uint8_t combined[4096];
        size_t clen = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) {
                        memcpy(combined + clen,
                            acts[i].u.send_data.header, hl);
                        clen += hl;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(
                            acts[i].u.send_data.payload);
                        memcpy(combined + clen,
                            moq_rcbuf_data(acts[i].u.send_data.payload),
                            pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
        moq_session_on_data_bytes(c, rx, combined, clen, true, 0);

        size_t too_small = offsetof(moq_event_t, u) + 1;
        uint8_t buf[sizeof(moq_event_t)];
        size_t count = 99;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(c, buf, 1,
            too_small, &count) == MOQ_ERR_ABI_MISMATCH);
        MOQ_TEST_CHECK(count == 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 5);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Advancing APIs reject after terminal close ==================== */

    /* Subscribe response after close. */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;

        moq_session_on_transport_close(sv, 0, 1000);
        moq_event_t drain;
        while (moq_session_poll_events(sv, &drain, 1) == 1)
            moq_event_cleanup(&drain);
        moq_action_t acts[4];
        while (moq_session_poll_actions(sv, acts, 4) > 0) {
            for (size_t i = 0; i < 4; i++) moq_action_cleanup(&acts[i]);
        }

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc,
            2000) == MOQ_ERR_CLOSED);

        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = 0x1;
        MOQ_TEST_CHECK(moq_session_reject_subscribe(sv, ssub, &rej,
            2000) == MOQ_ERR_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_actions(sv, acts, 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Fetch response/data after close. */
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
        moq_session_fetch(c, &fcfg, 0, &fh);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t sfh = ev.u.fetch_request.fetch;

        moq_session_on_transport_close(sv, 0, 1000);
        moq_event_t drain;
        while (moq_session_poll_events(sv, &drain, 1) == 1)
            moq_event_cleanup(&drain);
        moq_action_t acts[4];
        while (moq_session_poll_actions(sv, acts, 4) > 0) {
            for (size_t i = 0; i < 4; i++) moq_action_cleanup(&acts[i]);
        }

        moq_accept_fetch_cfg_t acfg;
        moq_accept_fetch_cfg_init(&acfg);
        acfg.end_group = 1;
        MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sfh, &acfg,
            2000) == MOQ_ERR_CLOSED);

        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = 0x1;
        MOQ_TEST_CHECK(moq_session_reject_fetch(sv, sfh, &rej,
            2000) == MOQ_ERR_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_actions(sv, acts, 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Publish response after close. */
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
        moq_session_publish(c, &pcfg, 0, &ph);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t sph = ev.u.publish_request.pub;

        moq_session_on_transport_close(sv, 0, 1000);
        moq_event_t drain;
        while (moq_session_poll_events(sv, &drain, 1) == 1)
            moq_event_cleanup(&drain);
        moq_action_t acts[4];
        while (moq_session_poll_actions(sv, acts, 4) > 0) {
            for (size_t i = 0; i < 4; i++) moq_action_cleanup(&acts[i]);
        }

        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, sph, &acfg,
            2000) == MOQ_ERR_CLOSED);

        moq_reject_publish_cfg_t rej;
        moq_reject_publish_cfg_init(&rej);
        rej.error_code = 0x1;
        MOQ_TEST_CHECK(moq_session_reject_publish(sv, sph, &rej,
            2000) == MOQ_ERR_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_actions(sv, acts, 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Track status response after close. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_track_status_cfg_t tcfg;
        moq_track_status_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        tcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_track_status_handle_t th;
        moq_session_track_status(c, &tcfg, 0, &th);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_track_status_handle_t sth = ev.u.track_status_request.handle;

        moq_session_on_transport_close(sv, 0, 1000);
        moq_event_t drain;
        while (moq_session_poll_events(sv, &drain, 1) == 1)
            moq_event_cleanup(&drain);
        moq_action_t acts[4];
        while (moq_session_poll_actions(sv, acts, 4) > 0) {
            for (size_t i = 0; i < 4; i++) moq_action_cleanup(&acts[i]);
        }

        moq_accept_track_status_cfg_t acfg;
        moq_accept_track_status_cfg_init(&acfg);
        MOQ_TEST_CHECK(moq_session_accept_track_status(sv, sth,
            &acfg, 2000) == MOQ_ERR_CLOSED);

        moq_reject_track_status_cfg_t rej;
        moq_reject_track_status_cfg_init(&rej);
        rej.error_code = 0x1;
        MOQ_TEST_CHECK(moq_session_reject_track_status(sv, sth,
            &rej, 2000) == MOQ_ERR_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_actions(sv, acts, 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Namespace response after close. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_namespace_cfg_t pn;
        moq_publish_namespace_cfg_init(&pn);
        pn.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        moq_announcement_t ann;
        moq_session_publish_namespace(sv, &pn, 0, &ann);
        pump_actions_to_peer(sv, c, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_announcement_t cann = ev.u.namespace_published.ann;

        moq_session_on_transport_close(c, 0, 1000);
        moq_event_t drain;
        while (moq_session_poll_events(c, &drain, 1) == 1)
            moq_event_cleanup(&drain);
        moq_action_t acts[4];
        while (moq_session_poll_actions(c, acts, 4) > 0) {
            for (size_t i = 0; i < 4; i++) moq_action_cleanup(&acts[i]);
        }

        moq_accept_namespace_cfg_t acfg;
        moq_accept_namespace_cfg_init(&acfg);
        MOQ_TEST_CHECK(moq_session_accept_namespace(c, cann,
            &acfg, 2000) == MOQ_ERR_CLOSED);

        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = 0x1;
        MOQ_TEST_CHECK(moq_session_reject_namespace(c, cann,
            &rej, 2000) == MOQ_ERR_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_actions(c, acts, 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Request credit: semantic and D16 aliases are equivalent ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 42, 100, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK_EQ_U64(
            moq_session_peer_request_capacity(c),
            moq_session_peer_max_request_id(c));
        MOQ_TEST_CHECK_EQ_U64(
            moq_session_local_request_capacity(c),
            moq_session_local_max_request_id(c));
        MOQ_TEST_CHECK_EQ_U64(
            moq_session_peer_request_capacity(sv),
            moq_session_peer_max_request_id(sv));
        MOQ_TEST_CHECK_EQ_U64(
            moq_session_local_request_capacity(sv),
            moq_session_local_max_request_id(sv));

        MOQ_TEST_CHECK_EQ_INT(
            moq_session_grant_request_capacity(sv, 200, 1000),
            MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(
            moq_session_local_request_capacity(sv),
            moq_session_local_max_request_id(sv));
        MOQ_TEST_CHECK_EQ_U64(
            moq_session_local_request_capacity(sv), 200);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* IDLE TIMEOUT                                                   */
    /* ============================================================== */

    /* == Default config: no idle timeout fires ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_session_tick(c, 999999999);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(c), UINT64_MAX);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Idle timeout closes established session ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.idle_timeout_us = 5000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        MOQ_TEST_CHECK(moq_session_next_deadline_us(c) != UINT64_MAX);

        moq_session_tick(c, 1000000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK_EQ_HEX(ev.u.closed.code,
            (uint64_t)MOQ_CLOSE_IDLE_TIMEOUT);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound activity extends idle deadline ======================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.idle_timeout_us = 10000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        /* Tick at 8000 — within timeout, no close. */
        moq_session_tick(c, 8000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Inbound activity at 9000 resets the deadline. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 9000, &sub);
        pump_actions_to_peer(c, sv, 9000);

        /* Accept from server is inbound control bytes to client. */
        { moq_event_t ev;
          moq_session_poll_events(sv, &ev, 1);
          moq_accept_subscribe_cfg_t acc;
          moq_accept_subscribe_cfg_init(&acc);
          moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub,
              &acc, 9000);
          moq_event_cleanup(&ev); }
        pump_actions_to_peer(sv, c, 9000);
        { moq_event_t ev;
          while (moq_session_poll_events(c, &ev, 1) > 0)
              moq_event_cleanup(&ev); }

        /* Tick at 15000 — would have timed out at 10000 without
         * the activity at 9000, but should be alive until 19000. */
        moq_session_tick(c, 15000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Tick at 20000 — past 9000 + 10000 = 19000 deadline. */
        moq_session_tick(c, 20000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Closed session does not emit duplicate close events ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.idle_timeout_us = 1000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_session_tick(c, 100000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        moq_event_cleanup(&ev);

        moq_session_tick(c, 200000);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound activity extends idle deadline ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.idle_timeout_us = 10000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        /* Outbound subscribe at 8000 refreshes the deadline. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &scfg, 8000,
            &sub), MOQ_OK);

        /* Tick at 15000 — would have expired at original deadline
         * without the outbound activity at 8000. */
        moq_session_tick(c, 15000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Tick at 19000 — past 8000 + 10000 = 18000. */
        moq_session_tick(c, 19000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == process_pending does not extend idle deadline ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.idle_timeout_us = 10000;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_session_process_pending(c, 8000);
        moq_session_tick(c, 11000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Server create arms idle deadline immediately ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.idle_timeout_us = 5000;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&scfg, 1000, &sv), MOQ_OK);

        /* The idle deadline is armed from creation time, so a silent server
         * advertises a finite deadline before any peer activity. */
        MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(sv), 6000);

        /* Tick just before the deadline keeps the session open. */
        moq_session_tick(sv, 5999);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* Tick at the deadline closes the silent server session. */
        moq_session_tick(sv, 6000);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK_EQ_HEX(ev.u.closed.code,
            (uint64_t)MOQ_CLOSE_IDLE_TIMEOUT);
        moq_event_cleanup(&ev);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Late inbound activity does not revive an expired session ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.idle_timeout_us = 5000;          /* deadline at 6000 */

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *sv = NULL, *c = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&scfg, 1000, &sv), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_create(&ccfg, 1000, &c), MOQ_OK);

        /* Client emits CLIENT_SETUP. */
        moq_session_start(c, 1000);

        /* The peer's first setup bytes arrive at 7000 — past the 6000
         * deadline, with no intervening tick. Idle timeout is authoritative:
         * the server closes rather than reviving and completing setup. */
        pump_actions_to_peer(c, sv, 7000);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t ev;
        bool saw_idle_close = false, saw_setup = false;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED &&
                ev.u.closed.code == (uint64_t)MOQ_CLOSE_IDLE_TIMEOUT)
                saw_idle_close = true;
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE)
                saw_setup = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_idle_close);
        MOQ_TEST_CHECK(!saw_setup);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Public hard close (moq_session_close) ======================= *
     * The transport-agnostic auth-denial path: an app holding only the
     * session (no adapter/connection object) hard-closes it. Must queue
     * BOTH the CLOSE_SESSION action (for the adapter) and the
     * SESSION_CLOSED event (for the app), superseding any stale queued
     * outputs, exactly like the internal close path. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_close(NULL, 1, "x", 0) == MOQ_ERR_INVAL);

        /* Queue stale output first: start() queues the SETUP send. */
        MOQ_TEST_CHECK(moq_session_start(s, 1000) == MOQ_OK);

        static const char reason[] = "auth denied";
        MOQ_TEST_CHECK(moq_session_close(s, 0x4442u, reason, 2000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        /* Idempotent: a second close is MOQ_OK and does NOT overwrite the
         * still-undrained original close outputs. */
        MOQ_TEST_CHECK(moq_session_close(s, 0x9999u, "other", 3000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

        /* Exactly ONE action: CLOSE_SESSION with the ORIGINAL code and
         * the caller's reason bytes (borrowed, per the documented
         * contract) -- the stale SETUP send was superseded. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(s, acts, 4);
        MOQ_TEST_CHECK(na == 1);
        MOQ_TEST_CHECK(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);
        MOQ_TEST_CHECK(acts[0].u.close_session.code == 0x4442u);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.len ==
                       sizeof(reason) - 1);
        MOQ_TEST_CHECK(acts[0].u.close_session.reason.len == 0 ||
                       memcmp(acts[0].u.close_session.reason.data, reason,
                              sizeof(reason) - 1) == 0);

        /* Exactly ONE event: SESSION_CLOSED, same code/reason. */
        moq_event_t evs[4];
        size_t ne = moq_session_poll_events(s, evs, 4);
        MOQ_TEST_CHECK(ne == 1);
        MOQ_TEST_CHECK(evs[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evs[0].u.closed.code == 0x4442u);
        MOQ_TEST_CHECK(evs[0].u.closed.reason.len == sizeof(reason) - 1);
        MOQ_TEST_CHECK(evs[0].u.closed.reason.len == 0 ||
                       memcmp(evs[0].u.closed.reason.data, reason,
                              sizeof(reason) - 1) == 0);
        moq_event_cleanup(&evs[0]);

        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_foundation");
    return failures;
}
