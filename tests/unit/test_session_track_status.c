#include <moq/codec.h>
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* == Outbound track_status → accept → OK event =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &handle)
            == MOQ_OK);

        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
        MOQ_TEST_CHECK(ev.u.track_status_request.track_namespace.count == 1);
        MOQ_TEST_CHECK(ev.u.track_status_request.track_name.len == 1);

        moq_accept_track_status_cfg_t acc;
        moq_accept_track_status_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_track_status(sv,
            ev.u.track_status_request.handle, &acc, 2000) == MOQ_OK);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_OK);
        MOQ_TEST_CHECK(moq_track_status_handle_eq(
            ev.u.track_status_ok.handle, handle));

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Accept with both LARGEST_OBJECT and EXPIRES set ============== *
     *  Both params must encode in ascending Type order (EXPIRES 0x08 before
     *  LARGEST_OBJECT 0x09); a valid REQUEST_OK is produced and both surface. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_track_status_handle_t handle;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);

        moq_accept_track_status_cfg_t acc;
        moq_accept_track_status_cfg_init(&acc);
        acc.has_largest = true; acc.largest_group = 11; acc.largest_object = 4;
        acc.has_expires = true; acc.expires_ms = 1000;
        MOQ_TEST_CHECK(moq_session_accept_track_status(sv,
            ev.u.track_status_request.handle, &acc, 2000) == MOQ_OK);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_OK);
        MOQ_TEST_CHECK(ev.u.track_status_ok.has_largest);
        MOQ_TEST_CHECK(ev.u.track_status_ok.largest_group == 11);
        MOQ_TEST_CHECK(ev.u.track_status_ok.largest_object == 4);
        MOQ_TEST_CHECK(ev.u.track_status_ok.has_expires);
        MOQ_TEST_CHECK(ev.u.track_status_ok.expires_ms == 1000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound track_status → reject → ERROR event ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &handle)
            == MOQ_OK);

        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);

        moq_reject_track_status_cfg_t rej;
        moq_reject_track_status_cfg_init(&rej);
        rej.error_code = 0x10;
        MOQ_TEST_CHECK(moq_session_reject_track_status(sv,
            ev.u.track_status_request.handle, &rej, 2000) == MOQ_OK);

        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_ERROR);
        MOQ_TEST_CHECK(ev.u.track_status_error.error_code == 0x10);

        /* Handle should be stale after error. */
        MOQ_TEST_CHECK(ts_resolve_handle(c, handle) < 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Handle stale after OK ======================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        moq_accept_track_status_cfg_t acc;
        moq_accept_track_status_cfg_init(&acc);
        moq_session_accept_track_status(sv,
            ev.u.track_status_request.handle, &acc, 2000);
        pump_actions_to_peer(sv, c, 2000);
        moq_session_poll_events(c, &ev, 1);

        MOQ_TEST_CHECK(ts_resolve_handle(c, handle) < 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Action WB on accept — no mutation ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        /* Send two track-status requests to get two pending entries. */
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_track_status_handle_t h1;
        moq_session_track_status(c, &cfg, 1000, &h1);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_track_status_handle_t sv_h1 =
            ev.u.track_status_request.handle;

        cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_track_status_handle_t h2;
        moq_session_track_status(c, &cfg, 1000, &h2);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &ev, 1);
        moq_track_status_handle_t sv_h2 =
            ev.u.track_status_request.handle;

        moq_accept_track_status_cfg_t acc;
        moq_accept_track_status_cfg_init(&acc);

        MOQ_TEST_CHECK(moq_session_accept_track_status(sv,
            sv_h1, &acc, 2000) == MOQ_OK);
        /* Queue full (1/1). Second accept should WB. */
        MOQ_TEST_CHECK(moq_session_accept_track_status(sv,
            sv_h2, &acc, 2000) == MOQ_ERR_WOULD_BLOCK);

        /* sv_h2 still live — no mutation on WB. */
        MOQ_TEST_CHECK(ts_resolve_handle(sv, sv_h2) >= 0);

        /* Drain and retry. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_accept_track_status(sv,
            sv_h2, &acc, 3000) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Action WB on reject — no mutation ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t1");
        moq_track_status_handle_t h1;
        moq_session_track_status(c, &cfg, 1000, &h1);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_track_status_handle_t sv_h1 =
            ev.u.track_status_request.handle;

        cfg.track_name = MOQ_BYTES_LITERAL("t2");
        moq_track_status_handle_t h2;
        moq_session_track_status(c, &cfg, 1000, &h2);
        pump_actions_to_peer(c, sv, 1000);
        moq_session_poll_events(sv, &ev, 1);
        moq_track_status_handle_t sv_h2 =
            ev.u.track_status_request.handle;

        moq_reject_track_status_cfg_t rej;
        moq_reject_track_status_cfg_init(&rej);
        rej.error_code = 0x10;

        MOQ_TEST_CHECK(moq_session_reject_track_status(sv,
            sv_h1, &rej, 2000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_reject_track_status(sv,
            sv_h2, &rej, 2000) == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(ts_resolve_handle(sv, sv_h2) >= 0);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        MOQ_TEST_CHECK(moq_session_reject_track_status(sv,
            sv_h2, &rej, 3000) == MOQ_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_OK routes to TRACK_STATUS_OK, not SUBSCRIBE_OK ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        moq_accept_track_status_cfg_t acc;
        moq_accept_track_status_cfg_init(&acc);
        moq_session_accept_track_status(sv,
            ev.u.track_status_request.handle, &acc, 2000);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_OK);
        MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_SUBSCRIBE_OK);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_UPDATE targeting track-status → NOT_SUPPORTED ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 2, 0, NULL, 0);

        MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 3000), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                uint64_t mt = decode_action_msg_type(&acts[i]);
                if (mt == MOQ_D16_REQUEST_ERROR) {
                    moq_control_envelope_t env;
                    moq_buf_reader_t r;
                    moq_buf_reader_init(&r,
                        acts[i].u.send_control.data,
                        acts[i].u.send_control.len);
                    moq_control_decode_envelope(&r, &env);
                    moq_d16_request_error_t err;
                    moq_d16_decode_request_error(env.payload,
                        env.payload_len, &err);
                    MOQ_TEST_CHECK_EQ_U64(err.request_id, 2);
                    MOQ_TEST_CHECK_EQ_HEX(err.error_code,
                        (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED);
                    found_err = true;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Request capacity blocked ==================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 0, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &handle)
            == MOQ_ERR_REQUEST_BLOCKED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Output handle invalidated on failure ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 0, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        handle._opaque = 0xDEADBEEF;
        moq_result_t rc = moq_session_track_status(c, &cfg, 1000, &handle);
        MOQ_TEST_CHECK(rc == MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(handle._opaque == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Large namespace encodes into send_buf ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.send_buffer_size = 8192;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        uint8_t big[4000];
        memset(big, 'x', sizeof(big));
        moq_bytes_t ns_parts[] = {{ big, sizeof(big) }};
        uint8_t nm[90];
        memset(nm, 'y', sizeof(nm));

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = (moq_bytes_t){ nm, sizeof(nm) };

        moq_track_status_handle_t handle;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &handle)
            == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == LARGEST_OBJECT surfaced on TRACK_STATUS_OK =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* Manually build REQUEST_OK with LARGEST_OBJECT param and feed
         * it to the client as if the server sent it. */
        uint8_t ok_buf[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));

        uint8_t loc_buf[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
        moq_buf_write_varint(&lw, 5);
        moq_buf_write_varint(&lw, 10);
        moq_kvp_entry_t ok_params[1] = {{
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc_buf, .value_len = moq_buf_writer_offset(&lw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_ok(&ow, 0, ok_params, 1);

        MOQ_TEST_CHECK(moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_OK);
        MOQ_TEST_CHECK(ev.u.track_status_ok.has_largest == true);
        MOQ_TEST_CHECK(ev.u.track_status_ok.largest_group == 5);
        MOQ_TEST_CHECK(ev.u.track_status_ok.largest_object == 10);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Unknown param in REQUEST_OK closes session =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* Build REQUEST_OK with unknown param (0xFF). */
        uint8_t ok_buf[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        uint8_t val[] = { 0x42 };
        moq_kvp_entry_t bad_params[1] = {{
            .type = 0xFF,
            .value = val, .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_ok(&ow, 0, bad_params, 1);

        moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == LARGEST_OBJECT trailing bytes close session ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        uint8_t ok_buf[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));

        uint8_t loc_buf[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
        moq_buf_write_varint(&lw, 5);
        moq_buf_write_varint(&lw, 10);
        loc_buf[moq_buf_writer_offset(&lw)] = 0xFF;
        moq_kvp_entry_t ok_params[1] = {{
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc_buf, .value_len = moq_buf_writer_offset(&lw) + 1,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_ok(&ow, 0, ok_params, 1);

        moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Duplicate LARGEST_OBJECT closes session ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_track_status_handle_t handle;
        moq_session_track_status(c, &cfg, 1000, &handle);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        uint8_t ok_buf[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));

        uint8_t loc1[4], loc2[4];
        moq_buf_writer_t l1, l2;
        moq_buf_writer_init(&l1, loc1, sizeof(loc1));
        moq_buf_writer_init(&l2, loc2, sizeof(loc2));
        moq_buf_write_varint(&l1, 1);
        moq_buf_write_varint(&l1, 2);
        moq_buf_write_varint(&l2, 3);
        moq_buf_write_varint(&l2, 4);
        moq_kvp_entry_t ok_params[2] = {{
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc1, .value_len = moq_buf_writer_offset(&l1),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }, {
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc2, .value_len = moq_buf_writer_offset(&l2),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_request_ok(&ow, 0, ok_params, 2);

        moq_session_on_control_bytes(c, ok_buf,
            moq_buf_writer_offset(&ow), 2000);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

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
        };
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ ns_parts, 2 };
        cfg.track_name = MOQ_BYTES_LITERAL("video");

        moq_track_status_handle_t h;
        MOQ_TEST_CHECK(moq_session_track_status(c, &cfg, 1000, &h) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
        MOQ_TEST_CHECK(ev.u.track_status_request.track_namespace.count == 2);
        MOQ_TEST_CHECK(memcmp(
            ev.u.track_status_request.track_namespace.parts[0].data,
            "org", 3) == 0);
        MOQ_TEST_CHECK(memcmp(
            ev.u.track_status_request.track_namespace.parts[1].data,
            "example", 7) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == TRACK_STATUS with auth token reaches server event ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_track_status_cfg_t tscfg;
        moq_track_status_cfg_init(&tscfg);
        tscfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        tscfg.track_name = MOQ_BYTES_LITERAL("t");
        uint8_t tok_val[] = { 0xAA };
        moq_auth_token_t tok = {
            .token_type = 55,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        tscfg.auth_tokens = &tok;
        tscfg.auth_token_count = 1;

        moq_track_status_handle_t tsh;
        MOQ_TEST_CHECK_EQ_INT(moq_session_track_status(c, &tscfg, 0, &tsh),
            MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.track_status_request.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(
            ev.u.track_status_request.tokens[0].token_type, 55);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.track_status_request.tokens[0].token_value.len, 1);
        MOQ_TEST_CHECK(
            ev.u.track_status_request.tokens[0].token_value.data[0] == 0xAA);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_track_status");
    return failures;
}
