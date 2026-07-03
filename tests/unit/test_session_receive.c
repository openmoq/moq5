#include "test_session_support.h"

static void wrap_release_fn(void *ctx, const uint8_t *data, size_t len)
{
    (void)data; (void)len;
    int *count = (int *)ctx;
    (*count)++;
}

/* Establish a pair and drive one accepted subscription, returning the
 * server-side subscription handle to open subgroups on. Shared setup for the
 * SUBGROUP_FINISHED cases below (kept out of main so it does not need the
 * FEED_SEND_DATA macro, which each caller applies to the SEND_DATA actions). */
static void sf_setup(int *out_failures,
                     moq_alloc_t *alloc, const moq_session_cfg_t *c_extra,
                     const moq_session_cfg_t *s_extra,
                     moq_session_t **c_out, moq_session_t **sv_out,
                     moq_subscription_t *server_sub_out)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(alloc, 10, 10, &c, &sv, c_extra, s_extra);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

    moq_subscription_t sub;
    MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
    pump_actions_to_peer(c, sv, 0);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t server_sub = ev.u.subscribe_request.sub;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
    pump_actions_to_peer(sv, c, 0);

    MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);

    *c_out = c; *sv_out = sv; *server_sub_out = server_sub;
    *out_failures += failures;
}

int main(void)
{
    int failures = 0;
    /* == 11. Receive data path ======================================= */

    /* Helper: feed one SEND_DATA action to a receiver session. */
    #define FEED_SEND_DATA(to, ref, act, now) do { \
        bool _hp = ((act).u.send_data.payload != NULL); \
        bool _fin = (act).u.send_data.fin; \
        if ((act).u.send_data.header_len > 0) \
            moq_session_on_data_bytes((to), (ref), \
                (act).u.send_data.header, (act).u.send_data.header_len, \
                _fin && !_hp, (now)); \
        if (_hp) \
            moq_session_on_data_bytes((to), (ref), \
                moq_rcbuf_data((act).u.send_data.payload), \
                moq_rcbuf_len((act).u.send_data.payload), \
                _fin, (now)); \
        if (!_hp && (act).u.send_data.header_len == 0 && _fin) \
            moq_session_on_data_bytes((to), (ref), NULL, 0, true, (now)); \
    } while (0)

    /* -- Happy path: 3 objects received ------------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        uint64_t track_alias = ev.u.subscribe_ok.track_alias;
        (void)track_alias;

        /* Server opens subgroup and writes 3 objects. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 200;

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);

        const char *frames[] = { "alpha", "beta", "gamma" };
        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)frames[i],
                              strlen(frames[i]), &p);
            MOQ_TEST_CHECK(moq_session_write_object(sv, sg, (uint64_t)i, p, 0) == MOQ_OK);
            moq_rcbuf_decref(p);
        }
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        /* Poll SEND_DATA actions from server and feed to client. */
        moq_action_t acts[16];
        size_t na;
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Client should have 3 OBJECT_RECEIVED events. */
        for (int i = 0; i < 3; i++) {
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
            MOQ_TEST_CHECK(ev.u.object_received.group_id == 0);
            MOQ_TEST_CHECK(ev.u.object_received.subgroup_id == 0);
            MOQ_TEST_CHECK(ev.u.object_received.object_id == (uint64_t)i);
            MOQ_TEST_CHECK(ev.u.object_received.publisher_priority == 200);
            MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
            MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == strlen(frames[i]));
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                                   frames[i], strlen(frames[i])) == 0);
            moq_event_cleanup(&ev);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Drain every pending SEND_DATA action from `sv` into receiver `c`. */
    #define FEED_ALL_DATA(from, to, ref) do { \
        moq_action_t _a[16]; size_t _n; \
        while ((_n = moq_session_poll_actions((from), _a, 16)) > 0) \
            for (size_t _i = 0; _i < _n; _i++) { \
                if (_a[_i].kind == MOQ_ACTION_SEND_DATA) \
                    FEED_SEND_DATA((to), (ref), _a[_i], 0); \
                moq_action_cleanup(&_a[_i]); \
            } \
    } while (0)

    /* -- SUBGROUP_FINISHED: one object then graceful FIN -------------- *
     * Order must be OBJECT_RECEIVED then SUBGROUP_FINISHED; end_of_group=false;
     * sub valid / pub invalid, matching the object event. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        moq_subscription_t server_sub;
        sf_setup(&failures, &alloc, NULL, NULL, &c, &sv, &server_sub);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 4;
        sg_cfg.subgroup_id = 2;
        sg_cfg.publisher_priority = 11;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"only", 4, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xA1);
        FEED_ALL_DATA(sv, c, rx_ref);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 0);
        moq_subscription_t obj_sub = ev.u.object_received.sub;
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.group_id == 4);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.subgroup_id == 2);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.end_of_group == false);
        MOQ_TEST_CHECK(moq_subscription_is_valid(ev.u.subgroup_finished.sub));
        MOQ_TEST_CHECK(!moq_publication_is_valid(ev.u.subgroup_finished.pub));
        MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subgroup_finished.sub, obj_sub));
        moq_event_cleanup(&ev);

        /* No further events. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBGROUP_FINISHED: empty subgroup (header then FIN) ---------- *
     * A parsed subgroup header with no objects still yields SUBGROUP_FINISHED
     * (no preceding OBJECT_RECEIVED). */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        moq_subscription_t server_sub;
        sf_setup(&failures, &alloc, NULL, NULL, &c, &sv, &server_sub);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 9;
        sg_cfg.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xA2);
        FEED_ALL_DATA(sv, c, rx_ref);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.group_id == 9);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.end_of_group == false);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBGROUP_FINISHED: END_OF_GROUP subgroup header -------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        moq_subscription_t server_sub;
        sf_setup(&failures, &alloc, NULL, NULL, &c, &sv, &server_sub);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 3;
        sg_cfg.subgroup_id = 1;
        sg_cfg.end_of_group = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xA3);
        FEED_ALL_DATA(sv, c, rx_ref);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.end_of_group == true);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBGROUP_FINISHED: streaming mode (chunks) ------------------- *
     * The final OBJECT_CHUNK (end=true) precedes SUBGROUP_FINISHED. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cx = MOQ_SESSION_CFG_INIT;
        cx.streaming_objects = true;
        moq_session_t *c = NULL, *sv = NULL;
        moq_subscription_t server_sub;
        sf_setup(&failures, &alloc, &cx, NULL, &c, &sv, &server_sub);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 6;
        sg_cfg.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"streamed", 8, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xA4);
        FEED_ALL_DATA(sv, c, rx_ref);

        /* Drain chunks until the terminal chunk, then expect SUBGROUP_FINISHED. */
        bool saw_end_chunk = false, saw_finished = false;
        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
                if (ev.u.object_chunk.end) saw_end_chunk = true;
                /* SUBGROUP_FINISHED must not arrive before the terminal chunk. */
                MOQ_TEST_CHECK(!saw_finished);
            } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                MOQ_TEST_CHECK(saw_end_chunk);
                saw_finished = true;
                MOQ_TEST_CHECK(ev.u.subgroup_finished.group_id == 6);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_end_chunk);
        MOQ_TEST_CHECK(saw_finished);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- No SUBGROUP_FINISHED on reset -------------------------------- *
     * A subgroup reset (RESET_STREAM) after an object must NOT emit
     * SUBGROUP_FINISHED. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        moq_subscription_t server_sub;
        sf_setup(&failures, &alloc, NULL, NULL, &c, &sv, &server_sub);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 2;
        sg_cfg.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"ab", 2, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xA5);
        FEED_ALL_DATA(sv, c, rx_ref);

        /* Object delivered. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Reset the subgroup and deliver the RESET to the receiver. */
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        moq_action_t a[8]; size_t n;
        while ((n = moq_session_poll_actions(sv, a, 8)) > 0)
            for (size_t i = 0; i < n; i++) moq_action_cleanup(&a[i]);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, rx_ref, 0, 0) == MOQ_OK);

        /* No SUBGROUP_FINISHED (nor any further event). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- No SUBGROUP_FINISHED for an unresolved FIRST_OBJECT subgroup - *
     * A FIRST_OBJECT-mode subgroup header whose stream FINs with no object
     * never resolves its subgroup ID, so no event is fabricated (a ZERO-mode
     * empty subgroup, tested above, does emit). Fed as raw d16 bytes because
     * open_subgroup cannot author FIRST_OBJECT mode. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* FIRST_OBJECT-mode subgroup header, then a bare FIN (no object). */
        uint8_t wire[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x12;   /* subgroup, ID mode 0b01 (FIRST_OBJECT) */
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_FIRST_OBJ;
        shdr.track_alias = alias;
        shdr.group_id = 5;
        shdr.publisher_priority = 128;
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &shdr) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xB1);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        /* No SUBGROUP_FINISHED (nor any object); session stays up. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- SUBGROUP_FINISHED survives a freed binding while parked ------- *
     * With a full event queue the final object parks SUBGROUP_FINISHED. If the
     * app frees the subscription before the retained FIN is retried, the event
     * is still owed (the object already transferred with the stored handle):
     * the retry must emit it, not degrade to STOP_DATA. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cx = MOQ_SESSION_CFG_INIT;
        cx.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cx, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);  /* drains SUBSCRIBE_OK */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 1;
        sg_cfg.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"z", 1, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        /* Object fills the 1-slot queue; the FIN parks SUBGROUP_FINISHED. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(0xB2);
        FEED_ALL_DATA(sv, c, rx_ref);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Free the client subscription while the finish is still parked. */
        MOQ_TEST_CHECK(moq_session_unsubscribe(c, sub, 0) == MOQ_OK);

        /* Retry the FIN: SUBGROUP_FINISHED must still emit (not STOP_DATA). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.group_id == 1);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    #undef FEED_ALL_DATA

    /* -- Legacy writers on an EXTENSIONS-enabled subgroup ------------- *
     * A subgroup opened with object_properties=true advertises the EXTENSIONS
     * bit, so the receiver decodes every object header as
     * delta + extensions_len [+ payload_len]. The legacy (non-_ex) writers must
     * therefore still emit a zero-length extensions field; otherwise they write
     * the non-extension header and the peer misparses payload_len as
     * extensions_len. Round-trips write_object, begin_object, and
     * write_status_object onto an extension-enabled subgroup and asserts the
     * receiver decodes each cleanly. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);

        /* Extension-enabled subgroup. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 200;
        sg_cfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);

        /* object 0: legacy complete-object write. */
        moq_rcbuf_t *p0 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"alpha", 5, &p0);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p0, 0) == MOQ_OK);
        moq_rcbuf_decref(p0);

        /* object 1: legacy streamed object. */
        MOQ_TEST_CHECK(moq_session_begin_object(sv, sg, 1, 4, 0) == MOQ_OK);
        moq_rcbuf_t *p1 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"beta", 4, &p1);
        MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, p1, 0) == MOQ_OK);
        moq_rcbuf_decref(p1);
        MOQ_TEST_CHECK(moq_session_end_object(sv, sg, 0) == MOQ_OK);

        /* object 2: legacy terminal status object (END_OF_GROUP). */
        MOQ_TEST_CHECK(moq_session_write_status_object(
            sv, sg, 2, MOQ_OBJECT_END_OF_GROUP, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        /* Deliver the subgroup stream to the receiver. */
        moq_action_t acts[16];
        size_t na;
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(77);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        /* The receiver must decode all three objects (a misparse of the first
         * object header would never emit these events). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 0);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL &&
                       moq_rcbuf_len(ev.u.object_received.payload) == 5);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 1);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL &&
                       moq_rcbuf_len(ev.u.object_received.payload) == 4);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 2);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_END_OF_GROUP);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Byte-at-a-time chunking -------------------------------------- */
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

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        /* Collect all SEND_DATA bytes into a flat buffer. */
        uint8_t all_bytes[512];
        size_t total = 0;
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (acts[i].u.send_data.header_len > 0) {
                        memcpy(all_bytes + total, acts[i].u.send_data.header,
                               acts[i].u.send_data.header_len);
                        total += acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t plen = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(all_bytes + total,
                               moq_rcbuf_data(acts[i].u.send_data.payload), plen);
                        total += plen;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Feed one byte at a time. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(50);
        for (size_t i = 0; i < total; i++) {
            MOQ_TEST_CHECK(moq_session_on_data_bytes(
                c, rx_ref, &all_bytes[i], 1, false, 0) == MOQ_OK);
        }
        /* Send FIN. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(
            c, rx_ref, NULL, 0, true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                               "hello", 5) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Multi-object with ID gaps ------------------------------------ */
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

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        uint64_t ids[] = { 0, 5, 10 };
        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &p);
            moq_session_write_object(sv, sg, ids[i], p, 0);
            moq_rcbuf_decref(p);
        }
        moq_session_close_subgroup(sv, sg, 0);

        /* Feed all data bytes to client. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(77);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        for (int i = 0; i < 3; i++) {
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
            MOQ_TEST_CHECK(ev.u.object_received.object_id == ids[i]);
            moq_event_cleanup(&ev);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- FIN before complete payload → PROTOCOL_VIOLATION ------------- */
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

        /* Server opens subgroup and writes a 100-byte object. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        uint8_t big_payload[100];
        memset(big_payload, 'A', 100);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, big_payload, 100, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        /* Collect all data bytes. */
        uint8_t all_bytes[512];
        size_t total = 0;
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (acts[i].u.send_data.header_len > 0) {
                        memcpy(all_bytes + total, acts[i].u.send_data.header,
                               acts[i].u.send_data.header_len);
                        total += acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t plen = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(all_bytes + total,
                               moq_rcbuf_data(acts[i].u.send_data.payload), plen);
                        total += plen;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Feed only half then FIN → should close with PROTOCOL_VIOLATION. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(88);
        size_t half = total / 2;
        moq_session_on_data_bytes(c, rx_ref, all_bytes, half, false, 0);
        moq_session_on_data_bytes(c, rx_ref, NULL, 0, true, 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- Unknown track alias → STOP_DATA, session stays alive --------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode a subgroup header with a bogus track_alias. */
        uint8_t hdr_buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr_buf, sizeof(hdr_buf));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 9999;
        hdr.group_id = 0;
        hdr.subgroup_id = 0;
        hdr.publisher_priority = 128;
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &hdr) == MOQ_OK);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(42);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(
            c, rx_ref, hdr_buf, moq_buf_writer_offset(&w), false, 0) == MOQ_OK);

        /* Session should still be alive. */
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Should have a STOP_DATA action. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(c, acts, 8);
        bool found_stop = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_STOP_DATA)
                found_stop = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_stop);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- event_cleanup idempotent ------------------------------------- */
    {
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_SETUP_COMPLETE;
        moq_event_cleanup(&ev);

        ev.kind = MOQ_EVENT_OBJECT_RECEIVED;
        ev.u.object_received.payload = NULL;
        moq_event_cleanup(&ev);
        moq_event_cleanup(&ev);
    }

    /* -- destroy decrefs queued event payloads ------------------------ */
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

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        /* Feed to client but do NOT poll events. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(33);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Destroy without polling events → payloads must be decreffed. */
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- on_data_reset cleans up stream ------------------------------- */
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

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Feed subgroup header to create an rx_stream. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(55);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_session_on_data_bytes(c, rx_ref,
                    acts[i].u.send_data.header,
                    acts[i].u.send_data.header_len,
                    false, 0);
            }
            moq_action_cleanup(&acts[i]);
        }

        /* Reset the stream. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, rx_ref, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- recv_payload_bytes accounting: poll releases budget ----------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 48;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Write two 20-byte objects. Budget is 48, so the second should
         * fail unless the first is released on poll. Wire overhead for
         * subgroup header + object header is ~13 bytes per SEND_DATA. */
        uint8_t twenty[20];
        memset(twenty, 'A', 20);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, twenty, 20, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        /* Feed first object to client. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(44);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Poll and cleanup the first event → releases budget. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Write and deliver second object. Must succeed (budget freed). */
        moq_rcbuf_create(&alloc, twenty, 20, &p);
        moq_session_write_object(sv, sg, 1, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 1);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- event-queue-full retryable ----------------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        /* DON'T drain SUBSCRIBE_OK from client — leave it queued to fill event queue. */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"retry", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        /* Feed first object. Event queue has 1 SUBSCRIBE_OK already,
         * capacity=2, so one OBJECT_RECEIVED fits. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(66);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        /* Drain both queued events. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                               "retry", 5) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- event-queue-full + retry via zero-byte on_data_bytes ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        /* SUBSCRIBE_OK fills the 1-slot event queue. */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"blk", 3, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        /* Feed the data. Event queue full → should return WOULD_BLOCK. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(77);
        moq_action_t acts[16];
        size_t na;
        moq_result_t feed_rc = MOQ_OK;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    bool hp = (acts[i].u.send_data.payload != NULL);
                    if (acts[i].u.send_data.header_len > 0) {
                        moq_result_t r = moq_session_on_data_bytes(c, rx_ref,
                            acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len,
                            acts[i].u.send_data.fin && !hp, 0);
                        if (r < 0) feed_rc = r;
                    }
                    if (feed_rc >= 0 && hp) {
                        moq_result_t r = moq_session_on_data_bytes(c, rx_ref,
                            moq_rcbuf_data(acts[i].u.send_data.payload),
                            moq_rcbuf_len(acts[i].u.send_data.payload),
                            acts[i].u.send_data.fin, 0);
                        if (r < 0) feed_rc = r;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
        }
        MOQ_TEST_CHECK(feed_rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain the SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Retry via zero-byte on_data_bytes. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 3);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                               "blk", 3) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- STOP_DATA queue-full retryable (unknown alias) ---------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_actions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        /* Encode a subgroup header with bogus alias. */
        uint8_t hdr_buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr_buf, sizeof(hdr_buf));
        moq_d16_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.type = 0x14;
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 9999;
        hdr.group_id = 0;
        hdr.subgroup_id = 0;
        hdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &hdr);

        /* Fill action queue so STOP_DATA can't queue. */
        moq_session_tick(c, 0);  /* advancing call */
        /* Subscribe to fill an action slot. forward=false on purpose: a
         * forwarding pending subscription would (correctly) DEFER the
         * unknown-alias stream as a reordering candidate; this case exercises
         * the STOP_DATA retry path, so the pending sub must be non-forwarding. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        sub_cfg.has_forward = true;
        sub_cfg.forward = false;
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);

        /* Feed bogus data — action queue full → WOULD_BLOCK. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(42);
        moq_result_t rc = moq_session_on_data_bytes(
            c, rx_ref, hdr_buf, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain actions to make room. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Retry via zero-byte on_data_bytes → STOP_DATA queues now. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(
            c, rx_ref, NULL, 0, false, 0) == MOQ_OK);

        na = moq_session_poll_actions(c, acts, 4);
        bool found_stop = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_STOP_DATA)
                found_stop = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_stop);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- zero-length Normal payload: non-NULL zero-length rcbuf ------- */
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

        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        /* Craft a subgroup header + zero-length Normal object manually. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = ev.u.subscribe_ok.track_alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        /* object: delta=0, payload_len=0, status=NORMAL(0x0) */
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(70);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- END_OF_GROUP/END_OF_TRACK status objects: NULL payload -------- */
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

        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* Craft subgroup with two status objects:
         * object 0 = END_OF_GROUP (0x3), object 1 = END_OF_TRACK (0x4) */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        /* object 0: delta=0, payload_len=0, status=END_OF_GROUP */
        moq_buf_write_varint(&w, 0);  /* delta */
        moq_buf_write_varint(&w, 0);  /* payload_len */
        moq_buf_write_varint(&w, 0x3); /* END_OF_GROUP */
        /* object 1: delta=0, payload_len=0, status=END_OF_TRACK */
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0x4);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(71);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_END_OF_GROUP);
        MOQ_TEST_CHECK(ev.u.object_received.payload == NULL);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK);
        MOQ_TEST_CHECK(ev.u.object_received.payload == NULL);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- data after FIN → PROTOCOL_VIOLATION -------------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* Send subgroup header + zero-length Normal + FIN. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(80);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Feed a syntactically valid subgroup header on the same ref. */
        uint8_t wire2[64];
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, wire2, sizeof(wire2));
        moq_d16_subgroup_header_t shdr2;
        memset(&shdr2, 0, sizeof(shdr2));
        shdr2.type = 0x14;
        shdr2.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr2.track_alias = alias;
        shdr2.group_id = 1;
        shdr2.subgroup_id = 0;
        shdr2.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w2, &shdr2);

        moq_session_on_data_bytes(c, rx_ref, wire2,
            moq_buf_writer_offset(&w2), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- extensions >64 bytes succeed (parsed from input_buf) ---------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* Craft a subgroup with has_extensions=true and a 100-byte ext block. */
        uint8_t wire[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        /* Type 0x15 = has_extensions + subgroup_id present. */
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);

        /* Valid KVP extension: type=1 (odd, delta=1), value_len=96,
         * 96 value bytes. Total block = 1 + 1 + 96 = 98 bytes (>64). */
        uint8_t ext_block[100];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, ext_block, sizeof(ext_block));
        moq_buf_write_varint(&ew, 1);
        moq_buf_write_varint(&ew, 96);
        for (int i = 0; i < 96; i++)
            moq_buf_write_raw(&ew, (const uint8_t *)"X", 1);
        size_t ext_sz = moq_buf_writer_offset(&ew);

        moq_buf_write_varint(&w, 0);          /* object delta */
        moq_buf_write_varint(&w, ext_sz);     /* extension length */
        moq_buf_write_raw(&w, ext_block, ext_sz);
        moq_buf_write_varint(&w, 0);          /* payload_len */
        moq_buf_write_varint(&w, 0);          /* status NORMAL */

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(90);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_event_t ext_ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ext_ev, 1) == 1);
        MOQ_TEST_CHECK(ext_ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ext_ev.u.object_received.properties != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ext_ev.u.object_received.properties) == ext_sz);
        moq_event_cleanup(&ext_ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- max_data_streams=1: FIN frees slot for next stream ----------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_data_streams = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* Stream A: complete object + FIN. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t ref_a = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_a,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);
        /* Stream A's graceful FIN also emits SUBGROUP_FINISHED. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* Stream B on different ref must succeed (slot freed). */
        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 1;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t ref_b = moq_stream_ref_from_u64(101);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_b,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 1);
        moq_event_cleanup(&ev);

        /* Reusing ref_a after FIN closes with PROTOCOL_VIOLATION. */
        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 2;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_session_on_data_bytes(c, ref_a, wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- zero-byte on unknown ref is a no-op, no slot allocated -------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_data_streams = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        /* Zero-byte no-op on ref1 must not consume the slot. */
        moq_stream_ref_t ref1 = moq_stream_ref_from_u64(200);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref1,
            NULL, 0, false, 0) == MOQ_OK);

        /* Real stream on ref2 must succeed with the single slot. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(201);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref2,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- FIN tombstoned after successful emit + reuse rejected --------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        cextra.max_data_streams = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t ref_a = moq_stream_ref_from_u64(300);
        /* The object fills the 1-slot queue, so SUBGROUP_FINISHED backpressures
         * (WOULD_BLOCK, stream held in PENDING_FINISHED, not yet tombstoned). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_a,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Retry: SUBGROUP_FINISHED now queues and the stream is tombstoned. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_a, NULL, 0, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 99;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_session_on_data_bytes(c, ref_a, wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- pending_fin with blocked emit -------------------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        cextra.max_data_streams = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        /* DON'T drain SUBSCRIBE_OK — event queue stays full. */

        /* Get alias from the queued event without draining. */
        /* We know alias is 1 (first subscription accepted by server). */
        uint64_t alias = 1;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t ref_a = moq_stream_ref_from_u64(400);
        /* Feed object+FIN. Event queue full → WOULD_BLOCK, fin preserved. */
        moq_result_t rc = moq_session_on_data_bytes(c, ref_a,
            wire, moq_buf_writer_offset(&w), true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Drain SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Retry zero-byte. The pending object emits and refills the 1-slot
         * queue, so SUBGROUP_FINISHED backpressures (WOULD_BLOCK). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_a,
            NULL, 0, false, 0) == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Retry again: SUBGROUP_FINISHED queues and the stream tombstones. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, ref_a,
            NULL, 0, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* ref_a is tombstoned. Reuse with valid header closes session. */
        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 99;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_session_on_data_bytes(c, ref_a, wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        /* A different ref should have worked (before the close). */

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- two objects + FIN in one buffer, blocked after first ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Craft subgroup header + two zero-length Normal objects + FIN. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0); /* object 0 */
        moq_d16_encode_object_header(&w, 0, 0); /* object 1 */

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(500);
        /* Feed entire buffer with FIN. First object emits, second
         * hits WOULD_BLOCK because event queue is full. */
        moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Drain first object. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 0);
        moq_event_cleanup(&ev);

        /* Retry. Second object emits and refills the queue, so
         * SUBGROUP_FINISHED backpressures (WOULD_BLOCK). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            NULL, 0, false, 0) == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 1);
        moq_event_cleanup(&ev);

        /* Retry: SUBGROUP_FINISHED queues, FIN tombstones. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            NULL, 0, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* ref is tombstoned. Reuse closes session. */
        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 99;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_session_on_data_bytes(c, rx_ref, wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- three objects with payload + FIN, blocked after first ---------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Build one buffer: subgroup header + obj0 (zero-len Normal)
         * + obj1 (100-byte payload) + obj2 (zero-len Normal) + FIN.
         * obj1's payload is large enough that obj2 bytes are NOT in
         * hdr_buf when obj1 completes — they're in the caller buffer. */
        uint8_t wire[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);  /* obj 0: zero-len Normal */

        /* obj 1: delta=0, 100-byte payload */
        moq_buf_write_varint(&w, 0);     /* delta */
        moq_buf_write_varint(&w, 100);   /* payload_len */
        for (int i = 0; i < 100; i++)
            moq_buf_write_raw(&w, (const uint8_t *)"P", 1);

        moq_d16_encode_object_header(&w, 0, 0);  /* obj 2: zero-len Normal */

        size_t wire_len = moq_buf_writer_offset(&w);
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(600);

        /* Feed entire buffer + FIN. obj0 emits, obj1 blocks. */
        moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
            wire, wire_len, true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Drain obj0, retry. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 0);
        moq_event_cleanup(&ev);

        rc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        /* obj1 emits, obj2 blocks. */
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 1);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 100);
        moq_event_cleanup(&ev);

        rc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        /* obj2 emits and refills the queue, so SUBGROUP_FINISHED backpressures. */
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 2);
        moq_event_cleanup(&ev);

        /* Retry: SUBGROUP_FINISHED queues, FIN tombstones. */
        rc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* Reuse of this ref closes session. */
        moq_buf_writer_init(&w, wire, sizeof(wire));
        shdr.group_id = 99;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_session_on_data_bytes(c, rx_ref, wire,
            moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- receive budget rejects over-limit input without alloc --------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 30;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Feed 100 bytes on a new stream. Budget is 30 so it must be
         * rejected with STOP_DATA, not allocated. */
        uint8_t big[100];
        memset(big, 'X', 100);
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(700);
        moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
            big, 100, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* No OBJECT_RECEIVED should have been emitted. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        /* STOP_DATA should have been queued (or NEED_STOP pending). */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        bool found_stop = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_STOP_DATA)
                found_stop = true;
            moq_action_cleanup(&acts[i]);
        }
        if (!found_stop && rc == MOQ_ERR_WOULD_BLOCK) {
            moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
            na = moq_session_poll_actions(c, acts, 4);
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_STOP_DATA)
                    found_stop = true;
                moq_action_cleanup(&acts[i]);
            }
        }
        MOQ_TEST_CHECK(found_stop);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- input_buf allocation stays within configured budget ----------- */
    {
        byte_alloc_state_t bas = {0};
        moq_alloc_t alloc = { &bas, byte_alloc, byte_realloc, byte_free };

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 64;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Reset peak to isolate receive-path allocations. */
        int64_t baseline = bas.live_bytes;
        bas.peak_bytes = bas.live_bytes;

        /* Feed 10 bytes, then 10 more. The first is freed on compact
         * before the second arrives. With exact-size input_buf, peak
         * allocation is 10 bytes (not 256). */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(800);
        uint8_t small[10];
        memset(small, 0, sizeof(small));
        small[0] = 0x14; /* valid subgroup type */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            small, 10, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            small, 10, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        int64_t rx_peak = bas.peak_bytes - baseline;
        MOQ_TEST_CHECK(rx_peak <= 64);
        MOQ_TEST_CHECK(rx_peak < 256);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(bas.balance == 0);
    }

    /* -- input_buf grows geometrically, not exact-fit per chunk --------- *
     * A large object-extension block that arrives split across many small
     * chunks accumulates in the rx input staging buffer until the whole block
     * can be parsed. Exact-fit growth reallocs once per chunk; geometric
     * (doubling) growth keeps the realloc count logarithmic while preserving
     * the bytes exactly and staying within the receive budget. */
    {
        byte_alloc_state_t bas = {0};
        moq_alloc_t alloc = { &bas, byte_alloc, byte_realloc, byte_free };

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 4096;  /* generous: isolate growth */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Subgroup header (has_extensions) + one object header: object 0 with a
         * single large KVP extension whose value bytes arrive in small chunks. */
        enum { GEOM_EXT_VLEN = 384, GEOM_CHUNK = 8 };
        uint8_t ext_block[512];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, ext_block, sizeof(ext_block));
        moq_buf_write_varint(&ew, 1);             /* KVP type 1 (odd: has value) */
        moq_buf_write_varint(&ew, GEOM_EXT_VLEN); /* value length */
        for (int i = 0; i < GEOM_EXT_VLEN; i++) {
            uint8_t b = (uint8_t)(i & 0xFF);
            moq_buf_write_raw(&ew, &b, 1);
        }
        size_t ext_len = moq_buf_writer_offset(&ew);

        uint8_t hdr[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr, sizeof(hdr));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;  /* has_extensions + subgroup_id present */
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_buf_write_varint(&w, 0);        /* object_id delta -> 0 */
        moq_buf_write_varint(&w, ext_len);  /* extension block length */

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(801);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            hdr, moq_buf_writer_offset(&w), false, 0) == MOQ_OK);

        /* Isolate the extension-staging reallocs from setup/header allocs. */
        bas.realloc_calls = 0;

        size_t n_chunks = 0;
        for (size_t off = 0; off < ext_len; off += GEOM_CHUNK) {
            size_t clen = ext_len - off < GEOM_CHUNK ? ext_len - off : GEOM_CHUNK;
            MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
                ext_block + off, clen, false, 0) == MOQ_OK);
            n_chunks++;
        }
        /* Exact-fit reallocs ~once per chunk; doubling stays well under half. */
        MOQ_TEST_CHECK(n_chunks > 8);
        MOQ_TEST_CHECK(bas.realloc_calls < (int64_t)n_chunks / 2);

        /* Complete the object (empty payload, NORMAL). A correct parse of the
         * fully-accumulated extension block is the data-preservation proof:
         * a single corrupted byte would fail KVP parsing and close the session. */
        uint8_t tail[2];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tail, sizeof(tail));
        moq_buf_write_varint(&tw, 0);  /* payload length 0 */
        moq_buf_write_varint(&tw, 0);  /* status NORMAL */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            tail, moq_buf_writer_offset(&tw), false, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.properties != NULL);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(bas.balance == 0);  /* recv_input_bytes fully released */
    }

    /* -- deferred replay hard failure latches the session (not silent retry) - *
     * A subgroup that arrives for an alias not yet established (while a
     * forwarding subscribe is pending) is buffered "deferred". When the alias
     * binds, the buffered object is replayed. If that replay hits a hard error
     * (NOMEM) -- which has no return channel here -- it must latch a terminal
     * close, not be silently swallowed into an invisible retry. */
    {
        fail_alloc_state_t fas = {0};
        moq_alloc_t alloc = fail_allocator(&fas);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 65536;  /* room for the buffered object */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

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
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        /* Server accepts with a KNOWN track alias, but hold SUBSCRIBE_OK from c
         * (do not pump sv->c yet) so c's subscribe stays pending. */
        enum { DEFER_ALIAS = 7, DEFER_PAYLOAD_LEN = 4099 };
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = DEFER_ALIAS;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);

        /* Feed a subgroup for the still-unbound alias -> c buffers it deferred. */
        uint8_t wire[DEFER_PAYLOAD_LEN + 32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x12;  /* non-ext, FIRST_OBJ */
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_FIRST_OBJ;
        shdr.track_alias = DEFER_ALIAS;
        shdr.group_id = 0;
        shdr.publisher_priority = 100;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_buf_write_varint(&w, 0);                  /* object delta -> 0 */
        moq_buf_write_varint(&w, DEFER_PAYLOAD_LEN);  /* payload length */
        for (int i = 0; i < DEFER_PAYLOAD_LEN; i++) {
            uint8_t b = (uint8_t)(i & 0xFF);
            moq_buf_write_raw(&w, &b, 1);
        }
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(802);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Arm a hard failure the replay will hit (the replay allocates the
         * delivered payload rcbuf, a header+payload slab of at least the
         * payload length -- nothing else in the replay allocates this large),
         * then bind the alias: the SUBSCRIBE_OK handler replays the deferred
         * object synchronously. */
        fas.fail_min_size = DEFER_PAYLOAD_LEN;
        pump_actions_to_peer(sv, c, 0);
        fas.fail_min_size = 0;

        /* Option A: the hard replay failure latches a terminal internal close
         * rather than being swallowed into a silent, invisible retry. */
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* == 12. Subgroup ID modes ======================================== */

    /* -- FIRST_OBJ mode: subgroup_id equals first object's absolute ID -- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        /* Type 0x12 = no ext, mode=FIRST_OBJ, no EOG, explicit prio */
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x12;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_FIRST_OBJ;
        shdr.track_alias = alias;
        shdr.group_id = 5;
        shdr.publisher_priority = 100;
        moq_d16_encode_subgroup_header(&w, &shdr);

        /* First object: delta=7 → object_id=7 → subgroup_id should be 7 */
        moq_buf_write_varint(&w, 7);
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);

        /* Second object: delta=0 → object_id=8. subgroup_id stays 7 */
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(900);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 5);
        MOQ_TEST_CHECK(ev.u.object_received.subgroup_id == 7);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 7);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.subgroup_id == 7);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 8);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- ZERO mode: subgroup_id is always 0 ----------------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        /* Type 0x10 = no ext, mode=ZERO, no EOG, explicit prio */
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x10;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO;
        shdr.track_alias = alias;
        shdr.group_id = 3;
        shdr.publisher_priority = 200;
        moq_d16_encode_subgroup_header(&w, &shdr);

        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(901);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.subgroup_id == 0);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 3);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 13. Object extensions ========================================= */

    /* -- extensions with fragmented delivery ----------------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);

        /* Build valid KVP extension: type=1 (odd, delta=1), value_len=76,
         * 76 value bytes. Total extension block = 1 + 1 + 76 = 78 bytes. */
        uint8_t ext_block[80];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, ext_block, sizeof(ext_block));
        moq_buf_write_varint(&ew, 1);
        moq_buf_write_varint(&ew, 76);
        for (int i = 0; i < 76; i++)
            moq_buf_write_raw(&ew, (const uint8_t *)"E", 1);
        size_t ext_len = moq_buf_writer_offset(&ew);

        /* object: delta=0, ext_len, ext_data, payload_len=3, "abc" */
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, ext_len);
        moq_buf_write_raw(&w, ext_block, ext_len);
        moq_buf_write_varint(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);

        size_t total = moq_buf_writer_offset(&w);
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(902);

        size_t half = total / 2;
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, half, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire + half, total - half, true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.properties != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.properties) == ext_len);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 3);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- extensions on non-Normal status: rejected ---------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);

        /* object delta=0, ext_len=2, ext_data="XX", payload_len=0, status=END_OF_GROUP */
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"XX", 2);
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 3);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(903);
        moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- event_cleanup decrefs extensions ------------------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));

        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);

        /* Valid KVP extension: type=1 (odd, delta=1), value_len=3, "HEL".
         * Total ext block = 1 + 1 + 3 = 5 bytes. */
        uint8_t ext_kvp[8];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, ext_kvp, sizeof(ext_kvp));
        moq_buf_write_varint(&ew, 1);  /* delta → type=1 (odd) */
        moq_buf_write_varint(&ew, 3);  /* value_len */
        moq_buf_write_raw(&ew, (const uint8_t *)"HEL", 3);
        size_t ext_sz = moq_buf_writer_offset(&ew);

        moq_buf_write_varint(&w, 0);       /* object delta */
        moq_buf_write_varint(&w, ext_sz);   /* extension length */
        moq_buf_write_raw(&w, ext_kvp, ext_sz);
        moq_buf_write_varint(&w, 0);       /* payload_len */
        moq_buf_write_varint(&w, 0);       /* status NORMAL */

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(904);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.properties != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(ev.u.object_received.properties) == 1);

        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(ev.u.object_received.properties == NULL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 14. Publisher-side on_data_stop ================================ */

    /* -- write_object failure does not incref caller rcbuf --------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_session_close_subgroup(sv, sg, 0);

        moq_action_t acts[8];
        size_t nd = moq_session_poll_actions(sv, acts, 8);
        for (size_t di = 0; di < nd; di++) moq_action_cleanup(&acts[di]);
        moq_session_tick(sv, 1);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"X", 1, &p);
        uint32_t rc_before = moq_rcbuf_refcount(p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 2) ==
                        MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(p) == rc_before);
        moq_rcbuf_decref(p);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- OOM during input append is retryable -------------------------- */
    {
        fail_alloc_state_t fas = {0};
        moq_alloc_t falloc = fail_allocator(&fas);

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&falloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        /* Fail the next alloc (input_buf alloc). */
        fas.call_count = 0;
        fas.fail_at = 1;
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(950);
        moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        /* Retry with failures disabled. */
        fas.fail_at = 0;
        rc = moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* -- END_OF_GROUP bit surfaced in event --------------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Type 0x1C = no ext, mode=PRESENT, END_OF_GROUP, explicit prio */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x1C;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.end_of_group = true;
        shdr.track_alias = alias;
        shdr.group_id = 5;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(900);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.end_of_group == true);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 5);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- non-EOG subgroup has end_of_group==false ---------------------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(901);
        moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.u.object_received.end_of_group == false);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- malformed extension KVP closes with PROTOCOL_VIOLATION ------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Type 0x15 = has_extensions, mode=PRESENT, explicit prio */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x15;
        shdr.has_extensions = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        /* object delta=0 */
        moq_buf_write_varint(&w, 0);
        /* extension length=3, then 3 garbage bytes (not valid KVP) */
        moq_buf_write_varint(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"\xff\xff\xff", 3);
        /* payload_len=0, status=NORMAL */
        moq_buf_write_varint(&w, 0);
        moq_buf_write_varint(&w, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(910);
        moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- ABI_MISMATCH: SEND_DATA head, element too small -------------- */
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
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Drain SUBSCRIBE_OK from sv so only data actions remain. */
        moq_action_t drain_acts[4];
        moq_session_poll_actions(sv, drain_acts, 4);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, sv_sub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"X", 1, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        /* Queue has: SEND_DATA (header-only), SEND_DATA (with rcbuf) */

        /* Poll with element_size that fits prefix but not SEND_DATA.
         * All SEND_DATA actions require full element_size, even
         * header-only, to prevent silent data loss. */
        size_t small_size = offsetof(moq_action_t, u) +
                            sizeof(moq_send_control_action_t);
        uint8_t buf[2][256];
        size_t count = 0;
        moq_result_t rc = moq_session_poll_actions_ex(sv, buf, 2,
            small_size, &count);
        MOQ_TEST_CHECK(rc == MOQ_ERR_ABI_MISMATCH);
        MOQ_TEST_CHECK(count == 0);

        /* Full-size poll gets both. */
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

    /* -- ABI_MISMATCH: OBJECT_RECEIVED head, element too small -------- */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ssub, &acfg, 0);
        pump_actions_to_peer(sv, c, 0);
        moq_session_poll_events(c, &ev, 1);
        uint64_t alias = ev.u.subscribe_ok.track_alias;
        moq_event_cleanup(&ev);

        /* Feed an object to queue OBJECT_RECEIVED. */
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.type = 0x14;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        moq_d16_encode_subgroup_header(&w, &shdr);
        moq_d16_encode_object_header(&w, 0, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(920);
        moq_session_on_data_bytes(c, rx_ref,
            wire, moq_buf_writer_offset(&w), true, 0);

        /* Poll with element too small for OBJECT_RECEIVED. */
        size_t small_size = offsetof(moq_event_t, u) +
                            sizeof(moq_setup_complete_event_t);
        uint8_t buf[256];
        size_t count = 0;
        moq_result_t rc = moq_session_poll_events_ex(c, buf, 1,
            small_size, &count);
        MOQ_TEST_CHECK(rc == MOQ_ERR_ABI_MISMATCH);
        MOQ_TEST_CHECK(count == 0);

        /* Full-size poll works. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- on_data_stop: closed session → ERR_CLOSED -------------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc, MOQ_PERSPECTIVE_SERVER);
        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK(moq_session_on_data_stop(s, ref, 0, 0) == MOQ_ERR_CLOSED);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* -- on_data_stop: oversized error_code → ERR_INVAL --------------- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK(moq_session_on_data_stop(sv, ref,
            UINT64_MAX, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 16. SimPair deterministic seeds ============================== */

    /* == Streaming OBJECT_CHUNK: basic single object ================== */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        /* Feed data in two chunks: header+partial, then rest+FIN. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(77);
        moq_action_t acts[16];
        size_t na;
        uint8_t combined[4096];
        size_t clen = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) {
                        memcpy(combined + clen, acts[i].u.send_data.header, hl);
                        clen += hl;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen,
                            moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed all at once. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, clen, true, 0) == MOQ_OK);

        /* Should get OBJECT_CHUNK with begin=true, end=true (small). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk),
            "hello", 5) == 0);
        MOQ_TEST_CHECK(ev.u.object_chunk.group_id == 0);
        MOQ_TEST_CHECK(ev.u.object_chunk.object_id == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming OBJECT_CHUNK: zero-length object =================== */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, NULL, 0, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(78);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk == NULL);
        MOQ_TEST_CHECK(ev.u.object_chunk.payload_length == 0);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming zero-copy FIN truncation must close (not pin) ======== */
    {
        /* A continuation chunk fed via on_data_rcbuf takes the zero-copy fast
         * path. A FIN that arrives there with the object payload still short
         * must close (truncated payload at FIN), exactly like the copy path --
         * not return MOQ_OK leaving the rx slot pinned in STREAMING_PAYLOAD.
         * Three scenarios share one captured wire image (header + 20 payload
         * bytes): (1) rcbuf truncated FIN closes; (2) on_data_bytes truncated
         * FIN already closes (parity control); (3) rcbuf full-payload FIN
         * completes cleanly (positive control). */
        const char *payload = "abcdefghijklmnopqrst";  /* 20 bytes */
        const size_t PAYLEN = 20;

        for (int scenario = 0; scenario < 3; scenario++) {
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
            if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)payload, PAYLEN, &p);
            moq_session_write_object(sv, sg, 0, p, 0);
            moq_rcbuf_decref(p);

            /* Capture the full wire image (header + 20 payload bytes). */
            uint8_t wire[4096];
            size_t wlen = 0;
            moq_action_t acts[16];
            size_t na;
            while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
                for (size_t i = 0; i < na; i++) {
                    if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                        size_t hl = acts[i].u.send_data.header_len;
                        if (hl > 0) {
                            memcpy(wire + wlen, acts[i].u.send_data.header, hl);
                            wlen += hl;
                        }
                        if (acts[i].u.send_data.payload) {
                            size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                            memcpy(wire + wlen,
                                moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                            wlen += pl;
                        }
                    }
                    moq_action_cleanup(&acts[i]);
                }
            MOQ_TEST_CHECK(wlen >= PAYLEN);
            size_t hdr_len = wlen - PAYLEN;

            moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(800 + scenario);

            /* Feed 1 (all scenarios): header + first 5 payload bytes, no FIN.
             * Leaves the rx in STREAMING_PAYLOAD, payload_written = 5. */
            moq_rcbuf_t *f1 = NULL;
            moq_rcbuf_create(&alloc, wire, hdr_len + 5, &f1);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_rcbuf(c, rx_ref,
                f1, false, 0), (int)MOQ_OK);
            moq_rcbuf_decref(f1);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                (int)MOQ_SESS_ESTABLISHED);

            /* Drain the begin chunk. */
            while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

            if (scenario == 0) {
                /* rcbuf truncated FIN: 10 more bytes (total 15 < 20) + FIN. */
                moq_rcbuf_t *f2 = NULL;
                moq_rcbuf_create(&alloc, (const uint8_t *)payload + 5, 10, &f2);
                moq_session_on_data_rcbuf(c, rx_ref, f2, true, 0);
                moq_rcbuf_decref(f2);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                    (int)MOQ_SESS_CLOSED);
            } else if (scenario == 1) {
                /* Copy-path parity: same truncated FIN via on_data_bytes. */
                moq_session_on_data_bytes(c, rx_ref,
                    (const uint8_t *)payload + 5, 10, true, 0);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                    (int)MOQ_SESS_CLOSED);
            } else {
                /* Positive control: rcbuf with the remaining 15 bytes + FIN
                 * completes the 20-byte payload -- terminal chunk, clean. */
                moq_rcbuf_t *f2 = NULL;
                moq_rcbuf_create(&alloc, (const uint8_t *)payload + 5, 15, &f2);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_rcbuf(c, rx_ref,
                    f2, true, 0), (int)MOQ_OK);
                moq_rcbuf_decref(f2);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                    (int)MOQ_SESS_ESTABLISHED);
                MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
                MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
                MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
                moq_event_cleanup(&ev);
            }

            /* Drain any residual events. */
            while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

            moq_session_destroy(c);
            moq_session_destroy(sv);
            MOQ_TEST_CHECK(as.balance == 0);
        }
    }

    /* == Whole-object mode unchanged with streaming_objects=false ====== */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(79);
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
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 4);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming WOULD_BLOCK begin chunk retry ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* DON'T drain SUBSCRIBE_OK — event queue full (1/1). */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(100);
        uint8_t combined[4096];
        size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed data — should WOULD_BLOCK because event queue is full. */
        moq_result_t drc = moq_session_on_data_bytes(c, rx_ref,
            combined, clen, true, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Drain the filler event (SUBSCRIBE_OK). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Retry with NULL/0. The begin+end chunk emits and refills the 1-slot
         * queue, so SUBGROUP_FINISHED backpressures (WOULD_BLOCK). */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Should get exactly 1 OBJECT_CHUNK with correct data. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk), "hello", 5) == 0);
        }
        MOQ_TEST_CHECK(ev.u.object_chunk.group_id == 0);
        MOQ_TEST_CHECK(ev.u.object_chunk.object_id == 0);
        moq_event_cleanup(&ev);

        /* Retry: SUBGROUP_FINISHED now queues. */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* No more events. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming WOULD_BLOCK final chunk retry ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* Drain SUBSCRIBE_OK so we can control timing. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* 30-byte payload, fed in 3 steps to force 3 chunks. */
        uint8_t payload[30];
        memset(payload, 0xAB, sizeof(payload));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(101);
        uint8_t combined[4096];
        size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Find where the payload starts by measuring header overhead.
         * The wire format is: subgroup_header + object_header(delta,
         * ext_len?, payload_len) + payload. The payload is the last
         * 30 bytes. */
        MOQ_TEST_CHECK(clen > 30);
        size_t hdr_len = clen - 30;

        /* Step 1: Feed header + first 10 payload bytes. Emits begin chunk. */
        moq_result_t rc = moq_session_on_data_bytes(c, rx_ref,
            combined, hdr_len + 10, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
        moq_event_cleanup(&ev);

        /* Step 2: Feed next 10 payload bytes. Emits continuation chunk
         * which fills the 1-slot queue. */
        rc = moq_session_on_data_bytes(c, rx_ref,
            combined + hdr_len + 10, 10, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        /* DON'T drain — leave continuation chunk in the queue. */

        /* Step 3: Feed final 10 payload bytes + FIN. The final chunk
         * must WOULD_BLOCK because the queue is full. */
        rc = moq_session_on_data_bytes(c, rx_ref,
            combined + hdr_len + 20, 10, true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain the continuation chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
        moq_event_cleanup(&ev);

        /* Retry — the final chunk emits and refills the 1-slot queue, so
         * SUBGROUP_FINISHED backpressures (WOULD_BLOCK). */
        rc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 10);
        }
        moq_event_cleanup(&ev);

        /* Retry: SUBGROUP_FINISHED now queues. */
        rc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* No more events. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming multi-chunk payload ================================= */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* 100-byte payload, fed in 10-byte pieces. */
        uint8_t payload[100];
        for (int i = 0; i < 100; i++) payload[i] = (uint8_t)i;
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(102);
        uint8_t combined[4096];
        size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed in 10-byte pieces to force multiple chunks. */
        uint8_t reassembled[100];
        size_t reassembled_len = 0;
        bool saw_begin = false, saw_end = false;
        int chunk_count = 0;

        for (size_t off = 0; off < clen; off += 10) {
            size_t feed = (clen - off < 10) ? clen - off : 10;
            bool is_last = (off + feed >= clen);
            moq_result_t frc = moq_session_on_data_bytes(c, rx_ref,
                combined + off, feed, is_last, 0);
            MOQ_TEST_CHECK(frc == MOQ_OK);

            while (moq_session_poll_events(c, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                    moq_event_cleanup(&ev);
                    continue;
                }
                MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
                if (ev.u.object_chunk.begin) {
                    MOQ_TEST_CHECK(!saw_begin);
                    saw_begin = true;
                    MOQ_TEST_CHECK(ev.u.object_chunk.payload_length == 100);
                }
                if (ev.u.object_chunk.end) {
                    MOQ_TEST_CHECK(!saw_end);
                    saw_end = true;
                }
                if (ev.u.object_chunk.chunk) {
                    size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
                    MOQ_TEST_CHECK(reassembled_len + cl <= sizeof(reassembled));
                    memcpy(reassembled + reassembled_len,
                        moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
                    reassembled_len += cl;
                }
                chunk_count++;
                moq_event_cleanup(&ev);
            }
        }

        MOQ_TEST_CHECK(saw_begin);
        MOQ_TEST_CHECK(saw_end);
        MOQ_TEST_CHECK(reassembled_len == 100);
        MOQ_TEST_CHECK(memcmp(reassembled, payload, 100) == 0);
        MOQ_TEST_CHECK(chunk_count >= 2);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming NOMEM chunk retry =================================== */
    {
        fail_alloc_state_t fas = { .balance = 0, .call_count = 0, .fail_at = 0 };
        moq_alloc_t alloc = fail_allocator(&fas);

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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"test", 4, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(103);
        uint8_t combined[4096];
        size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* First do a successful feed to measure alloc count. */
        fas.call_count = 0;
        moq_result_t frc = moq_session_on_data_bytes(c, rx_ref,
            combined, clen, true, 0);
        MOQ_TEST_CHECK(frc == MOQ_OK);
        int baseline = fas.call_count;

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        moq_event_cleanup(&ev);
        /* Drain the first object's SUBGROUP_FINISHED. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* Now set up a second object to test NOMEM on the chunk rcbuf.
         * The last alloc in the baseline is the chunk rcbuf. */
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &p2);
        moq_session_write_object(sv, sg, 1, p2, 0);
        moq_rcbuf_decref(p2);
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref2 = moq_stream_ref_from_u64(104);
        clen = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Fail on the last alloc (the chunk rcbuf create). */
        fas.call_count = 0;
        fas.fail_at = baseline;
        frc = moq_session_on_data_bytes(c, rx_ref2, combined, clen, true, 0);
        MOQ_TEST_CHECK(frc == MOQ_ERR_NOMEM);

        /* Clear the fail and retry. */
        fas.fail_at = 0;
        frc = moq_session_on_data_bytes(c, rx_ref2, NULL, 0, false, 0);
        MOQ_TEST_CHECK(frc == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk), "data", 4) == 0);
        }
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* == Streaming reset terminal: mid-object data reset ================ */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Write a 20-byte object. */
        uint8_t payload[20];
        memset(payload, 0xCC, sizeof(payload));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        /* Feed subgroup header + partial object to client. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(200);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed only the first half (header + partial payload). */
        size_t half = clen / 2;
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, half, false, 0) == MOQ_OK);

        /* Drain the begin chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
        moq_event_cleanup(&ev);

        /* Reset the stream mid-object. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, rx_ref, 0x1, 0) == MOQ_OK);

        /* Should get terminal RESET chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_RESET);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk == NULL);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming reset terminal: pending chunk then reset ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* Drain SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        uint8_t payload[30];
        memset(payload, 0xDD, sizeof(payload));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(201);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed header + first 10 bytes → begin chunk fills queue. */
        MOQ_TEST_CHECK(clen > 30);
        size_t hdr_len = clen - 30;
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, hdr_len + 10, false, 0) == MOQ_OK);

        /* Drain begin chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        moq_event_cleanup(&ev);

        /* Feed next 10 bytes — continuation chunk fills queue. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined + hdr_len + 10, 10, false, 0) == MOQ_OK);
        /* DON'T drain — queue full with continuation chunk. */

        /* Reset. Should need to push pending chunk first → WB. */
        moq_result_t rrc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        MOQ_TEST_CHECK(rrc == MOQ_ERR_WOULD_BLOCK);

        /* Drain continuation chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
        moq_event_cleanup(&ev);

        /* Retry reset — pushes pending chunk, then WB on terminal. */
        rrc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        if (rrc == MOQ_ERR_WOULD_BLOCK) {
            /* Pending payload chunk emitted, terminal still pending. */
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
            MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
            moq_event_cleanup(&ev);

            /* One more retry for the terminal. */
            rrc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        }
        MOQ_TEST_CHECK(rrc == MOQ_OK);

        /* Terminal RESET event. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_RESET);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk == NULL);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming reset terminal: WB then retry via on_data_bytes ===== */
    {
        /* handle_data_reset() may store the terminal RESET as PENDING_CHUNK
         * when the event queue is full. The documented retry path is
         * on_data_bytes(s, ref, NULL, 0, false, now) -- the generic
         * PENDING_CHUNK retry handler. That handler must free the rx slot
         * for a terminal reset (not recycle it to AWAITING_OBJECT as a normal
         * object completion). Otherwise the reset stream stays pinned and,
         * with max_data_streams=1, a fresh stream is refused with STOP_DATA. */
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        cextra.max_data_streams = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        uint8_t payload[30];
        memset(payload, 0xDD, sizeof(payload));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(250);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(clen > 30);
        size_t hdr_len = clen - 30;

        /* Feed header + first 10 bytes → begin chunk fills queue; drain it. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, hdr_len + 10, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        moq_event_cleanup(&ev);

        /* Feed next 10 bytes → continuation chunk fills queue; don't drain. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined + hdr_len + 10, 10, false, 0) == MOQ_OK);

        /* Reset mid-object: must push the pending continuation chunk first,
         * queue full → WOULD_BLOCK, terminal RESET stored as PENDING_CHUNK. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, rx_ref, 0x1, 0)
            == MOQ_ERR_WOULD_BLOCK);

        /* Drain continuation chunk → room for the terminal. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == false);
        moq_event_cleanup(&ev);

        /* Retry via the documented on_data_bytes path (NOT on_data_reset). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0)
            == MOQ_OK);

        /* Terminal RESET event emitted. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_RESET);
        moq_event_cleanup(&ev);

        /* The reset rx slot must be freed: a fresh data stream (group 1)
         * must be accepted, not refused with STOP_DATA (max_data_streams=1). */
        moq_subgroup_cfg_t sg2_cfg;
        moq_subgroup_cfg_init(&sg2_cfg);
        sg2_cfg.group_id = 1;
        moq_subgroup_handle_t sg2;
        moq_session_open_subgroup(sv, ssub, &sg2_cfg, 0, &sg2);
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &p2);
        moq_session_write_object(sv, sg2, 0, p2, 0);
        moq_rcbuf_decref(p2);
        moq_session_close_subgroup(sv, sg2, 0);

        uint8_t combined2[4096]; size_t clen2 = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined2 + clen2, acts[i].u.send_data.header, hl); clen2 += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined2 + clen2, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen2 += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref2 = moq_stream_ref_from_u64(251);
        /* The begin+end chunk emits and fills the 1-slot queue; the trailing
         * SUBGROUP_FINISHED backpressures (WOULD_BLOCK). The point here is that
         * the fresh stream is ACCEPTED (chunk delivered, no STOP_DATA). */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref2,
            combined2, clen2, true, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Accepted: an OBJECT_CHUNK event, and NO STOP_DATA action queued. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(c, acts, 16), 0);

        /* Retry drains the pending SUBGROUP_FINISHED. */
        moq_session_on_data_bytes(c, rx_ref2, NULL, 0, false, 0);
        while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming reset terminal: WB on terminal, retry after drain === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* Drain SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(202);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed all data — begin+end chunk fills queue. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, clen, false, 0) == MOQ_OK);
        /* DON'T drain — queue full with begin+end chunk. */

        /* Reset — should WB because queue is full. */
        moq_result_t rrc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        /* The begin+end chunk already completed the object normally,
         * so parse_state moved to AWAITING_OBJECT. No terminal needed. */
        if (rrc == MOQ_OK) {
            /* Object completed before reset arrived — correct. */
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
            MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
            MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
            MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
            moq_event_cleanup(&ev);
        }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming reset: no terminal before object begins ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        /* Create an rx stream with just the subgroup header (no object). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Feed subgroup header only (open_subgroup action). */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(203);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        /* Reset before any object is written. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, rx_ref, 0x1, 0) == MOQ_OK);

        /* No OBJECT_CHUNK events — no object was begun. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming reset: pending final chunk completes normally ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* DON'T drain SUBSCRIBE_OK — queue full (1/1). */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"done", 4, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(204);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed all data — begin+end chunk goes to PENDING_CHUNK because
         * the event queue is full (SUBSCRIBE_OK occupying the slot). */
        moq_result_t drc = moq_session_on_data_bytes(c, rx_ref,
            combined, clen, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Now reset arrives while the final chunk is pending. The pending
         * chunk has end=true — the object completed normally. Reset should
         * push that final chunk and NOT add a terminal=RESET event. */
        drc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Drain SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Retry reset. Pending final chunk should push successfully. */
        drc = moq_session_on_data_reset(c, rx_ref, 0x1, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);

        /* Should get the normal final chunk, NOT a terminal=RESET. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk), "done", 4) == 0);
        }
        moq_event_cleanup(&ev);

        /* No more events — no spurious terminal=RESET. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Streaming multi-object subgroup ================================ */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 7;
        sg_cfg.subgroup_id = 3;
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        static const struct { uint64_t id; const char *d; size_t l; } objs[] = {
            { 0, "alpha", 5 },
            { 1, "bravo!", 6 },
            { 2, "c", 1 },
        };

        for (int oi = 0; oi < 3; oi++) {
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)objs[oi].d,
                objs[oi].l, &p);
            moq_session_write_object(sv, sg, objs[oi].id, p, 0);
            moq_rcbuf_decref(p);
        }
        moq_session_close_subgroup(sv, sg, 0);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(205);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, clen, true, 0) == MOQ_OK);

        for (int oi = 0; oi < 3; oi++) {
            MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
            MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
            MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
            MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL);
            MOQ_TEST_CHECK(ev.u.object_chunk.group_id == 7);
            MOQ_TEST_CHECK(ev.u.object_chunk.subgroup_id == 3);
            MOQ_TEST_CHECK(ev.u.object_chunk.object_id == objs[oi].id);
            MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
            if (ev.u.object_chunk.chunk) {
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == objs[oi].l);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk),
                objs[oi].d, objs[oi].l) == 0);
            }
            moq_event_cleanup(&ev);
        }

        /* The subgroup's graceful FIN emits SUBGROUP_FINISHED after the objects. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Index: many rx streams resolve by stream_ref ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_data_streams = 8;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Create 8 subgroups → 8 independent data streams to client. */
        for (int i = 0; i < 8; i++) {
            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            sg_cfg.group_id = (uint64_t)i;
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &p);
            moq_session_write_object(sv, sg, 0, p, 0);
            moq_rcbuf_decref(p);
        }

        /* Feed each subgroup to the client on a unique stream_ref. */
        moq_action_t acts[64]; size_t na;
        int data_count = 0;
        while ((na = moq_session_poll_actions(sv, acts, 64)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    moq_stream_ref_t ref = moq_stream_ref_from_u64(
                        acts[i].u.send_data.stream_ref._v + 10000);
                    bool hp = (acts[i].u.send_data.payload != NULL);
                    bool fin = acts[i].u.send_data.fin;
                    if (acts[i].u.send_data.header_len > 0)
                        moq_session_on_data_bytes(c, ref,
                            acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len,
                            fin && !hp, 0);
                    if (hp)
                        moq_session_on_data_bytes(c, ref,
                            moq_rcbuf_data(acts[i].u.send_data.payload),
                            moq_rcbuf_len(acts[i].u.send_data.payload),
                            fin, 0);
                    data_count++;
                }
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(data_count > 8);

        /* All objects should have been received. */
        int obj_count = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) obj_count++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj_count == 8);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Index: collision chain survives removal ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* max_data_streams=4 → index cap=8, mask=7.
         * Keys 1, 11, 14 all hash to slot 4 under idx_hash & 7. */
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_data_streams = 4;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Open 3 subgroups and write one object on each (no close →
         * streams stay open in AWAITING_OBJECT). */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sgs[3];
        uint64_t refs[] = { 1, 11, 14 };
        for (int i = 0; i < 3; i++) {
            sg_cfg.group_id = (uint64_t)i;
            moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sgs[i]);
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &p);
            moq_session_write_object(sv, sgs[i], 0, p, 0);
            moq_rcbuf_decref(p);
        }

        /* Feed all 3 streams to client with the colliding refs.
         * Don't send FIN so streams stay open. */
        moq_action_t acts[32]; size_t na;
        int stream_idx = 0;
        while ((na = moq_session_poll_actions(sv, acts, 32)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    moq_stream_ref_t ref = moq_stream_ref_from_u64(
                        refs[stream_idx < 3 ? stream_idx : 2]);
                    if (acts[i].u.send_data.header_len > 0)
                        moq_session_on_data_bytes(c, ref,
                            acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len, false, 0);
                    if (acts[i].u.send_data.payload) {
                        moq_session_on_data_bytes(c, ref,
                            moq_rcbuf_data(acts[i].u.send_data.payload),
                            moq_rcbuf_len(acts[i].u.send_data.payload),
                            false, 0);
                        stream_idx++;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* All 3 first objects received. */
        int obj_count = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) obj_count++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj_count == 3);

        /* Reset the middle stream (ref=11). This removes it from the
         * index and must backshift to keep ref=14 findable. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(c,
            moq_stream_ref_from_u64(11), 0x1, 0) == MOQ_OK);

        /* Write a second object on the third subgroup (group_id=2). */
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"after", 5, &p2);
        moq_session_write_object(sv, sgs[2], 1, p2, 0);
        moq_rcbuf_decref(p2);

        /* Collect the second object's wire bytes. */
        uint8_t wire2[256]; size_t w2len = 0;
        while ((na = moq_session_poll_actions(sv, acts, 32)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire2 + w2len, acts[i].u.send_data.header, hl); w2len += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire2 + w2len, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        w2len += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed it on ref=14 — this MUST find the rx stream via index
         * lookup. If backshift broke the chain, this creates a new
         * stream and the subgroup header parse fails. */
        moq_stream_ref_t tail_ref = moq_stream_ref_from_u64(14);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, tail_ref,
            wire2, w2len, false, 0) == MOQ_OK);

        /* Assert the second object was received on group_id=2. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 2);
        MOQ_TEST_CHECK(ev.u.object_received.object_id == 1);
        MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
        if (ev.u.object_received.payload)
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 5);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Index: reset rx stream, reuse slot ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_data_streams = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Create rx stream, reset it, then create another on a different
         * stream_ref. This tests index remove + slot reuse. */
        moq_stream_ref_t ref = moq_stream_ref_from_u64(500);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"a", 1, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        while (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* New stream_ref for the second stream (can't reuse after FIN). */
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(501);
        sg_cfg.group_id = 1;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"b", 1, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, ref2, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.group_id == 1);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy: continuation chunk via on_data_rcbuf ================ */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        /* Write a 20-byte object via streaming send to control
         * the header/payload split precisely. */
        static const uint8_t payload[20] = {
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
        };
        moq_session_begin_object(sv, sg, 0, 20, 0);
        moq_rcbuf_t *pd = NULL;
        moq_rcbuf_create(&alloc, payload, 20, &pd);
        moq_session_write_object_data(sv, sg, pd, 0);
        moq_rcbuf_decref(pd);
        moq_session_end_object(sv, sg, 0);

        /* Collect: first action is header-only, second is payload-only. */
        uint8_t header_wire[64]; size_t hlen = 0;
        uint8_t payload_wire[64]; size_t plen = 0;
        moq_action_t acts[16]; size_t na;
        int act_idx = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (act_idx == 0) {
                        /* Subgroup header. */
                        memcpy(header_wire + hlen, acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len);
                        hlen += acts[i].u.send_data.header_len;
                    } else if (act_idx == 1) {
                        /* Object header (header-only from begin_object). */
                        memcpy(header_wire + hlen, acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len);
                        hlen += acts[i].u.send_data.header_len;
                    } else if (act_idx == 2) {
                        /* Payload data (from write_object_data). */
                        if (acts[i].u.send_data.payload) {
                            size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                            memcpy(payload_wire, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                            plen = pl;
                        }
                    }
                    act_idx++;
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(600);

        /* Step 1: Feed header bytes via raw on_data_bytes. This parses
         * the subgroup + object header and emits a begin chunk. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            header_wire, hlen, false, 0) == MOQ_OK);

        /* Drain the begin chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        moq_event_cleanup(&ev);

        /* Step 2: Feed payload via on_data_rcbuf. The parser is now in
         * STREAMING_PAYLOAD with empty input_buf. The continuation chunk
         * should be a zero-copy slice of the input rcbuf. */
        int release_count = 0;
        moq_rcbuf_t *transport_buf = NULL;
        moq_rcbuf_wrap(&alloc, payload_wire, plen,
            wrap_release_fn, &release_count, &transport_buf);

        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(c, rx_ref,
            transport_buf, false, 0) == MOQ_OK);
        moq_rcbuf_decref(transport_buf);
        MOQ_TEST_CHECK(release_count == 0);

        /* The continuation chunk should point into the original payload. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 20);
            /* Zero-copy proof: chunk data pointer is within the
             * original payload_wire buffer (not a copy). */
            uintptr_t cp = (uintptr_t)moq_rcbuf_data(ev.u.object_chunk.chunk);
            uintptr_t lo = (uintptr_t)payload_wire;
            uintptr_t hi = lo + plen;
            MOQ_TEST_CHECK(cp >= lo && cp < hi);
        }
        MOQ_TEST_CHECK(release_count == 0);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(release_count == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy: on_data_bytes does NOT expose caller memory ======== */
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
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"rawtest", 7, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(601);
        uint8_t combined[4096]; size_t clen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(combined + clen, acts[i].u.send_data.header, hl); clen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(combined + clen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        clen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed via raw on_data_bytes. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            combined, clen, false, 0) == MOQ_OK);

        /* Chunk data must NOT point into combined[] (must be a copy). */
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK && ev.u.object_chunk.chunk) {
                uintptr_t p = (uintptr_t)moq_rcbuf_data(ev.u.object_chunk.chunk);
                uintptr_t lo = (uintptr_t)combined;
                uintptr_t hi = lo + sizeof(combined);
                MOQ_TEST_CHECK(p < lo || p >= hi);
            }
            moq_event_cleanup(&ev);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy: WOULD_BLOCK on continuation, retry emits chunk ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
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
        /* Drain SUBSCRIBE_OK so we can control queue timing. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);

        static const uint8_t payload[10] = {1,2,3,4,5,6,7,8,9,10};
        moq_session_begin_object(sv, sg, 0, 10, 0);
        moq_rcbuf_t *pd = NULL;
        moq_rcbuf_create(&alloc, payload, 10, &pd);
        moq_session_write_object_data(sv, sg, pd, 0);
        moq_rcbuf_decref(pd);
        moq_session_end_object(sv, sg, 0);

        /* Collect header and payload wire bytes. */
        uint8_t header_wire[64]; size_t hlen = 0;
        uint8_t payload_wire[64]; size_t plen = 0;
        moq_action_t acts[16]; size_t na;
        int aidx = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (aidx <= 1) {
                        size_t hl = acts[i].u.send_data.header_len;
                        if (hl > 0) { memcpy(header_wire + hlen, acts[i].u.send_data.header, hl); hlen += hl; }
                    } else {
                        if (acts[i].u.send_data.payload) {
                            size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                            memcpy(payload_wire, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                            plen = pl;
                        }
                    }
                    aidx++;
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(700);

        /* Step 1: Feed headers via on_data_bytes. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref,
            header_wire, hlen, false, 0) == MOQ_OK);

        /* Drain begin chunk (fills queue, then queue is empty). */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        moq_event_cleanup(&ev);

        /* Step 2: Fill the 1-slot queue with a GOAWAY event. */
        moq_session_goaway(sv, NULL, 0, 0);
        pump_actions_to_peer(sv, c, 0);
        /* GOAWAY event now fills the client's 1-slot queue. */

        /* Step 3: Feed payload via on_data_rcbuf — should WOULD_BLOCK
         * because queue is full. */
        int release_count = 0;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(&alloc, payload_wire, plen, wrap_release_fn,
            &release_count, &tb);
        moq_result_t drc = moq_session_on_data_rcbuf(c, rx_ref,
            tb, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Release callback must NOT fire yet — pending slice holds ref. */
        moq_rcbuf_decref(tb);
        MOQ_TEST_CHECK(release_count == 0);

        /* Drain GOAWAY filler event. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);

        /* Step 4: Retry via on_data_bytes NULL/0 retry. */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);

        /* Exactly one final chunk. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == false);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk)
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 10);
        MOQ_TEST_CHECK(release_count == 0);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(release_count == 1);

        /* No more events, session still alive. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        while ((na = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy: raw on_data_bytes + rcbuf still copies begin ======= */
    /* (Covered by the partial subgroup header test below — that test
     * proves the hdr_len guard prevents false zero-copy.) */

    /* == Zero-copy relay: rcbuf pointer identity through forwarding ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Three sessions: publisher (pub_sv), relay (relay_c + relay_sv),
         * subscriber (sub_c). relay_c receives from publisher,
         * relay_sv sends to subscriber. */
        moq_session_cfg_t relay_c_extra = MOQ_SESSION_CFG_INIT;
        relay_c_extra.streaming_objects = true;

        moq_session_t *pub_sv = NULL, *relay_c = NULL;
        establish_pair(&alloc, 10, 10, &relay_c, &pub_sv,
            &relay_c_extra, NULL);

        moq_session_t *sub_c = NULL, *relay_sv = NULL;
        establish_pair(&alloc, 10, 10, &sub_c, &relay_sv, NULL, NULL);

        /* Publisher side: subscribe relay_c → pub_sv, accept. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("relay");
        moq_subscription_t rc_sub;
        moq_session_subscribe(relay_c, &sub_cfg, 0, &rc_sub);
        pump_actions_to_peer(relay_c, pub_sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(pub_sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t pub_ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(pub_sv, pub_ssub, &acc, 0);
        pump_actions_to_peer(pub_sv, relay_c, 0);
        if (moq_session_poll_events(relay_c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Subscriber side: subscribe sub_c → relay_sv, accept. */
        moq_subscription_t sc_sub;
        moq_session_subscribe(sub_c, &sub_cfg, 0, &sc_sub);
        pump_actions_to_peer(sub_c, relay_sv, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(relay_sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t rs_ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_session_accept_subscribe(relay_sv, rs_ssub, &acc, 0);
        pump_actions_to_peer(relay_sv, sub_c, 0);
        if (moq_session_poll_events(sub_c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Publisher writes a 20-byte object via streaming send. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t pub_sg;
        moq_session_open_subgroup(pub_sv, pub_ssub, &sg_cfg, 0, &pub_sg);
        moq_session_begin_object(pub_sv, pub_sg, 0, 20, 0);
        static const uint8_t payload[20] = {
            10,20,30,40,50,60,70,80,90,100,
            11,21,31,41,51,61,71,81,91,101
        };
        moq_rcbuf_t *pd = NULL;
        moq_rcbuf_create(&alloc, payload, 20, &pd);
        moq_session_write_object_data(pub_sv, pub_sg, pd, 0);
        moq_rcbuf_decref(pd);
        moq_session_end_object(pub_sv, pub_sg, 0);

        /* Collect publisher's header and payload wire bytes. */
        uint8_t hdr_wire[64]; size_t hwl = 0;
        uint8_t pay_wire[64]; size_t pwl = 0;
        moq_action_t acts[16]; size_t na;
        int aidx = 0;
        while ((na = moq_session_poll_actions(pub_sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (aidx <= 1 && acts[i].u.send_data.header_len > 0) {
                        memcpy(hdr_wire + hwl, acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len);
                        hwl += acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(pay_wire, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        pwl = pl;
                    }
                    aidx++;
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed header to relay via raw on_data_bytes. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(800);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(relay_c, rx_ref,
            hdr_wire, hwl, false, 0) == MOQ_OK);

        /* Drain begin chunk from relay. */
        MOQ_TEST_CHECK(moq_session_poll_events(relay_c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        uint64_t obj_payload_len = ev.u.object_chunk.payload_length;
        moq_event_cleanup(&ev);

        /* Feed payload to relay via on_data_rcbuf — the "transport buffer". */
        int release_count = 0;
        moq_rcbuf_t *transport_buf = NULL;
        moq_rcbuf_wrap(&alloc, pay_wire, pwl, wrap_release_fn,
            &release_count, &transport_buf);

        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(relay_c, rx_ref,
            transport_buf, false, 0) == MOQ_OK);
        moq_rcbuf_decref(transport_buf);

        /* Poll the continuation chunk from relay_c.
         * It should be a zero-copy slice of the transport buffer. */
        MOQ_TEST_CHECK(moq_session_poll_events(relay_c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);

        const uint8_t *received_ptr = moq_rcbuf_data(ev.u.object_chunk.chunk);
        size_t received_len = moq_rcbuf_len(ev.u.object_chunk.chunk);
        MOQ_TEST_CHECK(received_len == 20);

        /* Zero-copy proof: pointer identity + byte match. */
        MOQ_TEST_CHECK(pwl == 20);
        uintptr_t rp = (uintptr_t)received_ptr;
        uintptr_t lo = (uintptr_t)pay_wire;
        uintptr_t hi = lo + pwl;
        MOQ_TEST_CHECK(rp >= lo && rp < hi);
        MOQ_TEST_CHECK(memcmp(received_ptr, payload, 20) == 0);

        /* Forward to downstream: relay_sv sends to sub_c. */
        moq_subgroup_handle_t relay_sg;
        moq_session_open_subgroup(relay_sv, rs_ssub, &sg_cfg, 0, &relay_sg);
        moq_session_begin_object(relay_sv, relay_sg, 0, obj_payload_len, 0);
        moq_session_write_object_data(relay_sv, relay_sg,
            ev.u.object_chunk.chunk, 0);
        moq_session_end_object(relay_sv, relay_sg, 0);

        /* Poll SEND_DATA from relay_sv — the payload rcbuf should
         * carry the SAME pointer as the received chunk. */
        const uint8_t *forwarded_ptr = NULL;
        while ((na = moq_session_poll_actions(relay_sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA &&
                    acts[i].u.send_data.payload) {
                    forwarded_ptr = moq_rcbuf_data(acts[i].u.send_data.payload);
                }
                moq_action_cleanup(&acts[i]);
            }
        MOQ_TEST_CHECK(forwarded_ptr != NULL);
        MOQ_TEST_CHECK(forwarded_ptr == received_ptr);

        /* Release must NOT fire yet — event still holds a ref. */
        MOQ_TEST_CHECK(release_count == 0);

        /* Cleanup the event (decrefs the chunk slice). */
        moq_event_cleanup(&ev);

        /* Release fires now — all refs gone. */
        MOQ_TEST_CHECK(release_count == 1);

        moq_session_close_subgroup(pub_sv, pub_sg, 0);
        moq_session_close_subgroup(relay_sv, relay_sg, 0);

        while ((na = moq_session_poll_actions(pub_sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(relay_sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(pub_sv);
        moq_session_destroy(relay_c);
        moq_session_destroy(sub_c);
        moq_session_destroy(relay_sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy begin: full object in one rcbuf ===================== */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"begin_zc!", 9, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        /* Collect ALL wire bytes into one buffer. */
        uint8_t wire[4096]; size_t wlen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire + wlen, acts[i].u.send_data.header, hl); wlen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire + wlen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        wlen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed entire wire buffer as one rcbuf (subgroup header +
         * object header + payload + FIN in one call). */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(900);
        int release_count = 0;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(&alloc, wire, wlen, wrap_release_fn,
            &release_count, &tb);
        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(c, rx_ref,
            tb, true, 0) == MOQ_OK);
        moq_rcbuf_decref(tb);

        /* Should get a begin+end chunk. The payload portion should
         * be a zero-copy slice of the transport buffer. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 9);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk),
                "begin_zc!", 9) == 0);
            uintptr_t cp = (uintptr_t)moq_rcbuf_data(ev.u.object_chunk.chunk);
            uintptr_t lo = (uintptr_t)wire;
            uintptr_t hi = lo + wlen;
            MOQ_TEST_CHECK(cp >= lo && cp < hi);
        }
        MOQ_TEST_CHECK(release_count == 0);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(release_count == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy begin: partial subgroup header forces copy ========= */
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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"partial!", 8, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        uint8_t wire[4096]; size_t wlen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire + wlen, acts[i].u.send_data.header, hl); wlen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire + wlen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        wlen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed 1 byte of subgroup header via raw API. This puts it
         * into hdr_buf, setting hdr_len > 0. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(901);
        moq_session_on_data_bytes(c, rx_ref, wire, 1, false, 0);

        /* Feed the rest via on_data_rcbuf. With hdr_len > 0,
         * input_src_rcbuf must NOT be set → begin chunk copies. */
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(&alloc, wire + 1, wlen - 1, NULL, NULL, &tb);
        moq_session_on_data_rcbuf(c, rx_ref, tb, false, 0);
        moq_rcbuf_decref(tb);

        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK && ev.u.object_chunk.chunk) {
                uintptr_t cp = (uintptr_t)moq_rcbuf_data(ev.u.object_chunk.chunk);
                uintptr_t lo = (uintptr_t)wire;
                uintptr_t hi = lo + sizeof(wire);
                MOQ_TEST_CHECK(cp < lo || cp >= hi);
            }
            moq_event_cleanup(&ev);
        }

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy begin: WOULD_BLOCK retries correctly ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.streaming_objects = true;
        cextra.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        /* DON'T drain SUBSCRIBE_OK — fills the 1-slot queue. */

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"wb_begin", 8, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        uint8_t wire[4096]; size_t wlen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire + wlen, acts[i].u.send_data.header, hl); wlen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire + wlen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        wlen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(902);
        int release_count = 0;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(&alloc, wire, wlen, wrap_release_fn,
            &release_count, &tb);

        /* Feed entire object as one rcbuf — queue full → WOULD_BLOCK. */
        moq_result_t drc = moq_session_on_data_rcbuf(c, rx_ref,
            tb, true, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(tb);
        MOQ_TEST_CHECK(release_count == 0);

        /* Drain SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        /* Retry. The begin+end chunk emits and refills the 1-slot queue, so
         * SUBGROUP_FINISHED backpressures (WOULD_BLOCK). */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_ERR_WOULD_BLOCK);

        /* Should get exactly one begin+end chunk, no extras. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 8);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk),
                "wb_begin", 8) == 0);
        }
        MOQ_TEST_CHECK(release_count == 0);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(release_count == 1);

        /* Retry: SUBGROUP_FINISHED now queues. */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        /* No more events, session alive. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-copy begin: NOMEM on slice retries from input_buf ======== */
    {
        fail_alloc_state_t fas = { .balance = 0, .call_count = 0, .fail_at = 0 };
        moq_alloc_t alloc = fail_allocator(&fas);

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
        moq_subscription_t sub;
        moq_session_subscribe(c, &sub_cfg, 0, &sub);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ssub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"nomem", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        uint8_t wire[4096]; size_t wlen = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire + wlen, acts[i].u.send_data.header, hl); wlen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire + wlen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        wlen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(903);

        /* First do a baseline to find the alloc count. */
        fas.call_count = 0;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(&alloc, wire, wlen, NULL, NULL, &tb);
        moq_result_t drc = moq_session_on_data_rcbuf(c, rx_ref,
            tb, true, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);
        moq_rcbuf_decref(tb);
        int baseline = fas.call_count;

        while (moq_session_poll_events(c, &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Second object: fail on the last alloc (the slice header). */
        moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg);
        p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"retry", 5, &p);
        moq_session_write_object(sv, sg, 1, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        wlen = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(wire + wlen, acts[i].u.send_data.header, hl); wlen += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(wire + wlen, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        wlen += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        moq_stream_ref_t rx_ref2 = moq_stream_ref_from_u64(904);
        fas.call_count = 0;
        fas.fail_at = baseline;
        tb = NULL;
        moq_rcbuf_wrap(&alloc, wire, wlen, NULL, NULL, &tb);
        drc = moq_session_on_data_rcbuf(c, rx_ref2, tb, true, 0);
        moq_rcbuf_decref(tb);
        MOQ_TEST_CHECK(drc == MOQ_ERR_NOMEM);

        /* Clear fault and retry. */
        fas.fail_at = 0;
        drc = moq_session_on_data_bytes(c, rx_ref2, NULL, 0, false, 0);
        MOQ_TEST_CHECK(drc == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
        MOQ_TEST_CHECK(ev.u.object_chunk.begin == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.end == true);
        MOQ_TEST_CHECK(ev.u.object_chunk.chunk != NULL);
        if (ev.u.object_chunk.chunk) {
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_chunk.chunk) == 5);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_chunk.chunk),
                "retry", 5) == 0);
        }
        moq_event_cleanup(&ev);

        /* The subgroup's graceful FIN emits SUBGROUP_FINISHED after the object. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* -- FIN-only delivered while a stream is in PENDING_EMIT is retained ---
     * Regression for the inbound pending-retry FIN-loss: with the event queue
     * full (max_events=1), object 1 backs up into PENDING_EMIT. A FIN-only
     * delivery (len==0) must set pending_fin even though the pending retry
     * WOULD_BLOCKs, so that once the queue drains the object emits AND the rx
     * stream is finished/freed (the bridge marks fin_retained on that
     * WOULD_BLOCK, so the session must actually retain the FIN). */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cx; memset(&cx, 0, sizeof(cx));
        cx.max_events = 1;     /* one queued event -> obj1 pends after obj0 */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cx, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0)
                       == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        /* Drain the client's SUBSCRIBE_OK so the size-1 queue starts empty. */
        while (moq_session_poll_events(c, &ev, 1) > 0) moq_event_cleanup(&ev);

        /* Server writes two objects to one subgroup (no close: FIN sent
         * separately below). */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 200;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg)
                       == MOQ_OK);
        for (int i = 0; i < 2; i++) {
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)"xy", 2, &p);
            MOQ_TEST_CHECK(moq_session_write_object(sv, sg, (uint64_t)i, p, 0)
                           == MOQ_OK);
            moq_rcbuf_decref(p);
        }

        /* Feed the two objects' SEND_DATA to the client under one rx ref.
         * obj0 emits (queue full); obj1 backs up into PENDING_EMIT. */
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }
        MOQ_TEST_CHECK(moq_session_has_transport_stream(c, rx_ref));

        /* FIN-only while pending: must WOULD_BLOCK but retain the FIN. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(c, rx_ref, NULL, 0, true, 0)
                       == MOQ_ERR_WOULD_BLOCK);

        /* Drain obj0 to free a queue slot, then re-drive (empty) as the bridge
         * would: obj1 emits and the retained FIN finishes/frees the stream. */
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);
        for (int iter = 0; iter < 8 &&
             moq_session_has_transport_stream(c, rx_ref); iter++) {
            moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
            while (moq_session_poll_events(c, &ev, 1) > 0) moq_event_cleanup(&ev);
        }

        /* The stream must be gone: the FIN was retained and completed. */
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(c, rx_ref));
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* A delivered object payload is now assembled into an rcbuf allocated by
     * the receive path (moq_rcbuf_alloc_uninit). That rcbuf must remain valid
     * after the session is destroyed, as long as the allocator ctx stays
     * valid: the app can hold an OBJECT_RECEIVED payload past session teardown
     * and clean it up later. Prove the payload survives destroy, its bytes are
     * intact, and the deferred cleanup frees with no allocator imbalance. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 0, &sub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, server_sub, &acc, 0) == MOQ_OK);
        pump_actions_to_peer(sv, c, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, server_sub, &sg_cfg, 0, &sg) == MOQ_OK);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"persistent", 10, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);

        moq_action_t acts[16];
        size_t na;
        moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(909);
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0) {
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                    FEED_SEND_DATA(c, rx_ref, acts[i], 0);
                moq_action_cleanup(&acts[i]);
            }
        }

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_rcbuf_t *held = ev.u.object_received.payload;
        MOQ_TEST_CHECK(held != NULL);
        /* Retain the payload; the event itself is cleaned up now. */
        moq_rcbuf_incref(held);
        moq_event_cleanup(&ev);

        /* Destroy both sessions while still holding the payload. The allocator
         * ctx (as) outlives them. */
        moq_session_destroy(c);
        moq_session_destroy(sv);

        /* Payload bytes remain intact and the buffer is still usable. */
        MOQ_TEST_CHECK(moq_rcbuf_len(held) == 10);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(held), "persistent", 10) == 0);

        /* Deferred cleanup after destroy frees via the copied allocator. */
        moq_rcbuf_decref(held);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_receive");
    return failures;
}
