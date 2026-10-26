/* The last Object a subgroup stream carried, on its terminal events: a FIN
 * reports the last Object it carried, a reset the last one it carried
 * completely, and a stream that carried none reports no Object. An Object
 * counts as carried once its last byte left as an event (or was consumed
 * for a suppressed Object), never because its header parsed: one whose
 * payload could not be allocated never completed. */
#include "test_session_support.h"

static void setup_with(int *out_failures, moq_alloc_t *alloc,
                       const moq_session_cfg_t *c_extra, moq_session_t **c_out,
                       moq_session_t **sv_out, moq_subscription_t *server_sub_out)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(alloc, 10, 10, &c, &sv, c_extra, NULL);
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

static void setup(int *out_failures, moq_alloc_t *alloc, moq_session_t **c_out,
                  moq_session_t **sv_out, moq_subscription_t *server_sub_out)
{
    setup_with(out_failures, alloc, NULL, c_out, sv_out, server_sub_out);
}

/* An allocator that refuses the first request of exactly `fail_size`. */
typedef struct {
    test_alloc_state_t memory;
    size_t last_size, fail_size, failed;
} oom_state_t;

static void *oom_alloc(size_t n, void *ctx)
{
    oom_state_t *st = ctx;
    st->last_size = n;
    if (st->fail_size && n == st->fail_size) {
        st->failed++;
        st->fail_size = 0;
        return NULL;
    }
    return test_alloc(n, &st->memory);
}

static void *oom_realloc(void *p, size_t old_n, size_t n, void *ctx)
{
    oom_state_t *st = ctx;
    return test_realloc(p, old_n, n, &st->memory);
}

static void oom_free(void *p, size_t n, void *ctx)
{
    oom_state_t *st = ctx;
    test_free(p, n, &st->memory);
}

/* Feed every SEND_DATA the publisher queued. With `hold_last_payload`, the
 * payload bytes of the final data action are withheld, so the receiver has
 * parsed that Object's header but not its payload. */
static void feed(moq_session_t *from, moq_session_t *to, moq_stream_ref_t ref,
                 bool hold_last_payload)
{
    moq_action_t a[32];
    size_t n = 0, got;
    while (n < 32 && (got = moq_session_poll_actions(from, a + n, 32 - n)) > 0)
        n += got;
    size_t last = n;
    for (size_t i = 0; i < n; i++)
        if (a[i].kind == MOQ_ACTION_SEND_DATA) last = i;
    for (size_t i = 0; i < n; i++) {
        if (a[i].kind == MOQ_ACTION_SEND_DATA) {
            const moq_action_t *d = &a[i];
            bool hp = d->u.send_data.payload != NULL;
            bool fin = d->u.send_data.fin;
            bool hold = hold_last_payload && i == last && hp;
            if (d->u.send_data.header_len > 0)
                moq_session_on_data_bytes(to, ref, d->u.send_data.header,
                    d->u.send_data.header_len, fin && !hp, 0);
            if (hp && !hold)
                moq_session_on_data_bytes(to, ref,
                    moq_rcbuf_data(d->u.send_data.payload),
                    moq_rcbuf_len(d->u.send_data.payload), fin, 0);
            if (!hp && d->u.send_data.header_len == 0 && fin)
                moq_session_on_data_bytes(to, ref, NULL, 0, true, 0);
        }
        moq_action_cleanup(&a[i]);
    }
}

static void write_obj(moq_alloc_t *alloc, moq_session_t *sv,
                      moq_subgroup_handle_t sg, uint64_t id)
{
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(alloc, (const uint8_t *)"abcd", 4, &p);
    (void)moq_session_write_object(sv, sg, id, p, 0);
    moq_rcbuf_decref(p);
}

static void drain_publisher(moq_session_t *sv)
{
    moq_action_t a[8];
    size_t n;
    while ((n = moq_session_poll_actions(sv, a, 8)) > 0)
        for (size_t i = 0; i < n; i++) moq_action_cleanup(&a[i]);
}

/* Poll events until a subgroup terminal; count the Objects before it. */
static bool next_terminal(moq_session_t *c, moq_event_t *ev, int *objects)
{
    *objects = 0;
    while (moq_session_poll_events(c, ev, 1) == 1) {
        if (ev->kind == MOQ_EVENT_SUBGROUP_FINISHED ||
            ev->kind == MOQ_EVENT_SUBGROUP_RESET)
            return true;
        if (ev->kind == MOQ_EVENT_OBJECT_RECEIVED) (*objects)++;
        moq_event_cleanup(ev);
    }
    return false;
}

int main(void)
{
    int failures = 0;

    /* FIN after Objects 0, 1 and 3 of an END_OF_GROUP subgroup: the last
     * Object is 3, and the detail covers it. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 5;
        sc.end_of_group = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 0);
        write_obj(&alloc, sv, sg, 1);
        write_obj(&alloc, sv, sg, 3);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);
        feed(sv, c, moq_stream_ref_from_u64(0xB1), false);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == 3);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.end_of_group);
        MOQ_TEST_CHECK(ev.detail_size >=
            offsetof(moq_subgroup_finished_event_t, last_object_id) + sizeof(uint64_t));
        MOQ_TEST_CHECK(ev.u.subgroup_finished.has_last_object);
        MOQ_TEST_CHECK(ev.u.subgroup_finished.last_object_id == 3);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* An empty subgroup that FINs carried no Object. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 6;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, 0) == MOQ_OK);
        feed(sv, c, moq_stream_ref_from_u64(0xB2), false);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == 0);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        MOQ_TEST_CHECK(!ev.u.subgroup_finished.has_last_object);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* A reset between Objects: Objects 0 and 2 completed, so the last is 2. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 7;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 0);
        write_obj(&alloc, sv, sg, 2);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB3);
        feed(sv, c, ref, false);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0x5, 0) == MOQ_OK);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == 2);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_RESET);
        MOQ_TEST_CHECK(ev.detail_size >=
            offsetof(moq_subgroup_reset_event_t, last_object_id) + sizeof(uint64_t));
        MOQ_TEST_CHECK(ev.u.subgroup_reset.error_code == 0x5);
        MOQ_TEST_CHECK(ev.u.subgroup_reset.has_last_object);
        MOQ_TEST_CHECK(ev.u.subgroup_reset.last_object_id == 2);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* A reset that cuts Object 4's payload short: only Object 1 completed. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 8;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 1);
        write_obj(&alloc, sv, sg, 4);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB4);
        feed(sv, c, ref, true);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0x6, 0) == MOQ_OK);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_RESET);
        MOQ_TEST_CHECK(ev.u.subgroup_reset.has_last_object);
        MOQ_TEST_CHECK(ev.u.subgroup_reset.last_object_id == 1);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* A reset that cuts the first Object short: nothing completed. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 9;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB5);
        feed(sv, c, ref, true);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0x7, 0) == MOQ_OK);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == 0);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_RESET);
        MOQ_TEST_CHECK(!ev.u.subgroup_reset.has_last_object);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* The payload of the first Object cannot be allocated after its header
     * parsed, then the stream is reset: no Object completed. After a
     * completed Object 0, the same failure on Object 1 leaves Object 0 the
     * last. */
    for (int predecessor = 0; predecessor < 2; predecessor++) {
        oom_state_t st = {0};
        moq_alloc_t alloc = { &st, oom_alloc, oom_realloc, oom_free };
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup(&failures, &alloc, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 11;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB6);
        if (predecessor) {
            write_obj(&alloc, sv, sg, 0);
            feed(sv, c, ref, false);
        }
        uint8_t big[997];
        memset(big, 0xAB, sizeof(big));
        moq_rcbuf_t *p = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc, big, sizeof(big), &p) == MOQ_OK);
        size_t payload_size = st.last_size;
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 1, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
        st.fail_size = payload_size;
        feed(sv, c, ref, true);
        MOQ_TEST_CHECK(st.failed == 1);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0x8, 0) == MOQ_OK);
        moq_event_t ev;
        int objs;
        MOQ_TEST_CHECK(next_terminal(c, &ev, &objs) && objs == predecessor);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_RESET);
        MOQ_TEST_CHECK(ev.u.subgroup_reset.has_last_object == (predecessor != 0));
        if (predecessor)
            MOQ_TEST_CHECK(ev.u.subgroup_reset.last_object_id == 0);
        moq_event_cleanup(&ev);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(st.memory.balance == 0);
    }

    /* Event pressure: with a one-event queue, Object 1 waits complete behind
     * Object 0's event when the reset comes. The reset emits it first, once
     * there is room, and then reports it as the last. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cx = MOQ_SESSION_CFG_INIT;
        cx.max_events = 1;
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup_with(&failures, &alloc, &cx, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 12;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 0);
        write_obj(&alloc, sv, sg, 1);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB7);
        feed(sv, c, ref, false);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        (void)moq_session_on_data_reset(c, ref, 0x9, 0);
        int objs = 0;
        moq_event_t ev;
        bool got = false;
        for (int i = 0; i < 8 && !got; i++) {
            int n;
            got = next_terminal(c, &ev, &n);
            objs += n;
            if (!got) (void)moq_session_on_data_reset(c, ref, 0x9, 0);
        }
        MOQ_TEST_CHECK(got && objs == 2);
        if (got) {
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBGROUP_RESET);
            MOQ_TEST_CHECK(ev.u.subgroup_reset.has_last_object);
            MOQ_TEST_CHECK(ev.u.subgroup_reset.last_object_id == 1);
            moq_event_cleanup(&ev);
        }
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Streaming: Objects 0 and 1 complete as chunks, then a reset between
     * Objects: the subgroup terminal names Object 1. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cx = MOQ_SESSION_CFG_INIT;
        cx.streaming_objects = true;
        moq_session_t *c, *sv;
        moq_subscription_t ss;
        setup_with(&failures, &alloc, &cx, &c, &sv, &ss);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 13;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ss, &sc, 0, &sg) == MOQ_OK);
        write_obj(&alloc, sv, sg, 0);
        write_obj(&alloc, sv, sg, 1);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB8);
        feed(sv, c, ref, false);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
        drain_publisher(sv);
        MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0xA, 0) == MOQ_OK);
        moq_event_t ev;
        bool got = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
                got = true;
                MOQ_TEST_CHECK(ev.u.subgroup_reset.has_last_object);
                MOQ_TEST_CHECK(ev.u.subgroup_reset.last_object_id == 1);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    return failures ? 1 : 0;
}
