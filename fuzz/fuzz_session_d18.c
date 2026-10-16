/*
 * libFuzzer target for the draft-18 sans-I/O session engine (moq_session_t).
 *
 * Invariants under test:
 *   1. Version and capability invariants:
 *      - A session created with MOQ_VERSION_DRAFT_18 always reports
 *        moq_session_version() == MOQ_VERSION_DRAFT_18,
 *        moq_session_supports_zero_field_track_namespace() == true, and
 *        moq_session_supports_fetch_datagram() == true across its lifetime.
 *   2. Inbound D18 request-bidi, control-uni, data-uni, and datagram safety:
 *      - Arbitrary wire bytes fed to moq_session_on_bidi_stream_bytes,
 *        moq_session_on_control_bytes, moq_session_on_data_bytes, and
 *        moq_session_on_datagram never crash, leak, or corrupt state,
 *        whether requests are accepted, updated, or reset.
 *   3. Exact allocator balance:
 *      - Every allocation and byte allocated through moq_alloc_t is freed
 *        by the time moq_session_destroy() returns (balance == 0 and
 *        bytes_alloc == bytes_free).
 */

#include <moq/control_d18.h>
#include <moq/session.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT 4096

typedef struct {
    int64_t balance;
    size_t  bytes_alloc;
    size_t  bytes_free;
} fuzz_alloc_state_t;

static void *fuzz_alloc(size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    void *p = malloc(size > 0 ? size : 1);
    if (p) {
        s->balance++;
        s->bytes_alloc += size;
    }
    return p;
}

static void *fuzz_realloc(void *ptr, size_t old_size, size_t new_size,
                          void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (new_size == 0) {
        if (ptr) {
            free(ptr);
            s->balance--;
            s->bytes_free += old_size;
        }
        return NULL;
    }
    if (!ptr)
        return fuzz_alloc(new_size, ctx);
    void *np = realloc(ptr, new_size);
    if (np) {
        s->bytes_free += old_size;
        s->bytes_alloc += new_size;
    }
    return np;
}

static void fuzz_free(void *ptr, size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (ptr) {
        s->balance--;
        s->bytes_free += size;
    }
    free(ptr);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(s, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++)
            moq_action_cleanup(&acts[i]);
    }
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0)
        moq_event_cleanup(&ev);
}

static bool drain_and_accept_requests(moq_session_t *s, bool *out_accepted_sub)
{
    bool accepted = false;
    if (out_accepted_sub)
        *out_accepted_sub = false;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (moq_session_state(s) == MOQ_SESS_ESTABLISHED) {
            switch (ev.kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                if (moq_session_accept_subscribe(
                        s, ev.u.subscribe_request.sub, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                if (out_accepted_sub)
                    *out_accepted_sub = true;
                break;
            }
            case MOQ_EVENT_PUBLISH_REQUEST: {
                moq_accept_publish_cfg_t acc;
                moq_accept_publish_cfg_init(&acc);
                if (moq_session_accept_publish(
                        s, ev.u.publish_request.pub, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            case MOQ_EVENT_FETCH_REQUEST: {
                moq_accept_fetch_cfg_t acc;
                moq_accept_fetch_cfg_init(&acc);
                acc.end_group = ev.u.fetch_request.end_group;
                acc.end_object = ev.u.fetch_request.end_object;
                if (moq_session_accept_fetch(
                        s, ev.u.fetch_request.fetch, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            case MOQ_EVENT_NAMESPACE_PUBLISHED: {
                moq_accept_namespace_cfg_t acc;
                moq_accept_namespace_cfg_init(&acc);
                if (moq_session_accept_namespace(
                        s, ev.u.namespace_published.ann, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            case MOQ_EVENT_NS_SUB_REQUEST: {
                moq_accept_ns_sub_cfg_t acc;
                moq_accept_ns_sub_cfg_init(&acc);
                if (moq_session_accept_ns_sub(
                        s, ev.u.ns_sub_request.handle, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
                moq_accept_subscribe_tracks_cfg_t acc;
                moq_accept_subscribe_tracks_cfg_init(&acc);
                if (moq_session_accept_subscribe_tracks(
                        s, ev.u.subscribe_tracks_request.handle, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            case MOQ_EVENT_TRACK_STATUS_REQUEST: {
                moq_accept_track_status_cfg_t acc;
                moq_accept_track_status_cfg_init(&acc);
                if (moq_session_accept_track_status(
                        s, ev.u.track_status_request.handle, &acc, 1) != MOQ_OK)
                    __builtin_trap();
                accepted = true;
                break;
            }
            default:
                break;
            }
        }
        moq_event_cleanup(&ev);
        drain_actions(s);
    }
    return accepted;
}

static moq_session_t *make_d18_session(const moq_alloc_t *alloc,
                                       moq_perspective_t persp)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = alloc;
    cfg.perspective = persp;
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_actions = 16;
    cfg.max_events = 8;
    cfg.max_subscriptions = 8;
    cfg.max_data_streams = 8;
    cfg.max_fetches = 4;
    cfg.max_publishes = 4;
    cfg.max_announcements = 4;
    cfg.max_namespace_subscriptions = 4;
    cfg.max_track_subscriptions = 4;
    cfg.max_track_statuses = 4;
    cfg.max_object_payload_size = 4096;
    cfg.max_receive_buffer_bytes = 8192;
    cfg.send_buffer_size = 2048;
    cfg.recv_buffer_size = 2048;
    cfg.output_scratch_size = 4096;
    cfg.send_auth_token_cache_size = true;
    cfg.auth_token_cache_size = 256;

    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK || !s)
        __builtin_trap();

    if (moq_session_version(s) != MOQ_VERSION_DRAFT_18 ||
        !moq_session_supports_zero_field_track_namespace(s) ||
        !moq_session_supports_fetch_datagram(s))
        __builtin_trap();

    if (moq_session_start(s, 0) != MOQ_OK)
        __builtin_trap();
    drain_actions(s);

    uint8_t setup[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_setup_opts_t sopts;
    memset(&sopts, 0, sizeof(sopts));
    sopts.has_max_auth_token_cache_size = true;
    sopts.max_auth_token_cache_size = 256;
    if (moq_d18_encode_setup_opts(&w, &sopts) != MOQ_OK)
        __builtin_trap();
    if (moq_session_on_control_bytes(s, setup,
                                     moq_buf_writer_offset(&w), 0) != MOQ_OK)
        __builtin_trap();
    drain_actions(s);
    drain_events(s);
    if (moq_session_state(s) != MOQ_SESS_ESTABLISHED)
        __builtin_trap();
    return s;
}

static void establish_client_subscription(moq_session_t *client,
                                          moq_subscription_t *out_sub,
                                          moq_stream_ref_t *out_ref)
{
    moq_bytes_t ns_parts[] = { { (const uint8_t *)"fuzz", 4 } };
    moq_namespace_t ns = { ns_parts, 1 };

    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

    moq_subscription_t sub_h;
    moq_stream_ref_t sub_ref = moq_stream_ref_from_u64(0);
    if (moq_session_subscribe(client, &sub_cfg, 1, &sub_h) != MOQ_OK)
        __builtin_trap();

    moq_action_t act;
    if (moq_session_poll_actions(client, &act, 1) == 1) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            sub_ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    if (sub_ref._v == 0)
        __builtin_trap();
    drain_actions(client);

    uint8_t sok_buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, sok_buf, sizeof(sok_buf));
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    if (moq_d18_encode_subscribe_ok(
            &w, 0 /* track_alias */, &mp,
            (moq_bytes_t){ NULL, 0 }) != MOQ_OK)
        __builtin_trap();
    if (moq_session_on_bidi_stream_bytes(
            client, sub_ref, sok_buf,
            moq_buf_writer_offset(&w), false, 1) != MOQ_OK)
        __builtin_trap();
    drain_actions(client);
    drain_events(client);
    if (moq_session_state(client) != MOQ_SESS_ESTABLISHED)
        __builtin_trap();
    if (out_sub) *out_sub = sub_h;
    if (out_ref) *out_ref = sub_ref;
}

static void establish_client_fetch(moq_session_t *client)
{
    moq_bytes_t ns_parts[] = { { (const uint8_t *)"fuzz", 4 } };
    moq_namespace_t ns = { ns_parts, 1 };

    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace = ns;
    fcfg.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
    fcfg.start_group = 0;
    fcfg.start_object = 0;
    fcfg.end_group = 4;
    fcfg.end_object = 0;

    moq_fetch_t fetch_h;
    moq_stream_ref_t fetch_ref = moq_stream_ref_from_u64(0);
    if (moq_session_fetch(client, &fcfg, 1, &fetch_h) != MOQ_OK)
        __builtin_trap();

    moq_action_t act;
    if (moq_session_poll_actions(client, &act, 1) == 1) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            fetch_ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    if (fetch_ref._v == 0)
        __builtin_trap();
    drain_actions(client);

    uint8_t fok_buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, fok_buf, sizeof(fok_buf));
    if (moq_d18_encode_fetch_ok(
            &w, false, (moq_d18_location_t){ 2, 2 },
            (moq_bytes_t){ NULL, 0 }) != MOQ_OK)
        __builtin_trap();
    if (moq_session_on_bidi_stream_bytes(
            client, fetch_ref, fok_buf,
            moq_buf_writer_offset(&w), false, 1) != MOQ_OK)
        __builtin_trap();
    drain_actions(client);
    drain_events(client);
    if (moq_session_state(client) != MOQ_SESS_ESTABLISHED)
        __builtin_trap();
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > FUZZ_MAX_INPUT)
        return 0;

    fuzz_alloc_state_t as = { 0, 0, 0 };
    moq_alloc_t alloc = { &as, fuzz_alloc, fuzz_realloc, fuzz_free };

    /*
     * Route 1: Established D18 server receiving request-bidi bytes on a new
     * request stream (SUBSCRIBE, PUBLISH, FETCH, PUBLISH_NAMESPACE,
     * SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, TRACK_STATUS). When the input
     * starts with a complete envelope followed by trailing bytes, accept the
     * initial request first and then feed the remainder on the established
     * request stream; when a single-envelope request is accepted, follow it
     * with a REQUEST_UPDATE on that established stream.
     */
    {
        moq_session_t *server = make_d18_session(&alloc, MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0xB001);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data, size);
        moq_control_envelope_t env;
        bool has_env = (moq_d18_decode_envelope(&r, &env) == MOQ_OK);
        size_t first_len = size;
        if (has_env &&
            moq_buf_reader_offset(&r) > 0 &&
            moq_buf_reader_offset(&r) < size) {
            first_len = moq_buf_reader_offset(&r);
        }

        (void)moq_session_on_bidi_stream_bytes(
            server, req_ref, data, first_len, false, 1);
        drain_actions(server);
        bool accepted_sub = false;
        bool accepted = drain_and_accept_requests(server, &accepted_sub);

        if (first_len < size &&
            moq_session_state(server) != MOQ_SESS_CLOSED) {
            (void)moq_session_on_bidi_stream_bytes(
                server, req_ref, data + first_len, size - first_len,
                false, 2);
            drain_actions(server);
            (void)drain_and_accept_requests(server, NULL);
        } else if (accepted &&
                   moq_session_state(server) != MOQ_SESS_CLOSED) {
            uint64_t upd_req_id = 2;
            if (has_env && env.payload_len > 0 && env.payload[0] != 0)
                upd_req_id = 0;

            uint8_t upd_buf[32];
            moq_buf_writer_t uw;
            moq_buf_writer_init(&uw, upd_buf, sizeof(upd_buf));
            moq_d18_msg_params_t up;
            memset(&up, 0, sizeof(up));
            up.has_subscriber_priority = true;
            up.subscriber_priority = 64;
            if (moq_d18_encode_request_update(&uw, upd_req_id, &up) != MOQ_OK)
                __builtin_trap();
            moq_result_t urc = moq_session_on_bidi_stream_bytes(
                server, req_ref, upd_buf,
                moq_buf_writer_offset(&uw), false, 2);
            drain_actions(server);
            if (accepted_sub) {
                if (urc != MOQ_OK ||
                    moq_session_state(server) != MOQ_SESS_ESTABLISHED)
                    __builtin_trap();
                bool saw_updated = false;
                moq_event_t ev;
                while (moq_session_poll_events(server, &ev, 1) > 0) {
                    if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED)
                        saw_updated = true;
                    moq_event_cleanup(&ev);
                }
                if (!saw_updated)
                    __builtin_trap();
            } else {
                drain_events(server);
            }
        }

        if (moq_session_state(server) != MOQ_SESS_CLOSED) {
            (void)moq_session_on_bidi_stream_stop(server, req_ref, 0, 3);
            (void)moq_session_on_bidi_stream_reset(server, req_ref, 0, 3);
            drain_actions(server);
            drain_events(server);
        }
        moq_session_destroy(server);
    }

    /*
     * Route 2: Established D18 server with a pre-accepted inbound SUBSCRIBE
     * (request_id = 4, leaving 0 and 2 available for standalone update seeds)
     * receiving arbitrary update/lifecycle bytes on that established bidi.
     */
    {
        moq_session_t *server = make_d18_session(&alloc, MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t est_ref = moq_stream_ref_from_u64(0xB002);
        uint8_t sub_buf[64];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub_buf, sizeof(sub_buf));
        moq_bytes_t ns_parts[] = { { (const uint8_t *)"fuzz", 4 } };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d18_msg_params_t sp;
        memset(&sp, 0, sizeof(sp));
        if (moq_d18_encode_subscribe(
                &sw, 4, &ns,
                (moq_bytes_t){ (const uint8_t *)"track", 5 },
                &sp) != MOQ_OK)
            __builtin_trap();
        if (moq_session_on_bidi_stream_bytes(
                server, est_ref, sub_buf,
                moq_buf_writer_offset(&sw), false, 1) != MOQ_OK)
            __builtin_trap();
        drain_actions(server);
        bool accepted_sub = false;
        if (!drain_and_accept_requests(server, &accepted_sub) ||
            !accepted_sub ||
            moq_session_state(server) != MOQ_SESS_ESTABLISHED)
            __builtin_trap();

        (void)moq_session_on_bidi_stream_bytes(
            server, est_ref, data, size, false, 2);
        drain_actions(server);
        drain_events(server);
        moq_session_destroy(server);
    }

    /*
     * Route 3: Established D18 server receiving unidirectional control-stream
     * bytes (GOAWAY, duplicate SETUP, unknown control messages).
     */
    {
        moq_session_t *server = make_d18_session(&alloc, MOQ_PERSPECTIVE_SERVER);
        (void)moq_session_on_control_bytes(server, data, size, 2);
        drain_actions(server);
        drain_events(server);
        moq_session_destroy(server);
    }

    /*
     * Route 4: Established D18 client with an accepted subscription
     * (track_alias = 0) receiving datagram bytes.
     */
    {
        moq_session_t *client = make_d18_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        establish_client_subscription(client, NULL, NULL);
        (void)moq_session_on_datagram(client, data, size, 2);
        drain_actions(client);
        drain_events(client);
        moq_session_destroy(client);
    }

    /*
     * Route 5: Established D18 client with an accepted subscription
     * (track_alias = 0) and accepted standalone fetch (request_id = 2)
     * receiving unidirectional data-stream bytes (subgroup, fetch, padding).
     */
    {
        moq_session_t *client = make_d18_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        establish_client_subscription(client, NULL, NULL);
        establish_client_fetch(client);
        moq_stream_ref_t dref = moq_stream_ref_from_u64(0xD001);
        (void)moq_session_on_data_bytes(
            client, dref, data, size, true, 2);
        drain_actions(client);
        drain_events(client);
        moq_session_destroy(client);
    }

    /*
     * Route 6: Established D18 client with an established subscription and
     * an in-flight outbound REQUEST_UPDATE receiving response-stream bytes
     * (REQUEST_OK / REQUEST_UPDATE_OK, REQUEST_ERROR, PUBLISH_DONE, GOAWAY).
     */
    {
        moq_session_t *client = make_d18_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        moq_subscription_t sub_h;
        moq_stream_ref_t sub_ref = moq_stream_ref_from_u64(0);
        establish_client_subscription(client, &sub_h, &sub_ref);

        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 64;
        if (moq_session_update_subscription(client, sub_h, &ucfg, 2) != MOQ_OK)
            __builtin_trap();
        drain_actions(client);
        if (moq_session_state(client) != MOQ_SESS_ESTABLISHED)
            __builtin_trap();

        (void)moq_session_on_bidi_stream_bytes(
            client, sub_ref, data, size, false, 3);
        drain_actions(client);
        drain_events(client);
        moq_session_destroy(client);
    }

    /*
     * Route 7: Established D18 client with a pending outbound SUBSCRIBE
     * receiving first-response bytes (SUBSCRIBE_OK, REQUEST_ERROR, REDIRECT,
     * GOAWAY).
     */
    {
        moq_session_t *client = make_d18_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t ns_parts[] = { { (const uint8_t *)"fuzz", 4 } };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

        moq_subscription_t sub_h;
        moq_stream_ref_t sub_ref = moq_stream_ref_from_u64(0);
        if (moq_session_subscribe(client, &sub_cfg, 1, &sub_h) != MOQ_OK)
            __builtin_trap();

        moq_action_t act;
        if (moq_session_poll_actions(client, &act, 1) == 1) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                sub_ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        drain_actions(client);
        if (sub_ref._v == 0 ||
            moq_session_state(client) != MOQ_SESS_ESTABLISHED)
            __builtin_trap();

        (void)moq_session_on_bidi_stream_bytes(
            client, sub_ref, data, size, false, 2);
        drain_actions(client);
        drain_events(client);
        moq_session_destroy(client);
    }

    if (as.balance != 0 || as.bytes_alloc != as.bytes_free)
        __builtin_trap();

    return 0;
}
