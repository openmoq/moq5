#include <moq/media_sender.h>
#include <moq/auth.h>
#include <moq/sim.h>
#include "test_session_support.h"
#include <string.h>

static int failures;
moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session, uint64_t now);
void moq_media_sender_test_free(moq_media_sender_t *s);

typedef struct auth_state {
    unsigned namespace_calls, publish_calls;
    int deny_action;
    bool deny_catalog;
    uint8_t bytes[5];
} auth_state_t;

static moq_result_t select_auth(void *ctx, const moq_auth_request_t *request,
    moq_auth_token_t *out, size_t capacity, size_t *count)
{
    auth_state_t *state = ctx;
    MOQ_TEST_CHECK(capacity >= 1 && request->ns.count == 2);
    if (request->ns.count == 2)
        MOQ_TEST_CHECK(request->ns.parts[1].len == 3 &&
                       memcmp(request->ns.parts[1].data, "a/b", 3) == 0);
    if (request->action == MOQ_AUTH_PUBLISH_NAMESPACE) {
        state->namespace_calls++;
        MOQ_TEST_CHECK(request->name.len == 0);
    } else {
        MOQ_TEST_CHECK(request->action == MOQ_AUTH_PUBLISH);
        state->publish_calls++;
    }
    if ((int)request->action == state->deny_action ||
        (state->deny_catalog && request->name.len == 7 &&
         memcmp(request->name.data, "catalog", 7) == 0)) return MOQ_ERR_INVAL;
    state->bytes[0] = (uint8_t)request->action;
    state->bytes[1] = 0;
    state->bytes[2] = (uint8_t)request->name.len;
    state->bytes[3] = request->name.len ? request->name.data[0] : 0;
    state->bytes[4] = 0xff;
    out[0] = (moq_auth_token_t){1, {state->bytes, sizeof(state->bytes)}};
    *count = 1;
    return MOQ_OK;
}

static void inspect(const moq_resolved_token_t *tokens, size_t count,
                    unsigned action, moq_bytes_t name, bool use_static)
{
    MOQ_TEST_CHECK(count == 1);
    if (count != 1) return;
    MOQ_TEST_CHECK(tokens[0].token_type == 1);
    uint8_t expected[] = {(uint8_t)action, 0, (uint8_t)name.len,
        name.len ? name.data[0] : 0, 0xff};
    const uint8_t static_expected[] = {0xd2, 0, 0x80};
    const uint8_t *bytes = use_static ? static_expected : expected;
    size_t length = use_static ? sizeof(static_expected) : sizeof(expected);
    MOQ_TEST_CHECK(tokens[0].token_value.len == length);
    if (tokens[0].token_value.len == length)
        MOQ_TEST_CHECK(memcmp(tokens[0].token_value.data, bytes, length) == 0);
}

static void run_case(moq_version_t version, int deny_action, bool use_static,
                     uint32_t max_actions, bool limited_credit, bool deny_catalog)
{
    test_alloc_state_t as = {0}; moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = &alloc; pc.version = version; pc.max_actions = max_actions;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = 32;
    pc.server_initial_request_capacity = limited_credit ? 1 : 32;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&pc, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp); moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    moq_event_t ev;
    while (moq_session_poll_events(cl, &ev, 1)) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1)) moq_event_cleanup(&ev);
    auth_state_t state = {.deny_action = deny_action, .deny_catalog = deny_catalog};
    moq_auth_source_t source; moq_auth_source_init_sized(&source, sizeof(source));
    uint8_t static_bytes[] = {0xd2, 0, 0x80};
    moq_auth_token_t token = {1, {static_bytes, sizeof(static_bytes)}};
    if (use_static) { source.tokens = &token; source.token_count = 1; }
    else { source.select = select_auth; source.ctx = &state; }
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("auth"), MOQ_BYTES_LITERAL("a/b")};
    cfg.namespace_ = (moq_namespace_t){parts, 2};
    cfg.publish_tracks = true; cfg.request_auth = &source;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) { moq_simpair_destroy(sp); return; }
    memset(static_bytes, 0xaa, sizeof(static_bytes));
    moq_media_track_cfg_t tc; moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("video"); tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.is_live = true; tc.bitrate = 100000; tc.emit_sap_timeline = true;
    moq_media_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &track) == MOQ_OK);
    moq_media_track_t *primary_track = track;
    MOQ_TEST_CHECK(!moq_media_sender_track_is_published(s, primary_track));
    unsigned namespaces = 0, publications = 0, fetch_objects = 0;
    bool subscribe_sent = false, fetch_sent = false;
    moq_subscription_t catalog_sub = {0};
    for (int cycle = 0; cycle < 20; ++cycle) {
        if (cycle == 10 && deny_action < 0 && !deny_catalog) {
            tc.name = MOQ_BYTES_LITERAL("late");
            tc.emit_sap_timeline = false;
            MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &track) == MOQ_OK);
        }
        if (cycle == 8 && limited_credit) {
            MOQ_TEST_CHECK(publications == 0);
            MOQ_TEST_CHECK(fetch_objects == 1);
            MOQ_TEST_CHECK(moq_session_grant_request_capacity(sv, 32,
                moq_simpair_now_us(sp)) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }
        moq_media_sender_test_pump(s, cl, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        while (moq_session_poll_events(sv, &ev, 1)) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                namespaces++;
                inspect(ev.u.namespace_published.tokens, ev.u.namespace_published.token_count,
                        MOQ_AUTH_PUBLISH_NAMESPACE, (moq_bytes_t){0}, use_static);
                moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
                MOQ_TEST_CHECK(moq_session_accept_namespace(sv, ev.u.namespace_published.ann,
                        &ac, moq_simpair_now_us(sp)) == MOQ_OK);
            } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                publications++;
                inspect(ev.u.publish_request.tokens, ev.u.publish_request.token_count,
                        MOQ_AUTH_PUBLISH, ev.u.publish_request.track_name, use_static);
                moq_accept_publish_cfg_t accept;
                moq_accept_publish_cfg_init_sized(&accept, sizeof(accept));
                /* Accepted even with Forward off: acknowledgement is distinct
                 * from subscriber demand or permission to emit objects. */
                accept.forward = false;
                MOQ_TEST_CHECK(moq_session_accept_publish(sv, ev.u.publish_request.pub,
                    &accept, moq_simpair_now_us(sp)) == MOQ_OK);
            }
            else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && limited_credit && !fetch_sent) {
                moq_fetch_cfg_t fc; moq_fetch_cfg_init_sized(&fc, sizeof(fc));
                fc.is_joining = true; fc.joining_relative = true;
                fc.joining_start = 0; fc.joining_sub = catalog_sub;
                moq_fetch_t fh;
                MOQ_TEST_CHECK(moq_session_fetch(sv, &fc, moq_simpair_now_us(sp), &fh) == MOQ_OK);
                fetch_sent = true;
            } else if (ev.kind == MOQ_EVENT_FETCH_ERROR) {
                MOQ_TEST_CHECK(false);
            } else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                fetch_objects++;
            }
            moq_event_cleanup(&ev);
        }
        if (limited_credit && namespaces && !subscribe_sent) {
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            sc.track_namespace = cfg.namespace_;
            sc.track_name = MOQ_BYTES_LITERAL("catalog");
            sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            MOQ_TEST_CHECK(moq_session_subscribe(sv, &sc, moq_simpair_now_us(sp), &catalog_sub) == MOQ_OK);
            subscribe_sent = true;
        }
    }
    if (deny_action >= 0 || deny_catalog) {
        MOQ_TEST_CHECK(moq_media_sender_is_fatal(s));
        MOQ_TEST_CHECK(moq_media_sender_fatal_code(s) == MOQ_MEDIA_SENDER_FATAL_AUTHORIZATION);
        MOQ_TEST_CHECK(publications == (deny_catalog ? 2u : 0u));
        if (deny_action == MOQ_AUTH_PUBLISH_NAMESPACE) MOQ_TEST_CHECK(namespaces == 0);
    } else {
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        MOQ_TEST_CHECK(moq_media_sender_track_is_published(s, primary_track));
        MOQ_TEST_CHECK(namespaces == 1 && publications == 4);
        if (!use_static) {
            MOQ_TEST_CHECK(state.namespace_calls == 1);
            MOQ_TEST_CHECK(state.publish_calls == publications);
        }
    }
    moq_media_sender_test_free(s);
    while (moq_session_poll_events(cl, &ev, 1)) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1)) moq_event_cleanup(&ev);
    moq_simpair_destroy(sp); MOQ_TEST_CHECK(as.balance == 0);
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t i = 0; i < 2; ++i) {
        run_case(versions[i], -1, false, 0, false, false);
        run_case(versions[i], -1, true, 0, false, false);
        run_case(versions[i], MOQ_AUTH_PUBLISH_NAMESPACE, false, 0, false, false);
        run_case(versions[i], MOQ_AUTH_PUBLISH, false, 0, false, false);
        run_case(versions[i], -1, false, 2, false, false);
        /* Draft 18 uses stream admission, not draft-16 request capacity. */
        if (versions[i] == MOQ_VERSION_DRAFT_16)
            run_case(versions[i], -1, false, 0, true, false);
        run_case(versions[i], -1, false, 0, false, true);
    }
    MOQ_TEST_PASS("media_sender_auth");
    return failures ? 1 : 0;
}
