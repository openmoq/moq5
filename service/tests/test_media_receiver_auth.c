/* Real session request capture for the managed receiver's auth boundary. */
#include "../../core/src/session/session_internal.h"
#include "test_support.h"
#include <moq/media_receiver.h>
#include <moq/sim.h>
#include <moq/subscriber.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static int failures;
#ifdef MOQ_RECEIVER_WRAP_ALLOC
void *__real_malloc(size_t);
void __real_free(void *);
static int fail_after = -1;
static void *live_allocs[8192];
static size_t live_count;
void *__wrap_malloc(size_t n)
{
    if (fail_after == 0) {
        fail_after = -1;
        return NULL;
    }
    if (fail_after > 0)
        --fail_after;
    void *p = __real_malloc(n);
    if (p && live_count < 8192)
        live_allocs[live_count++] = p;
    return p;
}
void __wrap_free(void *p)
{
    for (size_t i = 0; i < live_count; ++i) {
        if (live_allocs[i] == p) {
            live_allocs[i] = live_allocs[--live_count];
            break;
        }
    }
    __real_free(p);
}
#endif
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *);
void moq_media_receiver_test_pump(moq_media_receiver_t *, moq_session_t *, uint64_t);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *, moq_session_t *,
                                                  uint64_t);
void moq_media_receiver_test_ingest(moq_media_receiver_t *, uint64_t, uint64_t, const char *,
                                    size_t);

/* Independently frozen pre-auth layout, including compiler tail padding. */
typedef struct {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
} old_receiver_cfg_t;
typedef struct {
    uint32_t struct_size;
    moq_sub_track_t *track;
    bool relative;
    uint64_t joining_start;
    moq_group_order_t group_order;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
} old_join_cfg_t;

static void legacy_initializers(void)
{
    union {
        moq_media_receiver_cfg_t align;
        unsigned char b[sizeof(moq_media_receiver_cfg_t) + 16];
    } r;
    void (*init[])(moq_media_receiver_cfg_t *) = {moq_media_receiver_cfg_init,
                                                  moq_media_receiver_cfg_init_live,
                                                  moq_media_receiver_cfg_init_flow_control};
    for (size_t k = 0; k < 3; ++k) {
        memset(&r, 0xa5, sizeof(r));
        init[k]((moq_media_receiver_cfg_t *)r.b);
        MOQ_TEST_CHECK_EQ_SIZE(r.align.struct_size, sizeof(old_receiver_cfg_t));
        for (size_t i = sizeof(old_receiver_cfg_t); i < sizeof(r.b); ++i)
            MOQ_TEST_CHECK(r.b[i] == 0xa5);
    }
    union {
        moq_sub_joining_fetch_cfg_t align;
        unsigned char b[sizeof(moq_sub_joining_fetch_cfg_t) + 16];
    } j;
    memset(&j, 0xa5, sizeof(j));
    moq_sub_joining_fetch_cfg_init((moq_sub_joining_fetch_cfg_t *)j.b);
    MOQ_TEST_CHECK_EQ_SIZE(j.align.struct_size, sizeof(old_join_cfg_t));
    for (size_t i = sizeof(old_join_cfg_t); i < sizeof(j.b); ++i)
        MOQ_TEST_CHECK(j.b[i] == 0xa5);
}

typedef struct {
    int calls[9];
    int deny; /* 1 catalog, 2 fetch, 3 media, 4 update */
    moq_result_t denial;
    unsigned char value[4];
} source_ctx_t;
static moq_result_t select_auth(void *ctx, const moq_auth_request_t *req, moq_auth_token_t *out,
                                size_t capacity, size_t *count)
{
    source_ctx_t *s = ctx;
    MOQ_TEST_CHECK(capacity > 0);
    MOQ_TEST_CHECK(req->ns.count == 2);
    if (req->ns.count == 2) {
        MOQ_TEST_CHECK(req->ns.parts[0].len == 3 && !memcmp(req->ns.parts[0].data, "svc", 3));
        MOQ_TEST_CHECK(req->ns.parts[1].len == 4 && !memcmp(req->ns.parts[1].data, "demo", 4));
    }
    bool cat = req->name.len == 7 && !memcmp(req->name.data, "catalog", 7);
    MOQ_TEST_CHECK(cat || (req->name.len == 5 && !memcmp(req->name.data, "video", 5)));
    s->calls[req->action]++;
    int path = req->action == MOQ_AUTH_FETCH            ? 2
               : req->action == MOQ_AUTH_REQUEST_UPDATE ? 4
               : cat                                    ? 1
                                                        : 3;
    if (s->deny == path) {
        if (s->denial == MOQ_OK) { *count = 0; return MOQ_OK; }
#ifdef MOQ_RECEIVER_WRAP_ALLOC
        if (s->denial == MOQ_ERR_NOMEM)
            fail_after = 0;
        else
#endif
            return s->denial;
    }
    s->value[0] = 0xa0;
    s->value[1] = 0;
    s->value[2] = (unsigned char)req->action;
    s->value[3] = cat ? 1 : 2;
    out[0] = (moq_auth_token_t){.token_type = 16, .token_value = {s->value, 4}};
    *count = 1;
    return MOQ_OK;
}
static void check_tokens(const moq_resolved_token_t *tokens, size_t count, int action, bool catalog,
                         bool anonymous)
{
    MOQ_TEST_CHECK_EQ_SIZE(count, anonymous ? 0 : 1);
    if (count != 1) return;
    MOQ_TEST_CHECK(tokens != NULL);
    if (!tokens) return;
    const unsigned char expected[] = {0xa0, 0, (unsigned char)action, catalog ? 1 : 2};
    MOQ_TEST_CHECK_EQ_U64(tokens[0].token_type, 16);
    MOQ_TEST_CHECK_EQ_SIZE(tokens[0].token_value.len, 4);
    MOQ_TEST_CHECK(tokens[0].token_value.data != NULL);
    if (tokens[0].token_value.len == 4 && tokens[0].token_value.data)
        MOQ_TEST_CHECK(!memcmp(tokens[0].token_value.data, expected, 4));
}
static void request_flow(moq_version_t version, int deny, bool manual, bool anonymous,
                         moq_result_t denial, int block_path)
{
    source_ctx_t source = {.deny = deny, .denial = denial};
    moq_auth_source_t auth;
    moq_auth_source_init_sized(&auth, sizeof(auth));
    auth.select = select_auth;
    auth.ctx = &source;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp)
        return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *client = moq_simpair_client(sp), *server = moq_simpair_server(sp);
    moq_event_t ev;
    while (moq_session_poll_events(client, &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(server, &ev, 1) == 1)
        moq_event_cleanup(&ev);
    moq_media_receiver_cfg_t rcfg = {0};
    rcfg.struct_size = anonymous ? sizeof(old_receiver_cfg_t) : sizeof(rcfg);
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME;
    rcfg.auto_subscribe = !manual;
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo")};
    rcfg.namespace_ = (moq_namespace_t){ns, 2};
    rcfg.request_auth = &auth;
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) {
        moq_simpair_destroy(sp);
        return;
    }
    /* The source descriptor is borrowed only until construction. */
    auth.select = NULL;
    auth.ctx = NULL;
    int catalog = 0, fetch = 0, media = 0, updates = 0, acks = 0;
    bool ingested = false, paused = false, blocked = false;
    moq_media_track_t *track = NULL;
    for (int cycle = 0; cycle < 20; cycle++) {
        uint64_t now = moq_simpair_now_us(sp);
        if (!blocked && (block_path == 1 || (block_path == 2 && catalog) ||
                         (block_path == 3 && ingested) || (block_path >= 4 && paused))) {
            /* Deterministically exhaust action capacity without modifying the
             * request or invoking another selector. Restore before transport IO. */
            size_t cap = client->action_cap;
            client->action_cap = 0;
            for (int retry = 0; retry < 3; ++retry) {
                moq_media_receiver_test_pump(r, client, now);
                memset(source.value, 0xdd, sizeof(source.value));
                if (block_path == 5 && retry == 0) {
                    /* Coalesce the unsent pause away, then request a new pause.
                     * The second request must select anew, with no phantom ack. */
                    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, track, NULL) == MOQ_OK);
                    moq_media_receiver_test_pump(r, client, now);
                    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, track) == MOQ_OK);
                }
            }
            client->action_cap = cap;
            MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
            blocked = true;
        }
        moq_media_receiver_test_pump(r, client, now);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        while (moq_session_poll_events(server, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                bool cat = ev.u.subscribe_request.track_name.len == 7;
                if (cat)
                    catalog++;
                else
                    media++;
                check_tokens(ev.u.subscribe_request.tokens, ev.u.subscribe_request.token_count,
                             MOQ_AUTH_SUBSCRIBE, cat, anonymous);
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                acc.has_largest = true;
                acc.largest_group = 1;
                acc.largest_object = 0;
                MOQ_TEST_CHECK(moq_session_accept_subscribe(server, ev.u.subscribe_request.sub,
                                                            &acc, now) == MOQ_OK);
            } else if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                fetch++;
                check_tokens(ev.u.fetch_request.tokens, ev.u.fetch_request.token_count,
                             MOQ_AUTH_FETCH, true, anonymous);
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updates++;
                check_tokens(ev.u.subscribe_updated.tokens, ev.u.subscribe_updated.token_count,
                             MOQ_AUTH_REQUEST_UPDATE, false, anonymous);
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (fetch && !ingested) {
            const char *json =
                "{\"version\":1,\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":"
                "true,\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";
            moq_media_receiver_test_ingest(r, 1, 0, json, strlen(json));
            ingested = true;
        }
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) {
                track = te.track;
                if (manual)
                    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, track, NULL) == MOQ_OK);
            }
            if (te.kind == MOQ_MEDIA_TRACK_UPDATE_OK)
                acks++;
        }
        if (media && track && !paused && cycle > 5) {
            MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, track) == MOQ_OK);
            paused = true;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(catalog, deny == 1 ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(fetch, deny == 1 || deny == 2 ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(media, deny && deny <= 3 ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(updates, deny ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(acks, deny ? 0 : 1); /* core auto-acks accepted updates only */
    MOQ_TEST_CHECK(moq_media_receiver_is_fatal(r) == (deny != 0));
    if (deny)
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_fatal_code(r),
                              MOQ_MEDIA_RECEIVER_FATAL_AUTH_FAILED);
    if (anonymous)
        MOQ_TEST_CHECK(source.calls[MOQ_AUTH_SUBSCRIBE] == 0);
    else {
        MOQ_TEST_CHECK_EQ_INT(source.calls[MOQ_AUTH_SUBSCRIBE], deny == 1 || deny == 2 ? 1 : 2);
        MOQ_TEST_CHECK_EQ_INT(source.calls[MOQ_AUTH_FETCH], deny == 1 ? 0 : 1);
        MOQ_TEST_CHECK_EQ_INT(source.calls[MOQ_AUTH_REQUEST_UPDATE], deny && deny <= 3 ? 0
                                                                     : block_path == 5 ? 2
                                                                                       : 1);
    }
    if (deny && track) {
        moq_media_track_state_t state;
        MOQ_TEST_CHECK(moq_media_receiver_track_state(r, track, &state) == MOQ_OK);
        MOQ_TEST_CHECK(state == MOQ_MEDIA_TRACK_STATE_ENDED);
    }
    if (block_path)
        MOQ_TEST_CHECK(blocked);
    moq_media_receiver_test_destroy_with_session(r, client, moq_simpair_now_us(sp));
    moq_simpair_destroy(sp);
}

static void sized_initializers(void)
{
    void (*init[])(moq_media_receiver_cfg_t *, size_t) = {
        moq_media_receiver_cfg_init_sized, moq_media_receiver_cfg_init_live_sized,
        moq_media_receiver_cfg_init_flow_control_sized};
    for (size_t k = 0; k < 3; ++k) {
        for (size_t n = 0; n <= sizeof(moq_media_receiver_cfg_t) + 8; ++n) {
            union {
                moq_media_receiver_cfg_t cfg;
                unsigned char b[sizeof(moq_media_receiver_cfg_t) + 8];
            } u;
            memset(&u, 0xa5, sizeof(u));
            init[k](&u.cfg, n);
            size_t written = n < sizeof(uint32_t) ? 0 : n < sizeof(u.cfg) ? n : sizeof(u.cfg);
            if (written)
                MOQ_TEST_CHECK_EQ_SIZE(u.cfg.struct_size, written);
            for (size_t i = written; i < sizeof(u.b); ++i)
                MOQ_TEST_CHECK(u.b[i] == 0xa5);
            if (n >= sizeof(u.cfg)) {
                MOQ_TEST_CHECK(u.cfg.request_auth == NULL);
                MOQ_TEST_CHECK(u.cfg.overflow.policy == (k == 0 ? MOQ_MEDIA_OVERFLOW_UNSET
                                                         : k == 1
                                                             ? MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME
                                                             : MOQ_MEDIA_OVERFLOW_FLOW_CONTROL));
            }
        }
    }
    for (size_t n = 0; n <= sizeof(moq_sub_joining_fetch_cfg_t) + 8; ++n) {
        union {
            moq_sub_joining_fetch_cfg_t cfg;
            unsigned char b[sizeof(moq_sub_joining_fetch_cfg_t) + 8];
        } u;
        memset(&u, 0xa5, sizeof(u));
        moq_sub_joining_fetch_cfg_init_sized(&u.cfg, n);
        size_t written = n < sizeof(uint32_t) ? 0 : n < sizeof(u.cfg) ? n : sizeof(u.cfg);
        if (written)
            MOQ_TEST_CHECK_EQ_SIZE(u.cfg.struct_size, written);
        for (size_t i = written; i < sizeof(u.b); ++i)
            MOQ_TEST_CHECK(u.b[i] == 0xa5);
    }
}

static void static_source_ownership(void)
{
    unsigned char value[] = {0xa0, 0, MOQ_AUTH_SUBSCRIBE, 1};
    moq_auth_token_t token = {16, {value, sizeof(value)}};
    moq_auth_source_t auth;
    moq_auth_source_init_sized(&auth, sizeof(auth));
    auth.tokens = &token;
    auth.token_count = 1;
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo")};
    cfg.namespace_ = (moq_namespace_t){ns, 2};
    cfg.request_auth = &auth;
#ifdef MOQ_RECEIVER_WRAP_ALLOC
    size_t baseline = live_count;
    bool succeeded = false;
    for (int n = 0; n < 32; ++n) {
        fail_after = n;
        moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
        fail_after = -1;
        if (r) {
            succeeded = true;
            moq_media_receiver_test_destroy_with_session(r, NULL, 0);
        }
        MOQ_TEST_CHECK_EQ_SIZE(live_count, baseline);
        if (succeeded)
            break;
    }
    MOQ_TEST_CHECK(succeeded);
#endif
    /* A torn pointer is absent, even when its unseen bytes are poison. */
    for (size_t n = 0; n < sizeof(cfg); ++n) {
        moq_media_receiver_cfg_t prefix = cfg;
        prefix.struct_size = (uint32_t)n;
        prefix.request_auth = (const moq_auth_source_t *)(uintptr_t)1;
        moq_media_receiver_t *old = moq_media_receiver_test_new_cfg(&prefix);
        MOQ_TEST_CHECK((old != NULL) == (n >= sizeof(old_receiver_cfg_t)));
        if (old)
            moq_media_receiver_test_destroy_with_session(old, NULL, 0);
    }
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(r != NULL);
    if (!r)
        return;
    memset(value, 0xdd, sizeof(value));
    memset(&token, 0, sizeof(token));
    memset(&auth, 0, sizeof(auth));
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = moq_alloc_default();
    scfg.server_send_request_capacity = true;
    scfg.server_initial_request_capacity = 16;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    if (!sp) {
        moq_media_receiver_test_destroy_with_session(r, NULL, 0);
        return;
    }
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_media_receiver_test_pump(r, moq_simpair_client(sp), moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    int seen = 0;
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            seen++;
            check_tokens(ev.u.subscribe_request.tokens, ev.u.subscribe_request.token_count,
                         MOQ_AUTH_SUBSCRIBE, true, false);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(seen, 1);
    moq_media_receiver_test_destroy_with_session(r, moq_simpair_client(sp), moq_simpair_now_us(sp));
    moq_simpair_destroy(sp);
}

int main(void)
{
    legacy_initializers();
    sized_initializers();
    static_source_ownership();
    moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t v = 0; v < 2; v++) {
        request_flow(versions[v], 0, false, false, MOQ_ERR_INVAL, 0);
        request_flow(versions[v], 0, true, false, MOQ_ERR_INVAL, 0);
        request_flow(versions[v], 0, false, true, MOQ_ERR_INVAL, 0);
        for (int path = 1; path <= 5; path++)
            request_flow(versions[v], 0, false, false, MOQ_ERR_INVAL, path);
        for (int deny = 1; deny <= 4; deny++) {
            request_flow(versions[v], deny, false, false, MOQ_ERR_INVAL, 0);
            request_flow(versions[v], deny, true, false, MOQ_DONE, 0);
            request_flow(versions[v], deny, false, false, MOQ_OK, 0);
            request_flow(versions[v], deny, false, false, MOQ_ERR_WOULD_BLOCK, 0);
#ifdef MOQ_RECEIVER_WRAP_ALLOC
            request_flow(versions[v], deny, false, false, MOQ_ERR_NOMEM, 0);
#endif
        }
    }
    MOQ_TEST_PASS("media_receiver_auth");
    return failures ? 1 : 0;
}
