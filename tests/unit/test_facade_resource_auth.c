#include <moq/publisher.h>
#include <moq/subscriber.h>
#include <moq/sim.h>
#include "test_alloc_support.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <string.h>

#define REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr); \
        exit(1); \
    } \
} while (0)

typedef struct fixture {
    test_alloc_state_t allocations;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *client;
    moq_session_t *server;
    moq_subscriber_t *sub;
    moq_sub_track_t *track;
    moq_publisher_t *pub;
} fixture_t;

static const uint8_t expected_bytes[] = {0x80, 0, 0xff, 0x42};
static const moq_bytes_t ns_parts[] = {{(const uint8_t *)"resource", 8}};

static void pump(fixture_t *f)
{
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
}

static void no_actions(moq_session_t *s)
{
    moq_action_t action;
    REQUIRE(moq_session_poll_actions(s, &action, 1) == 0);
}

static void no_events(moq_session_t *s)
{
    moq_event_t event;
    REQUIRE(moq_session_poll_events(s, &event, 1) == 0);
}

static void setup(fixture_t *f, moq_version_t version)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->allocations);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = version;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    REQUIRE(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    pump(f);
    f->client = moq_simpair_client(f->pair);
    f->server = moq_simpair_server(f->pair);
    REQUIRE(moq_session_state(f->client) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(f->server) == MOQ_SESS_ESTABLISHED);
    drain(f->client);
    drain(f->server);
}

static void teardown(fixture_t *f)
{
    if (f->sub) moq_sub_destroy(f->sub);
    if (f->pub) moq_pub_destroy(f->pub);
    drain(f->client);
    drain(f->server);
    moq_simpair_destroy(f->pair);
    REQUIRE(f->allocations.balance == 0);
}

static void subscribe(fixture_t *f)
{
    moq_sub_cfg_t cfg;
    moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.max_fills = 1;
    REQUIRE(moq_sub_create(f->client, &f->alloc, &cfg, &f->sub) == MOQ_OK);
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init(&tc);
    tc.track_namespace = (moq_namespace_t){ns_parts, 1};
    tc.track_name = MOQ_BYTES_LITERAL("track");
    REQUIRE(moq_sub_subscribe(f->sub, &tc, moq_simpair_now_us(f->pair),
                              &f->track) == MOQ_OK);
    pump(f);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(f->server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    no_events(f->server);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 10;
    ac.largest_object = 5;
    REQUIRE(moq_session_accept_subscribe(f->server, sub, &ac,
                                          moq_simpair_now_us(f->pair)) == MOQ_OK);
    pump(f);
    REQUIRE(moq_sub_tick(f->sub, moq_simpair_now_us(f->pair)) == MOQ_OK);
    no_events(f->server);
    no_actions(f->client);
}

static void check_tokens(const moq_resolved_token_t *tokens, size_t count,
                         bool authenticated)
{
    if (!authenticated) {
        REQUIRE(count == 0);
        return;
    }
    REQUIRE(count == 2 && tokens != NULL);
    REQUIRE(tokens[0].token_type == 7);
    REQUIRE(tokens[0].token_value.len == sizeof(expected_bytes));
    REQUIRE(tokens[0].token_value.data != NULL);
    REQUIRE(memcmp(tokens[0].token_value.data, expected_bytes,
                   sizeof(expected_bytes)) == 0);
    REQUIRE(tokens[1].token_type == 8);
    REQUIRE(tokens[1].token_value.len == 0);
}

static void tokens_init(moq_auth_token_t tokens[2], uint8_t bytes[4])
{
    memcpy(bytes, expected_bytes, sizeof(expected_bytes));
    tokens[0] = (moq_auth_token_t){7, {bytes, sizeof(expected_bytes)}};
    tokens[1] = (moq_auth_token_t){8, {NULL, 0}};
}

static void namespace_row(moq_version_t version, bool authenticated)
{
    fixture_t f;
    setup(&f, version);
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    REQUIRE(moq_pub_create(f.client, &f.alloc, &pc, &f.pub) == MOQ_OK);
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){ns_parts, 1};
    tc.track_name = MOQ_BYTES_LITERAL("one");
    tc.advertise_namespace = true;
    moq_auth_token_t tokens[2];
    uint8_t bytes[4];
    tokens_init(tokens, bytes);
    tc.namespace_auth_tokens = tokens;
    tc.namespace_auth_token_count = 2;
    size_t start = offsetof(moq_pub_track_cfg_t, namespace_auth_tokens);
    size_t end = offsetof(moq_pub_track_cfg_t, namespace_auth_token_count) + sizeof(size_t);
    moq_pub_track_t *track = NULL;
    for (size_t n = start + 1; n < end; ++n) {
        tc.struct_size = (uint32_t)n;
        REQUIRE(moq_pub_add_track(f.pub, &tc, moq_simpair_now_us(f.pair),
                                 &track) == MOQ_ERR_INVAL);
        REQUIRE(track == NULL);
        no_actions(f.client);
    }
    tc.struct_size = sizeof(tc);
    tc.namespace_auth_tokens = NULL;
    REQUIRE(moq_pub_add_track(f.pub, &tc, moq_simpair_now_us(f.pair),
                             &track) == MOQ_ERR_INVAL);
    REQUIRE(track == NULL);
    no_actions(f.client);
    tc.namespace_auth_tokens = tokens;
    if (!authenticated) tc.struct_size = (uint32_t)start;
    REQUIRE(moq_pub_add_track(f.pub, &tc, moq_simpair_now_us(f.pair), &track) == MOQ_OK);
    REQUIRE(track != NULL);
    memset(bytes, 0x11, sizeof(bytes));
    memset(tokens, 0, sizeof(tokens));
    pump(&f);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(f.server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
    REQUIRE(ev.u.namespace_published.track_namespace.count == 1);
    check_tokens(ev.u.namespace_published.tokens, ev.u.namespace_published.token_count,
                 authenticated);
    moq_event_cleanup(&ev);
    no_events(f.server);

    /* Reusing a pending shared namespace neither replaces credentials nor
     * reads the now-invalid unused pair. There is no second advertisement. */
    tc.struct_size = sizeof(tc);
    tc.track_name = MOQ_BYTES_LITERAL("two");
    tc.namespace_auth_tokens = NULL;
    tc.namespace_auth_token_count = 1;
    moq_pub_track_t *second = NULL;
    REQUIRE(moq_pub_add_track(f.pub, &tc, moq_simpair_now_us(f.pair), &second) == MOQ_OK);
    REQUIRE(second != NULL && second != track);
    no_actions(f.client);
    pump(&f);
    no_events(f.server);
    teardown(&f);
}

static void update_row(moq_version_t version, bool authenticated)
{
    fixture_t f;
    setup(&f, version);
    subscribe(&f);
    moq_sub_update_cfg_t uc;
    moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 19;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    location.end_group = 1;
    location.end_object = 1;
    if (version == MOQ_VERSION_DRAFT_22) {
        uc.fill = &fill;
        uc.location = &location;
        uc.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT |
                                    MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
        uc.object_delivery_timeout_ms = UINT64_MAX;
        uc.subgroup_delivery_timeout_ms = 0;
    }
    moq_auth_token_t tokens[2];
    uint8_t bytes[4];
    tokens_init(tokens, bytes);
    uc.auth_tokens = tokens;
    uc.auth_token_count = 2;
    size_t start = offsetof(moq_sub_update_cfg_t, auth_tokens);
    size_t end = offsetof(moq_sub_update_cfg_t, auth_token_count) + sizeof(size_t);
    for (size_t n = start + 1; n < end; ++n) {
        uc.struct_size = (uint32_t)n;
        REQUIRE(moq_sub_update_subscription(f.sub, f.track, &uc,
                   moq_simpair_now_us(f.pair)) == MOQ_ERR_INVAL);
        no_actions(f.client);
    }
    uc.struct_size = sizeof(uc);
    uc.auth_tokens = NULL;
    /* Native failure must release the facade's sole reserved fill slot. */
    for (size_t i = 0; i < 3; i++) {
        REQUIRE(moq_sub_update_subscription(f.sub, f.track, &uc,
                   moq_simpair_now_us(f.pair)) == MOQ_ERR_INVAL);
        REQUIRE(f.client->fills.live == 0);
        no_actions(f.client);
    }
    uc.auth_tokens = tokens;
    if (!authenticated) uc.struct_size = (uint32_t)start;
    REQUIRE(moq_session_tick(f.client, moq_simpair_now_us(f.pair)) == MOQ_OK);
    size_t send_cap = f.client->send_cap;
    f.client->send_cap = 256;
    uint8_t filler[255];
    memset(filler, 0x5a, sizeof(filler));
    REQUIRE(queue_send_control(f.client, filler, sizeof(filler)) == MOQ_OK);
    size_t event_tail = f.client->event_tail;
    for (size_t i = 0; i < 2; i++) {
        REQUIRE(moq_sub_update_subscription(f.sub, f.track, &uc,
                   moq_simpair_now_us(f.pair)) == MOQ_ERR_WOULD_BLOCK);
        REQUIRE(f.client->fills.live == 0 && f.client->event_tail == event_tail);
        REQUIRE(f.client->action_tail - f.client->action_head == 1);
    }
    moq_action_t action;
    REQUIRE(moq_session_poll_actions(f.client, &action, 1) == 1);
    REQUIRE(action.kind == MOQ_ACTION_SEND_CONTROL);
    REQUIRE(action.u.send_control.len == sizeof(filler));
    REQUIRE(memcmp(action.u.send_control.data, filler, sizeof(filler)) == 0);
    moq_action_cleanup(&action);
    no_actions(f.client);
    f.client->send_cap = send_cap;
    size_t fill_live = f.client->fills.live;
    REQUIRE(moq_sub_update_subscription(f.sub, f.track, &uc,
               moq_simpair_now_us(f.pair)) == MOQ_OK);
    REQUIRE(f.client->fills.live == fill_live + (version == MOQ_VERSION_DRAFT_22));
    memset(bytes, 0x11, sizeof(bytes));
    memset(tokens, 0, sizeof(tokens));
    pump(&f);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(f.server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    REQUIRE(ev.u.subscribe_updated.has_subscriber_priority);
    REQUIRE(ev.u.subscribe_updated.subscriber_priority == 19);
    if (version == MOQ_VERSION_DRAFT_22) {
        const moq_subscription_update_options_t *o = ev.u.subscribe_updated.options;
        REQUIRE(o != NULL && ev.u.subscribe_updated.fill_requested);
        REQUIRE(o->location.kind == MOQ_LOCATION_FILTER_OBJECT_END);
        REQUIRE(o->location.end_group == 1 && o->location.end_object == 1);
        REQUIRE(o->object_delivery_timeout_ms == UINT64_MAX);
        REQUIRE(o->subgroup_delivery_timeout_ms == 0);
    }
    check_tokens(ev.u.subscribe_updated.tokens, ev.u.subscribe_updated.token_count,
                 authenticated);
    moq_event_cleanup(&ev);
    if (version == MOQ_VERSION_DRAFT_22) {
        REQUIRE(moq_session_poll_events(f.server, &ev, 1) == 1);
        REQUIRE(ev.kind == MOQ_EVENT_FILL_REQUEST);
        moq_event_cleanup(&ev);
    }
    no_events(f.server);
    teardown(&f);
}

static void joining_row(moq_version_t version, bool authenticated)
{
    fixture_t f;
    setup(&f, version);
    subscribe(&f);
    moq_sub_joining_fetch_cfg_t jc;
    moq_sub_joining_fetch_cfg_init_sized(&jc, sizeof(jc));
    jc.track = f.track;
    jc.relative = true;
    jc.joining_start = 3;
    moq_auth_token_t tokens[2];
    uint8_t bytes[4];
    tokens_init(tokens, bytes);
    jc.auth_tokens = tokens;
    jc.auth_token_count = 2;
    size_t start = MOQ_SUB_JOINING_FETCH_CFG_V0_SIZE;
    size_t end = offsetof(moq_sub_joining_fetch_cfg_t, auth_token_count) + sizeof(size_t);
    moq_sub_fetch_req_t *req = NULL;
    for (size_t n = start + 1; n < end; ++n) {
        jc.struct_size = (uint32_t)n;
        REQUIRE(moq_sub_joining_fetch(f.sub, &jc, moq_simpair_now_us(f.pair),
                                      &req) == MOQ_ERR_INVAL);
        REQUIRE(req == NULL);
        no_actions(f.client);
    }
    jc.struct_size = sizeof(jc);
    jc.auth_tokens = NULL;
    REQUIRE(moq_sub_joining_fetch(f.sub, &jc, moq_simpair_now_us(f.pair),
                                  &req) == MOQ_ERR_INVAL);
    REQUIRE(req == NULL);
    no_actions(f.client);
    jc.auth_tokens = tokens;
    if (!authenticated) jc.struct_size = (uint32_t)start;
    if (version == MOQ_VERSION_DRAFT_22) {
        REQUIRE(moq_sub_joining_fetch(f.sub, &jc, moq_simpair_now_us(f.pair),
                                      &req) == MOQ_ERR_UNSUPPORTED);
        REQUIRE(req == NULL);
        no_actions(f.client);
        teardown(&f);
        return;
    }
    REQUIRE(moq_sub_joining_fetch(f.sub, &jc, moq_simpair_now_us(f.pair), &req) == MOQ_OK);
    REQUIRE(req != NULL);
    memset(bytes, 0x11, sizeof(bytes));
    memset(tokens, 0, sizeof(tokens));
    pump(&f);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(f.server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    REQUIRE(ev.u.fetch_request.start_group == 7);
    check_tokens(ev.u.fetch_request.tokens, ev.u.fetch_request.token_count, authenticated);
    moq_event_cleanup(&ev);
    no_events(f.server);
    teardown(&f);
}

static void combined_subscribe_row(bool authenticated)
{
    fixture_t f;
    setup(&f, MOQ_VERSION_DRAFT_22);
    moq_sub_cfg_t cfg;
    moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.max_tracks = 1;
    cfg.max_fills = 1;
    REQUIRE(moq_sub_create(f.client, &f.alloc, &cfg, &f.sub) == MOQ_OK);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init(&tc);
    tc.track_namespace = (moq_namespace_t){ns_parts, 1};
    tc.track_name = MOQ_BYTES_LITERAL("combined");
    tc.fill = &fill;
    moq_auth_token_t tokens[2];
    uint8_t bytes[4];
    tokens_init(tokens, bytes);
    tc.auth_token_count = 2;
    tc.auth_tokens = NULL;
    for (size_t i = 0; i < 3; i++) {
        REQUIRE(moq_sub_subscribe(f.sub, &tc, moq_simpair_now_us(f.pair),
                                  &f.track) == MOQ_ERR_INVAL);
        REQUIRE(f.track == NULL && f.client->fills.live == 0);
        no_actions(f.client);
    }
    tc.auth_tokens = authenticated ? tokens : NULL;
    tc.auth_token_count = authenticated ? 2 : 0;
    REQUIRE(moq_sub_subscribe(f.sub, &tc, moq_simpair_now_us(f.pair),
                              &f.track) == MOQ_OK);
    REQUIRE(f.track != NULL && f.client->fills.live == 1);
    REQUIRE(f.client->action_tail - f.client->action_head == 1);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(f.client, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_FILL_CREATED);
    moq_event_cleanup(&ev);
    no_events(f.client);
    memset(bytes, 0x11, sizeof(bytes));
    memset(tokens, 0, sizeof(tokens));
    pump(&f);
    REQUIRE(moq_session_poll_events(f.server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    REQUIRE(ev.u.subscribe_request.options != NULL);
    REQUIRE(ev.u.subscribe_request.options->flags & MOQ_SUBSCRIPTION_ADMISSION_FILL);
    check_tokens(ev.u.subscribe_request.tokens, ev.u.subscribe_request.token_count,
                 authenticated);
    moq_event_cleanup(&ev);
    /* One original SUBSCRIBE carries both auth and fill; no fabricated FETCH. */
    no_events(f.server);
    REQUIRE(f.server->peer_token_cache.used_bytes == 0);
    teardown(&f);
}

static void initializers(void)
{
    /* Layout replicas independently pin the pre-tail allocations. */
    struct old_pub {
        uint32_t struct_size;
        moq_namespace_t track_namespace;
        moq_bytes_t track_name;
        bool advertise_namespace;
        uint8_t publisher_priority;
        uint64_t max_retained_bytes;
        bool has_publisher_priority;
        uint8_t reserved1[7];
        bool monotonic_groups;
        uint8_t reserved2[7];
        moq_bytes_t track_properties;
        bool dynamic_groups;
    };
    _Static_assert(sizeof(struct old_pub) ==
                   offsetof(moq_pub_track_cfg_t, namespace_auth_tokens),
                   "namespace auth must follow the complete D22 allocation");
    _Static_assert(offsetof(struct old_pub, track_properties) ==
                   offsetof(moq_pub_track_cfg_t, track_properties),
                   "track properties offset changed");
    struct old_update {
        uint32_t struct_size;
        bool has_subscriber_priority;
        uint8_t subscriber_priority;
        bool has_forward;
        bool forward;
        bool has_delivery_timeout;
        uint64_t delivery_timeout_us;
        bool has_filter;
        moq_subscribe_filter_t filter;
        uint64_t start_group, start_object, end_group;
        const moq_fill_cfg_t *fill;
        uint32_t reserved_location;
        const moq_location_filter_cfg_t *location;
        _Alignas(uint64_t) uint32_t delivery_timeout_flags;
        uint32_t reserved;
        uint64_t object_delivery_timeout_ms, subgroup_delivery_timeout_ms;
    };
    _Static_assert(sizeof(struct old_update) == offsetof(moq_sub_update_cfg_t, auth_tokens),
                   "update auth must follow the complete D22 allocation");
    _Static_assert(offsetof(struct old_update, location) ==
                   offsetof(moq_sub_update_cfg_t, location), "location offset changed");
    _Static_assert(offsetof(struct old_update, delivery_timeout_flags) ==
                   offsetof(moq_sub_update_cfg_t, delivery_timeout_flags),
                   "typed timeout offset changed");
    struct old_join {
        uint32_t struct_size;
        moq_sub_track_t *track;
        bool relative;
        uint64_t joining_start;
        moq_group_order_t group_order;
        bool has_subscriber_priority;
        uint8_t subscriber_priority;
    };
    _Static_assert(sizeof(struct old_join) == MOQ_SUB_JOINING_FETCH_CFG_V0_SIZE,
                   "joining fetch frozen allocation changed");
    union storage {
        moq_sub_joining_fetch_cfg_t cfg;
        uint8_t bytes[sizeof(moq_sub_joining_fetch_cfg_t) + 8];
    } u;
    memset(&u, 0xa5, sizeof(u));
    moq_sub_joining_fetch_cfg_init(&u.cfg);
    REQUIRE(u.cfg.struct_size == sizeof(struct old_join));
    for (size_t n = sizeof(struct old_join); n < sizeof(u); ++n)
        REQUIRE(u.bytes[n] == 0xa5);
    for (size_t size = 0; size <= sizeof(u); ++size) {
        memset(&u, 0xa5, sizeof(u));
        moq_sub_joining_fetch_cfg_init_sized(&u.cfg, size);
        size_t written = size < sizeof(uint32_t) ? 0 : size;
        if (written > sizeof(u.cfg)) written = sizeof(u.cfg);
        if (written) REQUIRE(u.cfg.struct_size == written);
        for (size_t n = sizeof(uint32_t); n < written; ++n) REQUIRE(u.bytes[n] == 0);
        for (size_t n = written; n < sizeof(u); ++n) REQUIRE(u.bytes[n] == 0xa5);
    }
    moq_sub_joining_fetch_cfg_init(NULL);
    moq_sub_joining_fetch_cfg_init_sized(NULL, sizeof(u));

    moq_pub_track_cfg_t pc;
    memset(&pc, 0xa5, sizeof(pc));
    moq_pub_track_cfg_init(&pc);
    REQUIRE(pc.struct_size == offsetof(moq_pub_track_cfg_t, max_retained_bytes));
    for (size_t n = pc.struct_size; n < sizeof(pc); ++n)
        REQUIRE(((const uint8_t *)&pc)[n] == 0xa5);
    moq_sub_update_cfg_t uc;
    memset(&uc, 0xa5, sizeof(uc));
    moq_sub_update_cfg_init(&uc);
    REQUIRE(uc.struct_size == offsetof(moq_sub_update_cfg_t, delivery_timeout_us) +
                              sizeof(uc.delivery_timeout_us));
    for (size_t n = uc.struct_size; n < sizeof(uc); ++n)
        REQUIRE(((const uint8_t *)&uc)[n] == 0xa5);
}

int main(void)
{
    initializers();
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                    MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        for (unsigned auth = 0; auth < 2; ++auth) {
            namespace_row(versions[v], auth != 0);
            update_row(versions[v], auth != 0);
            joining_row(versions[v], auth != 0);
        }
    }
    combined_subscribe_row(false);
    combined_subscribe_row(true);
    puts("facade resource auth: initializers and 20 operation rows PASS");
    return 0;
}
