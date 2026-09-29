/* Public-API scheduling snapshots. Expected keys come from the caller's
 * priorities, never session internals or a decoded copy of the same action. */
#include <moq/session.h>
#include <moq/sim.h>
#include "test_support.h"
#include <stdlib.h>

static int failures;

/* Fixture failures must stop before invalid handles obscure the first error. */
#define REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "fixture failed: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

typedef struct priority_case {
    uint8_t subscriber;
    uint8_t publisher;
    uint64_t key;
} priority_case_t;

static const priority_case_t cases[] = {
    { 37, 91, UINT64_C(0x1255b) },
    { 0, 255, UINT64_C(0x100ff) },
    { 255, 0, UINT64_C(0x1ff00) },
    { 0, 0, UINT64_C(0x10000) },
    { 255, 255, UINT64_C(0x1ffff) }
};

static moq_namespace_t track_namespace(void)
{
    static const moq_bytes_t parts[] = {
        { .data = (const uint8_t *)"priority", .len = sizeof("priority") - 1 }
    };
    return (moq_namespace_t){ parts, 1 };
}

static void pump(moq_simpair_t *pair)
{
    REQUIRE(moq_simpair_run_until_quiescent(pair, 32, NULL) == MOQ_OK);
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1)
        moq_event_cleanup(&e);
}

static moq_event_t event_of(moq_session_t *s, moq_event_kind_t kind)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == kind) return e;
        moq_event_cleanup(&e);
    }
    fprintf(stderr, "missing event %u\n", (unsigned)kind);
    exit(EXIT_FAILURE);
}

static moq_simpair_t *make_pair(moq_version_t version)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.seed = 1;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_initial_request_capacity = 32;
    moq_simpair_t *pair = NULL;
    REQUIRE(moq_simpair_create(&cfg, &pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(pair) == MOQ_OK);
    pump(pair);
    drain_events(moq_simpair_client(pair));
    drain_events(moq_simpair_server(pair));
    return pair;
}

static moq_subscription_t subscribe(moq_simpair_t *pair, uint8_t priority,
                                     moq_subscription_t *remote)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = track_namespace();
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = priority;
    moq_subscription_t local;
    REQUIRE(moq_session_subscribe(moq_simpair_client(pair), &cfg,
        moq_simpair_now_us(pair), &local) == MOQ_OK);
    pump(pair);
    moq_event_t e = event_of(moq_simpair_server(pair), MOQ_EVENT_SUBSCRIBE_REQUEST);
    *remote = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.has_largest = true;
    accept.largest_group = 4;
    accept.largest_object = 2;
    REQUIRE(moq_session_accept_subscribe(moq_simpair_server(pair), *remote,
        &accept, moq_simpair_now_us(pair)) == MOQ_OK);
    pump(pair);
    e = event_of(moq_simpair_client(pair), MOQ_EVENT_SUBSCRIBE_OK);
    moq_event_cleanup(&e);
    return local;
}

static moq_publication_t publish(moq_simpair_t *pair, uint8_t priority,
                                 moq_publication_t *remote)
{
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init(&cfg);
    cfg.track_namespace = track_namespace();
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.has_forward = true;
    cfg.forward = true;
    moq_publication_t local;
    REQUIRE(moq_session_publish(moq_simpair_server(pair), &cfg,
        moq_simpair_now_us(pair), &local) == MOQ_OK);
    pump(pair);
    moq_event_t e = event_of(moq_simpair_client(pair), MOQ_EVENT_PUBLISH_REQUEST);
    *remote = e.u.publish_request.pub;
    moq_event_cleanup(&e);
    moq_accept_publish_cfg_t accept;
    moq_accept_publish_cfg_init(&accept);
    accept.has_subscriber_priority = true;
    accept.subscriber_priority = priority;
    REQUIRE(moq_session_accept_publish(moq_simpair_client(pair), *remote,
        &accept, moq_simpair_now_us(pair)) == MOQ_OK);
    pump(pair);
    e = event_of(moq_simpair_server(pair), MOQ_EVENT_PUBLISH_OK);
    moq_event_cleanup(&e);
    return local;
}

/* Consume all actions, retaining owned SEND_DATA outputs across advancing calls.
 * No data is delivered: SimPair is used only for handshake/control exchange. */
static size_t take_data(moq_session_t *s, moq_action_t *out, size_t capacity)
{
    size_t count = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        REQUIRE(a.kind != MOQ_ACTION_CLOSE_SESSION);
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            REQUIRE(count < capacity);
            out[count++] = a;
        } else {
            moq_action_cleanup(&a);
        }
    }
    return count;
}

static void check_data(moq_action_t *a, uint64_t key, moq_stream_ref_t stream,
                       bool fin)
{
    MOQ_TEST_CHECK(a->detail_size >=
        offsetof(moq_send_data_action_t, scheduling_priority) +
            sizeof(a->u.send_data.scheduling_priority));
    MOQ_TEST_CHECK_EQ_U64(a->u.send_data.scheduling_priority, key);
    MOQ_TEST_CHECK_EQ_U64(a->u.send_data.stream_ref._v, stream._v);
    MOQ_TEST_CHECK(a->u.send_data.fin == fin);
}

static void test_subgroup(moq_version_t version, bool publication,
                           const priority_case_t *c)
{
    moq_simpair_t *pair = make_pair(version);
    moq_session_t *sender = moq_simpair_server(pair);
    moq_session_t *receiver = moq_simpair_client(pair);
    moq_subscription_t local_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t remote_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t local_pub = MOQ_PUBLICATION_INVALID;
    moq_publication_t remote_pub = MOQ_PUBLICATION_INVALID;
    if (publication) local_pub = publish(pair, c->subscriber, &remote_pub);
    else local_sub = subscribe(pair, c->subscriber, &remote_sub);

    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 5;
    sg_cfg.publisher_priority = c->publisher;
    moq_subgroup_handle_t sg;
    uint64_t now = moq_simpair_now_us(pair);
    REQUIRE((publication
        ? moq_session_open_pub_subgroup(sender, local_pub, &sg_cfg, now, &sg)
        : moq_session_open_subgroup(sender, remote_sub, &sg_cfg, now, &sg)) == MOQ_OK);
    const uint8_t bytes[] = { 1, 2, 3 };
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &payload) == MOQ_OK);
    REQUIRE(moq_session_write_object(sender, sg, 0, payload, now) == MOQ_OK);
    moq_action_t old[2];
    REQUIRE(take_data(sender, old, 2) == 2);
    moq_stream_ref_t stream = old[0].u.send_data.stream_ref;
    MOQ_TEST_CHECK(old[0].u.send_data.header_len > 0);
    MOQ_TEST_CHECK(old[0].u.send_data.payload == NULL);
    MOQ_TEST_CHECK(old[1].u.send_data.payload == payload);

    uint8_t updated = c->subscriber == 0 ? 255 : 0;
    if (publication) {
        moq_publication_update_cfg_t update;
        moq_publication_update_cfg_init(&update);
        update.has_subscriber_priority = true;
        update.subscriber_priority = updated;
        REQUIRE(moq_session_update_publication(receiver, remote_pub, &update, now) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t update;
        moq_subscription_update_cfg_init(&update);
        update.has_subscriber_priority = true;
        update.subscriber_priority = updated;
        REQUIRE(moq_session_update_subscription(receiver, local_sub, &update, now) == MOQ_OK);
    }
    pump(pair);
    moq_event_t e = event_of(sender, publication ? MOQ_EVENT_PUBLISH_UPDATED
                                                : MOQ_EVENT_SUBSCRIBE_UPDATED);
    moq_event_cleanup(&e);
    now = moq_simpair_now_us(pair);
    REQUIRE(moq_session_write_object(sender, sg, 1, payload, now) == MOQ_OK);
    REQUIRE(moq_session_close_subgroup(sender, sg, now) == MOQ_OK);
    /* A stale public handle proves retirement before the queued outputs poll. */
    MOQ_TEST_CHECK(moq_session_close_subgroup(sender, sg, now) == MOQ_ERR_STALE_HANDLE);
    moq_action_t current[2];
    REQUIRE(take_data(sender, current, 2) == 2);
    uint64_t new_key = (updated == 0 ? UINT64_C(0x10000) : UINT64_C(0x1ff00))
                       | c->publisher;
    check_data(&current[0], new_key, stream, false);
    check_data(&current[1], new_key, stream, true);
    MOQ_TEST_CHECK(current[0].u.send_data.payload == payload);
    MOQ_TEST_CHECK(current[1].u.send_data.payload == NULL);
    for (size_t i = 0; i < 2; ++i) {
        check_data(&old[i], c->key, stream, false);
        moq_action_cleanup(&old[i]);
        moq_action_cleanup(&current[i]);
    }
    moq_rcbuf_decref(payload);
    moq_simpair_destroy(pair);
}

static void test_fetch(moq_version_t version, bool joining,
                        const priority_case_t *c)
{
    moq_simpair_t *pair = make_pair(version);
    moq_session_t *sender = moq_simpair_server(pair);
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = track_namespace();
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.end_group = 4;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = c->subscriber;
    moq_subscription_t remote_sub = MOQ_SUBSCRIPTION_INVALID;
    if (joining) {
        /* Deliberately different: FETCH must use its own subscriber priority. */
        cfg.joining_sub = subscribe(pair, 113, &remote_sub);
        cfg.is_joining = true;
        cfg.joining_relative = true;
        cfg.joining_start = 4;
    }
    moq_fetch_t local;
    REQUIRE(moq_session_fetch(moq_simpair_client(pair), &cfg,
        moq_simpair_now_us(pair), &local) == MOQ_OK);
    pump(pair);
    moq_event_t e = event_of(sender, MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t remote = e.u.fetch_request.fetch;
    if (joining)
        MOQ_TEST_CHECK(moq_subscription_eq(e.u.fetch_request.joining_sub, remote_sub));
    moq_event_cleanup(&e);
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.end_group = 4;
    accept.end_object = 2;
    uint64_t now = moq_simpair_now_us(pair);
    REQUIRE(moq_session_accept_fetch(sender, remote, &accept, now) == MOQ_OK);
    moq_fetch_object_cfg_t object;
    moq_fetch_object_cfg_init(&object);
    object.publisher_priority = c->publisher;
    const uint8_t bytes[] = { 4, 5, 6 };
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &payload) == MOQ_OK);
    object.payload = payload;
    REQUIRE(moq_session_write_fetch_object(sender, remote, &object, now) == MOQ_OK);
    /* Change publisher priority on the same stream before any actions poll. */
    object.object_id = 1;
    object.publisher_priority = c->publisher == 0 ? 255 : 0;
    REQUIRE(moq_session_write_fetch_object(sender, remote, &object, now) == MOQ_OK);
    REQUIRE(moq_session_end_fetch(sender, remote, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_end_fetch(sender, remote, now) == MOQ_ERR_STALE_HANDLE);
    moq_action_t actions[4];
    REQUIRE(take_data(sender, actions, 4) == 4);
    moq_stream_ref_t stream = actions[0].u.send_data.stream_ref;
    uint64_t subscriber_key = c->key & UINT64_C(0x1ff00);
    check_data(&actions[0], subscriber_key | 128, stream, false);
    check_data(&actions[1], c->key, stream, false);
    check_data(&actions[2], subscriber_key | object.publisher_priority, stream, false);
    check_data(&actions[3], subscriber_key | object.publisher_priority, stream, true);
    MOQ_TEST_CHECK(actions[0].u.send_data.header_len > 0);
    MOQ_TEST_CHECK(actions[0].u.send_data.payload == NULL);
    MOQ_TEST_CHECK(actions[1].u.send_data.payload == payload);
    MOQ_TEST_CHECK(actions[2].u.send_data.payload == payload);
    for (size_t i = 0; i < 4; ++i) moq_action_cleanup(&actions[i]);
    moq_rcbuf_decref(payload);
    moq_simpair_destroy(pair);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
            test_subgroup(versions[v], false, &cases[i]);
            test_subgroup(versions[v], true, &cases[i]);
            test_fetch(versions[v], false, &cases[i]);
            test_fetch(versions[v], true, &cases[i]);
        }
    }
    MOQ_TEST_PASS("priority_metadata (40 public-API scenarios, drafts 16/18)");
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
