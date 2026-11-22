/* A media sender and a media receiver attached to one endpoint share one
 * session and so one queue of events. The receiver's subscriber reads it and
 * offers each event to the sender's publisher first; the sender only flushes.
 *
 * Invariants pinned here, on drafts 16 and 22, through the real pump hooks
 * over a simulated peer:
 *   - the sender's namespace acceptance reaches it through the reading
 *     receiver, so the sender becomes ready;
 *   - the peer's SUBSCRIBE for the sender's catalog is answered by the sender
 *     while the receiver is attached and reading;
 *   - the receiver still gets its own events: on draft 22 its catalog
 *     subscription is answered and the first catalog object makes it ready;
 *   - neither service turns fatal. */
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/sim.h>
#include "test_alloc_support.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *);
void moq_endpoint_test_set_facade_version(moq_endpoint_t *, moq_version_t);
void moq_endpoint_test_pump_cycle(moq_endpoint_t *, moq_session_t *, uint64_t);
void moq_endpoint_test_drain_posted(moq_endpoint_t *, moq_session_t *, uint64_t);
void moq_endpoint_test_drain_terminal(moq_endpoint_t *);
size_t moq_endpoint_test_task_count(moq_endpoint_t *);

static unsigned failures;
static const char *case_name;
static bool check(bool value, const char *expr, int line)
{
    if (!value) {
        fprintf(stderr, "FAIL %s:%d: %s\n", case_name, line, expr);
        failures++;
    }
    return value;
}
#define CHECK(e) check((e), #e, __LINE__)
#define REQUIRE(e) do { if (!CHECK(e)) return false; } while (0)
#define STEP(e) do { if (!(e)) goto done; } while (0)

static const char catalog_json[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const moq_bytes_t ns_part = { (const uint8_t *)"shared", 6 };

typedef struct fixture {
    moq_simpair_t *pair;
    moq_endpoint_t *endpoint;
    moq_media_receiver_t *receiver;
    moq_media_sender_t *sender;
    /* The peer's view. */
    unsigned namespaces;
    bool has_receiver_sub, has_peer_sub, served, refused;
    moq_subscription_t receiver_sub, peer_sub;
    unsigned ready;
} fixture_t;

static moq_session_t *peer(fixture_t *f) { return moq_simpair_server(f->pair); }
static moq_session_t *client(fixture_t *f) { return moq_simpair_client(f->pair); }
static uint64_t now(fixture_t *f) { return moq_simpair_now_us(f->pair); }

/* The peer, playing the relay: accept namespaces and the receiver's catalog
 * subscription; note the answer to its own subscription. */
static void peer_service(fixture_t *f)
{
    moq_event_t ev;
    for (unsigned n = 0; n < 64; n++) {
        if (moq_session_poll_events(peer(f), &ev, 1) != 1) return;
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            moq_accept_namespace_cfg_t accept;
            moq_accept_namespace_cfg_init(&accept);
            CHECK(moq_session_accept_namespace(peer(f), ev.u.namespace_published.ann,
                                               &accept, now(f)) == MOQ_OK);
            f->namespaces++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            if (!f->has_receiver_sub) {
                f->receiver_sub = ev.u.subscribe_request.sub;
                f->has_receiver_sub = true;
                moq_accept_subscribe_cfg_t accept;
                moq_accept_subscribe_cfg_init(&accept);
                CHECK(moq_session_accept_subscribe(peer(f), f->receiver_sub, &accept,
                                                   now(f)) == MOQ_OK);
            }
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            if (f->has_peer_sub && ev.u.subscribe_ok.sub._opaque == f->peer_sub._opaque)
                f->served = true;
            break;
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            if (f->has_peer_sub && ev.u.subscribe_error.sub._opaque == f->peer_sub._opaque)
                f->refused = true;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void round_trip(fixture_t *f)
{
    moq_endpoint_test_pump_cycle(f->endpoint, client(f), now(f));
    (void)moq_simpair_run_until_quiescent(f->pair, 64, NULL);
    peer_service(f);
    (void)moq_simpair_run_until_quiescent(f->pair, 64, NULL);
}

static void observations(fixture_t *f)
{
    moq_media_track_event_t event;
    for (unsigned n = 0; n < 64; n++) {
        if (moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) != MOQ_OK) return;
        if (event.kind == MOQ_MEDIA_CATALOG_READY) f->ready++;
    }
}

static bool start(fixture_t *f, moq_version_t version)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.seed = 71;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    REQUIRE(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    moq_event_t event;
    while (moq_session_poll_events(peer(f), &event, 1)) moq_event_cleanup(&event);
    while (moq_session_poll_events(client(f), &event, 1)) moq_event_cleanup(&event);

    f->endpoint = moq_endpoint_test_make_bare();
    REQUIRE(f->endpoint != NULL);
    moq_endpoint_test_set_facade_version(f->endpoint, version);

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = (moq_namespace_t){ &ns_part, 1 };
    rcfg.auto_subscribe = false;
    REQUIRE(moq_media_receiver_attach(f->endpoint, &rcfg, &f->receiver) == MOQ_OK);

    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live_sized(&scfg, sizeof(scfg));
    scfg.namespace_ = (moq_namespace_t){ &ns_part, 1 };
    REQUIRE(moq_media_sender_attach(f->endpoint, &scfg, &f->sender) == MOQ_OK);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1000000;
    tc.is_live = true;
    moq_media_track_t *track = NULL;
    REQUIRE(moq_media_sender_add_track(f->sender, &tc, &track) == MOQ_OK);

    /* Both services start: the sender's namespace and the receiver's catalog
     * subscription reach the peer. */
    for (unsigned n = 0; n < 16 && !(f->namespaces && f->has_receiver_sub); n++)
        round_trip(f);
    REQUIRE(f->namespaces >= 1);
    REQUIRE(f->has_receiver_sub);
    /* The namespace's acceptance reached the sender through the receiver. */
    for (unsigned n = 0; n < 8 && !moq_media_sender_is_ready(f->sender); n++)
        round_trip(f);
    REQUIRE(moq_media_sender_is_ready(f->sender));
    return true;
}

static void finish(fixture_t *f)
{
    if (f->sender) moq_media_sender_destroy(f->sender);
    if (f->receiver) moq_media_receiver_destroy(f->receiver);
    if (f->endpoint) {
        for (unsigned i = 0; f->pair && i < 16 && moq_endpoint_test_task_count(f->endpoint); i++) {
            moq_endpoint_test_drain_posted(f->endpoint, client(f), now(f));
            (void)moq_simpair_run_until_quiescent(f->pair, 64, NULL);
        }
        moq_endpoint_test_drain_terminal(f->endpoint);
        CHECK(moq_endpoint_test_task_count(f->endpoint) == 0);
        moq_endpoint_test_free_bare(f->endpoint);
    }
    if (f->pair) moq_simpair_destroy(f->pair);
}

/* The peer subscribes to the sender's catalog while the receiver reads. */
static bool sender_served(fixture_t *f)
{
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ (moq_bytes_t *)&ns_part, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"catalog", 7 };
    REQUIRE(moq_session_subscribe(peer(f), &sc, now(f), &f->peer_sub) == MOQ_OK);
    f->has_peer_sub = true;
    (void)moq_simpair_run_until_quiescent(f->pair, 64, NULL);
    for (unsigned n = 0; n < 16 && !f->served && !f->refused; n++)
        round_trip(f);
    CHECK(f->served);
    CHECK(!f->refused);
    return f->served;
}

static bool write_catalog(fixture_t *f)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = 0;
    cfg.subgroup_id = 0;
    moq_subgroup_handle_t stream;
    REQUIRE(moq_session_open_subgroup(peer(f), f->receiver_sub, &cfg, now(f), &stream) == MOQ_OK);
    moq_object_cfg_t obj;
    moq_object_cfg_init(&obj);
    obj.object_id = 0;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)catalog_json,
                             strlen(catalog_json), &obj.payload) == MOQ_OK);
    moq_result_t rc = moq_session_write_object_ex(peer(f), stream, &obj, now(f));
    moq_rcbuf_decref(obj.payload);
    REQUIRE(rc == MOQ_OK);
    REQUIRE(moq_session_close_subgroup(peer(f), stream, now(f)) == MOQ_OK);
    for (unsigned n = 0; n < 8 && !f->ready; n++) {
        round_trip(f);
        observations(f);
    }
    return true;
}

static void shared(moq_version_t version, bool catalog)
{
    fixture_t f;
    case_name = version == MOQ_VERSION_DRAFT_22 ? "shared_endpoint_d22" : "shared_endpoint_d16";
    STEP(start(&f, version));
    STEP(sender_served(&f));
    if (catalog) {
        STEP(write_catalog(&f));
        CHECK(f.ready == 1);
    }
    CHECK(!moq_media_receiver_is_fatal(f.receiver));
    CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    finish(&f);
}

int main(void)
{
    shared(MOQ_VERSION_DRAFT_16, false);
    shared(MOQ_VERSION_DRAFT_22, true);
    printf("shared endpoint: 2 cases, %u failures\n", failures);
    return failures ? 1 : 0;
}
