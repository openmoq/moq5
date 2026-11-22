/*
 * One reader for a session that a subscriber facade shares with a publisher
 * facade (moq_core_internal_sub_tick_routed, core_spi.h), under each wire
 * profile.
 *
 * The client session carries a publisher serving test/out and a subscriber
 * subscribed to test/in; only the subscriber polls the session. The server is
 * a raw session.
 *
 * Invariants pinned here:
 *   - a peer's SUBSCRIBE for the publisher's track is offered to the
 *     publisher, which takes and accepts it; the subscriber's own subscription
 *     is answered on the same pass;
 *   - an event the publisher does not own reaches the subscriber untouched;
 *   - a session-wide GOAWAY reaches both facades, once each;
 *   - control (draft 22): the subscriber's plain tick, as the session's only
 *     reader, consumes that SUBSCRIBE itself, and the publisher never sees it.
 */
#include <moq/publisher.h>
#include <moq/subscriber.h>
#include <moq/sim.h>
#include "../../core/src/spi/core_spi.h"
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

static const moq_bytes_t ns_parts[] = { { (const uint8_t *)"test", 4 } };
#define NS ((moq_namespace_t){ (moq_bytes_t *)ns_parts, 1 })

typedef struct {
    test_alloc_state_t as;
    moq_alloc_t        alloc;
    moq_simpair_t     *sp;
    moq_publisher_t   *pub;
    moq_pub_track_t   *ptrack;
    moq_subscriber_t  *sub;
    moq_sub_track_t   *track;
    unsigned           routed, taken;
    unsigned           pub_goaways, sub_goaways;
} fx_t;

static uint64_t now(fx_t *f) { return moq_simpair_now_us(f->sp); }
static moq_session_t *client(fx_t *f) { return moq_simpair_client(f->sp); }
static moq_session_t *server(fx_t *f) { return moq_simpair_server(f->sp); }
static void network(fx_t *f) { moq_simpair_run_until_quiescent(f->sp, 32, NULL); }

static void on_pub_goaway(void *ctx, moq_bytes_t uri)
{
    (void)uri;
    ((fx_t *)ctx)->pub_goaways++;
}

static void on_sub_goaway(void *ctx, moq_bytes_t uri)
{
    (void)uri;
    ((fx_t *)ctx)->sub_goaways++;
}

/* The other owner of the session's events: the publisher. */
static moq_core_internal_route_t to_publisher(void *ctx, const moq_event_t *ev,
                                              uint64_t now_us)
{
    fx_t *f = (fx_t *)ctx;
    f->routed++;
    moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    (void)moq_pub_handle_event(f->pub, ev, now_us, &res);
    if (res != MOQ_PUB_EVENT_CONSUMED) return MOQ_CORE_INTERNAL_ROUTE_LEFT;
    f->taken++;
    return MOQ_CORE_INTERNAL_ROUTE_TAKEN;
}

/* One pass of the shared session: the subscriber reads, the publisher only
 * flushes what the events it took staged. */
static void pass(fx_t *f, bool routed)
{
    if (routed)
        MOQ_TEST_CHECK(moq_core_internal_sub_tick_routed(f->sub, now(f), to_publisher, f) >= 0);
    else
        MOQ_TEST_CHECK(moq_sub_tick(f->sub, now(f)) >= 0);
    (void)moq_pub_flush(f->pub, now(f));
    network(f);
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
}

/* Take the next event of `kind` from the raw server, dropping others. */
static bool server_take(fx_t *f, moq_event_kind_t kind, moq_event_t *out)
{
    for (unsigned n = 0; n < 64; n++) {
        if (moq_session_poll_events(server(f), out, 1) != 1) return false;
        if (out->kind == kind) return true;
        moq_event_cleanup(out);
    }
    return false;
}

static void fx_setup(fx_t *f, moq_version_t ver)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc; cfg.seed = 71; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK);
    moq_simpair_start(f->sp);
    network(f);
    drain(client(f));
    drain(server(f));

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    pcfg.on_goaway = on_pub_goaway;
    pcfg.callbacks.ctx = f;
    MOQ_TEST_CHECK(moq_pub_create(client(f), &f->alloc, &pcfg, &f->pub) == MOQ_OK);
    moq_pub_track_cfg_t ptcfg; moq_pub_track_cfg_init(&ptcfg);
    ptcfg.track_namespace = NS;
    ptcfg.track_name = MOQ_BYTES_LITERAL("out");
    MOQ_TEST_CHECK(moq_pub_add_track(f->pub, &ptcfg, now(f), &f->ptrack) == MOQ_OK);

    moq_sub_cfg_t scfg; moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    moq_sub_callbacks_init(&scfg.callbacks);
    scfg.callbacks.ctx = f;
    scfg.on_goaway = on_sub_goaway;
    MOQ_TEST_CHECK(moq_sub_create(client(f), &f->alloc, &scfg, &f->sub) == MOQ_OK);
}

static void fx_teardown(fx_t *f)
{
    moq_sub_destroy(f->sub);
    moq_pub_destroy(f->pub);
    drain(server(f));
    drain(client(f));
    moq_simpair_destroy(f->sp);
    MOQ_TEST_CHECK(f->as.balance == 0);
}

/* The client subscribes to test/in and the server accepts; the server
 * subscribes to test/out. Returns the server's subscription. */
static moq_subscription_t cross_subscribe(fx_t *f)
{
    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace = NS;
    tcfg.track_name = MOQ_BYTES_LITERAL("in");
    MOQ_TEST_CHECK(moq_sub_subscribe(f->sub, &tcfg, now(f), &f->track) == MOQ_OK);
    network(f);
    moq_event_t ev;
    MOQ_TEST_CHECK(server_take(f, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t in = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t accept; moq_accept_subscribe_cfg_init(&accept);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(server(f), in, &accept, now(f)) == MOQ_OK);

    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = NS;
    sc.track_name = MOQ_BYTES_LITERAL("out");
    moq_subscription_t out = { 0 };
    MOQ_TEST_CHECK(moq_session_subscribe(server(f), &sc, now(f), &out) == MOQ_OK);
    network(f);
    return out;
}

static bool server_got_subscribe_ok(fx_t *f, moq_subscription_t out)
{
    moq_event_t ev;
    for (unsigned n = 0; n < 64; n++) {
        if (moq_session_poll_events(server(f), &ev, 1) != 1) return false;
        bool ok = ev.kind == MOQ_EVENT_SUBSCRIBE_OK &&
                  ev.u.subscribe_ok.sub._opaque == out._opaque;
        moq_event_cleanup(&ev);
        if (ok) return true;
    }
    return false;
}

static const char *pname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "d16" : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d22";
}

static void test_routed(moq_version_t ver)
{
    fx_t f;
    fx_setup(&f, ver);
    moq_subscription_t out = cross_subscribe(&f);
    pass(&f, true);
    pass(&f, true);
    if (f.taken == 0) fprintf(stderr, "  %s: the publisher took nothing\n", pname(ver));
    MOQ_TEST_CHECK(f.taken >= 1);
    MOQ_TEST_CHECK(f.routed > f.taken);   /* the subscriber's own were left */
    MOQ_TEST_CHECK(server_got_subscribe_ok(&f, out));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(f.pub, f.ptrack));
    MOQ_TEST_CHECK(moq_sub_track_is_active(f.track));

    static const uint8_t uri[] = "moqt://elsewhere.example/";
    MOQ_TEST_CHECK(moq_session_goaway(server(&f), uri, sizeof(uri) - 1, now(&f)) == MOQ_OK);
    network(&f);
    pass(&f, true);
    MOQ_TEST_CHECK(f.pub_goaways == 1);
    MOQ_TEST_CHECK(f.sub_goaways == 1);
    fx_teardown(&f);
}

static void test_plain_tick_swallows(moq_version_t ver)
{
    fx_t f;
    fx_setup(&f, ver);
    moq_subscription_t out = cross_subscribe(&f);
    pass(&f, false);
    pass(&f, false);
    MOQ_TEST_CHECK(moq_sub_track_is_active(f.track));
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(f.pub, f.ptrack));
    MOQ_TEST_CHECK(!server_got_subscribe_ok(&f, out));
    fx_teardown(&f);
}

int main(void)
{
    const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22
    };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++)
        test_routed(versions[i]);
    test_plain_tick_swallows(MOQ_VERSION_DRAFT_22);
    if (failures) {
        fprintf(stderr, "subscriber_routed: %d failure(s)\n", failures);
        return 1;
    }
    printf("subscriber_routed: OK\n");
    return 0;
}
