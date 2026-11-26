/*
 * publish_paused: a publish_tracks sender announces each track with Forward 0,
 * sends nothing until the peer resumes that publication, and reports the demand
 * through moq_media_sender_has_media_demand(). Drives the production
 * sender_hook against a SimPair session.
 */
#include <moq/media_sender.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "test_support.h"
#include "test_alloc_support.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifndef MOQ_MEDIA_SENDER_HAS_PUBLISH_PAUSED
#error "media_sender.h must advertise publish_paused to source consumers"
#endif

static int failures = 0;

/* Test seam (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);

typedef struct {
    moq_publication_t pub;
    bool seen;
    bool forward;      /* as PUBLISH carried it */
    int  objects;
} peer_track_t;

typedef struct {
    moq_media_sender_t *s;
    moq_simpair_t      *sp;
    peer_track_t        video;
    peer_track_t        catalog;
    bool                accept_forwarding;  /* PUBLISH_OK asks for Forward 1 */
    moq_version_t       version;
} fix_t;

static bool name_is(moq_bytes_t name, const char *want)
{
    return name.len == strlen(want) && memcmp(name.data, want, name.len) == 0;
}

static void peer_drain(fix_t *f)
{
    moq_session_t *srv = moq_simpair_server(f->sp);
    uint64_t now = moq_simpair_now_us(f->sp);
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            (void)moq_session_accept_namespace(srv,
                ev.u.namespace_published.ann, &ac, now);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            peer_track_t *t = name_is(ev.u.publish_request.track_name, "v")
                ? &f->video : &f->catalog;
            t->pub = ev.u.publish_request.pub;
            t->seen = true;
            t->forward = ev.u.publish_request.forward;
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
            /* A relay keeps the publisher's Forward State; a draft-16/18
             * PUBLISH_OK that omitted it would ask for forwarding. Draft-22
             * PUBLISH_OK carries no Forward State. */
            ac.has_forward = f->version != MOQ_VERSION_DRAFT_22;
            ac.forward = t->forward || f->accept_forwarding;
            MOQ_TEST_CHECK(moq_session_accept_publish(srv, t->pub, &ac, now)
                           == MOQ_OK);
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (f->video.seen && ev.u.object_received.pub._opaque ==
                                     f->video.pub._opaque)
                f->video.objects++;
            else if (f->catalog.seen && ev.u.object_received.pub._opaque ==
                                            f->catalog.pub._opaque)
                f->catalog.objects++;
        }
        moq_event_cleanup(&ev);
    }
}

static void pump(fix_t *f)
{
    for (int i = 0; i < 8; i++) {
        moq_media_sender_test_pump(f->s, moq_simpair_client(f->sp),
                                   moq_simpair_now_us(f->sp));
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
        peer_drain(f);
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    }
}

static void set_forward(fix_t *f, peer_track_t *t, bool forward)
{
    moq_publication_update_cfg_t cfg;
    moq_publication_update_cfg_init(&cfg);
    cfg.has_forward = true;
    cfg.forward = forward;
    MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_server(f->sp),
        t->pub, &cfg, moq_simpair_now_us(f->sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(f->sp, 8, NULL);
}

static void write_video(moq_media_sender_t *s, moq_media_track_t *v,
                        uint64_t pts)
{
    uint8_t d[8];
    memset(d, 0x11, sizeof(d));
    moq_rcbuf_t *b = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &b)
                   == MOQ_OK);
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = true;
    o.starts_group = true;
    o.presentation_time_us = pts;
    if (moq_media_sender_write(s, v, &o) != MOQ_OK)
        moq_rcbuf_decref(b);
}

static moq_simpair_t *pair(moq_alloc_t *alloc, moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&cfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

static void run(moq_version_t ver, bool paused)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t f;
    memset(&f, 0, sizeof(f));
    f.sp = pair(&alloc, ver);
    f.version = ver;

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ ns_parts, 2 };
    cfg.publish_tracks = true;
    cfg.publish_paused = paused;
    f.s = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(f.s != NULL);

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    moq_media_track_t *v = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(f.s, &tc, &v) == MOQ_OK);

    pump(&f);
    MOQ_TEST_CHECK(f.video.seen && f.catalog.seen);
    MOQ_TEST_CHECK(f.video.forward == !paused);
    MOQ_TEST_CHECK(f.catalog.forward == !paused);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f.s));
    /* A publication is demand only while it forwards; it is no subscriber. */
    MOQ_TEST_CHECK(moq_media_sender_has_media_demand(f.s) == !paused);
    MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(f.s));

    write_video(f.s, v, 1000);
    pump(&f);
    MOQ_TEST_CHECK((f.video.objects > 0) == !paused);
    MOQ_TEST_CHECK((f.catalog.objects > 0) == !paused);

    if (paused) {
        /* Resume the video publication alone. */
        set_forward(&f, &f.video, true);
        pump(&f);
        MOQ_TEST_CHECK(moq_media_sender_has_media_demand(f.s));
        MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(f.s));
        write_video(f.s, v, 34000);
        pump(&f);
        MOQ_TEST_CHECK(f.video.objects > 0);
        MOQ_TEST_CHECK_EQ_INT(f.catalog.objects, 0);

        /* Resuming the catalog sends it. */
        set_forward(&f, &f.catalog, true);
        pump(&f);
        MOQ_TEST_CHECK(f.catalog.objects > 0);

        /* Pausing again withdraws the demand. */
        set_forward(&f, &f.video, false);
        pump(&f);
        MOQ_TEST_CHECK(!moq_media_sender_has_media_demand(f.s));
    }

    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.s));
    moq_media_sender_test_free(f.s);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(f.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    moq_simpair_destroy(f.sp);
    MOQ_TEST_CHECK(as.balance == 0);
    printf("ran version=0x%llx paused=%d\n", (unsigned long long)ver, paused);
}

/* Drafts 16 and 18: a PUBLISH_OK with Forward 1 starts a paused publication. */
static void run_peer_asks_for_forwarding(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t f;
    memset(&f, 0, sizeof(f));
    f.sp = pair(&alloc, ver);
    f.version = ver;
    f.accept_forwarding = true;

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ ns_parts, 2 };
    cfg.publish_tracks = true;
    cfg.publish_paused = true;
    f.s = moq_media_sender_test_new_cfg(&cfg);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    moq_media_track_t *v = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(f.s, &tc, &v) == MOQ_OK);

    pump(&f);
    MOQ_TEST_CHECK(f.video.seen && !f.video.forward);
    MOQ_TEST_CHECK(moq_media_sender_has_media_demand(f.s));
    write_video(f.s, v, 1000);
    pump(&f);
    MOQ_TEST_CHECK(f.video.objects > 0);

    moq_media_sender_test_free(f.s);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(f.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    moq_simpair_destroy(f.sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

int main(void)
{
    const moq_version_t versions[3] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    MOQ_TEST_CHECK(!moq_media_sender_has_media_demand(NULL));
    for (int i = 0; i < 3; i++) {
        run(versions[i], false);
        run(versions[i], true);
    }
    run_peer_asks_for_forwarding(MOQ_VERSION_DRAFT_16);
    run_peer_asks_for_forwarding(MOQ_VERSION_DRAFT_18);
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
