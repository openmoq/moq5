/*
 * White-box tests for the sender's lifecycle callbacks (on_ready / on_closed),
 * deterministic and with no network. They drive the PRODUCTION fire helpers via
 * the test seam -- the same sender_fire_ready / sender_fire_closed the network
 * thread runs -- so the single-fire latch, the fatal-vs-clean flag, and the
 * callback dispatch are exercised exactly as in the shipping path. The
 * end-to-end wire behavior (a real relay accepting the publication, a peer
 * closing the session) is covered separately by the loopback tests in
 * test_media_sender.c, which need the network adapter.
 */
#include <moq/media_sender.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

static int failures = 0;

/* Test seam (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new(void);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_callbacks(moq_media_sender_t *s,
                                         const moq_media_sender_callbacks_t *cb);
void moq_media_sender_test_fire_ready(moq_media_sender_t *s);
void moq_media_sender_test_fire_closed(moq_media_sender_t *s, bool is_fatal,
                                       uint64_t fatal_code);
void moq_media_sender_test_fire_fatal(moq_media_sender_t *s, uint64_t code);
void moq_media_sender_test_fire_publish_finished(moq_media_sender_t *s,
                                                 moq_media_track_t *track);
moq_media_track_t *moq_media_sender_test_catalog_track(moq_media_sender_t *s);

/* Add an app-visible RAW video track to a test sender (returns its handle). */
static moq_media_track_t *add_video(moq_media_sender_t *s, const char *name)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_add_track(s, &tc, &t), (int)MOQ_OK);
    return t;
}

/* -- Callback observers ----------------------------------------------- */

struct obs {
    int      ready_calls;
    void    *ready_ctx;
    moq_media_sender_t *ready_sender;

    int      closed_calls;
    void    *closed_ctx;
    bool     closed_is_fatal;
    uint64_t closed_code;

    int                 track_closed_calls;
    moq_media_track_t  *track_closed_last;
};

static void on_ready_cb(void *ctx, moq_media_sender_t *sender)
{
    struct obs *o = (struct obs *)ctx;
    o->ready_calls++;
    o->ready_ctx = ctx;
    o->ready_sender = sender;
}

static void on_closed_cb(void *ctx, moq_media_sender_t *sender,
                         bool is_fatal, uint64_t fatal_code)
{
    (void)sender;
    struct obs *o = (struct obs *)ctx;
    o->closed_calls++;
    o->closed_ctx = ctx;
    o->closed_is_fatal = is_fatal;
    o->closed_code = fatal_code;
}

static void on_track_closed_cb(void *ctx, moq_media_sender_t *sender,
                               moq_media_track_t *track)
{
    (void)sender;
    struct obs *o = (struct obs *)ctx;
    o->track_closed_calls++;
    o->track_closed_last = track;
}

static void install(moq_media_sender_t *s, struct obs *o)
{
    moq_media_sender_callbacks_t cb;
    moq_media_sender_callbacks_init(&cb);
    cb.ctx = o;
    cb.on_ready = on_ready_cb;
    cb.on_closed = on_closed_cb;
    cb.on_track_closed = on_track_closed_cb;
    moq_media_sender_test_set_callbacks(s, &cb);
}

int main(void)
{
    /* on_ready fires exactly once, with the registered ctx + sender handle,
     * and does not re-fire on a second edge (latch). */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        MOQ_TEST_CHECK(s != NULL);
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);

        moq_media_sender_test_fire_ready(s);
        MOQ_TEST_CHECK_EQ_INT(o.ready_calls, 1);
        MOQ_TEST_CHECK(o.ready_ctx == &o);
        MOQ_TEST_CHECK(o.ready_sender == s);

        moq_media_sender_test_fire_ready(s);   /* second edge: latched */
        MOQ_TEST_CHECK_EQ_INT(o.ready_calls, 1);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_ready_fires_once");
    }

    /* A clean close surfaces on_closed(is_fatal=false, code=0), once. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);

        moq_media_sender_test_fire_closed(s, false, 0);
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);
        MOQ_TEST_CHECK(o.closed_is_fatal == false);
        MOQ_TEST_CHECK_EQ_INT((int)o.closed_code, 0);

        moq_media_sender_test_fire_closed(s, false, 0);   /* latched */
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_closed_clean");
    }

    /* The fatal path (sender_set_fatal) marks fatal AND fires
     * on_closed(is_fatal=true) inline with the sender's fatal code. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);

        moq_media_sender_test_fire_fatal(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);
        MOQ_TEST_CHECK(o.closed_is_fatal == true);
        MOQ_TEST_CHECK_EQ_INT((int)o.closed_code,
                              (int)MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);

        /* A second fatal (e.g. a later distinct failure) does not re-fire. */
        moq_media_sender_test_fire_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_closed_fatal");
    }

    /* Cross-path latch: whichever terminal edge lands first wins. A fatal that
     * fired first is not overridden by a later clean-close edge. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);

        moq_media_sender_test_fire_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        moq_media_sender_test_fire_closed(s, false, 0);   /* loses the race */
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);
        MOQ_TEST_CHECK(o.closed_is_fatal == true);        /* still the fatal */

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_closed_latch_first_wins");
    }

    /* A peer terminating an app track's publication surfaces on_track_closed
     * once for THAT track (mapped by pub_track), latched, and does NOT fire the
     * session on_closed. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);
        moq_media_track_t *v = add_video(s, "video");

        moq_media_sender_test_fire_publish_finished(s, v);
        MOQ_TEST_CHECK_EQ_INT(o.track_closed_calls, 1);
        MOQ_TEST_CHECK(o.track_closed_last == v);
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 0);  /* session still alive */

        moq_media_sender_test_fire_publish_finished(s, v);   /* repeat: latched */
        MOQ_TEST_CHECK_EQ_INT(o.track_closed_calls, 1);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_track_closed_app");
    }

    /* Two app tracks terminate independently: each fires once, for its own
     * handle. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);
        moq_media_track_t *v = add_video(s, "video");
        moq_media_track_t *a = add_video(s, "audio");

        moq_media_sender_test_fire_publish_finished(s, v);
        MOQ_TEST_CHECK_EQ_INT(o.track_closed_calls, 1);
        MOQ_TEST_CHECK(o.track_closed_last == v);
        moq_media_sender_test_fire_publish_finished(s, a);
        MOQ_TEST_CHECK_EQ_INT(o.track_closed_calls, 2);
        MOQ_TEST_CHECK(o.track_closed_last == a);
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 0);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.on_track_closed_two_tracks");
    }

    /* Terminating the internal CATALOG track's publication does NOT fire
     * on_track_closed (it is not app media) -- it surfaces the session on_closed
     * (clean), because without a catalog the sender cannot operate. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        struct obs o; memset(&o, 0, sizeof(o));
        install(s, &o);
        moq_media_track_t *cat = moq_media_sender_test_catalog_track(s);
        MOQ_TEST_CHECK(cat != NULL);

        moq_media_sender_test_fire_publish_finished(s, cat);
        MOQ_TEST_CHECK_EQ_INT(o.track_closed_calls, 0);   /* not app media */
        MOQ_TEST_CHECK_EQ_INT(o.closed_calls, 1);         /* session done */
        MOQ_TEST_CHECK(o.closed_is_fatal == false);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.catalog_finished_is_close");
    }

    /* No callbacks registered: firing the edges is a safe no-op (no crash). */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_video(s, "video");
        moq_media_sender_test_fire_ready(s);
        moq_media_sender_test_fire_publish_finished(s, v);
        moq_media_sender_test_fire_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        moq_media_sender_test_fire_closed(s, false, 0);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_lifecycle.no_callbacks_safe");
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
