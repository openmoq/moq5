/*
 * Regression: a receiver that joins after the catalog was published still gets
 * it, under every draft.
 *
 * Pairs a REAL moq_media_receiver_t (driven through receiver_hook) with a real
 * server session over a simpair. The server accepts the catalog SUBSCRIBE with a
 * Largest Object of {0, 0} but never writes the catalog on the subscription, as a
 * relay does for a subscriber that arrives after the publisher's only catalog
 * object. The catalog can then reach the receiver only as a past object:
 *
 *   draft 18: SUBSCRIBE, then a Joining FETCH(offset 0) served on a fetch stream.
 *   draft 21: there is no Joining FETCH (the profile refuses it), so the receiver
 *             asks for a fill on the SUBSCRIBE (3.4), served on a fill stream.
 *
 * Before the draft-21 fill path, the receiver's Joining FETCH was refused under
 * draft 21 and it waited for a catalog update that never came.
 */
#include <moq/media_receiver.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* -- Test seam (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);

static const char CATALOG_JSON[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"role\":\"video\","
    "\"codec\":\"avc1.42001f\",\"isLive\":true}]}";

typedef struct {
    bool subscribed;     /* catalog SUBSCRIBE accepted */
    int  fills;          /* catalog served on a fill stream */
    int  fetches;        /* catalog served for a Joining FETCH */
    bool failed;
} server_t;

static bool write_catalog(moq_session_t *s, moq_fetch_t fh, uint64_t now)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)CATALOG_JSON,
                         sizeof(CATALOG_JSON) - 1, &payload) != MOQ_OK)
        return false;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 128;
    oc.payload = payload;
    bool ok = moq_session_write_fetch_object(s, fh, &oc, now) == MOQ_OK;
    moq_rcbuf_decref(payload);
    return ok && moq_session_end_fetch(s, fh, now) == MOQ_OK;
}

static void serve(moq_session_t *s, server_t *st, uint64_t now)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_subscription_t sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            ac.has_track_alias = true;
            ac.track_alias = 1;
            ac.has_largest = true;           /* the catalog was published... */
            ac.largest_group = 0;
            ac.largest_object = 0;
            if (moq_session_accept_subscribe(s, sub, &ac, now) != MOQ_OK) {
                st->failed = true;
            } else {
                st->subscribed = true;   /* ...but is not written on the sub */
                if (moq_session_sub_fill_pending(s, sub)) {
                    moq_fill_info_t info;
                    moq_fetch_t fh;
                    if (moq_session_open_fill(s, sub, now, &info, &fh) == MOQ_OK &&
                        !info.empty && info.start_group == 0 &&
                        info.end_group == 0 && info.end_object == 0 &&
                        write_catalog(s, fh, now))
                        st->fills++;
                    else
                        st->failed = true;
                }
            }
        } else if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            moq_fetch_t fh = ev.u.fetch_request.fetch;
            moq_accept_fetch_cfg_t fc;
            moq_accept_fetch_cfg_init(&fc);
            fc.end_group = 0;
            fc.end_object = 0;
            if (moq_session_accept_fetch(s, fh, &fc, now) == MOQ_OK &&
                write_catalog(s, fh, now))
                st->fetches++;
            else
                st->failed = true;
        }
        moq_event_cleanup(&ev);
    }
}

static void run(moq_version_t version, const char *label)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 21;
    cfg.initial_now_us = 1000;
    cfg.version = version;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_version(client) == version);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = false;   /* discovery only */
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) { moq_simpair_destroy(sp); return; }

    server_t st;
    memset(&st, 0, sizeof(st));
    bool track_added = false, catalog_ready = false;
    uint64_t now = moq_simpair_now_us(sp);
    for (int cycle = 0; cycle < 60 && !catalog_ready; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        now = moq_simpair_now_us(sp);
        serve(server, &st, now);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        now = moq_simpair_now_us(sp);

        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) track_added = true;
            else if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }
        if (moq_media_receiver_is_fatal(r)) break;
    }

    fprintf(stderr, "%s: subscribed=%d fills=%d fetches=%d track_added=%d "
            "catalog_ready=%d\n", label, st.subscribed, st.fills, st.fetches,
            track_added, catalog_ready);
    MOQ_TEST_CHECK(!st.failed);
    MOQ_TEST_CHECK(st.subscribed);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    MOQ_TEST_CHECK(track_added);
    MOQ_TEST_CHECK(catalog_ready);
    if (version == MOQ_VERSION_DRAFT_21) {
        MOQ_TEST_CHECK(st.fills == 1);     /* the fill on the SUBSCRIBE... */
        MOQ_TEST_CHECK(st.fetches == 0);   /* ...and no Joining FETCH */
    } else {
        MOQ_TEST_CHECK(st.fills == 0);
        MOQ_TEST_CHECK(st.fetches == 1);
    }

    moq_media_receiver_test_free(r);
    moq_simpair_destroy(sp);
}

int main(void)
{
    run(MOQ_VERSION_DRAFT_18, "draft 18");
    run(MOQ_VERSION_DRAFT_21, "draft 21");
    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_fill_catalog");
    return failures ? 1 : 0;
}
