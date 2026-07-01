/*
 * moq_media_receiver_t discovery: cfg validation, attach/create ownership,
 * endpoint attachment gating, and the live catalog flow against a real
 * threaded-picoquic loopback server publishing an MSF catalog (retained
 * group, obtained by the receiver via a Joining FETCH).
 */
#include <moq/media_receiver.h>
#include <moq/picoquic_threaded.h>
#include <moq/publisher.h>
#include <moq/session.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include "test_support.h"
#include "media_builders.h"  /* shared CENC-protected init builder */

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

/* -- catalog-publishing server (direct facade) ----------------------- */

static const char k_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"cmaf\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"timescale\":90000,"
    "\"width\":1280,\"height\":720,\"framerate\":30,\"bitrate\":2000000},"
    "{\"name\":\"audio\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"opus\",\"samplerate\":48000,"
    "\"channelConfig\":\"2\",\"lang\":\"en\"}"
    "]}";

typedef struct {
    moq_publisher_t *pub;
    moq_pub_track_t *catalog_track;
    moq_pub_track_t *media_track;
    const char      *catalog_json;     /* what the retained catalog serves */
    const char      *media_track_name; /* non-NULL: add + serve this track */
    atomic_int       publish_target;   /* test thread raises; pump catches up */
    atomic_bool      media_subscribed; /* latched when the media track ever
                                          has a subscriber server-side */
    atomic_bool      catalog_subscribed; /* same latch for the catalog track */
    int              published;
    bool             failed;
    /* FETCH-only raw-session server (fetch_only_pump) state. */
    moq_subscription_t catalog_sub;      /* accepted catalog SUBSCRIBE */
    bool             catalog_sub_set;
    atomic_bool      fetch_served;       /* catalog delivered via joining FETCH */
    /* live->VOD conversion (MSF §11.3) state. */
    const char      *vod_catalog_json;   /* conversion catalog (isLive:false +
                                            trackDuration); NULL: no conversion */
    atomic_int       convert_after;      /* finish + publish VOD once published
                                            reaches this (0 disables) */
    uint64_t         convert_status;     /* SUBSCRIBE_DONE status for the finish
                                            (MOQ_PUB_DONE_TRACK_ENDED for VOD) */
    atomic_bool      finish_done;        /* latched once finish_subscribers
                                            succeeds: never finish a re-joiner */
    atomic_bool      converted;          /* latched once conversion is published */
    atomic_bool      media_rejoined;     /* latched when the media track regains a
                                            subscriber after the conversion */
} srv_state_t;

/* One LOC media catalog: a single RAW video track. Objects are grouped in
 * threes; object 0 of each group carries independent frame marking. */
static const char k_media_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000}"
    "]}";

/* The MSF §11.3 conversion of k_media_catalog_json: the SAME "v" tuple with the
 * one sanctioned mutation -- isLive true->false and trackDuration added. Every
 * other (immutable §5.3) attribute matches the live catalog exactly, so the
 * receiver accepts it as a live->VOD conversion (TRACK_UPDATED, same handle). */
static const char k_media_vod_catalog_json[] =
    "{\"version\":1,\"generatedAt\":2,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,"
    "\"trackDuration\":8072340}"
    "]}";

/* Two-track variant: "v" plus a second track, for scenarios that need a
 * TRACK_ADDED overflow while a subscribable media track exists. */
static const char k_media_catalog_two_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000},"
    "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"opus\",\"samplerate\":48000,"
    "\"channelConfig\":\"2\"}"
    "]}";

/* Catalog with a media track "v" plus a CMSF SAP event-timeline track "sap"
 * (eventType/mimeType/depends per MSF §8.2). */
static const char k_sap_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000},"
    "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"v\"]},"
    /* A non-SAP event timeline (different eventType): never auto-subscribed,
     * and even when explicitly subscribed it must NOT surface via poll_sap nor
     * leak into poll_object. */
    "{\"name\":\"scores\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.example.scores\",\"mimeType\":\"application/json\","
    "\"depends\":[\"v\"]},"
    /* A media timeline (MSF §7): never auto-subscribed; even when explicitly
     * subscribed its payload must NOT be parsed as media nor surface via
     * poll_object/poll_sap (no payload codec). */
    "{\"name\":\"history\",\"packaging\":\"mediatimeline\",\"isLive\":true,"
    "\"mimeType\":\"application/json\",\"depends\":[\"v\"]}"
    "]}";

/* A scripted SAP timeline. After receiver de-dup the unique media Locations are
 * [0,0],[0,1],[1,0],[2,0],[5,0],[5,1]; two objects are malformed (a fully
 * truncated one, and a valid-record-then-malformed-tail one) -> 2 parse_drops. */
static const struct { uint64_t g, o; const char *json; } k_sap_script[] = {
    { 0, 0, "[{\"l\":[0,0],\"data\":[2,0]}]" },
    { 0, 1, "[{\"l\":[0,1],\"data\":[3,2100]}]" },
    { 1, 0, "[{\"l\":[0,0],\"data\":[2,0]},{\"l\":[0,1],\"data\":[3,2100]},"
            "{\"l\":[1,0],\"data\":[2,4000]}]" },
    /* Faithful §8.3 independent: the full accumulated history (so the receiver's
     * monotonic de-dup is correct even if subgroup streams arrive out of order). */
    { 2, 0, "[{\"l\":[0,0],\"data\":[2,0]},{\"l\":[0,1],\"data\":[3,2100]},"
            "{\"l\":[1,0],\"data\":[2,4000]},{\"l\":[2,0],\"data\":[2,8000]}]" },
    { 3, 0, "[{\"l\":[3,0],\"data\":[2," },   /* fully truncated -> parse_drop */
    /* A VALID first record then a malformed tail: the whole object is a parse
     * drop and [5,0] must NOT leak nor advance the dedup cursor. */
    { 5, 0, "[{\"l\":[5,0],\"data\":[2,5000]},{\"l\":[5,1],\"data\":[2," },
    /* The legitimate cumulative resend: [5,0] and [5,1] still surface, proving
     * the earlier partial did not poison sap_last_*. */
    { 6, 0, "[{\"l\":[0,0],\"data\":[2,0]},{\"l\":[0,1],\"data\":[3,2100]},"
            "{\"l\":[1,0],\"data\":[2,4000]},{\"l\":[2,0],\"data\":[2,8000]},"
            "{\"l\":[5,0],\"data\":[2,5000]},{\"l\":[5,1],\"data\":[3,5100]}]" },
};
#define K_SAP_SCRIPT_N ((int)(sizeof(k_sap_script) / sizeof(k_sap_script[0])))

typedef struct {
    moq_publisher_t *pub;
    moq_pub_track_t *catalog_track;
    moq_pub_track_t *media_track;   /* "v" (added so its SUBSCRIBE is accepted) */
    moq_pub_track_t *sap_track;     /* "sap" */
    moq_pub_track_t *scores_track;  /* "scores" (non-SAP event timeline) */
    moq_pub_track_t *history_track; /* "history" (media timeline) */
    int              published_n;   /* SAP script entries written so far */
    atomic_bool      scores_published; /* the one "scores" object was written */
    atomic_bool      history_published; /* the one "history" object was written */
    bool             failed;
} sap_srv_state_t;

static sap_srv_state_t g_sap;

static int sap_server_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    sap_srv_state_t *st = (sap_srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;

    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(session, moq_alloc_default(), &pcfg, &st->pub)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
        moq_namespace_t ns = { parts, 2 };
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->catalog_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("v");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->media_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("sap");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->sap_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("scores");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->scores_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("history");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->history_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)k_sap_catalog_json,
                             strlen(k_sap_catalog_json), &payload) < 0) {
            st->failed = true; return -1;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(st->pub,
                                                      st->catalog_track, &rg);
        moq_rcbuf_decref(payload);
        if (src != MOQ_OK) { st->failed = true; return -1; }
    }

    (void)moq_pub_tick(st->pub, now_us);
    /* Publish the scripted SAP objects once the timeline track is subscribed. */
    if (st->sap_track && moq_pub_has_subscriber(st->pub, st->sap_track)) {
        while (st->published_n < K_SAP_SCRIPT_N) {
            int i = st->published_n;
            moq_rcbuf_t *payload = NULL;
            if (moq_rcbuf_create(moq_alloc_default(),
                                 (const uint8_t *)k_sap_script[i].json,
                                 strlen(k_sap_script[i].json), &payload) < 0) {
                st->failed = true; return -1;
            }
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
            ocfg.group_id = k_sap_script[i].g;
            ocfg.object_id = k_sap_script[i].o;
            ocfg.payload = payload;
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(st->pub, st->sap_track,
                                                       &ocfg, now_us);
            moq_rcbuf_decref(payload);
            if (wrc == MOQ_ERR_WOULD_BLOCK || wrc == MOQ_ERR_WRONG_STATE)
                break;   /* retry next pump */
            if (wrc != MOQ_OK) { st->failed = true; return -1; }
            st->published_n++;
        }
    }
    /* Publish one (well-formed JSON) object on the non-SAP timeline once it is
     * subscribed; the receiver must drop it (not SAP, not media). */
    if (st->scores_track && !atomic_load(&st->scores_published) &&
        moq_pub_has_subscriber(st->pub, st->scores_track)) {
        static const char scores_json[] =
            "[{\"l\":[0,0],\"data\":{\"home\":1,\"away\":0}}]";
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)scores_json,
                             strlen(scores_json), &payload) == MOQ_OK) {
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
            ocfg.group_id = 0;
            ocfg.object_id = 0;
            ocfg.payload = payload;
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(st->pub,
                                                       st->scores_track,
                                                       &ocfg, now_us);
            moq_rcbuf_decref(payload);
            if (wrc == MOQ_OK) atomic_store(&st->scores_published, true);
        }
    }
    /* Publish one explicit media-timeline object (§7.1.1) on the media timeline
     * once it is subscribed; the receiver decodes it and surfaces a typed record
     * via poll_media_timeline (never poll_sap/poll_object). */
    if (st->history_track && !atomic_load(&st->history_published) &&
        moq_pub_has_subscriber(st->pub, st->history_track)) {
        static const char history_json[] = "[[0,[0,0],1759924158381]]";
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)history_json,
                             strlen(history_json), &payload) == MOQ_OK) {
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
            ocfg.group_id = 0;
            ocfg.object_id = 0;
            ocfg.payload = payload;
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(st->pub,
                                                       st->history_track,
                                                       &ocfg, now_us);
            moq_rcbuf_decref(payload);
            if (wrc == MOQ_OK) atomic_store(&st->history_published, true);
        }
    }
    (void)moq_pub_tick(st->pub, now_us);
    return 0;
}

/* Catalog with a media track "v" plus an MSF §7 media-timeline track "tl". */
static const char k_mt_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000},"
    "{\"name\":\"tl\",\"packaging\":\"mediatimeline\",\"isLive\":true,"
    "\"mimeType\":\"application/json\",\"depends\":[\"v\"]}"
    "]}";

/* A scripted media timeline. Object [1,0] is a §7.3 independent re-send of the
 * accessible history, so its [0,0] and [0,2] records must NOT re-surface; only
 * [1,0] is new. Object [1,1] is malformed (a 2-item record) -> one parse_drop,
 * nothing enqueued, dedup cursor untouched. Unique surfaced Locations are
 * [0,0], [0,2], [1,0]. */
static const struct { uint64_t g, o; const char *json; } k_mt_script[] = {
    { 0, 0, "[[0,[0,0],100]]" },
    { 0, 1, "[[1000,[0,2],200]]" },
    { 1, 0, "[[0,[0,0],100],[1000,[0,2],200],[2000,[1,0],300]]" },
    { 1, 1, "[[0,[0,0]]]" },   /* malformed: 2-item record -> parse_drop */
};
#define K_MT_SCRIPT_N ((int)(sizeof(k_mt_script) / sizeof(k_mt_script[0])))

typedef struct {
    moq_publisher_t *pub;
    moq_pub_track_t *catalog_track;
    moq_pub_track_t *media_track;   /* "v" (added so its SUBSCRIBE is accepted) */
    moq_pub_track_t *tl_track;      /* "tl" media timeline */
    atomic_int       published_n;   /* timeline script entries written so far
                                       (pump thread writes, test thread reads) */
    bool             failed;
} mt_srv_state_t;

static mt_srv_state_t g_mt;

static int mt_server_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    mt_srv_state_t *st = (mt_srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;

    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(session, moq_alloc_default(), &pcfg, &st->pub)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
        moq_namespace_t ns = { parts, 2 };
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->catalog_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("v");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->media_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = ns;
        tc.track_name = MOQ_BYTES_LITERAL("tl");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->tl_track)
            != MOQ_OK) { st->failed = true; return -1; }
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)k_mt_catalog_json,
                             strlen(k_mt_catalog_json), &payload) < 0) {
            st->failed = true; return -1;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(st->pub,
                                                      st->catalog_track, &rg);
        moq_rcbuf_decref(payload);
        if (src != MOQ_OK) { st->failed = true; return -1; }
    }

    (void)moq_pub_tick(st->pub, now_us);
    /* Publish the scripted timeline objects once the track is subscribed. */
    if (st->tl_track && moq_pub_has_subscriber(st->pub, st->tl_track)) {
        while (atomic_load(&st->published_n) < K_MT_SCRIPT_N) {
            int i = atomic_load(&st->published_n);
            moq_rcbuf_t *payload = NULL;
            if (moq_rcbuf_create(moq_alloc_default(),
                                 (const uint8_t *)k_mt_script[i].json,
                                 strlen(k_mt_script[i].json), &payload) < 0) {
                st->failed = true; return -1;
            }
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
            ocfg.group_id = k_mt_script[i].g;
            ocfg.object_id = k_mt_script[i].o;
            ocfg.payload = payload;
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(st->pub, st->tl_track,
                                                       &ocfg, now_us);
            moq_rcbuf_decref(payload);
            if (wrc == MOQ_ERR_WOULD_BLOCK || wrc == MOQ_ERR_WRONG_STATE)
                break;   /* retry next pump */
            if (wrc != MOQ_OK) { st->failed = true; return -1; }
            (void)atomic_fetch_add(&st->published_n, 1);
        }
    }
    (void)moq_pub_tick(st->pub, now_us);
    return 0;
}

#define OBJS_PER_GROUP 3
#define OBJ_PAYLOAD_LEN 16

/* Publish LOC objects up to the requested target. Holds off until the
 * receiver's subscription is established server-side: live objects
 * written with no subscriber are (correctly) not delivered, and the
 * data-path scenarios must not lose their fixed targets to that race. */
static bool srv_publish_media(srv_state_t *st, uint64_t now_us)
{
    if (!moq_pub_has_subscriber(st->pub, st->media_track)) return true;
    atomic_store(&st->media_subscribed, true);
    int target = atomic_load(&st->publish_target);
    while (st->published < target) {
        int i = st->published;
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = (uint64_t)i * 1000u;
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = (i % OBJS_PER_GROUP) == 0;
        moq_rcbuf_t *props = NULL;
        if (moq_loc_encode(moq_alloc_default(), MOQ_LOC_PROFILE_01, &h,
                           &props) != MOQ_OK)
            return false;
        uint8_t bytes[OBJ_PAYLOAD_LEN];
        memset(bytes, (uint8_t)i, sizeof(bytes));
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                             &payload) < 0) {
            if (props) moq_rcbuf_decref(props);
            return false;
        }
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init(&ocfg);
        ocfg.group_id = (uint64_t)(i / OBJS_PER_GROUP);
        ocfg.object_id = (uint64_t)(i % OBJS_PER_GROUP);
        ocfg.payload = payload;
        ocfg.properties = props;
        /* One subgroup per group; the flag is a subgroup-header property
         * and must be uniform across the subgroup's objects. */
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(st->pub, st->media_track,
                                                   &ocfg, now_us);
        moq_rcbuf_decref(payload);
        if (props) moq_rcbuf_decref(props);
        if (wrc == MOQ_ERR_WOULD_BLOCK) return true;   /* retry next pump */
        /* WRONG_STATE = the subscriber paused us (forward=0): the session
         * refuses subgroup writes until forward is restored. Retry. */
        if (wrc == MOQ_ERR_WRONG_STATE) return true;
        if (wrc != MOQ_OK) return false;
        st->published++;
    }
    return true;
}

static srv_state_t g_srv;

static int server_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    srv_state_t *st = (srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;

    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(session, moq_alloc_default(), &pcfg,
                           &st->pub) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ parts, 2 };
        tcfg.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(st->pub, &tcfg, now_us,
                              &st->catalog_track) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        if (st->media_track_name) {
            moq_pub_track_cfg_t mcfg;
            moq_pub_track_cfg_init(&mcfg);
            mcfg.track_namespace = (moq_namespace_t){ parts, 2 };
            mcfg.track_name = (moq_bytes_t){
                (const uint8_t *)st->media_track_name,
                strlen(st->media_track_name) };
            if (moq_pub_add_track(st->pub, &mcfg, now_us,
                                  &st->media_track) != MOQ_OK) {
                st->failed = true;
                return -1;
            }
        }
        const char *json = st->catalog_json ? st->catalog_json
                                            : k_catalog_json;
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)json,
                             strlen(json), &payload) < 0) {
            st->failed = true;
            return -1;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(st->pub,
                                                      st->catalog_track,
                                                      &rg);
        moq_rcbuf_decref(payload);
        if (src != MOQ_OK) {
            st->failed = true;
            return -1;
        }
    }
    (void)moq_pub_tick(st->pub, now_us);
    if (st->catalog_track && moq_pub_has_subscriber(st->pub, st->catalog_track))
        atomic_store(&st->catalog_subscribed, true);
    if (st->media_track && !srv_publish_media(st, now_us)) {
        st->failed = true;
        return -1;
    }

    /* MSF §11.3 live->VOD conversion: once `convert_after` objects are
     * published, finish live subscribers with the configured SUBSCRIBE_DONE
     * status (Track Ended for VOD; any other status is a normal terminal end).
     * When a VOD catalog is configured, also publish it as a NEW catalog
     * generation (group 1) and refresh the retained group so late joiners pull
     * it. The finish and the catalog publish are retried independently: the
     * finish_done latch ensures finish_subscribers runs exactly once, and the
     * catalog write is all-or-nothing, so a WOULD_BLOCK on either retries only
     * the unfinished part next pump (never a second finish). */
    if (st->media_track && atomic_load(&st->convert_after) > 0 &&
        st->published >= atomic_load(&st->convert_after) &&
        !atomic_load(&st->converted)) {
        /* Finish exactly once. finish_done latches success so a later WOULD_BLOCK
         * on the catalog publish below retries ONLY the catalog -- never a second
         * finish_subscribers, which could complete a subscriber that has already
         * re-joined (mirrors media_sender.c clearing vod_finish_pending). */
        if (!atomic_load(&st->finish_done)) {
            moq_result_t fr = moq_pub_finish_subscribers(
                st->pub, st->media_track, st->convert_status, now_us);
            if (fr == MOQ_ERR_WOULD_BLOCK || fr == MOQ_ERR_WRONG_STATE) {
                (void)moq_pub_tick(st->pub, now_us);
                return 0;   /* retry the finish next pump */
            }
            if (fr != MOQ_OK) { st->failed = true; return -1; }
            atomic_store(&st->finish_done, true);
        }

        if (st->vod_catalog_json) {
            moq_rcbuf_t *payload = NULL;
            if (moq_rcbuf_create(moq_alloc_default(),
                                 (const uint8_t *)st->vod_catalog_json,
                                 strlen(st->vod_catalog_json), &payload) < 0) {
                st->failed = true;
                return -1;
            }
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
            ocfg.group_id = 1;
            ocfg.object_id = 0;
            ocfg.payload = payload;
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(
                st->pub, st->catalog_track, &ocfg, now_us);
            if (wrc == MOQ_ERR_WOULD_BLOCK || wrc == MOQ_ERR_WRONG_STATE) {
                moq_rcbuf_decref(payload);
                (void)moq_pub_tick(st->pub, now_us);
                return 0;   /* nothing written yet (all-or-nothing): retry */
            }
            if (wrc != MOQ_OK) {
                moq_rcbuf_decref(payload);
                st->failed = true;
                return -1;
            }
            moq_pub_retained_object_t robj = {
                .object_id = 0, .payload = payload };
            moq_pub_retained_group_cfg_t rg;
            moq_pub_retained_group_cfg_init(&rg);
            rg.group_id = 1;
            rg.objects = &robj;
            rg.object_count = 1;
            moq_result_t src = moq_pub_set_retained_group(
                st->pub, st->catalog_track, &rg);
            moq_rcbuf_decref(payload);
            if (src != MOQ_OK) { st->failed = true; return -1; }
        }
        atomic_store(&st->converted, true);
    }

    (void)moq_pub_tick(st->pub, now_us);

    /* After the conversion finished the live subscribers, a fresh subscriber on
     * the media track means the receiver re-subscribed for VOD. The server does
     * NOT publish new media post-conversion (production sender refuses that): the
     * latch is the join-side proof of re-subscribe. */
    if (atomic_load(&st->converted) && st->media_track &&
        moq_pub_has_subscriber(st->pub, st->media_track))
        atomic_store(&st->media_rejoined, true);
    return 0;
}

/* Server that knows the catalog track but refuses every subscription
 * (the publisher facade only answers subscribes for tracks it has). */
static int reject_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    srv_state_t *st = (srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;
    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_REJECT_ALL;
        if (moq_pub_create(session, moq_alloc_default(), &pcfg,
                           &st->pub) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ parts, 2 };
        tcfg.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(st->pub, &tcfg, now_us,
                              &st->catalog_track) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
    }
    (void)moq_pub_tick(st->pub, now_us);
    return 0;
}

/* Raw-session server that delivers the catalog ONLY via a joining FETCH.
 *
 * It accepts the catalog SUBSCRIBE while advertising a Largest Object (so the
 * subscription goes active -- the state a joining FETCH attaches to) but never
 * writes a subgroup object on that subscription. The catalog reaches the
 * receiver solely as a FETCH_OBJECT, which is what proves the MSF-01 §5
 * SUBSCRIBE + Joining FETCH(offset=0) path in media_receiver.c. Uses the public
 * core session serving APIs directly -- no publisher facade, no FETCH-serving
 * public API. */
static int fetch_only_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    srv_state_t *st = (srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;

    moq_event_t evs[8];
    size_t n = moq_session_poll_events(session, evs, 8);
    for (size_t i = 0; i < n; i++) {
        moq_event_t *e = &evs[i];
        if (e->kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            ac.has_track_alias = true;
            ac.track_alias = 1;
            ac.has_largest = true;     /* active sub for the joining FETCH */
            ac.largest_group = 0;
            ac.largest_object = 0;
            if (moq_session_accept_subscribe(session,
                    e->u.subscribe_request.sub, &ac, now_us) == MOQ_OK) {
                st->catalog_sub = e->u.subscribe_request.sub;
                st->catalog_sub_set = true;
                atomic_store(&st->catalog_subscribed, true);
            } else {
                st->failed = true;
            }
            /* Deliberately no write_object: the catalog must arrive via FETCH. */
        } else if (e->kind == MOQ_EVENT_FETCH_REQUEST) {
            moq_fetch_t fh = e->u.fetch_request.fetch;
            moq_accept_fetch_cfg_t fc;
            moq_accept_fetch_cfg_init(&fc);
            fc.end_group = 0;          /* single object at (group 0, object 0) */
            fc.end_object = 0;
            bool ok = moq_session_accept_fetch(session, fh, &fc, now_us)
                      == MOQ_OK;
            if (ok) {
                const char *json = st->catalog_json ? st->catalog_json
                                                    : k_media_catalog_json;
                moq_rcbuf_t *payload = NULL;
                if (moq_rcbuf_create(moq_alloc_default(),
                                     (const uint8_t *)json, strlen(json),
                                     &payload) == MOQ_OK) {
                    moq_fetch_object_cfg_t oc;
                    moq_fetch_object_cfg_init(&oc);
                    oc.group_id = 0;
                    oc.subgroup_id = 0;
                    oc.object_id = 0;
                    oc.publisher_priority = 128;
                    oc.payload = payload;
                    ok = moq_session_write_fetch_object(session, fh, &oc,
                                                        now_us) == MOQ_OK;
                    moq_rcbuf_decref(payload);
                } else {
                    ok = false;
                }
                if (ok)
                    ok = moq_session_end_fetch(session, fh, now_us) == MOQ_OK;
                if (ok) atomic_store(&st->fetch_served, true);
                else st->failed = true;
            } else {
                st->failed = true;
            }
        }
        moq_event_cleanup(e);
    }
    return 0;
}

typedef int (*pump_fn_t)(moq_pq_threaded_t *, uint64_t, void *);

static moq_pq_threaded_t *start_server_with(const char *cert,
                                            const char *key,
                                            pump_fn_t pump,
                                            srv_state_t *st,
                                            int *out_port)
{
    /* Port range disjoint from test_endpoint_lifecycle's (14400 + pid%997
     * + call slots reaches ~16550 under parallel ctest). */
    static int calls = 0;
    int base = 17800 + (int)(getpid() % 997) + (calls++ * 131);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 13;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_pump = pump;
        cfg.on_pump_ctx = st;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

/* -- helpers ----------------------------------------------------------- */

static void fill_cfg(moq_media_receiver_cfg_t *cfg, moq_bytes_t *parts)
{
    moq_media_receiver_cfg_init_live(cfg);
    parts[0] = MOQ_BYTES_LITERAL("svc");
    parts[1] = MOQ_BYTES_LITERAL("demo");
    cfg->namespace_ = (moq_namespace_t){ parts, 2 };
}

static moq_endpoint_cfg_t ep_cfg(char *urlbuf, size_t cap, int port)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    snprintf(urlbuf, cap, "moqt://127.0.0.1:%d", port);
    c.url = (moq_bytes_t){ (const uint8_t *)urlbuf, strlen(urlbuf) };
    c.insecure_skip_verify = true;
    return c;
}

/* Start a media server + a create-owned receiver for the data path. */
static bool start_media_pair(const char *cert, const char *key,
                             moq_media_overflow_policy_t policy,
                             uint32_t max_objects, uint64_t max_bytes,
                             moq_media_time_mode_t time_mode,
                             moq_pq_threaded_t **out_srv,
                             moq_media_receiver_t **out_r)
{
    int port = 0;
    memset(&g_srv, 0, sizeof(g_srv));
    g_srv.catalog_json = k_media_catalog_json;
    g_srv.media_track_name = "v";
    moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                               &g_srv, &port);
    if (!srv) return false;

    static char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
    static moq_bytes_t parts[2];
    moq_media_receiver_cfg_t cfg;
    fill_cfg(&cfg, parts);
    cfg.endpoint = &ec;
    cfg.auto_subscribe = true;
    cfg.time_mode = time_mode;
    cfg.overflow.policy = policy;
    cfg.overflow.max_objects = max_objects;
    cfg.overflow.max_bytes = max_bytes;
    moq_media_receiver_t *r = NULL;
    if (moq_media_receiver_create(&cfg, &r) != MOQ_OK) {
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        return false;
    }
    *out_srv = srv;
    *out_r = r;
    return true;
}

/* Drain track events until CATALOG_READY; return the single track handle.
 * On failure, log why (timeout vs fatal vs closed) -- discovery failures
 * here are otherwise indistinguishable in the FAIL line. */
static moq_media_track_t *drain_until_ready(moq_media_receiver_t *r)
{
    moq_media_track_t *track = NULL;
    moq_media_track_event_t ev;
    for (int i = 0; i < 300; i++) {
        (void)moq_media_receiver_wait(r, 100000);
        while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
            if (ev.kind == MOQ_MEDIA_TRACK_ADDED) track = ev.track;
            if (ev.kind == MOQ_MEDIA_CATALOG_READY) return track;
        }
        if (moq_media_receiver_is_fatal(r)) {
            fprintf(stderr, "drain_until_ready: fatal code=%llu\n",
                    (unsigned long long)moq_media_receiver_fatal_code(r));
            return NULL;
        }
    }
    fprintf(stderr,
            "drain_until_ready: timeout (closed=%d, srv_failed=%d, "
            "catalog_subscribed=%d)\n",
            (int)moq_media_receiver_is_closed(r), (int)g_srv.failed,
            (int)atomic_load(&g_srv.catalog_subscribed));
    return NULL;
}

static bool wait_received(moq_media_receiver_t *r, uint64_t want, int ticks)
{
    moq_media_receiver_stats_t st;
    for (int i = 0; i < ticks; i++) {
        (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
        if (st.objects_received >= want) return true;
        usleep(50000);
    }
    return false;
}

static bool bytes_is(moq_bytes_t b, const char *s)
{
    size_t n = strlen(s);
    return b.len == n && b.data && memcmp(b.data, s, n) == 0;
}

/* Run a discovery-only receiver against a server serving catalog_json, and
 * capture the first track's resolved init bytes (desc.init_data). Returns
 * whether desc.has_init was set; *out_len/out hold the decoded init bytes. */
static bool discover_first_init(const char *cert, const char *key,
                                const char *catalog_json,
                                uint8_t *out, size_t cap, size_t *out_len)
{
    *out_len = 0;
    int port = 0;
    memset(&g_srv, 0, sizeof(g_srv));
    g_srv.catalog_json = catalog_json;
    moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                               &g_srv, &port);
    if (!srv) return false;

    bool has_init = false;
    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
    moq_bytes_t parts[2];
    moq_media_receiver_cfg_t cfg;
    fill_cfg(&cfg, parts);
    cfg.endpoint = &ec;
    cfg.auto_subscribe = false;   /* discovery only */
    moq_media_receiver_t *r = NULL;
    if (moq_media_receiver_create(&cfg, &r) == MOQ_OK) {
        bool got = false;
        moq_media_track_event_t ev;
        for (int i = 0; i < 300 && !got; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                    has_init = ev.desc->has_init;
                    size_t n = ev.desc->init_data.len;
                    if (n && n <= cap) {
                        memcpy(out, ev.desc->init_data.data, n);
                        *out_len = n;
                    }
                    got = true;
                }
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        moq_media_receiver_destroy(r);
    }
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return has_init;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    /* == cfg validation (no network) =================================== */
    {
        moq_media_receiver_t *r = (moq_media_receiver_t *)(uintptr_t)0x1;
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;

        /* NULL / undersized. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(NULL, &r),
                              (int)MOQ_ERR_INVAL);
        fill_cfg(&cfg, parts);
        cfg.struct_size = 4;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);

        /* Missing namespace. */
        fill_cfg(&cfg, parts);
        cfg.namespace_.count = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);

        /* Incoherent namespace span. */
        fill_cfg(&cfg, parts);
        parts[1] = (moq_bytes_t){ NULL, 4 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);

        /* Overflow policy is a forced choice: plain init leaves it UNSET. */
        moq_media_receiver_cfg_init(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.overflow.policy,
                              (int)MOQ_MEDIA_OVERFLOW_UNSET);
        parts[0] = MOQ_BYTES_LITERAL("svc");
        parts[1] = MOQ_BYTES_LITERAL("demo");
        cfg.namespace_ = (moq_namespace_t){ parts, 2 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);

        /* Presets choose policies. */
        moq_media_receiver_cfg_init_live(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.overflow.policy,
                              (int)MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME);
        moq_media_receiver_cfg_init_flow_control(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.overflow.policy,
                              (int)MOQ_MEDIA_OVERFLOW_FLOW_CONTROL);

        /* Bogus time mode. */
        fill_cfg(&cfg, parts);
        cfg.time_mode = (moq_media_time_mode_t)42;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);

        /* create requires an endpoint cfg... */
        fill_cfg(&cfg, parts);
        cfg.endpoint = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(r == NULL);
    }

    /* == attach/create ownership + stop gating ========================= */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_endpoint_t *ep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&ec, &ep),
                              (int)MOQ_OK);

        /* ...and attach forbids one (structural "ignored"). */
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_attach(ep, &cfg, &r),
                              (int)MOQ_ERR_INVAL);

        fill_cfg(&cfg, parts);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_attach(ep, &cfg, &r),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(r != NULL);

        /* v0: one receiver per endpoint. */
        moq_media_receiver_t *r2 = NULL;
        moq_media_receiver_cfg_t cfg2;
        moq_bytes_t parts2[2];
        fill_cfg(&cfg2, parts2);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_attach(ep, &cfg2, &r2),
                              (int)MOQ_ERR_WRONG_STATE);

        /* The attachment gates stop. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep),
                              (int)MOQ_ERR_WRONG_STATE);

        /* == catalog flow: TRACK_ADDED x2 then CATALOG_READY =========== */
        moq_media_track_event_t ev;
        moq_media_track_t *video = NULL, *audio = NULL;
        int added = 0;
        bool ready = false;
        for (int i = 0; i < 300 && !ready; i++) {
            moq_result_t wrc = moq_media_receiver_wait(r, 100000);
            if (wrc == MOQ_ERR_CLOSED) break;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.kind == MOQ_MEDIA_TRACK_ADDED) {
                    /* CATALOG_READY strictly after every TRACK_ADDED. */
                    MOQ_TEST_CHECK(!ready);
                    added++;
                    MOQ_TEST_CHECK(ev.track != NULL && ev.desc != NULL);
                    MOQ_TEST_CHECK(moq_media_track_desc_get(ev.track) ==
                                   ev.desc);
                    MOQ_TEST_CHECK_EQ_U64(ev.config_generation, 0);
                    if (bytes_is(ev.desc->name, "video")) video = ev.track;
                    if (bytes_is(ev.desc->name, "audio")) audio = ev.track;
                } else if (ev.kind == MOQ_MEDIA_CATALOG_READY) {
                    MOQ_TEST_CHECK(ev.track == NULL && ev.desc == NULL);
                    ready = true;
                }
            }
        }
        MOQ_TEST_CHECK(ready);
        MOQ_TEST_CHECK_EQ_INT(added, 2);
        MOQ_TEST_CHECK(video != NULL && audio != NULL);
        MOQ_TEST_CHECK(video != audio);   /* distinct stable handles */
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        /* Structured info: the CMAF video track (no init data -> MSF
         * timescale fallback) and the RAW/LOC audio track. */
        if (video) {
            const moq_media_track_desc_t *d = moq_media_track_desc_get(video);
            MOQ_TEST_CHECK(bytes_is(d->codec, "avc1.64001f"));
            MOQ_TEST_CHECK(bytes_is(d->role, "video"));
            MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging,
                                  (int)MOQ_MEDIA_PACKAGING_CMAF);
            MOQ_TEST_CHECK_EQ_INT((int)d->info.media_type,
                                  (int)MOQ_MEDIA_TYPE_VIDEO);
            MOQ_TEST_CHECK_EQ_U64(d->info.timescale, 90000);
            MOQ_TEST_CHECK(d->has_width && d->width == 1280);
            MOQ_TEST_CHECK(d->has_height && d->height == 720);
            MOQ_TEST_CHECK(d->has_framerate &&
                           d->framerate_millis == 30000);
            MOQ_TEST_CHECK(d->has_bitrate && d->bitrate == 2000000);
            MOQ_TEST_CHECK(!d->has_init);
        }
        if (audio) {
            const moq_media_track_desc_t *d = moq_media_track_desc_get(audio);
            MOQ_TEST_CHECK(bytes_is(d->codec, "opus"));
            MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging,
                                  (int)MOQ_MEDIA_PACKAGING_RAW);
            MOQ_TEST_CHECK_EQ_INT((int)d->info.media_type,
                                  (int)MOQ_MEDIA_TYPE_AUDIO);
            MOQ_TEST_CHECK(d->has_samplerate && d->samplerate == 48000);
            MOQ_TEST_CHECK(bytes_is(d->channel_config, "2"));
            MOQ_TEST_CHECK(bytes_is(d->lang, "en"));
            MOQ_TEST_CHECK(!d->has_init);
        }

        /* Queue drained, nothing terminal: DONE. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(r, &ev, sizeof(ev)),
                              (int)MOQ_DONE);

        /* destroy detaches; stop then succeeds. */
        moq_media_receiver_destroy(r);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        moq_endpoint_destroy(ep);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        MOQ_TEST_CHECK(!g_srv.failed);
        if (g_srv.pub) { /* freed with the server's session by teardown */ }
    }

    /* == catalog discovered via joining FETCH (no live subscribe object) =
     * MSF-01 §5: the receiver subscribes (LargestObject) then issues a
     * Joining FETCH(offset=0). The server advertises a Largest Object on the
     * SUBSCRIBE_OK but never writes a subscribe object -- the catalog reaches
     * the receiver ONLY as a FETCH_OBJECT. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.catalog_json = k_media_catalog_json;
        moq_pq_threaded_t *srv = start_server_with(cert, key, fetch_only_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        cfg.auto_subscribe = false;   /* discovery only: catalog via FETCH */
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_OK);

        moq_media_track_t *track = drain_until_ready(r);
        MOQ_TEST_CHECK(track != NULL);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        /* The catalog could only have come from the FETCH -- no live
         * subscribe object was ever written. */
        MOQ_TEST_CHECK(atomic_load(&g_srv.fetch_served));
        if (track) {
            const moq_media_track_desc_t *d = moq_media_track_desc_get(track);
            MOQ_TEST_CHECK(bytes_is(d->name, "v"));
        }

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        MOQ_TEST_CHECK(!g_srv.failed);
    }

    /* == create-owns-endpoint path + track-event overflow =============== *
     * max_track_events=1 with a two-track catalog: the second TRACK_ADDED
     * cannot be queued -> the receiver terminalizes with the overflow code
     * and the already-queued event still drains before CLOSED. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        cfg.max_track_events = 1;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(r != NULL);

        bool fatal = false;
        for (int i = 0; i < 300 && !fatal; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            fatal = moq_media_receiver_is_fatal(r);
        }
        MOQ_TEST_CHECK(fatal);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_fatal_code(r),
                              MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW);

        /* The queued first event drains, then CLOSED (not DONE). */
        moq_media_track_event_t ev;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(r, &ev, sizeof(ev)),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_MEDIA_TRACK_ADDED);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(r, &ev, sizeof(ev)),
                              (int)MOQ_ERR_CLOSED);

        moq_media_receiver_destroy(r);   /* owns + stops its endpoint */
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == catalog subscription rejected: receiver terminalizes ========== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server_with(cert, key, reject_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_OK);

        bool fatal = false;
        for (int i = 0; i < 300 && !fatal; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            fatal = moq_media_receiver_is_fatal(r);
        }
        MOQ_TEST_CHECK(fatal);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_fatal_code(r),
                              MOQ_MEDIA_RECEIVER_FATAL_CATALOG_REJECTED);
        moq_media_track_event_t ev;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(r, &ev, sizeof(ev)),
                              (int)MOQ_ERR_CLOSED);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == attach to a terminal endpoint is refused, no attachment kept == *
     * Force a fast terminal: EXACT draft-18 against a draft-16-only
     * server fails the handshake fatally. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        static const moq_version_t v18 = MOQ_VERSION_DRAFT_18;
        ec.versions.struct_size = sizeof(moq_version_offer_t);
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = &v18;
        ec.versions.version_count = 1;
        moq_endpoint_t *ep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&ec, &ep),
                              (int)MOQ_OK);
        bool terminal = false;
        for (int i = 0; i < 300 && !terminal; i++) {
            (void)moq_endpoint_wait(ep, 100000);
            terminal = moq_endpoint_is_fatal(ep) ||
                       moq_endpoint_is_closed(ep);
        }
        MOQ_TEST_CHECK(terminal);

        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_attach(ep, &cfg, &r),
                              (int)MOQ_ERR_CLOSED);
        MOQ_TEST_CHECK(r == NULL);

        /* No attachment was retained: stop succeeds immediately. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        moq_endpoint_destroy(ep);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == data path: auto_subscribe delivers parsed objects ============= */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 0, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;

        moq_media_track_t *track = drain_until_ready(r);
        MOQ_TEST_CHECK(track != NULL);

        atomic_store(&g_srv.publish_target, 6);   /* two whole groups */
        MOQ_TEST_CHECK(wait_received(r, 6, 200));

        for (int i = 0; i < 6; i++) {
            moq_media_object_t obj;
            moq_result_t prc = MOQ_DONE;
            for (int w = 0; w < 100 && prc != MOQ_OK; w++) {
                prc = moq_media_receiver_poll_object(r, &obj, sizeof(obj));
                if (prc == MOQ_DONE)
                    (void)moq_media_receiver_wait(r, 50000);
            }
            MOQ_TEST_CHECK_EQ_INT((int)prc, (int)MOQ_OK);
            if (prc != MOQ_OK) break;
            /* Cross-queue guarantee held: the handle is the TRACK_ADDED
             * one. Ownership transferred: spans valid until cleanup. */
            MOQ_TEST_CHECK(obj.track == track);
            MOQ_TEST_CHECK_EQ_INT((int)obj.packaging,
                                  (int)MOQ_MEDIA_PACKAGING_RAW);
            MOQ_TEST_CHECK_EQ_SIZE(obj.payload.len, OBJ_PAYLOAD_LEN);
            MOQ_TEST_CHECK(obj.payload.data &&
                           obj.payload.data[0] == (uint8_t)i);
            MOQ_TEST_CHECK_EQ_INT((int)obj.keyframe,
                                  (int)((i % OBJS_PER_GROUP) == 0));
            MOQ_TEST_CHECK(obj.has_capture_time);
            MOQ_TEST_CHECK_EQ_U64(obj.capture_time_us,
                                  (uint64_t)i * 1000u);
            MOQ_TEST_CHECK_EQ_U64(obj.decode_time_us, (uint64_t)i * 1000u);
            MOQ_TEST_CHECK_EQ_U64(obj.config_generation, 0);
            moq_media_object_cleanup(&obj);
        }

        /* Track-event queue is independent: no spurious events. */
        moq_media_track_event_t tev;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(r, &tev, sizeof(tev)),
                              (int)MOQ_DONE);

        moq_media_receiver_stats_t st;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(r, &st, sizeof(st)),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_received, 6);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        MOQ_TEST_CHECK(!g_srv.failed);
    }

    /* == live->VOD conversion (MSF §11.3): finish live subscribers with Track
     *    Ended, publish the conversion catalog. The receiver sees TRACK_UPDATED
     *    (VOD desc) and NOT TRACK_ENDED, stays non-fatal, and the SAME handle
     *    re-subscribes for VOD (proved via the server-side join latch). This is
     *    the end-to-end shape previously blocked by the transport-bridge fatal. */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 0, MOQ_MEDIA_TIME_RAW, &srv, &r));
        if (!srv || !r) return 1;

        moq_media_track_t *track = drain_until_ready(r);
        MOQ_TEST_CHECK(track != NULL);

        /* Deliver the live phase first. */
        atomic_store(&g_srv.publish_target, 6);
        MOQ_TEST_CHECK(wait_received(r, 6, 200));

        /* Convert: finish live subscribers (Track Ended) + VOD catalog. */
        g_srv.vod_catalog_json = k_media_vod_catalog_json;
        g_srv.convert_status = MOQ_PUB_DONE_TRACK_ENDED;
        atomic_store(&g_srv.convert_after, 6);

        int updated = 0, ended = 0;
        for (int i = 0; i < 300 && updated == 0; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            moq_media_object_t obj;
            while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK)
                moq_media_object_cleanup(&obj);
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.track != track) continue;
                if (ev.kind == MOQ_MEDIA_TRACK_UPDATED) updated++;
                if (ev.kind == MOQ_MEDIA_TRACK_ENDED) ended++;
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK_EQ_INT(updated, 1);
        MOQ_TEST_CHECK_EQ_INT(ended, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        /* The handle survived and now describes VOD content. */
        const moq_media_track_desc_t *d = moq_media_track_desc_get(track);
        MOQ_TEST_CHECK(d && !d->is_live && d->has_track_duration &&
                       d->track_duration_ms == 8072340);

        /* Re-subscribe probe (moq_sub_release_track + one-shot re-arm, not a
         * terminal end): the receiver issues a fresh SUBSCRIBE for the now-VOD
         * track, so the media track regains a subscriber server-side. We probe
         * the join, not new media -- the production sender refuses post-conversion
         * writes, so this stays aligned with real service behavior. */
        bool rejoined = false;
        for (int i = 0; i < 200 && !rejoined; i++) {
            (void)moq_media_receiver_wait(r, 50000);
            moq_media_object_t o;
            while (moq_media_receiver_poll_object(r, &o, sizeof(o)) == MOQ_OK)
                moq_media_object_cleanup(&o);
            moq_media_track_event_t te;
            while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
                if (te.track == track && te.kind == MOQ_MEDIA_TRACK_ENDED) ended++;
            }
            rejoined = atomic_load(&g_srv.media_rejoined);
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(rejoined);
        moq_media_track_state_t ts = MOQ_MEDIA_TRACK_STATE_ENDED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_track_state(r, track, &ts), (int)MOQ_OK);
        MOQ_TEST_CHECK(ts != MOQ_MEDIA_TRACK_STATE_ENDED);

        /* No late TRACK_ENDED slipped in during/after the re-subscribe. */
        MOQ_TEST_CHECK_EQ_INT(ended, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        MOQ_TEST_CHECK(!g_srv.failed);
    }

    /* == non-Track-Ended SUBSCRIBE_DONE terminalizes the track: a finish with a
     *    status other than Track Ended ends the track via the normal ended path
     *    (TRACK_ENDED, no VOD re-arm). ================================== */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 0, MOQ_MEDIA_TIME_RAW, &srv, &r));
        if (!srv || !r) return 1;

        moq_media_track_t *track = drain_until_ready(r);
        MOQ_TEST_CHECK(track != NULL);

        atomic_store(&g_srv.publish_target, 6);
        MOQ_TEST_CHECK(wait_received(r, 6, 200));

        /* Finish with a non-Track-Ended status (0x3 Subscription Ended): no VOD
         * catalog, so the track must terminalize. */
        g_srv.vod_catalog_json = NULL;
        g_srv.convert_status = 0x3u;
        atomic_store(&g_srv.convert_after, 6);

        int ended = 0;
        for (int i = 0; i < 300 && ended == 0; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            moq_media_object_t obj;
            while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK)
                moq_media_object_cleanup(&obj);
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.track == track && ev.kind == MOQ_MEDIA_TRACK_ENDED) ended++;
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK_EQ_INT(ended, 1);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_track_state_t ts = MOQ_MEDIA_TRACK_STATE_ACTIVE;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_track_state(r, track, &ts), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)ts, (int)MOQ_MEDIA_TRACK_STATE_ENDED);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        MOQ_TEST_CHECK(!g_srv.failed);
    }

    /* == stalled consumer: counted DROP_GROUP, never a teardown ======== */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 4, 0, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;
        MOQ_TEST_CHECK(drain_until_ready(r) != NULL);

        atomic_store(&g_srv.publish_target, 9);   /* three groups, cap 4 */
        MOQ_TEST_CHECK(wait_received(r, 9, 200));

        moq_media_receiver_stats_t st;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(r, &st, sizeof(st)),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(st.objects_dropped > 0);
        MOQ_TEST_CHECK(st.groups_dropped > 0);
        MOQ_TEST_CHECK(st.overflow_events > 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        MOQ_TEST_CHECK(!moq_media_receiver_is_closed(r));

        /* The surviving head is group-aligned (a keyframe). */
        moq_media_object_t obj;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)), (int)MOQ_OK);
        MOQ_TEST_CHECK(obj.keyframe);
        moq_media_object_cleanup(&obj);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == stalled consumer: DROP_TO_KEYFRAME keeps a decodable head ====== */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 4, 0, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;
        MOQ_TEST_CHECK(drain_until_ready(r) != NULL);

        atomic_store(&g_srv.publish_target, 9);
        MOQ_TEST_CHECK(wait_received(r, 9, 200));

        moq_media_receiver_stats_t st;
        (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
        MOQ_TEST_CHECK(st.objects_dropped > 0);
        MOQ_TEST_CHECK(st.overflow_events > 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_object_t obj;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)), (int)MOQ_OK);
        MOQ_TEST_CHECK(obj.keyframe);   /* never deltas without their key */
        moq_media_object_cleanup(&obj);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == FLOW_CONTROL: pause on pressure, resume on drain, no drops ===== */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, 4, 0, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;
        moq_media_track_t *track = drain_until_ready(r);
        MOQ_TEST_CHECK(track != NULL);

        /* 6 objects against cap 4 (absorb ceiling 6): pauses, drops none. */
        atomic_store(&g_srv.publish_target, 6);
        MOQ_TEST_CHECK(wait_received(r, 6, 200));

        moq_media_receiver_stats_t st;
        bool paused = false;
        for (int i = 0; i < 100 && !paused; i++) {
            (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
            paused = st.paused;
            if (!paused) usleep(50000);
        }
        MOQ_TEST_CHECK(paused);
        MOQ_TEST_CHECK(st.pause_transitions >= 1);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);

        /* Drain everything: the receiver resumes below the low-water mark
         * and new objects flow again. */
        int got = 0;
        moq_media_object_t obj;
        for (int w = 0; w < 200 && got < 6; w++) {
            if (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
                moq_media_object_cleanup(&obj);
                got++;
            } else {
                (void)moq_media_receiver_wait(r, 50000);
            }
        }
        MOQ_TEST_CHECK_EQ_INT(got, 6);

        bool resumed = false;
        for (int i = 0; i < 100 && !resumed; i++) {
            (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
            resumed = !st.paused;
            if (!resumed) usleep(50000);
        }
        MOQ_TEST_CHECK(resumed);

        atomic_store(&g_srv.publish_target, 9);
        got = 0;
        for (int w = 0; w < 200 && got < 3; w++) {
            if (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
                MOQ_TEST_CHECK(obj.payload.data &&
                               obj.payload.data[0] >= 6);
                moq_media_object_cleanup(&obj);
                got++;
            } else {
                (void)moq_media_receiver_wait(r, 50000);
            }
        }
        MOQ_TEST_CHECK_EQ_INT(got, 3);
        (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == byte budget enforced + destroy with queued objects ============= */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        /* 40-byte budget, 16-byte payloads: two fit, the third forces the
         * group policy by BYTES (object cap stays at the default). */
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 40, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;
        MOQ_TEST_CHECK(drain_until_ready(r) != NULL);

        atomic_store(&g_srv.publish_target, 6);
        MOQ_TEST_CHECK(wait_received(r, 6, 200));

        moq_media_receiver_stats_t st;
        (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
        MOQ_TEST_CHECK(st.objects_dropped > 0);
        MOQ_TEST_CHECK(st.bytes_queued <= 40);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        /* Destroy WITHOUT draining: queued rcbufs/samples must release
         * cleanly (ASAN guards this scenario). */
        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == oversized object: the byte budget is hard even when empty ====== *
     * payload (16B) > max_bytes (8B): eviction cannot help, the incoming
     * object is dropped and counted, and the rest of its group follows
     * via the straggler filter. Never enqueued over budget, never fatal. */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 8, MOQ_MEDIA_TIME_RAW,
            &srv, &r));
        if (!srv || !r) return 1;
        MOQ_TEST_CHECK(drain_until_ready(r) != NULL);

        atomic_store(&g_srv.publish_target, 3);
        MOQ_TEST_CHECK(wait_received(r, 3, 200));

        moq_media_receiver_stats_t st;
        (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
        MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 0);
        MOQ_TEST_CHECK_EQ_U64(st.bytes_queued, 0);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 3);
        MOQ_TEST_CHECK(st.keyframes_dropped >= 1);
        MOQ_TEST_CHECK(st.overflow_events > 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_object_t obj;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)), (int)MOQ_DONE);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == discovery fatal blocks auto_subscribe in the same cycle ======== *
     * Two-track catalog against max_track_events=1: the second
     * TRACK_ADDED terminalizes the receiver mid-catalog. auto_subscribe
     * must NOT issue media subscriptions afterwards -- the server never
     * sees a subscriber on "v". */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.catalog_json = k_media_catalog_two_json;
        g_srv.media_track_name = "v";
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_receiver_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        cfg.auto_subscribe = true;
        cfg.max_track_events = 1;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                              (int)MOQ_OK);

        bool fatal = false;
        for (int i = 0; i < 300 && !fatal; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            fatal = moq_media_receiver_is_fatal(r);
        }
        MOQ_TEST_CHECK(fatal);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_fatal_code(r),
                              MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW);

        /* Grace for a wrongly-issued SUBSCRIBE to have reached the
         * server, then pin that none ever did. */
        usleep(300000);
        MOQ_TEST_CHECK(!atomic_load(&g_srv.media_subscribed));

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SHARED_EPOCH: reported times rebased to one receiver epoch ===== */
    {
        moq_pq_threaded_t *srv = NULL;
        moq_media_receiver_t *r = NULL;
        MOQ_TEST_CHECK(start_media_pair(cert, key,
            MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 0,
            MOQ_MEDIA_TIME_SHARED_EPOCH, &srv, &r));
        if (!srv || !r) return 1;
        MOQ_TEST_CHECK(drain_until_ready(r) != NULL);

        atomic_store(&g_srv.publish_target, 2);
        MOQ_TEST_CHECK(wait_received(r, 2, 200));

        moq_media_object_t obj;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(obj.decode_time_us, 0);   /* epoch anchor */
        moq_media_object_cleanup(&obj);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(obj.decode_time_us, 1000);
        moq_media_object_cleanup(&obj);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == RAW/LOC catalog initData survives to TRACK_ADDED ================ *
     * Regression: a LOC track's catalog initData (the encoder's decoder
     * config) must reach the receiver as desc.init_data, and be exposed as
     * desc.init.codec_config so consumers read extradata uniformly. */
    {
        const moq_alloc_t *al = moq_alloc_default();
        static const uint8_t extradata[] = {
            0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x09, 0xAB, 0xCD, 0xEF };
        moq_rcbuf_t *b64 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
            al, (moq_bytes_t){ extradata, sizeof(extradata) }, &b64), (int)MOQ_OK);

        static char cat[512];
        snprintf(cat, sizeof(cat),
            "{\"version\":1,\"generatedAt\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,"
            "\"initData\":\"%.*s\"}]}",
            (int)moq_rcbuf_len(b64), (const char *)moq_rcbuf_data(b64));

        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.catalog_json = cat;
        g_srv.media_track_name = "v";
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (srv) {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
            moq_bytes_t parts[2];
            moq_media_receiver_cfg_t cfg;
            fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            cfg.auto_subscribe = false;   /* discovery only; we inspect desc */
            moq_media_receiver_t *r = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                                  (int)MOQ_OK);

            /* Capture the desc init bytes during the TRACK_ADDED event. */
            bool got = false;
            uint8_t init_copy[64] = {0}, cc_copy[64] = {0};
            size_t init_len = 0, cc_len = 0;
            bool has_init = false, codec_ok = false;
            uint32_t timescale = 0;
            moq_media_track_event_t ev;
            for (int i = 0; i < 300 && !got; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                    if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                        has_init = ev.desc->has_init;
                        timescale = ev.desc->info.timescale;
                        codec_ok = bytes_is(ev.desc->codec, "av01");
                        init_len = ev.desc->init_data.len;
                        if (init_len && init_len <= sizeof(init_copy))
                            memcpy(init_copy, ev.desc->init_data.data, init_len);
                        cc_len = ev.desc->init.codec_config.len;
                        if (cc_len && cc_len <= sizeof(cc_copy))
                            memcpy(cc_copy, ev.desc->init.codec_config.data, cc_len);
                        got = true;
                    }
                }
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK(got);
            MOQ_TEST_CHECK(codec_ok);
            MOQ_TEST_CHECK_EQ_U64((uint64_t)timescale, 1000000);
            /* desc.init_data == the original extradata (decoded). */
            MOQ_TEST_CHECK_EQ_U64(init_len, sizeof(extradata));
            MOQ_TEST_CHECK(memcmp(init_copy, extradata, sizeof(extradata)) == 0);
            /* desc.init.codec_config == the same bytes (uniform extradata). */
            MOQ_TEST_CHECK_EQ_U64(cc_len, sizeof(extradata));
            MOQ_TEST_CHECK(memcmp(cc_copy, extradata, sizeof(extradata)) == 0);
            MOQ_TEST_CHECK(has_init);

            moq_media_receiver_destroy(r);
            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);
        }
        moq_rcbuf_decref(b64);
    }

    /* == initRef resolves through initDataList; inline initData wins ===== *
     * Receiver-side resolution is packaging-agnostic, so these use LOC tracks
     * (extradata is surfaced verbatim, no container parse needed). */
    {
        const moq_alloc_t *al = moq_alloc_default();
        static const uint8_t ref_data[]    = { 0x01, 0x64, 0x00, 0x1f, 0xAA, 0xBB };
        static const uint8_t inline_data[] = { 0xDE, 0xAD, 0xBE, 0xEF };
        moq_rcbuf_t *b64_ref = NULL, *b64_inline = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
            al, (moq_bytes_t){ ref_data, sizeof(ref_data) }, &b64_ref), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
            al, (moq_bytes_t){ inline_data, sizeof(inline_data) }, &b64_inline),
            (int)MOQ_OK);

        /* initRef-only: init lives in initDataList; track points via initRef. */
        static char cat1[512];
        snprintf(cat1, sizeof(cat1),
            "{\"version\":\"1\",\"generatedAt\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,"
            "\"initRef\":\"i1\"}],"
            "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
            (int)moq_rcbuf_len(b64_ref), (const char *)moq_rcbuf_data(b64_ref));
        uint8_t got1[64]; size_t got1_len = 0;
        MOQ_TEST_CHECK(discover_first_init(cert, key, cat1,
                                           got1, sizeof(got1), &got1_len));
        MOQ_TEST_CHECK_EQ_U64(got1_len, sizeof(ref_data));
        MOQ_TEST_CHECK(memcmp(got1, ref_data, sizeof(ref_data)) == 0);

        /* both present: inline initData takes precedence over initRef. */
        static char cat2[640];
        snprintf(cat2, sizeof(cat2),
            "{\"version\":\"1\",\"generatedAt\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,"
            "\"initData\":\"%.*s\",\"initRef\":\"i1\"}],"
            "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
            (int)moq_rcbuf_len(b64_inline), (const char *)moq_rcbuf_data(b64_inline),
            (int)moq_rcbuf_len(b64_ref), (const char *)moq_rcbuf_data(b64_ref));
        uint8_t got2[64]; size_t got2_len = 0;
        MOQ_TEST_CHECK(discover_first_init(cert, key, cat2,
                                           got2, sizeof(got2), &got2_len));
        MOQ_TEST_CHECK_EQ_U64(got2_len, sizeof(inline_data));
        MOQ_TEST_CHECK(memcmp(got2, inline_data, sizeof(inline_data)) == 0);

        moq_rcbuf_decref(b64_ref);
        moq_rcbuf_decref(b64_inline);
    }

    /* == CMSF metadata: maxSap + contentProtectionRefIDs + resolver ===== */
    {
        static const char cat[] =
            "{\"version\":\"1\",\"generatedAt\":1,"
            "\"contentProtections\":[{\"refID\":\"cp1\",\"defaultKID\":[\"kid\"],"
            "\"scheme\":\"cbcs\",\"drmSystem\":{\"systemID\":\"sys-A\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"timescale\":90000,"
            "\"maxGrpSapStartingType\":2,\"maxObjSapStartingType\":3,"
            "\"contentProtectionRefIDs\":[\"cp1\"]}]}";

        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.catalog_json = cat;
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (srv) {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
            moq_bytes_t parts[2];
            moq_media_receiver_cfg_t cfg;
            fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            cfg.auto_subscribe = false;
            moq_media_receiver_t *r = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                                  (int)MOQ_OK);

            bool got = false, hg = false, ho = false, cp_ref_ok = false;
            uint32_t grp = 0, obj = 0;
            moq_media_track_t *track = NULL;
            moq_media_track_event_t ev;
            for (int i = 0; i < 300 && !got; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                    if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                        hg = ev.desc->has_max_grp_sap; grp = ev.desc->max_grp_sap;
                        ho = ev.desc->has_max_obj_sap; obj = ev.desc->max_obj_sap;
                        cp_ref_ok = (ev.desc->content_protection_ref_id_count == 1)
                            && bytes_is(ev.desc->content_protection_ref_ids[0], "cp1");
                        track = ev.track;
                        got = true;
                    }
                }
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK(got);
            MOQ_TEST_CHECK(hg && grp == 2);
            MOQ_TEST_CHECK(ho && obj == 3);
            MOQ_TEST_CHECK(cp_ref_ok);

            /* Resolve the ref to its DRM entry while the receiver is alive. */
            if (track) {
                const moq_media_track_desc_t *d = moq_media_track_desc_get(track);
                const moq_cmsf_content_protection_t *cp =
                    moq_media_receiver_find_content_protection(
                        r, d->content_protection_ref_ids[0]);
                MOQ_TEST_CHECK(cp != NULL);
                if (cp) {
                    MOQ_TEST_CHECK(bytes_is(cp->scheme, "cbcs"));
                    MOQ_TEST_CHECK(bytes_is(cp->drm_system.system_id, "sys-A"));
                }
                MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
                    r, (moq_bytes_t){ (const uint8_t *)"none", 4 }) == NULL);
            }
            moq_media_receiver_destroy(r);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == CMSF §4.2: protected CMAF init params surfaced via desc.init ==== *
     * A CMAF track's CENC init segment (sinf/schm/schi/tenc) is parsed and the
     * tenc parameters reach the app on desc.init (has_cenc + scheme + KID +
     * isProtected + IV size), while the original codec stays resolved. */
    {
        const moq_alloc_t *al = moq_alloc_default();
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1f };
        const uint8_t kid[16] = {
            0x10, 0x77, 0xef, 0xec, 0xc0, 0xb2, 0x4d, 0x02,
            0xac, 0xe3, 0x3c, 0x1e, 0x52, 0xe2, 0xfb, 0x4b };
        uint8_t initseg[1024];
        size_t init_len = moq_test_build_cmaf_init_cenc(
            initseg, 1, 90000, 1280, 720, avcc, sizeof(avcc),
            "cbcs", 1, 8, kid, 0, MOQ_TEST_CENC_OK);
        moq_rcbuf_t *b64 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
            al, (moq_bytes_t){ initseg, init_len }, &b64), (int)MOQ_OK);

        static char cat[2048];
        snprintf(cat, sizeof(cat),
            "{\"version\":\"1\",\"generatedAt\":1,"
            "\"contentProtections\":[{\"refID\":\"cp1\",\"defaultKID\":[\"kid\"],"
            "\"scheme\":\"cbcs\",\"drmSystem\":{\"systemID\":\"sys-A\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"timescale\":90000,"
            "\"contentProtectionRefIDs\":[\"cp1\"],\"initRef\":\"i1\"}],"
            "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
            (int)moq_rcbuf_len(b64), (const char *)moq_rcbuf_data(b64));

        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.catalog_json = cat;
        moq_pq_threaded_t *srv = start_server_with(cert, key, server_pump,
                                                   &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (srv) {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
            moq_bytes_t parts[2];
            moq_media_receiver_cfg_t cfg;
            fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            cfg.auto_subscribe = false;
            moq_media_receiver_t *r = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                                  (int)MOQ_OK);

            bool got = false, cenc = false, sch = false, kidok = false;
            uint8_t isprot = 0, ivsz = 0;
            moq_media_track_event_t ev;
            for (int i = 0; i < 300 && !got; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                    if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                        if (ev.desc->has_init && ev.desc->init.has_cenc) {
                            cenc = true;
                            sch = bytes_is(ev.desc->init.scheme, "cbcs");
                            isprot = ev.desc->init.default_is_protected;
                            ivsz = ev.desc->init.default_per_sample_iv_size;
                            kidok = (ev.desc->init.default_kid.len == 16) &&
                                memcmp(ev.desc->init.default_kid.data,
                                       kid, 16) == 0;
                        }
                        got = true;
                    }
                }
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK(got);
            MOQ_TEST_CHECK(cenc);
            MOQ_TEST_CHECK(sch);
            MOQ_TEST_CHECK(isprot == 1);
            MOQ_TEST_CHECK(ivsz == 8);
            MOQ_TEST_CHECK(kidok);
            moq_media_receiver_destroy(r);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        moq_rcbuf_decref(b64);
    }

    /* == SAP event timeline: discovery, auto-skip, poll_sap, dedup ====== *
     * The eventtimeline track is discovered with full metadata, NOT
     * auto-subscribed, and after an explicit subscribe its objects surface as
     * typed, de-duplicated SAP records via poll_sap (never poll_object). A
     * malformed timeline object is a single, non-fatal parse drop. */
    {
        int port = 0;
        memset(&g_sap, 0, sizeof(g_sap));
        moq_pq_threaded_t *srv = start_server_with(cert, key, sap_server_pump,
                                                   (srv_state_t *)&g_sap, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (srv) {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
            moq_bytes_t parts[2];
            moq_media_receiver_cfg_t cfg;
            fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            cfg.auto_subscribe = true;   /* must still skip the eventtimeline track */
            moq_media_receiver_t *r = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                                  (int)MOQ_OK);

            moq_media_track_t *sap = NULL, *vid = NULL, *scores = NULL,
                              *history = NULL;
            bool ready = false, meta_ok = false, scores_meta_ok = false,
                 history_meta_ok = false;
            moq_media_track_event_t ev;
            for (int i = 0; i < 300 && !ready; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                    if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                        if (bytes_is(ev.desc->name, "sap")) {
                            sap = ev.track;
                            meta_ok =
                                bytes_is(ev.desc->packaging, "eventtimeline") &&
                                bytes_is(ev.desc->event_type,
                                         "org.ietf.moq.cmsf.sap") &&
                                bytes_is(ev.desc->mime_type, "application/json") &&
                                ev.desc->depends_count == 1 &&
                                bytes_is(ev.desc->depends[0], "v");
                        } else if (bytes_is(ev.desc->name, "scores")) {
                            scores = ev.track;
                            /* Discovered with its (non-SAP) eventType exposed. */
                            scores_meta_ok =
                                bytes_is(ev.desc->packaging, "eventtimeline") &&
                                bytes_is(ev.desc->event_type, "org.example.scores");
                        } else if (bytes_is(ev.desc->name, "history")) {
                            history = ev.track;
                            /* Discovered with raw packaging + mimeType + depends
                             * surfaced, and NO eventType (MSF §7.2). */
                            history_meta_ok =
                                bytes_is(ev.desc->packaging, "mediatimeline") &&
                                bytes_is(ev.desc->mime_type, "application/json") &&
                                ev.desc->event_type.len == 0 &&
                                ev.desc->depends_count == 1 &&
                                bytes_is(ev.desc->depends[0], "v");
                        } else if (bytes_is(ev.desc->name, "v")) {
                            vid = ev.track;
                        }
                    }
                    if (ev.kind == MOQ_MEDIA_CATALOG_READY) ready = true;
                }
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK(ready);
            MOQ_TEST_CHECK(sap != NULL && vid != NULL && scores != NULL &&
                           history != NULL);
            MOQ_TEST_CHECK(meta_ok);
            MOQ_TEST_CHECK(scores_meta_ok);
            MOQ_TEST_CHECK(history_meta_ok);

            /* auto_subscribe skips ALL event timeline tracks: they stay
             * DISCOVERED (the SAP one and the non-SAP one alike). */
            if (sap) {
                moq_media_track_state_t stt = MOQ_MEDIA_TRACK_STATE_ENDED;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_track_state(r, sap, &stt),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)stt,
                    (int)MOQ_MEDIA_TRACK_STATE_DISCOVERED);
            }
            if (scores) {
                moq_media_track_state_t stt = MOQ_MEDIA_TRACK_STATE_ENDED;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_track_state(r, scores, &stt),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)stt,
                    (int)MOQ_MEDIA_TRACK_STATE_DISCOVERED);
            }
            /* The media timeline track is likewise discovered, not subscribed. */
            if (history) {
                moq_media_track_state_t stt = MOQ_MEDIA_TRACK_STATE_ENDED;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_track_state(r, history, &stt),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)stt,
                    (int)MOQ_MEDIA_TRACK_STATE_DISCOVERED);
            }
            /* Nothing surfaces before an explicit subscribe. */
            moq_media_sap_record_t rec0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_sap(r, &rec0, sizeof(rec0)),
                (int)MOQ_DONE);

            if (sap)
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_subscribe_track(r, sap, NULL),
                    (int)MOQ_OK);

            moq_media_sap_record_t recs[16];
            int nrec = 0;
            for (int i = 0; i < 300 && nrec < 6; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (nrec < 16 &&
                       moq_media_receiver_poll_sap(r, &recs[nrec],
                                                   sizeof(recs[0])) == MOQ_OK)
                    nrec++;
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK_EQ_INT(nrec, 6);
            if (nrec >= 6) {
                MOQ_TEST_CHECK(recs[0].track == sap);
                MOQ_TEST_CHECK_EQ_U64(recs[0].group, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[0].object, 0);
                MOQ_TEST_CHECK_EQ_INT((int)recs[0].sap_type,
                                      (int)MOQ_SAP_TYPE_2);
                MOQ_TEST_CHECK_EQ_U64(recs[0].ept_ms, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[1].group, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[1].object, 1);
                MOQ_TEST_CHECK_EQ_INT((int)recs[1].sap_type,
                                      (int)MOQ_SAP_TYPE_3);
                MOQ_TEST_CHECK_EQ_U64(recs[1].ept_ms, 2100);
                MOQ_TEST_CHECK_EQ_U64(recs[2].group, 1);
                MOQ_TEST_CHECK_EQ_U64(recs[2].object, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[2].ept_ms, 4000);
                MOQ_TEST_CHECK_EQ_U64(recs[3].group, 2);
                MOQ_TEST_CHECK_EQ_U64(recs[3].object, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[3].ept_ms, 8000);
                /* The valid-then-malformed object surfaced NOTHING; these come
                 * from the later legitimate resend (dedup cursor was not poisoned). */
                MOQ_TEST_CHECK_EQ_U64(recs[4].group, 5);
                MOQ_TEST_CHECK_EQ_U64(recs[4].object, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[4].ept_ms, 5000);
                MOQ_TEST_CHECK_EQ_U64(recs[5].group, 5);
                MOQ_TEST_CHECK_EQ_U64(recs[5].object, 1);
                MOQ_TEST_CHECK_EQ_INT((int)recs[5].sap_type,
                                      (int)MOQ_SAP_TYPE_3);
                MOQ_TEST_CHECK_EQ_U64(recs[5].ept_ms, 5100);
            }

            /* Two malformed objects -> exactly two non-fatal parse drops (the
             * fully-truncated one and the valid-record-then-malformed-tail one). */
            moq_media_receiver_stats_t mst;
            for (int i = 0; i < 200; i++) {
                (void)moq_media_receiver_get_stats(r, &mst, sizeof(mst));
                if (mst.parse_drops >= 2) break;
                usleep(50000);
            }
            MOQ_TEST_CHECK_EQ_U64(mst.parse_drops, 2);
            MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

            /* No SAP object ever leaks into the media object path. */
            moq_media_object_t mo;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_object(r, &mo, sizeof(mo)),
                (int)MOQ_DONE);

            /* A non-SAP event timeline (scores), even explicitly subscribed and
             * with an object delivered, never surfaces via poll_sap and never
             * leaks into poll_object. A media timeline (history) likewise never
             * surfaces via poll_sap/poll_object -- it is parsed and surfaced via
             * poll_media_timeline instead (checked below). */
            if (scores)
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_subscribe_track(r, scores, NULL),
                    (int)MOQ_OK);
            if (history)
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_subscribe_track(r, history, NULL),
                    (int)MOQ_OK);
            for (int i = 0; i < 200 &&
                 !(atomic_load(&g_sap.scores_published) &&
                   atomic_load(&g_sap.history_published)); i++) {
                (void)moq_media_receiver_wait(r, 50000);
            }
            MOQ_TEST_CHECK(atomic_load(&g_sap.scores_published));
            MOQ_TEST_CHECK(atomic_load(&g_sap.history_published));
            /* Give the delivered objects time to be processed. */
            for (int i = 0; i < 10; i++)
                (void)moq_media_receiver_wait(r, 50000);
            moq_media_sap_record_t rec_extra;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_sap(r, &rec_extra, sizeof(rec_extra)),
                (int)MOQ_DONE);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_object(r, &mo, sizeof(mo)),
                (int)MOQ_DONE);

            /* The media-timeline object DID surface as a typed record on the
             * history handle (payload "[[0,[0,0],1759924158381]]"). */
            if (history) {
                moq_media_timeline_record_t mt;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_poll_media_timeline(r, &mt, sizeof(mt)),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK(mt.track == history);
                MOQ_TEST_CHECK_EQ_U64(mt.media_time_ms, 0);
                MOQ_TEST_CHECK_EQ_U64(mt.group, 0);
                MOQ_TEST_CHECK_EQ_U64(mt.object, 0);
                MOQ_TEST_CHECK_EQ_U64(mt.wallclock_ms, 1759924158381ull);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_poll_media_timeline(r, &mt, sizeof(mt)),
                    (int)MOQ_DONE);
            }

            MOQ_TEST_CHECK(!g_sap.failed);
            moq_media_receiver_destroy(r);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == media timeline (MSF §7.1.1): discovery, explicit subscribe, typed
     *    poll_media_timeline, §7.3 re-send dedup, malformed parse_drop ====== */
    {
        int port = 0;
        memset(&g_mt, 0, sizeof(g_mt));
        moq_pq_threaded_t *srv = start_server_with(cert, key, mt_server_pump,
                                                   (srv_state_t *)&g_mt, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (srv) {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
            moq_bytes_t parts[2];
            moq_media_receiver_cfg_t cfg;
            fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            cfg.auto_subscribe = true;   /* must still skip the mediatimeline track */
            moq_media_receiver_t *r = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&cfg, &r),
                                  (int)MOQ_OK);

            moq_media_track_t *tl = NULL, *vid = NULL;
            bool ready = false, tl_meta_ok = false;
            moq_media_track_event_t ev;
            for (int i = 0; i < 300 && !ready; i++) {
                (void)moq_media_receiver_wait(r, 100000);
                while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                    if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                        if (bytes_is(ev.desc->name, "tl")) {
                            tl = ev.track;
                            tl_meta_ok =
                                bytes_is(ev.desc->packaging, "mediatimeline") &&
                                bytes_is(ev.desc->mime_type, "application/json") &&
                                ev.desc->event_type.len == 0 &&
                                ev.desc->depends_count == 1 &&
                                bytes_is(ev.desc->depends[0], "v");
                        } else if (bytes_is(ev.desc->name, "v")) {
                            vid = ev.track;
                        }
                    }
                    if (ev.kind == MOQ_MEDIA_CATALOG_READY) ready = true;
                }
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK(ready);
            MOQ_TEST_CHECK(tl != NULL && vid != NULL);
            MOQ_TEST_CHECK(tl_meta_ok);

            /* Discovered, not auto-subscribed. */
            if (tl) {
                moq_media_track_state_t stt = MOQ_MEDIA_TRACK_STATE_ENDED;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_track_state(r, tl, &stt), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)stt,
                    (int)MOQ_MEDIA_TRACK_STATE_DISCOVERED);
            }

            /* ABI: arg validation before anything is queued. */
            moq_media_timeline_record_t probe;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_media_timeline(r, &probe, 4),
                (int)MOQ_ERR_INVAL);   /* out_size below the v0 base */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_media_timeline(r, NULL, sizeof(probe)),
                (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_media_timeline(NULL, &probe,
                                                            sizeof(probe)),
                (int)MOQ_ERR_INVAL);
            /* Nothing surfaces before an explicit subscribe. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_media_timeline(r, &probe,
                                                            sizeof(probe)),
                (int)MOQ_DONE);

            moq_media_receiver_stats_t st_before;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_get_stats(r, &st_before, sizeof(st_before)),
                (int)MOQ_OK);

            if (tl)
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_media_receiver_subscribe_track(r, tl, NULL),
                    (int)MOQ_OK);

            /* Wait for the whole script (including the trailing malformed obj). */
            for (int i = 0; i < 300 &&
                 atomic_load(&g_mt.published_n) < K_MT_SCRIPT_N; i++)
                (void)moq_media_receiver_wait(r, 50000);
            MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_mt.published_n), K_MT_SCRIPT_N);

            /* Collect surfaced records: expect exactly [0,0],[0,2],[1,0] once. */
            moq_media_timeline_record_t recs[8];
            size_t nrec = 0;
            for (int i = 0; i < 200 && nrec < 3; i++) {
                (void)moq_media_receiver_wait(r, 50000);
                moq_media_timeline_record_t rec;
                while (nrec < 8 &&
                       moq_media_receiver_poll_media_timeline(
                           r, &rec, sizeof(rec)) == MOQ_OK)
                    recs[nrec++] = rec;
                if (moq_media_receiver_is_fatal(r)) break;
            }
            MOQ_TEST_CHECK_EQ_SIZE(nrec, 3);
            if (nrec == 3) {
                MOQ_TEST_CHECK(recs[0].track == tl);
                MOQ_TEST_CHECK_EQ_U64(recs[0].media_time_ms, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[0].group, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[0].object, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[0].wallclock_ms, 100);
                MOQ_TEST_CHECK_EQ_U64(recs[1].media_time_ms, 1000);
                MOQ_TEST_CHECK_EQ_U64(recs[1].group, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[1].object, 2);
                MOQ_TEST_CHECK_EQ_U64(recs[1].wallclock_ms, 200);
                MOQ_TEST_CHECK_EQ_U64(recs[2].media_time_ms, 2000);
                MOQ_TEST_CHECK_EQ_U64(recs[2].group, 1);
                MOQ_TEST_CHECK_EQ_U64(recs[2].object, 0);
                MOQ_TEST_CHECK_EQ_U64(recs[2].wallclock_ms, 300);
            }

            /* Drain any tail and confirm no duplicates from the §7.3 re-send. */
            for (int i = 0; i < 6; i++)
                (void)moq_media_receiver_wait(r, 50000);
            moq_media_timeline_record_t tail;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_poll_media_timeline(r, &tail, sizeof(tail)),
                (int)MOQ_DONE);

            /* The one malformed object counted exactly one parse_drop. */
            moq_media_receiver_stats_t st_after;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_receiver_get_stats(r, &st_after, sizeof(st_after)),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(st_after.parse_drops, st_before.parse_drops + 1);
            MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

            MOQ_TEST_CHECK(!g_mt.failed);
            moq_media_receiver_destroy(r);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    MOQ_TEST_PASS("media_receiver");
    return failures != 0;
}
