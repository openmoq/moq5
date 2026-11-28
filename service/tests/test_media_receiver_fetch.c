/* Standalone FETCH of a media track (moq_media_receiver_fetch_track): the
 * production receiver TU driven through its real endpoint pump hook over a
 * simulated client/server pair, with the server answering on the wire. */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#include <moq/sim.h>
#include <moq/loc.h>
#include "test_support.h"
#include "test_alloc_support.h"

#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"

static int failures;

#define FX_CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #x); \
    failures++; return false; } } while (0)

static const uint8_t fx_body[] = { 0x31, 0, 0x33 };
static const uint8_t fx_video[] = { 0xa4, 0, 0xb5 };
#define FX_GROUP 5u
#define FX_MAX_FETCHES 4u

typedef struct fx_peer_fetch {
    moq_fetch_t fetch;
    bool named_v;
    size_t ns_count;              /* the FETCH's namespace, its first two fields */
    char ns[2][16];
    size_t ns_len[2];
    uint64_t start_group, start_object, end_group, end_object;
} fx_peer_fetch_t;

typedef struct fx {
    test_alloc_state_t ledger;
    moq_alloc_t alloc;
    moq_version_t version;
    moq_simpair_t *pair;
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    bool media_accepted;
    bool deny_fetch;              /* resource credentials: FETCH denied */
    unsigned outcomes;            /* auth outcome callbacks */
    moq_media_auth_scope_t outcome_scope;
    moq_media_auth_outcome_t outcome;
    moq_media_track_t *outcome_track;
    unsigned fetch_requests;
    moq_subscription_t catalog_sub;  /* the catalog SUBSCRIBE the server saw */
    bool catalog_seen;
    fx_peer_fetch_t peer[FX_MAX_FETCHES];
} fx_t;

static uint64_t fx_now(fx_t *f) { return moq_simpair_now_us(f->pair); }
static moq_session_t *fx_server(fx_t *f) { return moq_simpair_server(f->pair); }
static moq_session_t *fx_client(fx_t *f) { return moq_simpair_client(f->pair); }

static void fx_finish(fx_t *f)
{
    if (f->receiver) {
        if (f->pair)
            moq_media_receiver_test_destroy_with_session(f->receiver,
                fx_client(f), fx_now(f));
        else
            moq_media_receiver_test_free(f->receiver);
    }
    if (f->pair) moq_simpair_destroy(f->pair);
    MOQ_TEST_CHECK(f->ledger.balance == 0);
}

/* The media SUBSCRIBE is accepted (draft 22 with the LOC-04 Track
 * Properties the fetched Objects are parsed against); the catalog's is left
 * unanswered, its catalog having been ingested through the test seam. */
static bool fx_accept_media(fx_t *f, moq_subscription_t sub)
{
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    moq_rcbuf_t *properties = NULL;
    if (f->version == MOQ_VERSION_DRAFT_22) {
        moq_loc_metadata_t loc;
        moq_loc_metadata_init_sized(&loc, sizeof(loc));
        loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_TIMESCALE;
        loc.video_config = (moq_bytes_t){ fx_video, sizeof(fx_video) };
        loc.timescale = 1000;
        FX_CHECK(moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22,
            MOQ_LOC_PROFILE_04, &loc, &properties) == MOQ_OK);
        ac.track_properties = (moq_bytes_t){ moq_rcbuf_data(properties),
                                             moq_rcbuf_len(properties) };
    }
    moq_result_t rc = moq_session_accept_subscribe(fx_server(f), sub, &ac, fx_now(f));
    if (properties) moq_rcbuf_decref(properties);
    FX_CHECK(rc == MOQ_OK);
    f->media_accepted = true;
    return true;
}

/* Deliver everything in flight and answer the server's requests. */
static bool fx_network(fx_t *f)
{
    for (unsigned round = 0; round < 3; round++) {
        FX_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        moq_event_t ev;
        for (unsigned n = 0; n < 64 && moq_session_poll_events(fx_server(f), &ev, 1); n++) {
            bool ok = true;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_bytes_t name = ev.u.subscribe_request.track_name;
                if (name.len == 1 && name.data[0] == 'v')
                    ok = fx_accept_media(f, ev.u.subscribe_request.sub);
                else if (name.len == 7 && memcmp(name.data, "catalog", 7) == 0) {
                    f->catalog_sub = ev.u.subscribe_request.sub;   /* left pending */
                    f->catalog_seen = true;
                }
            } else if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                if (f->fetch_requests < FX_MAX_FETCHES) {
                    const moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                    fx_peer_fetch_t *p = &f->peer[f->fetch_requests];
                    p->fetch = fr->fetch;
                    p->named_v = fr->track_name.len == 1 && fr->track_name.data[0] == 'v';
                    p->ns_count = fr->track_namespace.count;
                    for (size_t k = 0; k < 2 && k < fr->track_namespace.count; k++) {
                        moq_bytes_t part = fr->track_namespace.parts[k];
                        p->ns_len[k] = part.len < sizeof(p->ns[k]) ? part.len : sizeof(p->ns[k]);
                        if (p->ns_len[k]) memcpy(p->ns[k], part.data, p->ns_len[k]);
                    }
                    p->start_group = fr->start_group;
                    p->start_object = fr->start_object;
                    p->end_group = fr->end_group;
                    p->end_object = fr->end_object;
                }
                f->fetch_requests++;
            }
            moq_event_cleanup(&ev);
            if (!ok) return false;
        }
    }
    return true;
}

/* One full receiver pass (the endpoint's receiver hook), then the network. */
static bool fx_pump(fx_t *f)
{
    moq_media_receiver_test_pump(f->receiver, fx_client(f), fx_now(f));
    return fx_network(f);
}

static const uint8_t fx_token[] = { 't', 'o', 'k' };

/* A credential source that allows every request but FETCH, which it denies. */
static moq_result_t fx_select(void *ctx, const moq_media_auth_request_t *request,
    moq_auth_token_t *out, size_t cap, size_t *count, moq_media_auth_decision_t *decision)
{
    fx_t *f = ctx;
    if (request->action == MOQ_MEDIA_AUTH_ACTION_FETCH) {
        *decision = MOQ_MEDIA_AUTH_DENY;
        return MOQ_OK;
    }
    if (cap < 1) return MOQ_ERR_INVAL;
    out[0] = (moq_auth_token_t){ f->version == MOQ_VERSION_DRAFT_16 ? 7 : UINT64_MAX,
                                 { fx_token, sizeof(fx_token) } };
    *count = 1;
    *decision = MOQ_MEDIA_AUTH_ALLOW;
    return MOQ_OK;
}

static void fx_outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    fx_t *f = ctx;
    f->outcomes++;
    f->outcome_scope = info->scope;
    f->outcome = info->outcome;
    f->outcome_track = info->track;
}

/* The catalog fx_start_auth ingests in place of its own, when set. */
static const char *fx_catalog_json;

/* A pair at `version`, a receiver on it with one LOC track "v" subscribed
 * (passive: Forward 0) and established; with deny_fetch, a credential source
 * that denies every FETCH. */
static bool fx_start_auth(fx_t *f, moq_version_t version, bool passive,
                          uint32_t max_fetches_per_track, bool deny_fetch)
{
    static moq_bytes_t ns[] = {
        { .data = (const uint8_t *)"fetch", .len = sizeof("fetch") - 1 }
    };
    static const char catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
        "\"role\":\"video\",\"codec\":\"av01\"}]}";
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->ledger);
    f->version = version;
    f->deny_fetch = deny_fetch;
    moq_simpair_cfg_t pair_cfg = MOQ_SIMPAIR_CFG_INIT;
    pair_cfg.alloc = &f->alloc;
    pair_cfg.version = version;
    pair_cfg.client_send_request_capacity = pair_cfg.server_send_request_capacity = true;
    pair_cfg.client_initial_request_capacity = pair_cfg.server_initial_request_capacity = 32;
    FX_CHECK(moq_simpair_create(&pair_cfg, &f->pair) == MOQ_OK);
    FX_CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    FX_CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    FX_CHECK(moq_session_state(fx_client(f)) == MOQ_SESS_ESTABLISHED);
    FX_CHECK(moq_session_version(fx_client(f)) == version);
    moq_session_t *ends[] = { fx_client(f), fx_server(f) };
    for (size_t i = 0; i < 2; i++) {
        moq_event_t event;
        for (unsigned j = 0; j < 32 && moq_session_poll_events(ends[i], &event, 1); j++)
            moq_event_cleanup(&event);
    }

    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.companion_profile = version == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    cfg.max_fetches_per_track = max_fetches_per_track;
    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    auth.ctx = f;
    auth.select = fx_select;
    auth.on_outcome = fx_outcome;
    if (deny_fetch) cfg.resource_auth = &auth;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->receiver = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    FX_CHECK(f->receiver != NULL);
    f->receiver->transport_version = version;
    f->receiver->joins_by_fill = version == MOQ_VERSION_DRAFT_22;
    moq_sub_cfg_t sub_cfg;
    moq_sub_cfg_init_sized(&sub_cfg, sizeof(sub_cfg));
    sub_cfg.callbacks.ctx = f->receiver;
    sub_cfg.callbacks.on_subscribed = receiver_on_subscribed;
    sub_cfg.callbacks.on_subscribe_error = receiver_on_subscribe_error;
    sub_cfg.on_subscribe_done = receiver_on_subscribe_done;
    sub_cfg.on_update_ok = receiver_on_update_ok;
    FX_CHECK(moq_sub_create(fx_client(f), &f->alloc, &sub_cfg, &f->receiver->sub) == MOQ_OK);
    const char *json = fx_catalog_json ? fx_catalog_json : catalog;
    moq_media_receiver_test_ingest(f->receiver, 0, 0, json, strlen(json));
    moq_media_track_event_t event;
    for (unsigned i = 0; i < 16 &&
         moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK; i++)
        if (event.kind == MOQ_MEDIA_TRACK_ADDED) f->track = event.track;
    FX_CHECK(f->track != NULL);

    const bool forward = !passive;
    moq_media_receiver_track_subscribe_cfg_t tcfg;
    moq_media_receiver_track_subscribe_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.forward = &forward;
    FX_CHECK(moq_media_receiver_subscribe_track(f->receiver, f->track, &tcfg) == MOQ_OK);
    /* The first pass subscribes the catalog and the track; the server
     * accepts the track; the second pass takes its acceptance. */
    FX_CHECK(fx_pump(f));
    FX_CHECK(fx_pump(f));
    FX_CHECK(f->media_accepted);
    FX_CHECK(f->track->sub_track && moq_sub_track_is_active(f->track->sub_track));
    FX_CHECK(f->track->passive == passive);
    while (moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK) {}
    return !moq_media_receiver_is_fatal(f->receiver);
}

static bool fx_start(fx_t *f, moq_version_t version, bool passive,
                     uint32_t max_fetches_per_track)
{
    return fx_start_auth(f, version, passive, max_fetches_per_track, false);
}

/* The fetch of group FX_GROUP whole: [FX_GROUP:0, end of FX_GROUP]. */
static moq_result_t fx_fetch(fx_t *f)
{
    moq_media_fetch_cfg_t cfg;
    moq_media_fetch_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.start_group = FX_GROUP;
    cfg.start_object = 0;
    cfg.end_group = FX_GROUP;
    cfg.end_object = 0;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = 9;
    return moq_media_receiver_fetch_track(f->receiver, f->track, &cfg);
}

/* Poll the next track event and require it to be `kind`. */
static bool fx_event(fx_t *f, moq_media_track_event_kind_t kind,
                     moq_media_track_event_t *out)
{
    memset(out, 0xa5, sizeof(*out));
    FX_CHECK(moq_media_receiver_poll_track(f->receiver, out, sizeof(*out)) == MOQ_OK);
    FX_CHECK(out->struct_size == sizeof(*out));
    FX_CHECK(out->kind == kind);
    FX_CHECK(out->track == f->track);
    return true;
}

static bool fx_no_event(fx_t *f)
{
    moq_media_track_event_t ev;
    /* No endpoint: an empty receiver reports CLOSED rather than DONE. */
    FX_CHECK(moq_media_receiver_poll_track(f->receiver, &ev, sizeof(ev)) != MOQ_OK);
    return true;
}

/* Send the fetch and take its FETCH_CREATED: the request ID. */
static bool fx_created(fx_t *f, uint64_t *id)
{
    unsigned before = f->fetch_requests;
    FX_CHECK(fx_pump(f));
    FX_CHECK(f->fetch_requests == before + 1);
    const fx_peer_fetch_t *p = &f->peer[before];
    FX_CHECK(p->named_v && p->start_group == FX_GROUP && p->start_object == 0);
    moq_media_track_event_t ev;
    FX_CHECK(fx_event(f, MOQ_MEDIA_TRACK_FETCH_CREATED, &ev));
    FX_CHECK(ev.fill_reason == 0 && ev.fill_error_domain == 0 && ev.fill_error_code == 0);
    *id = ev.initiating_request_id;
    return true;
}

static bool fx_buffers(fx_t *f, uint64_t timestamp, moq_rcbuf_t **payload,
                       moq_rcbuf_t **properties)
{
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    loc.timestamp = timestamp;
    loc.marking_len = 1;
    loc.marking[0] = 0x20;
    bool d22 = f->version == MOQ_VERSION_DRAFT_22;
    FX_CHECK(moq_loc_encode_metadata(&f->alloc, f->version,
        d22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01, &loc, properties) == MOQ_OK);
    moq_result_t rc = moq_rcbuf_create(&f->alloc, fx_body, sizeof(fx_body), payload);
    if (rc != MOQ_OK) { moq_rcbuf_decref(*properties); *properties = NULL; }
    FX_CHECK(rc == MOQ_OK);
    return true;
}

static bool fx_accept(fx_t *f, unsigned idx)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = f->peer[idx].end_group;
    ac.end_object = f->peer[idx].end_object;
    FX_CHECK(moq_session_accept_fetch(fx_server(f), f->peer[idx].fetch, &ac,
                                      fx_now(f)) == MOQ_OK);
    return true;
}

/* Objects FX_GROUP:0..count-1 on the accepted fetch `idx`, then its end. */
static bool fx_serve(fx_t *f, unsigned idx, unsigned count, bool end)
{
    for (unsigned k = 0; k < count; k++) {
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = FX_GROUP;
        oc.subgroup_id = 0;
        oc.object_id = k;
        oc.publisher_priority = 7;
        FX_CHECK(fx_buffers(f, 9000 + k, &oc.payload, &oc.properties));
        moq_result_t rc = moq_session_write_fetch_object(fx_server(f), f->peer[idx].fetch,
                                                         &oc, fx_now(f));
        moq_rcbuf_decref(oc.payload);
        moq_rcbuf_decref(oc.properties);
        FX_CHECK(rc == MOQ_OK);
    }
    if (end)
        FX_CHECK(moq_session_end_fetch(fx_server(f), f->peer[idx].fetch, fx_now(f)) == MOQ_OK);
    return fx_network(f);
}

static bool fx_reject(fx_t *f, unsigned idx, moq_request_error_t code)
{
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = code;
    FX_CHECK(moq_session_reject_fetch(fx_server(f), f->peer[idx].fetch, &rj,
                                      fx_now(f)) == MOQ_OK);
    return fx_network(f);
}

/* Poll `count` Objects: FETCH origin, the fetch's ID, FX_GROUP:0.. in order;
 * with `last`, nothing is queued after them. */
static bool fx_objects_of(fx_t *f, uint64_t id, unsigned count, bool last)
{
    for (unsigned k = 0; k < count; k++) {
        moq_media_object_t obj;
        memset(&obj, 0, sizeof(obj));
        FX_CHECK(moq_media_receiver_poll_object(f->receiver, &obj, sizeof(obj)) == MOQ_OK);
        bool ok = obj.track == f->track &&
            obj.delivery_origin == MOQ_MEDIA_DELIVERY_FETCH &&
            obj.initiating_request_id == id &&
            obj.group_id == FX_GROUP && obj.object_id == k &&
            obj.payload.len == sizeof(fx_body) &&
            memcmp(obj.payload.data, fx_body, sizeof(fx_body)) == 0;
        moq_media_object_cleanup(&obj);
        FX_CHECK(ok);
    }
    if (!last) return true;
    moq_media_object_t none;
    memset(&none, 0, sizeof(none));
    FX_CHECK(moq_media_receiver_poll_object(f->receiver, &none, sizeof(none)) != MOQ_OK);
    return true;
}

static bool fx_objects(fx_t *f, uint64_t id, unsigned count)
{
    return fx_objects_of(f, id, count, true);
}

static bool fx_terminal(fx_t *f, uint64_t id, uint32_t reason, uint32_t domain,
                        uint64_t code)
{
    moq_media_track_event_t ev;
    FX_CHECK(fx_event(f, MOQ_MEDIA_TRACK_FETCH_TERMINAL, &ev));
    FX_CHECK(ev.initiating_request_id == id);
    FX_CHECK(ev.fill_reason == reason && ev.fill_error_domain == domain &&
             ev.fill_error_code == code);
    FX_CHECK(ev.fill_unreported_loss_causes == 0);   /* not an MPEG2TS track */
    return true;
}

static const char *fx_vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "d16" : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d22";
}

/* -- Tests ---------------------------------------------------------------- */

/* Created, then the fetch's Objects tagged FETCH with its request ID, then
 * exactly one COMPLETE terminal, and only after the last Object is polled.
 * Live counters stay untouched; the track's fetch counter counts them. */
static bool run_complete(moq_version_t version, bool passive)
{
    fx_t f;
    bool ok = fx_start(&f, version, passive, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK;
    ok = ok && fx_created(&f, &id);
    ok = ok && fx_accept(&f, 0) && fx_serve(&f, 0, 3, true);
    ok = ok && fx_pump(&f);
    /* The fetch has ended at the facade, but its Objects are queued. */
    ok = ok && fx_no_event(&f);
    ok = ok && f.track->fetch_objects_received == 3;
    ok = ok && fx_objects(&f, id, 3);
    ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_COMPLETE, MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_no_event(&f);
    /* The fetch is closed for the track's bound and its ID is gone. */
    ok = ok && moq_media_receiver_cancel_fetch(f.receiver, f.track, id) ==
               MOQ_ERR_STALE_HANDLE;
    ok = ok && moq_session_state(fx_client(&f)) == MOQ_SESS_ESTABLISHED;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    return ok;
}

static void test_complete(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                       MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; v < 3; v++) {
        bool ok = run_complete(versions[v], true);
        printf("%s: fetch_complete_passive_%s\n", ok ? "PASS" : "FAIL",
               fx_vname(versions[v]));
    }
    /* A forwarding track (draft 18: draft 22 would add its join fill). */
    bool ok = run_complete(MOQ_VERSION_DRAFT_18, false);
    printf("%s: fetch_complete_active_d18\n", ok ? "PASS" : "FAIL");
}

/* The peer refuses the FETCH: one PEER_ERROR terminal with its code. */
static void test_peer_error(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    ok = ok && fx_reject(&f, 0, MOQ_REQUEST_ERROR_DOES_NOT_EXIST) && fx_pump(&f);
    ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_PEER_ERROR,
                           MOQ_MEDIA_FETCH_ERROR_REQUEST,
                           MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
    ok = ok && fx_no_event(&f) && fx_objects(&f, id, 0);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_peer_error_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* cancel_fetch by ID: one CANCELLED terminal, after the Object already
 * queued; an unknown ID is STALE_HANDLE; the session stays up. */
static void test_cancel(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    ok = ok && fx_accept(&f, 0) && fx_serve(&f, 0, 1, false) && fx_pump(&f);
    ok = ok && moq_media_receiver_cancel_fetch(f.receiver, f.track, id + 1000) ==
               MOQ_ERR_STALE_HANDLE;
    ok = ok && moq_media_receiver_cancel_fetch(f.receiver, f.track, id) == MOQ_OK;
    /* Idempotent while pending. */
    ok = ok && moq_media_receiver_cancel_fetch(f.receiver, f.track, id) == MOQ_OK;
    ok = ok && fx_pump(&f) && fx_pump(&f);
    ok = ok && fx_no_event(&f);              /* its Object is still queued */
    ok = ok && fx_objects(&f, id, 1);
    ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_CANCELLED, MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_no_event(&f);
    ok = ok && moq_media_receiver_cancel_fetch(f.receiver, f.track, id) ==
               MOQ_ERR_STALE_HANDLE;
    ok = ok && moq_session_state(fx_client(&f)) == MOQ_SESS_ESTABLISHED;
    ok = ok && !moq_media_receiver_is_fatal(f.receiver);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_local_cancel_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* A track whose MSF-02 entry names a namespace of its own is fetched under
 * that namespace, as it is subscribed, not under the receiver's. */
static void test_track_namespace(moq_version_t version)
{
    static const char catalog[] =
        "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"v\",\"namespace\":\"other/ns\",\"packaging\":\"loc\","
        "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1,"
        "\"framerate\":30}]}";
    fx_t f;
    fx_catalog_json = catalog;
    bool ok = fx_start(&f, version, true, 0);
    fx_catalog_json = NULL;
    ok = ok && f.track->ns_override;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_pump(&f) && f.fetch_requests == 1;
    const fx_peer_fetch_t *p = &f.peer[0];
    ok = ok && p->named_v && p->ns_count == 2 &&
         p->ns_len[0] == 5 && memcmp(p->ns[0], "other", 5) == 0 &&
         p->ns_len[1] == 2 && memcmp(p->ns[1], "ns", 2) == 0;
    if (!ok)
        fprintf(stderr, "  fetch namespace: %zu fields, first \"%.*s\"\n", p->ns_count,
                (int)p->ns_len[0], p->ns[0]);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_track_namespace_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* A fetched Object's subgroup is not reported by the facade: its record says
 * so (has_subgroup false, subgroup_id 0) rather than naming a subgroup the
 * wire did not carry. The Object here was served on subgroup 3. */
static void test_subgroup_unknown(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id) && fx_accept(&f, 0);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = FX_GROUP;
    oc.subgroup_id = 3;
    oc.object_id = 0;
    oc.publisher_priority = 7;
    oc.payload = oc.properties = NULL;
    ok = ok && fx_buffers(&f, 9000, &oc.payload, &oc.properties);
    if (ok) {
        ok = moq_session_write_fetch_object(fx_server(&f), f.peer[0].fetch, &oc,
                                            fx_now(&f)) == MOQ_OK;
        moq_rcbuf_decref(oc.payload);
        moq_rcbuf_decref(oc.properties);
    }
    ok = ok && fx_network(&f) && fx_pump(&f);
    moq_media_object_t obj;
    memset(&obj, 0, sizeof(obj));
    bool got = ok && moq_media_receiver_poll_object(f.receiver, &obj, sizeof(obj)) == MOQ_OK;
    ok = got && obj.delivery_origin == MOQ_MEDIA_DELIVERY_FETCH &&
         obj.initiating_request_id == id && !obj.has_subgroup && obj.subgroup_id == 0;
    if (got) moq_media_object_cleanup(&obj);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_subgroup_unknown_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* On draft 22 a fetched Object is parsed against its own response's Track
 * Properties (FETCH_OK), never its subscription's: here FETCH_OK carries
 * VIDEO_CONFIG 11 22 33 and SUBSCRIBE_OK a4 00 b5. */
static void test_fetch_track_properties(void)
{
    static const uint8_t fetch_video[] = { 0x11, 0x22, 0x33 };
    fx_t f;
    bool ok = fx_start(&f, MOQ_VERSION_DRAFT_22, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    moq_rcbuf_t *props = NULL;
    if (ok) {
        moq_loc_metadata_t loc;
        moq_loc_metadata_init_sized(&loc, sizeof(loc));
        loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_TIMESCALE;
        loc.video_config = (moq_bytes_t){ fetch_video, sizeof(fetch_video) };
        loc.timescale = 1000;
        ok = moq_loc_encode_metadata(&f.alloc, MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_04,
                                     &loc, &props) == MOQ_OK;
    }
    if (ok) {
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init_sized(&ac, sizeof(ac));
        ac.end_group = f.peer[0].end_group;
        ac.end_object = f.peer[0].end_object;
        ac.track_properties = (moq_bytes_t){ moq_rcbuf_data(props), moq_rcbuf_len(props) };
        ok = moq_session_accept_fetch(fx_server(&f), f.peer[0].fetch, &ac,
                                      fx_now(&f)) == MOQ_OK;
    }
    if (props) moq_rcbuf_decref(props);
    ok = ok && fx_network(&f) && fx_serve(&f, 0, 1, true) && fx_pump(&f);
    moq_media_object_t obj;
    memset(&obj, 0, sizeof(obj));
    bool got = ok && moq_media_receiver_poll_object(f.receiver, &obj, sizeof(obj)) == MOQ_OK;
    const moq_loc_metadata_t *loc = got ? &obj.metadata.loc : NULL;
    ok = got && obj.delivery_origin == MOQ_MEDIA_DELIVERY_FETCH &&
         (loc->present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) &&
         loc->video_config.len == sizeof(fetch_video) &&
         memcmp(loc->video_config.data, fetch_video, sizeof(fetch_video)) == 0;
    if (got) moq_media_object_cleanup(&obj);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_track_properties_d22\n", ok ? "PASS" : "FAIL");
}

/* One unobserved command per track: WOULD_BLOCK until its FETCH_CREATED is
 * polled, by any output size; then the next command is accepted. */
static void test_would_block(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    ok = ok && fx_fetch(&f) == MOQ_OK;
    ok = ok && fx_fetch(&f) == MOQ_ERR_WOULD_BLOCK;
    ok = ok && fx_pump(&f) && f.fetch_requests == 1;
    /* Sent, its receipt queued but not polled. */
    ok = ok && fx_fetch(&f) == MOQ_ERR_WOULD_BLOCK;
    moq_media_track_event_t ev;
    memset(&ev, 0, sizeof(ev));
    /* The v0 prefix alone still consumes the receipt. */
    ok = ok && moq_media_receiver_poll_track(f.receiver, &ev,
                   MEDIA_TRACK_EVENT_V0_SIZE) == MOQ_OK &&
              ev.kind == MOQ_MEDIA_TRACK_FETCH_CREATED;
    ok = ok && fx_fetch(&f) == MOQ_OK;
    uint64_t second = 0;
    ok = ok && fx_created(&f, &second);
    /* Two fetches of the track are open at once, each with its own ID. */
    ok = ok && receiver_open_fetches_locked(f.receiver, f.track) == 2;
    ok = ok && f.receiver->fill_doms[RECEIVER_FILL_DOMS].initiating_request_id !=
               f.receiver->fill_doms[RECEIVER_FILL_DOMS + 1].initiating_request_id;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_would_block_until_created_%s\n", ok ? "PASS" : "FAIL",
           fx_vname(version));
}

/* max_fetches_per_track open fetches: the next call is MOQ_ERR_CAPACITY until
 * one of them ends and its terminal is polled. */
static void test_limit(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 2);
    uint64_t a = 0, b = 0, c = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &a);
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &b);
    ok = ok && a != b;
    ok = ok && fx_fetch(&f) == MOQ_ERR_CAPACITY;
    /* The first ends: still open until its terminal is polled. */
    ok = ok && fx_reject(&f, 0, MOQ_REQUEST_ERROR_INVALID_RANGE) && fx_pump(&f);
    ok = ok && fx_fetch(&f) == MOQ_ERR_CAPACITY;
    ok = ok && fx_terminal(&f, a, MOQ_MEDIA_FETCH_PEER_ERROR,
                           MOQ_MEDIA_FETCH_ERROR_REQUEST, MOQ_REQUEST_ERROR_INVALID_RANGE);
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &c);
    ok = ok && c != a && c != b;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_limit_per_track_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* withdraw_fetch before the executor takes the command: nothing is sent and
 * no event follows; after it was sent, WRONG_STATE. */
static void test_withdraw(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    ok = ok && moq_media_receiver_withdraw_fetch(f.receiver, f.track) == MOQ_DONE;
    ok = ok && fx_fetch(&f) == MOQ_OK;
    ok = ok && moq_media_receiver_withdraw_fetch(f.receiver, f.track) == MOQ_OK;
    ok = ok && moq_media_receiver_withdraw_fetch(f.receiver, f.track) == MOQ_DONE;
    ok = ok && fx_pump(&f) && fx_pump(&f);
    ok = ok && f.fetch_requests == 0 && fx_no_event(&f);
    /* The slot is free again; once sent, the command cannot be withdrawn. */
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_pump(&f) && f.fetch_requests == 1;
    ok = ok && moq_media_receiver_withdraw_fetch(f.receiver, f.track) ==
               MOQ_ERR_WRONG_STATE;
    moq_media_track_event_t ev;
    ok = ok && fx_event(&f, MOQ_MEDIA_TRACK_FETCH_CREATED, &ev);
    ok = ok && moq_media_receiver_withdraw_fetch(f.receiver, f.track) == MOQ_DONE;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_withdraw_before_send_%s\n", ok ? "PASS" : "FAIL",
           fx_vname(version));
}

/* The session closes with a fetch open: exactly one SESSION_CLOSED
 * terminal, after its delivered Object. */
static void test_session_close(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    ok = ok && fx_accept(&f, 0) && fx_serve(&f, 0, 1, false) && fx_pump(&f);
    ok = ok && moq_session_close(fx_server(&f), 0, NULL, fx_now(&f)) == MOQ_OK;
    ok = ok && fx_network(&f) && fx_pump(&f) && fx_pump(&f);
    ok = ok && moq_session_state(fx_client(&f)) == MOQ_SESS_CLOSED;
    ok = ok && fx_no_event(&f);              /* its Object is still queued */
    ok = ok && fx_objects(&f, id, 1);
    ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_SESSION_CLOSED,
                           MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_pump(&f) && fx_no_event(&f);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_session_closed_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* The catalog subscription is refused with a fetch open, then the session
 * closes. Whether the open fetch still gets a terminal is decided in
 * receiver_on_subscribe_error: a refusal while the catalog is still being
 * bootstrapped goes through receiver_bootstrap_fail, which sets
 * catalog_ack_failed, so the close path (receiver_close_fetches) ends the
 * fetch with exactly one SESSION_CLOSED; a refusal after the catalog took
 * effect is an ordinary fatal (CATALOG_REJECTED) and synthesizes no
 * terminal, as for fills. In this fixture the draft 22 catalog is still
 * bootstrapping (its joining fetch) at the refusal and the draft 16/18 one
 * has taken effect: the difference is the fixture's timing, not a protocol
 * rule. */
static void test_catalog_refused_then_close(moq_version_t version, bool bootstrapping)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && f.catalog_seen;
    ok = ok && f.receiver->catalog_bootstrap.active == bootstrapping;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    ok = ok && fx_accept(&f, 0) && fx_pump(&f);
    moq_reject_subscribe_cfg_t reject;
    moq_reject_subscribe_cfg_init(&reject);
    reject.error_code = 0;
    ok = ok && moq_session_reject_subscribe(fx_server(&f), f.catalog_sub, &reject,
                                            fx_now(&f)) == MOQ_OK;
    ok = ok && fx_network(&f) && fx_pump(&f);
    ok = ok && moq_media_receiver_is_fatal(f.receiver);
    ok = ok && f.receiver->catalog_ack_failed == bootstrapping;
    ok = ok && moq_session_close(fx_server(&f), 0, NULL, fx_now(&f)) == MOQ_OK;
    ok = ok && fx_network(&f) && fx_pump(&f) && fx_pump(&f);
    ok = ok && moq_session_state(fx_client(&f)) == MOQ_SESS_CLOSED;
    if (bootstrapping)
        ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_SESSION_CLOSED,
                               MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_pump(&f) && fx_no_event(&f);
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_catalog_refused_%s_then_close_%s\n", ok ? "PASS" : "FAIL",
           bootstrapping ? "while_bootstrapping" : "after_effect", fx_vname(version));
}

/* The peer resets the fetch's data stream after two Objects: one RESET
 * terminal with the stream code, after those Objects. */
static void test_reset(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t id = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &id);
    ok = ok && fx_accept(&f, 0) && fx_serve(&f, 0, 2, false) && fx_pump(&f);
    ok = ok && moq_session_abort_fetch(fx_server(&f), f.peer[0].fetch, 7,
                                       fx_now(&f)) == MOQ_OK;
    ok = ok && fx_network(&f) && fx_pump(&f);
    ok = ok && fx_no_event(&f);              /* its Objects are still queued */
    ok = ok && fx_objects(&f, id, 2);
    ok = ok && fx_terminal(&f, id, MOQ_MEDIA_FETCH_RESET, MOQ_MEDIA_FETCH_ERROR_STREAM, 7);
    ok = ok && fx_no_event(&f);
    ok = ok && moq_session_state(fx_client(&f)) == MOQ_SESS_ESTABLISHED;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_peer_reset_%s\n", ok ? "PASS" : "FAIL", fx_vname(version));
}

/* The credential policy denies the FETCH: nothing is sent, the command's
 * receipt is a NOT_SENT FETCH_TERMINAL (ID zero, local_result MOQ_DONE),
 * the outcome is request-scoped, and the track and its slot stay usable. */
static void test_not_sent(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start_auth(&f, version, true, 0, true);
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_pump(&f) && fx_pump(&f);
    ok = ok && f.fetch_requests == 0;
    moq_media_track_event_t ev;
    ok = ok && fx_event(&f, MOQ_MEDIA_TRACK_FETCH_TERMINAL, &ev);
    ok = ok && ev.initiating_request_id == 0 &&
         ev.fill_reason == MOQ_MEDIA_FETCH_NOT_SENT &&
         ev.fill_error_domain == MOQ_MEDIA_FETCH_ERROR_NONE &&
         ev.fill_error_code == 0 && ev.local_result == MOQ_DONE;
    ok = ok && fx_no_event(&f);
    ok = ok && f.outcomes == 1 && f.outcome_scope == MOQ_MEDIA_AUTH_SCOPE_REQUEST &&
         f.outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED && f.outcome_track == f.track;
    moq_media_track_state_t state;
    ok = ok && moq_media_receiver_track_state(f.receiver, f.track, &state) == MOQ_OK &&
         state != MOQ_MEDIA_TRACK_STATE_FAILED && !f.track->auth_failed;
    /* The slot is free again: the next command is recorded (and refused). */
    ok = ok && fx_fetch(&f) == MOQ_OK;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_not_sent_policy_denied_%s\n", ok ? "PASS" : "FAIL",
           fx_vname(version));
}

/* Two fetches of one track in flight; the second created completes first.
 * Each Object carries its own fetch's ID and each fetch gets its own
 * terminal, the open one unaffected by the other's end. */
static void test_two_in_flight(moq_version_t version)
{
    fx_t f;
    bool ok = fx_start(&f, version, true, 0);
    uint64_t a = 0, b = 0;
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &a);
    ok = ok && fx_fetch(&f) == MOQ_OK && fx_created(&f, &b);
    ok = ok && a != b;
    ok = ok && fx_accept(&f, 0) && fx_accept(&f, 1);
    /* B first. */
    ok = ok && fx_serve(&f, 1, 2, true) && fx_pump(&f);
    ok = ok && fx_no_event(&f);
    ok = ok && fx_objects(&f, b, 2);
    ok = ok && fx_terminal(&f, b, MOQ_MEDIA_FETCH_COMPLETE, MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_no_event(&f);
    ok = ok && receiver_open_fetches_locked(f.receiver, f.track) == 1;
    /* Then A, still open. */
    ok = ok && fx_serve(&f, 0, 3, true) && fx_pump(&f);
    ok = ok && fx_no_event(&f);
    ok = ok && fx_objects(&f, a, 3);
    ok = ok && fx_terminal(&f, a, MOQ_MEDIA_FETCH_COMPLETE, MOQ_MEDIA_FETCH_ERROR_NONE, 0);
    ok = ok && fx_no_event(&f);
    ok = ok && f.track->fetch_objects_received == 5;
    fx_finish(&f);
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_two_in_flight_reverse_order_%s\n", ok ? "PASS" : "FAIL",
           fx_vname(version));
}

/* Synchronous refusals, which record nothing. */
static void test_refusals(void)
{
    fx_t f;
    bool ok = fx_start(&f, MOQ_VERSION_DRAFT_18, true, 0);
    moq_media_fetch_cfg_t cfg;
    moq_media_fetch_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.start_group = 4;
    cfg.end_group = 3;
    ok = ok && moq_media_receiver_fetch_track(f.receiver, f.track, &cfg) == MOQ_ERR_INVAL;
    cfg.end_group = 4;
    cfg.start_object = 2;
    cfg.end_object = 2;           /* an empty range */
    ok = ok && moq_media_receiver_fetch_track(f.receiver, f.track, &cfg) == MOQ_ERR_INVAL;
    cfg.end_object = 3;
    cfg.struct_size = (uint32_t)(MOQ_MEDIA_FETCH_CFG_V0_SIZE - 1);
    ok = ok && moq_media_receiver_fetch_track(f.receiver, f.track, &cfg) ==
               MOQ_ERR_ABI_MISMATCH;
    cfg.struct_size = (uint32_t)MOQ_MEDIA_FETCH_CFG_V0_SIZE;
    ok = ok && moq_media_receiver_fetch_track(NULL, f.track, &cfg) == MOQ_ERR_INVAL;
    ok = ok && moq_media_receiver_fetch_track(f.receiver, f.track, NULL) == MOQ_ERR_INVAL;
    ok = ok && moq_media_receiver_unsubscribe_track(f.receiver, f.track) == MOQ_OK;
    ok = ok && moq_media_receiver_fetch_track(f.receiver, f.track, &cfg) ==
               MOQ_ERR_WRONG_STATE;
    ok = ok && f.track->fetch_cmd_state == FETCH_CMD_NONE;
    ok = ok && fx_pump(&f) && f.fetch_requests == 0;
    fx_finish(&f);

    /* The initializers: the pointer-only form stamps the frozen floor, the
     * sized form min(size, sizeof); a size that cannot hold struct_size is a
     * no-op. */
    moq_media_fetch_cfg_t c;
    memset(&c, 0xa5, sizeof(c));
    moq_media_fetch_cfg_init(&c);
    ok = ok && c.struct_size == MOQ_MEDIA_FETCH_CFG_V0_SIZE && c.start_group == 0 &&
         !c.has_subscriber_priority && c.subscriber_priority == 0;
    memset(&c, 0xa5, sizeof(c));
    moq_media_fetch_cfg_init_sized(&c, sizeof(c) + 8);
    ok = ok && c.struct_size == sizeof(c) && c.end_object == 0;
    memset(&c, 0xa5, sizeof(c));
    moq_media_fetch_cfg_init_sized(&c, sizeof(uint32_t) - 1);
    ok = ok && c.struct_size == 0xa5a5a5a5u;
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_synchronous_refusals_and_init\n", ok ? "PASS" : "FAIL");
}

/* The receiver configuration's appended bound: a short struct_size leaves
 * it at the default 4; a covered zero is 4 as well. */
static void test_cfg_bound(void)
{
    static moq_bytes_t ns[] = { { .data = (const uint8_t *)"x", .len = 1 } };
    bool ok = true;
    for (int row = 0; row < 3; row++) {
        test_alloc_state_t ledger = {0};
        moq_alloc_t alloc = test_allocator(&ledger);
        moq_media_receiver_cfg_t cfg;
        moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
        cfg.namespace_ = (moq_namespace_t){ ns, 1 };
        cfg.max_fetches_per_track = row == 2 ? 0 : 7;
        if (row == 1)
            cfg.struct_size = (uint32_t)offsetof(moq_media_receiver_cfg_t,
                                                 max_fetches_per_track);
        moq_media_receiver_test_set_alloc(&alloc);
        moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
        moq_media_receiver_test_set_alloc(NULL);
        ok = ok && r != NULL;
        if (r) {
            ok = ok && r->max_fetches_per_track == (row == 0 ? 7u : 4u);
            moq_media_receiver_test_free(r);
        }
        ok = ok && ledger.balance == 0;
    }
    MOQ_TEST_CHECK(ok);
    printf("%s: fetch_cfg_max_fetches_per_track\n", ok ? "PASS" : "FAIL");
}

int main(void)
{
    test_cfg_bound();
    test_refusals();
    test_complete();
    test_peer_error(MOQ_VERSION_DRAFT_18);
    test_peer_error(MOQ_VERSION_DRAFT_22);
    test_cancel(MOQ_VERSION_DRAFT_18);
    test_cancel(MOQ_VERSION_DRAFT_22);
    test_would_block(MOQ_VERSION_DRAFT_18);
    test_would_block(MOQ_VERSION_DRAFT_22);
    test_limit(MOQ_VERSION_DRAFT_18);
    test_limit(MOQ_VERSION_DRAFT_22);
    test_withdraw(MOQ_VERSION_DRAFT_18);
    test_withdraw(MOQ_VERSION_DRAFT_22);
    test_session_close(MOQ_VERSION_DRAFT_18);
    test_session_close(MOQ_VERSION_DRAFT_22);
    test_catalog_refused_then_close(MOQ_VERSION_DRAFT_18, false);
    test_catalog_refused_then_close(MOQ_VERSION_DRAFT_22, true);
    test_reset(MOQ_VERSION_DRAFT_18);
    test_reset(MOQ_VERSION_DRAFT_22);
    test_not_sent(MOQ_VERSION_DRAFT_18);
    test_not_sent(MOQ_VERSION_DRAFT_22);
    test_two_in_flight(MOQ_VERSION_DRAFT_18);
    test_two_in_flight(MOQ_VERSION_DRAFT_22);
    test_track_namespace(MOQ_VERSION_DRAFT_18);
    test_track_namespace(MOQ_VERSION_DRAFT_22);
    test_subgroup_unknown(MOQ_VERSION_DRAFT_18);
    test_subgroup_unknown(MOQ_VERSION_DRAFT_22);
    test_fetch_track_properties();
    printf("receiver_fetch: %d failures\n", failures);
    return failures ? 1 : 0;
}
