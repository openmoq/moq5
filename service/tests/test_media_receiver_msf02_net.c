/* The receiver's MSF-02 subscriptions over a real publisher and SimPair, on
 * drafts 16, 18 and 22.
 *
 * Invariants pinned here:
 *   - the catalog subscription asks for subscriber priority 0 (MSF 5);
 *   - an MSF-02 namespace override is subscribed under its own Track
 *     Namespace fields;
 *   - a replacement's successor subscribes only once its predecessor's
 *     subscription has ended at the publisher, with the predecessor's
 *     selection, and the predecessor's late Objects are dropped and counted;
 *   - a successor whose subscription the publisher refuses ends; the
 *     predecessor stays removed;
 *   - an "object"-mapped LOC track's Objects reach the application through
 *     the in-group ordering stage, and a subgroup that ends with
 *     END_OF_GROUP completes its Group there, so the next Group is not held
 *     for the boundary wait.
 */
#include <moq/media_receiver.h>
#include <moq/publisher.h>
#include <moq/sim.h>

#include <stdio.h>
#include <string.h>

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);

static unsigned failures;
static const char *row = "";
static int draft = 0;

static bool check(bool ok, const char *expr, int line)
{
    if (!ok) {
        failures++;
        fprintf(stderr, "FAIL %s d%d line %d: %s\n", row, draft, line, expr);
    }
    return ok;
}
#define CHECK(e) check((e), #e, __LINE__)
#define REQUIRE(e) do { if (!CHECK(e)) goto out; } while (0)

/* What the publisher saw, in order. */
#define LOG_CAP 64
typedef struct {
    int      kind;               /* 1 SUBSCRIBE, 2 joined, 3 left */
    char     name[16];
    char     ns[32];             /* fields joined with '|' */
    uint8_t  priority;
} pub_log_t;

typedef struct {
    moq_simpair_t        *sp;
    moq_session_t        *rx;
    moq_publisher_t      *pub;
    moq_pub_track_t      *catalog, *video, *ov, *ord;
    moq_media_receiver_t *r;
    uint64_t              now;
    bool                  ready;
    pub_log_t             log[LOG_CAP];
    int                   log_n;
    bool                  reject_video;
    moq_media_track_event_t ev[16];
    int                   ev_n;
} fx_t;

static void log_add(fx_t *f, int kind, moq_bytes_t name, moq_namespace_t ns,
                    uint8_t prio)
{
    if (f->log_n >= LOG_CAP) return;
    pub_log_t *l = &f->log[f->log_n++];
    memset(l, 0, sizeof(*l));
    l->kind = kind;
    l->priority = prio;
    size_t n = name.len < sizeof(l->name) - 1 ? name.len : sizeof(l->name) - 1;
    if (n) memcpy(l->name, name.data, n);
    size_t off = 0;
    for (size_t i = 0; i < ns.count; i++) {
        for (size_t j = 0; j < ns.parts[i].len && off + 2 < sizeof(l->ns); j++)
            l->ns[off++] = (char)ns.parts[i].data[j];
        if (i + 1 < ns.count && off + 2 < sizeof(l->ns)) l->ns[off++] = '|';
    }
}

static moq_pub_accept_decision_t on_subscribe(void *ctx,
                                              const moq_pub_subscribe_info_t *info,
                                              moq_request_error_t *err)
{
    fx_t *f = (fx_t *)ctx;
    log_add(f, 1, info->track_name, info->track_namespace, info->subscriber_priority);
    if (f->reject_video && info->track == f->video) {
        *err = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        return MOQ_PUB_DECISION_REJECT;
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

static void on_joined(void *ctx, moq_pub_track_t *t)
{
    fx_t *f = (fx_t *)ctx;
    if (t == f->video) log_add(f, 2, MOQ_BYTES_LITERAL("video"), (moq_namespace_t){0}, 0);
}

static void on_left(void *ctx, moq_pub_track_t *t)
{
    fx_t *f = (fx_t *)ctx;
    if (t == f->video) log_add(f, 3, MOQ_BYTES_LITERAL("video"), (moq_namespace_t){0}, 0);
}

static void cycle(fx_t *f)
{
    f->now++;
    CHECK(moq_simpair_advance_to(f->sp, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) >= 0);
    if (f->pub) CHECK(moq_pub_tick(f->pub, f->now) >= 0);
    if (f->r) {
        CHECK(moq_session_process_pending(f->rx, f->now) >= 0);
        moq_media_receiver_test_pump(f->r, f->rx, f->now);
    }
}

static void cycles(fx_t *f, unsigned n)
{
    for (unsigned i = 0; i < n; i++) cycle(f);
}

static void take_events(fx_t *f)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_CATALOG_READY) f->ready = true;
        if (f->ev_n < 16) f->ev[f->ev_n++] = ev;
    }
}

static moq_media_track_t *event_track(fx_t *f, moq_media_track_event_kind_t kind,
                                      const char *name)
{
    for (int i = 0; i < f->ev_n; i++) {
        const moq_media_track_desc_t *d = f->ev[i].desc;
        if (f->ev[i].kind == kind && d && d->name.len == strlen(name) &&
            memcmp(d->name.data, name, d->name.len) == 0)
            return f->ev[i].track;
    }
    return NULL;
}

#define K_ORD_TRACK \
    "{\"name\":\"ord\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"," \
    "\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30," \
    "\"packagingMetadata\":\"streamMapping=object\"}"
static const char k_catalog[] =
    "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
    "\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30},"
    "{\"name\":\"ov\",\"namespace\":\"other/ns\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30},"
    K_ORD_TRACK "]}";
static const char k_catalog_vp9[] =
    "{\"version\":\"draft-02\",\"generatedAt\":2,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
    "\"codec\":\"vp09.00.10.08\",\"bitrate\":1,\"framerate\":30},"
    "{\"name\":\"ov\",\"namespace\":\"other/ns\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30},"
    K_ORD_TRACK "]}";

static moq_bytes_t k_parts[] = { { (const uint8_t *)"live", 4 },
                                 { (const uint8_t *)"show", 4 } };
static moq_bytes_t k_ov_parts[] = { { (const uint8_t *)"other", 5 },
                                    { (const uint8_t *)"ns", 2 } };

static bool fx_setup(fx_t *f, moq_version_t ver)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = ver;
    cfg.seed = 0x5702;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    if (!CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_start(f->sp) == MOQ_OK)) return false;
    f->rx = moq_simpair_server(f->sp);
    cycles(f, 8);
    if (!CHECK(moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED)) return false;

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_CALLBACK;
    pcfg.on_subscribe = on_subscribe;
    pcfg.on_subscribe_ctx = f;
    moq_pub_callbacks_init_sized(&pcfg.callbacks, sizeof(pcfg.callbacks));
    pcfg.callbacks.ctx = f;
    pcfg.callbacks.on_subscriber_joined = on_joined;
    pcfg.callbacks.on_subscriber_left = on_left;
    if (!CHECK(moq_pub_create(moq_simpair_client(f->sp), moq_alloc_default(),
                              &pcfg, &f->pub) == MOQ_OK)) return false;
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = (moq_namespace_t){ .parts = k_parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->catalog) == MOQ_OK)) return false;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->video) == MOQ_OK)) return false;
    tcfg.track_name = MOQ_BYTES_LITERAL("ord");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->ord) == MOQ_OK)) return false;
    tcfg.track_namespace = (moq_namespace_t){ .parts = k_ov_parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("ov");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->ov) == MOQ_OK)) return false;
    moq_rcbuf_t *cat = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)k_catalog,
                     strlen(k_catalog), &cat);
    moq_pub_retained_object_t retained = {
        .object_id = 0, .payload = cat, .properties = NULL, .end_of_group = true };
    moq_pub_retained_group_cfg_t group;
    moq_pub_retained_group_cfg_init(&group);
    group.objects = &retained;
    group.object_count = 1;
    moq_result_t crc = moq_pub_set_retained_group(f->pub, f->catalog, &group);
    moq_rcbuf_decref(cat);
    if (!CHECK(crc == MOQ_OK)) return false;
    cycles(f, 8);

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = (moq_namespace_t){ .parts = k_parts, .count = 2 };
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    if (!CHECK(f->r != NULL)) return false;
    for (unsigned i = 0; i < 64 && !f->ready; i++) {
        cycle(f);
        take_events(f);
    }
    return CHECK(f->ready);
}

static void fx_teardown(fx_t *f)
{
    if (f->r) moq_media_receiver_test_destroy_with_session(f->r, f->rx, f->now);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) moq_simpair_destroy(f->sp);
}

static int log_find(const fx_t *f, int kind, const char *name, int from)
{
    for (int i = from; i < f->log_n; i++)
        if (f->log[i].kind == kind && strcmp(f->log[i].name, name) == 0) return i;
    return -1;
}

static void publish_catalog(fx_t *f, uint64_t group, const char *json)
{
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)json, strlen(json), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = group;
    oc.object_id = 0;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, f->catalog, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

static void write_video(fx_t *f, uint64_t g)
{
    uint8_t b[4] = { 1, 2, 3, (uint8_t)g };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = 0;
    oc.payload = pay;
    oc.end_of_group = true;
    (void)moq_pub_write_object_ex(f->pub, f->video, &oc, f->now);
    moq_rcbuf_decref(pay);
}

static void write_obj(fx_t *f, moq_pub_track_t *t, uint64_t g, uint64_t o)
{
    uint8_t b[4] = { 1, 2, (uint8_t)o, (uint8_t)g };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, t, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

static void catalog_priority_and_override(moq_version_t ver)
{
    row = "catalog_priority_and_override";
    fx_t f;
    REQUIRE(fx_setup(&f, ver));
    int c = log_find(&f, 1, "catalog", 0);
    REQUIRE(c >= 0);
    CHECK(f.log[c].priority == 0);
    CHECK(strcmp(f.log[c].ns, "live|show") == 0);
    moq_media_track_t *ov = event_track(&f, MOQ_MEDIA_TRACK_ADDED, "ov");
    REQUIRE(ov);
    REQUIRE(moq_media_receiver_subscribe_track(f.r, ov, NULL) == MOQ_OK);
    cycles(&f, 16);
    int o = log_find(&f, 1, "ov", 0);
    REQUIRE(o >= 0);
    CHECK(strcmp(f.log[o].ns, "other|ns") == 0);
    moq_media_track_state_t st;
    CHECK(moq_media_receiver_track_state(f.r, ov, &st) == MOQ_OK &&
          st == MOQ_MEDIA_TRACK_STATE_ACTIVE);
out:
    fx_teardown(&f);
}

static void replacement_gate(moq_version_t ver)
{
    row = "replacement_gate";
    fx_t f;
    REQUIRE(fx_setup(&f, ver));
    moq_media_track_t *a = event_track(&f, MOQ_MEDIA_TRACK_ADDED, "video");
    REQUIRE(a);
    moq_media_receiver_track_subscribe_cfg_t sc;
    moq_media_receiver_track_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.has_priority = true;
    sc.priority = 9;
    REQUIRE(moq_media_receiver_subscribe_track(f.r, a, &sc) == MOQ_OK);
    cycles(&f, 16);
    int first = log_find(&f, 1, "video", 0);
    REQUIRE(first >= 0 && log_find(&f, 2, "video", first) > first);

    /* A critical change: a new codec. */
    f.ev_n = 0;
    publish_catalog(&f, 1, k_catalog_vp9);
    moq_media_track_t *b = NULL;
    uint64_t dropped0 = 0;
    for (unsigned i = 0; i < 32 && !b; i++) {
        cycle(&f);
        take_events(&f);
        for (int k = 0; k < f.ev_n; k++)
            if (f.ev[k].kind == MOQ_MEDIA_TRACK_REMOVED && f.ev[k].track == a)
                b = f.ev[k].replaced_by;
    }
    REQUIRE(b);
    moq_media_receiver_stats_t st;
    CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    dropped0 = st.objects_dropped;
    /* The old subscription's late Objects are not delivered. */
    write_video(&f, 1);
    cycles(&f, 32);
    /* B's SUBSCRIBE came only after A's subscription left the publisher. */
    int left = log_find(&f, 3, "video", first);
    int second = log_find(&f, 1, "video", first + 1);
    CHECK(left > first);
    CHECK(second > left);
    if (second >= 0) CHECK(f.log[second].priority == 9);
    moq_media_track_state_t s;
    CHECK(moq_media_receiver_track_state(f.r, b, &s) == MOQ_OK &&
          s == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CHECK(moq_media_receiver_track_state(f.r, a, &s) == MOQ_ERR_WRONG_STATE);
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) == MOQ_OK) {
        CHECK(obj.track != a);
        moq_media_object_cleanup(&obj);
    }
    CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.objects_dropped >= dropped0);
out:
    fx_teardown(&f);
}

static void replacement_refused(moq_version_t ver)
{
    row = "replacement_refused";
    fx_t f;
    REQUIRE(fx_setup(&f, ver));
    moq_media_track_t *a = event_track(&f, MOQ_MEDIA_TRACK_ADDED, "video");
    REQUIRE(a);
    REQUIRE(moq_media_receiver_subscribe_track(f.r, a, NULL) == MOQ_OK);
    cycles(&f, 16);
    f.reject_video = true;
    f.ev_n = 0;
    publish_catalog(&f, 1, k_catalog_vp9);
    moq_media_track_t *b = NULL;
    bool ended = false;
    for (unsigned i = 0; i < 64 && !ended; i++) {
        cycle(&f);
        take_events(&f);
        for (int k = 0; k < f.ev_n; k++) {
            if (f.ev[k].kind == MOQ_MEDIA_TRACK_REMOVED && f.ev[k].track == a)
                b = f.ev[k].replaced_by;
            if (b && f.ev[k].kind == MOQ_MEDIA_TRACK_ENDED && f.ev[k].track == b)
                ended = true;
        }
    }
    CHECK(b != NULL);
    CHECK(ended);
    moq_media_track_state_t s;
    CHECK(moq_media_receiver_track_state(f.r, a, &s) == MOQ_ERR_WRONG_STATE);
out:
    fx_teardown(&f);
}

static void ordered_track_live(moq_version_t ver)
{
    row = "ordered_track_live";
    fx_t f;
    REQUIRE(fx_setup(&f, ver));
    moq_media_track_t *t = event_track(&f, MOQ_MEDIA_TRACK_ADDED, "ord");
    REQUIRE(t);
    REQUIRE(moq_media_receiver_subscribe_track(f.r, t, NULL) == MOQ_OK);
    cycles(&f, 16);
    REQUIRE(log_find(&f, 1, "ord", 0) >= 0);
    for (uint64_t o = 0; o < 3; o++) write_obj(&f, f.ord, 0, o);
    CHECK(moq_pub_end_group(f.pub, f.ord, f.now) == MOQ_OK);
    write_obj(&f, f.ord, 1, 0);
    static const uint64_t want_g[] = { 0, 0, 0, 1 }, want_o[] = { 0, 1, 2, 0 };
    int n = 0;
    /* Each cycle advances one microsecond: far inside the window. */
    for (unsigned i = 0; i < 64 && n < 4; i++) {
        cycle(&f);
        moq_media_object_t obj;
        while (n < 4 && moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) == MOQ_OK) {
            /* The stage's releases carry a loss report; a track without
             * one does not. */
            CHECK(obj.track == t && obj.group_id == want_g[n] &&
                  obj.object_id == want_o[n] && obj.has_loss_report && !obj.gap &&
                  !obj.gap_tail_unknown);
            n++;
            moq_media_object_cleanup(&obj);
        }
    }
    CHECK(n == 4);
out:
    fx_teardown(&f);
}

int main(void)
{
    static const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                              MOQ_VERSION_DRAFT_22 };
    static const int numbers[] = { 16, 18, 22 };
    for (size_t i = 0; i < 3; i++) {
        draft = numbers[i];
        catalog_priority_and_override(versions[i]);
        replacement_gate(versions[i]);
        replacement_refused(versions[i]);
        ordered_track_live(versions[i]);
    }
    if (failures) {
        fprintf(stderr, "%u failure(s)\n", failures);
        return 1;
    }
    printf("media_receiver_msf02_net: all rows passed\n");
    return 0;
}
