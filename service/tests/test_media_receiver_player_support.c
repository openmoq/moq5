/* The receiver surfaces a player is built on, over a real publisher and
 * SimPair, on drafts 16, 18 and 22.
 *
 * Pinned:
 *   - get_info reports the effective configuration, the resolved companion
 *     and draft once a session is seen, and counts the track events polled;
 *   - poll_object_within transfers the head only when its whole charge
 *     fits, and otherwise dequeues nothing and reports the charge; the
 *     charge is moq_media_object_charge of the polled object;
 *   - TRACK_ENDED says whether a done ended the track, with its status; an
 *     END_OF_TRACK end has none; a caller struct that does not cover the
 *     block gets none;
 *   - the descriptor carries the catalog's renderGroup and whether it had
 *     a label.
 */
#include <moq/media_receiver.h>
#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/loc.h>

#include <stdio.h>
#include <string.h>

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_free(moq_media_receiver_t *r);

static unsigned failures;
static const char *row;

static bool check(bool ok, const char *expr, int line)
{
    if (!ok) {
        failures++;
        fprintf(stderr, "FAIL %s line %d: %s\n", row, line, expr);
    }
    return ok;
}
#define CHECK(e) check((e), #e, __LINE__)
#define REQUIRE(e) do { if (!CHECK(e)) goto out; } while (0)

static const char k_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\","
    "\"renderGroup\":3,\"label\":\"\"}]}";
static const char *g_catalog = k_catalog;
typedef struct {
    moq_simpair_t        *sp;
    moq_session_t        *rx;
    moq_publisher_t      *pub;
    moq_pub_track_t      *catalog, *video, *side;
    moq_media_receiver_t *r;
    moq_media_track_t    *track, *side_track;
    uint64_t              now;
    bool                  ready;
} fx_t;


/* One step of everything except the application's receiver polls. */
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

static void track_events(fx_t *f)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc->name.len == 4)
            f->side_track = ev.track;
        else if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f->track = ev.track;
        else if (ev.kind == MOQ_MEDIA_CATALOG_READY) f->ready = true;
    }
}

/* `lane`: 0 off, 1 on, 2 on but outside the caller's struct_size. */
static bool fx_setup(fx_t *f, moq_version_t ver, moq_media_overflow_policy_t policy,
                     uint32_t max_objects, int lane, uint32_t max_terminals)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = ver;
    cfg.seed = 0x5713;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    if (!CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_start(f->sp) == MOQ_OK)) return false;
    f->rx = moq_simpair_server(f->sp);
    cycles(f, 8);
    if (!CHECK(moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED)) return false;

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (!CHECK(moq_pub_create(moq_simpair_client(f->sp), moq_alloc_default(),
                              &pcfg, &f->pub) == MOQ_OK)) return false;
    static moq_bytes_t parts[] = { { (const uint8_t *)"lane", 4 },
                                   { (const uint8_t *)"test", 4 } };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->catalog) == MOQ_OK))
        return false;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->video) == MOQ_OK))
        return false;
    bool mixed = false;
    tcfg.track_name = MOQ_BYTES_LITERAL("side");
    if (mixed && !CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->side) == MOQ_OK))
        return false;
    moq_rcbuf_t *cat = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)g_catalog,
                     strlen(g_catalog), &cat);
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
    rcfg.namespace_ = tcfg.track_namespace;
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = policy;
    rcfg.overflow.max_objects = max_objects;
    rcfg.overflow.max_bytes = 1u << 20;
    rcfg.surface_stream_terminals = lane != 0;
    rcfg.max_stream_terminals = max_terminals;
    if (lane == 2)
        rcfg.struct_size = (uint32_t)offsetof(moq_media_receiver_cfg_t,
                                              _reserved_companion);
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    if (!CHECK(f->r != NULL)) return false;
    for (unsigned i = 0; i < 64 && !f->ready; i++) {
        cycle(f);
        track_events(f);
    }
    if (!CHECK(f->ready && f->track && (!mixed || f->side_track))) return false;
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    if (!CHECK(moq_media_receiver_subscribe_track(f->r, f->track, &options) == MOQ_OK))
        return false;
    if (mixed && !CHECK(moq_media_receiver_subscribe_track(f->r, f->side_track,
                                                           &options) == MOQ_OK))
        return false;
    cycles(f, 24);
    return true;
}

/* The receiver's subscriber facade is torn down through the session, as
 * the endpoint does; freeing the receiver alone would leave it behind. */
static void fx_teardown(fx_t *f)
{
    if (f->r) moq_media_receiver_test_destroy_with_session(f->r, f->rx, f->now);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) moq_simpair_destroy(f->sp);
}

static void write_obj(fx_t *f, uint64_t g, uint64_t o, bool eog)
{
    uint8_t b[8] = { (uint8_t)g, (uint8_t)o, 1, 2, 3, 4, 5, 6 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.end_of_group = eog;
    CHECK(moq_pub_write_object_ex(f->pub, f->video, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}





static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}



/* The effective configuration, the resolved companion and draft, and the
 * count of track events polled. */
static void test_info(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "info_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 0, 1, 0));
    moq_media_receiver_info_t info;
    memset(&info, 0xEE, sizeof(info));
    REQUIRE(moq_media_receiver_get_info(f.r, &info, sizeof(info)) == MOQ_OK);
    CHECK(info.struct_size == sizeof(info));
    CHECK(info.time_mode == MOQ_MEDIA_TIME_RAW);
    CHECK(info.overflow_policy == MOQ_MEDIA_OVERFLOW_DROP_GROUP);
    CHECK(info.max_objects == 256);
    CHECK(info.max_bytes == (1u << 20));
    CHECK(!info.auto_subscribe);
    CHECK(info.surface_stream_terminals);
    CHECK(info.max_stream_terminals == 64);
    CHECK(info.transport_version == (uint32_t)ver);
    CHECK(info.companion_profile == (ver == MOQ_VERSION_DRAFT_22
                                         ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
                                         : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01));
    CHECK(info.track_events_polled >= 2);   /* TRACK_ADDED, CATALOG_READY */
    uint64_t polled = info.track_events_polled;
    CHECK(moq_media_receiver_get_info(f.r, &info, MOQ_MEDIA_RECEIVER_INFO_V0_SIZE - 1) ==
          MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_media_receiver_get_info(NULL, &info, sizeof(info)) == MOQ_ERR_INVAL);
    CHECK(moq_media_receiver_get_info(f.r, &info, sizeof(info)) == MOQ_OK);
    CHECK(info.track_events_polled == polled);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The bounded poll: a head larger than the bound stays queued with its
 * charge reported; a large enough bound transfers that same object, whose
 * charge matches. */
static void test_poll_within(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "poll_within_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    /* With LOC properties, so the charge counts that buffer too. */
    moq_loc_metadata_t md;
    moq_loc_metadata_init_sized(&md, sizeof(md));
    md.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    md.timestamp = 1700000000000000u;
    moq_rcbuf_t *props = NULL;
    REQUIRE(moq_loc_encode_metadata(moq_alloc_default(), ver,
        ver == MOQ_VERSION_DRAFT_22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01,
        &md, &props) == MOQ_OK);
    for (uint64_t o = 0; o < 2; o++) {
        uint8_t b[8] = { 0 };
        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
        moq_pub_object_cfg_t oc;
        moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
        oc.group_id = 0;
        oc.object_id = o;
        oc.payload = pay;
        oc.properties = props;
        CHECK(moq_pub_write_object_ex(f.pub, f.video, &oc, f.now) == MOQ_OK);
        moq_rcbuf_decref(pay);
    }
    moq_rcbuf_decref(props);
    cycles(&f, 16);
    moq_media_object_t obj;
    uint64_t charge = 0;
    CHECK(moq_media_receiver_poll_object_within(f.r, 3, &charge, &obj, sizeof(obj)) ==
          MOQ_ERR_BUFFER);
    CHECK(charge >= 8);
    uint64_t need = charge;
    CHECK(moq_media_receiver_poll_object_within(f.r, need - 1, NULL, &obj, sizeof(obj)) ==
          MOQ_ERR_BUFFER);
    charge = 0;
    REQUIRE(moq_media_receiver_poll_object_within(f.r, need, &charge, &obj, sizeof(obj)) ==
            MOQ_OK);
    CHECK(charge == need);
    CHECK(obj.group_id == 0 && obj.object_id == 0);
    CHECK(obj.properties_ref != NULL);
    CHECK(moq_media_object_charge(&obj) == charge);
    CHECK(charge > moq_rcbuf_len(obj.payload_ref));
    moq_media_object_cleanup(&obj);
    REQUIRE(moq_media_receiver_poll_object_within(f.r, UINT64_MAX, NULL, &obj, sizeof(obj)) ==
            MOQ_OK);
    CHECK(obj.object_id == 1);
    moq_media_object_cleanup(&obj);
    CHECK(moq_media_object_charge(NULL) == 0);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

static bool poll_ended(fx_t *f, size_t size, moq_media_track_event_t *out)
{
    for (unsigned i = 0; i < 64; i++) {
        cycle(f);
        moq_media_track_event_t ev;
        memset(&ev, 0, sizeof(ev));
        while (moq_media_receiver_poll_track(f->r, &ev, size) == MOQ_OK)
            if (ev.kind == MOQ_MEDIA_TRACK_ENDED) {
                *out = ev;
                return true;
            }
    }
    return false;
}

/* TRACK_ENDED from an error done carries its status; from END_OF_TRACK it
 * carries none; a caller struct short of the block gets none. */
static void test_done_status(moq_version_t ver, int variant)
{
    static const char *const vn[] = { "error", "end_of_track", "short_struct",
                                      "partial_block" };
    char label[64];
    snprintf(label, sizeof(label), "done_status_%s_%s", vn[variant], vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    write_obj(&f, 0, 0, false);
    cycles(&f, 8);
    if (variant == 1) CHECK(moq_pub_end_track(f.pub, f.video, f.now) == MOQ_OK);
    /* variant 2 and 3: the done, read through a struct short of the block. */
    else CHECK(moq_pub_finish_subscribers(f.pub, f.video, 0x5u, f.now) == MOQ_OK);
    size_t size = variant == 2 ? offsetof(moq_media_track_event_t, has_done_status)
                : variant == 3 ? offsetof(moq_media_track_event_t, done_status) + 4
                               : sizeof(moq_media_track_event_t);
    moq_media_track_event_t ev;
    REQUIRE(poll_ended(&f, size, &ev));
    CHECK(ev.track == f.track);
    if (variant == 0) {
        CHECK(ev.has_done_status);
        CHECK(ev.done_status == 0x5u);
    } else {
        CHECK(!ev.has_done_status);
        CHECK(ev.done_status == 0);
    }
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The descriptor's render group and label presence. */
static void test_desc(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "desc_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    moq_media_track_desc_t d;
    REQUIRE(moq_media_receiver_track_desc_copy(f.r, f.track, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.has_render_group && d.render_group == 3);
    CHECK(d.has_label && d.label.len == 0);
    const moq_media_track_desc_t *g = moq_media_track_desc_get(f.track);
    REQUIRE(g != NULL);
    CHECK(g->has_render_group && g->render_group == 3);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

int main(void)
{
    const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                   MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < 3; i++) {
        test_info(vers[i]);
        test_poll_within(vers[i]);
        for (int v = 0; v < 4; v++) test_done_status(vers[i], v);
        test_desc(vers[i]);
    }
    if (failures) {
        printf("FAIL: %u receiver player-support failures\n", failures);
        return 1;
    }
    printf("PASS: all receiver player-support tests\n");
    return 0;
}
